# Conductor

[Conductor OSS](https://github.com/conductor-oss/conductor) 3.31.0 — the engine
Netflix built and contributed out, now maintained by Orkes. Apache 2.0.

**Status: all 4 DAGs verified** (2026-08-06, server 3.31.0, `conductor-python`
2.0.0, Python 3.14). Every spec edge case exercised, including all three saga
triggers and the compensation dead-letter. **Fifteen defects** found and fixed —
five in the engine's own shipped configuration and SDK, ten in this
implementation.

Conductor is the only tool in the bake-off where the workflow definition is
**data pushed over HTTP** and the task bodies are a separate, independently
deployed process. Nothing scans a folder; nothing is registered by a connecting
worker. That split is the thing to look at.

## Layout

```
conductor/
  workflows/*.json      10 workflow definitions -- the orchestration itself
  taskdefs.json         28 task definitions -- retry policy, timeouts, concurrency caps
  dag{1,2,3,4}_*.py     task bodies only (@worker_task functions)
  worker.py             hosts all 26 task types; POLLS the server
  register.py           pushes workflows + taskdefs to the server
  start_workflow.py     starts a run, follows it, prints task transitions
  stop_worker.sh        stops the worker AND its 26 spawned children (see Findings)
  env.sh                host-side wiring -- source this first, always
```

Engine config lives in `shared-services/conductor/config.properties` (mounted
into the container) — **not** in environment variables, for a reason documented
at the top of that file and in Findings below.

## Launch

### 1. Backbone + engine

```bash
just up                     # postgres + the four mock services
just up conductor           # + the Conductor server (API :8000, UI :8127)
just seed conductor         # conductor_dag1 / _dag3 / _dag4 schemas + fixtures
```

The engine is a **single container**: Spring Boot on :8080 and nginx serving the
UI on :5000 internally. It needs **no Elasticsearch** —
`conductor.indexing.type=postgres` puts metadata, the task queues *and* the
search index in the shared Postgres. After hatchet-lite this is the lightest
engine in the bake-off.

Wait for health before doing anything else — first boot runs Flyway migrations:

```bash
until curl -sf -o /dev/null http://localhost:8000/health; do sleep 3; done
```

### 2. Register the definitions

```bash
source conductor/env.sh
uv run python conductor/register.py
```

Idempotent; re-run it after editing anything under `workflows/` or
`taskdefs.json`. It re-reads the definitions back off the server afterwards,
because a 200 from the bulk upsert is not proof the server kept what you sent.

### 3. Start the worker

```bash
source conductor/env.sh
uv run python conductor/worker.py
```

**Stop it with Ctrl-C or `./conductor/stop_worker.sh` — never `pkill -f
worker.py`.** See Findings; this one cost real time.

### 4. Run the DAGs

```bash
source conductor/env.sh
uv run python conductor/start_workflow.py dag1 --wait
uv run python conductor/start_workflow.py dag2 --wait
uv run python conductor/start_workflow.py dag3 --amount 250 --wait
uv run python conductor/start_workflow.py dag4 --order-total high --wait
```

UI: <http://localhost:8127> — the execution graph, per-task input/output, and
the task queues are all there, with no login because Conductor OSS has no
authentication at all.

### Edge-case switches

| Command | Exercises |
|---|---|
| `dag2 --no-auto-resume` | callback never arrives → WAIT times out |
| `dag2 --empty` | fetch succeeds with zero items → SWITCH `defaultCase` |
| `dag3 --from-account ACC-004` | suspended account → validation branch |
| `dag4 --bad-address` | non-retriable `InvalidAddress` → saga |
| `dag4 --no-decide` + `AUTO_DECIDE_ACTION=none` | approval timeout → saga |
| `AUTO_DECIDE_ACTION=rejected` | approval rejected → saga |

Worker-side fault injection (set on the **worker**, not the starter — the worker
executes the task):

```bash
GATEWAY_SUCCESS_RATE=0 GATEWAY_TIMEOUT_RATE=1 ...   # DAG 3 retriable exhaustion
GATEWAY_SUCCESS_RATE=0 GATEWAY_TIMEOUT_RATE=0 GATEWAY_SERVER_ERROR_RATE=0  # declined
FORCE_SHIPPING_FAILURE=1                            # DAG 4 shipping saga
FORCE_SHIPPING_FAILURE=1 FORCE_COMPENSATION_FAILURE=1   # dead-letter
```

## What this implementation demonstrates

- **`FORK_JOIN_DYNAMIC`** — true runtime task creation. A worker returns a list
  of task descriptors and the engine materialises one real task per element.
  Used for DAG 1's per-CSV load and DAG 2's per-item detail fetch.
- **`WAIT`** — suspension that costs nothing: no worker, no thread, no poll.
  Resumed by an HTTP POST from outside.
- **`SUB_WORKFLOW`** — DAG 4's three sub-workflows are separate registered,
  independently versioned, independently startable definitions.
- **`failureWorkflow`** — Conductor's saga trigger, used in DAG 3 and DAG 4.
- **`SWITCH` / `TERMINATE`** — conditional branching and early exit with a
  chosen terminal status and output.
- **`optional: true`** — graceful degradation declared rather than coded.
- **`NonRetryableException`** → `FAILED_WITH_TERMINAL_ERROR`, verified to skip
  the remaining retries entirely.
- **Retry policy as data** — `retryLogic`/`retryDelaySeconds`/`concurrentExecLimit`
  live on the task definition, shared by every workflow that references the task.

## Findings

### Definitions are data, and that is genuinely different

Every other tool in the bake-off ships workflow *code* (Airflow, Prefect,
Dagster, Temporal, Hatchet, Flyte, Luigi) or workflow *files* that a server
loads (Kestra, Argo, Step Functions, Google Workflows). Conductor is the only
one where the definition is a JSON document `PUT` into a running server's
metadata store and versioned there.

The consequences are real and cut both ways:

- **Changing orchestration needs no worker deploy.** Re-run `register.py` and
  the next execution uses the new graph. Only a change to a *task body* needs
  the workers restarted. That is a cleaner split than any other tool here.
- **`version` is first-class.** Bumping it leaves the old definition addressable,
  so in-flight executions finish on the graph they started with. Argo and Kestra
  have no equivalent; Temporal makes you write versioning code by hand.
- **But the definition is not the source of truth.** The server is. Nothing stops
  someone editing a workflow in the UI, and then the repo and the engine
  disagree with no error anywhere. `register.py` re-reads after writing for
  exactly this reason. A production deployment would need drift detection that
  none of the tooling provides.

### The expression language is deliberately thin, so logic moves into workers

`${task.output.field}` substitution and `SWITCH` on a value is roughly all there
is. There is no `len()`, no comparison, no arithmetic. Every branch decision in
this implementation is therefore precomputed by a worker into a string that the
JSON merely routes on — `approval_required: "yes"|"no"`, `has_items: "yes"|"no"`.

This is a defensible design (the graph stays inspectable; logic stays testable
in Python) but it means the JSON is not self-explanatory: you cannot read
`dag4_order_fulfillment.json` and learn what the approval threshold is. Kestra's
Pebble and Argo's Go templates both let more logic live in the definition.

### Task timeouts are sweeper-enforced, and fire late

**Measured: a 60s `timeoutSeconds` fired at 103s; a 120s one fired at 180s.**
Both with `conductor.app.workflowOffsetTimeout=5s`, well below the 30s default.

Timeouts are not timers. A background sweeper re-decides running workflows and
notices that a task has overrun; the lateness is bounded by how often that sweep
reaches your workflow, not by the value you configured. Treat `timeoutSeconds`
as **"not before"**, never as a deadline. For a human-approval SLA measured in
hours this is irrelevant; for a 60s API callback it is a 70% overshoot.

Temporal, by contrast, fires timer-driven timeouts to the second.

### `outputParameters` are evaluated even when the workflow fails

A literal in `outputParameters` appears in the output of a **failed** run. The
first version of DAG 3 had `"outcome": "paid"`, and a declined payment therefore
reported `outcome: paid` alongside `status: FAILED`. DAG 4 had the same bug with
`"shipped"`. Both now derive the value from a task that only runs on the success
path, so it comes back `null` when that path was not taken.

Anything reading workflow output must check `status` first. This is a sharp edge
for downstream consumers and easy to ship without noticing.

### `failureWorkflow` inherits the failed workflow's input — including the gaps

The failure workflow receives the failed workflow's **entire input map**, plus
`workflowId`, `reason`, `failureStatus`, `failureTaskId` and `failedWorkflow`.
That inheritance is what makes compensation addressable without plumbing: it
already knows `order_id`.

The trap is that keys *absent* from that input still resolve. `dag4_compensation`
reads `${workflow.input.cancel_status}`, which the rejection path supplies and
the failureWorkflow path does not — so it arrives as an **explicit `null`**,
which overrides the Python default and hit a `NOT NULL` constraint. A parameter
with a default is not safe; it must be coerced (`status = status or "cancelled"`).

### Two saga mechanisms, and using both is right

DAG 4 triggers compensation three ways, deliberately split across both:

| Trigger | Mechanism | Why |
|---|---|---|
| Approval **rejected** | `SWITCH` → explicit `SUB_WORKFLOW` | A planned business outcome. Visible in the execution graph; the workflow ends **COMPLETED** with `outcome: cancelled_by_rejection`, because correctly cancelling an order is a success. |
| Approval **timeout** | `failureWorkflow` | Unplanned. The WAIT times out, the sub-workflow fails, the parent fails, Conductor starts the compensation. |
| **Shipping failure** | `failureWorkflow` | Unplanned, same path — both after retry exhaustion and after a terminal `InvalidAddress`. |

Verified end to end, with inventory restored and the order cancelled in every
case. Compensation is idempotent by construction (`WHERE status = 'reserved'`),
confirmed by running it twice: `released_count: 0, already_released: true`, no
double credit.

The dead-letter also works: `dag4_compensation` has its own `failureWorkflow`,
so when compensation itself exhausts its retries the order lands in
`compensation_failed` with `MANUAL INTERVENTION REQUIRED`. Verified with
`FORCE_COMPENSATION_FAILURE=1`.

### Resume is the cleanest of any tool here

```
POST /api/tasks/{workflowId}/{taskRefName}/COMPLETED
Content-Type: application/json

{...}   # becomes the task's output
```

No SDK. No token. No relay process. No authentication. The `resume_data` handle
is just `{workflow_id, task_ref_name}`. Compare:

- **Kestra** — needs basic auth *and* multipart/form-data; `execution.resumeUrl`
  does not exist in any version.
- **Hatchet** — cannot be an HTTP callback target at all (bearer auth +
  `{key,data}` envelope), so it needs `hatchet/event_relay.py`.
- **Temporal** — needs a signal client, hence `temporal/signal_server.py`.
- **Step Functions** — needs a task token and `boto3`.

Conductor needed **zero** new infrastructure: the mock services POST directly to
the engine.

The reason is also the problem: **there is no authentication to satisfy.**
Anyone who can reach the API can complete any task in any workflow, read every
execution, and rewrite every definition. That is fine behind a mesh and
disqualifying otherwise, and it is the single biggest mark against Conductor OSS.

### One OS process per task type

`TaskHandler` starts a **process** per registered task type — 26 here, measured
at **1.72 GB RSS idle** (~65 MB each) plus a 106 MB supervisor. It is not a
thread pool over a shared queue. For a worker hosting a
handful of task types that is fine; for one hosting fifty it is not, and the
answer is to split workers by task type (which Conductor supports well, via
`domain` and separate deployments) rather than to scale the process count.

### Conductor OSS vs Orkes — nothing the spec needed was gated

Everything in all four DAGs is in the Apache-2.0 OSS build. The `HUMAN` task
exists in OSS as a stub that waits indefinitely for external completion, which is
all DAG 4 needs (this implementation uses `WAIT`, which is equivalent for the
purpose and better documented). What Orkes gates is the *surrounding* product:
visual editor, human-task **forms**, AI/agent tasks, event connectors, RBAC/SSO.
No orchestration primitive was missing.

## Defects found and fixed (2026-08-06)

**In Conductor's own shipped artefacts (5):**

1. **The shipped `config-postgres.properties` does not boot the shipped image.**
   It sets `conductor.file-storage.enabled=true` + `type=conductor`, for which
   3.31.0 has no matching `FileStorage` bean: *"Parameter 0 of constructor in
   FileStorageServiceImpl required a bean of type FileStorage that could not be
   found"*, and the server exits. Disabled it.
2. **`SPRING_DATASOURCE_*` environment variables are silently ignored.**
   `Conductor.loadExternalConfig()` reads the `CONFIG_PROP` file and calls
   `System.setProperty()` for every key; Java system properties outrank OS
   environment variables in Spring Boot, so the file always wins. The standard
   12-factor way to configure a Spring Boot image does not work on this image.
   Fixed by mounting our own properties file.
3. **…and the failure mode is opaque.** With the datasource wrong, Hikari logs
   `HikariPool-1 - Starting...` once a second for ~25s with no error, then fails
   with the real cause (`UnknownHostException: postgresdb` — a sidecar the
   compose file we were not using would have provided) buried at the bottom of a
   six-level nested Spring bean-creation stack trace.
4. **`conductor.app.sweeperFrequencyMillis` does not exist** in 3.31.0 (zero
   occurrences in the source) despite being widely recommended. Spring ignores
   unknown keys, and **`/api/admin/config` echoes back whatever you set rather
   than what the server bound**, so a dead property looks perfectly applied.
   Replaced with the real `sweeperThreadCount` / `sweeperWorkflowPollTimeout` /
   `workflowOffsetTimeout`.
5. **Endpoint cardinality is inverted between sibling resources.**
   `POST /metadata/workflow` takes **one** object (a list 500s with a Jackson
   deserialisation error); `POST /metadata/taskdefs` takes a **list**. Also, the
   task-completion endpoint is `POST /api/tasks/{workflowId}/{taskRefName}/{status}`,
   not the `/api/queue/update/...` that older docs still give.

**In `conductor-python` 2.0.0 (2):**

6. **A bare `list` annotation on a worker parameter crashes the argument binder.**
   `convert_from_dict_or_list` calls `typing.get_args(cls)[0]` whenever the
   runtime value is a list, so an unparameterised `list` raises
   `IndexError: tuple index out of range` before the task body ever runs. Must
   be `list[dict]`. Bare `dict` is fine, which makes the asymmetry easy to miss.
   Hit at five call sites.
7. **`pkill -f worker.py` orphans every task-runner child.** `TaskHandler` runs
   each task type in a spawned child whose cmdline is
   `python -c from multiprocessing.spawn import spawn_main; ...` — no mention of
   `worker.py`. Killing the supervisor leaves all 26 polling forever, claiming
   tasks and executing them with stale code and a stale environment, so a
   "restarted" worker silently competes with every previous generation.
   **114 orphans accumulated across three restarts**, and the symptom was DAG 3
   returning results from three different code versions at random — which looked
   exactly like a flaky gateway. Fixed with a SIGTERM handler in `worker.py` and
   `stop_worker.sh`. Compare Hatchet, where a SIGKILLed worker stays ACTIVE in
   the engine and survives an engine restart: same class of trap.

**In this implementation (8):**

8. `prepare_csv_fanout(csv_files: list)` and four siblings — defect 6 above.
9. `call_shipping_api` omitted the required `shipping_address`, got FastAPI's
   **422**, and — because it classified on status code rather than the service's
   `error_type` — reported a clean `InvalidAddress`. A malformed request
   masquerading as a successful saga trigger, and the most dangerous defect here
   because the workflow *looked* correct end to end. Now keys off `error_type`
   and distinguishes a business rejection from a bad request.
10. `"outcome": "paid"` in DAG 3's `outputParameters` — leaked into failed runs.
11. `"outcome": "shipped"` in DAG 4's — same.
12. `update_order_cancelled(status="cancelled")` — the default never applied on
    the `failureWorkflow` path, which passes an explicit `null`. `NOT NULL`
    violation.
13. Gateway rates were module-level constants read at import time, so forcing a
    branch required reasoning about when each of 26 processes imported the
    module. Now read per call.
14. DAG 2's fan-out did not rewrite `fixture-service` → `localhost` in detail
    URLs. The collection is fetched by a *container* so the URLs it derives point
    at the compose DNS name, but the fan-out runs on the *host*. Caught before
    first run from CLAUDE.md's warning, which is why it is listed here rather
    than as a failure.
15. `per_page=0` was the wrong way to test DAG 2's empty branch — fixture-service
    422s, which exercises the *error payload* case instead. `page=99999` returns
    a bare `[]` with HTTP 200.

## Verified behaviour

| Capability | Evidence |
|---|---|
| Dynamic fan-out | DAG 1: 3 tasks from 3 CSVs; DAG 2: 5 detail fetches, all runtime-created |
| Suspend/resume | DAG 2 + DAG 4, workflow `IN_PROGRESS` with zero workers engaged, resumed by HTTP POST |
| Callback timeout | DAG 2 `--no-auto-resume` → `TIMED_OUT` (at 103s for a 60s config) |
| Callback error payload | DAG 2 with a 422 upstream → task `FAILED` |
| Empty-result branch | DAG 2 `--empty` → `SWITCH` defaultCase → `TERMINATE` COMPLETED |
| Retry + exponential backoff | DAG 3: 5 attempts at +2s/+4s/+8s/+16s; DAG 4 shipping at +2s/+4s/+8s |
| Non-retriable classification | `FAILED_WITH_TERMINAL_ERROR` with `retryCount=0` despite `retryCount: 4` |
| Idempotency | DAG 3 replayed key → rejected at validation, balances unchanged |
| Graceful degradation | `optional: true` notification tasks |
| Saga via rejection | inventory restored, order `cancelled`, workflow COMPLETED |
| Saga via timeout | `failureWorkflow` ran, inventory restored |
| Saga via shipping failure | both retry-exhaustion and terminal `InvalidAddress` |
| Idempotent double-compensation | `released_count: 0, already_released: true` |
| Compensation dead-letter | order → `compensation_failed`, manual-intervention flag |
| Concurrent last-unit race | 2 workflows, 2 units: stock 2 → 0, never negative, loser compensated |
| Schema isolation | all writes confined to `conductor_dag{1,3,4}` |
| Inventory conservation | totals identical to seed after the full suite |

## Not yet exercised

- **Multi-worker / `domain` routing.** One worker process hosts all 26 task
  types. Conductor's task `domain` feature (route a task type to a specific
  worker pool) is the mechanism for dependency isolation and is untested here.
- **Restart/replay from the UI.** `restartable: true` is set on every definition
  but no run has been restarted.
- **Durability under worker loss.** Not tested the way Temporal's was (`kill -9`
  mid-approval). Conductor's model suggests it should survive — state is in
  Postgres and workers are stateless pollers, so a suspended WAIT task is
  entirely server-side — but that is analysis, not evidence.
- **External payload storage.** `conductor.external-payload-storage.type=postgres`
  is configured; no payload here is large enough to trigger it.
- **Scale.** Largest fan-out was 5. `concurrentExecLimit` (10 for DAG 1, 20 for
  DAG 2) is registered and honoured by the engine but was never actually the
  binding constraint.
- **Authentication.** There is none to test in OSS.

# Temporal

Temporal Python SDK implementation of the four bake-off DAGs. See `../README.md`
for the DAG specs and `../RUNNING.md` for cross-cutting setup (container
networking, the resume-broker model, teardown).

- **Definition style:** imperative Python, `@workflow.defn` / `@activity.defn`
- **Wait mechanism:** native signals — `workflow.wait_condition()` on signal state, relayed by `signal_server.py`
- **Engine:** `temporalio/auto-setup` container (`:7233`, UI `:8233`); worker runs on the host
- **Schema namespace:** `BAKEOFF_NS=temporal` → `temporal_dag1` / `_dag3` / `_dag4`

---

## Launch

**Status: all four DAGs verified end-to-end on 2026-08-02** — Temporal Server
1.27.2, UI 2.34.0, `temporalio` SDK 1.29.0, Python 3.14.2, Podman.

### 1. Backbone + engine

```bash
just up temporal         # postgres :54321, mocks :8090-8092, temporal :7233, UI :8233
just seed temporal       # create temporal_dag{1,3,4} schemas + seed fixtures
```

**Wait for the server to finish acquiring its shards** before starting anything.
Immediately after `up`, client calls fail with `RPCError: shard status unknown`
and then `RPCError: Timeout expired`. Roughly 30–60s on this machine:

```bash
until podman logs shared-services_temporal_1 2>&1 | grep -q "Acquired shard"; do sleep 3; done
```

`just up temporal` also starts a **`temporal-worker` container**. It competes for
the same task queue as the host worker below — stop one or the other:

```bash
podman stop shared-services_temporal-worker_1
```

### 2. Worker + signal relay (host)

Two processes, two shells. Both must be running before any workflow is started.

```bash
cd temporal

# Worker
TEMPORAL_ADDRESS=localhost:7233 \
POSTGRES_HOST=localhost POSTGRES_PORT=54321 \
POSTGRES_DB=orchestration POSTGRES_USER=orchestration POSTGRES_PASSWORD=orchestration \
BAKEOFF_NS=temporal \
CALLBACK_FETCH_SERVICE_URL=http://localhost:8090 \
APPROVAL_SERVICE_URL=http://localhost:8091 \
SHIPPING_SERVICE_URL=http://localhost:8092 \
SIGNAL_SERVER_URL=http://host.containers.internal:8095 \
  uv run --project .. python worker.py

# Signal relay (separate shell) -- must bind 0.0.0.0, containers call into it
TEMPORAL_ADDRESS=localhost:7233 \
  uv run --project .. uvicorn signal_server:app --host 0.0.0.0 --port 8095
```

`SIGNAL_SERVER_URL` is baked into the `callback_url` the workflows hand to the
mock-service *containers*, so it must be the host gateway as seen from a
container (`host.containers.internal` on Podman) — never `localhost`. Everything
else is `localhost` because the worker itself runs on the host against published
ports.

### 3. Run the DAGs

`start_workflow.py` is the client — Temporal has no deployment registry, so a
workflow only exists once a client starts it against the task queue.

```bash
cd temporal
export TEMPORAL_ADDRESS=localhost:7233

uv run --project .. python start_workflow.py dag1     # ~1s
uv run --project .. python start_workflow.py dag2     # ~4s  (GitHub rate limit — see below)
uv run --project .. python start_workflow.py dag3     # ~3s
uv run --project .. python start_workflow.py dag4     # ~10s (10s auto-approve delay)
```

Every subcommand has working defaults; ids auto-generate so runs don't collide
with the idempotency checks. Flags for reaching specific branches:

| Command | Reaches |
|---|---|
| `dag3 --from-account ACC-004` | validation failure (suspended account) |
| `dag3 --amount 999999` | validation failure (insufficient balance) |
| `dag3 --payment-id X` twice | duplicate-payment branch |
| `dag4 --total low` | under the $500 threshold, approval skipped |
| `dag4 --total high` | manager approval required |
| `dag4 --bad-address` | non-retryable `InvalidAddress` → saga compensation |

Inspect runs in the UI at <http://localhost:8233>, or:

```bash
podman exec shared-services_temporal_1 temporal workflow list --address temporal:7233
podman exec shared-services_temporal_1 temporal workflow show --workflow-id <id> --address temporal:7233
```

### Forcing the DAG 4 edge cases

The approval service auto-approves after 10s (`AUTO_DECIDE_ACTION=approved`), so
the other two saga triggers need it out of the way:

- **Rejection** — decide before the auto-decider fires:
  ```bash
  id=$(curl -s 'http://localhost:8091/approval-requests?status=pending' | jq -r '.approval_requests[0].approval_request_id')
  curl -sX POST "http://localhost:8091/approval-requests/$id/decide" \
    -H 'content-type: application/json' \
    -d '{"decision":"rejected","approver":"me","reason":"testing"}'
  ```
- **Timeout** — recreate the approval service with auto-decide off, then wait out
  the **hardcoded 120s** `wait_condition` timeout:
  ```bash
  podman rm -f shared-services_approval-service_1
  podman run -d --name shared-services_approval-service_1 \
    --network shared-services_default --network-alias approval-service \
    -p 8091:8091 -e AUTO_DECIDE_ACTION=none \
    localhost/shared-services_approval-service:latest \
    uvicorn app:app --host 0.0.0.0 --port 8091
  # restore afterwards: podman rm -f ... && just up temporal
  ```
- **Worker crash mid-workflow** — same auto-decide-off setup, then with DAG 4
  waiting for approval: `kill -9 $(pgrep -f "python worker.py")`, decide the
  approval while it is down, and restart the worker with the same env block. It
  picks up where it left off. Keep the outage under the child workflow's 180s
  execution timeout.

### Re-running

DAG 3 keys idempotency on `payment_id`, DAG 4 on `order_id`; both auto-generate.
DAG 4 *consumes inventory* — `WIDGET-A` starts at 100 and `--total high` reserves
40 per run, so the fourth consecutive run fails validation with `Insufficient
stock`. Reset with `just down-clean && just up temporal && just seed temporal`,
or restore the four rows directly.

---

## What this implementation demonstrates

| DAG | Temporal idioms |
|---|---|
| 1 | `asyncio.gather()` fan-out over activities, shared `RetryPolicy`, dataclass I/O through the default data converter |
| 2 | `@workflow.signal` + `@workflow.query`, `workflow.wait_condition()` with a timeout, external HTTP → signal relay |
| 3 | `RetryPolicy(non_retryable_error_types=[...])` for retriable-vs-terminal classification, try/except failure branch |
| 4 | Child workflows as composition units, signal-based human approval, saga compensation as a plain reversed `compensations` list in ordinary `try/except` |

---

## Findings

Observations bearing on `../comparison.md` (Temporal currently scores **88**).

### The four DAGs ran as committed — no code defects

This is the notable result. Prefect needed five blocking fixes and Airflow
fourteen before anything ran; Temporal's four workflows and 20 activities
executed correctly on the first attempt, including both suspend/resume paths and
all three saga triggers. Every failure encountered during this session was
environmental (server warmup, exhausted fixtures) or a deliberate edge case.

What was missing was **not code but a way to invoke it**: no starter client
existed, so there was no path from `worker.py` to a running workflow. Added as
`start_workflow.py` (see Fixes).

### Signals are a genuinely native suspend — and cost nothing

DAG 2's history is the evidence:

```
11  TimerStarted                 <-- wait_condition timeout arms
12  WorkflowExecutionSignaled    <-- callback-fetch-service -> signal_server -> signal
16  TimerCanceled
```

The workflow is not running while suspended — no process, no slot, no polling
loop — and it resumes mid-function with local state intact. Contrast Prefect,
where zero-cost suspension requires a deployment, `persist_result`, re-execution
from the top, and `as_subflow=False` (which destroys the sub-workflow lineage).
DAG 4 gets both at once here: `ManagerApprovalWorkflow` is a real child workflow
*and* a durable wait, visible as a parent/child tree in the UI. That mutual
exclusivity Prefect suffers simply doesn't arise.

The one piece Temporal doesn't supply is the HTTP edge: an external system can't
POST to Temporal directly, so `signal_server.py` (78 lines of FastAPI) translates
`POST /fetch-callback?workflow_id=…` into `handle.signal(...)`. Step Functions'
`.waitForTaskToken` needs no such shim. Small, but it is a real extra component
to run and secure.

### Saga compensation is ordinary code, and it works

`OrderFulfillmentWorkflow` keeps a `compensations` list and unwinds it in reverse
inside `except`. No framework primitive — same as Prefect — but the difference is
what backs it: durable execution means a worker crash mid-compensation resumes
where it left off rather than losing the stack. All three triggers verified
(rejection, timeout, shipping failure), each leaving the DB fully consistent.

Two rough edges worth noting:

- **Compensated runs end `FAILED`.** The workflow compensates and then raises
  `ApplicationError(type="OrderCancelled")`. Correct semantically, but a clean
  compensation and a crash look identical in the UI without opening the run —
  the same criticism `prefect/README.md` makes.
- **`OrderFulfillmentOutput` has `status: "shipped" | "cancelled"` and a
  `failure_reason` field that is never populated**, because the cancel path
  raises instead of returning. Dead fields; either return the cancelled output or
  drop them.

### Durable execution: verified by killing the worker mid-approval

The 10/10 durability score is the one claim worth testing directly rather than
citing. `SIGKILL` on the worker (no graceful shutdown) while DAG 4 sat waiting
for manager approval, **69 seconds** of total worker downtime, and the approval
decided *during* the outage. The child workflow's history is the whole argument:

```
11  05:20:30  TimerStarted               <-- 120s approval timeout arms
                                          -- 05:20:53  kill -9 the worker
12  05:21:07  WorkflowExecutionSignaled  <-- approval decided with NO worker alive
13  05:21:07  WorkflowTaskScheduled
14  05:21:17  WorkflowTaskTimedOut       <-- 10s task timeout, nobody polling
15  05:21:17  WorkflowTaskScheduled      <-- redelivered, not failed
                                          -- 05:22:02  worker restarted
16  05:22:02  WorkflowTaskStarted        <-- new process picks up 55s-old task
18  05:22:02  TimerCanceled
25  05:22:02  WorkflowExecutionCompleted
```

What this demonstrates, point by point:

- **The signal was accepted with no worker running.** The approval service got
  `callback_status: 200` at 05:21:07; the server persisted the event. Nothing was
  buffered in a process that then died — the event is in history before any
  worker sees it.
- **`temporal task-queue describe` showed `ApproximateBacklogCount: 1`,
  `TasksDispatchRate: 0`,** last poller "25 seconds ago". The work was parked
  server-side, visibly.
- **A missed workflow task is redelivered, not failed.** Event 14 is
  `WorkflowTaskTimedOut` followed immediately by a fresh `WorkflowTaskScheduled`.
  The default 10s task timeout expired four times over during the outage and cost
  nothing.
- **The replacement process resumed mid-function with local state intact.** It
  was a different PID with an empty heap; it rebuilt the workflow's state by
  replaying history, then continued at the `wait_condition` as though it had
  never stopped.
- **Completed work was not redone.** `reserve_inventory` had already run before
  the kill; inventory moved 96→56 exactly once and the reservation row stayed
  single. Activity results come from history on replay, so side effects are not
  repeated.
- **Ordering beat the clock.** The signal landed at 05:21:07 and the 120s timer
  would have fired at 05:22:30. Because replay follows history order, the
  already-recorded signal won even though the worker only returned at 05:22:02 —
  the outage could have outlasted the timeout without changing the outcome.

The run finished `shipped` with tracking, and as a bonus the shipping activity
hit a real 504 on its first attempt and retried through it. Total cost of a hard
worker crash mid-workflow: **zero** — no lost work, no duplicate side effects, no
manual intervention, and nothing in the final result distinguishes it from a
clean run.

For the comparison, this is the concrete difference between "has retries" and
"durable execution". Airflow's deferrable operators survive a triggerer restart
because the wait is server-side too, but the *task* state is re-derived from the
DAG-run row rather than replayed; Prefect's paused run holds a process, and its
suspended run re-executes the flow function from the top on resume. Temporal
resumed a partially-executed function.

### Non-retryable classification is declarative and correct

`non_retryable_error_types=["PaymentDeclined"]` on the `RetryPolicy` — one line,
no hook, no re-raising inside a callback the way Prefect's `retry_condition_fn`
requires. Verified both directions in the same batch of runs: two random declines
failed on attempt 1 with zero retries, while gateway 500s and timeouts retried
and then succeeded.

Also worth crediting for observability: **retries do not pollute history.**
Attempts appear as attempt counters on the pending activity, and only the
terminal failure emits `ActivityTaskFailed`. A 3-attempt activity leaves the same
history shape as a 1-attempt one, which is why `grep -c ActivityTaskFailed`
returns 0 for runs that visibly retried — look at the worker log or the pending
activity, not the event count.

### Gaps in this implementation (not in Temporal)

- **The spec's concurrency caps are absent.** DAG 1 (10) and DAG 2 (20) both use
  bare `asyncio.gather()`, which is unbounded, and `worker.py` sets no
  `max_concurrent_activities` (SDK default 100). Same defect class as Prefect's.
  Temporal can express both — a `asyncio.Semaphore` in workflow code for the
  per-run cap, worker options or a task-queue rate limit for the global one — but
  neither is wired up. DAG 1's sample ZIP has only 3 CSVs and DAG 2's fixture
  returned 8 items, so nothing hit a limit during testing.
- **DAG 4's 120s approval timeout is hardcoded** in the workflow body rather than
  a parameter, so exercising the timeout path means editing code or waiting the
  full two minutes. Prefect plumbs it through as a flow parameter; worth copying.
- **DAG 3's gateway failure rates are hardcoded** (5% decline / 15% 500 / 20%
  timeout) inside `process_payment`, unlike the shipping service's env-tunable
  rates. Reaching the decline branch took ~20 runs. An env knob would make it
  deterministic.

### Operational notes

- **Shard flapping on startup.** The single-node `auto-setup` server repeatedly
  logged `shard status unknown` / re-`Acquired shard` for the first minute, and
  one `start_workflow` call died with `RPCError: Timeout expired` mid-session.
  Retrying worked. Plausibly WSL2 + a shared Postgres also serving the bake-off
  schemas; worth knowing it is transient rather than a config error.
- **`auto-setup` shares the bake-off Postgres**, creating `temporal` and
  `temporal_visibility` databases beside `orchestration`. Fine for evaluation,
  and it keeps the footprint to one database server, but it is why the engine's
  load and the DAGs' load land on the same instance.
- **The compose `temporal-worker` service contradicts the compose comment**,
  which says "no Dockerfiles exist for them" — `temporal/Dockerfile` does exist
  and the service builds and starts. Its `SIGNAL_SERVER_URL` uses
  `host.docker.internal`, which happens to work here because Podman aliases that
  name to `10.255.255.254` alongside `host.containers.internal` (unlike the mock
  services, which *pin* it to the finch address 192.168.5.2 via `extra_hosts` and
  therefore do break). Untested beyond startup — the host worker is the
  documented path, and the signal relay is not containerized either way.

### Verified behaviour

- **DAG 1:** ZIP → 3 CSVs loaded in parallel (5/5/10 rows) → `combined_report` (10 rows) → Parquet (3.7 KB at `/tmp/temporal-dag1/output/`). All four tables in `temporal_dag1`; `BAKEOFF_NS` isolation working.
- **DAG 2:** submit → **true signal suspend** (`TimerStarted` → `WorkflowExecutionSignaled` → `TimerCanceled`) → 8 items fanned out → combined, `failed: 0`. Relay logged the container's `POST /fetch-callback?workflow_id=…&run_id=…` → 200.
- **DAG 3 happy path:** `ACC-001`→`ACC-003`, transaction `completed`, notification sent, balances moved.
- **DAG 3 retriable:** gateway 500 and timeout errors retried with backoff and then succeeded — 3 occurrences across 28 runs, all invisible in history as designed.
- **DAG 3 non-retryable:** two random `PaymentDeclined`s, each **one attempt, no retries**; both recorded a `failed` transaction and sent a failure notification before re-raising (graceful degradation via `_handle_failure`).
- **DAG 3 validation:** suspended account → `Source account ACC-004 is suspended`; over-balance → `Insufficient balance: 4700.00 < 999999.0`. Both fail before any gateway call.
- **DAG 3 idempotency:** re-running `PAY-IDEM-001` hit `Duplicate payment: existing transaction with status completed`. One row, no double charge.
- **DAG 4 approval → ship:** three child workflows (`reserve-inventory`, `manager-approval`, `shipping`) in the parent's history, auto-approved at 10s, order `shipped` with tracking, inventory 100→60 with 40 reserved.
- **DAG 4 skip approval:** 2× `WIDGET-A` ($59.98 < $500) → approval bypassed → shipped in 0.7s.
- **DAG 4 rejection → compensation:** rejected at ~2s → reservation `released`, order `cancelled`, inventory restored to 100/0, workflow ends `FAILED` with `Order … cancelled: Order rejected by …`.
- **DAG 4 timeout → compensation:** with auto-decide off, `wait_condition` expired at 120s → decision `expired` → same clean compensation.
- **DAG 4 late decision does not resurrect:** deciding the expired request afterwards returned `callback_status: 502` (signal to a closed workflow), order stayed `cancelled`, inventory **not** re-decremented.
- **DAG 4 shipping failure → compensation:** `--bad-address` → non-retryable `InvalidAddress` (one attempt) → child workflow failed → parent compensated, order `cancelled`, reservation `released`.
- **DAG 4 worker crash mid-approval:** `kill -9` during the approval wait, approval decided 14s into a **69s** worker outage, workflow parked with `ApproximateBacklogCount: 1` and zero pollers, `WorkflowTaskTimedOut` → redelivered → resumed by a fresh process → `shipped`. Inventory decremented exactly once; shipping's own 504 retried through on top. See the durable-execution section above.

### Not yet exercised

`temporal workflow reset` / replay against changed code (workflow versioning);
the containerized worker beyond startup; concurrent contention for `RARE-D`'s
last 2 units; shipping *retry* exhaustion (only the non-retryable address path
was forced); killing the worker mid-*activity* rather than mid-wait (would test
activity heartbeat/timeout redelivery, a different mechanism); Temporal's own
schedules and search attributes.

---

## Blockers / gotchas for the wider bake-off

- **DAG 2's URL is now `fixture-service`, not GitHub** (fixed 2026-08-04). The old
  `api.github.com/users/octocat/repos` default shared a 60/hour per-IP limit with
  every other implementation. `start_workflow.py` now defaults to
  `http://fixture-service:8099/books?base=http://localhost:8099` — compose DNS name
  because callback-fetch-service (a container) fetches the collection, `?base=`
  because the host worker fetches the details. Still overridable with `--url` /
  `DAG2_URL`.
- **Two workers, one task queue.** Leaving the compose `temporal-worker` running
  alongside the host worker means activities land on whichever polls first, with
  different env — confusing and non-deterministic. Stop one.

---

## Fixes applied (2026-08-02)

1. **`start_workflow.py` added** — a client with one subcommand per DAG, working
   defaults, auto-generated idempotency ids, and flags for the failure branches
   (`--from-account`, `--amount`, `--payment-id`, `--total`, `--bad-address`).
   Without it there was no way to start any of the four workflows.

No changes were needed to the workflows or activities themselves.

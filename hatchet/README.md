# Hatchet

Hatchet Python SDK implementation of the four bake-off DAGs. See `../README.md`
for the DAG specs and `../RUNNING.md` for cross-cutting setup (container
networking, the resume-broker model, teardown).

- **Definition style:** imperative Python, `hatchet.workflow()` + `@wf.task` / `@wf.durable_task`
- **Wait mechanism:** durable event waits (`aio_wait_for_event` / `aio_wait_for`), fed by an HTTP→event relay
- **Engine:** `hatchet-lite` container (gRPC `:7077`, API/dashboard `:8888`); worker on the host
- **Schema namespace:** `BAKEOFF_NS=hatchet` → `hatchet_dag1` / `_dag3` / `_dag4`

---

## Launch

**Status: all four DAGs verified end-to-end on 2026-08-03** — hatchet-lite
(engine schema v1.0.135), `hatchet-sdk` 1.33.10, Python 3.14.2, Podman.

### 1. Backbone + engine

```bash
just up hatchet          # postgres :54321, mocks :8090-8092, engine :8888 + :7077
just seed hatchet        # create hatchet_dag{1,3,4} schemas + seed fixtures
podman stop shared-services_hatchet-worker_1   # compose worker would race the host one
```

### 2. Mint a client token

The token can only be created after the engine is up, so it can't live in
compose. The `default` tenant is seeded by hatchet-lite:

```bash
podman exec shared-services_postgres_1 \
  psql -U orchestration -d hatchet -c 'select id, name from "Tenant";'

podman exec shared-services_hatchet-engine_1 \
  /hatchet-admin token create --config /config \
  --tenant-id 707d0855-80ab-4e1f-a156-f1c4546cbf52 --name bakeoff \
  | tail -1 > shared-services/hatchet.token      # gitignored
```

### 3. Environment

```bash
source hatchet/env.sh     # from the repo root
```

**Source this rather than exporting by hand.** Four of its settings are
non-obvious and each one costs an hour if you miss it — see Findings:

| Var | Why |
|---|---|
| `HATCHET_CLIENT_SERVER_URL=http://localhost:8888` | belt-and-braces since compose now sets the engine's `SERVER_URL`; needed for tokens minted before that |
| `HATCHET_CLIENT_HOST_PORT=localhost:7077` | the token's `grpc_broadcast_address` is `hatchet-engine:7070`, compose-only |
| `HATCHET_CLIENT_NAMESPACE=bakeoff` | isolates action names from stale worker registrations |
| `HATCHET_CLIENT_TOKEN` from the **file**, overriding any inherited value | `.envrc` exports a stale JWT that otherwise wins |

### 4. Worker + event relay (host)

Two processes, two shells:

```bash
source hatchet/env.sh && cd hatchet

uv run --project .. python worker.py                                  # worker
uv run --project .. uvicorn event_relay:app --host 0.0.0.0 --port 8096  # relay
```

Stop the worker with **SIGTERM, never SIGKILL** — see Findings.

### 5. Run the DAGs

```bash
source hatchet/env.sh && cd hatchet

uv run --project .. python start_workflow.py dag1     # ~10s
uv run --project .. python start_workflow.py dag2     # ~20s (GitHub rate limit)
uv run --project .. python start_workflow.py dag3     # ~5s
uv run --project .. python start_workflow.py dag4     # ~25s (10s auto-approve)
```

Flags for the failure branches:

| Command | Reaches |
|---|---|
| `dag3 --from-account ACC-004` | validation failure (suspended account) |
| `dag3 --amount 999999` | validation failure (insufficient balance) |
| `dag3 --payment-id X` twice | duplicate-payment branch |
| `dag4 --total low` | under the $500 threshold, approval skipped |
| `dag4 --bad-address` | non-retryable `InvalidAddress` → saga compensation |
| `APPROVAL_TIMEOUT_SECONDS=5` on the **worker**, then `dag4 --total high` | approval expiry → compensation (5s beats the 10s auto-decider) |

Rejection needs the decision in before the auto-decider:

```bash
id=$(curl -s 'http://localhost:8091/approval-requests?status=pending' \
     | python3 -c "import sys,json;print(json.load(sys.stdin)['approval_requests'][0]['approval_request_id'])")
curl -sX POST "http://localhost:8091/approval-requests/$id/decide" \
  -H 'content-type: application/json' \
  -d '{"decision":"rejected","approver":"me","reason":"testing"}'
```

### Re-running

DAG 3 keys idempotency on `payment_id`, DAG 4 on `order_id`; both auto-generate.
DAG 4 **consumes inventory** — `--total high` reserves 40 of `WIDGET-A`'s 100 per
run, so the third consecutive run fails validation. Reset the four inventory
rows or `just down-clean && just up hatchet && just seed hatchet`.

DAG 3's gateway declines 5% of the time at random; an occasional failed run is
**correct behaviour**, not a regression.

---

## What this implementation demonstrates

| DAG | Hatchet idioms |
|---|---|
| 1 | `aio_run_no_wait()` child-workflow fan-out + `aio_result()` join, task-level `retries` / `backoff_factor` |
| 2 | `@wf.durable_task` + `aio_wait_for_event` with a CEL filter on the event payload |
| 3 | `NonRetryableException` for terminal errors, `@wf.on_failure_task` for failure recording |
| 4 | Child workflows as composition units, `aio_wait_for` + `OrGroup(UserEventCondition, SleepCondition)` for wait-or-timeout, `on_failure_task` as the saga compensator |

---

## Findings

Observations bearing on `../comparison.md` (Hatchet currently scores **70**).

### Durable event waits are real, and the ergonomics are good

DAG 2 and DAG 4 suspend on `aio_wait_for_event` / `aio_wait_for`. The task is not
running while suspended, it resumes mid-function, and DAG 4 keeps its child
workflow *and* its durable wait at the same time — the same property that makes
Temporal's DAG 4 clean, and that Prefect cannot do (its zero-cost suspend
requires severing the parent link). Verified on a 5s suspend in DAG 2 and a
120s approval wait in DAG 4.

Composing a wait with a timeout is genuinely nice: an `OrGroup` of a
`UserEventCondition` and a `SleepCondition`, and the result dict is keyed by
each condition's `readable_data_key`, so you can tell which branch fired:

```python
result = await context.aio_wait_for(
    f"approval-{order_id}",
    OrGroup([
        UserEventCondition(event_key="approval_decision",
                           expression=f"input.order_id == '{order_id}'",
                           readable_data_key="approval_decision"),
        SleepCondition(duration=timedelta(seconds=120),
                       readable_data_key="approval_timeout"),
    ]),
)
# -> {"CREATE": {"approval_decision": [ {...payload...} ]}}
```

### …but three defaults silently defeat them

Each of these produced *no error anywhere* — the workflow simply sat in
`RUNNING` forever, or was cancelled with a message that pointed nowhere useful.
Together they are the strongest argument that Hatchet's durable-execution story
is younger than Temporal's.

1. **No DURABLE worker slots unless the workflows are passed to `worker()`.**
   The SDK derives required slot types from the `workflows=` argument;
   `worker.register_workflow(...)` afterwards is too late, and a durable task
   with no durable slot is **never dispatched**. Nothing logs a warning. The
   worker looks healthy and regular tasks run fine.
2. **`durable_task` defaults `execution_timeout` to 1 minute** — and that ceiling
   applies to the *suspended* wait. A durable wait for human approval dies at 60s
   with `Task exceeded timeout of 1m`, which reads like a hung task rather than a
   config default. Every durable wait needs an explicit `execution_timeout`.
3. **A SIGKILLed worker stays `ACTIVE` in the engine** and keeps being assigned
   work it will never run. After five hard restarts this session there were four
   phantom `ACTIVE` workers registered for the same action names; durable tasks
   went to the phantoms and hung indefinitely while regular tasks still ran.
   Restarting the engine does **not** clear them (the state is in Postgres, not
   the connection). Setting `HATCHET_CLIENT_NAMESPACE` gives a restarted worker
   its own action names and sidesteps the problem entirely; stopping with SIGTERM
   avoids creating them.

A useful diagnostic: a minimal durable workflow under a *fresh* name works even
while the real ones hang — that's how the phantom-worker cause was isolated,
after the CEL expression and slot config had both been cleared as suspects.

### Hatchet cannot be an HTTP callback target

DAG 2 and DAG 4 need an external service to resume them. Hatchet's event
endpoint is `POST /api/v1/tenants/{tenant}/events`, which requires a bearer token
and a `{key, data}` envelope — the mock services send neither. So `event_relay.py`
(≈90 lines of FastAPI) receives their plain POST, folds the query-string
correlation fields into the payload, and pushes a proper event with the SDK's
credentials.

Temporal needs the same shim (`signal_server.py`) for the same reason. Step
Functions' `.waitForTaskToken` and Prefect's `POST /flow_runs/{id}/resume` do
not. Worth one line in the comparison: **event-sourced engines with SDK-only
ingress need a bridge component for third-party callbacks**; HTTP-native ones
don't.

The relay must run in the **same namespace** as the worker — the SDK applies the
namespace prefix to event keys on both the push and the wait side, so a
mismatched namespace means the event never matches and you're back to a silent
infinite wait.

### The token's embedded URLs are wrong for a host worker — one of them dangerously so

`hatchet-admin token create` embeds `server_url: http://localhost:8080` and
`grpc_broadcast_address: hatchet-engine:7070`, and the SDK trusts both. Neither
is right when the worker runs on the host: hatchet-lite's API is published on
**8888**, and the gRPC broadcast name only resolves inside compose.

The `:8080` default is actively hazardous in this repo, because **Airflow runs on
:8080**. Hatchet SDK calls silently talked to Airflow and returned:

```
{"error":"/api/v1 has been removed in Airflow 3, please use its upgraded version /api/v2 instead."}
```

**Fixed at the source** (2026-08-04): compose sets `SERVER_URL:
http://localhost:8888` on the engine, so newly minted tokens carry the right
claim — verified by decoding a fresh token (`server_url` and `aud` both `:8888`)
and by resolving it in the SDK with `HATCHET_CLIENT_SERVER_URL` unset.
`env.sh` keeps the override anyway, for tokens minted before the change. See the
port map in `../RUNNING.md` §0. Anyone running Hatchet and Airflow on one host
without both of these will hit it.

### `.envrc`'s Hatchet token shadows a freshly minted one

`.envrc` exports a long-lived `HATCHET_CLIENT_TOKEN` from an older engine. Any
`${VAR:-default}` pattern therefore keeps the stale one, and the failure is a
bare `UNAUTHENTICATED: invalid auth token` from gRPC with no hint that it's an
identity problem. `env.sh` deliberately lets the **file win** over the
environment. (`CLAUDE.md` already flags the JWT as local-only; this is the
concrete way it bites.)

### Saga compensation is an `on_failure_task`, which is tidier than a hand-rolled stack

`@order_fulfillment_wf.on_failure_task` fires when any task fails after retries,
and `context.task_run_errors` gives it the failed task names and messages — so
the cancellation reason written to the DB names the culprit
(`check_and_approve: NonRetryableException: Order ... not approved`). That is
better provenance than Prefect's or Temporal's hand-maintained compensation
lists produce by default.

The trade-off: it is a *failure* hook, not a compensation *stack*. It cannot
unwind step-by-step in reverse order — it's one function that has to work out
what to undo by querying state. Fine for three compensations; it would not scale
to a long saga the way an explicit reversed stack does. And as with the others, a
compensated run ends `FAILED`, indistinguishable at a glance from a crash.

### Non-retryable classification is declarative

`NonRetryableException` is a first-class SDK exception — no policy list, no
predicate hook. Verified from the engine's own attempt counters: declines and
validation failures ended at `attempt=1` while `retries=5` was configured, and
retriable gateway errors retried and then succeeded (30 retriable failures across
26 runs, all recovered). One decline landed at `attempt=2` — correct: attempt 1
hit a *retriable* gateway error and the re-roll came up declined.

### Gaps in this implementation (not in Hatchet)

- **The spec's concurrency caps are absent.** DAG 1 (10) and DAG 2 (20) fan out
  by spawning child workflows in an unbounded loop. `worker(slots=40)` is a
  worker-wide cap, not the per-DAG limit the spec asks for. Hatchet has
  `concurrency=` on tasks/workflows; it is not wired up. Same gap as Prefect and
  Temporal.
- **DB credentials travel in the workflow input.** Every DAG passes `db_config`
  (including the password) as part of the child-workflow payload, so it is
  persisted in the engine's event history. Harmless at evaluation scale, wrong in
  principle — the child could read the same env the parent does.
- **Blocking I/O in async tasks.** All DB work is synchronous `psycopg2` inside
  `async def`, which is what triggers Hatchet's own
  `THE TIME TO START THE TASK RUN IS TOO LONG, THE EVENT LOOP MAY BE BLOCKED`
  warning under load. Correct fix is `asyncio.to_thread` or an async driver.

### Verified behaviour

- **DAG 1:** ZIP → 3 CSVs loaded by parallel child workflows (5/5/10 rows) → `combined_report` (10 rows) → Parquet (7.6 KB). All four tables in `hatchet_dag1`.
- **DAG 2:** submit → **durable suspend** (`wait_for_callback` started 23:36:32, finished 23:36:38 on the relayed event) → 8 items fanned out to child workflows → combined, `failed: 0`.
- **DAG 3 happy path:** `ACC-001`→`ACC-003`, transaction `completed`, notification sent.
- **DAG 3 retriable:** 30 gateway 500/timeout failures across 26 runs, all retried through to success.
- **DAG 3 non-retryable:** 3 random declines, each terminal at `attempt=1`; `on_failure_task` recorded a `failed` transaction with the error message and sent a failure notification.
- **DAG 3 validation:** suspended account and `Insufficient balance: 4900.00 < 999999`, both terminal before any gateway call.
- **DAG 3 idempotency:** re-running `PAY-HIDEM-1` hit the duplicate branch; one row, no double charge.
- **DAG 4 approval → ship:** three child workflows, auto-approved at 10s, `shipped` with tracking, inventory 100→60 with 40 reserved.
- **DAG 4 skip approval:** 2× `WIDGET-A` ($59.98 < $500) → `auto_approved`, no approval request raised.
- **DAG 4 rejection → compensation:** reservation `released`, order `cancelled` with `check_and_approve: NonRetryableException: ...` as the recorded reason, inventory restored to 100/0.
- **DAG 4 timeout → compensation:** `APPROVAL_TIMEOUT_SECONDS=5` beat the 10s auto-decider → `decision=expired` → same clean compensation; the late approval did **not** resurrect the order.
- **DAG 4 shipping failure → compensation:** `--bad-address` → non-retryable `InvalidAddress: Missing address fields: street, city, state, zip` → compensated.

### Not yet exercised

Worker crash/restart mid-workflow (the durability claim itself — and given the
phantom-worker behaviour above, the interesting question is whether a *killed*
worker's durable wait is picked up by its replacement); the containerized worker
beyond startup; `concurrency=` caps; Hatchet's rate limits, cron triggers, and
dashboard observability; the `on_events=` trigger path (everything here was
invoked directly by the client).

---

## Blockers / gotchas for the wider bake-off

- **Hatchet and Airflow both want :8080.** Airflow holds it on this host; the
  Hatchet token points at it. Always set `HATCHET_CLIENT_SERVER_URL`.
- **DAG 2's GitHub URL** counts against the shared 60/hour unauthenticated limit.
  Override with `--url` or `DAG2_URL`.
- **Two workers, one action set.** The compose `hatchet-worker` container and a
  host worker will both claim tasks, with different env. Stop one.

---

## Fixes applied (2026-08-03)

All four DAGs were non-functional as committed:

1. **`BAKEOFF_NS` schema isolation wired in** (`dag1`/`dag3`/`dag4`) — previously wrote unqualified names into `public`, where nothing exists on a fresh volume. DAG 1 self-creates; DAG 3/4 fail fast with the `just seed hatchet` hint.
2. **Worker slot config fixed** — workflows moved into `hatchet.worker(workflows=[...])` with an explicit `durable_slots`, without which DAG 2 and DAG 4's durable tasks were never dispatched.
3. **`execution_timeout` set on both durable waits** — the 1-minute default cancelled them mid-wait.
4. **CEL expressions corrected** — `{{ .correlation_id }} == '...'` (template syntax) never matches; the payload is addressed as `input.correlation_id`.
5. **DAG 4's dead timeout branch replaced** — `aio_wait_for_event` accepts no timeout, so `except TimeoutError` could never fire and a missing decision hung forever. Now `aio_wait_for` + `OrGroup(UserEventCondition, SleepCondition)` with the branch detected from the result key, and the timeout exposed as `APPROVAL_TIMEOUT_SECONDS`.
6. **`event_relay.py` added** — Hatchet's event API can't be a raw callback target (auth + envelope), so the mock services' callbacks were going nowhere.
7. **DAG 4 FK ordering** — reservations were inserted before the `orders` row they reference, so every new `order_id` failed the non-deferrable FK. The upsert now runs first. (Same bug Prefect and Argo had.)
8. **`env.sh` added** — server URL, gRPC host, namespace, and file-wins token precedence, all of which fail silently or misleadingly when wrong.
9. **`start_workflow.py` added** — nothing existed to invoke the workflows; `worker.py` only registered them.
10. **`shared-services/hatchet.token` gitignored** — `RUNNING.md` §5 tells you to write a long-lived JWT to a tracked path.

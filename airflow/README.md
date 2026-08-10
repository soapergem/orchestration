# Apache Airflow

Airflow 3.x implementation of the four bake-off DAGs. See `../README.md` for the
DAG specs and `../RUNNING.md` for cross-cutting setup (container networking, the
resume-broker model, teardown).

- **Definition style:** DAG 1/3 classic operators (`PythonOperator`, `>>` and
  `set_upstream`); DAG 2/4 TaskFlow (`@dag` / `@task` / `@task.branch`)
- **Wait mechanism:** deferrable operators + custom triggers polling the mock
  services — Airflow has no inbound resume, so it always polls
- **Engine:** `airflow standalone` on the host; no engine container
- **Schema namespace:** `BAKEOFF_NS=airflow` → `airflow_dag1` / `_dag3` / `_dag4`

---

## Launch

**Status: all four DAGs verified end-to-end on 2026-08-02** (Airflow 3.2.1,
Python 3.14, Podman) — both via `airflow dags test` and under the real
scheduler + triggerer in `airflow standalone`.

### 1. Backbone

```bash
just up                  # postgres :54321 + mocks :8090-8092
just seed airflow        # create airflow_dag{1,3,4} schemas + seed fixtures
```

`just seed` is required on an existing `pgdata` volume — `init-db.sql` only runs
on a fresh one. It is idempotent.

### 2. Environment

`source env.sh` does all of this; the block below is what it sets. Airflow
subprocesses inherit the launching shell's environment, so it must be sourced
before starting anything. `.envrc` (direnv) already covers `POSTGRES_HOST`/
`POSTGRES_PORT`.

```bash
cd airflow
export AIRFLOW_HOME=$PWD/.airflow-home          # keeps state out of ~/airflow
export AIRFLOW__CORE__DAGS_FOLDER=$PWD
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export PYTHONPATH=$PWD                          # REQUIRED — see Findings
export POSTGRES_HOST=localhost POSTGRES_PORT=54321
export BAKEOFF_NS=airflow
export CALLBACK_FETCH_SERVICE_URL=http://localhost:8090
export APPROVAL_SERVICE_URL=http://localhost:8091
export SHIPPING_SERVICE_URL=http://localhost:8092
export ETL_ZIP_PATH=$PWD/../test-data/sample-data.zip
export ETL_EXTRACT_DIR=$AIRFLOW_HOME/data/extracted
export ETL_OUTPUT_DIR=$AIRFLOW_HOME/data/output
mkdir -p "$ETL_EXTRACT_DIR" "$ETL_OUTPUT_DIR"
```

The `ETL_*` defaults are container paths (`/opt/airflow/data/...`), so DAG 1
fails with a missing-ZIP error on the host unless they are overridden.

### 3. Run

```bash
uv run --project .. airflow db migrate     # first time only
uv run --project .. airflow standalone     # UI on :8080, creds in $AIRFLOW_HOME/simple_auth_manager_passwords.json
```

Or run a DAG headlessly, which is how the four were verified:

```bash
uv run --project .. airflow dags test dag1_csv_etl              # ~13s
uv run --project .. airflow dags test dag2_api_fanout           # ~90s (GitHub rate-limit warning applies)
uv run --project .. airflow dags test dag3_payment              # ~20s (flaky gateway retries)
uv run --project .. airflow dags test dag4_order_fulfillment    # ~25s (10s auto-approve delay)
```

`dags test` runs the triggerer inline, so deferrable operators work without a
separate process. Under `standalone` the triggerer is its own component and must
be up **and able to import `triggers/`** or DAG 2/4 will sit in `deferred`
forever — see the first finding.

Stopping it: `standalone`'s children (`api_server`, `worker`, `serve-logs`)
rewrite their process titles and outlive a `pkill -f "airflow standalone"`. A
leftover `api_server` holds :8080 and the next `standalone` then dies during DB
migration with almost nothing in its log. Kill by PID from
`ps -eo pid,args | grep "airflow "` — and note that `pkill -f airflow` also
matches the shell you typed it in.

### Knobs

| What | How |
|---|---|
| DAG 4 approval outcome | `AUTO_DECIDE_ACTION` on the approval-service container (`approved` default, 10s delay) |
| DAG 4 manual decision | `POST localhost:8091/approval-requests/<id>/decide` within 10s of the request appearing |
| DAG 4 rejection/timeout saga | reject the approval, or set `approval_timeout` below the auto-decide delay in `ManagerApprovalOperator` |
| DAG 3 failure branch | `-c '{"from_account":"ACC-004"}'` (suspended) or `'{"amount":99999}'` (insufficient) |
| DAG 4 validation failure | `-c '{"customer_id":"CUST-99"}'` (inactive) |
| Shipping flakiness | `SHIPPING_SUCCESS_RATE` on the shipping-service container (0.70) |

### Re-running

`order_id` (DAG 4) and `payment_id`/`idempotency_key` (DAG 3) are idempotency
keys. DAG 4's defaults to a value derived from the run id, so re-triggering is
always a clean order. DAG 3's is the literal `PAY-001`, so the second run takes
the duplicate-payment branch by design — pass `-c '{"payment_id":"PAY-002",
"idempotency_key":"PAY-002"}'` for a fresh one.

---

## What this implementation demonstrates

- Classic operator instantiation with explicit `dag=`, `>>` chaining, and
  `set_upstream`/`set_downstream` (DAG 1, DAG 3)
- TaskFlow API: `@dag`, `@task`, `@task.branch`, `@task_group` (DAG 2, DAG 4)
- Dynamic task mapping: `PythonOperator.partial(...).expand()` over an upstream
  XCom list (DAG 1) and `@task(...).expand()` (DAG 2)
- Custom deferrable operators with custom `BaseTrigger` subclasses in
  `triggers/` (DAG 2, DAG 4)
- Trigger rules for best-effort and compensation semantics
  (`ALL_DONE`, `ONE_FAILED`, `NONE_FAILED_MIN_ONE_SUCCESS`)
- Typed exceptions plus `on_failure_callback` for retry classification and saga
  compensation

## Findings

### The triggerer cannot import anything from the DAGs folder

The single worst failure mode found here, and it is invisible in
`airflow dags test`. Only the dag-processor puts `AIRFLOW__CORE__DAGS_FOLDER`
on `sys.path`; the Airflow 3 triggerer no longer parses DAGs, so it never does.
The trigger row is created and assigned, the triggerer picks it up, fails to
import the class, and reports `0 triggers currently running`:

```
Trigger failed to load code  classpath=triggers.approval_trigger.ApprovalTrigger
  error='ModuleNotFoundError("No module named '"'"'triggers'"'"'")'
```

The task then sits in `deferred` **forever** — no retry, no timeout, no failure.
The DAG's own `approval_timeout` never applies because it lives inside the
trigger that was never instantiated. Nothing surfaces in the task log or the UI;
the only evidence is one error line in the triggerer's own log. Verified:
approval auto-approved at 20:43:14, task still `deferred` at 20:56.

`export PYTHONPATH=$AIRFLOW__CORE__DAGS_FOLDER` fixes it (the alternative is
installing the triggers as a real package, or moving them to the plugins
folder). **Scoring note:** custom deferrable operators are Airflow's answer to
suspend/resume, and the packaging story around them is a genuine operational
sharp edge — a silent infinite hang, in the component that is supposed to make
long waits cheap.

### Deferral frees the worker, but the run is never zero-cost

The wait in DAG 2 and DAG 4 is a real deferral: the task releases its worker
slot and a `BaseTrigger` coroutine polls the mock service inside the triggerer.
That is materially better than a sensor blocking a slot, and it is Airflow's
best answer to "workflow suspends". It is not Temporal/Prefect-style suspension
though — the trigger keeps an async polling loop alive for the whole wait, and
the deferral is bounded by the operator's own `approval_timeout`, not by
durable state.

### Nothing outside Airflow can resume an Airflow task

The resume-broker services (`RUNNING.md` §2b) want a `provider` + `resume_data`
at registration and will 422 without one. A deferred task exposes no inbound
resume handle, so both DAG 2 and DAG 4 register the documented dead URL
(`http://localhost:0/noop`) and rely on their trigger polling instead. The
service records the decision before dispatching the resume, so the failing
resume leg is cosmetic — `/decide` returns 500 while the decision lands
correctly. **Scoring note:** this is a genuine capability gap versus Temporal
(signal), Prefect (REST resume), and Step Functions (task token).

### A compensated saga still reports the DAG run as successful

DAG 4's rejection path works: the operator releases inventory, the
`ONE_FAILED` compensation task marks the order `cancelled`, and the
notification reports it. But because compensation and notification are the leaf
tasks and both succeed, the DagRun finishes in state `success` with only
`manager_approval` red. Anything alerting on run state alone would miss a
rejected, fully-compensated order. Verified: `ORD-REJECT-1` → order `cancelled`,
both reservations `released`, run `success`.

### Saga compensation is entirely hand-rolled

Three mechanisms cooperate and none of them is a saga primitive: the operator
compensates inline on rejection/timeout, `on_failure_callback` compensates when
shipping fails, and a `ONE_FAILED` task acts as the safety net. Correctness
depends on `_release_inventory()` being idempotent, which it is (it filters on
`status = 'reserved'`). The three-sub-workflow structure the spec describes maps
onto one `@task_group` plus loose tasks; Airflow has no sub-workflow with its
own compensation scope since SubDagOperator was removed.

### `.expand()` only maps over a task's return value

`processed["items"]` raises `cannot map over XCom with custom key 'items'` at
parse time, even though the upstream task returns a dict. The fix is a
pass-through task that returns the list (`extract_items`), which also becomes
the branch target. Worth contrasting with Prefect's `.map()`, which takes any
expression.

### Airflow 3 moved things that older DAG code depends on

Three broke here, all cheap to fix but all runtime-fatal or noisy:

- `airflow.operators.python` → `airflow.providers.standard.operators.python`
  (deprecation warning today, removal later)
- `airflow.utils.trigger_rule` → `airflow.task.trigger_rule`
- `context["task_instance"].log` no longer exists — `RuntimeTaskInstance` is a
  pydantic model with no `.log`. Task code must use the `logging` module.

### `dag_run.conf or params` is a trap

Every task read its config that way, so `--conf '{"order_id":"X"}'` shadowed
*all* other params and the next read raised `KeyError`. Params are only usable
as defaults if conf is merged over them per key (`_conf()` in DAG 3/4).

### Bare `@task` functions cannot appear in a `>>` chain

`branch >> call_shipping_api` where `call_shipping_api` is the decorated
function (not `call_shipping_api()`) fails at import with `'_TaskDecorator'
object has no attribute 'update_relative'`. Airflow catches it at parse time —
the DAG shows as an import error rather than failing at runtime.

### `ALL_DONE` tasks receive `None`, not a skip

`send_notification` is `ALL_DONE` so it runs even when upstream failed, but
Airflow then resolves its XCom argument to `None` rather than skipping it —
`order_result.get(...)` raised `AttributeError`. Best-effort tasks need
Optional-typed inputs and a fallback (here: read the order status from the DB).

### Verified behaviour

| Scenario | Result |
|---|---|
| DAG 1 dynamic fan-out | 3 CSVs → mapped loads (customers 5, products 5, orders 10) → SQL join → 10-row Parquet |
| DAG 1 schema isolation | tables land in `airflow_dag1`, self-created |
| DAG 2 deferral | submit → defer → triggerer polls `/status` → resume |
| DAG 2 fan-out | 30 items, 30 successful detail fetches, combined |
| DAG 3 retry classification | `PaymentGatewayTimeout` retried with backoff, then success; `gw-txn-PAY-001-79958` recorded |
| DAG 3 balances | ACC-001 5000 → 4900, ACC-002 3000 → 3100 |
| DAG 4 happy path | validate → reserve → branch → deferred approval → auto-approved → ship → `shipped` |
| DAG 4 approval branch | $529.98 ≥ $500 threshold routes through `manager_approval` |
| DAG 4 saga (rejection) | inventory released, order `cancelled`, notification sent, stock restored |
| DAG 4 FK ordering | order row inserted before reservations (see *Fixes*) |
| Real scheduler | all four triggered via `airflow dags trigger` under `standalone` → `success`, including the deferral hand-off to the standalone triggerer |

### Not yet exercised

- **Concurrency caps.** `max_active_tis_per_dag` is set to the spec's 10 (DAG 1)
  and 20 (DAG 2), but the fixtures only ever produce 3 and 30 items with no
  observation of the cap actually throttling.
- **Approval timeout expiry.** The rejection path is verified; the 180s
  `ApprovalExpired` path is not.
- **Shipping failure saga.** `SHIPPING_SUCCESS_RATE=0.70` never tripped across
  these runs, so `_on_shipping_failure` compensation is untested — as are the
  `InvalidAddress` (non-retriable) versus `ShippingTimeout` (retriable) splits.
- **Dependency isolation.** All tasks share the one venv. `@task.virtualenv`,
  `@task.docker`, and `KubernetesPodOperator` are the escape hatches
  `comparison.md` cites; none is demonstrated here.
- **Last-unit contention** (`RARE-D`) and inactive-customer validation.
- Post-shipping reservation state: reservations stay `reserved` after a
  successful ship rather than moving to a terminal `fulfilled`, so
  `inventory.reserved_quantity` never drains on the happy path. Matches what the
  schema allows; may be worth aligning across tools.

## Fixes applied (2026-08-02)

Every one of these was found by running the DAGs; two were parse-time, the rest
runtime.

1. **DAG 2 import error** — `.expand(item=processed["items"])` rejected
   (`cannot map over XCom with custom key`); added the `extract_items`
   pass-through task and repointed the branch at it.
2. **DAG 4 import error** — `branch >> call_shipping_api` used the bare
   decorator; instantiate the task first and wire the resulting XComArg.
3. **Schema isolation** — all three DB DAGs now set `search_path` to
   `airflow_dag{1,3,4}` (`BAKEOFF_NS`). Previously they wrote flat tables into
   `public`, where DAG 1's CSV-derived `customers` (no `status` column) collides
   with DAG 4's seeded `customers`, so DAG 4 could not have run at all. DAG 1
   self-creates its schema; DAG 3/4 fail fast with the `just seed` hint.
4. **DAG 4 sample inputs** — params referenced `CUST-001`/`SKU-A`/`SKU-B`, none
   of which exist in `bootstrap_bakeoff()`; now `CUST-42` + `GADGET-B` +
   `WIDGET-A` ($529.98, over the approval threshold).
5. **DAG 4 FK ordering** — reservations were inserted before the `orders` row
   they reference, so every run died on
   `inventory_reservations_order_id_fkey`.
6. **DAG 4 resume registration** — the approval POST omitted
   `provider`/`resume_data` and got a 422; now registers `http_callback` with
   the documented dead URL. Same fix in DAG 2's fetch registration.
7. **`context["task_instance"].log`** — removed (Airflow 3 has no such
   attribute); DAG 3 and DAG 4 use module loggers.
8. **conf/params merge** — `_conf()` overlays `dag_run.conf` on `params` per
   key, so partial `--conf` no longer wipes the defaults.
9. **`send_notification` on failure** — accepts `None` and falls back to the
   order's DB status.
10. **Generated `order_id`** — DAG 4's default is derived from the run id, so
    re-triggering does not replay the same order through the idempotent no-op
    path (the lesson `CLAUDE.md` records from Prefect deployments).
11. **Validation now fails the run** — `validate_order` raised nothing on an
    unknown/inactive customer or short stock, and nothing branched on its
    `is_valid` flag, so an invalid order would reserve and ship anyway. It now
    raises `OrderValidationFailed`.
12. **Spec concurrency caps** — `max_active_tis_per_dag` 10 (DAG 1) / 20
    (DAG 2).
13. **DAG 2 `source_url`** — `/status` does not echo the requested URL, so the
    combined result reported `None`; the operator stamps it from its template
    field.
14. **`PYTHONPATH` for the triggerer** — added to `env.sh`; without it both
    deferrable tasks hang in `deferred` forever under the real scheduler. This
    is environment, not code: the DAG files themselves are unchanged.

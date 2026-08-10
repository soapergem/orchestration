# Dagster

Dagster 1.13.5, Python 3.14, all four DAGs verified end-to-end on 2026-08-02
against the local backbone (Postgres 54321 + the three mock services).

The directory is `dagster_bakeoff/`, **not** `dagster/`. A local package named
`dagster` shadows the installed library: `dagster dev -f repository.py` dies on
`attempted relative import with no known parent package`, and
`dagster dev -m dagster.repository` resolves to the *installed* package and
reports `No module named 'dagster.repository'`. Neither entry point can load a
code location under that name.

## Launch

### 1. Backbone

```bash
just up                 # postgres + callback-fetch + approval + shipping
just seed dagster       # creates dagster_dag1 / _dag3 / _dag4 schemas + fixtures
```

### 2. Environment

```bash
source dagster_bakeoff/env.sh
```

Dagster runs on the **host**, so every compose-DNS default in `resources.py`
has to be overridden. `env.sh` exports `BAKEOFF_NS`, `POSTGRES_HOST/PORT`, the
three `*_SERVICE_URL`s, the two shared spool directories, and — importantly —
`DAGSTER_HOME`. Without `DAGSTER_HOME` every command builds a throwaway
instance, so sensor cursors reset each tick and **no run can ever bridge to
another** — DAG 2 and DAG 4 simply never advance.

### 3. Run

DAG 1 and DAG 3 are single runs and need no daemon:

```bash
uv run dagster job execute -m dagster_bakeoff.repository \
  -j csv_etl_job -c dagster_bakeoff/run_configs/dag1_csv_etl.yaml
uv run dagster job execute -m dagster_bakeoff.repository \
  -j payment_processing_job -c dagster_bakeoff/run_configs/dag3_payment.yaml
```

DAG 2 and DAG 4 are sensor-bridged, so they need the daemon *and* the webserver:

```bash
uv run dagster dev -m dagster_bakeoff.repository        # UI on :3000
# then, in another shell (source env.sh again):
uv run dagster job launch -m dagster_bakeoff.repository \
  -j submit_fetch_job -c dagster_bakeoff/run_configs/dag2_submit_fetch.yaml
uv run dagster job launch -m dagster_bakeoff.repository \
  -j order_pre_approval_job -c dagster_bakeoff/run_configs/dag4_high_value.yaml
```

`job execute` runs in-process and bypasses the instance's run launcher, so a run
started that way never triggers `shipping_failure_sensor`. Use `job launch` for
anything whose failure must be compensated.

All three sensors carry `default_status=DefaultSensorStatus.RUNNING`, so they
start themselves — no toggling in the UI, which also makes a fresh clone
reproducible.

### Run configs

`run_configs/` holds one YAML per scenario: `dag1_csv_etl`,
`dag2_submit_fetch`, `dag3_payment`, `dag4_high_value` (999.98, needs
approval), `dag4_low_value` (59.98, ships inline). DAG 3's
`idempotency_key` must be changed per run or validation correctly rejects the
re-run as a duplicate.

## What this implementation demonstrates

- **Ops + graphs + `to_job()`**, not software-defined assets — these are
  task pipelines, and Dagster's asset layer would be the wrong idiom.
- **`DynamicOut`/`DynamicOutput` + `.map()`/`.collect()`** for runtime fan-out
  (DAG 1 per-CSV load, DAG 2 per-item detail fetch).
- **Conditional branching by optional outputs**: an op declares two
  `Out(is_required=False)` and yields exactly one; the unyielded branch's
  downstream ops report `Skipping step ... due to skipped dependencies` and the
  run still succeeds.
- **`RetryPolicy`** per op, with `Backoff.EXPONENTIAL` on the two flaky calls.
- **`Failure(allow_retries=False)`** for non-retriable business outcomes.
- **`ConfigurableResource`** for Postgres and HTTP, bound per job.
- **Sensors** as the suspend/resume substitute, and **`@run_failure_sensor`**
  as the saga trigger.

## Findings

### Dagster cannot suspend a run; sensors bridge runs instead

This is the headline divergence, and it costs more than it first looks.
Temporal signals, Prefect's `suspend_flow_run()`, and Airflow's deferrable
operators all park *one* execution and resume it. Dagster has no equivalent: an
op that waits must block a worker slot, so DAG 2 and DAG 4 are each split into
two jobs with a sensor in between.

The consequences that showed up in practice:

- **The unit of lineage is lost.** DAG 4 is three runs (pre-approval,
  post-approval, compensation) with no parent-child link in the UI. You cannot
  look at one page and see the fulfilment of one order; you correlate by
  `order_id` in the logs. Temporal's child workflows and Airflow's single DAG
  run both keep that.
- **State has to go somewhere the sensor can read it.** Here that's a JSON
  spool directory (`DAG2_CORRELATION_DIR`, `DAG4_APPROVAL_DIR`) holding the
  correlation id and the order payload. That is filesystem state outside
  Dagster's storage, and it is what would break first on multi-node or
  containerised execution — the same shared-storage problem Prefect hit with
  suspend/resume, but self-inflicted rather than framework-provided.
- **Resumption latency is a poll interval,** not an event. `minimum_interval_seconds=10`
  plus daemon scheduling put the observed approval→ship gap at 10–20s.
- **The orchestrator owns the deadline.** The approval service never expires a
  request, and there is no "wait until" primitive to time out, so
  `approval_sensor` compares `submitted_at + timeout_seconds` itself. The
  timeout only evaluates on a tick where the service *responds*: if the approval
  service is hard-down, requests stay pending indefinitely rather than
  compensating. Defensible, but it is a policy decision the code had to make
  that a suspend-capable tool gets for free.

### Saga compensation must be a run-status sensor, not a failure hook

Because the approval wait splits the workflow across runs, no single run's
`@failure_hook` can see the whole saga — the original code's docstring claimed a
failure hook triggered compensation, but no such hook existed and shipping
failures rolled back nothing. The working shape is
`@run_failure_sensor(monitored_jobs=[...], request_job=compensation_job)`,
recovering the `order_id` from the failed run's `run_config`.

That works, and it is genuinely decoupled, but note what it implies: the
compensation trigger is *outside* the workflow definition, keyed on a config
path (`ops.approved_order.config.order_id`). Rename that op and compensation
silently stops firing, with no error — the sensor just logs that it found no
order id. Temporal's `try/except` around a child workflow can't drift that way.

Compensation also has to tolerate a saga that failed before its first write.
`update_order_cancelled` originally raised `Order not found`, which turned a
partial rollback into a *failed compensation run* — the worst outcome. It now
returns `no_order_to_cancel`.

### Retry classification needs `allow_retries=False`, not just `Failure`

`raise Failure(...)` still honours the op's `RetryPolicy`. The declined-payment
and invalid-address paths were both written as bare `Failure` and would have
burned the full backoff ladder on a permanent business error. `allow_retries=False`
is the actual non-retriable switch. Verified: declined payments fail on attempt
1; gateway 5xx/timeouts retried 5s → 15s → 35s before failing the run.

### `ConfigurableResource` makes the schema binding clean

Because each job passes its own `resource_defs`, `bakeoff_postgres("dag3")` and
`bakeoff_postgres("dag1", create_schema=True)` bind different `search_path`s to
the same op code, and no op mentions a schema. This is nicer than the
module-level `DB_CONFIG` dict the Prefect and Airflow implementations use. The
DAG 1 vs DAG 3/4 split from `init-db.sql` (self-creating vs fail-fast) is one
boolean on the resource.

Field names are Pydantic fields, so `schema` is not usable — it collides with
Pydantic's own API. Hence `db_schema`.

### Op-level parallelism is process-per-step by default

`multiprocess_executor` launches a subprocess per step, so DAG 1's three CSV
loads and DAG 2's three detail fetches genuinely run in parallel, and each step
re-initialises its resources (visible as repeated `RESOURCE_INIT_STARTED`). That
is real isolation without configuring anything, but it also means every step
pays connection setup, and the default filesystem IO manager pickles every
input/output through `$DAGSTER_HOME/storage`.

### Verified behaviour

| Scenario | Result |
|---|---|
| DAG 1 | 3 CSVs fan-out loaded, `combined_report` 10 rows, Parquet written, all inside `dagster_dag1` |
| DAG 2 | submit → sensor bridge → 3-way fan-out → combine, 3/3 successes |
| DAG 3 happy | validate → gateway → DB debit/credit → notification |
| DAG 3 retriable | timeout retried once then succeeded; 5xx exhausted 5s/15s/35s then failed the run |
| DAG 3 non-retriable | decline failed on attempt 1, `@failure_hook` alert fired |
| DAG 3 invalid | suspended account → failure branch, failed txn row, failure notification, run still succeeds |
| DAG 4 low-value | 59.98 < 500 → ships inline in job 1, tracking number persisted |
| DAG 4 approved | 999.98 → approval → sensor → ship (one shipping retry) → `status=shipped` + shipment id |
| DAG 4 rejected | manual reject → compensation: 1 reservation released, order cancelled "Budget freeze" |
| DAG 4 timeout | 30s deadline with auto-decide off → compensation, "Approval timed out" |
| DAG 4 shipping failure | missing `zip` → non-retriable InvalidAddress → `run_failure_sensor` → compensation |

Final DB state was consistent: inventory returned to seed levels, three
cancellations with distinct `failure_reason`s.

### Not yet exercised

- Software-defined **assets**, asset checks, partitions, backfills — the
  features Dagster is actually differentiated on. This bake-off's four DAGs are
  task-shaped, so they exercise the part of Dagster that is *least* distinctive.
  Any scoring should say so explicitly.
- Non-default executors (`k8s_job_executor`, `docker_executor`) and therefore
  the dependency-isolation claim; the Prefect implementation does substantiate
  its equivalent.
- Declarative automation / auto-materialise policies.
- Concurrency caps (the spec's per-DAG limits) — not configured here; Dagster
  does them with run/op concurrency keys in `dagster.yaml`.
- Duplicate and late callbacks on DAG 2.

## Fixes applied (2026-08-02)

Nine defects, all pre-existing; the DAG files had never been run.

1. **Package name collision** — `dagster/` shadowed the installed library so no
   code location could load, by either `-f` or `-m`. Renamed to
   `dagster_bakeoff/`, relative imports made absolute.
2. **Connection config hard-coded to compose DNS** — `repository.py` pinned
   `host="postgres"` and `http://callback-fetch-service:8090`, unreachable from
   the host process Dagster documents you to run. Now env-driven with those as
   defaults.
3. **No `BAKEOFF_NS` isolation** — every table was unqualified against `public`,
   colliding with other runners and between DAG 1 and DAG 4. Added `db_schema` /
   `create_schema` on `PostgresResource` and `bakeoff_postgres()`.
4. **DAG 2 registration rejected with 422** — the fetch service refuses a
   request it cannot resume from, and the payload sent `callback_url: ""` with
   no provider. Now registers `http_callback` + the `http://localhost:0/noop`
   placeholder the other polling runners use.
5. **DAG 4 approval registration had the identical 422**, and it only surfaced
   after fix 4 because DAG 4 could not reach that step before.
6. **DAG 4 job 2 was three unconnected ops** — `call_shipping_api()`,
   `update_order_status()` and `send_order_notification()` took config, not
   inputs, so they ran *concurrently*: the order could be marked shipped before
   shipping was attempted, and the sensor passed empty strings, so
   `shipment_id`/`tracking_number` were never recorded. Rewired as a chain
   through an `approved_order` source op.
7. **DAG 4 low-value orders never shipped** — the `no_approval_needed` branch
   dead-ended with a comment admitting it. Now ships inline in job 1.
8. **FK ordering in `reserve_inventory`** — reservations were inserted before
   the `orders` row they reference, so every low-value run failed
   `inventory_reservations_order_id_fkey` four times over. Order insert moved
   first. (Same class of defect as Prefect's.)
9. **Order totals booked as 0.00** — the `orders` insert summed
   `item["unit_price"]` from config items that carry no price, instead of the
   total `validate_order` priced from the DB. Every order would have been
   below the approval threshold in the DB even when it wasn't in the branch
   logic.

Plus: `Failure(allow_retries=False)` on both non-retriable paths, compensation
made tolerant of a missing order row, approval timeout enforced in the sensor,
and the claimed-but-absent compensation trigger implemented as
`shipping_failure_sensor`.

## Gotchas

- **`just seed dagster` before DAG 3 or DAG 4.** They need seeded fixtures; the
  resource fails fast with the exact command to run rather than emitting a
  confusing `relation does not exist`.
- **`/decide` answers 500 while recording the decision.** Expected — Dagster
  registers the dead placeholder resume URL because it polls. The decision
  itself is stored correctly (verified: `status: rejected`).
- **Racing the auto-decider.** Compose sets `AUTO_DECIDE_ACTION=approved` with a
  10s delay, so testing rejection means POSTing `/decide` within that window,
  and testing timeout means bringing the service up with
  `AUTO_DECIDE_ACTION=none` (a one-key compose override file works).
- **DAG 2's GitHub URL rate-limits** — see the repo README; `per_page=3` keeps
  one run to ~4 calls.
- **`.dagster_home/` is local state** (run storage, pickled IO, sensor cursors)
  and is gitignored.

# Luigi

Luigi 3.7 implementation of the four bake-off DAGs. See `../README.md` for the
DAG specs and `../RUNNING.md` for cross-cutting setup (the resume-broker model,
the DAG 2 fan-out URL rule).

- **Definition style:** `luigi.Task` classes with `requires()` / `output()` / `run()`
- **Wait mechanism:** none — Luigi has no suspend, so DAG 2 and DAG 4 block a
  worker in a poll loop (the honest fallback; see Findings)
- **Engine:** none. `--local-scheduler` runs everything in the invoking process;
  `luigid` is an optional central scheduler, not required
- **Schema namespace:** `BAKEOFF_NS=luigi` → `luigi_dag1` / `_dag3` / `_dag4`

---

## Launch

**Status: all four DAGs verified end-to-end on 2026-08-06** (Luigi 3.7.1,
Python 3.14.2, Podman), including DAG 4's approval, skip-approval and
rejection→compensation paths, and DAG 3's retriable-vs-terminal classification.

### 1. Backbone

```bash
just up                  # postgres :54321 + mocks :8090-8092 + fixture :8099
just seed luigi          # create luigi_dag{1,3,4} + seed fixtures
```

`just seed luigi` is **required** — this implementation only gained schema
isolation on 2026-08-06, so nothing had ever created these schemas.

### 2. Environment

```bash
cd luigi
export PYTHONPATH="$PWD"                       # REQUIRED -- see below
export POSTGRES_HOST=localhost POSTGRES_PORT=54321
export BAKEOFF_NS=luigi
export CALLBACK_FETCH_SERVICE_URL=http://localhost:8090
export APPROVAL_SERVICE_URL=http://localhost:8091
export SHIPPING_SERVICE_URL=http://localhost:8092
```

**`PYTHONPATH` is only needed for the `--module` form.** Each DAG file ends in
`if __name__ == "__main__": luigi.run()`, so there are two ways to invoke it:

| Form | `PYTHONPATH` needed? | Why |
|---|---|---|
| `python dag3_payment.py SendNotification …` | **no** | `sys.path[0]` is the script's own directory |
| `luigi --module dag3_payment SendNotification …` | **yes** | `luigi` is a console script, so `sys.path[0]` is the venv's `bin/`; the bare `__import__("dag3_payment")` then fails |

Without it the `--module` form dies with
`ModuleNotFoundError: No module named 'dag3_payment'` before any task runs —
the same class of trap as Airflow's triggerer `PYTHONPATH` requirement
(`../airflow/env.sh`). Verified both ways: the direct form ran DAG 3's four
tasks with `PYTHONPATH` explicitly unset.

### 3. Run the DAGs

Either invocation style works; these use the `--module` form (hence the
`PYTHONPATH` above). Swap in `python dag1_csv_etl.py ConvertToParquet …` to drop
that requirement.

```bash
# DAG 1 -- 3 CSVs loaded in parallel, SQL transform, Parquet
uv run --project .. luigi --module dag1_csv_etl ConvertToParquet \
  --zip-path ../test-data/sample-data.zip \
  --extract-dir /tmp/luigi-dag1/extracted \
  --output-dir /tmp/luigi-dag1/output \
  --run-id etl-001 --workers 4 --local-scheduler

# DAG 2 -- note the ?base= override, see below
uv run --project .. luigi --module dag2_api_fanout CombineResults \
  --url "http://fixture-service:8099/books?per_page=30&base=http://localhost:8099" \
  --run-id fanout-001 --workers 4 --local-scheduler

# DAG 3
uv run --project .. luigi --module dag3_payment SendNotification \
  --payment-id PAY-001 --amount 100.00 --currency USD \
  --from-account ACC-001 --to-account ACC-003 \
  --run-id PAY-001 --local-scheduler

# DAG 4 -- 559.97 clears the 500 threshold, so this takes the approval path
uv run --project .. luigi --module dag4_order_fulfillment SendOrderNotification \
  --order-id ORD-001 --customer-id CUST-42 \
  --items-json '[{"sku":"WIDGET-A","quantity":2,"unit_price":29.99},
                 {"sku":"GADGET-B","quantity":1,"unit_price":499.99}]' \
  --shipping-address-json '{"street":"1 Main St","city":"Springfield","state":"IL","zip":"62701","country":"US"}' \
  --run-id ORD-001 --workers 1 --local-scheduler
```

Two CLI facts that cost time:

- **DAG 2 needs `&base=http://localhost:8099` appended.** The *collection* is
  fetched by callback-fetch-service (a container), so its host must be the
  compose DNS name — but the *detail* URLs are fetched by Luigi's own tasks on
  the **host**, and fixture-service derives them from the request. Without the
  override the fan-out tries to resolve `fixture-service` from the host and hangs
  until the poll timeout. This is documented in `../CLAUDE.md`; it applies to
  every host-run fan-out, not just Luigi.
- **Only the *root* task's parameters are bare CLI flags.** `--max-retries 5`
  fails with `unrecognized arguments` because `max_retries` belongs to
  `ProcessPayment`, not to the root `SendNotification`. Use
  `--ProcessPayment-max-retries 5`.

### Re-running

Every task's `output()` is a `luigi.LocalTarget` marker under
`/tmp/luigi-markers/dag<N>/<run-id>/`. **Re-running the same `--run-id` skips
every completed task** — that is Luigi's whole idempotency model. Vary the
`run-id` for a fresh run, or delete the marker directory.

DAG 3/4 also key idempotency on `payment_id` / `order_id` in the database, so a
repeated id hits the idempotent-skip branch even with a new `run-id`.

---

## What this implementation demonstrates

| DAG | Luigi idioms |
|---|---|
| 1 | `requires()` returning a list for fan-out, `--workers N` for parallelism, `LocalTarget` markers as the completion record |
| 2 | Blocking poll loop in `run()` (no suspend), per-item `FetchItemDetail` tasks discovered from a prior task's output |
| 3 | Hand-rolled retry loop with exception-type branching inside `run()` |
| 4 | Conditional logic inside `requires()`/`run()` rather than in the graph; hand-rolled compensation in the failing task |

---

## Findings

Observations bearing on `../comparison.md` (Luigi currently scores **38**, the
lowest of the twelve).

### Everything the other tools get from the framework is hand-written here

That is the single honest summary. Luigi provides task dependencies, target-based
idempotency, and a CLI. Everything else in these four DAGs is application code:

| Concern | Every other tool | Luigi |
|---|---|---|
| Retries + backoff | declarative policy | `for attempt in range(max_retries)` inside `run()` |
| Retriable vs terminal | typed policy / exception registry | `except PaymentDeclined: raise` vs `except PaymentGateway5xx: sleep` |
| Conditional branching | Choice / `conditional()` / `when:` | `if` inside the task body |
| Saga compensation | on-failure hooks or ordinary durable code | `try/except` in `CallShippingAPI` that unwinds by hand |
| Suspend for approval | signals / gates / task tokens | blocking `while` loop holding a worker |

The code works — all four DAGs pass — but none of it is *visible* to the
orchestrator, which is what the score reflects.

### The retry loop is completely silent

`ProcessPayment` retries up to 5 times with exponential backoff and jitter, and
logs **nothing** per attempt. From outside the process there is no way to tell
whether a task succeeded first time or on the fifth: no retry counter in any UI,
no structured event, nothing in the marker file. Verified by forcing the
retriable branch — the run took five attempts and the only external evidence was
the final traceback. Contrast Hatchet or Temporal, where attempt count is
first-class metadata.

### No suspend means a worker is held for the entire wait

DAG 2's fetch and DAG 4's approval both block. With `--workers 1` (DAG 4's
documented invocation) the whole pipeline is stalled by one human decision. This
is not a defect in the implementation — it is the honest fallback, and the files
say so in their `DIVERGENCE` comments — but it is the reason Luigi scores 0/5 on
suspend/resume. The approval service's 10s auto-decide delay makes it look cheap;
a real approval taking a day would hold a worker for a day.

### Conditional branching cannot change the graph

DAG 4's "skip approval for small orders" is implemented as an `if` inside
`ManagerApproval.run()`, so **the task always runs** — verified: the
skip-approval run still scheduled 6 tasks, identical to the approval run, and the
marker recorded `{"decision": "not_required", "reason": "Order total 29.97 below
threshold 500.0"}`. The DAG shape is fixed; only the task's internal behaviour
varies. Argo skips the node outright, Step Functions routes around it, and both
make the branch visible in the execution graph. Luigi's is invisible.

### Compensation works, but the run still ends Failed

On rejection, `CallShippingAPI` unwinds by hand — releases the reservations,
marks the order cancelled with a reason, sends the cancellation notification —
and then re-raises, so Luigi reports `1 failed` and `2 had failed dependencies`.
Semantically right, and identical to Prefect's behaviour, but "compensated
cleanly" and "crashed" are indistinguishable from the summary. With no persistent
UI, the only way to tell them apart is to read the DB.

### Schema isolation was the last holdout, and it mattered

Luigi was the eighth and final implementation to get `BAKEOFF_NS`. Before this,
it wrote unqualified table names into `public`, which is why the `public.*`
fixtures had to be kept alive for it alone. Now that DAG 1/3/4 scope to
`luigi_dag{1,3,4}`, Luigi no longer reads `public.*` at all. Those tables are
**not quite dead yet** — the *deployed* Step Functions state machines still use
them until `terraform -chdir=terraform/aws apply` lands their `BAKEOFF_NS`
change — but Luigi was the last of the local implementations keeping them alive.
Verified: after a DAG 3 run, `luigi_dag3.accounts` shows ACC-001 at 4900.00 while
`public.accounts` still shows 5000.00, untouched.

---

## Verified behaviour

- **DAG 1:** 7 tasks, 3 CSVs loaded in parallel at `--workers 4` → `luigi_dag1`
  with customers 5 / products 5 / orders 10 / `combined_report` 10 → Parquet
  (3,688 bytes, 10 rows × 11 columns). Matches Prefect and Flyte exactly.
- **DAG 2:** 33 tasks — async submit → poll → 30 `FetchItemDetail` tasks → combine.
  `total_items: 30, successful: 30, failed: 0, errors: []` against the fixture
  Books API.
- **DAG 3 happy path:** 4 tasks; ACC-001 5000 → 4900, ACC-003 0 → 100,
  transaction `completed` with a gateway id.
- **DAG 3 non-retriable:** forced `PaymentDeclined` fails on the **first**
  attempt with no retry, and records a `failed` transaction whose `error_message`
  carries the decline payload.
- **DAG 3 retriable:** forced `PaymentGateway5xx` exhausts all 5 attempts, then
  records a `failed` transaction with the 500 message. Correctly distinguished
  from the decline above.
- **DAG 4 approval path:** 6 tasks; `APR-E371A886C2AF` approved by
  `auto-decider`, order `shipped` with tracking, total 559.97, both reservation
  rows present, inventory decremented 100→98 and 50→49.
- **DAG 4 skip-approval:** 3× `THING-C` (29.97 < 500) → shipped, **zero**
  `approval_requests` rows, marker records `decision: not_required`.
- **DAG 4 rejection → compensation:** reservations `released` with `released_at`
  set, order `cancelled` with `failure_reason`, approval recorded `rejected`,
  cancellation notification emitted, and **inventory restored** — WIDGET-A went
  100 → 98 (approved run) → 96 (this run) → 98 (released).
- **Schema isolation:** `public.*` provably untouched by any of the above.

## Not yet exercised

- **Shipping-failure compensation.** The shipping service is 70% success, so the
  rejection path was forced instead; a shipping failure would exercise the same
  compensation code by a different trigger.
- **Approval timeout → compensation.** `poll_timeout` is a parameter, so setting
  it below the service's 10s auto-decide delay should reach it.
- Duplicate decision, and concurrent contention for `RARE-D`'s last 2 units.
- **`luigid` central scheduler.** Everything was run with `--local-scheduler`.
  The web UI at :8082 and its 24-hour default retention are unverified, which is
  the basis of the 1/10 audit-trail score.

---

## Fixes applied (2026-08-06)

All four DAGs needed changes before they would run:

1. **`BAKEOFF_NS` schema isolation wired into DAG 1/3/4** — previously wrote
   unqualified names into `public`. DAG 1 self-creates its schema (tables come
   from CSVs); DAG 3/4 fail fast with a `just seed luigi` hint, because they need
   seeded fixtures. Luigi was the last implementation to get this.
2. **DAG 4 FK ordering** — `inventory_reservations` rows were inserted before the
   `orders` upsert they reference, and the FK is non-deferrable, so any new
   `order_id` failed. The upsert now runs first. **Identical to the bug found in
   Prefect, Argo and Flyte.**
3. **DAG 2 fetch registration had no `provider`** — the callback-fetch service is
   a resume broker and rejects a registration it cannot classify, so this
   returned `422 cannot infer provider`. Now registers `http_callback` with a
   deliberately dead URL, since Luigi polls and can never receive a callback.
4. **DAG 4 approval registration had no `provider`** — same 422, same fix.
5. **DAG 4's documented invocation used `CUST-001` and `SKU-A`**, neither of
   which exists in the seed data. Now `CUST-42` with `WIDGET-A`/`GADGET-B`,
   totalling 559.97 so the documented command exercises the approval path.
6. **DAG 3's documented invocation paid into `ACC-002`** (Bob's account) rather
   than the merchant account `ACC-003` that every other implementation uses.

No changes were needed to the task graphs themselves — Luigi's model is simple
enough that the structure was right; everything that broke was integration
detail. That is worth noting alongside the low score: **simple models have fewer
ways to be subtly wrong.** Compare Flyte, where the graph semantics themselves
were misunderstood in five places.

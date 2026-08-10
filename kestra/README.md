# Kestra

Kestra implementation of the four bake-off DAGs. See `../README.md` for the DAG
specs and `../RUNNING.md` for cross-cutting setup (container networking, the
resume-broker model, teardown).

- **Definition style:** declarative YAML flows, one file per DAG; DAG 4's three
  sub-workflows live in `subflows/`
- **Wait mechanism:** native `io.kestra.plugin.core.flow.Pause` with declared
  `onResume:` inputs; resumed out-of-band via the REST resume endpoint
- **Engine:** `kestra/kestra:latest` (1.3.30) standalone container; **no separate
  worker** — script tasks launch their own sibling containers
- **Schema namespace:** `kestra_dag1` / `_dag3` / `_dag4` via flow-level
  `pg_schema` variables

---

## Launch

**Status: all four DAGs verified end-to-end on 2026-08-04** (Kestra 1.3.30,
Podman). Includes native Pause suspend/resume for DAG 2 *and* DAG 4, saga
compensation via rejection *and* timeout *and* shipping failure, the spec's
concurrency caps, and `kestra_*` schema isolation.

### 1. Backbone

```bash
just up                  # postgres :54321 + mocks :8090-8092 + Books API :8099
just seed kestra         # create kestra_dag{1,3,4} schemas + seed fixtures
```

`just seed` is required on an existing `pgdata` volume — `init-db.sql` only runs
on a fresh one. It is idempotent.

### 2. Kestra — needs a container socket

Script tasks default to Kestra's **Docker task runner**, so the server needs a
Docker-compatible Engine API socket. Verified working on Podman:

```bash
systemctl --user enable --now podman.socket
just up kestra   # UI :8081 -- the Justfile auto-detects the podman socket
```

The host port defaults to **8081** — 8080 belongs to Airflow in the repo's port
map (`../RUNNING.md` §0). Remap with `KESTRA_PORT`; it is the *host* port only,
so callbacks are unaffected (`kestra.url` still points at `kestra:8080`):

```bash
KESTRA_PORT=8085 CONTAINER_SOCK=/run/user/$(id -u)/podman/podman.sock just up kestra
```

UI login: `admin@orchestration.local` / `Orchestration_123` (basic auth is
mandatory in Kestra OSS ≥ 0.24.0 and cannot be disabled).

### 3. Load the flows

`RUNNING.md`'s documented `kestra flow namespace update` command does **not
work**: there is no `kestra` binary on `PATH` (it is `java -jar /app/kestra`),
`flow validate` now defers to `kestractl`, and the CLI's API calls are
unauthenticated against a basic-auth server. Load over REST instead — one POST
per file, which also gives per-file error messages:

```bash
AUTH='admin@orchestration.local:Orchestration_123'
cd kestra
for f in *.yaml subflows/*.yaml; do
  curl -s -u "$AUTH" -X POST "http://localhost:8081/api/v1/main/flows" \
    -H 'Content-Type: application/x-yaml' --data-binary @"$f"
done
```

Re-loading an existing flow returns 409; use `PUT /flows/{namespace}/{id}` to
update. Swap `POST /flows/validate` for a parse check without registering.

### 4. Run

DAG 1 and 2 pull their inputs from **fixture-service** (`../RUNNING.md` §0b),
which `just up` already started. DAG 1 is the mandatory case: Kestra downloads
its ZIP from a `zip_url` where other tools read a local path. Task containers sit
on the compose network, so address it as `fixture-service:8099` — no host gateway,
and never `localhost`.

```bash
AUTH='admin@orchestration.local:Orchestration_123'
BASE=http://localhost:8081/api/v1/main

# DAG 1
curl -u "$AUTH" -X POST "$BASE/executions/orchestration.etl/csv_etl_pipeline" \
  -F 'zip_url=http://fixture-service:8099/sample-data.zip'

# DAG 2  (fixture-service's Books API; ?per_page=30 overshoots the cap of 20)
curl -u "$AUTH" -X POST "$BASE/executions/orchestration.api/api_fanout_with_callback" \
  -F 'url=http://fixture-service:8099/books'

# DAG 3
curl -u "$AUTH" -X POST "$BASE/executions/orchestration.payments/payment_processing" \
  -F payment_id=KES-PAY-001 -F amount=125.50 \
  -F from_account=ACC-001 -F to_account=ACC-003 -F idempotency_key=KES-IDEM-001

# DAG 4  (>= 500.00 requires approval; address needs street/city/state/zip)
curl -u "$AUTH" -X POST "$BASE/executions/orchestration.fulfillment/order_fulfillment" \
  -F order_id=KES-ORD-001 -F customer_id=CUST-42 \
  -F 'items=[{"sku":"GADGET-B","quantity":2,"unit_price":499.99}]' \
  -F 'shipping_address={"street":"9 High St","city":"Austin","state":"TX","zip":"78702"}'
```

---

## Tool idioms demonstrated

- **Declarative YAML** with `type:`-addressed plugins — no SDK, no build step
- **`pluginDefaults`** to set the task runner once per flow instead of per task
- **`ForEach` + `concurrencyLimit`** for dynamic fan-out with a cap
- **`Pause` + `onResume:`** typed inputs for true suspend/resume
- **`Subflow` + flow-level `outputs:`** for DAG 4's three sub-workflows
- **`If` / `then` / `else`** for conditional branching
- **`errors:`** blocks as flow-level failure handlers
- **`allowFailure: true`** for graceful degradation (DAG 3's notification)
- **First-class JDBC plugins** (`postgresql.Query` / `.Queries`) — SQL without
  writing a Python task
- **Two task runners**: `Process` (in the server container) vs `Docker`
  (container-per-task). DAG 3 uses `Process`; the rest use `Docker`.

---

## Findings

Things here that bear on `../comparison.md` scoring.

### Suspend/resume works, but the caller must be Kestra-aware

Kestra has genuine suspend/resume: the execution parks, an unrelated process
wakes it, and typed data crosses the boundary. Two caveats that cost it against
Step Functions and Temporal:

1. **No pre-built callback URL.** There is no `execution.resumeUrl` — the
   documented execution context is only `id`, `startDate`, `state`,
   `originalId`. You build the URL yourself from `{{ execution.id }}`.
2. **The resume endpoint is fussy.** `POST /api/v1/{tenant}/executions/{id}/resume`
   requires authentication (**401** otherwise) and accepts **only**
   `multipart/form-data` (**415** on JSON *and* on urlencoded). A generic "POST
   my JSON to this URL" webhook sender therefore *cannot* resume Kestra. This is
   why the shared mocks needed a dedicated `kestra` provider
   (`shared-services/{callback-fetch,approval}-service/app.py`) rather than
   reusing `http_callback`.

Because the execution id is an identifier and not a capability token, the resume
call needs separate credentials. **Kestra OSS has exactly one credential** — the
shared admin account; `api-tokens`, `service-accounts` and `users` endpoints all
404, as service accounts are Enterprise-only. So any machine-to-machine
integration holds full admin rights. Step Functions scopes this with an IAM
policy at no cost; Temporal has namespace-scoped identities.

Also: for DAG 4 the resume handle must be the **subflow's** execution id, not the
parent's — the `Pause` lives in `manager_approval_flow`, so resuming the parent
is a no-op.

### `Pause` timeout is an absolute cap that keeps counting after resume

`timeout:` on a `Pause` is not a "how long to wait for the callback" budget. It
caps total task duration and **keeps ticking after the resume**: a Pause that had
already succeeded flips back to `FAILED` — failing the whole execution — if the
total elapses while downstream tasks are still running. Observed directly: a
`PT60S` pause failed at T+61s while the fan-out was still going, after having
been resumed at T+4s. The timeout has to cover the entire downstream branch,
which makes it useless as a callback deadline.

### Concurrency caps: `ForEach.concurrencyLimit` holds exactly

Verified rather than assumed. `fixture-service` serves a corpus of thousands, so
pointing DAG 2 at `/books?per_page=30` puts 30 detail fetches behind a
`concurrencyLimit: 20`.
Reconstructing peak simultaneity from the task attempt windows gives **exactly
20** — the cap engages and holds, and all 30 fetches still complete
(`total/ok/fail = 30/30/0`). DAG 1's cap of 10 remains unexercised because the
sample ZIP only holds 3 CSVs.

### Retries have no error-type predicate

`retry:` applies uniformly to every exception. There is no equivalent of
Prefect's `retry_condition_fn` or Airflow's exception classification, so the
"retriable vs. non-retriable" distinction the spec asks for cannot be expressed
declaratively. Both DAG 3 and DAG 4 demonstrate the cost:

- DAG 3 retries `PaymentDeclined` five times. A declined card will stay declined.
- DAG 4 retries `InvalidAddress` **twelve** times (parent `Subflow` retry 3 ×
  inner `ship` retry 4) with exponential backoff before compensating — an
  execution left running for over ten minutes on permanently invalid input.

The only workaround is to branch in the task body and exit successfully, which
moves error classification out of the orchestrator and into the script.

### Postgres loss is fatal, and the restart can wedge

Kestra uses Postgres for both the repository and the queue, and treats a lost
connection as fatal rather than retrying: every queue-consumer thread logs `Fatal
error while polling the '<type>' queue. Initiating shutdown.` There is no
reconnect setting; upstream has open issues in this area (kestra-io/kestra#4076,
#5147, #10358). Measured deliberately rather than inferred:

| Scenario | Outcome |
|---|---|
| ~5s blip | all consumers log the fatal error, process **survives** |
| Postgres back *before* the restart | exits **status 0**, `restart: unless-stopped` fires, self-heals in ~45–90s |
| Postgres still down *at* the restart | `UnknownHostException: postgres`, JVM stays alive, **never retries** — wedged indefinitely |

Three things make this worse than a plain crash:

1. **Exit status 0.** A zero exit reads as a clean shutdown, so `restart:
   on-failure` never fires. It has to be `always` / `unless-stopped`.
2. **The wedged state lies.** The container reports `running` with a live JVM
   while serving nothing. Only an HTTP probe distinguishes it — hence the
   healthcheck on `/ping`, which flips to `unhealthy` after ~150s. Neither podman
   nor docker restarts on unhealthy, so recovery stays manual (`podman restart`,
   ~18s).
3. **Ordinary commands trigger it.** `podman compose up -d <other-service>`
   recreates `postgres` as a side effect. That is how this was hit **six times**
   while testing the other DAGs — twice it took Kestra down mid-verification.

Flow definitions survive (the repository is Postgres-backed too); in-flight
executions do not. Airflow and Temporal both reconnect instead.

### Docker task runner: isolation is real, but per task and unconfigured

The Docker task runner gives genuine per-task dependency isolation (each script
gets a fresh `python:3.13-slim`), which substantiates the isolation claim in
`comparison.md`. Two costs:

- **Every task pays a `pip install` on every attempt.** No image caching of
  dependencies; a 3-CSV fan-out installs `psycopg2-binary` three times.
- **Task containers are siblings, not children.** They join the default bridge
  and cannot resolve compose service names, so `networkMode` must be set
  explicitly or every DB task fails with `could not translate host name
  "postgres"`. This is the Kestra analogue of Prefect's ephemeral-compute traps.

The `Process` runner (used by DAG 3) avoids both but forfeits isolation and runs
everything inside the server container.

### `kestra.variables.globals` did not resolve in `pluginDefaults`

`{{ globals.x }}` and `{{ envs.x }}` both failed with `Unable to find 'x'` inside
a `pluginDefaults` block, even with `kestra.variables.globals.task_network` set
in `KESTRA_CONFIGURATION` (confirmed present in the container's env). Flow-level
`variables:` work fine, so `task_network` is declared per flow. Not investigated
further — worth a re-check before citing it as a limitation.

### Smaller ones

- **`EachParallel` is deprecated** in 1.x; `ForEach` replaces it and is where
  `concurrencyLimit` lives. Validation reports this as a `deprecationPaths` entry
  rather than a warning, so it is easy to miss.
- **`Query` rejects multi-statement SQL** outright ("Query task support only a
  single SQL statement") rather than splitting it — use `Queries`.
- **`inputFiles` maps one destination filename to one `kestra://` URI.** Pointing
  a directory-shaped key at a whole `outputFiles` map silently writes a single
  file containing the stringified map.
- **Interpolating rendered JSON into a Python literal is unsafe.** ForEach output
  maps contain `\"`, and Python's triple-quoted strings process backslashes, so
  the JSON is corrupted before `json.loads` sees it. Pass it via `inputFiles`.
- **Flow-level `outputs:` render even when the producing task failed**, and an
  unresolved reference raises `Failed to render output values`, masking the real
  error. Every output value needs a `?? ''` fallback.
- **A parent `Subflow` task exposes only `{state, executionId}`.** Inner task
  outputs are unreachable unless the subflow declares flow-level `outputs:`.

---

## Not yet exercised

- **Schedules.** DAG 1's `Schedule` trigger is present but `disabled: true`;
  every run here was API-triggered.
- **Webhook triggers.** DAG 2/3/4 declare `Webhook` triggers that were never
  fired — executions were started via the executions API instead.
- **DAG 2 partial-failure aggregation.** `combine_results` now reports
  `failed`/`errors` honestly, but was only observed on an all-success run; the
  `status: partial` branch is untested.
- **DAG 3's declined-payment path.** The gateway simulates a 5% decline that was
  never rolled; only timeout/5xx retries were observed.
- **Duplicate and late resumes.** The mocks' `/resume` endpoint can re-fire a
  decision, which should be ignored idempotently. Not tried.
- **DAG 1's concurrency cap.** `concurrencyLimit: 10` is set, but the sample ZIP
  holds only 3 CSVs, so it never engages. (DAG 2's cap of 20 *is* now verified —
  see Findings.)
- **Kestra's own retry/replay UI**, and `errors:` vs `afterExecution` semantics.

---

## Fixes applied

Every one of the four DAGs was broken. Counting distinct defects:

**Universal**

1. **All 30 script tasks crashed on `from kestra import Kestra`.** The SDK is
   preinstalled in the server's `/app/.venv` (so DAG 3's `Process` tasks worked)
   but absent from Docker-runner images. No `beforeCommands` installed it. Added
   `kestra` to all 16 `pip install` lines, plus `beforeCommands` to the 10 tasks
   that had none — including every `errors:` handler, so the failure path failed
   too.
2. **Task containers could not resolve `postgres`.** Added a `pluginDefaults`
   block per flow setting `Docker.networkMode` from a `task_network` variable.

**DAG 1**

3. `inputFiles` pointed a `extracted/` key at the whole `outputFiles` map →
   `NotADirectoryError`. Now maps `{{ taskrun.value }}` to the per-file URI.
4. `run_sql_transform` used `Query` for two statements → now `Queries`.
5. Wrote unqualified table names into `public`, **overwriting the shared
   `orders`/`customers` fixtures** that Dagster and Luigi read. Now
   `search_path=kestra_dag1` + `currentSchema=`, and self-creates its schema.
6. `EachParallel` → `ForEach` with `concurrencyLimit: 10` (spec cap, previously
   absent).

**DAG 2**

7. **`{{ execution.resumeUrl }}` does not exist** — the flow died before pausing.
   Now registers `{{ execution.id }}` with the fetch service's new `kestra`
   provider.
8. `Pause` declared no `onResume:` inputs, so no data could cross the resume
   boundary; the consumer also read `.onResume` instead of `.onResume.payload`.
9. **`combine_results` ignored the fan-out entirely** — it re-listed its own
   inputs and hardcoded `errors = []`, so `failed` was structurally always 0.
   Now reads the real `ForEach` outputs and reports partial failure.
10. `EachParallel` → `ForEach` with `concurrencyLimit: 20` (spec cap).
11. `Pause` timeout raised `PT60S` → `PT300S` (see the timeout finding).

**DAG 4**

12. Unqualified table names → `kestra_dag4` across the flow and two subflows.
    This is what surfaced #13.
13. **FK insert ordering:** `reserve_inventory_flow` inserted
    `inventory_reservations` before the parent `orders` row →
    `ForeignKeyViolation`. Latent while it wrote to the legacy `public` tables,
    which lack that constraint. Order row now inserted first.
14. **All three subflows lacked flow-level `outputs:`**, so the parent's
    `check_decision` could not see the approval decision and every approved order
    fell through to compensation. Ten parent expressions rewritten from
    `outputs.<task>.outputs.<inner>.vars.<f>` to `outputs.<task>.outputs.<f>`.
15. Those new outputs then broke the *failure* path (`Unable to find 'carrier'`
    when `ship` failed) — all 11 values given `?? ''` fallbacks.

**Shared services** (`../shared-services/`)

16. Added a `kestra` resume provider to `callback-fetch-service` and
    `approval-service`: authenticates with `SecretStr` credentials and posts
    `multipart/form-data`, inferred from `resume_data.execution_id`.

**Repo plumbing**

17. `docker-compose.yml`: `KESTRA_PORT` and `CONTAINER_SOCK` parameterised, and
    `KESTRA_URL`/`KESTRA_USER`/`KESTRA_PASSWORD` wired into both mocks.
18. Data repair: dropped DAG 1's `public.products` / `public.combined_report`
    residue and rebuilt `public.orders` / `public.customers` to the
    `bootstrap_bakeoff` shape. **Note:** the rebuilt `orders.customer_id` carries
    a FK to `public.customers` that may not have existed before — the original
    was destroyed before it could be inspected, and the surviving legacy tables
    suggest a looser earlier DDL. Drop that one constraint if Dagster or Luigi
    inserts orders for customers outside the fixture set.

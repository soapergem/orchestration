# Prefect

Prefect 3.x implementation of the four bake-off DAGs. See `../README.md` for the
DAG specs and `../RUNNING.md` for cross-cutting setup (container networking, the
resume-broker model, teardown).

- **Definition style:** imperative Python, `@flow` / `@task` decorators
- **Wait mechanism:** native `pause_flow_run()` + REST resume (default); polling fallback via `APPROVAL_WAIT_MODE=poll`
- **Engine:** `prefect server` on the host; no engine container
- **Schema namespace:** `BAKEOFF_NS=prefect` → `prefect_dag1` / `_dag3` / `_dag4`

---

## Launch

**Status: all four DAGs verified end-to-end on 2026-07-27** (Prefect 3.7.1,
Python 3.14.2, Podman).

### 1. Backbone

```bash
just up                  # postgres :54321 + mocks :8090-8092
just seed prefect        # create prefect_dag{1,3,4} schemas + seed fixtures
```

`just seed` is required on an existing `pgdata` volume — `init-db.sql` only runs
on a fresh one. It is idempotent.

### 2. Prefect server — must bind 0.0.0.0

```bash
uv run prefect server start --host 0.0.0.0   # UI + API on :4200
```

**`--host 0.0.0.0` is required for DAG 4.** The approval-service *container*
resumes the flow by calling the Prefect API on the host; a server bound to the
default `127.0.0.1` refuses that connection and the approval silently times out.
(Verified: with loopback binding, `host.containers.internal:4200` from inside the
container gives `ConnectError`.)

Do **not** rely on the ephemeral API server — running a flow with no server up
fails with `Timed out while attempting to connect to ephemeral Prefect API
server` on Python 3.14.

### 3. Run the flows

With direnv loaded, `POSTGRES_HOST`/`POSTGRES_PORT` come from `.envrc`;
otherwise export them.

```bash
export PREFECT_API_URL=http://127.0.0.1:4200/api
export POSTGRES_HOST=localhost POSTGRES_PORT=54321
export CALLBACK_FETCH_SERVICE_URL=http://localhost:8090
export APPROVAL_SERVICE_URL=http://localhost:8091
export SHIPPING_SERVICE_URL=http://localhost:8092

python dag2_api_fanout.py            # ~20s  (see GitHub rate-limit warning)
python dag3_payment.py               # ~2s
python dag4_order_fulfillment.py     # ~25s  (10s auto-approve delay)
```

**DAG 1 needs explicit paths** — its defaults (`/data/input/data.zip`) are
container paths:

```bash
mkdir -p /tmp/dag1/{input,extracted,output}
cp ../test-data/sample-data.zip /tmp/dag1/input/data.zip
python -c "
from dag1_csv_etl import csv_etl_pipeline
csv_etl_pipeline(zip_path='/tmp/dag1/input/data.zip',
                 extract_dir='/tmp/dag1/extracted',
                 output_dir='/tmp/dag1/output')"
```

### 4. All four DAGs as deployments (recommended)

`serve_all.py` registers **five** deployments from one process — the four DAGs
plus DAG 4's separately-invoked approval flow. No work pool needed; each run
executes as a subprocess of the runner.

```bash
cd prefect
APPROVAL_WAIT_MODE=suspend \
PREFECT_API_URL=http://127.0.0.1:4200/api \
POSTGRES_HOST=localhost POSTGRES_PORT=54321 \
CALLBACK_FETCH_SERVICE_URL=http://localhost:8090 \
APPROVAL_SERVICE_URL=http://localhost:8091 \
SHIPPING_SERVICE_URL=http://localhost:8092 \
ETL_ZIP_PATH=$PWD/../.local-data/input/data.zip \
ETL_EXTRACT_DIR=$PWD/../.local-data/extracted \
ETL_OUTPUT_DIR=$PWD/../.local-data/output \
  python serve_all.py             # leave running
```

Then, from another shell — **no parameters needed**, every flow has working
defaults:

```bash
prefect deployment run 'csv_etl_pipeline/dag1-csv-etl'
prefect deployment run 'api_fanout_pipeline/dag2-api-fanout'
prefect deployment run 'payment_processing/dag3-payment'
prefect deployment run 'order_fulfillment/dag4-order-fulfillment'
```

Override per run with `-p name=value`, or use the UI's generated parameter form.

Two things to know:

- **The runner passes its own environment to every run it launches.** Set the
  `POSTGRES_*` / `*_SERVICE_URL` / `ETL_*` vars on the runner, not per
  invocation. Getting this wrong fails in a confusing way: the flow falls back to
  compose DNS names (`postgres:5432`) and can't resolve them from the host.
- **DAG 1's paths come from `ETL_*`** (same names as the airflow
  implementation). The `/data/...` fallbacks are container paths that don't exist
  on the host. `.local-data/` is gitignored scratch space.

### 5. Docker work pool — one container per flow run

`serve_all.py` gives *process* isolation (every run is a subprocess of one runner
sharing one Python env). `deploy_docker.py` gives **container** isolation, which
is what `../comparison.md`'s isolation claim actually refers to.

```bash
cd prefect
podman build -t localhost/prefect-bakeoff:latest -f Dockerfile .   # code is baked in
prefect work-pool create --type docker bakeoff-docker              # once
PREFECT_API_URL=http://127.0.0.1:4200/api python deploy_docker.py

# worker (needs the Docker-compatible API socket):
DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock \
PREFECT_API_URL=http://127.0.0.1:4200/api \
  prefect worker start --pool bakeoff-docker

prefect deployment run 'payment_processing/dag3-payment-docker'
```

Registers `dag1-csv-etl-docker`, `dag2-api-fanout-docker`,
`dag3-payment-docker`, `dag4-order-fulfillment-docker`,
`dag4-manager-approval-docker`. **Rebuild the image after editing a DAG** — the
code is in the image, not mounted.

Non-obvious bits, all of which cost time to find:

- **`image_pull_policy: "Never"`** is required for a locally built image; the
  default tries to pull `localhost/prefect-bakeoff` from a registry and fails.
- **Nothing may point at `localhost`** — inside the container that is the
  container. Everything goes through the host gateway (`HOST_GATEWAY`), including
  Postgres on `:54321` and the Prefect API itself.
- **Work pool names starting with `prefect` are rejected** as reserved.
- **DAG 1 needs a bind mount** (`volumes: [<host>/.local-data:/data]`) since it
  is the only DAG with filesystem I/O.
- **Suspend mode needs shared result storage.** `suspend_flow_run()` resumes in a
  *brand new container* that cannot see the previous one's local storage, so
  `PREFECT_LOCAL_STORAGE_PATH=/results` is bind-mounted to the host. With that in
  place it works — the resumed run logs `Finished in state Cached(type=COMPLETED)`
  for its already-completed task, i.e. it rehydrated persisted state written by a
  container that no longer exists. In production this would be S3/GCS rather than
  a bind mount.

#### Container runtimes

The docker worker speaks the **Docker Engine API** via docker-py, so what matters
is whether your runtime serves that API — not whether a `docker` CLI exists.

| Runtime | Status | Setup |
|---|---|---|
| **podman** | **Verified here** | `systemctl --user enable --now podman.socket`, then `DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock`. docker-py reports "Podman Engine", API 1.41. Host gateway `host.containers.internal`. |
| **docker** | Expected to work | `DOCKER_HOST` usually unset. Host gateway `host.docker.internal`. |
| **finch** | **Unverified** | The finch CLI alone does not serve the Docker Engine API (nerdctl + containerd in a Lima VM). The separate [`runfinch/finch-daemon`](https://github.com/runfinch/finch-daemon) does expose a dockerd-like socket implementing a **subset** of Docker Engine API v1.43 (typically `unix:///tmp/nerdctl/nerdctl.sock`). Open questions: whether prefect-docker's required endpoints are in that subset, and on macOS whether the in-VM socket can be forwarded to the host. Set `HOST_GATEWAY=host.docker.internal`. |

`HOST_GATEWAY`, `DOCKER_IMAGE`, `DOCKER_WORK_POOL`, and `LOCAL_DATA_DIR` are all
env-overridable, so retargeting to another runtime needs no code edits. If the
docker pool can't be made to work on a given machine, `serve_all.py` runs the
identical DAG code with process isolation.

A K8s work pool would be the third tier; not attempted, and it would need a
registry the cluster can pull from rather than a `localhost/` image.

#### Why the idempotency keys now auto-generate

Deployment parameters are **static**, but DAG 3's `payment_id` and DAG 4's
`order_id` are idempotency keys. A fixed default meant the first run worked and
every subsequent one hit the duplicate/skip branch — DAG 3 raised
`Duplicate payment: existing transaction with status completed`. Both now default
to a generated id, so deployments and scripts are re-runnable; the `__main__`
blocks document how to pass explicit values to reach the failure branches
deliberately.

### Knobs

| Env var | Default | Purpose |
|---|---|---|
| `APPROVAL_WAIT_MODE` | `pause` | `suspend` = process exits while waiting (needs deployments); `pause` = process stays alive; `poll` = polling loop |
| `MANAGER_APPROVAL_DEPLOYMENT` | `manager_approval_flow/dag4-manager-approval` | Deployment invoked for the approval step in `suspend` mode |
| `PREFECT_RESUME_API_URL` | `http://host.containers.internal:4200/api` | Prefect API **as seen from the mock-service container**. Podman uses `host.containers.internal`; Docker/finch use `host.docker.internal`. |
| `MAX_CSV_LOAD_CONCURRENCY` | `10` | DAG 1 fan-out cap (spec) |
| `MAX_FANOUT_CONCURRENCY` | `20` | DAG 2 fan-out cap (spec) |

Timeouts are flow parameters: `order_fulfillment(approval_timeout=120,
approval_poll_interval=5)`. Setting `approval_timeout` below the service's
`AUTO_DECIDE_DELAY_SECONDS` (10s) is how the timeout path gets tested.

### Re-running

DAG 3 keys idempotency on `payment_id`, DAG 4 on `order_id`. Re-running the same
id hits the idempotent-skip branch instead of doing work — vary the id, or reset
with `just down-clean && just up && just seed prefect`.

DAG 3's gateway is deliberately flaky (5% non-retriable decline), so an
occasional `PaymentDeclined` traceback is **correct behaviour**, not a failure:
the flow records the failed transaction, notifies, then re-raises.

---

## What this implementation demonstrates

| DAG | Prefect idioms |
|---|---|
| 1 | `.map()` fan-out with `unmapped()` constants, `.result()` as the join barrier, `ThreadPoolTaskRunner(max_workers=)` concurrency cap |
| 2 | `@task(retries=, retry_delay_seconds=[...])` backoff, bounded `.map()` fan-out, early-return conditional branch |
| 3 | Typed exceptions + `retry_condition_fn` for retriable-vs-terminal classification |
| 4 | Subflows as composition units, `pause_flow_run()` + REST resume, hand-rolled compensation stack |

---

## Findings

Observations bearing on `../comparison.md` (Prefect currently scores **67**).

### Native suspend/resume works, but there are two tiers

DAG 4's approval wait now uses `pause_flow_run()`. The approval service registers
*this flow run's* own resume endpoint as its `http_callback` handle, so deciding
the approval un-pauses the flow directly. No Cloud features, no automations, no
webhook receiver — just `POST /api/flow_runs/{id}/resume`, which exists in OSS.

The important distinction for scoring:

| | Process | Requires |
|---|---|---|
| `pause_flow_run()` | **stays alive** polling for resume — a slot is still held | nothing |
| `suspend_flow_run()` | **exits**; run is rescheduled on resume — zero cost while waiting | a **deployment** + `persist_result` |

Quoting the installed `prefect/flow_runs.py`: *"In order suspend a flow run in
this way, the flow needs to have an associated deployment and results need to be
configured with the `persist_result` option."*

So Prefect legitimately belongs in `../README.md`'s "native suspend + API resume"
tier. **Both tiers are now implemented and verified** — see the next section for
what the `suspend` tier actually costs.

`total_run_time` **excludes** waiting time in *both* modes (a pause-mode approval
run showed 0.58s against 10.45s wall), so it is a nice duration metric but **not**
evidence of which mode you're in. The discriminator is in the logs — see the
suspend section below.

### Zero-cost suspension costs you the sub-workflow lineage

`APPROVAL_WAIT_MODE=suspend` + `serve_all.py` gets true suspension working, and
the logs show it unambiguously:

```
03:39:12  Suspending flow run, execution will be rescheduled when this flow run is resumed.
03:39:13  Finished in state Suspended(type=PAUSED)      <-- run ENDS, process exits
03:39:34  Approval will resume flow run b9caaf74...     <-- 21s later, re-executed from the top
03:39:35  Approval APR-293CC5AB5F74 decided: approved by auto-decider
```

Contrast pause mode, which logs `Resuming flow run execution!` and never ends the
run. Note the flow function **re-executes from the top** on resume — that's why
`persist_result=True` is required, and why `submit_approval_request` must be
idempotent (it is: cached task state, plus a caller-supplied id).

Getting there took two non-obvious steps, and the second one has a real cost:

1. **The approval flow can't be a subflow.** `suspend_flow_run()` inside one dies
   with `RuntimeError: Flow run cannot be suspended: Cannot suspend subflows.`
   Verified directly with a minimal repro. So the parent invokes it via
   `run_deployment()` instead.
2. **`run_deployment()` alone is not enough** — it sets `parent_task_run_id` by
   default, and Prefect's check rejects *any* run with a parent, deployment or
   not. It fails with the identical "Cannot suspend subflows" error. You must
   pass **`as_subflow=False`**, which severs the parent/child link — and with it,
   **the sub-workflow nesting in the UI**. DAG 4's composition is no longer
   visible as a tree; the approval run appears as an unrelated top-level run.

That trade-off is the finding worth carrying into the comparison: for Prefect,
**zero-cost suspension and sub-workflow lineage are mutually exclusive.**
DAG 4 specifies both (`ManagerApprovalWorkflow` as a sub-workflow *and* a
long-running durable wait). Temporal's child workflows give you both at once, and
Step Functions' nested state machines keep the parent link with
`.waitForTaskToken`. This is a concrete, demonstrable gap rather than a matter of
taste.

**And the parent still holds its process.** `run_deployment()` blocks until the
child finishes, so only the child suspends. Measured on one run: the approval run
consumed **0.44s** of run time across a **22.96s** wait, while its parent
consumed **40.96s** for a 40.96s wall clock — the full duration. Making the whole
chain zero-cost would require suspending the parent too, or decomposing it into
event-triggered deployments. So Prefect's zero-cost suspension is real but
**local to the suspending flow**, not to the workflow as a whole.

### Webhooks are Cloud-only; automations are not

Checked against the OSS server's OpenAPI spec: **`/webhooks` has zero routes.**
Automations (7 routes) and `POST /events` do exist self-hosted. So the
"external system → templated webhook URL → event → automation" chain is broken at
the first link in OSS; an external caller must speak Prefect's event API instead.
Worth a line in the licensing/tier column — but note it is **not** a blocker for
resume, which needs no webhook.

### Saga compensation is entirely hand-rolled

No framework primitive. `order_fulfillment` maintains an explicit list of
`(name, callable)` compensations and unwinds in reverse on failure. It works, but
every guarantee is the author's responsibility — contrast Temporal, where
compensation is ordinary code protected by durable execution.

On rejection/timeout the flow compensates and *then* re-raises, ending `Failed`.
Semantically right, but "compensated cleanly" and "crashed" look alike in the UI
without opening the run.

### Dependency isolation is real, and it is per flow run

The docker work pool substantiates the isolation claim rather than asserting it.
Concrete evidence: the same DAG 3 code that runs on **Python 3.14.2** in the host
venv ran on **Python 3.12.13** inside the flow-run container, against a separately
pinned dependency set, and wrote to Postgres normally. One container per flow run,
named after the run, auto-removed on exit.

But the granularity matters for scoring: isolation is **per flow run, not per
task**. Every task in a run shares that one container, so two tasks in the same
DAG cannot have conflicting dependencies — you'd have to split them into separate
flows. Argo and Flyte give per-*task* containers by design. The existing
`comparison.md` wording ("isolation is per flow run, not per task") is accurate
and now verified.

Also note what the isolation costs at the edges: DAG 1 needed a bind mount for
filesystem I/O, and suspend/resume needed shared result storage because the
resumed run gets a *new* container. Neither is a defect — they are the normal
consequences of ephemeral compute — but they are setup steps the process-pool
path doesn't need.

### Concurrency caps need an explicit task runner

`.map()` fans out with **no cap** by default; the spec's limits (10 for DAG 1, 20
for DAG 2) were simply absent. Bounding the flow's `ThreadPoolTaskRunner` is the
per-run fix and is what's implemented. For a cap shared *across* runs you'd want
`prefect.concurrency`'s global limits, which need server-side registration.

Caveat: DAG 1's sample ZIP has only 3 CSVs, so its cap of 10 is declared but
never reached. DAG 2's 30 items do exercise its cap.

### `.map()` silently zips dict arguments

Constant kwargs to `.map()` must be wrapped in `unmapped()`. A bare dict is
treated as an iterable of its keys, so `db_config=cfg` became a 5-element map
axis (`MappingLengthMismatch`). Hit DAG 1 and DAG 2 both.

### Non-retriable classification needs an explicit hook

`retries=` applies to *every* exception. The only lever is
`retry_condition_fn(task, task_run, state)`, which must re-raise the result to
inspect the exception type — workable but clumsy beside Temporal's
`non_retryable_error_types` or Step Functions' `Catch` on a named error.

Also: with `retry_delay_seconds` as a **list**, `retry_jitter_factor=1.0` appears
not to apply — observed delays were exactly 3/6/12/24s. Jitter seems to be
scalar-delay only. Confirm before crediting Prefect with jittered backoff.

### Verified behaviour

- **DAG 1:** 3 CSVs loaded in parallel → `combined_report` (10 rows) → Parquet (7.6 KB), tables in `prefect_dag1`.
- **DAG 2:** async submit → poll → 30 items fanned out → combined, `errors: []`. Concurrency cap enforced: with the cap set to 5, peak overlap across the run's 30 `fetch_item_detail` task runs was exactly **5** (computed from API start/end timestamps).
- **DAG 3:** `ACC-001`→`ACC-003`, $100 moved (5000→4900, 0→100), transaction `completed`. Forcing the roll: `PaymentDeclined` fails with **0 retries**; `PaymentGateway5xx` retries 5× at 3/6/12/24/48s. A real random decline recorded a `failed` transaction with `error_message` set.
- **DAG 4 happy path (pause mode):** paused → approval service POSTed to the resume endpoint → *"Resuming flow run execution!"* → shipped. `/decide` returns **200** with `callback_status: 201`.
- **DAG 4 skip-approval:** 3× `THING-C` ($29.97 < $500) → approval bypassed → shipped.
- **DAG 4 rejection → compensation:** reservations `released`, order `cancelled` with reason, notification sent, inventory restored.
- **DAG 4 approval timeout → compensation:** `approval_timeout=5` beats the 10s auto-decider → `FlowPauseTimeout` → `expired` → compensation ran.
- **DAG 4 late resume does not resurrect:** the auto-decider fired at 10s against a run that already failed at 5s. Both flow runs stayed `FAILED`, order stayed `cancelled`, inventory was **not** re-decremented. Prefect rejects resuming a non-paused run.
- **DAG 4 via deployment (`suspend` mode):** triggered with `prefect deployment run`, approval run reached `Suspended`, process exited, re-executed on resume, order `shipped` with tracking.
- **All four DAGs on the docker work pool:** one container per flow run (named after the run, auto-removed). DAG 3 wrote to Postgres from inside the container; DAG 1 wrote Parquet to the host through the bind mount; DAG 4 paused *inside* the container and was resumed by the approval service across the container boundary; DAG 4 in `suspend` mode exited its container and rehydrated persisted task state in a new one (`Cached(type=COMPLETED)`).
- **All five deployments triggered with zero parameters:** DAG 1 wrote Parquet using the runner's `ETL_*` env; DAG 2 completed 30 fan-out items; DAG 3 generated distinct ids across consecutive runs (`PAY-942B8ED0D41B`, `PAY-970D2DE6D4F8`) with no duplicate collision; DAG 4 generated `ORD-DEE90D0BDCAC` and shipped via the suspend path. Ad-hoc `python dagN_*.py` still works, and explicit parameters are still honoured.
- **Approval-flow failure fails closed:** the first suspend attempt (before `as_subflow=False`) crashed the approval run. The parent correctly treated a non-`Completed` child as `expired`, compensated, and left `ORD-SUSP-1` `cancelled` with both reservations `released` — an unplanned but useful test of `run_approval_deployment`'s fail-closed branch.

### Not yet exercised

Shipping failure after retries → compensation; non-retriable `InvalidAddress`;
duplicate decision; concurrent contention for `RARE-D`'s last 2 units. The
shipping service is 70% success, so failure paths need forced rates or repeats.

---

## Blockers / gotchas for the wider bake-off

### DAG 2's GitHub URL rate-limited us out — now fixed repo-wide

**Resolved 2026-08-04.** `dag2_api_fanout.py` used to default to
`https://api.github.com/orgs/PrefectHQ/repos`, ~31 unauthenticated calls per run
against a **60/hour per-IP** limit. This hit `remaining 0/60` and DAG 2 started
failing with HTTP 403 mid-test — and it was never Prefect-specific, since
`airflow/`, `argo/`, `google-workflows/` and the rest shared the same budget.

Every implementation now points at `fixture-service`'s Books API instead
(`../RUNNING.md` §0b). Prefect's default is:

```
http://fixture-service:8099/books?base=http://localhost:8099
```

The **host** is the compose DNS name because the collection is fetched by
callback-fetch-service, a container. The **`?base=`** rewrites the per-item detail
URLs to `localhost`, because Prefect's fan-out runs on the host and cannot resolve
`fixture-service`. Drop `?base=` for `deploy_docker.py`, whose flow runs are
containers.

### `host.docker.internal` in compose is pinned to the finch address

`docker-compose.yml` pins `extra_hosts: ["host.docker.internal:192.168.5.2"]` for
finch/Lima. On Podman that address is wrong — inside the container
`host.docker.internal` → `192.168.5.2` (unreachable, times out) while
`host.containers.internal` → `10.255.255.254` (correct). Hence
`PREFECT_RESUME_API_URL`'s Podman default. Anything host-targeted on Podman must
use `host.containers.internal`.

---

## Fixes applied (2026-07-27)

All four DAGs were non-functional as committed:

1. **`BAKEOFF_NS` schema isolation wired in** (`dag1`/`dag3`/`dag4`) — previously wrote unqualified names into `public`, where DAG 3/4's seeded tables don't exist. DAG 3/4 now fail fast with the `just seed` hint; DAG 1 self-creates.
2. **`unmapped()`** added to the `.map()` calls in DAG 1 and DAG 2.
3. **DAG 4 FK ordering** — reservation rows were inserted before the `orders` upsert they reference, so any new `order_id` failed the non-deferrable FK. Upsert now runs first.
4. **Sample inputs corrected to match seed data** — were `ACC-SRC-001` / `CUST-001` / `SKU-A`, none of which exist.
5. **DAG 3 non-retriable classification implemented** — `PaymentDeclined` was documented as non-retriable but had no `retry_condition_fn`, so declines were retried 5×.
6. **DAG 4 approval converted to `pause_flow_run()` + real resume URL** — replacing the polling loop and the dead `http://localhost:0/noop` placeholder. Polling kept behind `APPROVAL_WAIT_MODE=poll`. Also removed the spurious 500 from `/decide`.
7. **Spec concurrency caps enforced** — DAG 1 (10) and DAG 2 (20) via bounded task runners; previously unbounded.
8. **`approval_timeout` / `approval_poll_interval` plumbed through `order_fulfillment`** so the timeout→compensation edge case is testable without editing code.
9. **`serve_all.py` added + `APPROVAL_WAIT_MODE=suspend`** — registers all five deployments and implements true `suspend_flow_run()` suspension, including the `as_subflow=False` requirement and a fail-closed branch when the approval run doesn't complete.
10. **`Dockerfile` + `deploy_docker.py` added** — all four DAGs on a container-per-run `docker` work pool, verified end-to-end on Podman including suspend/resume across containers via bind-mounted result storage.
11. **Deployment-friendly flow defaults** — DAG 1's paths read from `ETL_*` env vars; DAG 3's `payment_id` and DAG 4's `order_id` generate a fresh id when omitted (static deployment parameters otherwise made every re-run an idempotent no-op); DAG 3/DAG 4 gained working defaults for their other required parameters so a deployment run needs no arguments.

Repo-level fixes from the same session: `pandas`/`requests` added to
`pyproject.toml` (9 files imported them undeclared), `POSTGRES_PORT=54321` added
to `.envrc`, `just seed` / `just rebuild` / `just psql` recipes added, and
`.gitignore` entries for `.venv/`, `.ruff_cache/`, `.prefect/`,
`presentation/site/`.

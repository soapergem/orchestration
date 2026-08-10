# Google Workflows

Google Cloud Workflows implementation of the four bake-off DAGs. See
`../README.md` for the DAG specs, `../RUNNING.md` §10 for setup, and
`../deployment.md` for how workflow code is installed across the eleven tools.

- **Definition style:** declarative YAML, one file per DAG, subworkflows for DAG 2/4
- **Wait mechanism:** `events.create_callback_endpoint` + `events.await_callback` —
  a genuinely suspended execution, no poller, up to one year
- **Engine:** fully managed. **It runs no code of your own**, so every step body
  is an HTTP call
- **Schema namespace:** `BAKEOFF_NS=google_workflows` → `google_workflows_dag1` / `_dag3` / `_dag4`
- **Database:** Neon (shared with Step Functions — the two orchestrators that
  need a publicly reachable Postgres)

---

## Launch

**Status: all four DAGs verified end to end on 2026-08-06**, in a throwaway
GCP project in `us-central1`, against Neon and the mock services on the arm64
Kubernetes cluster.

### 1. Prerequisites

```bash
# Mock services + fixture, publicly reachable (RUNNING.md 7c-i)
cd shared-services/deploy
KCTX=$KCTX PUBLIC_DOMAIN=$PUBLIC_DOMAIN GOOGLE_SA_KEY_FILE=/tmp/gcp-resume-key.json ./deploy-backbone.sh

# Neon: define the function, then create this runner's schemas
psql "$NEON_DATABASE_URL" -f shared-services/init-db.sql
psql "$NEON_DATABASE_URL" -c "SELECT bootstrap_bakeoff('google_workflows');"
```

### 2. Deploy

The image must exist before Cloud Run is created, and the Artifact Registry API
must exist before the image can be pushed — so the first deploy is three steps,
not one:

```bash
cd terraform/gcp
terraform init
terraform apply \
  -target=google_project_service.required -target=google_artifact_registry_repository.images
./scripts/build-push-task-service.sh   # PROJECT_ID from .envrc
terraform apply
```

Afterwards a code change is just `./scripts/build-push-task-service.sh &&
terraform apply` — the Cloud Run resource hashes `app.py`, so a new revision
rolls whenever the handlers actually change.

### 3. Run

```bash
export NEON_DATABASE_URL='postgresql://…'      # DAG 3/4 take db_config as an execution input
cd terraform/gcp/scripts
./run-workflow.sh dag1
./run-workflow.sh dag2
./run-workflow.sh dag3 --force-outcome declined
./run-workflow.sh dag4
```

### Knobs

| What | How |
|---|---|
| DAG 3 gateway outcome | `--force-outcome success\|declined\|server_error\|timeout` (per execution) |
| DAG 3 failure branches | `from_account: ACC-004` (suspended), `amount: 99999` (insufficient) |
| DAG 4 approval outcome | `AUTO_DECIDE_ACTION` on the approval service, or decide by hand within its 10s window |
| DAG 4 validation failure | `customer_id: CUST-99` (inactive) |
| DAG 2 fan-out width | `?per_page=N` on the fixture URL (default 30 here, service default 5) |

### Re-running

`run-workflow.sh` generates a fresh `payment_id` / `order_id` per execution, so
re-runs never collide with the idempotency keys. Pass `--order-id` to target a
specific order and exercise the idempotent-reservation path deliberately.

---

## What this implementation demonstrates

- `events.create_callback_endpoint` / `events.await_callback` — suspension with
  no runtime cost, the flagship capability
- `parallel` with `for` over a runtime list — genuine dynamic fan-out, with
  `concurrency_limit` for the spec's caps
- `try` / `retry` / `except` with a custom `predicate` subworkflow
- Subworkflows (`reserve_inventory_subworkflow`, `shipping_subworkflow`,
  `saga_compensation`) for DAG 4's three-part structure
- `auth: type: OIDC` for calling a private Cloud Run service
- Config via `user_env_vars` + `sys.get_env()`, rather than templating

## Findings

### The engine runs no code, and that is the whole story

Every other orchestrator here executes your Python. Workflows executes YAML that
can only make HTTP calls. So this implementation needed a **14-route HTTP
service** (`shared-services/gcp-task-service/`) before a single DAG could run —
unzip, load-csv, execute-sql, convert-to-parquet, validate-payment, the payment
gateway, notify, validate-order, reserve/release-inventory, update-order-status,
record-approval-decision. That service is a Cloud Run deployment with its own
image, IAM, and lifecycle.

`deployment.md` calls Google Workflows "the lightest" to deploy, and for the
*workflow definition* that is true: one `gcloud` command or one Terraform
resource, seconds, no build. The honest accounting is that the deployment cost
did not disappear, it moved. **Scoring note:** cheap to deploy, expensive to
*build*, and the task layer is exactly the thing the managed engine was supposed
to save you.

### Suspension really is free, and it is the best in the comparison

DAG 2 and DAG 4 suspend on a callback endpoint. There is no worker, no poller, no
billed step, and no timeout beyond the one you set (up to a year). Airflow's
triggerer keeps an async loop alive; Dagster bridges two jobs with a sensor;
Luigi cannot suspend at all. Only Temporal matches it, and Temporal needs a
worker fleet to do so.

The catch is the return path: a callback endpoint lives on
`workflowexecutions.googleapis.com` and **rejects unauthenticated POSTs**, so
whatever resumes the workflow needs Google credentials. That is why the resume
broker grew a fifth provider (`google_workflows`, alongside `stepfunctions`,
`http_callback`, `kestra`, `conductor`) that mints an OAuth2 access token — an
*access* token, not an OIDC ID token, which is what the workflow itself uses to
call Cloud Run. Same stack, two token types, opposite directions.

Verified empirically: `roles/workflows.invoker` **does** carry the callback
permission (no 403 on resume).

### Retry classification is HTTP status and nothing else

A retry predicate can see the response code and message. There are no exception
types to match on, so retriability has to be *encoded into the API*: the
simulated gateway returns **402** for a decline (non-retriable, routed straight
to failure handling) and **503/504** for transient faults (retried with backoff).
Compare Temporal and Prefect, which classify on the exception class, and Airflow,
which needs a `retry_condition_fn`.

This works, and it is arguably cleaner. But it means the classification lives in
the service, not the workflow — and a service that returns 500 for everything
gives the orchestrator nothing to work with.

### The expression language is small, and its edges are sharp

Seven defect classes surfaced, none of which any amount of reading would have
caught. In rough order of nastiness:

1. **A TypeError in a retry predicate replaces the original error.**
   `"TimeoutError" in e.message` is invalid — `in` requires a dict or array, not
   a string — so the predicate raised, and *that* error was delivered to
   `except` in place of the HTTP error. The replacement has no `code` key, so the
   decline branch died with `KeyError: key not found: code`. A bug in error
   handling wearing the costume of a bug in the error. Use `text.match_regex()`.
2. **`list.concat(list, val)` appends one value**; it is not concatenation.
   `list.concat(items, [item])` silently builds a list of one-element lists that
   only fails much later, where the elements are used as dicts.
3. **A `switch` with `next:` skips everything between it and its target.** Two
   steps that built `items_summary` sat in that gap, unreachable on both
   branches. Unset variables are **not** an error — they serialize as `null` — so
   the only symptom was a 422 from a service on another continent.
4. **`{}` is not a literal.** `${default(x, {})}` fails at deploy time; bind an
   empty map as plain YAML first.
5. **`shared:` requires the variable to already exist**, or deployment fails with
   "symbol is not a variable name".
6. Foreign keys still bite: `customer_id` was never threaded into the reserve
   subworkflow, so every reservation violated `orders_customer_id_fkey`.
7. Nothing ever wrote the `approval_requests` audit row — the handler only
   updated, and the workflow never sent the id.

Four of the seven are **deploy-time** failures, which is genuinely good: the
parser is strict and rejects bad YAML before it can run. The other three were
runtime, and two of those pointed somewhere other than the actual cause.

### Errors deserve their own note

The error map's shape depends on where you catch it. In a retry predicate you get
`code`, `message`, `body`, `headers`. In an `except` after that predicate
declines, you may get something else entirely. All 37 `e.code` / `e.message`
accesses in this tree now go through `default(map.get(e, …), …)` for that reason.

### Terraform: `templatefile()` is the wrong tool

The Workflows language uses `${…}` for its own expressions, which is exactly
Terraform's interpolation syntax — running these YAMLs through `templatefile()`
makes Terraform try to evaluate `${input.zip_url}` and fail. Environment-specific
values arrive as **`user_env_vars`** (provider v6.50+) read with `sys.get_env()`,
which also keeps the same file deployable by plain `gcloud`. This is the opposite
of the Step Functions path, where ASL JSON has no `${}` and `templatefile()` is
exactly right.

Each apply mints a **server-side revision**; in-flight executions stay on the
revision they started with.

### Two deploy-ordering traps

- The **Artifact Registry API** must be enabled before the image push, but the
  API enablement lives in the same Terraform that needs the image. Break the
  cycle with `apply -target` on the APIs first.
- The **Workflows service agent does not exist immediately** after the API is
  enabled. The first apply created 1 of 4 workflows and failed the rest with
  "Workflows service agent does not exist"; a plain re-apply fixed it.

### Verified behaviour

| Scenario | Result |
|---|---|
| DAG 1 fan-out | 3 CSVs in parallel (orders 10, customers 5, products 5) → 10-row `combined_report` → Parquet in GCS |
| DAG 1 schema | `google_workflows_dag1` self-created with all 4 tables |
| DAG 2 suspend/resume | native callback; broker recorded `provider=google_workflows`, `resume_count=1` |
| DAG 2 fan-out | 30 items, 30 successful, 0 failed |
| DAG 3 success | $100 moved, ACC-001 5000 → 4900, ACC-002 3000 → 3100 |
| DAG 3 decline | 402 → **no retries** → `PAYMENT_DECLINED` recorded |
| DAG 3 timeout | 504 → retried with backoff → `GATEWAY_ERROR` recorded after exhaustion |
| DAG 4 happy path | reserve → approval callback → ship → `shipped` with tracking |
| DAG 4 saga | order `cancelled`, both reservations `released`, inventory restored to 50/100 |
| DAG 4 audit row | `approval_requests` row written with approver and decision |
| Isolation | Step Functions' `public.accounts` untouched throughout (4750/250) |

### Not yet exercised

- **Approval rejection and timeout as distinct paths.** The saga was observed via
  an approval *error*, not a deliberate rejection or expiry.
- **Shipping failure compensation.** `SHIPPING_SUCCESS_RATE=0.70` never tripped
  across these runs, so `InvalidAddress` (non-retriable) vs `ShippingTimeout`
  (retriable) is untested.
- **The concurrency caps actually throttling.** `concurrency_limit` 10/20 is set
  and deploys, but the fixtures only produce 3 and 30 items.
- **Last-unit contention** (`RARE-D`) and inactive-customer validation.
- **Long suspensions.** Callbacks are timed out at 60s (DAG 2) and 180s (DAG 4);
  the up-to-a-year claim is taken from documentation, not measured.
- **Cost at volume.** Everything here fits inside the free tier.

## Fixes applied (2026-08-06)

Everything below was found by running it; the implementation had never executed.

1. **Four invented services** — the YAMLs called `db-proxy-service`,
   `processing-service`, `payment-gateway`, and `notification-service`, none of
   which existed. Replaced by `shared-services/gcp-task-service/` (14 routes,
   ported from the Step Functions lambdas so the two serverless paths stay
   comparable).
2. **`{}` literal** in two expressions — deploy-time parse error.
3. **`shared:` variables** `load_results` / `api_results` never initialised.
4. **`in` on a string** in all four files — 13 substring tests moved to
   `text.match_regex()`.
5. **`list.concat` misuse** in 3 places — nested arrays.
6. **Unreachable steps** — `items_summary` built after the branch that skipped it.
7. **`customer_id` not threaded** into `reserve_inventory_subworkflow` — FK violation.
8. **No approval audit row** — `/record-approval-decision` now upserts, and the
   workflow binds `approval_request_id` to a variable so both steps share it.
9. **37 unguarded `e.code`/`e.message` accesses** wrapped in `map.get` defaults.
10. **Placeholder inputs** — the ZIP URL, fixture URL, and DAG 3/4 sample ids all
    pointed at things that did not exist; now real seeded fixtures.
11. **Concurrency caps** — `concurrency_limit` 10 (DAG 1) / 20 (DAG 2).
12. **Resume authentication** — the `google_workflows` provider in the resume
    broker (`shared-services/{callback-fetch,approval}-service/app.py`), plus the
    `orch-resume` service account, key, and `deploy-backbone.sh` wiring.
    `google-auth` alone is not enough: `google.auth.transport.requests` needs
    `requests`, whose absence crash-looped both pods.

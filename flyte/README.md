# Flyte

Flyte 1.16 implementation of the four bake-off DAGs. See `../README.md` for the
DAG specs and `../RUNNING.md` for cross-cutting setup (§7 Kubernetes shared
setup, §9 the Flyte install).

- **Definition style:** typed Python — `@task` / `@workflow` / `@dynamic`, with
  every input and output a concrete dataclass (`types.py`)
- **Wait mechanism:** polling for DAG 2/4; native `wait_for_input` suspend is
  documented in the docstrings but **not implemented** (see Findings)
- **Engine:** `flyte-core` on Kubernetes; no local-run path
- **Schema namespace:** `DBConfig.namespace` → `flyte_dag1` / `_dag3` / `_dag4`

---

## Launch

**Status: all four DAGs run green on the arm64 cluster (2026-08-06),
`flyte-core-v1.16.8` on arm64.** Verified end to end:

| DAG | Execution | Evidence |
|---|---|---|
| 1 | `agdh8f6dx25zrwcqwcln` | 3 CSVs fanned out via `@dynamic`; `flyte_dag1` holds customers 5 / products 5 / orders 10 / `combined_report` 10; Parquet at `s3://my-s3-bucket/ae/…/combined_report.parquet` |
| 2 | `axvvwn9rqgt6jvkbncdf` | async submit → poll → 30-item fan-out → combined; `total_items: 30, successful: 30, failed: 0` against the fixture Books API |
| 3 | `a8fh56gj5zfpddbnw95v` | ACC-001 5000 → 4900, ACC-003 0 → 100, transaction `completed` with a gateway id, in `flyte_dag3` |
| 4 | `a4s9gcxrq95pdvdt2vr5` | full approval path: reserved → `pending_approval` → `APR-89633B9CD309` approved by `auto-decider` → `shipped` with tracking, total 559.97, both reservation rows present |

Those four ran under `flytesnacks:development`, the chart's default project. The
install was retrenched onto a single `bakeoff` project on 2026-08-12 (see
"Why are there so many Flyte namespaces?"), so **the DAGs need re-registering
under `bakeoff` before the next run**, and the executions above are only visible
in the console if `flytesnacks` is un-archived. Their evidence stands — nothing
about the DAG code changed.

**Seven distinct defect classes across ~25 sites** were fixed getting here — the
implementation had never been executed. (Counted by class because the same
mistakes recurred: 5 promise-output constructions, 4 conditional predicates, 5
missing ordering edges, and so on — see `../comparison.md` §Implementation
Friction for the breakdown.) They are individually documented under Findings; the ones
with the widest reach are that statement order in a `@workflow` implies nothing,
that `@dynamic` re-resolves images inside the task pod, and that local filesystem
paths do not survive a task boundary.

Three scripts, in order:

```bash
cd flyte
KCTX=my-arm64-cluster ./deploy-flyte.sh        # postgres + minio + bucket + flyte-core
KCTX=my-arm64-cluster ./register.sh dag3_payment.py
KCTX=my-arm64-cluster ./run.sh dag3
```

`register.sh` and `run.sh` both execute the client **inside the cluster** — see
"Registration only works from inside the cluster" below.

Prerequisites beyond the install:

1. **A task image for the cluster's architecture, in a registry it can pull.**
   Built by an in-cluster buildah Job (90s, native arm64) and pushed to
   `$ECR/orch-bakeoff-flyte` (set `ECR` in `.envrc`). Building on
   the x86 workstation is not possible: it needs qemu binfmt handlers and
   rootless podman cannot register them (`mount: permission denied`).
2. **An ECR pull secret in the task namespace**, attached to its default
   ServiceAccount. the arm64 cluster's managed `k8s-ecr-login-renew-docker-secret` lives
   only in `default` with a cronjob scoped there, so this one is minted separately
   (`aws ecr get-login-password`, 12h validity — re-mint when pulls start failing).
3. **The backbone aliased into the task namespace** —
   `shared-services/deploy/alias-backbone.sh` with
   `WORKFLOW_NS=bakeoff-development`, because task pods run there, not in
   `flyte`.

```bash
kubectl --context "$KCTX" -n flyte port-forward svc/flyteconsole 8083:80   # UI
```

---

## Why are there so many Flyte namespaces?

A stock install creates **ten** namespaces, and it is not leftovers or a broken
install. Nine of them are Flyte's tenancy model expressed as Kubernetes
namespaces:

```
flyte                        <- the control plane (flyteadmin, flytepropeller, ...)
flytesnacks-development      <- project x domain
flytesnacks-staging
flytesnacks-production
flyteexamples-development
flyteexamples-staging
flyteexamples-production
flytetester-development
flytetester-staging
flytetester-production
```

**This repo no longer runs the stock set** (changed 2026-08-12):
`deploy-flyte.sh` seeds a single `bakeoff` project, so the arm64 cluster has
`flyte` + `bakeoff-{development,staging,production}` — four namespaces, and the
nine above were archived and deleted. The rest of this section explains where
they came from and how the trim was done, because neither is obvious and the
teardown is not what you would guess.

### Where they come from

Two chart defaults multiply together:

| Setting | Default | Where |
|---|---|---|
| `flyteadmin.initialProjects` | `flytesnacks`, `flytetester`, `flyteexamples` | chart values |
| `configmap.domain.domains` | `development`, `staging`, `production` | chart values → `flyte-admin-base-config`/`domain.yaml` |

3 projects × 3 domains = 9 namespaces, each named `<project>-<domain>`.

`initialProjects` renders a `seed-projects` **init container** on the flyteadmin
Deployment (`templates/admin/deployment.yaml`) running
`flyteadmin migrate seed-projects <project>...`. Both settings are ordinary Helm
values — nothing about the nine is required, and only `flyte` itself is, since it
holds the control plane.

The `syncresources` Deployment creates them. It runs
`flyteadmin clusterresource run`, a reconcile loop that walks every
project/domain pair and applies the `clusterresource-template` ConfigMap with
`{{ namespace }}` substituted. That template holds exactly two entries:

- `aa_namespace.yaml` — the Namespace itself
- `ab_project_resource_quota.yaml` — a `project-quota` ResourceQuota

The `aa_` / `ab_` prefixes force apply order so the namespace exists before the
quota that targets it.

### Why Flyte works this way

**Project + domain is Flyte's isolation unit**, and it maps onto a Kubernetes
namespace so each pairing gets its own resource quota, service account, and RBAC
boundary. A workflow registered to `bakeoff:development` runs its task pods
in the `bakeoff-development` namespace — *not* in `flyte`, which holds only
the control plane. "Project" is roughly a team or application; "domain" is the
promotion stage. Domains are *global* — every project gets all of them, so the
count is strictly multiplicative.

### Trimming them

Set the two values. Either alone helps; both give the minimum of one namespace:

```bash
--set-json 'flyteadmin.initialProjects=["bakeoff"]' \
--set-json 'configmap.domain.domains=[{"id":"development","name":"development"}]'
```

`deploy-flyte.sh` now passes the first (`FLYTE_PROJECT`, default `bakeoff`) and
leaves the three domains alone — they cost nothing and staging/production are the
one part of Flyte's tenancy model worth having on display in a comparison deck.

`initialProjects: []` is legal — the init container is dropped entirely — but
then no project exists to register against, and you must
`flytectl create project` and wait out a sync interval before anything works.

**Trimming does not clean up an existing install**, and this is the part that
surprises:

- `seed-projects` only *adds*. Re-running Helm with a shorter list leaves the old
  projects in the metadata DB.
- `kubectl delete ns` alone **loses** — `syncresources` recreates the namespace
  within its 5m `refreshInterval`.
- What actually works is **archiving the project first**. flyteadmin's
  clusterresource data provider lists projects with a
  `state NotEqual ARCHIVED` filter, so an archived project drops out of the sync
  walk and the namespace delete sticks. flyteadmin has no *delete*-project API;
  archive is the supported route. The filter is visible in the `syncresources`
  pod's own log — it issues
  `SELECT * FROM "projects" WHERE state <> 1 ORDER BY created_at desc`, and
  `1` is `Project_ARCHIVED`.

Done here (2026-08-12) with the flyteadmin HTTP gateway, since `flytectl` was not
installed — `flytectl update project --project X --archive` is equivalent:

```bash
kubectl --context "$KCTX" -n flyte port-forward svc/flyteadmin 18088:80 &
for p in flytesnacks flytetester flyteexamples; do
  curl -s -X PUT "http://localhost:18088/api/v1/projects/$p" \
    -H 'Content-Type: application/json' \
    -d "{\"id\":\"$p\",\"name\":\"$p\",\"state\":\"ARCHIVED\"}"
done
kubectl --context "$KCTX" delete ns \
  flytesnacks-{development,staging,production} \
  flyteexamples-{development,staging,production} \
  flytetester-{development,staging,production}
```

Archiving also removes the project from `GET /api/v1/projects` and therefore from
the console's project picker. The executions underneath are **not** deleted —
they stay in the metadata Postgres — but they become unreachable in the UI until
you PUT the state back to `ACTIVE`. Nothing warns you about this.

### What it means for this bake-off

- **Task pods do not run in the `flyte` namespace.** When the bake-off DAGs are
  registered, their pods land in `bakeoff-development`, so the backbone's
  compose DNS names (`postgres`, `shipping-service`, …) have to resolve *there* —
  see `../RUNNING.md` §7c on aliasing the backbone.
- **Flyte handles this better than Argo does.** Its DB settings are a typed
  workflow *input* (`DBConfig`), so a run can be pointed at
  `postgres.orchestrators.svc.cluster.local` without touching workflow code.
  Argo's DAG YAML hard-codes `PGHOST` as a literal env value, so it needs the
  namespace aliases. That difference is worth a line in `../comparison.md`.
- **The nine were empty and free.** Each held only a ResourceQuota — no pods, no
  cost. They were noise, not overhead; an earlier install carried the same nine
  unused for 46 days.
- **They are not chart-managed.** `helm uninstall flyte` leaves project
  namespaces behind, which is why `../RUNNING.md`'s teardown deletes them
  explicitly. That list is now the three `bakeoff-*`.
- **Renaming the project is not free** — resolved 2026-08-13, but worth reading
  before renaming anything again. Registrations are per project/domain, so the
  four DAGs had to be re-registered under `bakeoff`, and the two per-namespace
  prerequisites lived in `flytesnacks-development` and went with it.

  The failure mode was ugly: the console showed the `bakeoff` project with **no
  workflows**, while 74 registrations sat in `flytesnacks` — which had been
  *archived*, and the UI lists only active projects. So the workflows had not
  been lost, they were invisible. Meanwhile `register.sh` and `run.sh` already
  defaulted to `PROJECT=bakeoff`, so nothing pointed at the discrepancy.

  Then the first run failed with `ImagePullBackOff`, because **Flyte's
  `clusterresource-template` provisions only a Namespace and a ResourceQuota —
  never image-pull credentials.** A brand-new project namespace therefore cannot
  pull the task image at all, and the launcher job in `flyte` succeeds while the
  task pod hangs, which reads as "the run started" in the console. Pod specs are
  immutable, so fixing the ServiceAccount afterwards does **not** rescue an
  already-created pod: delete it and let Flyte recreate it.

  Both are now handled — see Operational notes for the credential, and
  `alias-backbone.sh` for the aliases. All four DAGs verified in
  `bakeoff/development` on 2026-08-13.

---

## What this implementation demonstrates

| DAG | Flyte idioms |
|---|---|
| 1 | `@dynamic` for runtime fan-out over discovered CSVs, `ImageSpec` for declarative per-task dependencies, `FlyteFile`/`FlyteDirectory` typing |
| 2 | `@dynamic` bounded fan-out, `conditional()` for the branch |
| 3 | `conditional()` chains, typed exceptions, `@task(retries=)` |
| 4 | Sub-`@workflow`s as composition units, `conditional()` for approval routing, hand-rolled compensation |

Every task input/output is a `@dataclass_json` dataclass in `types.py` — no
untyped dicts cross a task boundary. That is the single biggest stylistic
difference from the other Python implementations and the main thing DAG 1 and
DAG 4 are here to show.

---

## Findings

Observations bearing on `../comparison.md`.

### Statement order in a @workflow means nothing

The most consequential finding, and it is silent rather than loud. A `@workflow`
body builds a graph; Flyte derives edges **only from data dependencies**. Code
that reads sequentially runs in parallel if the second statement does not consume
the first one's output:

```python
load_results   = load_all_csvs(csv_paths=csv_paths, db_config=cfg)
transform_result = run_sql_transform(db_config=cfg)   # consumes only `cfg`!
```

Verified on the cluster: `run_sql_transform` started **alongside** `unzip_file`
and died with `relation "orders" does not exist`. The fix is flytekit's explicit
ordering operator:

```python
load_results >> transform_result
```

This also invalidated a comfortable assumption: four assigned-but-unused task
results (`db_result`, two `order_update`s, `reservation`) were previously
described here as a deliberate ordering idiom. They are not — assigning a result
to an unused variable creates a *node* but no *edge*, so DAG 3 could notify a
customer before the ledger write and DAG 4 could ship before reserving stock.
All four are now real `>>` edges, and ruff reports zero warnings where it used to
report four.

For scoring: this is a genuine hazard of the implicit-dependency model. Airflow
and Argo make you declare edges, so the mistake is impossible; Prefect and
Temporal execute eagerly, so sequential code *is* sequential. Flyte looks like
Python but is not, and nothing warns you.

### @dynamic re-resolves images at run time, in the task pod

`FLYTE_TASK_IMAGE` in the registration client is not enough. A `@dynamic` task
builds its sub-graph **while executing**, so it re-evaluates the module's
`ImageSpec` inside its own pod — where that env var was absent — and emitted
`csv-etl:<hash>` for every fan-out sub-node. cri-o rejected it legibly
("short name mode is enforcing ... returns ambiguous list"); on a
Docker-shim cluster it would have been a confusing failed pull. The image
override therefore belongs in propeller's `default-env-vars` too, alongside the
blob credentials, which is what `deploy-flyte.sh` now does.

### Local paths do not survive a task boundary

`unzip_file` extracted to `/tmp/csv_extract` and returned `List[str]`. Each load
task runs in a **different pod**, so those paths do not exist: the fan-out died
with `No such file or directory: /tmp/csv_extract/customers.csv`. This is the
same lesson `../prefect/README.md` records for its Docker work pool, and it is
exactly what `FlyteFile` is for.

The files now travel as blobs via a small typed carrier
(`ExtractedCSV{table, file: FlyteFile}`) — the table name rides alongside because
it is derived from the original filename, which a blob round-trip does not
promise to preserve. DAG 1 also had to learn to fetch its archive over HTTP
(`http://fixture-service:8099/sample-data.zip`), since no local path is
meaningful to a task pod; that mirrors Argo's `zip-url` parameter.

Related, at the workflow boundary: `build_etl_output` must take the `FlyteFile`
itself, not `parquet_file.path`. Inside a `@workflow` that attribute is part of a
blob-typed Promise, and registration rejects the binding
("output variable 'convert_to_parquet.o0' has type [blob:{}] ... assigned to
[simple:STRING]").

### Nested dataclass predicates break conditionals — four times

`conditional().if_(...)` cannot evaluate a predicate over promise attributes.
Every occurrence had to move into a `bool`-returning task:

| Workflow | Predicate that failed | Extracted task |
|---|---|---|
| `payment_workflow` | `validated.validation.is_valid.is_true()` | `is_payment_valid` |
| `order_fulfillment_workflow` | `validated.validation.is_valid.is_true()` | `is_order_valid` |
| `approval_then_ship` | `decision.decision == "approved"` | `is_approved` |
| `order_valid_path` | `validated.total_amount >= validated.approval_threshold` | `needs_approval` |

The errors are misleading: `MismatchingTypes` claims the *upstream task's* output
is BOOLEAN (or FLOAT, or STRING) where the struct is expected, which points at the
wrong node entirely. One also produced `UnreachableNodes` for the branch.

### Remote launches need typed inputs, not plain dicts

`FlyteRemote.execute(inputs={...})` accepts plain dicts for flat dataclasses —
DAG 1 and DAG 3 launched that way — but silently cannot coerce a nested list:
DAG 4 failed with `Type of Val '<class list>' is not an instance of
typing.List[dict]` because `items: List[OrderItem]`. Since every type in
`types.py` is `@dataclass_json`, `run.sh` hydrates the real object with
`cls.from_dict(...)`, which handles arbitrary nesting.

### Registration only works from inside the cluster

`pyflyte register` fast-registers: it tars the code, asks flyteadmin for a signed
upload URL, and PUTs to it. That URL names the **in-cluster** endpoint
(`http://minio.flyte.svc.cluster.local:9000`), so a client on the workstation
does all the real work and then dies:

```
FlyteDownloadDataException / ConnectionError: NameResolutionError(
  "Failed to resolve 'minio.flyte.svc.cluster.local'")
```

The chart does render `storage.signedUrl.stowConfigOverride.endpoint`, but not as
a documented values key, so overriding it means drifting from the chart to
accommodate a client-side limitation. `register.sh` instead runs the client as a
Job in the cluster, where the name resolves. The task image doubles as the client
— it already has flytekit and every DAG dependency.

Two smaller traps in that path:

- **flytekit reaches for the OS keyring.** With none available (a container, or
  WSL2) it fails with `Failed to create the collection: Prompt dismissed`. Set
  `PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring`.
- **Don't `cp -r` a ConfigMap mount.** The mount contains a
  `..2026_08_06_.../` versioned symlink dir; copying it recursively leaks that
  path into the module name and pyflyte fails with `No module named 'flyte.'`.
  Copy `*.py` only.

### Task pods get no blob-store credentials by default

`flytepropeller`'s `plugins.k8s.default-env-vars` defaults to `[]`. Every task pod
therefore starts, tries to download its own code package from minio, and dies:

```
FlyteDownloadDataException: Failed to get data from
s3://my-s3-bucket/... Original exception: Unable to locate credentials
```

It is retried to exhaustion, so the workflow fails with nothing pointing at
configuration rather than the DAG. The fix is three env vars injected into every
task pod, which `deploy-flyte.sh` now sets:

```yaml
default-env-vars:
- FLYTE_AWS_ENDPOINT: http://minio.flyte.svc.cluster.local:9000
- FLYTE_AWS_ACCESS_KEY_ID: minio
- FLYTE_AWS_SECRET_ACCESS_KEY: miniostorage
```

Taken together with the missing minio deployment, the chart's `sandbox` storage
mode gets you a Flyte that installs cleanly and cannot run anything, for two
independent reasons.

### `retries=` does nothing unless the exception is a FlyteRecoverableException

The sharpest finding, and it inverts DAG 3's whole point. flytekit retries **only**
`FlyteRecoverableException`; every other exception is a permanent USER error and
the task's `retries=` count is ignored entirely.

`dag3_payment.py` declared `PaymentGatewayTimeout` and `PaymentGateway5xx` as
"Retriable" in their docstrings but inherited plain `Exception`, so `retries=5`
was inert. Verified on the cluster: a gateway timeout failed the workflow on the
**first** attempt, one pod, no retry — and `PaymentDeclined`, the deliberately
non-retriable one, behaved identically. The retriable-vs-terminal distinction the
DAG exists to demonstrate was a no-op.

Fixed by having the two retriable types inherit `FlyteRecoverableException` and
leaving `PaymentDeclined` a plain `Exception`. Note the asymmetry with the other
tools: Temporal names non-retryable types, Step Functions catches named errors,
Prefect needs a `retry_condition_fn` — Flyte alone encodes it in the exception's
base class, which means the classification lives in the exception hierarchy rather
than at the call site.

### A @workflow body cannot construct its own output dataclass

A `@workflow` is a DSL that builds a graph; it does not execute eagerly, so every
task result inside it is a `Promise`. Constructing a dataclass from Promises fails
at **registration** time with:

```
FlyteValidationException: Failed to bind output o0 ...
can not serialize 'Promise' object
```

This was present in **five** places — `csv_etl_pipeline`, both DAG 3 branch
sub-workflows, and both DAG 4 ones — i.e. the whole implementation was written as
if workflows were ordinary Python. The fix is a small `@task` that assembles the
output from real values (`build_etl_output`, `build_payment_output`,
`build_order_output`).

Related: **nested promise attribute access breaks conditionals.**
`conditional().if_(validated.validation.is_valid.is_true())` reads naturally and
fails at registration with `MismatchingTypes`, claiming `validate_payment`'s
output is BOOLEAN where the struct is expected — two-level attribute access
confuses branch-node type inference. Branching on a task that returns a plain
`bool` (`is_payment_valid`) sidesteps it.

### The chart configures a blob store it does not deploy

`storage.type` defaults to `sandbox`, which writes a config pointing at
`http://minio.<ns>.svc.cluster.local:9000` — but the chart has **no `minio` key
at all**, and `--set minio.enabled=true` is silently ignored. Flyte stores every
task input, output, and offloaded literal there, so the install comes up entirely
"Running" while being unable to execute a single workflow.

That is the exact state an earlier install sat in for 46 days: flyteadmin healthy,
storage endpoint dangling, `kubectl get flyteworkflows -A` returning
`No resources found`. **Nothing had ever run.** Full write-up in `../RUNNING.md`
§9b; `deploy-flyte.sh` fixes it.

Scoring note: this is a genuine "quick start lies to you" failure. The install
reports success at every layer and the gap is only visible if you cross-check the
storage endpoint against the Services that exist.

### `ImageSpec` silently targets the wrong architecture

`ImageSpec(platform=...)` defaults to `linux/amd64` in flytekit 1.16.26 — it only
picks arm64 when pushing to a *local* registry from an arm64 build host
(`flytekit/image_spec/image_spec.py`). Build on an x86 workstation, deploy to
arm64 nodes, and the image pulls fine, schedules fine, then dies with
`exec format error` — which reads like a broken entrypoint, not an arch mismatch.

All four DAG files now set it from the environment:

```python
platform=os.environ.get("FLYTE_IMAGE_PLATFORM", "linux/amd64")
```

```bash
export FLYTE_IMAGE_PLATFORM=linux/arm64      # match your cluster's nodes
```

Verified: unset → `linux/amd64`, set → `linux/arm64`, on all four images.

### Schema isolation travels as data, not environment

`BAKEOFF_NS` is an env var for every other runner. That does not work here:
**Flyte task pods do not inherit the launching shell's environment**, so an
`os.environ` lookup inside a task always sees the default. Instead
`DBConfig.namespace` carries it as a typed workflow input whose default is
resolved on the *client* at launch time.

This is more verbose than an env var and also strictly better: the namespace (and
the DB host, and everything else) is overridable per execution without touching
workflow code or redeploying. Verified against the live Postgres — all three
`_get_connection` helpers land in the correct `flyte_dagN`, DAG 1 self-creates its
schema, DAG 3/4 fail fast with a `bootstrap_bakeoff` hint.

### Two databases, and passwordless by default

The chart's defaults want **two** databases (`flyteadmin` and `datacatalog`) — the
older recorded install pointed both at `flyteadmin`. And it sets
`db.*.database.passwordPath=""`, so flyteadmin connects with *no password*, which
stock Postgres host auth rejects. `deploy-flyte.sh` sets
`POSTGRES_HOST_AUTH_METHOD=trust` to let the migrations run. Evaluation-grade
only, and worth noting as a rough edge in the chart's defaults.

---

## Not yet exercised

All four happy paths are verified. What remains untested:

- **Saga compensation.** DAG 4's rejection, approval-timeout, and
  shipping-failure paths are all unexercised, so `compensate_order` has never
  run. The shipping service is 70% success by design, so a failure path will turn
  up on its own eventually — but it should be forced deliberately.
- **DAG 3's failure branch** (`payment_failure_path`) and the non-retriable
  `PaymentDeclined` classification. The retriable side is now proven to work
  (`FlyteRecoverableException`), but nothing has confirmed a decline *stops* at
  one attempt.
- **Idempotency / re-run behaviour.** Every run so far used a fresh id.

Still true regardless of execution:

- **DAG 2 and DAG 4 wait by polling, not by suspending.** Both module docstrings
  document `wait_for_input(...)` plus a FlyteAdmin signal endpoint as the
  production approach; neither implements it. The spec calls for a genuine suspend
  on DAG 4, so Flyte's native-suspend tier remains **claimed in comments, not
  demonstrated** — and note the approval service has no `flyte` provider (the enum
  is stepfunctions / http_callback / kestra / conductor), so a real resume would
  need one added, as was done for Kestra.
- **Service URLs are hard-coded module constants** (`APPROVAL_SERVICE_URL`,
  `SHIPPING_SERVICE_URL` in `dag4_order_fulfillment.py`), unlike `DBConfig` which
  is a proper workflow input. Worth converting for consistency.
- **The spec's concurrency caps are absent.** DAG 2 fanned out all 30 items at
  once against a 20 cap; on a two-node cluster that is also a scheduling problem,
  not just a spec deviation.

### Operational notes

- **ECR tokens expire after 12 hours** — this blocked Flyte twice in one day
  before being fixed properly. **Now automated** (2026-08-13): the cluster's
  `k8s-ecr-login-renew` cronjob runs every 6 hours and its `targetNamespace` was
  widened from `default` to
  `default,flyte,bakeoff-development,bakeoff-staging,bakeoff-production`. It
  writes `k8s-ecr-login-renew-docker-secret` into each, and that is the secret
  `register.sh`/`run.sh` reference and each project namespace's `default`
  ServiceAccount carries. The hand-minted `ecr-bakeoff` secret is gone — one
  source of truth, renewed on a schedule.

  **Adding a new project/domain means adding its namespace to that list**, or
  every task pod in it will `ImagePullBackOff`:

  ```bash
  helm upgrade k8s-ecr-login-renew -n default --reuse-values \
    --repo https://nabsul.github.io/helm k8s-ecr-login-renew \
    --set targetNamespace='default\,flyte\,<...>\,bakeoff-newdomain'
  kubectl patch sa default -n bakeoff-newdomain \
    -p '{"imagePullSecrets":[{"name":"k8s-ecr-login-renew-docker-secret"}]}'
  ```

  Task pods run as the namespace's `default` ServiceAccount, which is why
  patching it covers every task without touching pod specs.
- **Rebuild the task image after adding a dependency** — it is baked in, not
  resolved by `ImageSpec` at registration.
- **Task pods are ~70s of overhead each**, so DAG 4's ~10 sequential nodes take
  10+ minutes. Not a defect, but it makes iteration slow.
- **`FIXTURE_BASE_URL` must point in-cluster** (`http://fixture-service:8099`).
  It defaults to the public `https://orch-fixture.$PUBLIC_DOMAIN` when
  `PUBLIC_DOMAIN` is set, which makes DAG 2's detail fetches leave the cluster
  over TLS and fail.
- **Task pods accumulate.** Flyte does not reap Completed pods promptly, and
  Flyte's `project-quota` ResourceQuota requires explicit CPU/memory *limits* on
  any pod you add to a project namespace by hand.

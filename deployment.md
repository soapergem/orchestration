# How workflow code gets installed

`comparison.md` scores what each orchestrator *can do*. This document covers a
question that only shows up in operations: **how does a workflow definition get
from a git repo into a running engine, and what does it cost to add one?**

The answer splits the twelve tools into four models that behave very differently
under change. Everything below is grounded in what this repo actually does —
commands are the ones in `RUNNING.md` and `terraform/aws/`.

---

## Summary

| Tool | Artifact | How it's installed | New workflow costs | New *dependency* costs |
|---|---|---|---|---|
| **Airflow** | `.py` in a DAGs folder | folder scan → serialized into metadata DB | drop the file, ~30s | image rebuild (or managed env update) |
| **Dagster** | Python module exporting `Definitions` | code location loaded by the daemon | add to `Definitions`, reload | image rebuild — per code location |
| **Prefect** | `@flow` function + a deployment record | `serve()` or `.deploy()` to a work pool | register a deployment | image rebuild (Docker pool) |
| **Temporal** | workflow/activity classes in a worker | worker registers them at startup | edit worker, redeploy worker | worker redeploy |
| **Hatchet** | same, via `worker.register_workflow()` | worker registers them at startup | edit worker, redeploy worker | worker redeploy |
| **Luigi** | `Task` classes in a script | *nothing* — you run the script | write a file | whatever your venv has |
| **Kestra** | flow YAML | `kestra flow namespace update` → engine DB | push YAML, no restart | per-task container image |
| **Argo** | `Workflow` / `WorkflowTemplate` CRD | `kubectl apply` | apply YAML | per-step image (or runtime `pip install`) |
| **Flyte** | `@workflow` Python + built images | `pyflyte register` → versioned server-side | register a new version | rebuild that task's `ImageSpec` |
| **Step Functions** | ASL JSON + Lambda zips | `terraform apply` | new `.tf` + state machine | new Lambda layer |
| **Google Workflows** | workflow YAML | `gcloud workflows deploy` or `google_workflows_workflow` → versioned revision | one command | n/a in the engine — but every step body is a service you deploy yourself |
| **Conductor** | workflow JSON **and, separately,** worker code | `PUT /api/metadata/workflow` → server metadata store; workers poll | push JSON, no worker restart | worker redeploy |

The last column is the one that matters. Adding a *workflow* is cheap almost
everywhere; adding a *library* is where the models diverge by an order of
magnitude.

---

## Model 1 — The engine reads your code

**Airflow, Dagster.** The engine imports Python you wrote and discovers
workflows as a side effect.

Airflow scans `AIRFLOW__CORE__DAGS_FOLDER` every ~30 seconds, imports every
`.py` file, finds module-level `DAG` objects, and **serializes** them into the
metadata database. The scheduler and UI read the serialized copy, never your
files. Airflow 3 wraps this in *bundles* — the folder is the default one
(`dags-folder`), and a Git bundle
(`airflow.providers.git.bundles.git.GitDagBundle`, configured via
`[dag_processor] dag_bundle_config_list`) fetches a repo directly, so DAG files
need never enter the image. Managed variants replace the bundle with a bucket:
MWAA syncs from S3, Cloud Composer from GCS.

Dagster is the same idea with an explicit entry point rather than a scan:
`dagster dev -m dagster_bakeoff.repository` loads one module that exports a
`Definitions` object listing every job and sensor. In production each *code
location* is its own gRPC server with its own image — which makes Dagster the
only tool in this family where dependency isolation between workflow groups is
a first-class deployment concept rather than a workaround.

**What this repo proves about the model:** discovery is not the same as
importability. Airflow's dag-processor puts the DAGs folder on `sys.path`; the
triggerer does not, so `from triggers.approval_trigger import ApprovalTrigger`
succeeded during parsing and failed at resume time, hanging DAG 2 and DAG 4 in
`deferred` **forever** with one line in a log nobody reads. Hence
`PYTHONPATH` in `airflow/env.sh`. Dagster's failure was structural in a
different way: naming the directory `dagster/` shadowed the installed library and
no code location would load at all.

Both also depend on ambient state that isn't the code: `DAGSTER_HOME` (without
it sensors get a fresh instance each tick and never bridge runs) and
`AIRFLOW_HOME`.

---

## Model 2 — The worker *is* the deployment

**Temporal, Hatchet, Prefect (`serve`), Luigi.** Nothing about a workflow is
stored server-side ahead of time. A worker process starts, announces what it can
execute, and polls.

`temporal/worker.py` registers all four workflows plus their activities;
`hatchet/worker.py` does it explicitly per workflow
(`worker.register_workflow(csv_etl_wf)`). Adding a fifth DAG means editing the
worker and restarting it — there is no `deploy` step, and equally no way to add a
workflow without shipping a new worker. Routing is by **task queue**, which
doubles as the isolation unit: two workers with different images on different
queues is how you get per-workflow dependencies.

Prefect straddles this and Model 3, and this repo has all three of its tiers
working:

| Tier | Mechanism | Isolation |
|---|---|---|
| ad-hoc | `python dag3_payment.py` | none |
| `serve_all.py` | `to_deployment()` + `serve()`, no work pool | process per flow run |
| `deploy_docker.py` | `.deploy()` to a Docker work pool | **container per flow run** |

The Docker tier is what substantiates Prefect's dependency-isolation claim, and
note what it does: `build=False, push=False`, because the image already contains
the flow code — so editing a DAG means rebuilding the image by hand. Isolation is
per flow *run*, not per task.

Luigi is the degenerate case: no registration, no server, no daemon required.
`uv run python dag1_csv_etl.py` and the DAG exists for as long as the process
does. Maximum simplicity, zero deployment story — consistent with its 38/100.

**The Temporal-specific cost:** because history is event-sourced and replayed,
changing a workflow's code while runs are in flight can break determinism.
Temporal has versioning/patching APIs for exactly this, and it is the only tool
here where *redeploying* a workflow definition is a correctness concern rather
than a logistics one. That cuts both ways — it is the same property that makes
its audit trail replayable.

---

## Model 3 — Push a definition to a control plane

**Step Functions, Google Workflows, Flyte, Argo, Kestra, Conductor.** The
definition is uploaded and stored by the engine. Your repo is the source, not the
runtime.

**Step Functions** is the most heavily built of the twelve. `terraform apply`
zips each DAG's `lambdas/` directory (`archive_file`), creates the functions, and
then `templatefile()` injects the resulting Lambda ARNs into the ASL JSON before
creating the state machine. Dependencies are **Lambda layers** built by shell
scripts beforehand — `scripts/build-psycopg2-layer.sh` and the pyarrow layer
(~140 MB unzipped, stacked under the 250 MB limit). So "add a dependency" means
build a layer, version it, attach it. Heavy, but every piece is versioned and
rollback is a Terraform revert.

**Google Workflows** looks like the lightest: `gcloud workflows deploy
payment-processing --source=dag3_payment.yaml`, or a `google_workflows_workflow`
resource in Terraform. One command, seconds, no build, and each deploy mints a
server-side **revision** that in-flight executions stay pinned to.

Two corrections, both learned by actually deploying it (`google-workflows/README.md`):

**`templatefile()` is the wrong tool here**, even though it is exactly right for
the ASL JSON next door. The Workflows language uses `${…}` for its own
expressions, so Terraform tries to interpolate `${input.zip_url}` and fails.
Environment-specific values go in **`user_env_vars`** (provider v6.50+), read with
`sys.get_env()` — which has the side benefit that the same file still deploys by
hand with `gcloud --set-env-vars`.

**The deployment cost did not vanish, it moved.** No code executes in the engine,
so there is nothing to have dependencies *for* — but there is also nothing to
execute your logic. This repo needed a **14-route Cloud Run service** before a
single DAG could run, which is its own image, IAM, and lifecycle. Cheap to
deploy, expensive to build. That trade is the honest headline, and it is easy to
miss when comparing "one command" against "image rebuild".

**Flyte** is the most sophisticated: `pyflyte register` sends versioned workflow
definitions to FlyteAdmin, and `ImageSpec` builds *and pushes a container image
per dependency set*, referenced as `container_image=payment_image`. Registration
is immutable and versioned server-side, so old runs keep their definition. Two
traps this repo documents: the registry must be reachable from both your build
host and the cluster (a `localhost/` image can't be pulled), and `ImageSpec`
defaults to amd64 — on an arm64 cluster you must set
`platform`/`FLYTE_IMAGE_PLATFORM` before registering.

**Argo** applies CRDs: `kubectl create -f argo/dag1-csv-etl.yaml` for one-shot
`Workflow`s, `kubectl apply -f argo/templates/` for the reusable
`WorkflowTemplate`s DAG 4 needs. There is no build step, because the Python is
**inlined into the YAML** as `source:` blocks — and the steps then
`pip install psycopg2-binary --quiet` at *runtime*, inside the pod, on every
execution. That is a real anti-pattern with real costs (network dependency at
run time, no pinning, no cache) and it is what "no image build needed" actually
buys you here. Worth flagging: `argo/scripts/*.py` are readable standalone copies
that **no YAML references** — the inlined blocks are the live code, so the two
can silently drift.

**Kestra** sits between Models 1 and 3. The flow YAMLs are mounted read-only at
`/flows` and then *pushed* into Kestra's own database with
`kestra flow namespace update orchestration.api /flows`. After that the engine
owns them, and the UI can edit flows in place — which diverges from the repo,
since the mount is `:ro`. Treat the CLI push as the only write path.

**Conductor is the cleanest split of the twelve, and the only true Model 3 + Model
2 hybrid.** The two halves deploy on completely independent cycles:

- The **orchestration** is JSON `PUT` to `/api/metadata/workflow`
  (`conductor/register.py`). It lands in the server's metadata store, versioned
  by an explicit `version` field. No restart of anything.
- The **task bodies** are a worker process that *polls* for task types
  (`conductor/worker.py`). It registers nothing; the server does not know or care
  which worker will pick a task up.

So changing the graph — reordering steps, adding a branch, swapping a
sub-workflow — needs no worker deploy at all, and changing a task body needs no
metadata push. No other tool here separates those. It also means `version` is
genuinely useful: bump it and in-flight executions finish on the definition they
started with, while new ones get the new graph. Temporal makes you hand-write
that with `patched()`; Argo and Kestra cannot do it at all.

The costs are the mirror image. **The repo is not the source of truth** — the
server is, the UI can edit definitions in place, and nothing detects drift, so
`register.py` re-reads the definitions after writing them. And retry policy,
timeouts and concurrency caps live in a *third* artefact (`taskdefs.json`,
`POST /api/metadata/taskdefs`), separate from both the workflow and the worker,
which is tidy once you know but is a third thing to keep in sync.

Dependencies are ordinary Python in the worker's environment, exactly as in
Model 2: a new library means redeploying the worker, and there is no per-task
isolation unless you split task types across separate workers (routed with task
`domain`).

---

## Cross-cutting

**Who owns the definition after deploy?** Models 1 and 2 keep the repo
authoritative — the engine's copy is derived and disposable. Models 3 gives the
engine its own stored copy, which enables real versioning (Flyte, Step Functions)
but also enables drift (Kestra's UI editor, a hand-edited state machine).

**Blast radius of a dependency change.** This is the sharpest divider:

- *Per-task/step images* — Argo, Flyte, Step Functions, Kestra script tasks. A
  new library affects exactly one step.
- *Per-run containers* — Prefect Docker pool, Airflow's `@task.docker`.
- *One shared environment* — Airflow default, Dagster within a code location,
  Temporal/Hatchet within a worker, Luigi always. A new library re-resolves the
  whole dependency set, and can break workflows that had nothing to do with the
  change. Dagster and Temporal at least offer a clean seam (code locations, task
  queues) rather than an escape hatch.

**In-flight runs during a deploy.** Flyte and Step Functions version definitions,
so running executions are unaffected. Airflow 3 now versions serialized DAGs —
this instance's metadata DB holds several `serialized_dag` rows for
`dag4_order_fulfillment` from today's edits, and each run records the version it
used. Temporal replays, so it needs explicit patching. Argo `Workflow` objects are
snapshots at submit time. Luigi has no concept of a run that outlives the process.

**Managed offerings keep the model, change the mechanism.** MWAA/Composer swap
the bundle for a bucket and the image rebuild for a slow environment update
(`requirements.txt` / a PyPI package list, 10–30 min, pip resolving in
production, no custom image). Temporal Cloud and Prefect Cloud host the
*control plane* only — workers stay yours, so Model 2's deployment story is
unchanged. Dagster+ uses a hybrid agent against your own images. Astronomer is
image-based, closest to baking DAGs in.

---

## Reading this for the recommendation

- **Cheapest to add a workflow to:** Google Workflows, Kestra, Airflow, Argo —
  push a file, no build. For Google Workflows, read that alongside the cost of
  the HTTP task layer every step calls; the *workflow* is cheap, the system is not.
- **Cheapest to add a *dependency* to:** Argo and Flyte (per-step images), and
  Luigi (there's nothing to deploy).
- **Most expensive either way:** Step Functions — layers, zips, Terraform. What
  you get is that everything is versioned and rollback is real.
- **Most coupled:** Temporal and Hatchet — no workflow exists without a worker
  deploy. For teams already shipping services this is a feature (workflows ride
  the app's existing CI/CD); for teams wanting analysts to add pipelines it is a
  blocker.
- **The trap to name explicitly in the deck:** "just drop a file in a folder"
  never covers dependencies, and it hides component-level packaging differences
  — Airflow's triggerer couldn't import a module its own dag-processor could.

## Caveats

Verified against this repo and this session: Airflow 3.2.1 behaviour, the
Airflow bundle config schema, Prefect's three tiers, Argo's inlined `source:`
blocks and runtime `pip install`, Kestra's `:ro` mount and CLI load,
`terraform/aws` packaging, and — verified by deploying all four DAGs on
2026-08-06 — Google Workflows' `user_env_vars` mechanism, its server-side
revisions, and the `templatefile()` collision.

Taken from documentation rather than exercised here: MWAA/Composer specifics
(including whether Composer 3 now permits custom worker images), Temporal
versioning-API details, and Dagster+ agent packaging. **Neither Argo nor Flyte
has ever completed a DAG run in this repo** (`RUNNING.md` §7c, §9b), so their
registration mechanics are read from the manifests and install steps, not
observed end-to-end.

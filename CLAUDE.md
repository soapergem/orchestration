# CLAUDE.md

## What this repo is

A **workflow-orchestrator bake-off**, not a production application. The goal is
to compare 12 orchestration systems, back the comparison with real working
example workflows in each, and deliver a slide presentation with a
recommendation.

Systems under evaluation: AWS Step Functions, Apache Airflow, Argo Workflows,
Dagster, Temporal, Kestra, Prefect, Flyte, Luigi, Hatchet, Google Workflows,
Conductor.

Because the deliverable is a *comparison*, the same four DAGs are implemented
independently in every orchestrator, each written in that tool's most idiomatic
style. Cross-tool code sharing is deliberately avoided — duplication is the
point, since the code is evidence about the tool.

## Read these first

| File | What it holds |
|---|---|
| `README.md` | The spec: evaluation criteria, the four DAG designs, shared-service API contracts. The source of truth for *what* each DAG must do. |
| `comparison.md` | The comparison matrix, the 100-point scoring rubric, score breakdown, per-tool differentiators. Currently: Temporal 88, Argo 74, Flyte/Hatchet 70, Luigi 38. |
| `RUNNING.md` | How to actually stand each orchestrator up locally (and Argo/Flyte on EKS). Container-networking rules, per-tool env vars, teardown. |
| `observability.md` | Deeper dive on audit trail / alerting / metrics / tracing for Temporal, Hatchet, Argo, Step Functions. |
| `deployment.md` | How workflow code gets *installed* in each of the 12 — the four packaging models (folder scan / worker-registers / control-plane push / none), and what adding a workflow vs. adding a dependency costs. |
| `terraform/aws/README.md` | Step Functions on real AWS (Lambdas + Neon Postgres + S3). |
| `google-workflows/README.md` | Google Workflows on real GCP (Cloud Run task layer + Neon). The seven expression-language defect classes are worth reading before touching any Workflows YAML. |
| `shared-services/deploy/README.md` | Mock services as a Helm chart on K3s (the AWS path needs them publicly reachable). |

## The four DAGs

Every orchestrator directory contains the same four, one file (or file set) per DAG:

1. **`dag1_csv_etl`** — unzip → parallel per-CSV load into Postgres → SQL transform → Parquet. Tests dynamic fan-out, retries, DB integration.
2. **`dag2_api_fanout`** — async callback (workflow *suspends*) → conditional branch → parallel detail fetches → combine. Tests suspend/resume by an external process. No database.
3. **`dag3_payment`** — validate → flaky gateway call with backoff+jitter → idempotent DB update → best-effort notification. Tests retriable vs. non-retriable error classification and graceful degradation.
4. **`dag4_order_fulfillment`** — reserve inventory → human approval (suspends) → shipping → with **saga compensation** on rejection/timeout/shipping failure, via three sub-workflows. The hardest one; it's where tools separate.

`README.md` has the full step lists, edge cases, and the feature-coverage matrix.
Keep implementations behaviorally equivalent across tools; where a tool *can't*
do something (e.g. Luigi has no suspend), implement the honest fallback and note
the gap — the divergence is a finding, not a bug to paper over.

## Layout

```
airflow/ dagster_bakeoff/ prefect/ luigi/   Python-native (no engine container)
                                            (Dagster's dir must NOT be named
                                            `dagster/` — it shadows the library)
temporal/ hatchet/                          Python SDK + engine in compose; worker on host
conductor/                                  JSON defs pushed to the engine (workflows/,
                                            taskdefs.json) + polling workers on host;
                                            engine config is a MOUNTED FILE, not env vars
kestra/ argo/ google-workflows/             YAML-defined (kestra/subflows/, argo/scripts/)
step-functions/dagN-*/                      ASL JSON + lambdas/ (+ sub-workflows/ for DAG 4)
flyte/                                       @task/@workflow Python, runs on K8s
shared-services/                             postgres + 4 FastAPI mocks + compose + init SQL
  callback-fetch-service/  approval-service/  shipping-service/  deploy/ (Helm)
  fixture-service/                           mock Books API (real Open Library data, CC0) + DAG 1's ZIP;
                                              serves two gitignored artefacts from test-data/ or S3
  gcp-task-service/                          the 14-route HTTP task layer Google Workflows calls —
                                              Workflows runs no code, so this is where its DAG logic lives
terraform/aws/                               Step Functions deployment (Lambdas, IAM, S3, SSM, ECR)
terraform/gcp/                               Google Workflows deployment (Cloud Run, 4 workflows, GCS, IAM)
presentation/                                Separate uv project: mkslides/reveal.js deck
private/                                     Gitignored — client-specific notes, never commit
test-data/                                    generators only; *.zip and books.json.gz are
  make-sample-data.py                         gitignored build artefacts, uploaded to S3 by terraform
```

`presentation/` is currently **untracked** in git. `presentation/site/` is
mkslides build output.

## Commands

```bash
uv sync                       # root project deps (all Python orchestrators)
# Both fixture data files are gitignored build artefacts -- generate once:
uv run --no-project test-data/make-sample-data.py                      # DAG 1's ZIP, instant
uv run --no-project shared-services/fixture-service/build_dataset.py   # DAG 2's corpus, ~1-2h
just                          # list recipes
just up                       # postgres + 4 mock services (always do this first)
just up temporal              # backbone + one engine profile (temporal|hatchet|kestra|conductor)
just up-all                   # backbone + ALL engines (no worker sidecars); heavier
just py-up [tools]            # start the host-run UIs: Dagster :3000, Prefect :4200,
                              # Airflow :8080 (they have NO engine container, so
                              # `up-all` cannot start them). py-down / py-status too.
just creds                    # logins for Airflow/Hatchet/Kestra/Postgres, and which
                              # services have no auth at all
just seed <runner>            # create that runner's schemas + seed fixtures
just reset <runner>           # drop those schemas and re-seed -- `seed` alone does NOT
                              # undo fixture drift (see Watch out)
just db-status <runner>       # which of the 3 databases that runner uses, + its schemas
                              # seed/reset/db-status route by runner name across local pod,
                              # in-cluster postgres and Neon; unknown names hard-fail
just rebuild                  # rebuild mock-service images (up does NOT rebuild)
just psql                     # psql shell against the bake-off DB
just down temporal            # stop that profile -- bare `just down` does NOT stop engines
just down-all                 # stop everything, engines included
just down-clean               # down-all + delete volumes
just logs
```

**`just down` leaves the engines running.** Engines are compose *profiles*, and
compose only acts on active profiles, so a bare `down` stops the backbone and
leaves temporal/hatchet/kestra/conductor holding their ports. Worse, they stay
attached to the pod and network, so the next `just up` fails with `network is
being used` / `container name is already in use` — if you see that, something
from a previous profile is still up. `just down-all` is the fix; its
`all_profiles` variable in the `Justfile` must gain any new engine you add,
since neither `--profile '*'` nor `COMPOSE_PROFILES` works under podman-compose.

Per-orchestrator run commands live in `RUNNING.md` — consult it rather than
guessing; the env-var wiring is non-obvious and tool-specific. Short version:

```bash
cd airflow  && AIRFLOW__CORE__DAGS_FOLDER=$PWD uv run airflow standalone   # :8080
source dagster_bakeoff/env.sh && uv run dagster dev -m dagster_bakeoff.repository  # :3000
cd prefect  && uv run prefect server start                                 # :4200
cd luigi    && uv run python dag1_csv_etl.py
cd temporal && uv run python worker.py    # + uvicorn signal_server:app --port 8095
just slides-serve                          # or: just slides-build
```

Ports: postgres **54321** (non-standard, deliberate), callback-fetch 8090,
approval 8091, shipping 8092, fixture-service 8099.

**One owner per host port** — `RUNNING.md` §0 is the canonical map, and
`./shared-services/check-ports.sh` checks it against what is actually listening.
Consult it before hard-coding a port anywhere. **8080 belongs to Airflow**;
Kestra's host port is therefore 8081, Flyte's console port-forward 8083, and the
Hatchet engine advertises `SERVER_URL: http://localhost:8888` so its minted
tokens don't send the SDK to Airflow. Container-internal ports are exempt —
services reach each other by compose DNS name, so only the host side must be
unique.

## Conventions

- **Container runtime is Podman here; finch on the user's other machine.** The
  `Justfile` auto-detects finch > podman > docker; override with
  `CONTAINER_RUNNER=`. Never hard-code `docker`. Anything needing the **Docker
  Engine API** (not just a CLI) is runtime-sensitive: Podman serves it via
  `systemctl --user enable --now podman.socket` →
  `DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock` (verified, reports
  "Podman Engine" at API 1.41). finch needs the separate `finch-daemon`, which
  implements only a *subset* of Docker API v1.43 — unverified here. Parameterise
  the gateway host and socket rather than assuming; see
  `prefect/deploy_docker.py`.
- **Env vars default to compose DNS names** (`postgres:5432`,
  `callback-fetch-service:8090`). Running on the *host* therefore requires
  explicit `POSTGRES_HOST=localhost POSTGRES_PORT=54321` and `localhost:809x`
  overrides. `.envrc` (direnv) exports several of these.
- **Nothing about hosts is hard-coded** — every callback target is an env var or
  server config, so switching container runtimes stays pure configuration.
- **Callback networking** (`RUNNING.md` §2): a callback target inside compose
  uses the service name (`hatchet-engine`, `kestra`); a target on the host uses
  the runtime's host-gateway hostname (`host.containers.internal` on Podman) —
  never `localhost`, which silently times out.
- **Resume-broker model** (`RUNNING.md` §2b): the callback-fetch and approval
  services store a provider-shaped `resume_data` blob at registration
  (`stepfunctions` → `{task_token}`, `http_callback` → `{callback_url}`) and
  perform the resume when triggered. Compose sets `AUTO_RESUME=true` /
  `AUTO_DECIDE_*` so DAG 2 and DAG 4 run hands-off; turn those off to exercise
  timeout / duplicate / late-callback edge cases.
- **DB isolation:** `init-db.sql` defines `bootstrap_bakeoff(ns)`, giving each
  runner its own `<ns>_dag1/_dag3/_dag4` schemas. A runner sets `BAKEOFF_NS` and
  its DB layer sets `search_path`. **Temporal, Prefect, Airflow, and Dagster
  implement this** (`temporal/activities.py`, `prefect/dag{1,3,4}_*.py`,
  `airflow/dag{1,3,4}_*.py`, `dagster_bakeoff/resources.py` — where it's one
  `ConfigurableResource` bound per job, the tidiest version of the pattern);
  **Argo and Flyte now implement it too** (2026-08-03): Argo via a `bakeoff-ns`
  workflow parameter → `BAKEOFF_NS` env → `search_path` in all 12 inline DB steps
  plus the 7 unreferenced `argo/scripts/*.py` copies; Flyte via
  `DBConfig.namespace`, carried as *data* because Flyte task pods don't inherit
  the launching shell's env; **Hatchet too** (2026-08-03), via `SCHEMA` in each
  DAG module's `get_db_connection`; **Conductor too** (2026-08-06), same per-module
  `SCHEMA` pattern, exported by `conductor/env.sh`.
  **Step Functions too** (2026-08-06), via `BAKEOFF_NS` → `SCHEMA` in each DAG's
  `lambdas/db.py`, passed as a Lambda env var from `var.bakeoff_ns`. That one
  matters because **Step Functions and Google Workflows share the same Neon
  database** — both need a publicly reachable Postgres, so the namespace is the
  only thing keeping `stepfunctions_dag3` and `google_workflows_dag3` apart.
  DAG 1 previously pinned a hardcoded `dag1_etl` schema, now
  `stepfunctions_dag1`; the old schema is orphaned but harmless. DAG 3/4
  previously wrote flat `public.*` tables in Neon.
  **every implementation now does** — Luigi was the last holdout in code and
  gained it 2026-08-06. The leftover `public.*` tables are **not dead yet**: the
  *deployed* Step Functions state machines still read them until
  `terraform -chdir=terraform/aws apply` lands its `BAKEOFF_NS` change. Once that
  applies, nothing reads `public.*` and it can be dropped. Note `init-db.sql`
  creates nothing there, so a fresh volume never had them. Pattern to
  copy: DAG 1 self-creates its schema (tables come from CSVs); DAG 3/4 fail fast
  if the schema is missing, because they need seeded fixtures.
  `init-db.sql`/`init-engines.sql` only run on a **fresh** `pgdata` volume — on
  an existing one use `just seed <runner>`, which reloads the function first.
- **Re-running a DAG is not the same as resetting it.** DAG 1 and DAG 2 are
  genuinely idempotent (DAG 1 drops and rebuilds its tables and overwrites a
  fixed `{table}.parquet`; DAG 2 touches no database). **DAG 3 and DAG 4 are
  idempotent by *refusal*** — `validate_payment` rejects a known
  `idempotency_key` as a duplicate and `reserve_inventory` returns
  `idempotent=True` for a known `order_id`, so re-running with the id you used
  last time reports success while doing nothing, and tests the duplicate path
  rather than the happy path. A real re-test needs a fresh id, which spends
  fixtures permanently: only saga compensation on a *failed* run returns stock.
  Drift accumulates (measured 2026-08-12: `temporal_dag4` WIDGET-A at 56/100,
  `temporal_dag3` ACC-001 at 4572.00/5000.00), and RARE-D seeds **2** units, so
  the concurrent last-unit race is one-shot per seed. `just seed` will **not**
  fix this — it is `CREATE TABLE IF NOT EXISTS` + `ON CONFLICT DO NOTHING`, so it
  restores structure and ignores values. Use **`just reset <runner>`**, which
  drops the three schemas and re-seeds; the engines' own metadata is untouched,
  so workflow history survives.
- **`seed` / `reset` / `db-status` route by runner across all three databases**
  (`scripts/bakeoff-db.sh`, 2026-08-12). They used to hardcode
  `compose exec postgres`, so naming a non-local runner operated on the local pod
  and *reported success* — `reset argo` re-created stray `argo_dag*` schemas
  there via `CREATE SCHEMA IF NOT EXISTS` while Argo's real data sat untouched in
  the cluster. That is where the local database's phantom argo/flyte/
  google_workflows namespaces came from. Routing now: local pod for the eight
  host-run tools, `deploy/bakeoff-postgres` in ns `orchestrators` for **argo** and
  **flyte**, `$NEON_DATABASE_URL` for **stepfunctions** and **google_workflows**.
  Unknown names are a **hard error** (they used to bootstrap schemas for the
  typo). A Neon reset prompts for the runner name unless given `--yes`, since it
  is a real cloud database — namespace isolation makes it safe between the two
  cloud runners, verified: resetting `stepfunctions` left `google_workflows_dag4`
  at 3 orders.
- **Python:** `requires-python >=3.14` at the root; some SDKs may lack 3.14
  wheels, so a throwaway 3.12 venv for workers is the documented workaround.
  `uv` for everything. Ruff config is global (`~/.config/ruff/ruff.toml`:
  isort + no-unused-noqa), not in-repo.
- **Code style:** every DAG file opens with a module docstring naming the DAG and
  listing the *tool idioms it demonstrates* (e.g. Airflow's "classic operator
  instantiation, `>>` chaining, `.expand()`"). Keep that — it's what makes the
  files readable as comparison evidence. Sections are separated by
  `# ---- ... ----` rule comments.
- There are **no tests**; verification is running the DAGs end-to-end.

## When updating the comparison

`comparison.md` claims are specific and cite mechanisms and version numbers.
Match that bar: state the mechanism, not just yes/no, and note caveats (retention
limits, paid-tier gates, "not by default"). If a score changes, update the
comparison table's Score row, the Score Breakdown, and the rationale section
together.

## Per-orchestrator READMEs

Each orchestrator folder gets a `README.md` that doubles as its lab notebook:
verified launch commands, the tool idioms the implementation demonstrates,
**Findings** (what bears on `comparison.md` scoring), what's *not* yet exercised,
and fixes applied. `prefect/README.md` is the template — follow its shape.
Cross-cutting concepts stay in `RUNNING.md`; don't restate them 12 times.

Testing status: **Prefect — all 4 DAGs verified** (2026-07-27), including native
`pause_flow_run()` and true `suspend_flow_run()` suspend/resume, saga compensation
via rejection *and* timeout, the spec's concurrency caps, and all four DAGs
registered as deployments (`prefect/serve_all.py`).

**Airflow — all 4 DAGs verified** (2026-08-02, Airflow 3.2.1), both via
`airflow dags test` and under the real scheduler/triggerer: deferrable
operators + custom triggers for both waits, saga compensation via rejection,
the spec's concurrency caps, and `BAKEOFF_NS` schema isolation. Fourteen
defects found and fixed, two of them parse-time — see `airflow/README.md`.
`source airflow/env.sh` before any Airflow command; **`PYTHONPATH` must include
the DAGs folder** or the triggerer cannot import `triggers/` and DAG 2/4 hang
in `deferred` silently forever.

**Temporal — all 4 DAGs verified** (2026-08-02, Server 1.27.2, SDK 1.29.0):
signal-based suspend/resume for DAG 2 and DAG 4, child workflows, saga
compensation via rejection *and* timeout *and* shipping failure, non-retryable
classification, and `BAKEOFF_NS` isolation. **Zero code defects** — the only gap
was that no starter client existed, now `temporal/start_workflow.py`.
**Durable execution demonstrated, not just claimed:** `kill -9` on the worker
mid-approval, approval decided during a 69s outage, resumed by a fresh process
with no lost work and no duplicated side effects. Wait for
`Acquired shard` in the server log before the first call, and stop the compose
`temporal-worker` container so it doesn't race the host worker on the same task
queue. See `temporal/README.md`.

**Dagster — all 4 DAGs verified** (2026-08-02, Dagster 1.13.5): sensor-bridged
suspend/resume for DAG 2 and DAG 4, saga compensation via rejection *and*
timeout *and* shipping failure, `allow_retries=False` classification, and
`BAKEOFF_NS` isolation. Nine defects found and fixed, one of them structural —
the directory could not be named `dagster/` at all. `source
dagster_bakeoff/env.sh` first (it sets `DAGSTER_HOME`, without which sensors
cannot bridge runs), and load the code location as `-m dagster_bakeoff.repository`.
See `dagster_bakeoff/README.md`.

**Argo — DAG 3 and DAG 4 verified on Kubernetes** (2026-08-03, Argo v4.0.8 on the
arm64 cluster): DAG 4 green on both the approval-required and
skip-approval branches, saga sub-workflows via `templateRef`, `BAKEOFF_NS`
isolation. Five defects, each masking the next — misplaced
`activeDeadlineSeconds`, `{{workflow.parameters.*}}` resolving against the
*caller* inside a `templateRef` (the template's own `spec.arguments` defaults are
ignored), a dag template not re-exporting child outputs, the same FK-ordering bug
Prefect had, and `when:` clauses dereferencing skipped tasks — fixed by
converting the DAG to `depends:`, since Argo forbids mixing `depends` and
`dependencies` in one template. DAG 1/2 unattempted (missing fixture URL; GitHub
rate limit). See `RUNNING.md` §8.

**Flyte — all 4 DAGs verified on Kubernetes** (2026-08-06,
`flyte-core-v1.16.8` on the arm64 cluster): `@dynamic` fan-out loading 3
CSVs plus Parquet to the blob store, a 30-item DAG 2 fan-out with zero failures,
DAG 3's payment moving $100, and DAG 4's full approval path ending `shipped` with
tracking. Saga compensation, DAG 3's decline branch and re-runs are still
untested. **Seven defect classes across ~25 sites** — the whole implementation had never been run.
Infrastructure: build task images **in-cluster** (arm64 from x86 needs qemu
binfmt, which rootless podman can't register), register **in-cluster**
(`pyflyte register` uploads to a signed URL naming
`minio.flyte.svc.cluster.local`), and give task pods blob credentials **plus**
`FLYTE_TASK_IMAGE` via `plugins.k8s.default-env-vars` — a `@dynamic` task
re-resolves `ImageSpec` inside its own pod, so the registration-time override is
not enough. Code: **statement order in a `@workflow` implies nothing** (Flyte
derives edges only from data dependencies — `run_sql_transform` ran alongside
`unzip_file`; four "unused variable" assignments were latent races, now `>>`
edges), **local paths don't survive a task boundary** (CSVs now travel as
`FlyteFile`), **`retries=` is inert unless the exception subclasses
`FlyteRecoverableException`**, a **`@workflow` cannot construct its own output
dataclass** from Promises (5 sites), **nested dataclass predicates break
`conditional()`** (4 sites), and the resume-broker contract needs an explicit
`provider`. The chart also configures a minio blob store it does **not** deploy —
why the earlier amd64-cluster install sat healthy for 46 days with zero executions. Scripts:
`flyte/deploy-flyte.sh`, `register.sh`, `run.sh`. See `flyte/README.md`.

**Kestra — all 4 DAGs verified** (2026-08-04, Kestra 1.3.30): native `Pause` +
`onResume:` suspend/resume for DAG 2 *and* DAG 4, saga compensation via rejection
*and* timeout *and* shipping failure, the spec's concurrency caps via
`ForEach.concurrencyLimit`, and `kestra_*` schema isolation. **Eighteen defects**,
the most of any tool so far. Script tasks default to the **Docker task runner**, so
`just up kestra` needs a Docker-compatible socket — the `Justfile` now
auto-detects it (`container_sock`), so `CONTAINER_SOCK` only needs setting to
override; it still needs `systemctl --user enable --now podman.socket` once — and every one of
the 30 script tasks crashed on `from kestra import Kestra`, which is preinstalled
only in the server's `/app/.venv` (why DAG 3, the lone `Process`-runner flow,
worked). Task containers are *siblings*: they need an explicit `networkMode` or
`postgres` won't resolve. **`execution.resumeUrl` does not exist in any Kestra
version** — the mocks gained a `kestra` resume provider (auth + multipart) because
Kestra's resume endpoint 401s unauthenticated and 415s on JSON. Kestra OSS has one
credential, the shared admin account; service accounts are Enterprise-only. DAG 1
had been silently overwriting the shared `public.orders`/`customers` fixtures —
repaired, see `kestra/README.md` §Fixes #18 for a caveat on the rebuilt FK. Also:
the server **exits status 0 when Postgres goes away** and never reconnects -- and if
the restart lands while Postgres is still down it wedges (live JVM, container says
`running`, serves nothing), which is why compose now has `restart: unless-stopped`
plus a `/ping` healthcheck on it; and
`retry:` has no error-type predicate, so `InvalidAddress` got retried 12 times.
See `kestra/README.md`.

**Google Workflows — all 4 DAGs verified on real GCP** (2026-08-06, a
throwaway project in `us-central1`): native `events.await_callback` suspend/resume for
DAG 2 *and* DAG 4, a 30-item fan-out, DAG 3's decline (402, no retries) and
timeout (504, retried then exhausted) branches, saga compensation with inventory
restored, and `BAKEOFF_NS` isolation on Neon *shared with Step Functions*.
**Seven expression-language defect classes** — `{}` is not a literal, `in` does
not work on strings, `list.concat` appends one value rather than concatenating,
`shared:` needs a pre-existing variable, a `switch` with `next:` silently skips
intervening steps, unset variables serialize as `null` instead of erroring, and a
TypeError raised inside a retry predicate *replaces* the original HTTP error on
its way to `except` (stripping `code`, so the decline branch died with a
`KeyError` pointing nowhere near the cause). Four fail at deploy time, which is
the good news.

The structural finding: **the engine executes no code**, so this DAG set needed a
14-route Cloud Run service (`shared-services/gcp-task-service/`) before anything
could run. Cheap to deploy, expensive to build — see `deployment.md`. Resume
required a fifth broker provider (`google_workflows`) minting an OAuth2 *access*
token; `roles/workflows.invoker` suffices. Config goes through `user_env_vars`,
**never** `templatefile()` — the Workflows language uses `${}` too. See
`google-workflows/README.md` and `RUNNING.md` §10.

**Hatchet — all 4 DAGs verified** (2026-08-03, hatchet-lite, SDK 1.33.10):
durable event waits for DAG 2 *and* DAG 4, saga compensation via rejection *and*
timeout *and* shipping failure, `NonRetryableException` classification, and
`hatchet_*` schema isolation. Ten defects. Three engine defaults each cause a
**silent infinite hang** with nothing in any log: durable tasks are never
dispatched unless the workflows are passed to `hatchet.worker(workflows=[...])`
(registering afterwards allocates no DURABLE slots); `durable_task` caps the
*suspended* wait at a 1-minute `execution_timeout`; and a **SIGKILLed worker
stays ACTIVE** in the engine, keeps being assigned durable tasks, and survives an
engine restart — always stop workers with SIGTERM, and `HATCHET_CLIENT_NAMESPACE`
sidesteps stale registrations. Condition CEL addresses the event payload as
`input.x`; `{{ .x }}` silently matches nothing. `aio_wait_for_event` takes **no
timeout** — use `aio_wait_for` + `OrGroup(UserEventCondition, SleepCondition)`.
Hatchet can't be an HTTP callback target (bearer auth + `{key,data}` envelope),
hence `hatchet/event_relay.py`, which must share the worker's namespace.
**`source hatchet/env.sh` before anything.** The minted token used to embed
`server_url: localhost:8080` — **Airflow** on this host, so SDK calls silently
returned Airflow's error JSON; compose now sets the engine's `SERVER_URL` to
`:8888` and `env.sh` overrides it too. See `hatchet/README.md`.

**Conductor — all 4 DAGs verified** (2026-08-06, server 3.31.0, `conductor-python`
2.0.0 on Python 3.14): `FORK_JOIN_DYNAMIC` runtime fan-out, `WAIT`-task
suspend/resume for DAG 2 *and* DAG 4, `SUB_WORKFLOW` composition, saga
compensation via rejection *and* timeout *and* shipping failure *and* the
compensation dead-letter, `FAILED_WITH_TERMINAL_ERROR` classification, and
`conductor_*` schema isolation. Every spec edge case exercised, including the
concurrent last-unit reservation race. **Score 75 — second place, but it does not
displace Temporal (88).** Fifteen defects. Three that will bite anyone:
**engine config must be a MOUNTED FILE** — Conductor turns config-file keys into
Java *system properties*, which outrank env vars, so `SPRING_DATASOURCE_*` is
silently ignored and Hikari just loops `Starting...`; **`pkill -f worker.py`
orphans all 26 spawned task-runner children** (their cmdline is
`multiprocessing.spawn`), which keep polling and executing stale code — 114
accumulated across three restarts and the symptom looked like a flaky external
service, so use `conductor/stop_worker.sh`; and **the stock
`config-postgres.properties` does not boot the stock image** (`file-storage.type=conductor`
has no matching bean). Also: task timeouts are sweeper-enforced and fire late
(60s measured at 103s), `outputParameters` are evaluated even on FAILED runs so
literals leak into failure output, and `conductor-python` crashes on a bare
`list` type annotation (`list[dict]` is required). Conductor OSS has **no
authentication whatsoever**. See `conductor/README.md` and `RUNNING.md` §6b.

**Luigi — all 4 DAGs verified** (2026-08-06, Luigi 3.7.1): DAG 4's approval,
skip-approval *and* rejection→compensation paths, DAG 3's retriable-vs-terminal
classification (forced decline fails on attempt 1; forced 5xx takes exactly 5
before giving up), and `BAKEOFF_NS` isolation — Luigi was the last implementation
to get it. **Six defects, none structural**: the FK-ordering bug shared with
Prefect/Argo/Flyte, two missing resume-broker `provider` fields, and documented
invocations naming seed rows that don't exist. Two CLI traps: **`PYTHONPATH` must
include `luigi/`** or `luigi --module X` dies with `ModuleNotFoundError` before
any task runs, and only the *root* task's parameters are bare flags
(`--max-retries` belongs to `ProcessPayment`, so it needs
`--ProcessPayment-max-retries`). The retry loop is **silent** — nothing external
reveals whether a task retried once or five times. See `luigi/README.md`.

**Step Functions — all four DAGs verified on real AWS** (2026-08-12,
`us-east-1`): DAG 1 loading 3 CSVs to `dag1_etl` plus Parquet to S3, DAG 2
suspending on a task token and resuming to a 5/5 fan-out, DAG 3 moving $100, and
DAG 4 through the approval path to `shipped` with tracking — *plus* an accidental
but real saga compensation (order `cancelled`, reservation `released`, inventory
restored). This corrects a "still untested" claim that stood here until
2026-08-12; an earlier campaign on 2026-07-14 had also run all four. See
`step-functions/README.md`, which did not exist before — Step Functions was the
only orchestrator with no lab notebook, which is exactly why the stale claim
survived unchallenged.

Three findings, each of which cost real time:
**(1) The resume credentials are a separate, silent deployment step.** Terraform
creates IAM user `orch-bakeoff-callback-resume` and an access key scoped to
`states:SendTaskSuccess`, but *nothing enforces* putting them in the K8s
`aws-resume-creds` Secret. At the time the Helm chart wired it and
`deploy-backbone.sh` (the §7c in-cluster path, written for Argo/Flyte) did not —
so the arm64 cluster's mocks had `google-resume-creds` and no AWS credentials.
**Both paths are now one chart** (2026-08-12); the Secret is still per-cluster
and not chart-managed, so the trap survives — it is just no longer
path-dependent. `boto3` then raised
`NoCredentialsError`, the resume never fired, and the token aged out. **Both
failure messages lie**: DAG 2 reports `FanOutError`, and DAG 4 reports
"Order rejected or approval timed out" — a credentials problem in another system
presented as a business decision. `helm list -n orchestrators` returning nothing
is the fastest way to spot the wrong deployment path.
**(2) DAG 2 needs `base=`, and Step Functions is a *third* case** beyond the
host-run/in-cluster split documented under "Watch out": the collection is fetched
in-cluster but the detail URLs are fetched by a **Lambda in AWS**, so
fixture-service handed back `http://fixture-service:8099/...` and every map
iteration died on `NameResolutionError`. It needs the *public* base
(`&base=https://orch-fixture...`). The rule is "whatever can reach the detail
URLs", not "wherever the collection was fetched".
**(3) `terraform/aws` had no state file and no backend** — the root cause of (1),
since `deploy.sh` reads the credentials from `terraform output`. A missing state
file presented as a business-logic failure two systems away.

**Terraform state is now remote for both clouds** (2026-08-12): a partial
`backend "s3" {}` in each of `terraform/aws` and `terraform/gcp`, configured at
init from a **gitignored `backend.hcl`** (template `backend.hcl.example`) so no
bucket name is committed. GCP's local state was migrated; AWS had none, so the
live deployment was adopted with **67 declarative `import` blocks** and now
reports *"No changes"* across 87 resources. Four gotchas worth knowing, all in
`step-functions/README.md`: an access key's **secret cannot be re-read**, so
importing one yields an empty output that `deploy.sh` would write into the K8s
Secret as an empty string (mint a fresh key instead); not everything in the
config existed in AWS (`fixture_reader`, `books_corpus` had to be created); the
Lambda layer scripts hardcoded `pip3` and now fall back to `uv`, whose rebuild
forces 2 immutable layer replacements on a first apply from any new machine; and
an interrupted apply leaves a lock needing `terraform force-unlock`.

**`BAKEOFF_NS` is now applied to AWS** (2026-08-12), so DAG 3/4 write
`stepfunctions_dag{3,4}` — verified: `PAY-NS-144256` and `ORD-NS-144256` landed
in the namespaced schemas with **zero** rows leaking to `public.*`. Note the
six-day gap where deployed code wrote `public.*` while the namespaced schemas sat
empty: **auditing `stepfunctions_dag*` during that window showed zeros and read
as "never run"** — always confirm which schema a tool's *deployed* code targets
before drawing conclusions from row counts. `public.*` and `dag1_etl` are now
historical and droppable. Expect the same class of breakage found in
Prefect, Airflow, Dagster, Argo, Kestra, Hatchet, Flyte, Conductor, and Google
Workflows (see their READMEs) — though Temporal shows it isn't inevitable. Two
cautionary cases worth carrying into those two: Kestra is the
one for *documentation* as well as code (`RUNNING.md` §6's flow-loading command
and callback story were both confidently specific and wrong), and Flyte is the
one for *silence* — its four workflows each needed structural change before they
would even register, and the install they were meant to run on had looked
healthy for 46 days while executing nothing.

**Deployments, if you add them for another tool:** flow parameters that double as
idempotency keys must default to a *generated* value, not a literal — deployment
parameters are static, so a fixed id turns every re-run into a no-op skip. And a
`serve()` runner passes its own environment to the runs it launches, so DB/service
vars belong on the runner, not per invocation.

Prefect has three execution tiers, all working: ad-hoc scripts,
`serve_all.py` (process-per-run), and `deploy_docker.py` (**container**-per-run,
which is what substantiates the dependency-isolation claim). Isolation is per
flow run, not per task. Two consequences of ephemeral compute that bit here and
will bite other tools: filesystem I/O needs a bind mount, and suspend/resume
needs *shared* result storage because the resumed run gets a new container.

## Watch out

- **`just up` does not rebuild images.** The mock services were found running a
  two-month-old image missing the entire resume-broker API (`GET
  /approval-requests` 405'd, no `/resume`). Run `just rebuild` after touching
  anything under `shared-services/*/app.py`, and check
  `podman exec <svc> grep -c '@app\.' /app/app.py` against the source if an
  endpoint 404s/405s unexpectedly.
- **`/decide` returns 500 when the registered resume URL is a placeholder.** The
  approval service records the decision *and* fires the resume; an orchestrator
  that only polls registers a dead URL (`http://localhost:0/noop`), so the resume
  leg fails while the decision is still recorded correctly. Cosmetic — don't
  debug it as a failure. Prefect no longer does this (real resume URL → 200).
- **Host-targeted callbacks on Podman must use `host.containers.internal`.**
  `docker-compose.yml` pins `host.docker.internal:192.168.5.2`, which is the
  **finch/Lima** gateway and is unreachable under Podman (verified: times out,
  while `host.containers.internal` → `10.255.255.254` works). A host process that
  containers must call back into also has to bind `0.0.0.0`, not `127.0.0.1`.
- **DAG 2 reads a local Books API, not GitHub** (repo-wide as of 2026-08-04).
  `shared-services/fixture-service` (:8099) serves `GET /books` -> summaries with
  detail URLs, then `GET /books/{id}`, over a committed extract of **real Open
  Library metadata (CC0 1.0)** -- thousands of works, refreshed with
  `build_dataset.py`, never fetched at runtime. It replaced `api.github.com`, which
  allows only **60 unauthenticated requests/hour per IP** while a default DAG 2 run
  cost 1 + 30, so two runs 403'd the campaign *across all orchestrators combined*.
  Goodreads is not an alternative: Amazon retired that API in Dec 2020.
  **The URL differs by where the fan-out runs**, and this is the easy thing to get
  wrong: the *collection* is fetched by callback-fetch-service (a container), so the
  host is always the compose DNS name -- but the *detail* URLs are fetched by the
  orchestrator's own tasks, and fixture-service derives them from the request. A
  **host-run** fan-out (Airflow, Prefect, Dagster, Luigi, Temporal, Hatchet,
  Conductor)
  therefore needs `?base=http://localhost:8099` appended, or it will try to resolve
  `fixture-service` from the host and fail. **Container/in-cluster** execution
  (Kestra, `prefect/deploy_docker.py`, Argo, Flyte) needs no override. `/books`
  returns a **bare array** with `X-Total-Count`/`Link` headers precisely so
  `isinstance(body, list)` normalizers keep working, and every tool's normalizer now
  reads `item.get("title") or item.get("name") or item.get("id")` so either shape
  works.
- **The Prefect/Airflow-style DAG files can't be trusted as working code** until
  run. Every one of Prefect's four had a blocking defect (wrong schema, missing
  `unmapped()`, FK insert ordering, sample inputs referencing non-existent seed
  rows, missing `retry_condition_fn`). Budget for that per tool.
- `.envrc` holds a long-lived Hatchet client JWT. It is **gitignored** (`*.env*`)
  and absent from history, so it's local-only — but don't copy it into files that
  *are* tracked. Being gitignored also means it's not a shared source of truth:
  each machine needs its own, and `RUNNING.md` §3 must stay accurate about what
  it exports.
- Terraform's AWS path uses the `AWS_PROFILE` named profile, real Neon Postgres via
  `NEON_DATABASE_URL`, and hardcoded dev credentials in the Flyte/K8s snippets —
  all evaluation-grade, explicitly not production-safe.
- `RUNNING.md` §"Status / caveats" flags what is verified-by-analysis vs.
  best-effort-from-docs (remaining engine image tags). Prefer verifying over
  trusting there. Hatchet's port layout and token subcommand are now confirmed;
  its callback story, like Kestra's, was wrong.
- Step Functions and Google Workflows have no local-run path — both are managed
  cloud services with no emulator for these DAGs, and both need a *publicly
  reachable* Postgres and mock services (`RUNNING.md` §7c-i). Google Workflows
  is now deployed and verified (`RUNNING.md` §10). Argo and Flyte are
  Kubernetes-only (`RUNNING.md` §7–§9, cluster-agnostic: §7 is the shared setup —
  cluster variables, the arm64 rule, the in-cluster backbone — then §8 Argo, §9
  Flyte). Two clusters are in play, distinguished by CPU architecture rather than
  by name — an **amd64** EKS Fargate cluster and an **arm64** OCI cluster. Set
  `KCTX` to whichever you are targeting (`.envrc.example`); no script assumes a
  current-context. The arm64 one is where task images must be built for arm64;
  the Fargate one has no dynamic provisioning, hence `STORAGE_CLASS=""`.
  **Both now run on the arm64 cluster** — Argo DAG 3 + DAG 4 (2026-08-03, v4.0.8)
  and all four Flyte DAGs (2026-08-06, `flyte-core-v1.16.8`); see their status
  entries above for what is still unexercised (Argo DAG 1/2; Flyte's saga
  compensation, DAG 3 decline branch and re-runs). Getting there took `RUNNING.md`
  §7c and §9b, which exist because of how the **amd64** cluster failed: Argo's
  submissions had no in-cluster Postgres or mock services, and Flyte sat healthy
  for 46 days with zero executions because the chart configures a minio blob
  store it never deploys. Neither failure surfaced as an error — that is the
  thing to watch for, not the specific bugs.

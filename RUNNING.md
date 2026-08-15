# Running the Bake-Off Locally

How to stand up each orchestrator against the shared services. §1–§6 cover the
**local** orchestrators; §7–§9 cover Argo and Flyte on Kubernetes, written to be
**cluster-agnostic** (any context, either architecture); §10 covers Google
Workflows on real GCP. Step Functions lives in `terraform/aws/README.md`.

Neither Google Workflows nor Step Functions has a local-run path — both are
managed cloud services with no emulator that runs these DAGs. They are also the
two that need a **publicly reachable** Postgres and mock services, which is why
§7c-i exists.

> **Container runtime:** the repo is runtime-agnostic. The `Justfile` auto-detects
> whichever of **finch**, **podman**, or **docker** you have installed (in that
> order) and drives its `compose` subcommand, so the `just up` / `just down`
> recipes below Just Work regardless of runtime. For the handful of raw commands
> that have no `just` recipe (engine `exec` calls, token minting), set a
> shorthand once so the examples are copy-pasteable from the repo root:
>
> ```bash
> export COMPOSE="finch compose -f shared-services/docker-compose.yml"
> #                ^^^^^ or "podman compose ..." / "docker compose ..."
> ```
>
> The one place the runtime genuinely matters is the **host-gateway hostname**
> for host-targeted callbacks — see §2.

---

## 0. Port map (one owner per port)

Every orchestrator gets its own host port so two can run side by side and
nothing silently talks to the wrong service. **Check this table before adding a
port anywhere.**

| Port | Owner | What |
|---|---|---|
| 54321 | shared | postgres (non-standard on purpose) |
| 8090 | shared | callback-fetch-service |
| 8091 | shared | approval-service |
| 8092 | shared | shipping-service |
| 8099 | shared | fixture-service — mock Books API for DAG 2 + DAG 1's ZIP (§0b) |
| 3000 | Dagster | `dagster dev` UI |
| 4200 | Prefect | `prefect server` UI + API |
| 8080 | **Airflow** | `airflow api_server` UI |
| 8793–8794 | Airflow | `serve-logs` (worker + triggerer log servers) |
| 8081 | Kestra | UI (`KESTRA_PORT`; container still listens on 8080 internally) |
| 8082 | Luigi | `luigid` central scheduler, if used (Luigi's own default) |
| 8083 | Flyte | `flyteconsole` port-forward |
| 8084 | presentation | `mkslides serve` presentation slides |
| 8085 | Flyte | console proxy (`just flyte-ui`) — merges flyteconsole + flyteadmin |
| 8095 | Temporal | signal-relay server (host) |
| 8096 | Hatchet | event-relay server (host) |
| 8233 | Temporal | Web UI |
| 8888 | Hatchet | engine REST API + dashboard |
| 7233 | Temporal | engine gRPC |
| 7077 | Hatchet | engine gRPC (container listens on 7070) |
| 2746 | Argo | `argo-server` port-forward (`just port-forwards`) |
| 54322 | shared | in-cluster `bakeoff-postgres` port-forward (`just port-forwards`) — the cluster twin of 54321 |
| 8000 | Conductor | server REST API (`/api`) — workers and `register.py` talk here |
| 8127 | Conductor | UI (nginx inside the same container, on :5000) |
| 8100 | Kruxia Flow | engine REST API + `/dashboard` — **not** its own default of 8080, which is Airflow's here |
| 8101 | Kruxia Flow | MCP server (disabled in this repo; port reserved so it cannot collide with Kestra's 8081) |

### Logins — `just creds`

Four of these UIs want a password and the rest want nothing, so run **`just
creds`** rather than hunting: it reads the live values and names the services
that have no auth. Where each one comes from, since they differ in kind:

| Service | Credential | Source |
|---|---|---|
| Airflow :8080 | `admin` / *generated* | Written to `airflow/.airflow-home/simple_auth_manager_passwords.json.generated` on the **first** `standalone` run. Later starts log *"previously generated … Not echoing it here"*, so the file is the only copy. Delete it and restart to rotate. |
| Hatchet :8888 | `admin@example.com` / `Admin123!!` | Seeded by the hatchet-lite image, not by our compose file. Verified via `POST /api/v1/users/login`. |
| Kestra :8081 | `admin@orchestration.local` / `Orchestration_123` | `docker-compose.yml` (`KESTRA_USER`/`KESTRA_PASSWORD`, overridable). Kestra OSS has exactly **one** credential — service accounts are Enterprise-only — so the mock services reuse this to drive its authenticated resume endpoint. |
| Postgres :54321 | `orchestration` / `orchestration` | `docker-compose.yml`. |

**No auth at all:** Conductor (:8127), Temporal UI (:8233), Dagster (:3000),
Prefect (:4200), luigid (:8082). **Kruxia Flow (:8100) is a deliberate opt-out
rather than an absence** — it requires OAuth2 on every request by default, and
this repo runs it with `KRUXIAFLOW_INSECURE_DEV=true` so the mock services can
resume a workflow with one unauthenticated POST. Note the flag governs *request*
auth only: `serve` still refuses to boot without an RSA keypair and a client
secret, which is why the profile carries a `keygen` one-shot. Conductor's is a finding rather than a
convenience — OSS has no authentication whatsoever, so anyone who can reach the
API can read every execution, complete any task and rewrite every definition.
It scores 3/5 on auth for exactly this.

`HATCHET_CLIENT_TOKEN` is **not** a UI login — it is the JWT the host worker
authenticates with, minted at runtime (§5). `just creds` reports whether it is
set, not its value.

All of this is evaluation-grade and local-only. It is safe to print because none
of it protects anything; Argo, Flyte, Step Functions and Google Workflows
authenticate through the cluster or cloud IAM instead and are not covered.

**8080 belongs to Airflow.** Three other things wanted it and each was moved or
pinned away:

- **Kestra** defaulted its *host* port to 8080; now 8081. The container still
  listens on 8080 internally and `kestra.url` is unchanged, because resume URLs
  are resolved container-to-container.
- **Hatchet** bakes a `server_url` claim into every minted token, and the SDK
  trusts it for all REST calls. hatchet-lite generates `http://localhost:8080`,
  so on this host the SDK talked to **Airflow** and returned
  `"/api/v1 has been removed in Airflow 3"`. Compose now sets
  `SERVER_URL: http://localhost:8888` on the engine so new tokens are correct;
  `hatchet/env.sh` also sets `HATCHET_CLIENT_SERVER_URL` as a belt-and-braces
  override for tokens minted before this change.
- **Flyte**'s console port-forward example used 8080; now 8083.

Ports inside containers are not part of this map — services reach each other by
compose DNS name and can reuse whatever they like. Only the **host** side has to
be unique.

Verify reality against the table at any time:

```bash
./shared-services/check-ports.sh
```

It prints the owner of every mapped port and flags anything listening in
2000–9999 that the map doesn't account for — which is how 8099 and Airflow's
`serve-logs` pair got added to it.

---

## 0b. fixture-service — the Books API (:8099)

The **fourth shared service** (`shared-services/fixture-service/`), alongside
callback-fetch, approval and shipping. It starts with `just up` — no separate
command. Two DAGs need it, for different reasons:

- **DAG 1 — Kestra only, and mandatory.** Kestra's `dag1_csv_etl` takes a
  `zip_url` *input* and downloads it, where Prefect and Airflow take a local
  path (`ETL_ZIP_PATH`). A Kestra script task runs in its own throwaway
  container, so the ZIP has to be reachable over HTTP. Kestra cannot run DAG 1
  without this.
- **DAG 2 — every tool.** DAG 2's spec only asks for "items, each with its own
  detail URL". Implementations reached for `api.github.com/orgs/<org>/repos`
  because it happens to have that shape, but unauthenticated GitHub allows
  **60 requests/hour per IP** and a default run costs 1 + 30 — so two runs
  exhaust the budget for *every orchestrator at once*, and the 403s look like
  flow bugs. Point DAG 2 at `/books` instead.

It is a **mock Books API** backed by **real Open Library metadata** — a paginated
collection of summaries, each carrying the URL of its own detail resource. That
list-then-fan-out-to-details shape is exactly what DAG 2 exercises, and books are
the canonical example domain for it.

| Endpoint | Used by |
|---|---|
| `GET /books` | DAG 2 initial fetch — page of summaries, each with a `url` |
| `GET /books/{id}` | DAG 2 fan-out — full record (ISBN, publishers, pages, rating…) |
| `GET /search.json` | same data in Open Library's `{numFound, start, docs}` envelope |
| `GET /subjects` | subject facets with counts, for `?subject=` |
| `GET /authors/{id}` | an author plus their works — a second fan-out shape if wanted |
| `GET /sample-data.zip` | DAG 1 `zip_url` (bind mount locally, S3 when deployed) |
| `GET /health` | readiness, corpus size, and confirms the `test-data/` mount |

`/books` supports `?page=`, `?per_page=`, `?subject=`, `?author=`, `?q=` (title
substring), `?year_from=` and `?year_to=`. It returns a **bare JSON array** with
`X-Total-Count` and `Link` headers rather than an envelope — deliberately
GitHub-shaped, because several orchestrators' DAG 2 normalize step tests
`isinstance(body, list)` and an envelope would silently yield zero items. Use
`/search.json` when you *want* the envelope.

**Address it by whichever name the caller can reach**, exactly like the other
mocks — `fixture-service:8099` from inside compose, `localhost:8099` from a
host-run worker:

```bash
curl 'http://localhost:8099/books?per_page=3'
curl 'http://localhost:8099/books?subject=fantasy&year_from=1950'
curl 'http://localhost:8099/books?author=tolkien'
curl  http://localhost:8099/books/OL27448W
```

Detail URLs are **built from the requesting URL**, so a container that fetches
`/books` gets `fixture-service:8099` links and a host process gets
`localhost:8099` ones — no host-gateway hostname anywhere, and nothing to
reconfigure between Podman and finch. Override with `?base=<url>` or
`FIXTURE_BASE_URL` only when the caller that fetches the collection and the caller
that fetches the details sit on *different* networks.

`FIXTURE_DEFAULT_PER_PAGE` (default 5) sets DAG 2's fan-out width when no
`?per_page` is given. The corpus is thousands of works, so `?per_page=30`
overshoots the spec's concurrency cap of 20 and actually exercises it — verified
against Kestra, which held peak concurrency at exactly 20.

### The two data files: built locally, S3 when deployed

Neither is committed and neither is baked into the image. `test-data/` holds the
generators; the artefacts are gitignored.

| File | Built by | Time | Needed for |
|---|---|---|---|
| `test-data/books.json.gz` | `shared-services/fixture-service/build_dataset.py` | ~1–2 h (flaky upstream) | DAG 2 — **essential** |
| `test-data/sample-data.zip` | `test-data/make-sample-data.py` | instant | DAG 1 — optional |

Each is resolved at startup as: **compose bind mount** → **download cache** →
**fetch from S3/HTTPS once on boot**. Local dev uses the mount and needs no AWS
access at all.

```bash
uv run --no-project test-data/make-sample-data.py                      # instant
uv run --no-project shared-services/fixture-service/build_dataset.py   # slow, once
```

`build_dataset.py` checkpoints after every page and `--resume` extends an existing
file, so a long build survives interruption. `make-sample-data.py` is
byte-stable — regenerating does not churn the S3 etag or Terraform's `filemd5()` —
and its CSV payloads are byte-identical to the archive it replaced, CRLF included.
All eleven DAG 1 implementations join `orders`/`customers`/`products` on those
exact columns, so do not change them casually.

**Deployed**, there is no host mount, so the pod fetches from S3 on boot:

```bash
cd terraform/aws
terraform output fixture_books_url               # s3://<bucket>/input/books.json.gz
terraform output fixture_sample_zip_url           # s3://<bucket>/input/sample-data.zip
terraform output fixture_objects_uploaded         # which artefacts were actually present
terraform output fixture_reader_access_key_id     # + fixture_reader_secret_access_key
```

Terraform uploads whichever artefacts exist locally — `count` on `fileexists()`,
so a plan does not hard-fail merely because the hour-long corpus build has not been
run. `fixture_objects_uploaded` tells you what actually landed. The bucket blocks
all public access, so the fetch needs credentials: a dedicated least-privilege IAM
user (`s3:GetObject` on `input/*` only — narrower than the callback user, whose
task tokens carry no ARN and so require `"*"`). `deploy.sh` creates the
`fixture-s3-creds` Secret from those outputs; `deploy-backbone.sh` reads
`FIXTURE_BOOKS_URL` / `FIXTURE_SAMPLE_ZIP_URL` / `AWS_*` from the environment. An
`https://` URL is also accepted and skips credentials entirely.

**Failure behaviour differs by file, deliberately** — verified for every case:

- **No corpus** → the service still starts (so logs are reachable) but `/health`
  returns **503 `no-corpus`** and all five corpus-backed routes return 503 naming
  what to set. Readiness therefore never passes, so Kubernetes shows it as broken
  rather than routing traffic to an empty library.
- **No archive** → only `/sample-data.zip` fails, with a 500 explaining how to
  provide it. `/health` reports `sample_data_present: false` but stays 200, because
  **DAG 2 does not need the archive.**

An unwritable cache, a bad URI, or missing credentials are logged and degrade the
same way; none of them can crash startup.

### Which URL each orchestrator uses

DAG 2 has **two** fetch legs, and they can run in different places. The
*collection* (`/books`) is fetched by **callback-fetch-service**, a container, so
its host is always the compose DNS name. The *detail* URLs are fetched by the
orchestrator's **own tasks**, and fixture-service derives them from the request —
so a host-run fan-out must override the base or it will try to resolve
`fixture-service` from the host and fail.

| Orchestrator | Fan-out runs | DAG 2 URL |
|---|---|---|
| Airflow, Prefect (host), Dagster, Luigi, Temporal, Hatchet | host process | `http://fixture-service:8099/books?base=http://localhost:8099` |
| Kestra, `prefect/deploy_docker.py` | container on the compose network | `http://fixture-service:8099/books` |
| Argo, Flyte | pod, in-cluster | `http://fixture-service:8099/books` |
| Step Functions | AWS Lambda, outside the cluster | `https://orch-fixture.<domain>/books` (public ingress) |

Every implementation's normalize step now reads
`item.get("title") or item.get("name") or item.get("id")`, so pointing the URL
somewhere else — including back at GitHub — is a config change, not a code change.

### Dataset and provenance

The corpus is an extract of **Open Library**, whose bibliographic metadata is
dedicated to the public domain under **CC0 1.0** — so it is redistributable here
without restriction. The records are real: real titles, authors, publication years,
publishers, ISBNs, edition counts, page counts and ratings. Identifier formats are
Open Library's own (`OL…W` works, `OL…A` authors), so the fixture looks like the
service it stands in for.

```bash
uv run --no-project shared-services/fixture-service/build_dataset.py --target 5000
```

It round-robins ~40 subjects so the corpus isn't single-genre, dedupes by work key,
and sorts by id so the artefact is byte-stable across rebuilds. **Open Library 503s
frequently under load**, so requests are retried with backoff, paced, and
checkpointed after every page; `--resume` extends an existing file. A 5k build runs
for an hour or two, which is exactly why the artefact is built once and then lives
in S3 rather than being fetched at image-build or start time.

Two paths deliberately not taken:

- **Goodreads.** Amazon [retired the public API in December 2020](https://developers.slashdot.org/story/20/12/17/1522242/goodreads-is-retiring-its-current-api-and-book-loving-developers-arent-happy)
  and stopped issuing keys, so there is no legitimate programmatic source.
- **Open Library's monthly bulk dumps.** The right tool for millions of records,
  but `ol_dump_works` alone is ~2.9 GB compressed — far past what a fixture needs
  or what belongs in a git repository.

`test-data/` holds only the generator now — `sample-data.zip` is gitignored build
output, and DAG 2's items are generated per request rather than written to disk.

---

## 1. The shared backbone (always first)

```bash
just up                         # postgres + callback-fetch + approval + shipping + fixture
```

This starts:

| Service | Host port | Purpose |
|---|---|---|
| postgres | 54321 | DB for all DAGs (+ empty `hatchet`/`kestra` DBs); non-standard host port to avoid clashing with a local Postgres |
| callback-fetch-service | 8090 | DAG 2 async fetch + callback |
| approval-service | 8091 | DAG 4 human approval |
| shipping-service | 8092 | DAG 4 flaky shipping API |
| fixture-service | 8099 | Mock Books API for DAG 2 + DAG 1's ZIP (§0b) — replaces DAG 2's GitHub default |

Each orchestrator engine is behind a **compose profile**, so the default is to
run one at a time: `just up <name>` (e.g. `just up temporal`, and likewise
`hatchet`, `kestra`, `conductor`).

**The table above is a port *reservation* map, not a status map.** Only four
tools have an engine container; Airflow, Dagster, Prefect and Luigi are
libraries run in your own venv, so no `just up*` recipe can start them and
3000 / 4200 / 8080 stay unowned until you do. Run
`./shared-services/check-ports.sh` to see what is actually listening, and
**`just py-up`** to start the four that serve a UI (a thin wrapper over
`scripts/py-procs.sh`, whose header carries the detail):

```bash
just py-up                     # Dagster :3000, Prefect :4200, Airflow :8080, luigid :8082
just py-up dagster prefect     # or name them
just py-status                 # what is up, and where
just py-down                   # stop them
```

These bind **0.0.0.0**, not 127.0.0.1, so a browser outside the VM (a WSL2 host,
another machine) can reach them. Airflow already defaults there; Dagster needs
`-h $DAGSTER_HOST` (its `env.sh` sets it) and Prefect needs
`PREFECT_SERVER_API_HOST`. `py-down` signals the whole **process group**: both
`dagster dev` and `airflow standalone` are supervisors, and killing the parent
alone orphans the children onto their ports — measured at 13 surviving processes
for Airflow. Workers and relay servers are not included; start those per the
per-tool sections below.

`luigi` here means **`luigid` only**, Luigi's optional central scheduler. It is
not needed to run anything — every documented Luigi invocation uses
`--local-scheduler` and never contacts it; point tasks at it with
`--scheduler-host localhost --scheduler-port 8082` instead. It needs a
`setuptools<81` pin to start at all (luigi 3.7.1 imports `pkg_resources`, removed
in setuptools 82) and an explicit `--state-path`, or it silently discards its
state on shutdown. Both are handled in the recipe; see `luigi/README.md`.

**`just up-all`** starts the backbone plus all four engines together. That is
safe — §0 gives every engine its own host port, and all four profiles resolve
with zero collisions — but heavier, since Kestra and Conductor are each a JVM.
Prefer one at a time unless you are comparing engines side by side.

All four engines share the one Postgres, and together they used to exhaust it:
measured on a bare `up-all`, temporal held **67** connections, hatchet 13,
conductor 10, kestra 10 — exactly the stock `max_connections=100`, with nothing
left for the mock services, host-run workers, or a GUI client like DBeaver. The
symptom is `FATAL: sorry, too many clients already`, and because the
superuser-reserved slots go too, even `podman exec … psql` is locked out, which
makes it look like the server is down rather than full. Fixed in
`docker-compose.yml` (2026-08-12) from both ends: `max_connections=300` on the
server, and caps on Temporal's pools (`SQL_MAX_CONNS`, `SQL_VIS_MAX_CONNS` — its
auto-setup image runs frontend/history/matching/worker in one container and each
opens its own pools, defaulting to 20 + 10 *per service*). Now ~62 of 300 in use
with everything up. Both are container-recreate changes, so a running stack needs
`just down-all && just up-all`, not a restart.

`up-all` deliberately does **not** start the `temporal-worker` and
`hatchet-worker` sidecars — it names the engine servers explicitly, because
`--scale <svc>=0` did *not* reliably suppress them under podman-compose. Those
are the only two
containerised workers, and running one alongside a host worker on the same task
queue silently splits runs between two processes with different code —
`hatchet-worker` would also need a `HATCHET_CLIENT_TOKEN` that has no default.
Run workers on the host, as each tool's section below describes.

Whatever you start, **stop it with `just down-all`** — a bare `just down` does
not reach profile-gated services (see §Teardown).

---

## 2. The callback-networking rule (read once)

DAG 2 and DAG 4 use an **async callback**: the orchestrator hands a
`callback_url` to a mock service (a container), and the service later POSTs the
result *back*. So the host in `callback_url` must be resolvable **from inside
the mock-service container**:

- **Callback target is itself a container** (Hatchet engine, Kestra server) →
  use the **compose service name** (`hatchet-engine`, `kestra`). Same network,
  just works. Runtime-independent.
- **Callback target runs on the HOST** (the Temporal signal server) → use your
  runtime's **host-gateway hostname**, *not* `localhost`. `localhost` inside a
  container is the container itself, so the callback silently times out.

  | Runtime | Host-gateway hostname |
  |---|---|
  | Docker Desktop | `host.docker.internal` |
  | Podman (4.7+) | `host.containers.internal` (also aliases `host.docker.internal`) |
  | Finch | `host.docker.internal` → `192.168.5.2` (the Lima gateway; **verified**). Caveat: finch does **not** reliably re-add this to a container on `finch compose up` **recreate**, so pin it explicitly with `extra_hosts: ["host.docker.internal:192.168.5.2"]` on any service that must call back to the host (the callback-fetch / approval services do). |

  Because `host.docker.internal` works on Docker and (as an alias) on Podman
  4.7+, the examples below use it as the default; podman users can equally use
  `host.containers.internal`. Every host is an env var (or, for Kestra, server
  config) — **nothing is hard-coded** — so switching runtimes is pure
  configuration.

The **polling** orchestrators — **Airflow, Dagster, Prefect, Luigi** — don't use
callbacks at all (they poll `GET /status` on the services), so none of this
applies to them. Host→published-port over `localhost` is fine.

---

## 2b. The resume-broker model (read once)

The callback-fetch and approval services are **resume brokers**. Instead of
auto-POSTing a result when work finishes, they store a provider-specific
*resume handle* at registration and perform the resume only when explicitly
triggered:

- **Register:** `POST /fetch-async` (or `/approval-requests`) with `provider` +
  `resume_data`. `stepfunctions` → `{task_token, region?}`; `http_callback` →
  `{callback_url}`. A bare `task_token` or top-level `callback_url` infers the
  provider, so existing callers keep working.
- **Resume:** for the fetch service, `POST /resume/<correlation_id>`; for the
  approval service, `POST /approval-requests/<id>/decide` (which decides *and*
  resumes). The `stepfunctions` provider calls `SendTaskSuccess`/`Failure` in
  process (needs AWS creds + region); `http_callback` POSTs the result to the
  stored URL — this is the path the container-networking rule above governs.
- **Inspect:** `GET /requests` / `GET /approval-requests` (add `?status=pending`)
  to see what's registered and awaiting a resume.

Consequence for hands-off runs:

- **Fetch service (DAG 2)** doesn't auto-fire by default. Either set
  `AUTO_RESUME=true` on the `callback-fetch-service` (in `docker-compose.yml`)
  so it resumes itself when the fetch completes — the hands-off option — or
  call `POST /resume/<correlation_id>` yourself (a harness, or by hand after
  `GET /requests?status=completed&resumed=false` shows the id). Leaving it
  manual, or submitting a single request with `auto_resume: false`, is how the
  timeout / duplicate / late-callback edge cases get exercised.
- **Approval service (DAG 4)** keeps `AUTO_DECIDE_*` (set in compose), so it
  decides *and* resumes on its own after the delay — DAG 4 still runs hands-off.

**Step Functions** now registers directly with the broker (task token in
`resume_data`); the old relay Lambdas are gone. For the SFN path to actually
resume, the service container needs AWS credentials and `AWS_REGION` in its
environment (SFN itself remains out of scope for local runs).

---

## 3. Python-native orchestrators (no engine container)

Install the project deps once: `uv sync`. Then run each from its own directory.

> **Python version caveat:** `pyproject.toml` pins `requires-python >=3.14`.
> If `temporalio`/`hatchet-sdk`/`luigi` don't yet ship 3.14 wheels, create a
> throwaway 3.12 venv for those workers: `uv venv --python 3.12 .venv-workers`
> and `uv pip install temporalio hatchet-sdk luigi httpx psycopg[binary] pyarrow`.

### Airflow
```bash
cd airflow
just seed airflow                                           # airflow_dag{1,3,4} schemas
source env.sh                                               # AIRFLOW_HOME, PYTHONPATH, service URLs
uv run --project .. airflow db migrate                      # first time only
uv run --project .. airflow standalone                      # UI on :8080
```
`env.sh` exports `PYTHONPATH=$PWD`, which is **required**: the triggerer does
not add the DAGs folder to `sys.path`, so without it the custom triggers fail to
import and DAG 2/4 hang in `deferred` indefinitely with no visible error.
Polls the fetch/approval services — no callback wiring needed, but registration
still has to declare a provider, so both DAGs register the dead-URL placeholder
(see §2b). The `ETL_*` paths and the service URLs default to container values
and must be overridden on the host; `airflow/README.md` has the full block, the
verified run commands, and the per-DAG knobs.

### Dagster
```bash
source dagster_bakeoff/env.sh                               # from the repo root
uv run dagster dev -m dagster_bakeoff.repository            # UI on :3000
```
The directory is `dagster_bakeoff/`, not `dagster/`: a local package named
`dagster` shadows the installed library and no code location will load under
either `-f` or `-m`. Always load it as a **module from the repo root**.

`env.sh` must be sourced first. Besides the usual host overrides it sets
`DAGSTER_HOME`; without it every command gets a throwaway instance, sensor
cursors reset each tick, and DAG 2/4 never advance past their first job.

DAG 2/4 waits are handled by the sensors in `sensors.py` polling `/status` and
`/approval-requests` — Dagster cannot suspend a run, so each of those DAGs is
two jobs bridged by a sensor, and saga compensation is a third job triggered by
a `@run_failure_sensor`. Registration declares the dead-URL placeholder
provider (see §2b). Use `dagster job launch` (not `job execute`) for anything
whose failure must compensate: `execute` runs in-process and never reaches the
instance's run launcher, so no run-status sensor fires. `dagster_bakeoff/README.md`
has the verified per-DAG commands and run configs.

### Prefect
```bash
cd prefect
uv run prefect server start                                 # UI on :4200, separate shell
uv run python dag1_csv_etl.py                               # run a flow
```
`callback_url` is a no-op placeholder; the flows poll.

### Luigi
```bash
cd luigi
just seed luigi                       # luigi_dag{1,3,4} -- required, see below
# Task name and parameters are mandatory; there is no default target.
uv run --project .. python dag3_payment.py SendNotification \
  --payment-id PAY-001 --amount 100.00 --currency USD \
  --from-account ACC-001 --to-account ACC-003 \
  --run-id PAY-001 --local-scheduler
```
No callback support by design (DAG 2 polls synchronously, holding a worker for
the whole wait — Luigi has no suspend).

Two Luigi-specific gotchas, both verified:

- **The `luigi --module <mod>` form needs `PYTHONPATH` to include `luigi/`**
  (`luigi` is a console script, so `sys.path[0]` is the venv's `bin/` and the
  bare `__import__` fails). Running the file directly, as above, does not — each
  DAG ends in `luigi.run()`.
- **Only the *root* task's parameters are bare CLI flags.** `--max-retries`
  belongs to `ProcessPayment`, so it must be `--ProcessPayment-max-retries`;
  otherwise Luigi exits with `unrecognized arguments`.

DAG 2's fan-out runs on the host, so its URL needs `&base=http://localhost:8099`
appended (§0b's host-run rule) — without it the detail fetches try to resolve
`fixture-service` from the host and hang until the poll timeout.

All four reach Postgres at `localhost:54321` (the non-standard host port from
the compose file) and the mock services at `localhost:8090–8092` (published
ports). The repo's `.envrc` exports `POSTGRES_HOST=localhost` and
`POSTGRES_PORT=54321`, so with **direnv** loaded no overrides are required;
without direnv, set those two vars in your shell first (the code otherwise
defaults to `postgres:5432`, the in-compose address, which won't resolve on the
host).

---

## 4. Temporal (server in compose, worker on host)

```bash
just up temporal                            # temporal:7233, UI on :8233

# The server needs ~30-60s to acquire its shards; until it does, every client
# call fails with "shard status unknown" / "Timeout expired".
until podman logs shared-services_temporal_1 2>&1 | grep -q "Acquired shard"; do sleep 3; done

# `up temporal` also starts a temporal-worker CONTAINER that polls the same task
# queue as the host worker below. Stop one or the other.
podman stop shared-services_temporal-worker_1
```

Run the **worker** and **signal-relay server** on the host. The signal server
is the callback target, and it runs on the host — so the mock-service
containers must reach it via your runtime's host-gateway hostname (§2):

```bash
cd temporal

# Worker
TEMPORAL_ADDRESS=localhost:7233 \
POSTGRES_HOST=localhost \
POSTGRES_PORT=54321 \
CALLBACK_FETCH_SERVICE_URL=http://localhost:8090 \
APPROVAL_SERVICE_URL=http://localhost:8091 \
SHIPPING_SERVICE_URL=http://localhost:8092 \
SIGNAL_SERVER_URL=http://host.docker.internal:8095 \
  uv run python worker.py

# Signal relay server (separate shell)
TEMPORAL_ADDRESS=localhost:7233 \
  uv run uvicorn signal_server:app --host 0.0.0.0 --port 8095
```

Why the host-gateway host in `SIGNAL_SERVER_URL` (here `host.docker.internal`;
podman users can use `host.containers.internal`): the worker bakes this host
into the `callback_url` it gives the fetch/approval **containers**, which then
POST back to your host's signal server. The worker itself reaches everything
else over `localhost` (published ports), hence the other overrides (the code
defaults to compose DNS names, which don't resolve on the host).

Also export `BAKEOFF_NS=temporal` on the worker (schema isolation) and run
`just seed temporal` first. Workflows are started by a client, not a
deployment — `temporal/start_workflow.py` has one subcommand per DAG plus flags
for the failure branches; see `temporal/README.md`.

---

## 5. Hatchet (engine in compose, worker on host)

```bash
just up hatchet                             # engine API on :8888, gRPC on :7077
```

Hatchet needs a **client token** that can only be minted after the engine is
up, so it can't be baked into compose. Generate one, then run the worker:

```bash
# 0. The compose worker would race the host one for the same actions.
podman stop shared-services_hatchet-worker_1
just seed hatchet                          # hatchet_dag{1,3,4} schemas

# 1. Mint a token (verified subcommand; the `default` tenant is seeded by
#    hatchet-lite -- list them with:
#    `$COMPOSE exec postgres psql -U orchestration -d hatchet -c 'select id,name from "Tenant";'`)
$COMPOSE exec hatchet-engine \
  /hatchet-admin token create --config /config \
  --tenant-id 707d0855-80ab-4e1f-a156-f1c4546cbf52 --name bakeoff \
  | tail -1 > shared-services/hatchet.token          # gitignored

# 2. Worker and event relay on the host, two shells:
source hatchet/env.sh                                # from the repo root
cd hatchet
uv run --project .. python worker.py
uv run --project .. uvicorn event_relay:app --host 0.0.0.0 --port 8096
```

**Source `hatchet/env.sh`** rather than exporting by hand — it carries four
settings that fail silently or misleadingly when wrong:

- `HATCHET_CLIENT_SERVER_URL=http://localhost:8888`. The minted token embeds
  `server_url: http://localhost:8080`, and the SDK trusts it. hatchet-lite's API
  is published on **8888**, and **:8080 on this host is Airflow** — without the
  override, Hatchet SDK calls return Airflow's `"/api/v1 has been removed in
  Airflow 3"` error.
- `HATCHET_CLIENT_HOST_PORT=localhost:7077`. The token's
  `grpc_broadcast_address` is `hatchet-engine:7070`, which only resolves inside
  compose.
- `HATCHET_CLIENT_NAMESPACE=bakeoff`. A worker killed with SIGKILL stays
  **ACTIVE** in the engine, keeps getting assigned durable tasks it will never
  run, and is not cleared by an engine restart. A namespace gives a restarted
  worker its own action names. **Stop workers with SIGTERM.**
- The token is read from the **file**, overriding any inherited
  `HATCHET_CLIENT_TOKEN` — `.envrc` exports a stale JWT from an older engine and
  it otherwise wins, failing as a bare `UNAUTHENTICATED: invalid auth token`.

The callback target is **`event_relay.py` on the host**, not the engine: Hatchet's
event endpoint needs a bearer token and a `{key, data}` envelope, which the mock
services don't send. `HATCHET_EVENT_RELAY_URL` therefore defaults to the host
gateway as a *container* sees it (`host.containers.internal:8096` on Podman, §2).
The relay must run in the same namespace as the worker — the SDK namespaces event
keys on both the push and the wait side.

Trigger the DAGs with `uv run --project .. python start_workflow.py dag{1,2,3,4}`;
see `hatchet/README.md` for the per-DAG flags and edge cases.

---

## 6. Kestra (everything in the container)

Kestra runs the flow YAMLs itself — no separate worker. But script tasks default
to the **Docker task runner**, which launches a sibling container per task, so
the server needs a Docker-compatible Engine API socket:

```bash
systemctl --user enable --now podman.socket                            # once
just up kestra                                                        # UI :8081
```

The host port defaults to **8081** (§0's port map — 8080 is Airflow's). Override
with `KESTRA_PORT`. That is the host side only; in-network callbacks still use
`kestra:8080`, and `kestra.url` is unchanged.

**DAG 1 and DAG 2 need `fixture-service`** (§0b), which `just up` already
started — Kestra's DAG 1 downloads its ZIP from a URL rather than reading a path.
Address it as `fixture-service:8099` from the flows; it is on the same compose
network, so no host gateway is involved.

Load the flows over REST (see `kestra/README.md` §3 for the loop). The
`$COMPOSE exec kestra kestra flow namespace update ...` form documented here
previously **does not work**: there is no `kestra` binary on `PATH` (it is
`java -jar /app/kestra`), `flow validate` defers to `kestractl`, and the CLI's
API calls are unauthenticated against a basic-auth server.

**Resume wiring.** `kestra.url: http://kestra:8080/` in the compose config is
still what keeps resume targets on the shared network, but Kestra does **not**
expose an `execution.resumeUrl` variable — no such thing exists in any version.
Flows register `{{ execution.id }}` with the mock services, whose `kestra`
provider builds `POST /api/v1/{tenant}/executions/{id}/resume` itself. That
endpoint needs credentials (401 otherwise) and `multipart/form-data` (415 on
JSON), which is why it is a distinct provider and not `http_callback`; the
credentials come from `KESTRA_USER` / `KESTRA_PASSWORD` on both mocks. For DAG 4
the handle is the **subflow's** execution id — the `Pause` lives there, so
resuming the parent is a no-op.

**Kestra dies when Postgres goes away, and can restart into a wedged state.**
It uses Postgres for both the repository *and* the queue, and treats a lost
connection as fatal: every queue-consumer thread logs `Fatal error while polling
the '<type>' queue. Initiating shutdown.` It does not reconnect and there is no
setting to make it (kestra-io/kestra#4076, #5147, #10358). Measured on 1.3.30:

| Scenario | Outcome |
|---|---|
| ~5s blip | all consumers log the fatal error, process **survives** |
| Postgres back *before* Kestra restarts | exits 0, restart policy fires, **self-heals** in ~45–90s |
| Postgres still down *when* Kestra restarts | `UnknownHostException: postgres`, JVM stays alive, **never retries** — wedged indefinitely |

Compose now sets `restart: unless-stopped` on every service. It **must** be
`unless-stopped`, not `on-failure`: the exit status is **0**, so a failure-only
policy never fires.

The wedge is why there is also a **healthcheck** on `/ping` (which needs no
credentials, unlike everything under `/api`). Wedged, the container still reports
`running` with a live JVM — the probe is the only thing that distinguishes it, and
it flips to `unhealthy` after ~150s (90s `start_period` + 4×15s). Neither podman
nor docker restarts an unhealthy container, so recovery is manual and takes about
18 seconds:

```bash
podman restart shared-services_kestra_1
```

**Practical consequence:** do not bounce `postgres` while Kestra is running.
`podman compose up -d <other-service>` recreates it as a side effect (and has been
seen to remove the kestra container outright), which is how this was hit six times
during testing. Prefer `podman start` / `podman restart` on individual containers
once the stack is up. In-flight executions are lost either way; flow definitions
survive, since the repository is Postgres-backed too.

---

## Quick reference: callback target per orchestrator

| Orchestrator | Wait mechanism | Callback target | Host to use |
|---|---|---|---|
| Airflow / Dagster / Prefect / Luigi | polling | n/a (orchestrator polls) | — |
| Temporal | signal relay (host process) | signal server | host-gateway:8095 (§2) |
| Hatchet | event ingestion | engine container | `hatchet-engine:8888` |
| Kestra | pause/resume webhook | server container | `kestra:8080` (via `kestra.url`) |
| Conductor | `WAIT` task + task-update API | server container | `conductor-server:8080` (via `CONDUCTOR_INTERNAL_URL`) |

---

## 6b. Conductor (server in compose, workers on host)

One container: Spring Boot API on :8080 and nginx serving the UI on :5000
internally, published as **:8000** and **:8127**. **No Elasticsearch** —
`conductor.indexing.type=postgres` puts metadata, task queues and the search
index in the shared Postgres, in a `conductor` database.

```bash
just up                 # backbone first
just up conductor       # + the server
just seed conductor     # conductor_dag1 / _dag3 / _dag4

# first boot runs Flyway migrations -- wait for health, do not race it
until curl -sf -o /dev/null http://localhost:8000/health; do sleep 3; done

source conductor/env.sh
uv run python conductor/register.py       # push workflow + task definitions
uv run python conductor/worker.py         # hosts all 26 task types
```

Then in another shell:

```bash
source conductor/env.sh
uv run python conductor/start_workflow.py dag1 --wait
```

UI at <http://localhost:8127> — no login, because Conductor OSS has no
authentication at all.

**Configure the engine with a mounted FILE, not environment variables.**
`shared-services/conductor/config.properties` is mounted to
`/app/config/bakeoff.properties` and named by `CONFIG_PROP`. Setting
`SPRING_DATASOURCE_URL` instead does nothing: Conductor reads the config file and
turns every key into a Java *system property*, which outranks OS environment
variables in Spring Boot. The failure mode is a silent `HikariPool-1 - Starting...`
loop with the real cause buried in a nested bean stack trace.

**Stop the worker with `./conductor/stop_worker.sh`, never `pkill -f worker.py`.**
The Python SDK runs each task type in a spawned child whose cmdline is
`python -c from multiprocessing.spawn import spawn_main; ...` — no mention of
`worker.py`. Killing the supervisor orphans all 26; they keep polling and keep
executing tasks with stale code, so a "restarted" worker silently competes with
every previous generation. 114 orphans accumulated across three restarts during
testing, and the symptom looked like a flaky external service.

Two more things that cost time and are worth knowing before you debug them:

- **Task timeouts fire late.** They are sweeper-enforced, not timer-driven: a 60s
  `timeoutSeconds` was measured firing at 103s, a 120s one at 180s. Treat the
  value as "not before".
- **`conductor.app.sweeperFrequencyMillis` does not exist** in 3.31.0, despite
  being widely recommended. Spring ignores unknown keys and `/api/admin/config`
  echoes back what you *set*, not what it *bound*, so a dead property looks
  applied. That endpoint validates nothing.

Full detail, including all fifteen defects, in `conductor/README.md`.

---

## Status / caveats

- Compose **network + env wiring** is the verified-by-analysis part. Remaining
  engine **image tags** are best-effort from docs and should be shaken out on a
  first real run — they're flagged inline above and in `docker-compose.yml`.
- **Hatchet is now verified by running it** (2026-08-03): all four DAGs
  end-to-end, §5 rewritten. hatchet-lite's port layout (gRPC 7070→7077, API 8888)
  and the `hatchet-admin token create` subcommand are both **confirmed correct**.
  What was wrong was the callback story: Hatchet's event API can't be a raw
  callback target, so `hatchet/event_relay.py` bridges it. See
  `hatchet/README.md`.
- **Kestra is now verified by running it** (2026-08-04, 1.3.30): all four DAGs
  end-to-end, §6 rewritten accordingly. The previously documented flow-loading
  command and the `execution.resumeUrl` callback story were both wrong — see
  `kestra/README.md` for the full defect list. Treat this as the cautionary case
  for the remaining untested tools: the doc-derived instructions were confidently
  specific and confidently incorrect.
- `init-engines.sql` (the empty `hatchet`/`kestra` DBs) only runs on a **fresh**
  `pgdata` volume. On an existing volume, create them manually (see that file).

**Kubernetes (§7–§9) — what is and isn't verified.** Verified today by inspecting
the live clusters and the upstream registries: node
architectures, available StorageClasses and ingress controllers, arm64 manifests
for every image the stack pulls, flytekit's `ImageSpec` platform-resolution logic,
Argo's failed-workflow history, and the absence of both a bake-off backbone and
Flyte's configured minio.

**Verified by deploying it (2026-08-03):** the §7c backbone is **live on the
arm64 cluster** in namespace `orchestrators` — Postgres (50 GiB `oci-bv` PVC,
`init-db.sql` applied on a fresh volume, `argo_*` and `flyte_*` schemas seeded)
plus all four mock services, each 1/1 Ready (fixture-service was added later;
all five workloads are Helm-managed as of 2026-08-12). Smoke-tested from inside the
cluster: TCP to all four, `GET /requests` and `GET /approval-requests` both 200,
and a real `POST /shipments` returning `shipped`. The pods were also confirmed to
be running current source (`@app.` route counts match the local `app.py`, and the
resume-broker endpoints are present).

**Both orchestrators are installed on the arm64 cluster (2026-08-03)** — Argo `v4.0.8` and
`flyte-core-v1.16.8`, all pods Running, with the §9b blob store deployed and its
bucket created.

**Argo DAG 3 and DAG 4 both run green end to end**, DB side effects confirmed in
`argo_dag3` / `argo_dag4`, and DAG 4 verified on *both* its approval-required and
skip-approval branches. Getting DAG 4 there took five distinct fixes, each masking
the next — see §8.

**All four Flyte DAGs now execute green on the arm64 cluster** (2026-08-06), verified by
DB side effects and workflow outputs. Argo DAG 3 and DAG 4 are green too, so on
Kubernetes both orchestrators run the full suite except Argo DAG 1 and DAG 2
(fixture wiring + the GitHub rate limit — Argo DAG 2 should be repointed at the
in-cluster fixture-service, which is now deployed and aliased).
**Still untested for Flyte:** saga compensation (rejection / timeout / shipping
failure), DAG 3's decline branch, and idempotent re-runs.

Three code-level gaps that previously stood between these instructions and a
green DAG are **now fixed and verified against the local Postgres** (2026-08-03):

1. **`ImageSpec` platform** — all four `flyte/dag*.py` now pass
   `platform=os.environ.get("FLYTE_IMAGE_PLATFORM", "linux/amd64")`. Verified:
   unset → `linux/amd64`, `FLYTE_IMAGE_PLATFORM=linux/arm64` → `linux/arm64`
   on every image (§7b).
2. **`BAKEOFF_NS` schema isolation** — implemented in both trees. Argo carries it
   as a `bakeoff-ns` workflow parameter → `BAKEOFF_NS` env → `search_path` in all
   **12** inline DB steps (and the 7 unreferenced `argo/scripts/*.py` copies);
   Flyte carries it as `DBConfig.namespace`, since task pods don't inherit the
   launching environment. Verified by executing every connect-and-scope block
   against the real Postgres: all 12 Argo sites and all 3 Flyte helpers land in
   the correct `<ns>_dagN`, DAG 1 self-creates, DAG 3/4 fail fast with a
   `bootstrap_bakeoff` hint. **§7c's role-level `search_path` workaround is
   therefore no longer needed** — it is kept there only as a fallback.
3. **Stale fixture ids** — `argo/dag3-payment.yaml` used `ACCT-001`/`ACCT-002`
   (nonexistent); now `ACC-001` → `ACC-003`. `argo/dag4-order-fulfillment.yaml`
   used `CUST-001`/`SKU-A`/`SKU-B`; now `CUST-42` with `WIDGET-A`/`GADGET-B`,
   totalling 559.97 so the default input exercises the approval path. All ids
   confirmed present in the seeded schemas.

---

## 7. Kubernetes: shared setup (read once)

Argo and Flyte are the two Kubernetes-only orchestrators. §8–§9 are written
against a handful of shell variables so the **same commands work on any
cluster** — an earlier draft was specific to one provider and silently assumed
amd64 nodes, no usable StorageClass, and no ingress controller.

### 7a. Pick a cluster

`kubectl ctx` (or `kubectl config get-contexts`) lists what you have.

**One cluster is in use, and `$KCTX` must point at it.** Node architecture is
load-bearing rather than cosmetic: it decides whether your task images run at all.

| | the cluster |
|---|---|
| Platform | OCI, 2 worker nodes, Oracle Linux 8.10, cri-o 1.36 |
| Node arch | **`arm64`** (aarch64) |
| Default StorageClass | `oci-bv` (`WaitForFirstConsumer`), plus `oci` |
| Ingress | Traefik (default class) + cert-manager |

Export once per shell — or better, put these in `.envrc` (see `.envrc.example`,
which is the committed template; `.envrc` itself is gitignored). **Every command
below passes `--context "$KCTX"` explicitly** — never rely on the current
context, since this machine has other kube contexts that are out of scope here:

```bash
export KCTX=my-arm64-cluster          # your kube context name
export ORCH_NS=orchestrators          # namespace holding the shared backbone (§7c)
export TARGET_ARCH=arm64              # MUST match the nodes; arm64 is the only cluster in use
export STORAGE_CLASS=oci-bv           # "" if no dynamic provisioning (falls back to emptyDir)
export INGRESS_CLASS=traefik          # ingress controller class, if you have one
export INGRESS_ENABLED=false          # true only if something OUTSIDE the cluster must call in
export CLUSTER_ISSUER=letsencrypt-prod
export BASE_DOMAIN=example.com        # YOUR domain — DNS you control (§7d)
export K8S_REGISTRY=...               # a registry BOTH your build host and the cluster can reach
```

Both installs track **latest** — no version pinning, which is fine for a bake-off
of *current* capability. The one obligation that follows: `comparison.md` cites version
numbers, so **record what you actually got** after installing (§8, §9c each show
the one-liner).

Sanity-check the arch assumption rather than trusting it:

```bash
kubectl --context "$KCTX" get nodes \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.nodeInfo.architecture}{"\n"}{end}'
```

### 7b. The architecture rule (read once)

Every image the cluster runs must have a manifest for `$TARGET_ARCH`. Most of
the stack already does; there is exactly **one** trap.

| Image | arm64? | Action |
|---|---|---|
| `quay.io/argoproj/{argocli,workflow-controller,argoexec}` | yes | none |
| `cr.flyte.org/flyteorg/{flyteadmin,flytepropeller,flyteconsole,datacatalog}-release` | yes | none |
| `python:3.12-slim` (every Argo DAG step, every mock-service base) | yes | none |
| `postgres:15-alpine`, `postgres:16`, `quay.io/minio/minio` | yes | none |
| **Flyte task images (`ImageSpec`)** | **no — built by you, defaults to amd64** | **set `platform`** |
| **Mock-service images** | **no — built by you** | **build for `$TARGET_ARCH`** |

Every "yes" row was verified with `podman manifest inspect`, not assumed — at the
tags of the earlier install (`argoproj` `v4.0.6`, `flyteorg` `v1.16.7`) plus the
base images above. Both projects publish arm64 consistently, so `latest` is
expected to be fine; if a pull ever fails on arm64, re-run the check:

```bash
podman manifest inspect <image>:<tag> | \
  python3 -c "import sys,json;print(sorted({m['platform']['architecture'] for m in json.load(sys.stdin)['manifests']}))"
```

**The `ImageSpec` trap.** `flyte/dag*.py` construct `ImageSpec(...)` with no
`platform`. In flytekit 1.16.26 that resolves to **`linux/amd64`** unless you are
pushing to a *local* registry from an arm64 build machine
(`flytekit/image_spec/image_spec.py`: `platform is None` → `linux/arm64` only in
that narrow case, else `linux/amd64`). This build host is x86_64 WSL2, so every
Flyte task image would come out amd64.

The failure mode is nasty: the image **pulls fine** and the pod **schedules
fine**, then the container dies with `exec format error`. It reads like a broken
entrypoint, not an arch mismatch. Make it env-driven so one code path serves both
clusters:

```python
platform=os.environ.get("FLYTE_IMAGE_PLATFORM", "linux/amd64")
```

```bash
export FLYTE_IMAGE_PLATFORM=linux/$TARGET_ARCH
```

The `flyte/` DAG files do **not** do this yet — see "Status / caveats".

**Mock services.** `terraform/aws/scripts/build-push-mock-services.sh`
hard-codes `--platform linux/arm64` (it was written for the arm64 K3s cluster).
That is correct for this cluster and wrong for an amd64 one; parameterise it on
`$TARGET_ARCH` before using it for both.

### 7c. The in-cluster backbone (the step that was missing)

**Neither Argo nor Flyte can complete a DAG without this.** It is the reason the
the only three bake-off workflows ever submitted to the earlier install all failed on 2026-06-18
while `hello-world` succeeded:

```
hello-world-9qwvz          Succeeded
csv-etl-pipeline-mm2dv     Failed
api-fanout-lz29q           Failed
payment-processing-lh4cd   Failed      # DAG 4 was never submitted at all
```

The DAG step containers resolve `PGHOST=postgres` and
`http://shipping-service:8092` — compose DNS names — and **no Postgres and no
mock service exists in that cluster** (`flyte/postgres` is Flyte's own metadata
DB, not the bake-off DB). There was nothing to connect to. §1's `just up` gives
you the backbone *locally*; a Kubernetes cluster needs its own copy.

Two hard constraints shape the deployment:

1. **The Service names must match the compose names**, because the Argo DAG YAML
   sets `PGHOST: "postgres"` and the mock-service hostnames as *literal env
   values* in every container spec. So: Services named `postgres`,
   `callback-fetch-service`, `approval-service`, `shipping-service`.
2. **Bare names only resolve in the pod's own namespace.** Argo runs its pods in
   `argo`; Flyte runs task pods in per-project namespaces
   (`bakeoff-development`, …). Rather than one Postgres per orchestrator,
   deploy the backbone once and **alias it** into each workflow namespace with
   `ExternalName` Services (`alias-backbone.sh`, below). Flyte doesn't strictly
   need the aliases — its DB settings are a typed `DBConfig` **input**, so it can
   take an FQDN directly — but Argo does.

#### Deploy

**Everything is one Helm chart** (`shared-services/deploy/`) as of 2026-08-12.
`deploy-backbone.sh` is now a 78-line wrapper over it, not a second
implementation — it renders the ConfigMaps the chart cannot template, then runs
`helm upgrade --install -f values-incluster.yaml`.

```bash
cd shared-services/deploy

# Postgres + the four mock services. Idempotent -- safe to re-run.
KCTX=my-cluster BASE_DOMAIN=example.com ./deploy-backbone.sh

# Then make the compose names resolve in each workflow namespace:
WORKFLOW_NS=argo ./alias-backbone.sh
```

Knobs live in `values-incluster.yaml` (`postgres.storageClass`,
`postgres.storage`, `packaging`, the two resume secret names) plus `ORCH_NS`,
`BASE_DOMAIN` and `SUBDOMAIN_PREFIX` in the environment. Anything else is
`--set` passed straight through.

**Why it was two things before, and what that cost.** The chart served the AWS
path (prebuilt ECR images, Neon) and the script served this one, and they had
drifted in *both* directions: the script had Postgres and no AWS resume
credentials; the chart had the credentials and no Postgres. Neither gap
announced itself — the missing `aws-resume-creds` Secret surfaced only as Step
Functions DAG 2 reporting `FanOutError` and DAG 4 reporting *"Order rejected or
approval timed out"*, a Kubernetes Secret problem presented as a business
decision two systems away. The chart also rendered Services as bare `approval` /
`callback-fetch` / `shipping`, which would have broken constraint 1 above the
moment anyone used it here, while the AWS path kept working because Lambdas come
in over the public ingress.

**Three credential Secrets are per-cluster and NOT chart-managed** —
`aws-resume-creds`, `google-resume-creds`, `fixture-s3-creds`, all from
`terraform output`. `shared-services/deploy/README.md` has the table and what
each failure looks like. The AWS one is the expensive one to forget.

**Adopting a cluster that ran the old script** needs `--take-ownership`, which
the wrapper passes. Three things bit doing exactly that on the arm64 cluster:
probe handlers are mutually exclusive and a patch *merges* them (so the chart
must match the live handler kind); **removals do not propagate on first adopt**,
because Helm has no prior manifest to diff against, so a stale env var survives
an upgrade that no longer renders it; and a PVC cannot shrink, so
`postgres.storage`/`storageClass` must match what is live.

**Source packaging is registry-free by design** (`packaging: source`, which
`values-incluster.yaml` selects). The mock services are pure Python, so they run
on the public `python:3.12-slim` with `app.py` mounted from a ConfigMap and
dependencies installed by an init container into a shared `PYTHONPATH`. The
chart's other mode (`packaging: image`, the default) uses prebuilt ECR images
for the AWS path. That buys three things worth having:

- **No image build and no cross-architecture step** — the same manifests work on
  amd64 and arm64, because the only images involved are upstream multi-arch ones.
- **No registry credentials.** The Helm chart in this directory needs per-arch
  images plus a pull secret that stays refreshed; on the arm64 cluster the only ECR secret
  is `k8s-ecr-login-renew-docker-secret` in `default`, whose cronjob has
  `TARGET_NAMESPACE=default`, so a copy into another namespace would go stale
  within 6 hours and break image pulls on the next pod restart.
- **The pods cannot run stale code.** CLAUDE.md's "Watch out" warns about mock
  services running a two-month-old image missing the resume-broker API. A
  ConfigMap mount makes that failure mode structurally impossible — and the
  Deployment carries a checksum annotation of `app.py`, so editing the source and
  re-running the script actually rolls the pods.

Note the Helm chart in this directory references `aws.resumeSecretName`
unconditionally via `secretKeyRef`, so without an `aws-resume-creds` Secret its
pods sit in `CreateContainerConfigError` — only the `stepfunctions` resume
provider actually reads those creds (§2b).

#### 7c-i. Public ingress (the off-cluster orchestrators)

In-cluster DNS is enough for Argo and Flyte. It is **not** enough for the two
orchestrators that run outside any cluster: Step Functions lambdas and Google
Workflows executions both call the mock services over the internet, and Google
Workflows additionally needs a *publicly fetchable* items API for DAG 2 — the
callback-fetch service is the one that performs that fetch, so a private URL
cannot work.

`deploy-backbone.sh` grew a `PUBLIC_DOMAIN` step for this, so the same script
covers both audiences and there is no second deployment mechanism to keep in
sync:

```bash
cd shared-services/deploy
PUBLIC_DOMAIN="$BASE_DOMAIN" ./deploy-backbone.sh
```

It creates one Ingress with four hosts (`orch-callback-fetch`, `orch-approval`,
`orch-shipping`, `orch-fixture`), a Traefik `redirectScheme` middleware for
http→https, and **one TLS secret per host** rather than a single SAN cert, so one
failed certificate cannot take the other three offline. Knobs: `CLUSTER_ISSUER`
(default `letsencrypt-prod`), `INGRESS_CLASS` (default `traefik`).

**Item detail URLs need `?base=`, not an env var.** The fixture derives DAG 2's
per-item detail URLs from `FIXTURE_BASE_URL`, falling back to the request host —
and behind a TLS-terminating ingress the request host reads as plain `http`, so
the fan-out would take a 301 before doing any work. The env var is the wrong lever
because the two audiences need different answers: Argo and Flyte pods want
`http://fixture-service:8099/...`, while off-cluster callers need the public
HTTPS host. So the deployment keeps the in-cluster default and off-cluster callers
override per request:

```
https://orch-fixture.example.com/books?per_page=30&base=https://orch-fixture.example.com
```

That is the same idiom `airflow/dag2_api_fanout.py` uses locally
(`?base=http://localhost:8099`). Setting `FIXTURE_BASE_URL` globally would hairpin
every in-cluster fan-out out through the public ingress and back.

**Verified live on the arm64 cluster (2026-08-06):** all four hosts serve HTTPS with
Let's Encrypt certificates, `http` 301s to `https`, and the fixture returns 30
items at `?per_page=30` with `https://` detail URLs.

**Whether certificates can issue before DNS moves depends on the solver.**
the arm64 cluster's `letsencrypt-prod` uses **DNS-01 via Cloudflare** for
`example.com`, so all four certs were issued while the hostnames still
pointed at the old cluster — the ACME challenge is a TXT record and never touches
this cluster. On an HTTP-01 issuer the orders stay `pending` until traffic
actually arrives.

**DNS is the one step that is not automatable from here.** `*.example.com`
is a Cloudflare wildcard pointing at the K3s cluster's public IP, so until
per-host A records exist, these hostnames resolve to the old cluster. Verify the
new cluster first by bypassing DNS entirely:

```bash
curl --resolve orch-fixture.example.com:443:<ingress-ip> \
  https://orch-fixture.example.com/health
```

Get `<ingress-ip>` from the ingress controller's `LoadBalancer` service:

```bash
kubectl --context "$KCTX" -n <ns> get svc traefik \
  -o jsonpath='{.status.loadBalancer.ingress[0].ip}'
```

More specific records win over a wildcard in Cloudflare, so four A records are
enough — the wildcard can stay as it is.

#### Two cluster-specific gotchas, both hit on the arm64 cluster

- **cri-o enforces fully-qualified image names.** `postgres:16` fails with
  `short name mode is enforcing, but image name postgres:16 returns ambiguous
  list` — surfacing as `ImageInspectError`, not a pull failure, which sends you
  looking in the wrong place. Every image needs its registry:
  `docker.io/library/postgres:16`. Hence the script's `IMAGE_PREFIX`. Argo and
  flyte-core manifests are unaffected — they already use `quay.io/…` and
  `cr.flyte.org/…`.
- **OCI block volumes have a 50 GiB minimum.** A PVC requesting 5 Gi binds at
  50 Gi. Harmless, but don't file it as a bug.

#### Seeding

`init-db.sql` defines `bootstrap_bakeoff(ns)` and, exactly like the compose
Postgres, **runs only on a fresh volume**. `deploy-backbone.sh` therefore reloads
the file before seeding, so it works on an existing volume too — the in-cluster
equivalent of `just seed <runner>`.

*Which* runners get onboarded is separate, and instance-specific: `init-runners.sh`
reads `$BAKEOFF_RUNNERS`, set here from `postgres.runners` in
`values-incluster.yaml` (`argo flyte`) and in compose to the 8 host-run tools.

**This used to be hardcoded in `init-db.sql`, and it leaked** (fixed 2026-08-12).
That file ended with `SELECT bootstrap_bakeoff('temporal'); … ('prefect');`, and
both databases mount the same file — so the cluster got two host-run runners it
will never execute. The reload above is what made it stick: `bakeoff-db.sh seed`
re-runs the whole file to refresh the function, so **`just seed flyte` replanted
`temporal_dag*` and `prefect_dag*` on the cluster every single time** (measured:
one seed, both schema sets back). Deleting them did nothing; the next seed
restored them. `init-db.sql` is now side-effect-free, which is what makes
reloading it safe. Clean up strays with
`./scripts/bakeoff-db.sh prune <runner> --from <local|cluster|neon>`.

Both K8s implementations now honour the namespace, so nothing further is needed:

- **Argo** — the `bakeoff-ns` workflow parameter (default `argo`) flows into a
  `BAKEOFF_NS` env var on every DB step, which sets
  `search_path = "<ns>_dagN"`. Override at submit time with
  `-p bakeoff-ns=<runner>`. DAG 1's schema is self-creating; DAG 3/4 fail fast
  with a `bootstrap_bakeoff` hint rather than emitting a confusing
  `relation does not exist`.
- **Flyte** — `DBConfig.namespace` (default from `BAKEOFF_NS` at launch time,
  else `flyte`). It travels as *data* because Flyte task pods do not inherit the
  launching shell's environment, which also makes it overridable per execution.

If you ever run an implementation that still writes unqualified names to
`public`, the zero-code-change fallback is a role-level `search_path`
(`ALTER ROLE orchestration SET search_path = <ns>_dag1, <ns>_dag3, <ns>_dag4,
public;`). It applies per **role**, not per session, so it is fine while one
runner owns the DB and misleading if two do.

#### Verify before touching an orchestrator

```bash
kubectl --context "$KCTX" -n "$ORCH_NS" run netcheck --rm -i --restart=Never \
  --image=docker.io/library/python:3.12-slim --command -- python -c "
import socket
for h,p in [('postgres',5432),('callback-fetch-service',8090),
            ('approval-service',8091),('shipping-service',8092)]:
    try:
        socket.create_connection((h,p),4); print('OK  ',h,p)
    except Exception as e: print('FAIL',h,p,e)"
```

All four must print `OK`. If they don't, stop here — every DAG failure past this
point will be a red herring.

---

## 8. Argo Workflows (Kubernetes)

Installed from the upstream manifest, not Helm. An earlier install ran `v4.0.6`; **this one
runs `v4.0.8`, installed 2026-08-03 and verified by a green DAG 3** (see §8
Notes). New installs take latest.

### Installation

```bash
# Idempotent: alias-backbone.sh (§7c) may already have created this namespace,
# and plain `create namespace` would fail with AlreadyExists.
kubectl --context "$KCTX" create namespace argo \
  --dry-run=client -o yaml | kubectl --context "$KCTX" apply -f -

kubectl --context "$KCTX" apply -n argo --server-side \
  -f https://github.com/argoproj/argo-workflows/releases/latest/download/install.yaml
```

`--server-side` is **required**: some CRDs exceed the 262 KB annotation limit for
client-side apply.

Record the version you landed on, for `comparison.md`:

```bash
kubectl --context "$KCTX" get deploy argo-server -n argo \
  -o jsonpath='{.spec.template.spec.containers[0].image}{"\n"}'
```

Grant the default service account permission to report task results (without
this every step fails at completion):

```bash
kubectl --context "$KCTX" apply -n argo -f - <<'EOF'
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: executor
  namespace: argo
rules:
- apiGroups:
  - argoproj.io
  resources:
  - workflowtaskresults
  verbs:
  - create
  - patch
  - get
  - list
  - watch
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: executor-default
  namespace: argo
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: executor
subjects:
- kind: ServiceAccount
  name: default
  namespace: argo
EOF
```

Disable HTTPS on the server so a plain port-forward works:

```bash
kubectl --context "$KCTX" patch deployment argo-server -n argo --type json -p='[
  {"op":"replace","path":"/spec/template/spec/containers/0/args","value":["server","--auth-mode=server","--secure=false"]},
  {"op":"replace","path":"/spec/template/spec/containers/0/readinessProbe/httpGet/scheme","value":"HTTP"}
]'
```

No arch work is needed — `argocli`, `workflow-controller`, and `argoexec` all
ship arm64 (§7b).

### Backbone

Argo's DAG YAML hard-codes `PGHOST: "postgres"` and the `*-service` hostnames as
literal env values, so run §7c with `ORCH_NS=argo`:

```bash
ORCH_NS=argo   # then follow §7c
```

### Accessing the UI

```bash
kubectl --context "$KCTX" port-forward -n argo svc/argo-server 2746:2746
# http://localhost:2746
```

### Registering and submitting workflows

All four DAGs are **`WorkflowTemplate`s** (changed 2026-08-12 — they used to be
`kind: Workflow` with a `generateName:`, submitted directly). Register once:

```bash
kubectl --context "$KCTX" apply -n argo \
  -f argo/templates/ \
  -f argo/dag1-csv-etl.yaml -f argo/dag2-api-fanout.yaml \
  -f argo/dag3-payment.yaml -f argo/dag4-order-fulfillment.yaml
```

That puts **eight** entries in the Workflow Templates tab: the four DAGs
(`csv-etl-pipeline`, `api-fanout`, `payment-processing`, `order-fulfillment`) and
DAG 4's four sub-workflow templates. Before the change the tab showed only the
latter four, which is why it looked unrelated to the bake-off.

Then submit a run. The upstream way is `argo submit --from
workflowtemplate/NAME`, which needs the `argo` CLI; `argo/submit.sh` generates
the equivalent stub with kubectl only, like the rest of this section:

```bash
KCTX=$KCTX ./argo/submit.sh payment-processing
KCTX=$KCTX ./argo/submit.sh payment-processing -p bakeoff-ns=argo -p input='{...}'
```

Runs appear in the **Workflows** tab. Watch one with
`kubectl --context "$KCTX" -n argo get wf -w`.

**A template's `spec.arguments` defaults resolve, but are not recorded.** A stub
that passes nothing still gets the right `{{workflow.parameters.*}}` values —
verified — yet the submitted Workflow object's own `spec.arguments` comes back
**empty**, so `kubectl get wf -o yaml` cannot tell you what a run actually used.
Pass `-p` explicitly when that matters.

This is the **opposite** of the `templateRef` rule two notes below: a spec-level
`workflowTemplateRef` honours the template's defaults, while a task-level
`templateRef` inside DAG 4 ignores them and resolves against the caller. Same
syntax family, inverted rule.

### Notes

- The default executor is emissary (the only option in Argo v4+).
- HTTPS is off for port-forward convenience. Do not expose this without TLS.
- Each DAG takes a `bakeoff-ns` parameter (default `argo`) that drives schema
  isolation; override with `-p bakeoff-ns=<runner>`. The sub-workflow
  WorkflowTemplates read it via `{{workflow.parameters.bakeoff-ns}}`, which
  resolves against the *calling* workflow — so invoke them from DAG 4 rather
  than submitting them standalone.
- `argo/scripts/*.py` are standalone copies of the inline step logic and are
  **not referenced by any YAML** (the manifests carry their source inline). They
  are kept in sync deliberately, so a change to a step means editing both.
- **`retryPolicy: Always` cannot classify errors** (found and fixed 2026-08-12).
  Argo retries on *pod failure*, and a pod that failed on purpose looks exactly
  like a pod that hit a blip. DAG 3 had this twice:
  - `handle-payment-failure` records the failed transaction and then
    `sys.exit(1)`s deliberately, to fail the workflow — and got retried 1 + 3
    times with backoff, re-printing the notification each time, before failing
    anyway.
  - Worse, `process-payment` exited **1 for all three** simulated outcomes, so a
    declined card (non-retriable, a business decision) burned all 5 attempts
    exactly like a gateway 5xx. DAG 3's entire retriable-vs-non-retriable
    requirement was therefore untested in Argo. Note
    `argo/scripts/process_payment.py` had `sys.exit(2)` with a
    "non-retriable" comment all along — the inline YAML copy had drifted from
    it, which is the sync risk in the note above, realised.

  Fixed with the exit-code convention the standalone scripts already used —
  **2 == non-retriable/terminal, 1 == retry me** — plus a predicate on both
  steps:

  ```yaml
  retryStrategy:
    limit: "5"
    retryPolicy: Always
    expression: 'lastRetry.exitCode != "2"'
  ```

  Use a *string* compare, not `asInt(lastRetry.exitCode)`: `exitCode` is `""` on
  an Error (as opposed to Failed) node and `asInt("")` throws inside the
  predicate — which would fail the retry decision itself, the same shape as the
  Google Workflows defect where a TypeError inside a retry predicate replaced the
  original error. Verified: a duplicate payment now runs
  `handle-validation-failure(0)` once and the workflow fails in 31s, where it
  previously logged `(0)`, `(1)`, `(2)`, `(3)`. The decline branch is
  inspection-only — it fires on a 5% random roll — but the predicate itself is
  what was empirically confirmed, and the decline path differs only by the
  literal exit code.

#### Status on the arm64 cluster (2026-08-03)

- **DAG 3 — green, first submission.** `ACC-001` 5000 → 4900, `ACC-003` 0 → 100,
  transaction `completed` with a gateway id, all inside `argo_dag3`.
- **DAG 4 — green on both branches**, after fixing five defects (below). Approval
  path: order `shipped` with tracking, `approval_requests` row `approved` by
  `auto-decider`, reservations recorded, inventory decremented. Skip-approval
  path (29.97 < 500 threshold): shipped with the approval chain correctly omitted
  and no approval row.
- **DAG 1 / DAG 2 — not attempted.** Both now have a source: `fixture-service`
  serves the DAG 1 ZIP and DAG 2's Books API (§0b), and is part of the in-cluster
  backbone chart (§7c), so `http://fixture-service:8099/...` resolves from a
  workflow pod. DAG 1's `zip-url` still defaults to the non-existent
  `https://example.com/data/sample-data.zip` — point it at
  `http://fixture-service:8099/sample-data.zip`.

#### The five DAG 4 defects, in the order they surfaced

Each one masked the next, so they could only be found by running it repeatedly.

1. **`activeDeadlineSeconds` nested under `script:`** in
   `manager-approval-template.yaml`. Template-level field; the API server rejects
   it outright with a strict-decoding error, so the WorkflowTemplate never
   registers.
2. **`{{workflow.parameters.*}}` inside a `templateRef`'d template resolves
   against the CALLING workflow**, and the template's own `spec.arguments`
   defaults are ignored entirely. `approval-service-url`,
   `poll-interval-seconds`, and `poll-max-attempts` were never defined by DAG 4,
   so submission failed with `failed to resolve
   {{workflow.parameters.approval-service-url}}` — Argo validates the whole call
   tree up front, so nothing ran. Fixed by making the template self-contained:
   those are now `inputs.parameters` with defaults, threaded to the steps that
   use them.
3. **A `dag`/`steps` template does not re-export its children's outputs.** The
   caller read `{{steps.run-approval.outputs.parameters.final-decision}}`, which
   the entrypoint never exposed. Fixed with an `outputs.parameters` block using
   `valueFrom.parameter:` (not `path:` — the value comes from a child task, not a
   file in this template's container).
4. **FK ordering in `reserve-inventory`** — reservation rows were inserted before
   the `orders` upsert they reference, and `inventory_reservations.order_id` has a
   non-deferrable FK, so any new `order_id` failed. **Identical to Prefect's
   fix 4**; the mirror copy in `argo/scripts/` had it too.
5. **`when:` that dereferences a possibly-skipped task.** With `dependencies:`, a
   skipped dependency leaves the task eligible, so Argo evaluates `when:` and
   dies with `unable to substitute {{tasks.X.outputs.parameters.Y}}`. Both paths
   hit this in mirror image. Fixed by converting the whole DAG to
   `depends: X.Succeeded`, which omits the task without evaluating `when` —
   and note Argo **rejects a DAG that mixes `depends` and `dependencies`**, so
   converting one task forces converting all ten.

Plus one behavioural-equivalence fix: `record-decision` only updated
`orders.status` and never wrote the `approval_requests` table that `init-db.sql`
creates for it. Argo was the **only** implementation of the eight not writing it.
It now upserts the decided row; the remaining divergence is that Argo has no
`pending` row mid-flight, because polling means this step is the first time it
touches the DB.

---|---|
  | `reserve-inventory` | `order-id`, `customer-id`, `items` |
  | `call-shipping-api` | `order-id`, `items`, `shipping-address` |
  | `manager-approval` | declares **no** inputs, yet reads four |

  and three genuinely-global parameters are referenced but never defined by DAG 4:
  `approval-service-url`, `poll-interval-seconds`, `poll-max-attempts`. Fixing it
  means correcting the scope at each reference site and adding those three to
  `argo/dag4-order-fulfillment.yaml`'s `arguments.parameters`.
- **DAG 1 / DAG 2 — not attempted.** Both now have a source: `fixture-service`
  serves the DAG 1 ZIP and DAG 2's Books API (§0b), and is part of the in-cluster
  backbone chart (§7c), so `http://fixture-service:8099/...` resolves from a
  workflow pod. DAG 1's `zip-url` still defaults to the non-existent
  `https://example.com/data/sample-data.zip` — point it at
  `http://fixture-service:8099/sample-data.zip`.

---

## 9. Flyte (Kubernetes)

Helm, `flyte-core` chart. An earlier install ran `v1.16.7`; new installs
take latest.

`flyte-binary` bundles Postgres as a sidecar, which broke on the earlier install due
to init-container ordering — hence `flyte-core` plus a standalone Postgres. On a
cluster with real storage either would work, but stay on `flyte-core` so the two
installs stay comparable.

### 9a. Install

One script — `flyte/deploy-flyte.sh`. It is the single source of truth for the
manifests; there is no copy-paste YAML here that can drift from it.

```bash
cd flyte
./deploy-flyte.sh
```

Knobs: `FLYTE_NS` (default `flyte`), `STORAGE_CLASS` (`""` → `emptyDir`, for a
cluster with no dynamic provisioning), `IMAGE_PREFIX`, `CHART_VERSION` (default: latest).

**Verified on the arm64 cluster: chart `flyte-core-v1.16.8`, all nine pods Running
(2026-08-03), and **all four DAGs execute green** (2026-08-06)** — DAG 1's
`@dynamic` fan-out plus Parquet to the blob store, DAG 2's 30-item fan-out with
zero failures, DAG 3 moving $100, and DAG 4's full approval path ending `shipped`
with tracking. Fourteen defects were fixed getting there; `flyte/README.md` has
them individually. The three with the widest reach: statement order in a
`@workflow` implies nothing (Flyte derives edges only from data dependencies),
`@dynamic` re-resolves images inside the task pod, and local filesystem paths do
not survive a task boundary.

The script does four things, three of which the upstream chart does not:

1. **Metadata Postgres** — Flyte's own DB, separate from the bake-off DB in §7c.
   `flyte-binary` bundles one as a sidecar but broke on that install's init-container
   ordering, so it is `flyte-core` plus a standalone Postgres. It creates **two**
   databases (`flyteadmin` and `datacatalog`) because that is what the chart's
   defaults expect — the older recorded command pointed both at `flyteadmin`.
2. **minio** — see §9b; this is the part that was missing.
3. **The bucket** — minio starts empty and Flyte does not create its own
   container, so an `mc mb` Job creates `my-s3-bucket`.
4. **`helm upgrade --install flyte-core`** with `postgres.enabled=false` and
   `common.ingress.enabled=false`.

Two details worth knowing:

- **`POSTGRES_HOST_AUTH_METHOD=trust`.** The chart sets
  `db.*.database.passwordPath=""`, so flyteadmin and datacatalog connect with *no
  password*; stock Postgres host auth rejects that. Trust auth is how the
  migrations get to run. Evaluation-grade only.
- **cri-o needs fully-qualified images** (§7c), hence `IMAGE_PREFIX` on the
  Postgres image. The flyte-core and minio images are already qualified
  (`cr.flyte.org/…`, `quay.io/…`) so they are unaffected.
- **Task pods get no blob-store credentials by default.** `flytepropeller`'s
  `plugins.k8s.default-env-vars` is `[]`, so every task pod dies trying to fetch
  its own code package (`Unable to locate credentials`), retried to exhaustion —
  a failure that looks like a DAG bug. `deploy-flyte.sh` injects
  `FLYTE_AWS_ENDPOINT` / `_ACCESS_KEY_ID` / `_SECRET_ACCESS_KEY`. Combined with
  §9b, the chart's `sandbox` storage mode gets you a Flyte that installs cleanly
  and cannot run anything, for two independent reasons.

### 9b. The blob store — the trap that broke the first install

**The chart configures a blob store it does not deploy.** `storage.type` defaults
to `sandbox`, which renders this into `flyte-admin-base-config`:

```yaml
storage:
  type: minio
  container: "my-s3-bucket"
  stow:
    config:
      access_key_id: minio
      secret_key: miniostorage
      endpoint: http://minio.flyte.svc.cluster.local:9000
```

But **the chart has no `minio` key at all** — `--set minio.enabled=true` is
silently accepted and ignored, and `helm template` emits no minio resources. Flyte
stores every task input, output, and offloaded literal there, so the install comes
up entirely "Running" while being unable to execute a single workflow.

That is precisely the state an earlier install was left in: flyteadmin healthy for 46 days, the
endpoint above pointing at a Service that does not exist, and
`kubectl get flyteworkflows -A` returning `No resources found`. Nothing ever ran.

`deploy-flyte.sh` deploys minio at exactly that endpoint with those credentials,
so the chart defaults stay untouched. **The check that catches this in one line:**

```bash
kubectl --context "$KCTX" -n flyte get cm flyte-admin-base-config \
  -o jsonpath='{.data.storage\.yaml}' | grep endpoint
kubectl --context "$KCTX" -n flyte get svc minio      # must exist
```

For a durable store instead, OCI Object Storage exposes an S3-compatible endpoint:
set `storage.type=s3` with `storage.s3.endpoint`, `.authType=accesskey`,
`.accessKey`, `.secretKey` and skip the in-cluster minio. In-cluster minio is the
right call for evaluation; it is not a production blob store.

### 9c. Versions

`comparison.md` cites version numbers, so record what you got:

```bash
helm --kube-context "$KCTX" list -n flyte
```

| Cluster | Chart | Note |
|---|---|---|
| arm64 cluster | `flyte-core-v1.16.8` | minio deployed, bucket created, healthy |
### 9d. Task images, registration, and running

Three prerequisites, all handled by `flyte/register.sh` and `flyte/run.sh`:

**1. Build the task image on the cluster, not the workstation.** `ImageSpec`
assumes a local container builder that can produce the cluster's architecture.
Cross-building arm64 from x86 needs qemu binfmt handlers, and rootless podman
cannot register them (`mount: permission denied (are you root?)`). An in-cluster
buildah Job builds natively in ~90s. The DAG files accept a prebuilt image via
`FLYTE_TASK_IMAGE`, which bypasses `ImageSpec` entirely so registration pushes
nothing.

**2. An image-pull secret in the *task* namespace**, attached to its default
ServiceAccount — task pods run in `<project>-<domain>`, not `flyte`. On the arm64 cluster
the managed ECR secret is scoped to `default`, so mint a separate one; it lasts
12 hours.

**3. Register from inside the cluster.** `pyflyte register` uploads the code
package to a signed URL that names `minio.flyte.svc.cluster.local`, unresolvable
from a workstation — registration does all its work and then fails with a
`NameResolutionError`. Running the client as a Job avoids reconfiguring Flyte for
a client-side limitation. Two traps in that path: flytekit needs
`PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring` (no OS keyring in a
container), and `cp -r` of a ConfigMap mount leaks its `..2026_…/` versioned
symlink dir into the module name (`No module named 'flyte.'`) — copy `*.py` only.

```bash
cd flyte
./register.sh dag3_payment.py
./run.sh dag3
```

`flyte/README.md` has the per-defect detail, including the two that only surface
at runtime: `retries=` is inert unless the exception subclasses
`FlyteRecoverableException`, and a `@workflow` body cannot construct its own
output dataclass from task results.

### 9e. Backbone and DB config

Flyte task pods run in per-project namespaces (`bakeoff-development` etc.),
not `flyte`. Either run §7c with `ORCH_NS` set to the project namespace you'll
execute in, or deploy the backbone once elsewhere and pass an FQDN — Flyte makes
this easy because the DB settings are a workflow **input**:

```python
DBConfig(host="postgres.orchestrators.svc.cluster.local", port=5432,
         database="orchestration", user="orchestration", password="orchestration")
```

That is a genuine Flyte advantage worth noting in `comparison.md`: the same DAG
retargets to a different DB without editing the workflow, whereas Argo's literal
env values require editing the YAML.

### 9f. Projects and namespaces

A stock `flyte-core` install lands **ten** namespaces: `flyte` for the control
plane, plus one per project × domain. Both factors are plain Helm values —
`flyteadmin.initialProjects` defaults to `flytesnacks`/`flytetester`/
`flyteexamples`, `configmap.domain.domains` to development/staging/production.
`deploy-flyte.sh` seeds a single project (`FLYTE_PROJECT`, default `bakeoff`), so
this cluster runs four namespaces, and `register.sh`/`run.sh` default
`PROJECT=bakeoff` to match.

**Trimming an install that already has the nine takes more than a Helm upgrade.**
`seed-projects` only adds, and `kubectl delete ns` loses a race with the
`syncresources` reconcile loop, which recreates the namespace within its 5m
`refreshInterval`. Archive the project first — flyteadmin's clusterresource
provider filters `state != ARCHIVED`, so archiving takes it out of the sync walk
and the delete sticks. There is no delete-project API; archive is the route.
Done on the arm64 cluster 2026-08-12; the commands are in `flyte/README.md`
§"Why are there so many Flyte namespaces?".

Two things that go with it. **Archiving hides the project** — and its execution
history, which is *not* deleted — from the console until you PUT the state back
to `ACTIVE`. That is how 74 registrations came to look like they had vanished:
the console showed the new `bakeoff` project with no workflows while everything
sat in the archived `flytesnacks`.

And **the per-namespace prerequisites live in the task namespace**, so a new
project starts without them:

- the `alias-backbone.sh` ExternalName Services, and
- an **image-pull secret**. Flyte's `clusterresource-template` provisions only a
  Namespace and a ResourceQuota, never credentials, so a fresh project namespace
  cannot pull the task image. The launcher job in `flyte` still succeeds, so the
  console reports a started execution while the task pod sits in
  `ImagePullBackOff`. Since 2026-08-13 the cluster's `k8s-ecr-login-renew`
  cronjob writes `k8s-ecr-login-renew-docker-secret` into every listed namespace
  every 6 hours — **add the new namespace to its `targetNamespace` list and patch
  that namespace's `default` ServiceAccount**; see `flyte/README.md`
  §Operational notes. Pod specs are immutable, so fixing this after a pod exists
  requires deleting the pod, not just the ServiceAccount.

### Accessing the UI

```bash
just flyte-ui                    # http://localhost:8085/console/  -- Ctrl-C stops it
```

**Do not just port-forward `svc/flyteconsole`.** That was the instruction here
until 2026-08-12 and it does not work: flyteconsole and flyteadmin are separate
Services, and only one of them answers each half of what the UI needs.

| path | flyteconsole | flyteadmin |
|---|---|---|
| `/console` | serves the SPA | 404 |
| `/api/v1/*` | **returns SPA HTML, 200** | returns JSON |

So the console loads, renders *"Select a project to get started"*, and lists
nothing — its `GET /api/v1/projects` receives flyteconsole's own catch-all HTML
with a 200 rather than JSON. Nothing reports an error. The natural readings are
both wrong: it is not an empty install (flyteadmin had `flytesnacks`,
`flyteexamples`, `flytetester` and 5+ executions the whole time — those three
have since been archived in favour of `bakeoff`, §9f) and it is not an
auth prompt (`useAuth: false` in `flyte-admin-base-config`; the console renders
its "Login" link unconditionally).

What merges them in a normal deployment is an **ingress** — `INGRESS_ENABLED=true`
in the flyte-core chart, routing `/console` to one backend and `/api` to the
other. This cluster runs Traefik but has no Flyte ingress, so
`scripts/flyte-console-proxy.py` stands in for one: it opens both port-forwards
itself (on 18083/18088, above the band `check-ports.sh` audits) and serves the
merged origin on **8085**. `--port`, `--namespace` and `--context` are all
overridable; it honours `$KCTX` and falls back to the current context.

`flytectl` against a flyteadmin port-forward remains the other option, and needs
none of this.

### Notes

- Credentials are hardcoded throughout. Evaluation-grade only.
- Flyte creates `<project>-<domain>` namespaces on install — one per pair, and
  the chart's three default projects × three domains is why a stock install lands
  ten namespaces. `deploy-flyte.sh` seeds only `bakeoff`, so the arm64 cluster has
  four: `flyte` + `bakeoff-{development,staging,production}`. See §9f.

---

## 10. Google Workflows (real GCP)

No local path exists: the engine is managed, and it executes no code of its own,
so every step body is an HTTP call to something you deploy. Full detail,
including the seven defect classes found by running it, is in
`google-workflows/README.md`; this is the short version.

**What has to exist first**

1. **Mock services + fixture, publicly reachable** — §7c-i, with
   `GOOGLE_SA_KEY_FILE` so the resume broker can authenticate back into
   Workflows.
2. **Neon seeded** — this runner shares the database with Step Functions, so the
   namespace is what keeps them apart:
   ```bash
   psql "$NEON_DATABASE_URL" -f shared-services/init-db.sql
   psql "$NEON_DATABASE_URL" -c "SELECT bootstrap_bakeoff('google_workflows');"
   ```
   `init-db.sql` is inert as of 2026-08-12 — it defines the function and
   onboards nothing, so the first command is safe to re-run. **Before that date
   it also bootstrapped `temporal` and `prefect`,** so a Neon database
   initialised with the old file may be carrying two empty stray schema sets
   from exactly this command. Check with
   `psql "$NEON_DATABASE_URL" -c "\dn"`; remove with
   `./scripts/bakeoff-db.sh prune temporal --from neon` (it refuses if anything
   ever ran there).

**Deploy** — three steps the first time, because of a genuine cycle: Cloud Run
validates its image at create time, the image cannot be pushed until Artifact
Registry exists, and the registry is created by the same Terraform.

```bash
cd terraform/gcp
terraform apply -var project_id=<project> \
  -target=google_project_service.required -target=google_artifact_registry_repository.images
PROJECT_ID=<project> ./scripts/build-push-task-service.sh
terraform apply -var project_id=<project>
```

> **Expect the first full apply to fail on 3 of 4 workflows** with "Workflows
> service agent does not exist". The service agent is provisioned asynchronously
> after the API is enabled; a plain re-apply succeeds.

**Run**

```bash
export NEON_DATABASE_URL='postgresql://…'    # DAG 3/4 take db_config as an execution INPUT
cd terraform/gcp/scripts
./run-workflow.sh dag1
./run-workflow.sh dag3 --force-outcome declined
./run-workflow.sh dag4 --order-id ORD-XYZ
```

**The callback rule (the Google Workflows analogue of §2).** DAG 2 and DAG 4
suspend on `events.await_callback`. The callback URL lives on
`workflowexecutions.googleapis.com` and **rejects unauthenticated POSTs**, so the
resume broker needs Google credentials — hence its fifth provider,
`google_workflows`, which mints an OAuth2 **access** token (not an OIDC ID token;
that is what the workflow uses in the other direction to call private Cloud Run).
`roles/workflows.invoker` is sufficient, verified by a successful resume.

**Config is `user_env_vars`, never `templatefile()`.** The Workflows language
uses `${…}` for its own expressions, so Terraform templating collides with it
head-on. See `deployment.md`.

**Teardown:** `terraform -chdir=terraform/gcp destroy -var project_id=<project>`.
The GCS bucket is `force_destroy = true`, and the Neon schemas are not managed by
Terraform — drop `google_workflows_dag{1,3,4}` by hand if you want them gone.

---

## Teardown

### Local

```bash
# Stop everything, engines included. Use this unless you have a reason not to.
just down-all

# Stop everything AND delete the volumes (postgres data, kestra storage).
just down-clean

# Targeted: pass the SAME profile you started with, or the engine survives.
just down temporal
just down hatchet
just down kestra
just down conductor
```

> **`just down` on its own does not stop the engines.** Every engine sits behind
> a compose `profiles:` key, and compose only acts on services whose profile is
> *active* — so a bare `down` removes the backbone (postgres + the four mocks)
> and leaves temporal / hatchet / kestra / conductor running, still holding
> 7233, 8888, 8081, 8000/8127. Use `just down-all`.
>
> This is worth more than a tidiness note, because the leftovers **break the
> next startup**. The surviving containers stay attached to the compose pod and
> network, so the following `just up` fails with
> `"<project>_default" has associated containers with it ... network is being
> used` and `container name "shared-services_postgres_1" is already in use`,
> and compose then reuses containers by name instead of recreating them. If you
> are seeing those errors, something from a previous profile is still running:
>
> ```bash
> podman ps                 # anything here that you did not just start?
> just down-all             # then start over
> ```
>
> Neither `--profile '*'` nor `COMPOSE_PROFILES` enables all profiles under
> podman-compose (both verified 2026-08-09), which is why the `all_profiles`
> variable in the `Justfile` lists them one by one. **Add new engines there.**

`./shared-services/check-ports.sh` is the quick way to confirm a teardown
actually finished — it lists anything still listening that the §0 port map
does not account for.

### Kubernetes

All of these take `--context "$KCTX"` — deleting a namespace on the wrong
cluster is the one mistake here that is not recoverable.

```bash
# Argo Workflows — delete with the SAME manifest you installed from. If latest
# has moved on since, `kubectl delete namespace argo` plus removing the
# argoproj.io CRDs is the reliable path.
kubectl --context "$KCTX" delete -n argo \
  -f https://github.com/argoproj/argo-workflows/releases/latest/download/install.yaml
kubectl --context "$KCTX" delete namespace argo

# Flyte. Uninstall the release FIRST -- deleting the namespace alone orphans the
# Helm release records. The PVCs and minio/postgres are not chart-managed
# (deploy-flyte.sh creates them), so the namespace delete is what removes them.
helm --kube-context "$KCTX" uninstall flyte -n flyte
kubectl --context "$KCTX" delete namespace flyte     # takes flyte-pgdata + flyte-minio PVCs
# Flyte's project namespaces are not chart-managed. One set per seeded project
# (§9f) -- this install seeds only `bakeoff`; a stock one leaves nine behind
# under flytesnacks-/flyteexamples-/flytetester-.
kubectl --context "$KCTX" delete ns bakeoff-{development,staging,production}

# Backbone. deploy-backbone.sh creates plain resources (no Helm release), so
# deleting the namespace takes everything -- including the PVC.
kubectl --context "$KCTX" delete namespace "$ORCH_NS"

# The ExternalName aliases live in the WORKFLOW namespace, so they go with it.
# If you kept that namespace, drop them individually:
kubectl --context "$KCTX" delete -n argo \
  svc/postgres svc/callback-fetch-service svc/approval-service svc/shipping-service

# Only if you used the Helm chart instead of deploy-backbone.sh:
helm --kube-context "$KCTX" uninstall mock-services -n "$ORCH_NS"
```

PVCs on a `Delete`-reclaim StorageClass (both of the arm64 cluster's are) take the
underlying volume with them — so deleting `$ORCH_NS` destroys the bake-off
database. A Helm *release*, by contrast, leaves its records behind if you delete
the namespace without uninstalling, so uninstall first in that case.

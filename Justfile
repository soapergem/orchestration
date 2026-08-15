# Auto-detect the container runner: finch > podman > docker. Override with
# `just --set container_runner <name>` or `CONTAINER_RUNNER=<name> just ...`.
container_runner := env("CONTAINER_RUNNER", `if command -v finch >/dev/null 2>&1; then echo finch; elif command -v podman >/dev/null 2>&1; then echo podman; else echo docker; fi`)
compose := container_runner + " compose -f shared-services/docker-compose.yml"

# Docker-compatible Engine API socket, for Kestra's DOCKER task runner.
#
# Load-bearing, not a nicety: kestra mounts `${CONTAINER_SOCK:-/var/run/docker.sock}`,
# and under Podman that default path does not exist, so the container cannot be
# created at all. podman-compose reports this as the thoroughly unhelpful
# `no container with name or ID "shared-services_kestra_1" found`. Auto-detecting
# it means `just up kestra` and `just up-all` work without remembering to export
# anything. Needs `systemctl --user enable --now podman.socket` once.
container_sock := env("CONTAINER_SOCK", `if [ -S /var/run/docker.sock ]; then echo /var/run/docker.sock; elif [ -S "/run/user/$(id -u)/podman/podman.sock" ]; then echo "/run/user/$(id -u)/podman/podman.sock"; else echo /var/run/docker.sock; fi`)

# Every engine profile in docker-compose.yml, for the teardown recipes.
#
# Compose only acts on services whose profile is ACTIVE, and all four engines sit
# behind a `profiles:` key. A bare `down` therefore removes the backbone and
# silently leaves temporal/hatchet/kestra/conductor running, still holding their
# host ports -- and because those containers stay attached to the pod and
# network, the NEXT `up` fails with "network is being used" and "container name
# is already in use". Neither `--profile '*'` nor COMPOSE_PROFILES works under
# podman-compose (both verified 2026-08-09), so the list has to be explicit.
#
# ADD NEW ENGINES HERE when you add a profile to docker-compose.yml.
all_profiles := "--profile temporal --profile hatchet --profile kestra --profile conductor --profile kruxiaflow"

# The engine SERVERS that `up-all` starts, named explicitly.
#
# Deliberately an allowlist rather than "everything in these profiles", because
# the temporal and hatchet profiles each also contain a containerised *worker*
# sidecar, and starting those is actively harmful: a container worker and a host
# worker poll the SAME task queue, so runs get silently split between two
# processes running different code (CLAUDE.md flags this for Temporal).
# hatchet-worker additionally needs HATCHET_CLIENT_TOKEN, which has no default
# and is minted at runtime, and both are `build:` services.
#
# `--scale temporal-worker=0` was tried first and does NOT reliably suppress a
# service here (podman-compose started it anyway, 2026-08-09) -- naming the
# services is the only form that held. ADD NEW ENGINE SERVERS HERE.
#
# kruxiaflow's two one-shots (keygen, catalog) are deliberately NOT listed:
# naming them here would make `up-all` block on services that exit, and the
# `depends_on: service_completed_successfully` on `kruxiaflow` already pulls
# them in and orders them correctly.
all_engines := "temporal temporal-ui hatchet-engine kestra conductor-server kruxiaflow"

# The profile-less backbone, spelled out so `up-all` can be ONE compose command.
# Doing it as two (`up -d` then `up -d <engines>`) makes the second invocation
# try to recreate postgres -- which is already running from the first -- and it
# fails with "container name is already in use" / "has dependent containers",
# plus a "no such container" line per engine. Harmless but alarming; one
# invocation is silent.
backbone := "postgres callback-fetch-service approval-service shipping-service fixture-service"

default:
    @just --list | grep -v "^    default$"

# Only ONE profile can be passed, and it must be one defined in
# docker-compose.yml (temporal|hatchet|kestra|conductor|kruxiaflow). Use `up-all` for every
# engine at once -- they no longer contend for host ports (RUNNING.md §0).

# Start the backbone (postgres + mocks); pass a profile to add one engine.
up profile="":
    CONTAINER_SOCK="{{ container_sock }}" {{ compose }} {{ if profile == "" { "" } else { "--profile " + profile } }} up -d

# Every engine at once. Viable because the §0 port map gives each one its own
# host port -- verified no collisions across all four profiles. Costs ~2 JVMs
# (Kestra, Conductor) plus Temporal and Hatchet, so it is heavier than the
# one-at-a-time default; use `just up <profile>` if you only need one.
#
# Does NOT start the two worker sidecars -- see `all_engines` above. Run each
# tool's worker on the host as RUNNING.md describes.

# Start the backbone + ALL engines (no worker sidecars).
up-all:
    CONTAINER_SOCK="{{ container_sock }}" {{ compose }} {{ all_profiles }} up -d {{ backbone }} {{ all_engines }}

# Pass the same profile you started with, or the engine survives this. `just
# down-all` is the version that does not require remembering.

# Stop the backbone; pass a profile to stop that engine too.
down profile="":
    {{ compose }} {{ if profile == "" { "" } else { "--profile " + profile } }} down

# Stop everything including all engines -- no need to recall the profile.
down-all:
    {{ compose }} {{ all_profiles }} down

# Deletes the pgdata and kestra-data volumes. Uses all_profiles for the same
# reason down-all does: otherwise it removes the volumes out from under engines
# that are still running, which is how you get a wedged Kestra.

# Stop everything and delete volumes (postgres data, kestra storage).
down-clean:
    {{ compose }} {{ all_profiles }} down -v

# `just up` does NOT rebuild, so run this after editing anything under
# shared-services/*/app.py -- otherwise you are testing against a stale image.

# Rebuild the mock-service images and restart them.
rebuild:
    {{ compose }} build
    {{ compose }} up -d --force-recreate \
      callback-fetch-service approval-service shipping-service

# Follow logs for the running services.
logs:
    {{ compose }} logs -f

# The 12 runners span THREE databases -- local pod, in-cluster postgres, Neon --
# so these route by runner name. Naming a runner that lives elsewhere used to
# operate on the local pod regardless and report success; unknown names used to
# create schemas for the typo. Both are now hard failures. See the script header.

# Create a runner's per-DAG schemas + seed fixtures, e.g. `just seed prefect`.
seed runner:
    CONTAINER_RUNNER="{{ container_runner }}" ./scripts/bakeoff-db.sh seed {{ runner }}

# Drop a runner's per-DAG schemas and re-seed fixtures, e.g. `just reset temporal`.
reset runner *args:
    CONTAINER_RUNNER="{{ container_runner }}" ./scripts/bakeoff-db.sh reset {{ runner }} {{ args }}

# Show which database a runner uses, and which of its schemas exist there.
db-status runner:
    CONTAINER_RUNNER="{{ container_runner }}" ./scripts/bakeoff-db.sh status {{ runner }}

# For strays only: schemas on a database the runner never runs against, e.g.
# `just db-prune temporal --from cluster`. Naming the runner's OWN database is a
# hard error (that is `reset`, which re-seeds), and any schema set holding run
# artifacts is refused unless --force. Seeded fixtures do not count as use.

# Drop a runner's schemas from a database that is NOT its home.
db-prune runner *args:
    CONTAINER_RUNNER="{{ container_runner }}" ./scripts/bakeoff-db.sh prune {{ runner }} {{ args }}

# Airflow, Dagster, Prefect and Luigi have **no engine container** -- they are
# libraries executed in your own venv, so `just up-all` cannot start them and
# RUNNING.md §0's port table shows 3000/4200/8080/8082 unowned until you do.
# The why, the 0.0.0.0 binding and the process-group teardown all live in the
# script's header.

# Start the host-run orchestrator UIs -- Dagster :3000, Prefect :4200, Airflow :8080, luigid :8082.
py-up *targets:
    ./scripts/py-procs.sh up {{ targets }}

# Stop the host-run orchestrator UIs started by `py-up`.
py-down *targets:
    ./scripts/py-procs.sh down {{ targets }}

# Show which host-run orchestrator UIs are up, and on which port.
py-status:
    ./scripts/py-procs.sh status

# Show the login for every service that has one, and name the ones that don't.
creds:
    ./scripts/creds.sh

# Hatchet and Kestra keep registrations as durable SERVER state, not as a
# projection of the source tree -- so a worker run with a different
# HATCHET_CLIENT_NAMESPACE leaves a full orphan set behind, and Kestra's image
# auto-loads six `tutorial.*` samples. Both make "what is deployed?"
# unanswerable from the UI. Dry run unless you pass `--apply`.

# Delete stale workflow registrations from Hatchet / Kestra (dry run by default).
prune target="all" *args:
    ./scripts/prune-registrations.sh {{ target }} {{ args }}

# flyteconsole and flyteadmin are separate Services; port-forwarding the console
# alone gives you a UI whose API calls land on its own HTML catch-all, so it
# lists no projects and looks unauthenticated. An ingress normally merges them
# and this cluster has none for Flyte -- the script stands in for one. Runs in
# the foreground and manages both port-forwards; Ctrl-C stops everything.

# Serve a working Flyte console (console + admin merged) on :8085.
flyte-ui *args:
    ./scripts/flyte-console-proxy.py {{ args }}

# `kubectl port-forward` blocks until killed, and just runs each recipe line as
# its own shell in sequence -- so two of them on two lines hangs on the first and
# never reaches the second. The script runs both at once and, because a forward
# also dies silently on pod restart or laptop sleep, supervises and reconnects
# each one. Honours KCTX like every other cluster script. Ctrl-C stops both.

# Hold open the argo (:2746) and in-cluster postgres (:54322) port-forwards.
port-forwards *targets:
    ./scripts/port-forwards.sh {{ targets }}

# Open a psql shell against the bake-off database.
psql:
    {{ compose }} exec postgres psql -U orchestration -d orchestration

# Build the presentation slides.
slides-build:
    uv --directory presentation run mkslides build

# Serve the presentation slides locally with live-reload (port 8084, avoiding Conductor's 8000).
slides-serve:
    uv --directory presentation run mkslides serve -a 0.0.0.0:8084

alias slides := slides-serve

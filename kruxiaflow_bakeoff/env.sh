# Environment for the Kruxia Flow bake-off implementation.
#
#   source kruxiaflow_bakeoff/env.sh
#
# WHY THIS DIRECTORY IS NOT CALLED `kruxiaflow/`
#
# The Python SDK's importable package is `kruxiaflow` (`from kruxiaflow.worker
# import ...`). A repo-root directory of that name shadows it on sys.path
# exactly as `dagster/` shadowed the Dagster library -- the single structural
# defect that tool produced. `dagster_bakeoff/` set the precedent; this follows
# it. Do not rename this directory.
#
# WHY THERE ARE TWO SETS OF HOSTS
#
# Kruxia Flow's built-in `std` worker runs INSIDE the engine container, so
# anything it touches must be addressed by compose DNS name. The custom Python
# worker runs on the HOST (same shape as Temporal/Hatchet/Conductor here), so
# the same services are `localhost:<published port>` to it. These are not
# interchangeable and the failure mode is a silent hang or a DNS error three
# steps into a run.
#
# The rule this implementation follows, to keep it to exactly two values rather
# than a per-activity decision: ALL database work goes through the built-in
# postgres_query / postgres_transaction activities (container view), and ALL
# custom Python goes through the host worker (host view).

# ---- engine ---------------------------------------------------------------
# 8100, not Kruxia Flow's own default of 8080 -- that belongs to Airflow here,
# and 8081 to Kestra. See RUNNING.md §0.
export KRUXIAFLOW_API_URL="${KRUXIAFLOW_API_URL:-http://localhost:8100}"

# The engine runs with KRUXIAFLOW_INSECURE_DEV=true, so no token is needed for
# any call. That is a local-evaluation choice, not the product default: without
# the flag Kruxia Flow requires OAuth2 on every request. If you turn it off,
# every curl below needs `-H "Authorization: Bearer $TOKEN"`.
export KRUXIAFLOW_CLIENT_ID="${KRUXIAFLOW_CLIENT_ID:-kruxiaflow-bakeoff}"
export KRUXIAFLOW_CLIENT_SECRET="${KRUXIAFLOW_CLIENT_SECRET:-kruxiaflow-bakeoff-dev-secret}"

# ---- schema isolation -----------------------------------------------------
# Every runner gets its own kruxiaflow_dag{1,3,4} schemas via
# bootstrap_bakeoff(). `just seed kruxiaflow` creates them; `just reset
# kruxiaflow` drops and re-seeds, which is what you need after a DAG 3/4 run
# spends fixtures (seed alone will NOT undo drift -- see CLAUDE.md).
export BAKEOFF_NS="${BAKEOFF_NS:-kruxiaflow}"

# ---- container view: what the built-in std worker sees ---------------------
# Passed as workflow INPUT to postgres_query / postgres_transaction activities
# and to any http_request the engine makes.
export KF_DB_URL_CONTAINER="postgres://orchestration:orchestration@postgres:5432/orchestration?options=-csearch_path%3D${BAKEOFF_NS}_dag1,${BAKEOFF_NS}_dag3,${BAKEOFF_NS}_dag4,public"
export KF_CALLBACK_FETCH_CONTAINER="http://callback-fetch-service:8090"
export KF_APPROVAL_CONTAINER="http://approval-service:8091"
export KF_SHIPPING_CONTAINER="http://shipping-service:8092"
export KF_FIXTURE_CONTAINER="http://fixture-service:8099"
# How the mock services address the engine when firing a resume. Compose DNS,
# and the container-internal port 8080 -- NOT the 8100 host publication.
export KF_ENGINE_CONTAINER="http://kruxiaflow:8080"

# ---- host view: what the custom Python worker sees -------------------------
export POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
export POSTGRES_PORT="${POSTGRES_PORT:-54321}"
export POSTGRES_DB="${POSTGRES_DB:-orchestration}"
export POSTGRES_USER="${POSTGRES_USER:-orchestration}"
export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-orchestration}"
export KF_SHIPPING_HOST="http://localhost:8092"
# DAG 2's detail URLs are derived by fixture-service from the request, and the
# fan-out runs in the HOST worker -- so this must be the localhost form or every
# detail fetch dies trying to resolve `fixture-service`. See CLAUDE.md
# "DAG 2 reads a local Books API".
export KF_FIXTURE_HOST="http://localhost:8099"

# ---- custom worker --------------------------------------------------------
# The worker type the DAGs name in `worker:`. `std` is the engine's built-in
# pool; this is ours, polling the same API.
export KRUXIAFLOW_WORKER="${KRUXIAFLOW_WORKER:-py-bakeoff}"
export KRUXIAFLOW_WORKER_ID="${KRUXIAFLOW_WORKER_ID:-py-bakeoff-01}"

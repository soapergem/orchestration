# Environment for running the Conductor workers on the HOST.
#
#   source conductor/env.sh
#
# Everything here is host-side wiring. The engine itself is configured by
# shared-services/conductor/config.properties (which it reads as a mounted file
# -- env vars do NOT reach it; see that file's header for why).

# ---- engine ----------------------------------------------------------------
# MUST include the /api suffix -- the SDK appends nothing.
#
# The SDK's built-in default is http://localhost:8080/api, and on this host 8080
# belongs to AIRFLOW (RUNNING.md §0). Left unset, every poll, every task update
# and every workflow start silently goes to Airflow and comes back as Airflow's
# error JSON -- the exact failure Hatchet's minted tokens caused. Always source
# this file before running a worker.
export CONDUCTOR_SERVER_URL="${CONDUCTOR_SERVER_URL:-http://localhost:8000/api}"
export CONDUCTOR_UI_SERVER_URL="${CONDUCTOR_UI_SERVER_URL:-http://localhost:8127}"

# ---- database --------------------------------------------------------------
# Workers run on the host, so these are the published host port and localhost,
# not the compose DNS names the containers use.
export POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
export POSTGRES_PORT="${POSTGRES_PORT:-54321}"
export POSTGRES_DB="${POSTGRES_DB:-orchestration}"
export POSTGRES_USER="${POSTGRES_USER:-orchestration}"
export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-orchestration}"

# Per-(runner, DAG) schema isolation -- search_path becomes "conductor_dag<N>".
# Onboard with `just seed conductor`.
export BAKEOFF_NS="${BAKEOFF_NS:-conductor}"

# ---- mock services ---------------------------------------------------------
# Host-side URLs: the workers call these directly from the host.
export CALLBACK_FETCH_SERVICE_URL="${CALLBACK_FETCH_SERVICE_URL:-http://localhost:8090}"
export APPROVAL_SERVICE_URL="${APPROVAL_SERVICE_URL:-http://localhost:8091}"
export SHIPPING_SERVICE_URL="${SHIPPING_SERVICE_URL:-http://localhost:8092}"
export FIXTURE_SERVICE_URL="${FIXTURE_SERVICE_URL:-http://localhost:8099}"

# How the *mock services* (containers) reach the Conductor API to complete a
# suspended WAIT task. Container-to-container, so a compose DNS name -- NOT the
# localhost:8000 the workers use. Passed as resume_data.base_url at registration
# time; the services fall back to their own CONDUCTOR_URL if it is absent.
export CONDUCTOR_INTERNAL_URL="${CONDUCTOR_INTERNAL_URL:-http://conductor-server:8080}"

# ---- DAG 1 fixture ---------------------------------------------------------
# Generate with: uv run --no-project test-data/make-sample-data.py
export ETL_ZIP_PATH="${ETL_ZIP_PATH:-$(git rev-parse --show-toplevel 2>/dev/null)/test-data/sample-data.zip}"
export ETL_OUTPUT_DIR="${ETL_OUTPUT_DIR:-/tmp/conductor_etl_output}"

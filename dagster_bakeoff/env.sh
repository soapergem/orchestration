# Source this before any Dagster command:  source dagster_bakeoff/env.sh
#
# Dagster runs on the *host*, so every default in resources.py (compose DNS
# names) has to be overridden -- see RUNNING.md "Container networking".

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")/.." && pwd)"

# Dagster needs a persistent home for the daemon, run storage, and sensor
# cursors; without it every command spins up a throwaway instance and sensors
# cannot bridge runs.
export DAGSTER_HOME="$REPO_ROOT/dagster_bakeoff/.dagster_home"
mkdir -p "$DAGSTER_HOME"

export BAKEOFF_NS=dagster

export POSTGRES_HOST=localhost
export POSTGRES_PORT=54321
export POSTGRES_DB=orchestration
export POSTGRES_USER=orchestration
export POSTGRES_PASSWORD=orchestration

export CALLBACK_FETCH_SERVICE_URL=http://localhost:8090
export APPROVAL_SERVICE_URL=http://localhost:8091
export SHIPPING_SERVICE_URL=http://localhost:8092

# Dagster owns host port 3000 (../RUNNING.md §0). `dagster dev` defaults to it;
# pass `-p $DAGSTER_PORT` if you ever need to move it.
export DAGSTER_PORT=3000

# Shared with the sensors, which read these paths to bridge runs.
export DAG2_CORRELATION_DIR=/tmp/dagster_dag2_correlations
export DAG4_APPROVAL_DIR=/tmp/dagster_dag4_approvals

# Host-side environment for running the Airflow bake-off DAGs.
#
#   cd airflow && source env.sh
#   uv run --project .. airflow standalone
#
# The defaults compiled into the DAG files are compose DNS names and container
# paths; on the host every one of them has to be overridden. See README.md.

_here="${0:A:h}"                      # zsh
[ -n "$BASH_SOURCE" ] && _here="$(cd "$(dirname "$BASH_SOURCE")" && pwd)"

export AIRFLOW_HOME="$_here/.airflow-home"
export AIRFLOW__CORE__DAGS_FOLDER="$_here"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

# REQUIRED for DAG 2 and DAG 4. Only the dag-processor puts the DAGs folder on
# sys.path; the triggerer does not, so it cannot import `triggers.*` and the
# deferred tasks hang in `deferred` with only a "Trigger failed to load code"
# line in the triggerer's own log. Everything must share this PYTHONPATH.
export PYTHONPATH="$_here${PYTHONPATH:+:$PYTHONPATH}"

export POSTGRES_HOST=localhost
export POSTGRES_PORT=54321
export POSTGRES_DB=orchestration
export POSTGRES_USER=orchestration
export POSTGRES_PASSWORD=orchestration
export BAKEOFF_NS=airflow

export CALLBACK_FETCH_SERVICE_URL=http://localhost:8090
export APPROVAL_SERVICE_URL=http://localhost:8091
export SHIPPING_SERVICE_URL=http://localhost:8092

# Airflow owns host port 8080 in the repo's port map (../RUNNING.md §0). Pinned
# explicitly rather than left to the default, so the claim is declared in one
# place and `just up kestra` / the Hatchet token can be checked against it.
export AIRFLOW__API__PORT=8080

export ETL_ZIP_PATH="$_here/../test-data/sample-data.zip"
export ETL_EXTRACT_DIR="$AIRFLOW_HOME/data/extracted"
export ETL_OUTPUT_DIR="$AIRFLOW_HOME/data/output"
mkdir -p "$ETL_EXTRACT_DIR" "$ETL_OUTPUT_DIR"

unset _here

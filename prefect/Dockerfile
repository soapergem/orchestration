# Image for running the Prefect bake-off DAGs on a `docker`-type work pool
# (one container per flow run). Built and used by deploy_docker.py.
#
# Python 3.12, not the 3.14 the repo pins: 3.12 has reliable wheels for
# psycopg2-binary / pandas / pyarrow, and the container runtime is independent of
# the host venv. Worth noting in the comparison -- per-run container isolation
# means each run can pin its own interpreter and dependency set, which is the
# whole point of the isolation claim.
FROM docker.io/library/python:3.12-slim

# Prefect's docker worker loads the flow from the deployment's `entrypoint`,
# resolved relative to the container's working directory.
WORKDIR /opt/prefect

RUN pip install --no-cache-dir \
      "prefect==3.7.1" \
      "psycopg2-binary>=2.9.12" \
      "pandas>=2.2" \
      "pyarrow>=16" \
      "httpx>=0.27" \
      "requests>=2.32"

COPY dag1_csv_etl.py dag2_api_fanout.py dag3_payment.py dag4_order_fulfillment.py ./

# DAG 1 reads/writes these; mounted at run time via the pool's `volumes` job
# variable so output survives the container.
ENV ETL_ZIP_PATH=/data/input/data.zip \
    ETL_EXTRACT_DIR=/data/extracted \
    ETL_OUTPUT_DIR=/data/output

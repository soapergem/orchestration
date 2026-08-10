"""
Serve all four bake-off DAGs as Prefect deployments.

Registers five deployments from a single process:

  csv_etl_pipeline/dag1-csv-etl
  api_fanout_pipeline/dag2-api-fanout
  payment_processing/dag3-payment
  order_fulfillment/dag4-order-fulfillment
  manager_approval_flow/dag4-manager-approval   (invoked by DAG 4 in suspend mode)

Why deployments at all, rather than `python dagN_*.py`:

  - Scheduling, a UI Run button with a generated parameter form, `prefect
    deployment run`, and being targetable by automations.
  - `suspend_flow_run()` (DAG 4's zero-cost wait) *requires* a deployment.
  - It is the closer analogue to Airflow/Dagster, which register flows by
    scanning a folder. Comparing script-invoked Prefect against auto-registered
    Airflow understates Prefect on scheduling and triggerability.

`prefect.serve()` needs no work pool -- each flow run executes as a subprocess of
this runner. For container- or K8s-per-run isolation, use `flow.deploy()` against
a `docker` / `kubernetes` work pool instead; that is also what would exercise the
per-flow-run isolation claim in ../comparison.md.

Run from the prefect/ directory. The runner passes its OWN environment to every
flow run it launches, so set the service/DB vars here -- not per invocation:

    APPROVAL_WAIT_MODE=suspend \
    PREFECT_API_URL=http://127.0.0.1:4200/api \
    POSTGRES_HOST=localhost POSTGRES_PORT=54321 \
    CALLBACK_FETCH_SERVICE_URL=http://localhost:8090 \
    APPROVAL_SERVICE_URL=http://localhost:8091 \
    SHIPPING_SERVICE_URL=http://localhost:8092 \
    ETL_ZIP_PATH=$PWD/../.local-data/input/data.zip \
    ETL_EXTRACT_DIR=$PWD/../.local-data/extracted \
    ETL_OUTPUT_DIR=$PWD/../.local-data/output \
      python serve_all.py

Then, in another shell:

    prefect deployment run 'payment_processing/dag3-payment'
    prefect deployment run 'order_fulfillment/dag4-order-fulfillment'
    prefect deployment run 'csv_etl_pipeline/dag1-csv-etl'
    prefect deployment run 'api_fanout_pipeline/dag2-api-fanout'

Omitting a parameter uses the deployment default; override per run with
`-p name=value`.

Note the flow defaults already handle the two idempotency-key parameters
(DAG 3's payment_id, DAG 4's order_id) by generating a fresh id when none is
given -- a deployment's parameters are static, so a fixed id would turn every
run after the first into an idempotent no-op.
"""

from dag1_csv_etl import csv_etl_pipeline
from dag2_api_fanout import api_fanout_pipeline
from dag3_payment import payment_processing
from dag4_order_fulfillment import manager_approval_flow, order_fulfillment

from prefect import serve

# DAG 1 takes its paths from ETL_* env vars (see dag1_csv_etl.py), so no explicit
# parameters here -- the runner's environment supplies them.
#
# DAG 2 reads fixture-service's Books API (RUNNING.md 0b) rather than
# api.github.com, whose 60 unauthenticated requests/hour could not survive ~31
# calls per run. The collection is fetched by callback-fetch-service (a
# container), hence the compose DNS name; ?base= rewrites the per-item detail
# URLs for the host-run fan-out, which cannot resolve `fixture-service`.
DAG2_URL = "http://fixture-service:8099/books?base=http://localhost:8099"


if __name__ == "__main__":
    serve(
        csv_etl_pipeline.to_deployment(
            name="dag1-csv-etl",
            tags=["bakeoff", "dag1"],
        ),
        api_fanout_pipeline.to_deployment(
            name="dag2-api-fanout",
            parameters={"url": DAG2_URL},
            tags=["bakeoff", "dag2"],
        ),
        payment_processing.to_deployment(
            name="dag3-payment",
            tags=["bakeoff", "dag3"],
        ),
        order_fulfillment.to_deployment(
            name="dag4-order-fulfillment",
            tags=["bakeoff", "dag4"],
        ),
        manager_approval_flow.to_deployment(
            name="dag4-manager-approval",
            tags=["bakeoff", "dag4", "approval"],
        ),
        limit=10,
    )

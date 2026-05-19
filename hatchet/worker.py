"""
Hatchet Worker

Registers all four DAG workflows (and their child workflows) with Hatchet
and starts the worker to poll for and execute tasks.

Usage:
    python worker.py

Environment variables:
    HATCHET_CLIENT_TOKEN    -- Hatchet API token
    HATCHET_CLIENT_TLS_STRATEGY -- e.g. "none" for local dev
    POSTGRES_HOST           -- default: postgres
    POSTGRES_PORT           -- default: 5432
    POSTGRES_DB             -- default: orchestration
    POSTGRES_USER           -- default: orchestration
    POSTGRES_PASSWORD       -- default: orchestration
    CALLBACK_FETCH_SERVICE_URL -- default: http://callback-fetch-service:8090
    APPROVAL_SERVICE_URL    -- default: http://approval-service:8091
    SHIPPING_SERVICE_URL    -- default: http://shipping-service:8092
    HATCHET_EVENT_API_URL   -- default: http://localhost:8080/api/v1/events
"""

import asyncio

from hatchet_sdk import Hatchet

# Import all workflow classes so they register with the hatchet instance.
# Each module creates its own `hatchet = Hatchet()` instance at module level,
# but we need a single shared instance for the worker. We re-import the
# workflow classes and register them with the worker's Hatchet instance.

from dag1_csv_etl import (
    CSVETLPipelineWorkflow,
    LoadCSVToPostgresWorkflow,
)
from dag2_api_fanout import (
    APIFanOutWorkflow,
    FetchItemDetailWorkflow,
)
from dag3_payment import PaymentProcessingWorkflow
from dag4_order_fulfillment import (
    ManagerApprovalWorkflow,
    OrderFulfillmentWorkflow,
    ReserveInventoryWorkflow,
    ShipOrderWorkflow,
)


def main():
    hatchet = Hatchet()

    worker = hatchet.worker(
        "orchestration-bakeoff-worker",
        max_runs=40,
    )

    # DAG 1: CSV ETL Pipeline
    worker.register_workflow(CSVETLPipelineWorkflow())
    worker.register_workflow(LoadCSVToPostgresWorkflow())

    # DAG 2: API Fan-Out with Async Callback
    worker.register_workflow(APIFanOutWorkflow())
    worker.register_workflow(FetchItemDetailWorkflow())

    # DAG 3: Payment Processing
    worker.register_workflow(PaymentProcessingWorkflow())

    # DAG 4: Order Fulfillment
    worker.register_workflow(OrderFulfillmentWorkflow())
    worker.register_workflow(ReserveInventoryWorkflow())
    worker.register_workflow(ManagerApprovalWorkflow())
    worker.register_workflow(ShipOrderWorkflow())

    print("Starting Hatchet worker with workflows:")
    print("  - CSVETLPipelineWorkflow + LoadCSVToPostgresWorkflow")
    print("  - APIFanOutWorkflow + FetchItemDetailWorkflow")
    print("  - PaymentProcessingWorkflow")
    print("  - OrderFulfillmentWorkflow + ReserveInventoryWorkflow + ManagerApprovalWorkflow + ShipOrderWorkflow")

    worker.start()


if __name__ == "__main__":
    main()

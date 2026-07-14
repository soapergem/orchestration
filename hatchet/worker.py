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

from hatchet_sdk import Hatchet

from dag1_csv_etl import csv_etl_wf, load_csv_wf
from dag2_api_fanout import api_fanout_wf, fetch_item_detail_wf
from dag3_payment import payment_wf
from dag4_order_fulfillment import (
    manager_approval_wf,
    order_fulfillment_wf,
    reserve_inventory_wf,
    ship_order_wf,
)


def main():
    hatchet = Hatchet()

    worker = hatchet.worker(
        "orchestration-bakeoff-worker",
        slots=40,
    )

    # DAG 1: CSV ETL Pipeline
    worker.register_workflow(csv_etl_wf)
    worker.register_workflow(load_csv_wf)

    # DAG 2: API Fan-Out with Async Callback
    worker.register_workflow(api_fanout_wf)
    worker.register_workflow(fetch_item_detail_wf)

    # DAG 3: Payment Processing
    worker.register_workflow(payment_wf)

    # DAG 4: Order Fulfillment
    worker.register_workflow(order_fulfillment_wf)
    worker.register_workflow(reserve_inventory_wf)
    worker.register_workflow(manager_approval_wf)
    worker.register_workflow(ship_order_wf)

    print("Starting Hatchet worker with workflows:")
    print("  - CSVETLPipeline + LoadCSVToPostgres")
    print("  - APIFanOut + FetchItemDetail")
    print("  - PaymentProcessing")
    print("  - OrderFulfillment + ReserveInventory + ManagerApproval + ShipOrder")

    worker.start()


if __name__ == "__main__":
    main()

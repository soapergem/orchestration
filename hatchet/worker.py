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
    HATCHET_EVENT_RELAY_URL -- default: http://host.containers.internal:8096
                               (event_relay.py, as a mock-service container sees it)
"""

from dag1_csv_etl import csv_etl_wf, load_csv_wf
from dag2_api_fanout import api_fanout_wf, fetch_item_detail_wf
from dag3_payment import payment_wf
from dag4_order_fulfillment import (
    manager_approval_wf,
    order_fulfillment_wf,
    reserve_inventory_wf,
    ship_order_wf,
)
from hatchet_sdk import Hatchet

WORKFLOWS = [
    # DAG 1: CSV ETL Pipeline
    csv_etl_wf,
    load_csv_wf,
    # DAG 2: API Fan-Out with Async Callback
    api_fanout_wf,
    fetch_item_detail_wf,
    # DAG 3: Payment Processing
    payment_wf,
    # DAG 4: Order Fulfillment
    order_fulfillment_wf,
    reserve_inventory_wf,
    manager_approval_wf,
    ship_order_wf,
]


def main():
    hatchet = Hatchet()

    # The workflows MUST be passed to `worker()` rather than registered after
    # the fact: the SDK derives required slot types from this list, and a
    # durable task with no DURABLE slot is simply never dispatched -- DAG 2 and
    # DAG 4 hang at their event wait with no error anywhere. `durable_slots` is
    # also set explicitly so the requirement survives a refactor of the list.
    worker = hatchet.worker(
        "orchestration-bakeoff-worker",
        slots=40,
        durable_slots=100,
        workflows=WORKFLOWS,
    )

    print("Starting Hatchet worker with workflows:")
    print("  - CSVETLPipeline + LoadCSVToPostgres")
    print("  - APIFanOut + FetchItemDetail")
    print("  - PaymentProcessing")
    print("  - OrderFulfillment + ReserveInventory + ManagerApproval + ShipOrder")

    worker.start()


if __name__ == "__main__":
    main()

"""
Temporal Worker -- registers all workflows and activities.

Run with:
    python worker.py

Environment variables:
    TEMPORAL_ADDRESS   -- Temporal server address (default: localhost:7233)
    TEMPORAL_NAMESPACE -- Temporal namespace (default: default)
    TASK_QUEUE         -- Task queue name (default: orchestration)
    POSTGRES_HOST      -- Postgres host (default: postgres)
    POSTGRES_PORT      -- Postgres port (default: 5432)
    POSTGRES_DB        -- Postgres database (default: orchestration)
    POSTGRES_USER      -- Postgres user (default: orchestration)
    POSTGRES_PASSWORD  -- Postgres password (default: orchestration)
"""

from __future__ import annotations

import asyncio
import logging
import os

from temporalio.client import Client
from temporalio.worker import Worker

# -- Workflows --
from dag1_csv_etl import CSVETLWorkflow
from dag2_api_fanout import APIFanOutWorkflow
from dag3_payment import PaymentWorkflow
from dag4_order_fulfillment import (
    ManagerApprovalWorkflow,
    OrderFulfillmentWorkflow,
    ReserveInventoryWorkflow,
    ShippingWorkflow,
)

# -- Activities --
from activities import (
    call_shipping_api,
    combine_results,
    convert_to_parquet,
    fetch_item_detail,
    handle_payment_failure,
    load_csv_to_postgres,
    process_payment,
    record_approval_decision,
    release_inventory,
    request_approval,
    reserve_inventory,
    run_sql_transform,
    send_order_notification,
    send_payment_notification,
    submit_async_fetch,
    unzip_file,
    update_order_status,
    update_payment_database,
    validate_order,
    validate_payment,
)


TEMPORAL_ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
TEMPORAL_NAMESPACE = os.environ.get("TEMPORAL_NAMESPACE", "default")
TASK_QUEUE = os.environ.get("TASK_QUEUE", "orchestration")

logger = logging.getLogger(__name__)


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    logger.info(
        "Connecting to Temporal at %s (namespace=%s, task_queue=%s)",
        TEMPORAL_ADDRESS,
        TEMPORAL_NAMESPACE,
        TASK_QUEUE,
    )

    client = await Client.connect(TEMPORAL_ADDRESS, namespace=TEMPORAL_NAMESPACE)

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        # -- All workflows --
        workflows=[
            CSVETLWorkflow,
            APIFanOutWorkflow,
            PaymentWorkflow,
            OrderFulfillmentWorkflow,
            ReserveInventoryWorkflow,
            ManagerApprovalWorkflow,
            ShippingWorkflow,
        ],
        # -- All activities --
        activities=[
            # DAG 1: CSV ETL
            unzip_file,
            load_csv_to_postgres,
            run_sql_transform,
            convert_to_parquet,
            # DAG 2: API Fan-Out
            submit_async_fetch,
            fetch_item_detail,
            combine_results,
            # DAG 3: Payment
            validate_payment,
            process_payment,
            update_payment_database,
            send_payment_notification,
            handle_payment_failure,
            # DAG 4: Order Fulfillment
            validate_order,
            reserve_inventory,
            release_inventory,
            request_approval,
            record_approval_decision,
            call_shipping_api,
            update_order_status,
            send_order_notification,
        ],
    )

    logger.info(
        "Worker started. Listening on task queue '%s'. Registered %d workflows, %d activities.",
        TASK_QUEUE,
        len(worker._workflow_worker._workflow_classes) if hasattr(worker, "_workflow_worker") else 7,
        len(worker._activity_worker._activities) if hasattr(worker, "_activity_worker") else 19,
    )

    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())

"""
Starter client for the Temporal bake-off workflows.

Temporal has no "deployment" concept -- a workflow is started by a client
calling ``start_workflow`` against the task queue a worker is polling.  This
script is that client, one subcommand per DAG, so the four workflows can be
launched the same way Prefect's ``serve_all.py`` deployments are.

Tool idioms demonstrated:
  - ``Client.connect`` + ``execute_workflow`` (start and await in one call)
  - explicit, caller-supplied workflow IDs (Temporal's dedup/idempotency unit)
  - dataclass arguments serialized by the SDK's default data converter

Usage:
    uv run python start_workflow.py dag1
    uv run python start_workflow.py dag3 --amount 250
    uv run python start_workflow.py dag4 --total high     # forces approval
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import uuid
from dataclasses import asdict
from datetime import timedelta

from activities import OrderItem
from dag1_csv_etl import CSVETLInput, CSVETLWorkflow
from dag2_api_fanout import APIFanOutInput, APIFanOutWorkflow
from dag3_payment import PaymentInput, PaymentWorkflow
from dag4_order_fulfillment import OrderFulfillmentInput, OrderFulfillmentWorkflow
from temporalio.client import Client

logger = logging.getLogger(__name__)

TEMPORAL_ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
TEMPORAL_NAMESPACE = os.environ.get("TEMPORAL_NAMESPACE", "default")
TASK_QUEUE = os.environ.get("TEMPORAL_TASK_QUEUE", "orchestration")


# ---- DAG argument builders ------------------------------------------------


def _dag1_args(args: argparse.Namespace) -> tuple[type, object, str]:
    return (
        CSVETLWorkflow,
        CSVETLInput(
            zip_file_path=args.zip_file,
            extract_dir=args.extract_dir,
            output_path=args.output_path,
        ),
        f"dag1-csv-etl-{uuid.uuid4().hex[:8]}",
    )


def _dag2_args(args: argparse.Namespace) -> tuple[type, object, str]:
    return (
        APIFanOutWorkflow,
        APIFanOutInput(url=args.url),
        f"dag2-api-fanout-{uuid.uuid4().hex[:8]}",
    )


def _dag3_args(args: argparse.Namespace) -> tuple[type, object, str]:
    payment_id = args.payment_id or f"PAY-{uuid.uuid4().hex[:8]}"
    return (
        PaymentWorkflow,
        PaymentInput(
            payment_id=payment_id,
            amount=args.amount,
            currency="USD",
            from_account=args.from_account,
            to_account=args.to_account,
            idempotency_key=payment_id,
        ),
        f"dag3-payment-{payment_id}",
    )


def _dag4_args(args: argparse.Namespace) -> tuple[type, object, str]:
    # "low" stays under the 500.00 approval threshold; "high" trips it.
    items = (
        [OrderItem(sku="WIDGET-A", quantity=2)]
        if args.total == "low"
        else [OrderItem(sku="WIDGET-A", quantity=40)]
    )
    order_id = args.order_id or f"ORD-{uuid.uuid4().hex[:8]}"
    # An address missing required fields makes the shipping service return the
    # non-retryable InvalidAddress error -- the third saga-compensation trigger.
    address = (
        {}
        if args.bad_address
        else {
            "street": "123 Main St",
            "city": "Springfield",
            "state": "IL",
            "zip": "62701",
            "country": "US",
        }
    )
    return (
        OrderFulfillmentWorkflow,
        OrderFulfillmentInput(
            order_id=order_id,
            customer_id=args.customer_id,
            items=items,
            shipping_address=address,
        ),
        f"dag4-order-{order_id}",
    )


# ---- Entrypoint -----------------------------------------------------------


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="dag", required=True)

    p1 = sub.add_parser("dag1", help="CSV ETL pipeline")
    p1.add_argument("--zip-file", default="../test-data/sample-data.zip")
    p1.add_argument("--extract-dir", default="/tmp/temporal-dag1/extracted")
    p1.add_argument("--output-path", default="/tmp/temporal-dag1/output")

    p2 = sub.add_parser("dag2", help="API fan-out with async callback")
    p2.add_argument("--url", default=os.environ.get("DAG2_URL", "http://fixture-service:8099/books?base=http://localhost:8099"))

    p3 = sub.add_parser("dag3", help="Payment processing")
    p3.add_argument("--payment-id", default=None)
    p3.add_argument("--amount", type=float, default=100.00)
    p3.add_argument("--from-account", default="ACC-001")
    p3.add_argument("--to-account", default="ACC-003")

    p4 = sub.add_parser("dag4", help="Order fulfillment with saga compensation")
    p4.add_argument("--order-id", default=None)
    p4.add_argument("--customer-id", default="CUST-42")
    p4.add_argument("--total", choices=["low", "high"], default="high")
    p4.add_argument(
        "--bad-address",
        action="store_true",
        help="send an empty shipping address -> non-retryable InvalidAddress -> compensation",
    )

    args = parser.parse_args()
    builder = {
        "dag1": _dag1_args,
        "dag2": _dag2_args,
        "dag3": _dag3_args,
        "dag4": _dag4_args,
    }[args.dag]
    wf, wf_input, workflow_id = builder(args)

    client = await Client.connect(TEMPORAL_ADDRESS, namespace=TEMPORAL_NAMESPACE)
    logger.info("Starting %s (id=%s) on task queue '%s'", wf.__name__, workflow_id, TASK_QUEUE)

    result = await client.execute_workflow(
        wf.run,
        wf_input,
        id=workflow_id,
        task_queue=TASK_QUEUE,
        execution_timeout=timedelta(minutes=15),
    )

    logger.info("Workflow %s completed", workflow_id)
    print(asdict(result) if hasattr(result, "__dataclass_fields__") else result)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    asyncio.run(main())

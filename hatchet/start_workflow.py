"""
Starter client for the Hatchet bake-off workflows.

Hatchet workflows are declared with ``on_events=[...]`` triggers, but they can
also be invoked directly by a client. This script does the latter -- one
subcommand per DAG -- so the four are runnable without hand-crafting events.

Tool idioms demonstrated:
  - ``workflow.aio_run()`` (invoke and await the full DAG result)
  - inputs as plain dicts validated by the workflow's ``EmptyModel`` extra fields
  - the same declarations a ``hatchet.event.push()`` trigger would drive

Usage:
    uv run python start_workflow.py dag1
    uv run python start_workflow.py dag3 --amount 250
    uv run python start_workflow.py dag4 --total high     # forces approval
"""

import argparse
import asyncio
import json
import logging
import os
import uuid

from dag1_csv_etl import csv_etl_wf
from dag2_api_fanout import api_fanout_wf
from dag3_payment import payment_wf
from dag4_order_fulfillment import order_fulfillment_wf

logger = logging.getLogger(__name__)


# ---- DAG argument builders ------------------------------------------------


def _dag1_args(args: argparse.Namespace) -> tuple:
    return csv_etl_wf, {
        "zip_path": args.zip_file,
        "extract_dir": args.extract_dir,
        "output_dir": args.output_dir,
    }


def _dag2_args(args: argparse.Namespace) -> tuple:
    return api_fanout_wf, {"url": args.url, "request_config": {}}


def _dag3_args(args: argparse.Namespace) -> tuple:
    payment_id = args.payment_id or f"PAY-{uuid.uuid4().hex[:8]}"
    return payment_wf, {
        "payment_id": payment_id,
        "amount": args.amount,
        "currency": "USD",
        "from_account": args.from_account,
        "to_account": args.to_account,
        "idempotency_key": payment_id,
    }


def _dag4_args(args: argparse.Namespace) -> tuple:
    # "low" stays under the 500.00 approval threshold; "high" trips it.
    items = (
        [{"sku": "WIDGET-A", "quantity": 2, "unit_price": 29.99}]
        if args.total == "low"
        else [{"sku": "WIDGET-A", "quantity": 40, "unit_price": 29.99}]
    )
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
    return order_fulfillment_wf, {
        "order_id": args.order_id or f"ORD-{uuid.uuid4().hex[:8]}",
        "customer_id": args.customer_id,
        "items": items,
        "shipping_address": address,
        "approval_threshold": 500.00,
    }


# ---- Entrypoint -----------------------------------------------------------


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="dag", required=True)

    p1 = sub.add_parser("dag1", help="CSV ETL pipeline")
    p1.add_argument("--zip-file", default="../test-data/sample-data.zip")
    p1.add_argument("--extract-dir", default="/tmp/hatchet-dag1/extracted")
    p1.add_argument("--output-dir", default="/tmp/hatchet-dag1/output")

    p2 = sub.add_parser("dag2", help="API fan-out with async callback")
    p2.add_argument(
        "--url",
        default=os.environ.get("DAG2_URL", "http://fixture-service:8099/books?base=http://localhost:8099"),
    )

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
    workflow, wf_input = builder(args)

    logger.info("Running %s with input: %s", workflow.name, json.dumps(wf_input))
    result = await workflow.aio_run(wf_input)
    logger.info("%s completed", workflow.name)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    asyncio.run(main())

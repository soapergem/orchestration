"""
Validates that all SKUs exist, customer is active, and computes total amount.
Read-only -- no mutations, so no compensation needed on failure.
"""

import json
import os
import sys

import psycopg2


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "postgres"),
        port=int(os.environ.get("PGPORT", "5432")),
        dbname=os.environ.get("PGDATABASE", "orchestration"),
        user=os.environ.get("PGUSER", "orchestration"),
        password=os.environ.get("PGPASSWORD", "orchestration"),
    )


def main():
    event = json.loads(os.environ["INPUT"])

    order_id = event["order_id"]
    customer_id = event["customer_id"]
    items = event["items"]
    approval_threshold = event.get("approval_threshold", 500.00)

    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute(
            "SELECT status FROM customers WHERE customer_id = %s",
            (customer_id,),
        )
        row = cur.fetchone()
        if not row:
            result = {
                **event,
                "validation": {
                    "is_valid": False,
                    "reason": f"Customer {customer_id} not found",
                },
            }
            json.dump(result, sys.stdout)
            return
        if row[0] != "active":
            result = {
                **event,
                "validation": {
                    "is_valid": False,
                    "reason": f"Customer {customer_id} is {row[0]}",
                },
            }
            json.dump(result, sys.stdout)
            return

        total_amount = 0
        for item in items:
            sku = item["sku"]
            quantity = item["quantity"]

            cur.execute(
                "SELECT available_quantity, unit_price FROM inventory WHERE sku = %s",
                (sku,),
            )
            row = cur.fetchone()
            if not row:
                result = {
                    **event,
                    "validation": {
                        "is_valid": False,
                        "reason": f"SKU {sku} not found",
                    },
                }
                json.dump(result, sys.stdout)
                return

            available, unit_price = row
            if available < quantity:
                result = {
                    **event,
                    "validation": {
                        "is_valid": False,
                        "reason": f"Insufficient stock for {sku}: requested {quantity}, available {available}",
                    },
                }
                json.dump(result, sys.stdout)
                return

            total_amount += unit_price * quantity

        result = {
            **event,
            "total_amount": float(total_amount),
            "approval_threshold": approval_threshold,
            "validation": {"is_valid": True, "reason": None},
        }
        json.dump(result, sys.stdout)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

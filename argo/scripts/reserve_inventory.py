"""
Atomically reserves inventory for all items in the order.
Creates reservation records and decrements available_quantity.
Uses a single transaction so it's all-or-nothing.
"""

import json
import os
import sys
import uuid
from datetime import datetime, timezone

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
    items = event["items"]
    customer_id = event.get("customer_id", "unknown")

    reservation_id = f"RES-{uuid.uuid4().hex[:12].upper()}"

    conn = get_connection()
    try:
        cur = conn.cursor()

        # Check for existing reservation (idempotency)
        cur.execute(
            "SELECT reservation_id FROM inventory_reservations WHERE order_id = %s AND status = 'reserved' LIMIT 1",
            (order_id,),
        )
        existing = cur.fetchone()
        if existing:
            result = {
                "reservation_id": existing[0],
                "items_reserved": [i["sku"] for i in items],
                "reserved_at": datetime.now(timezone.utc).isoformat(),
                "idempotent": True,
            }
            json.dump(result, sys.stdout)
            return

        items_reserved = []
        for item in items:
            sku = item["sku"]
            quantity = item["quantity"]

            cur.execute(
                """
                UPDATE inventory
                SET available_quantity = available_quantity - %s,
                    reserved_quantity = reserved_quantity + %s
                WHERE sku = %s AND available_quantity >= %s
                RETURNING sku
                """,
                (quantity, quantity, sku, quantity),
            )
            if cur.fetchone() is None:
                conn.rollback()
                print(
                    json.dumps({"error": f"InsufficientStock: Cannot reserve {quantity} of {sku}"}),
                    file=sys.stderr,
                )
                sys.exit(1)

            cur.execute(
                """
                INSERT INTO inventory_reservations (reservation_id, order_id, sku, quantity, status)
                VALUES (%s, %s, %s, %s, 'reserved')
                """,
                (f"{reservation_id}-{sku}", order_id, sku, quantity),
            )
            items_reserved.append(sku)

        # Create the order record
        total = sum(i["quantity"] * i.get("unit_price", 0) for i in items)
        cur.execute(
            """
            INSERT INTO orders (order_id, customer_id, total_amount, status)
            VALUES (%s, %s, %s, 'reserved')
            ON CONFLICT (order_id) DO UPDATE SET status = 'reserved', updated_at = NOW()
            """,
            (order_id, customer_id, total),
        )

        conn.commit()

        result = {
            "reservation_id": reservation_id,
            "items_reserved": items_reserved,
            "reserved_at": datetime.now(timezone.utc).isoformat(),
        }
        json.dump(result, sys.stdout)
    except Exception as e:
        conn.rollback()
        print(json.dumps({"error": str(e)}), file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

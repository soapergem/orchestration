"""
Saga compensation: reverses inventory reservations for an order.
Idempotent -- safe to call multiple times (no-op if already released).
"""

import json
import os
import sys
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
    failure_reason = event.get("failure_reason", "unknown")

    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT reservation_id, sku, quantity
            FROM inventory_reservations
            WHERE order_id = %s AND status = 'reserved'
            """,
            (order_id,),
        )
        reservations = cur.fetchall()

        if not reservations:
            result = {
                "order_id": order_id,
                "released": 0,
                "status": "no_reservations_to_release",
                "failure_reason": failure_reason,
            }
            json.dump(result, sys.stdout)
            return

        released = 0
        for reservation_id, sku, quantity in reservations:
            cur.execute(
                """
                UPDATE inventory
                SET available_quantity = available_quantity + %s,
                    reserved_quantity = reserved_quantity - %s
                WHERE sku = %s
                """,
                (quantity, quantity, sku),
            )
            cur.execute(
                """
                UPDATE inventory_reservations
                SET status = 'released', released_at = %s
                WHERE reservation_id = %s AND status = 'reserved'
                """,
                (datetime.now(timezone.utc), reservation_id),
            )
            released += 1

        conn.commit()

        result = {
            "order_id": order_id,
            "released": released,
            "status": "inventory_released",
            "failure_reason": failure_reason,
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

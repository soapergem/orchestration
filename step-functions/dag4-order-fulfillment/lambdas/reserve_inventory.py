"""
Lambda: ReserveInventory
Atomically reserves inventory for all items in the order.
Creates reservation records and decrements available_quantity.
Uses a single transaction so it's all-or-nothing.
"""

import uuid
from datetime import datetime, timezone

from db import get_db_connection


def handler(event, context):
    order_id = event["order_id"]
    items = event["items"]

    reservation_id = f"RES-{uuid.uuid4().hex[:12].upper()}"

    conn = get_db_connection()
    try:
        cur = conn.cursor()

        # Check for existing reservation (idempotency)
        cur.execute(
            "SELECT reservation_id FROM inventory_reservations WHERE order_id = %s AND status = 'reserved' LIMIT 1",
            (order_id,),
        )
        existing = cur.fetchone()
        if existing:
            return {
                "reservation_id": existing[0],
                "items_reserved": [i["sku"] for i in items],
                "reserved_at": datetime.now(timezone.utc).isoformat(),
                "idempotent": True,
            }

        # Create the order record first: inventory_reservations.order_id has a
        # FK to orders, so the order row must exist before the reservations.
        # Same transaction, so it's still all-or-nothing.
        total = sum(i["quantity"] * i["unit_price"] for i in items)
        cur.execute(
            """
            INSERT INTO orders (order_id, customer_id, total_amount, status)
            VALUES (%s, %s, %s, 'reserved')
            ON CONFLICT (order_id) DO UPDATE SET status = 'reserved', updated_at = NOW()
            """,
            (order_id, event.get("customer_id", "unknown"), total),
        )

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
                raise Exception(f"InsufficientStock: Cannot reserve {quantity} of {sku}")

            cur.execute(
                """
                INSERT INTO inventory_reservations (reservation_id, order_id, sku, quantity, status)
                VALUES (%s, %s, %s, %s, 'reserved')
                """,
                (f"{reservation_id}-{sku}", order_id, sku, quantity),
            )
            items_reserved.append(sku)

        conn.commit()

        return {
            "reservation_id": reservation_id,
            "items_reserved": items_reserved,
            "reserved_at": datetime.now(timezone.utc).isoformat(),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

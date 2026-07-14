"""
Lambda: ReleaseInventory
Saga compensation: reverses inventory reservations for an order.
Idempotent — safe to call multiple times (no-op if already released).
"""

from datetime import datetime, timezone

from db import get_db_connection


def handler(event, context):
    order_id = event["order_id"]

    conn = get_db_connection()
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
            return {
                "order_id": order_id,
                "released": 0,
                "status": "no_reservations_to_release",
                "failure_reason": event.get("failure_reason"),
            }

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

        return {
            "order_id": order_id,
            "released": released,
            "status": "inventory_released",
            "failure_reason": event.get("failure_reason"),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

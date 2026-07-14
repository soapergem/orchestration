"""
Lambda: UpdateOrderStatus
Updates the order record in the database. Used for both success (shipped) and
compensation (cancelled/failed) paths.
"""

from datetime import datetime, timezone

from db import get_db_connection


def handler(event, context):
    order_id = event["order_id"]
    status = event["status"]
    shipment_id = event.get("shipment_id")
    tracking_number = event.get("tracking_number")
    failure_reason = event.get("failure_reason")

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        now = datetime.now(timezone.utc)

        cur.execute(
            """
            UPDATE orders
            SET status = %s,
                shipment_id = COALESCE(%s, shipment_id),
                tracking_number = COALESCE(%s, tracking_number),
                failure_reason = COALESCE(%s, failure_reason),
                updated_at = %s
            WHERE order_id = %s
            RETURNING order_id, status
            """,
            (status, shipment_id, tracking_number, failure_reason, now, order_id),
        )
        result = cur.fetchone()
        conn.commit()

        if not result:
            raise Exception(f"Order {order_id} not found")

        return {
            "order_id": result[0],
            "status": result[1],
            "updated_at": now.isoformat(),
        }
    finally:
        conn.close()

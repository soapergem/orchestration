"""
Updates the order record in the database. Used for both success (shipped) and
compensation (cancelled/failed) paths.
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
    status = event["status"]
    shipment_id = event.get("shipment_id")
    tracking_number = event.get("tracking_number")
    failure_reason = event.get("failure_reason")

    conn = get_connection()
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
            print(json.dumps({"error": f"Order {order_id} not found"}), file=sys.stderr)
            sys.exit(1)

        output = {
            "order_id": result[0],
            "status": result[1],
            "updated_at": now.isoformat(),
        }
        json.dump(output, sys.stdout)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

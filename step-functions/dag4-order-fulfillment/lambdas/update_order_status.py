"""
Lambda: UpdateOrderStatus
Updates the order record in the database. Used for both success (shipped) and
compensation (cancelled/failed) paths.
"""

import json
from datetime import datetime, timezone

import boto3
import psycopg2


secrets = boto3.client("secretsmanager")


def get_connection(db_config):
    secret = secrets.get_secret_value(SecretId=db_config["secret_arn"])
    creds = json.loads(secret["SecretString"])
    return psycopg2.connect(
        host=creds["host"],
        port=creds.get("port", 5432),
        dbname=creds["dbname"],
        user=creds["username"],
        password=creds["password"],
    )


def handler(event, context):
    order_id = event["order_id"]
    status = event["status"]
    db_config = event["db_config"]
    shipment_id = event.get("shipment_id")
    tracking_number = event.get("tracking_number")
    failure_reason = event.get("failure_reason")

    conn = get_connection(db_config)
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

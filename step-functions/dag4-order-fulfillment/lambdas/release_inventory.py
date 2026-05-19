"""
Lambda: ReleaseInventory
Saga compensation: reverses inventory reservations for an order.
Idempotent — safe to call multiple times (no-op if already released).
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
    db_config = event["db_config"]

    conn = get_connection(db_config)
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
                "db_config": db_config,
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
            "db_config": db_config,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

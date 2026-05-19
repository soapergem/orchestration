"""
Lambda: ReserveInventory
Atomically reserves inventory for all items in the order.
Creates reservation records and decrements available_quantity.
Uses a single transaction so it's all-or-nothing.
"""

import json
import uuid
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
    items = event["items"]
    db_config = event["db_config"]

    reservation_id = f"RES-{uuid.uuid4().hex[:12].upper()}"

    conn = get_connection(db_config)
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

        # Create the order record
        total = sum(i["quantity"] * i["unit_price"] for i in items)
        cur.execute(
            """
            INSERT INTO orders (order_id, customer_id, total_amount, status)
            VALUES (%s, %s, %s, 'reserved')
            ON CONFLICT (order_id) DO UPDATE SET status = 'reserved', updated_at = NOW()
            """,
            (order_id, event.get("customer_id", "unknown"), total),
        )

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

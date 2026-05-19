"""
Lambda: ValidateOrder
Validates that all SKUs exist, customer is active, and computes total amount.
Read-only — no mutations, so no compensation needed on failure.
"""

import json

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
    customer_id = event["customer_id"]
    items = event["items"]
    db_config = event["db_config"]
    approval_threshold = event.get("approval_threshold", 500.00)

    conn = get_connection(db_config)
    try:
        cur = conn.cursor()

        cur.execute(
            "SELECT status FROM customers WHERE customer_id = %s",
            (customer_id,),
        )
        row = cur.fetchone()
        if not row:
            return {**event, "validation": {"is_valid": False, "reason": f"Customer {customer_id} not found"}}
        if row[0] != "active":
            return {**event, "validation": {"is_valid": False, "reason": f"Customer {customer_id} is {row[0]}"}}

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
                return {**event, "validation": {"is_valid": False, "reason": f"SKU {sku} not found"}}

            available, unit_price = row
            if available < quantity:
                return {
                    **event,
                    "validation": {
                        "is_valid": False,
                        "reason": f"Insufficient stock for {sku}: requested {quantity}, available {available}",
                    },
                }
            total_amount += unit_price * quantity

        return {
            **event,
            "total_amount": float(total_amount),
            "approval_threshold": approval_threshold,
            "validation": {"is_valid": True, "reason": None},
        }
    finally:
        conn.close()

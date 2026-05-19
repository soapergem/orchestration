"""
Lambda: HandlePaymentFailure
Records a payment failure in the database and prepares failure notification data.
"""

import json
from datetime import datetime, timezone

import boto3
import psycopg2


secrets = boto3.client("secretsmanager")


def get_db_connection(db_config):
    secret = secrets.get_secret_value(SecretId=db_config["secret_arn"])
    creds = json.loads(secret["SecretString"])

    return psycopg2.connect(
        host=db_config["host"],
        database=db_config["database"],
        user=creds["username"],
        password=creds["password"],
        port=creds.get("port", 5432),
    )


def handler(event, context):
    payment_id = event["payment_id"]
    idempotency_key = event.get("idempotency_key", payment_id)
    error_info = event.get("error", {})
    db_config = event["db_config"]

    error_message = error_info.get("Cause", error_info.get("reason", "Unknown error"))

    conn = get_db_connection(db_config)
    try:
        with conn.cursor() as cur:
            now = datetime.now(timezone.utc).isoformat()

            # Record failed transaction (idempotent)
            cur.execute(
                "SELECT id FROM transactions WHERE idempotency_key = %s",
                (idempotency_key,),
            )
            if cur.fetchone() is None:
                cur.execute(
                    """INSERT INTO transactions
                       (payment_id, idempotency_key, from_account, to_account,
                        amount, currency, status, error_message, created_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (
                        payment_id,
                        idempotency_key,
                        event.get("from_account"),
                        event.get("to_account"),
                        event.get("amount"),
                        event.get("currency"),
                        "failed",
                        error_message,
                        now,
                    ),
                )

        conn.commit()
    finally:
        conn.close()

    return {
        "payment_id": payment_id,
        "amount": event.get("amount"),
        "currency": event.get("currency"),
        "status": "failed",
        "failure_message": error_message,
    }

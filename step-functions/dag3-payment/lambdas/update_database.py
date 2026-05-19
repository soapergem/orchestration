"""
Lambda: UpdateDatabase
Records the payment result in the database: debits/credits accounts, writes transaction record.
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
    amount = event["amount"]
    from_account = event["from_account"]
    to_account = event["to_account"]
    idempotency_key = event.get("idempotency_key", payment_id)
    payment_result = event["payment_result"]
    db_config = event["db_config"]

    conn = get_db_connection(db_config)
    try:
        with conn.cursor() as cur:
            now = datetime.now(timezone.utc).isoformat()

            # Check idempotency -- don't double-apply
            cur.execute(
                "SELECT id FROM transactions WHERE idempotency_key = %s",
                (idempotency_key,),
            )
            if cur.fetchone() is not None:
                conn.rollback()
                return {
                    **event,
                    "db_update": {
                        "status": "skipped",
                        "reason": "Transaction already recorded (idempotent)",
                    },
                }

            # Debit source account
            cur.execute(
                "UPDATE accounts SET balance = balance - %s, updated_at = %s "
                "WHERE account_id = %s",
                (amount, now, from_account),
            )

            # Credit destination account
            cur.execute(
                "UPDATE accounts SET balance = balance + %s, updated_at = %s "
                "WHERE account_id = %s",
                (amount, now, to_account),
            )

            # Record transaction
            cur.execute(
                """INSERT INTO transactions
                   (payment_id, idempotency_key, from_account, to_account,
                    amount, currency, status, gateway_transaction_id, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    payment_id,
                    idempotency_key,
                    from_account,
                    to_account,
                    amount,
                    event["currency"],
                    "completed",
                    payment_result["gateway_transaction_id"],
                    now,
                ),
            )

        conn.commit()
    finally:
        conn.close()

    return {
        **event,
        "db_update": {"status": "success", "recorded_at": now},
    }

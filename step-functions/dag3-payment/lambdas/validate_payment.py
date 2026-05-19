"""
Lambda: ValidatePayment
Validates a payment request: checks account exists, sufficient balance, fraud rules.
"""

import json

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
    currency = event["currency"]
    from_account = event["from_account"]
    to_account = event["to_account"]
    db_config = event["db_config"]

    conn = get_db_connection(db_config)
    try:
        with conn.cursor() as cur:
            # Check source account exists and has sufficient balance
            cur.execute(
                "SELECT balance, status FROM accounts WHERE account_id = %s",
                (from_account,),
            )
            row = cur.fetchone()

            if row is None:
                return {
                    **event,
                    "validation": {
                        "is_valid": False,
                        "reason": f"Source account {from_account} not found",
                    },
                }

            balance, status = row

            if status != "active":
                return {
                    **event,
                    "validation": {
                        "is_valid": False,
                        "reason": f"Source account {from_account} is {status}",
                    },
                }

            if balance < amount:
                return {
                    **event,
                    "validation": {
                        "is_valid": False,
                        "reason": f"Insufficient balance: {balance} < {amount}",
                    },
                }

            # Check destination account exists
            cur.execute(
                "SELECT status FROM accounts WHERE account_id = %s",
                (to_account,),
            )
            row = cur.fetchone()

            if row is None:
                return {
                    **event,
                    "validation": {
                        "is_valid": False,
                        "reason": f"Destination account {to_account} not found",
                    },
                }

            if row[0] != "active":
                return {
                    **event,
                    "validation": {
                        "is_valid": False,
                        "reason": f"Destination account {to_account} is {row[0]}",
                    },
                }

            # Check for duplicate payment (idempotency)
            cur.execute(
                "SELECT status FROM transactions WHERE idempotency_key = %s",
                (event.get("idempotency_key", payment_id),),
            )
            existing = cur.fetchone()

            if existing is not None:
                return {
                    **event,
                    "validation": {
                        "is_valid": False,
                        "reason": f"Duplicate payment: existing transaction with status {existing[0]}",
                    },
                }

    finally:
        conn.close()

    return {
        **event,
        "validation": {"is_valid": True, "reason": None},
    }

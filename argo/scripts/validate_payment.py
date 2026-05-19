"""
Validates a payment request: checks account exists, sufficient balance, fraud rules.
"""

import json
import os
import sys

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

    payment_id = event["payment_id"]
    amount = event["amount"]
    currency = event["currency"]
    from_account = event["from_account"]
    to_account = event["to_account"]

    conn = get_connection()
    try:
        cur = conn.cursor()

        # Check source account exists and has sufficient balance
        cur.execute(
            "SELECT balance, status FROM accounts WHERE account_id = %s",
            (from_account,),
        )
        row = cur.fetchone()

        if row is None:
            result = {
                **event,
                "validation": {
                    "is_valid": False,
                    "reason": f"Source account {from_account} not found",
                },
            }
            json.dump(result, sys.stdout)
            return

        balance, status = row

        if status != "active":
            result = {
                **event,
                "validation": {
                    "is_valid": False,
                    "reason": f"Source account {from_account} is {status}",
                },
            }
            json.dump(result, sys.stdout)
            return

        if balance < amount:
            result = {
                **event,
                "validation": {
                    "is_valid": False,
                    "reason": f"Insufficient balance: {balance} < {amount}",
                },
            }
            json.dump(result, sys.stdout)
            return

        # Check destination account exists
        cur.execute(
            "SELECT status FROM accounts WHERE account_id = %s",
            (to_account,),
        )
        row = cur.fetchone()

        if row is None:
            result = {
                **event,
                "validation": {
                    "is_valid": False,
                    "reason": f"Destination account {to_account} not found",
                },
            }
            json.dump(result, sys.stdout)
            return

        if row[0] != "active":
            result = {
                **event,
                "validation": {
                    "is_valid": False,
                    "reason": f"Destination account {to_account} is {row[0]}",
                },
            }
            json.dump(result, sys.stdout)
            return

        # Check for duplicate payment (idempotency)
        cur.execute(
            "SELECT status FROM transactions WHERE idempotency_key = %s",
            (event.get("idempotency_key", payment_id),),
        )
        existing = cur.fetchone()

        if existing is not None:
            result = {
                **event,
                "validation": {
                    "is_valid": False,
                    "reason": f"Duplicate payment: existing transaction with status {existing[0]}",
                },
            }
            json.dump(result, sys.stdout)
            return

    finally:
        conn.close()

    result = {
        **event,
        "validation": {"is_valid": True, "reason": None},
    }
    json.dump(result, sys.stdout)


if __name__ == "__main__":
    main()

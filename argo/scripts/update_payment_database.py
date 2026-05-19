"""
Records the payment result in the database: debits/credits accounts, writes transaction record.
Idempotent -- checks for existing transaction before applying.
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

    payment_id = event["payment_id"]
    amount = event["amount"]
    from_account = event["from_account"]
    to_account = event["to_account"]
    idempotency_key = event.get("idempotency_key", payment_id)
    payment_result = event["payment_result"]

    conn = get_connection()
    try:
        cur = conn.cursor()
        now = datetime.now(timezone.utc).isoformat()

        # Check idempotency -- don't double-apply
        cur.execute(
            "SELECT id FROM transactions WHERE idempotency_key = %s",
            (idempotency_key,),
        )
        if cur.fetchone() is not None:
            conn.rollback()
            result = {
                **event,
                "db_update": {
                    "status": "skipped",
                    "reason": "Transaction already recorded (idempotent)",
                },
            }
            json.dump(result, sys.stdout)
            return

        # Debit source account
        cur.execute(
            "UPDATE accounts SET balance = balance - %s, updated_at = %s WHERE account_id = %s",
            (amount, now, from_account),
        )

        # Credit destination account
        cur.execute(
            "UPDATE accounts SET balance = balance + %s, updated_at = %s WHERE account_id = %s",
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

        result = {
            **event,
            "db_update": {"status": "success", "recorded_at": now},
        }
        json.dump(result, sys.stdout)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

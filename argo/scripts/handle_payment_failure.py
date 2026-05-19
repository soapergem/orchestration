"""
Records a payment failure in the database and prepares failure notification data.
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
    idempotency_key = event.get("idempotency_key", payment_id)
    error_info = event.get("error", {})

    error_message = error_info.get("Cause", error_info.get("reason", "Unknown error"))

    conn = get_connection()
    try:
        cur = conn.cursor()
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

    result = {
        "payment_id": payment_id,
        "amount": event.get("amount"),
        "currency": event.get("currency"),
        "status": "failed",
        "failure_message": error_message,
    }
    json.dump(result, sys.stdout)


if __name__ == "__main__":
    main()

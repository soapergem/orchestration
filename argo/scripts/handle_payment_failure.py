"""
Records a payment failure in the database and prepares failure notification data.
"""

import json
import os
import sys
from datetime import datetime, timezone

import psycopg2


def get_connection():
    conn = psycopg2.connect(
        host=os.environ.get("PGHOST", "postgres"),
        port=int(os.environ.get("PGPORT", "5432")),
        dbname=os.environ.get("PGDATABASE", "orchestration"),
        user=os.environ.get("PGUSER", "orchestration"),
        password=os.environ.get("PGPASSWORD", "orchestration"),
    )

    # ---- Per-(runner, DAG) schema isolation ----
    # Mirrors the inline copy in ../dag3-*.yaml. This schema holds seeded
    # fixtures, so it is not self-creating: SET search_path to a missing schema
    # succeeds silently and every later query then fails with a confusing
    # "relation does not exist".
    bakeoff_ns = os.environ.get("BAKEOFF_NS", "argo")
    schema = f"{bakeoff_ns}_dag3"
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
            (schema,),
        )
        if cur.fetchone() is None:
            conn.close()
            raise RuntimeError(
                f'schema "{schema}" does not exist -- seed it with: '
                f"SELECT bootstrap_bakeoff('{bakeoff_ns}');"
            )
        cur.execute(f'SET search_path TO "{schema}"')
    return conn


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

    # Exit code 2 signals a non-retriable error -- same convention as
    # process_payment.py. The failure is now recorded, so the workflow should
    # fail rather than retry this step; the template's
    # `retryStrategy.expression` keys on exactly this code. Exiting 1 here would
    # be read as "transient, try again" and burn all the retries.
    sys.exit(2)


if __name__ == "__main__":
    main()

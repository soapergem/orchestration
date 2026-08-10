"""
Validates a payment request: checks account exists, sufficient balance, fraud rules.
"""

import json
import os
import sys

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

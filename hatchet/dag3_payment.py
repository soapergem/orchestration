"""
DAG 3: Payment Processing

Validates a payment, processes it through a simulated gateway with retries,
updates the database, and sends notifications. Handles non-retriable errors
(PaymentDeclined) and uses an on_failure handler for error recording.

Hatchet features used:
- Task-level retries with backoff
- NonRetryableException for payment declined (should not retry)
- on_failure workflow handler for recording failures
- try/except for graceful degradation on notifications
- DAG-style sequential dependencies
"""

import json
import os
import random
from datetime import datetime, timezone

import psycopg2
from hatchet_sdk import Context, Hatchet, NonRetryableException

hatchet = Hatchet()

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

DB_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": int(os.environ.get("POSTGRES_PORT", "5432")),
    "dbname": os.environ.get("POSTGRES_DB", "orchestration"),
    "user": os.environ.get("POSTGRES_USER", "orchestration"),
    "password": os.environ.get("POSTGRES_PASSWORD", "orchestration"),
}


# Per-(runner, DAG) schema isolation -- see CLAUDE.md. DAG 3 does NOT create its
# schema: it needs the seeded accounts/transactions fixtures, so a missing schema
# is a setup error to surface, not something to paper over with empty tables.
BAKEOFF_NS = os.environ.get("BAKEOFF_NS", "hatchet")
SCHEMA = f"{BAKEOFF_NS}_dag3"


def get_db_connection(db_config: dict | None = None) -> psycopg2.extensions.connection:
    cfg = db_config or DB_CONFIG
    conn = psycopg2.connect(
        host=cfg["host"],
        port=cfg.get("port", 5432),
        dbname=cfg.get("dbname", cfg.get("database", "orchestration")),
        user=cfg["user"],
        password=cfg["password"],
    )
    schema = cfg.get("schema", SCHEMA)
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name = %s", (schema,))
        if cur.fetchone() is None:
            conn.close()
            raise RuntimeError(
                f'schema "{schema}" does not exist -- run `just seed {BAKEOFF_NS}` first'
            )
        cur.execute(f'SET search_path TO "{schema}"')
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------


class PaymentGatewayTimeout(Exception):
    """Retriable: gateway timed out."""


class PaymentGateway5xx(Exception):
    """Retriable: gateway returned 5xx."""


class PaymentDeclined(Exception):
    """Non-retriable: card was declined by issuing bank."""


# ---------------------------------------------------------------------------
# Payment Processing Workflow
# ---------------------------------------------------------------------------
#
# Payment Processing Pipeline:
# 1. validate_payment -- check accounts, balance, fraud, idempotency
# 2. process_payment -- call simulated payment gateway (retries=5, non-retriable for declined)
# 3. update_database -- record transaction (debit/credit accounts)
# 4. send_notification -- best-effort notification (graceful degradation)
#
# on_failure handler records the failure in the database and sends a failure notification.

payment_wf = hatchet.workflow(name="PaymentProcessing", on_events=["payment:process"])


@payment_wf.task(
    name="validate_payment",
    retries=3,
    backoff_factor=2.0,
    backoff_max_seconds=2,
)
async def validate_payment(input: dict, context: Context) -> dict:
    """Validate the payment: account existence, balance, status, idempotency."""
    input = input.model_dump()
    payment_id = input["payment_id"]
    amount = input["amount"]
    from_account = input["from_account"]
    to_account = input["to_account"]
    db_config = input.get("db_config") or DB_CONFIG

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
                    "validation": {
                        "is_valid": False,
                        "reason": f"Source account {from_account} not found",
                    },
                }

            balance, status = row

            if status != "active":
                return {
                    "validation": {
                        "is_valid": False,
                        "reason": f"Source account {from_account} is {status}",
                    },
                }

            if float(balance) < float(amount):
                return {
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
                    "validation": {
                        "is_valid": False,
                        "reason": f"Destination account {to_account} not found",
                    },
                }

            if row[0] != "active":
                return {
                    "validation": {
                        "is_valid": False,
                        "reason": f"Destination account {to_account} is {row[0]}",
                    },
                }

            # Check for duplicate payment (idempotency)
            idempotency_key = input.get("idempotency_key", payment_id)
            cur.execute(
                "SELECT status FROM transactions WHERE idempotency_key = %s",
                (idempotency_key,),
            )
            existing = cur.fetchone()

            if existing is not None:
                return {
                    "validation": {
                        "is_valid": False,
                        "reason": f"Duplicate payment: existing transaction with status {existing[0]}",
                    },
                }
    finally:
        conn.close()

    return {
        "validation": {"is_valid": True, "reason": None},
    }


@payment_wf.task(
    name="process_payment",
    parents=[validate_payment],
    retries=5,
    backoff_factor=2.0,
    backoff_max_seconds=3,
)
async def process_payment(input: dict, context: Context) -> dict:
    """
    Call the simulated payment gateway.
    Raises NonRetryableException for PaymentDeclined so Hatchet
    does not retry a declined card.
    """
    input = input.model_dump()
    validation_result = context.task_output(validate_payment)
    validation = validation_result["validation"]

    # If validation failed, skip processing -- raise non-retriable
    if not validation["is_valid"]:
        raise NonRetryableException(
            f"Payment validation failed: {validation['reason']}"
        )

    payment_id = input["payment_id"]
    amount = input["amount"]
    currency = input["currency"]

    # --- Simulated payment gateway ---
    # 60% success, 20% timeout (retriable), 15% 5xx (retriable), 5% declined (non-retriable)
    roll = random.random()

    if roll < 0.05:
        # Non-retriable: card declined
        raise NonRetryableException(
            json.dumps(
                {
                    "payment_id": payment_id,
                    "reason": "Card declined by issuing bank",
                    "decline_code": "insufficient_funds",
                }
            )
        )
    elif roll < 0.20:
        raise PaymentGateway5xx(
            f"Payment gateway returned 500 for payment {payment_id}"
        )
    elif roll < 0.40:
        raise PaymentGatewayTimeout(
            f"Payment gateway timed out for payment {payment_id}"
        )

    # Success
    gateway_transaction_id = f"gw-txn-{payment_id}-{random.randint(10000, 99999)}"

    return {
        "payment_result": {
            "status": "success",
            "gateway_transaction_id": gateway_transaction_id,
            "amount_charged": amount,
            "currency": currency,
        },
    }


@payment_wf.task(
    name="update_database",
    parents=[process_payment],
    retries=3,
    backoff_factor=2.0,
    backoff_max_seconds=2,
)
async def update_database(input: dict, context: Context) -> dict:
    """Record the payment: debit source, credit destination, write transaction."""
    input = input.model_dump()
    payment_id = input["payment_id"]
    amount = input["amount"]
    currency = input["currency"]
    from_account = input["from_account"]
    to_account = input["to_account"]
    idempotency_key = input.get("idempotency_key", payment_id)
    db_config = input.get("db_config") or DB_CONFIG

    process_result = context.task_output(process_payment)
    payment_result = process_result["payment_result"]

    conn = get_db_connection(db_config)
    try:
        with conn.cursor() as cur:
            now = datetime.now(timezone.utc).isoformat()

            # Idempotency check
            cur.execute(
                "SELECT id FROM transactions WHERE idempotency_key = %s",
                (idempotency_key,),
            )
            if cur.fetchone() is not None:
                conn.rollback()
                return {
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
                    currency,
                    "completed",
                    payment_result["gateway_transaction_id"],
                    now,
                ),
            )

        conn.commit()
    finally:
        conn.close()

    return {
        "db_update": {"status": "success", "recorded_at": now},
    }


@payment_wf.task(
    name="send_notification",
    parents=[update_database],
    retries=3,
    backoff_factor=2.0,
    backoff_max_seconds=2,
)
async def send_notification(input: dict, context: Context) -> dict:
    """
    Send a success notification. Uses try/except for graceful degradation --
    a notification failure should not fail the entire payment workflow.
    """
    input = input.model_dump()
    payment_id = input["payment_id"]
    amount = input.get("amount")
    currency = input.get("currency")

    try:
        process_result = context.task_output(process_payment)
        gateway_txn = process_result.get("payment_result", {}).get(
            "gateway_transaction_id", "N/A"
        )

        subject = f"Payment Successful: {payment_id}"
        body = (
            f"Payment {payment_id} for {amount} {currency} was processed successfully.\n"
            f"Gateway Transaction ID: {gateway_txn}"
        )

        # Simulated notification (in production: SES, SNS, webhook)
        print(f"NOTIFICATION: {subject}")
        print(f"BODY: {body}")

        return {
            "payment_id": payment_id,
            "notification": {
                "status": "sent",
                "subject": subject,
                "channel": "simulated",
            },
        }
    except Exception as e:
        # Graceful degradation: payment succeeded, notification failed
        print(f"WARNING: Notification failed for payment {payment_id}: {e}")
        return {
            "payment_id": payment_id,
            "notification": {
                "status": "failed",
                "error": str(e),
                "channel": "simulated",
            },
        }


@payment_wf.on_failure_task(name="handle_payment_failure")
async def handle_payment_failure(input: dict, context: Context) -> dict:
    """
    on_failure handler: records the payment failure in the database
    and sends a failure notification. This runs when any task in the
    workflow fails after exhausting retries.
    """
    input = input.model_dump()
    payment_id = input.get("payment_id", "unknown")
    idempotency_key = input.get("idempotency_key", payment_id)
    db_config = input.get("db_config") or DB_CONFIG

    # Extract error details from the failure context. In an on-failure task,
    # context.task_run_errors maps each failed task name to its error message.
    task_errors = context.task_run_errors
    failure_error = (
        "; ".join(f"{name}: {msg}" for name, msg in task_errors.items())
        or "Unknown error"
    )

    # Record the failed transaction in the database
    try:
        conn = get_db_connection(db_config)
        try:
            with conn.cursor() as cur:
                now = datetime.now(timezone.utc).isoformat()

                # Idempotent: only insert if not already recorded
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
                            input.get("from_account"),
                            input.get("to_account"),
                            input.get("amount"),
                            input.get("currency"),
                            "failed",
                            failure_error,
                            now,
                        ),
                    )
            conn.commit()
        finally:
            conn.close()
    except Exception as db_err:
        print(f"WARNING: Could not record failure in database: {db_err}")

    # Send failure notification (best-effort)
    try:
        amount = input.get("amount")
        currency = input.get("currency")
        subject = f"Payment Failed: {payment_id}"
        body = (
            f"Payment {payment_id} for {amount} {currency} has failed.\n"
            f"Reason: {failure_error}"
        )
        print(f"NOTIFICATION: {subject}")
        print(f"BODY: {body}")
    except Exception as notify_err:
        print(f"WARNING: Could not send failure notification: {notify_err}")

    return {
        "payment_id": payment_id,
        "status": "failed",
        "failure_message": failure_error,
    }

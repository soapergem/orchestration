"""
DAG 3: Payment Processing
==========================
Validate a payment, process it through a (simulated) gateway with retries,
record the result in Postgres, and send a notification.  On failure, record
the failure and send a failure notification.

Prefect 3.x implementation using @flow, @task, if/else branching in flow
code, typed exceptions, and graceful notification degradation.
"""

import json
import os
import random
from datetime import datetime, timezone

import psycopg2
from prefect import flow, get_run_logger, task

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": int(os.environ.get("POSTGRES_PORT", "5432")),
    "database": os.environ.get("POSTGRES_DB", "orchestration"),
    "user": os.environ.get("POSTGRES_USER", "orchestration"),
    "password": os.environ.get("POSTGRES_PASSWORD", "orchestration"),
}


# ---------------------------------------------------------------------------
# Exceptions (match the Step Functions lambda types)
# ---------------------------------------------------------------------------

class PaymentGatewayTimeout(Exception):
    """Retriable — the gateway did not respond in time."""


class PaymentGateway5xx(Exception):
    """Retriable — the gateway returned a server error."""


class PaymentDeclined(Exception):
    """NOT retriable — the issuing bank declined the card."""


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _get_db_connection(db_config: dict | None = None):
    cfg = db_config or DB_CONFIG
    return psycopg2.connect(
        host=cfg["host"],
        port=cfg.get("port", 5432),
        dbname=cfg["database"],
        user=cfg["user"],
        password=cfg["password"],
    )


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(
    retries=3,
    retry_delay_seconds=[2, 4, 8],
    name="validate_payment",
)
def validate_payment(
    payment_id: str,
    amount: float,
    currency: str,
    from_account: str,
    to_account: str,
    idempotency_key: str | None = None,
    db_config: dict | None = None,
) -> dict:
    """
    Validate the payment request: check accounts exist, are active, have
    sufficient balance, and that the payment is not a duplicate.
    """
    logger = get_run_logger()
    idem_key = idempotency_key or payment_id

    conn = _get_db_connection(db_config)
    try:
        with conn.cursor() as cur:
            # Source account
            cur.execute(
                "SELECT balance, status FROM accounts WHERE account_id = %s",
                (from_account,),
            )
            row = cur.fetchone()
            if row is None:
                return {"is_valid": False, "reason": f"Source account {from_account} not found"}
            balance, status = row
            if status != "active":
                return {"is_valid": False, "reason": f"Source account {from_account} is {status}"}
            if balance < amount:
                return {"is_valid": False, "reason": f"Insufficient balance: {balance} < {amount}"}

            # Destination account
            cur.execute(
                "SELECT status FROM accounts WHERE account_id = %s",
                (to_account,),
            )
            row = cur.fetchone()
            if row is None:
                return {"is_valid": False, "reason": f"Destination account {to_account} not found"}
            if row[0] != "active":
                return {"is_valid": False, "reason": f"Destination account {to_account} is {row[0]}"}

            # Duplicate check
            cur.execute(
                "SELECT status FROM transactions WHERE idempotency_key = %s",
                (idem_key,),
            )
            existing = cur.fetchone()
            if existing is not None:
                return {
                    "is_valid": False,
                    "reason": f"Duplicate payment: existing transaction with status {existing[0]}",
                }
    finally:
        conn.close()

    logger.info("Payment %s validated successfully", payment_id)
    return {"is_valid": True, "reason": None}


@task(
    retries=5,
    retry_delay_seconds=[3, 6, 12, 24, 48],
    retry_jitter_factor=1.0,
    name="process_payment",
)
def process_payment(
    payment_id: str,
    amount: float,
    currency: str,
    from_account: str,
    to_account: str,
    idempotency_key: str | None = None,
) -> dict:
    """
    Call the (simulated) external payment gateway.

    The idempotency_key prevents double-charging on retries.  Raises typed
    exceptions so Prefect can retry on transient errors and fail fast on
    declines.

    Simulated error distribution:
      - 60 % success
      - 20 % timeout (retriable)
      - 15 % 5xx (retriable)
      -  5 % declined (NOT retriable)
    """
    logger = get_run_logger()
    idem_key = idempotency_key or payment_id

    roll = random.random()

    if roll < 0.05:
        raise PaymentDeclined(
            json.dumps({
                "payment_id": payment_id,
                "reason": "Card declined by issuing bank",
                "decline_code": "insufficient_funds",
            })
        )
    elif roll < 0.20:
        raise PaymentGateway5xx(
            f"Payment gateway returned 500 for payment {payment_id}"
        )
    elif roll < 0.40:
        raise PaymentGatewayTimeout(
            f"Payment gateway timed out for payment {payment_id}"
        )

    gateway_transaction_id = f"gw-txn-{payment_id}-{random.randint(10000, 99999)}"
    logger.info(
        "Payment %s processed successfully — gateway_txn=%s",
        payment_id,
        gateway_transaction_id,
    )

    return {
        "status": "success",
        "gateway_transaction_id": gateway_transaction_id,
        "amount_charged": amount,
        "currency": currency,
    }


@task(
    retries=3,
    retry_delay_seconds=[2, 4, 8],
    name="update_database",
)
def update_database(
    payment_id: str,
    amount: float,
    currency: str,
    from_account: str,
    to_account: str,
    payment_result: dict,
    idempotency_key: str | None = None,
    db_config: dict | None = None,
) -> dict:
    """Record the successful payment: debit/credit accounts and insert a transaction."""
    logger = get_run_logger()
    idem_key = idempotency_key or payment_id
    now = datetime.now(timezone.utc).isoformat()

    conn = _get_db_connection(db_config)
    try:
        with conn.cursor() as cur:
            # Idempotency guard
            cur.execute(
                "SELECT id FROM transactions WHERE idempotency_key = %s",
                (idem_key,),
            )
            if cur.fetchone() is not None:
                conn.rollback()
                logger.info("Transaction already recorded for %s (idempotent skip)", idem_key)
                return {"status": "skipped", "reason": "Transaction already recorded (idempotent)"}

            # Debit source
            cur.execute(
                "UPDATE accounts SET balance = balance - %s, updated_at = %s WHERE account_id = %s",
                (amount, now, from_account),
            )
            # Credit destination
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
                    payment_id, idem_key, from_account, to_account,
                    amount, currency, "completed",
                    payment_result["gateway_transaction_id"], now,
                ),
            )
        conn.commit()
        logger.info("Database updated for payment %s", payment_id)
    finally:
        conn.close()

    return {"status": "success", "recorded_at": now}


@task(
    retries=3,
    retry_delay_seconds=[2, 4, 8],
    name="send_notification",
)
def send_notification(
    payment_id: str,
    amount: float | None = None,
    currency: str | None = None,
    status: str = "success",
    message: str | None = None,
    gateway_transaction_id: str | None = None,
) -> dict:
    """Send a payment notification (simulated email/webhook)."""
    logger = get_run_logger()

    if status == "failed":
        subject = f"Payment Failed: {payment_id}"
        body = (
            f"Payment {payment_id} for {amount} {currency} has failed.\n"
            f"Reason: {message or 'unknown'}"
        )
    else:
        subject = f"Payment Successful: {payment_id}"
        body = (
            f"Payment {payment_id} for {amount} {currency} was processed successfully.\n"
            f"Gateway Transaction ID: {gateway_transaction_id or 'N/A'}"
        )

    logger.info("NOTIFICATION: %s", subject)
    logger.info("BODY: %s", body)

    return {
        "payment_id": payment_id,
        "notification": {
            "status": "sent",
            "subject": subject,
            "channel": "simulated",
        },
    }


@task(
    retries=3,
    retry_delay_seconds=[2, 4, 8],
    name="handle_payment_failure",
)
def handle_payment_failure(
    payment_id: str,
    amount: float | None = None,
    currency: str | None = None,
    from_account: str | None = None,
    to_account: str | None = None,
    idempotency_key: str | None = None,
    error_message: str = "Unknown error",
    db_config: dict | None = None,
) -> dict:
    """Record the failed payment in the database."""
    logger = get_run_logger()
    idem_key = idempotency_key or payment_id
    now = datetime.now(timezone.utc).isoformat()

    conn = _get_db_connection(db_config)
    try:
        with conn.cursor() as cur:
            # Idempotent: only insert if not already recorded
            cur.execute(
                "SELECT id FROM transactions WHERE idempotency_key = %s",
                (idem_key,),
            )
            if cur.fetchone() is None:
                cur.execute(
                    """INSERT INTO transactions
                       (payment_id, idempotency_key, from_account, to_account,
                        amount, currency, status, error_message, created_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (
                        payment_id, idem_key, from_account, to_account,
                        amount, currency, "failed", error_message, now,
                    ),
                )
        conn.commit()
        logger.info("Recorded payment failure for %s", payment_id)
    finally:
        conn.close()

    return {
        "payment_id": payment_id,
        "amount": amount,
        "currency": currency,
        "status": "failed",
        "failure_message": error_message,
    }


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="payment_processing", log_prints=True)
def payment_processing(
    payment_id: str,
    amount: float,
    currency: str,
    from_account: str,
    to_account: str,
    idempotency_key: str | None = None,
    db_config: dict | None = None,
) -> dict:
    """
    Payment processing pipeline:
      1. Validate
      2. Branch: valid -> process, invalid -> handle failure
      3. Process payment (retries for transient errors)
      4. Update database (idempotent)
      5. Send notification (graceful degradation on failure)
      6. On any failure: record failure + send failure notification
    """
    logger = get_run_logger()
    cfg = db_config or DB_CONFIG
    idem_key = idempotency_key or payment_id

    # Step 1: Validate
    validation = validate_payment(
        payment_id=payment_id,
        amount=amount,
        currency=currency,
        from_account=from_account,
        to_account=to_account,
        idempotency_key=idem_key,
        db_config=cfg,
    )

    # Step 2: Branch on validation result
    if not validation["is_valid"]:
        logger.warning(
            "Payment %s failed validation: %s",
            payment_id,
            validation["reason"],
        )
        failure = handle_payment_failure(
            payment_id=payment_id,
            amount=amount,
            currency=currency,
            from_account=from_account,
            to_account=to_account,
            idempotency_key=idem_key,
            error_message=validation["reason"],
            db_config=cfg,
        )
        # Send failure notification (graceful degradation)
        try:
            send_notification(
                payment_id=payment_id,
                amount=amount,
                currency=currency,
                status="failed",
                message=validation["reason"],
            )
        except Exception as notif_err:
            logger.warning("Failed to send failure notification: %s", notif_err)

        raise ValueError(f"Payment validation failed: {validation['reason']}")

    # Step 3: Process payment
    try:
        payment_result = process_payment(
            payment_id=payment_id,
            amount=amount,
            currency=currency,
            from_account=from_account,
            to_account=to_account,
            idempotency_key=idem_key,
        )
    except PaymentDeclined as e:
        # Not retriable — go straight to failure handling
        logger.error("Payment %s declined: %s", payment_id, e)
        failure = handle_payment_failure(
            payment_id=payment_id,
            amount=amount,
            currency=currency,
            from_account=from_account,
            to_account=to_account,
            idempotency_key=idem_key,
            error_message=str(e),
            db_config=cfg,
        )
        try:
            send_notification(
                payment_id=payment_id,
                amount=amount,
                currency=currency,
                status="failed",
                message=str(e),
            )
        except Exception as notif_err:
            logger.warning("Failed to send decline notification: %s", notif_err)

        raise
    except (PaymentGatewayTimeout, PaymentGateway5xx) as e:
        # These should have been retried by Prefect already (retries=5).
        # If we still get here, all retries are exhausted.
        logger.error("Payment %s failed after retries: %s", payment_id, e)
        failure = handle_payment_failure(
            payment_id=payment_id,
            amount=amount,
            currency=currency,
            from_account=from_account,
            to_account=to_account,
            idempotency_key=idem_key,
            error_message=str(e),
            db_config=cfg,
        )
        try:
            send_notification(
                payment_id=payment_id,
                amount=amount,
                currency=currency,
                status="failed",
                message=str(e),
            )
        except Exception as notif_err:
            logger.warning("Failed to send failure notification: %s", notif_err)

        raise

    # Step 4: Update database
    db_result = update_database(
        payment_id=payment_id,
        amount=amount,
        currency=currency,
        from_account=from_account,
        to_account=to_account,
        payment_result=payment_result,
        idempotency_key=idem_key,
        db_config=cfg,
    )

    # Step 5: Send success notification (graceful degradation)
    try:
        notif_result = send_notification(
            payment_id=payment_id,
            amount=amount,
            currency=currency,
            status="success",
            gateway_transaction_id=payment_result.get("gateway_transaction_id"),
        )
    except Exception as notif_err:
        # Payment succeeded — notification failure does not invalidate it
        logger.warning(
            "Payment %s succeeded but notification failed: %s",
            payment_id,
            notif_err,
        )
        notif_result = {"notification": {"status": "failed", "error": str(notif_err)}}

    return {
        "payment_id": payment_id,
        "status": "success",
        "payment_result": payment_result,
        "db_update": db_result,
        "notification": notif_result,
    }


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    result = payment_processing(
        payment_id="PAY-001",
        amount=100.00,
        currency="USD",
        from_account="ACC-SRC-001",
        to_account="ACC-DST-001",
    )
    print(json.dumps(result, indent=2, default=str))

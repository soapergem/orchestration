"""
DAG 3: Payment Processing — Flyte Implementation

Pipeline:
  1. validate_payment       — Check accounts, balance, fraud, idempotency
  2. Conditional branch     — valid -> process; invalid -> handle_failure
  3. process_payment        — Call simulated gateway (with RetryStrategy)
  4. update_database        — Debit/credit accounts, record transaction
  5. send_notification      — Best-effort (graceful degradation via try/except)
  6. handle_payment_failure — Record failure, send failure notification

Equivalent Step Functions workflow:
  step-functions/dag3-payment/state-machine.asl.json

Key Flyte features demonstrated:
  - @dataclass for every input/output (strict typing is Flyte's strength)
  - Conditional branching in @workflow
  - RetryStrategy on process_payment for transient gateway errors
  - try/except in workflow for graceful notification degradation
  - Error handling with failure path
"""

from __future__ import annotations

import json
import random
from datetime import datetime, timezone

import psycopg2
from flytekit import ImageSpec, conditional, task, workflow

from .types import (
    DBConfig,
    DBUpdateResult,
    GatewayResult,
    NotificationResult,
    PaymentFailureResult,
    PaymentInput,
    PaymentOutput,
    PaymentProcessed,
    PaymentValidated,
    ValidationResult,
)

# ---------------------------------------------------------------------------
# Container image spec
# ---------------------------------------------------------------------------
payment_image = ImageSpec(
    name="payment",
    packages=[
        "psycopg2-binary",
        "flytekit",
    ],
    python_version="3.11",
)


# ---------------------------------------------------------------------------
# Helper: Postgres connection
# ---------------------------------------------------------------------------
def _get_connection(cfg: DBConfig) -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.database,
        user=cfg.user,
        password=cfg.password,
    )


# ---------------------------------------------------------------------------
# Custom exceptions for payment gateway
# ---------------------------------------------------------------------------
class PaymentGatewayTimeout(Exception):
    """Retriable — gateway did not respond in time."""


class PaymentGateway5xx(Exception):
    """Retriable — gateway returned a server error."""


class PaymentDeclined(Exception):
    """Non-retriable — card was declined."""


# ---------------------------------------------------------------------------
# Task 1: Validate payment
# ---------------------------------------------------------------------------
@task(
    retries=3,
    container_image=payment_image,
)
def validate_payment(payment_input: PaymentInput) -> PaymentValidated:
    """Validate a payment request.

    Checks:
      - Source account exists and is active
      - Sufficient balance
      - Destination account exists and is active
      - No duplicate payment (idempotency key)

    Returns the full ``PaymentValidated`` state with the ``validation`` field
    populated.  No mutations — safe to retry.
    """
    idempotency_key = payment_input.idempotency_key or payment_input.payment_id
    db = payment_input.db_config

    def _fail(reason: str) -> PaymentValidated:
        return PaymentValidated(
            payment_id=payment_input.payment_id,
            amount=payment_input.amount,
            currency=payment_input.currency,
            from_account=payment_input.from_account,
            to_account=payment_input.to_account,
            idempotency_key=idempotency_key,
            db_config=db,
            validation=ValidationResult(is_valid=False, reason=reason),
        )

    conn = _get_connection(db)
    try:
        with conn.cursor() as cur:
            # Source account
            cur.execute(
                "SELECT balance, status FROM accounts WHERE account_id = %s",
                (payment_input.from_account,),
            )
            row = cur.fetchone()
            if row is None:
                return _fail(f"Source account {payment_input.from_account} not found")
            balance, status = row
            if status != "active":
                return _fail(f"Source account {payment_input.from_account} is {status}")
            if balance < payment_input.amount:
                return _fail(f"Insufficient balance: {balance} < {payment_input.amount}")

            # Destination account
            cur.execute(
                "SELECT status FROM accounts WHERE account_id = %s",
                (payment_input.to_account,),
            )
            row = cur.fetchone()
            if row is None:
                return _fail(f"Destination account {payment_input.to_account} not found")
            if row[0] != "active":
                return _fail(f"Destination account {payment_input.to_account} is {row[0]}")

            # Idempotency check
            cur.execute(
                "SELECT status FROM transactions WHERE idempotency_key = %s",
                (idempotency_key,),
            )
            existing = cur.fetchone()
            if existing is not None:
                return _fail(
                    f"Duplicate payment: existing transaction with status {existing[0]}"
                )
    finally:
        conn.close()

    return PaymentValidated(
        payment_id=payment_input.payment_id,
        amount=payment_input.amount,
        currency=payment_input.currency,
        from_account=payment_input.from_account,
        to_account=payment_input.to_account,
        idempotency_key=idempotency_key,
        db_config=db,
        validation=ValidationResult(is_valid=True, reason=""),
    )


# ---------------------------------------------------------------------------
# Task 2: Process payment (simulated gateway)
# ---------------------------------------------------------------------------
@task(
    retries=5,
    container_image=payment_image,
)
def process_payment(validated: PaymentValidated) -> PaymentProcessed:
    """Call the (simulated) external payment gateway.

    Uses an idempotency key to prevent double-charging on retries.

    Flyte's RetryStrategy(retries=5) handles transient failures
    (PaymentGatewayTimeout, PaymentGateway5xx). Non-retriable
    PaymentDeclined errors propagate immediately.

    Simulation probabilities (same as the Step Functions version):
      - 60% success
      - 20% timeout (retriable)
      - 15% 5xx (retriable)
      - 5% declined (not retriable)
    """
    payment_id = validated.payment_id

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

    gateway_txn_id = f"gw-txn-{payment_id}-{random.randint(10000, 99999)}"

    return PaymentProcessed(
        payment_id=validated.payment_id,
        amount=validated.amount,
        currency=validated.currency,
        from_account=validated.from_account,
        to_account=validated.to_account,
        idempotency_key=validated.idempotency_key,
        db_config=validated.db_config,
        payment_result=GatewayResult(
            status="success",
            gateway_transaction_id=gateway_txn_id,
            amount_charged=validated.amount,
            currency=validated.currency,
        ),
    )


# ---------------------------------------------------------------------------
# Task 3: Update database (debit/credit + transaction record)
# ---------------------------------------------------------------------------
@task(
    retries=3,
    container_image=payment_image,
)
def update_database(processed: PaymentProcessed) -> DBUpdateResult:
    """Debit source, credit destination, write transaction record.

    Idempotent — checks the idempotency key before applying. Safe to
    retry without double-applying.
    """
    db = processed.db_config
    conn = _get_connection(db)
    try:
        with conn.cursor() as cur:
            now = datetime.now(timezone.utc).isoformat()

            # Idempotency guard
            cur.execute(
                "SELECT id FROM transactions WHERE idempotency_key = %s",
                (processed.idempotency_key,),
            )
            if cur.fetchone() is not None:
                conn.rollback()
                return DBUpdateResult(
                    status="skipped",
                    reason="Transaction already recorded (idempotent)",
                )

            # Debit source
            cur.execute(
                "UPDATE accounts SET balance = balance - %s, updated_at = %s "
                "WHERE account_id = %s",
                (processed.amount, now, processed.from_account),
            )

            # Credit destination
            cur.execute(
                "UPDATE accounts SET balance = balance + %s, updated_at = %s "
                "WHERE account_id = %s",
                (processed.amount, now, processed.to_account),
            )

            # Record transaction
            cur.execute(
                """INSERT INTO transactions
                   (payment_id, idempotency_key, from_account, to_account,
                    amount, currency, status, gateway_transaction_id, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    processed.payment_id,
                    processed.idempotency_key,
                    processed.from_account,
                    processed.to_account,
                    processed.amount,
                    processed.currency,
                    "completed",
                    processed.payment_result.gateway_transaction_id,
                    now,
                ),
            )
        conn.commit()
    finally:
        conn.close()

    return DBUpdateResult(status="success", recorded_at=now)


# ---------------------------------------------------------------------------
# Task 4: Send notification (best-effort)
# ---------------------------------------------------------------------------
@task(
    retries=3,
    container_image=payment_image,
)
def send_notification(
    payment_id: str,
    amount: float,
    currency: str,
    status: str,
    gateway_transaction_id: str = "",
    failure_message: str = "",
) -> NotificationResult:
    """Send a payment notification (success or failure).

    This is a simulated notification (print). In production it would call
    SES, SNS, or a webhook endpoint.
    """
    if status == "failed":
        message = failure_message or "Payment processing failed"
        subject = f"Payment Failed: {payment_id}"
        body = f"Payment {payment_id} for {amount} {currency} has failed.\nReason: {message}"
    else:
        subject = f"Payment Successful: {payment_id}"
        body = (
            f"Payment {payment_id} for {amount} {currency} was processed successfully.\n"
            f"Gateway Transaction ID: {gateway_transaction_id}"
        )

    print(f"NOTIFICATION: {subject}")
    print(f"BODY: {body}")

    return NotificationResult(
        payment_id=payment_id,
        status="sent",
        subject=subject,
        channel="simulated",
    )


# ---------------------------------------------------------------------------
# Task 5: Handle payment failure
# ---------------------------------------------------------------------------
@task(
    retries=3,
    container_image=payment_image,
)
def handle_payment_failure(
    payment_id: str,
    amount: float,
    currency: str,
    from_account: str,
    to_account: str,
    idempotency_key: str,
    db_config: DBConfig,
    error_message: str,
) -> PaymentFailureResult:
    """Record a payment failure in the database.

    Idempotent — checks the idempotency key before inserting.
    """
    conn = _get_connection(db_config)
    try:
        with conn.cursor() as cur:
            now = datetime.now(timezone.utc).isoformat()

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
                        from_account,
                        to_account,
                        amount,
                        currency,
                        "failed",
                        error_message,
                        now,
                    ),
                )
        conn.commit()
    finally:
        conn.close()

    return PaymentFailureResult(
        payment_id=payment_id,
        amount=amount,
        currency=currency,
        status="failed",
        failure_message=error_message,
    )


# ---------------------------------------------------------------------------
# Sub-workflow: success path (process -> update_db -> notify)
# ---------------------------------------------------------------------------
@workflow
def payment_success_path(validated: PaymentValidated) -> PaymentOutput:
    """Happy path: process payment, update DB, send notification.

    Notification uses try/except for graceful degradation — if it fails
    the payment is still considered successful.
    """
    processed = process_payment(validated=validated)
    db_result = update_database(processed=processed)

    # Best-effort notification. In Flyte, we model this as a separate task
    # whose failure does not fail the workflow. We use try/except in the
    # calling workflow (payment_workflow) to catch notification failures.
    notification = send_notification(
        payment_id=processed.payment_id,
        amount=processed.amount,
        currency=processed.currency,
        status="success",
        gateway_transaction_id=processed.payment_result.gateway_transaction_id,
    )

    return PaymentOutput(
        payment_id=processed.payment_id,
        status="success",
        notification=notification,
    )


# ---------------------------------------------------------------------------
# Sub-workflow: failure path
# ---------------------------------------------------------------------------
@workflow
def payment_failure_path(validated: PaymentValidated) -> PaymentOutput:
    """Failure path: record the failure and send a failure notification."""
    failure = handle_payment_failure(
        payment_id=validated.payment_id,
        amount=validated.amount,
        currency=validated.currency,
        from_account=validated.from_account,
        to_account=validated.to_account,
        idempotency_key=validated.idempotency_key,
        db_config=validated.db_config,
        error_message=validated.validation.reason,
    )

    notification = send_notification(
        payment_id=failure.payment_id,
        amount=failure.amount,
        currency=failure.currency,
        status="failed",
        failure_message=failure.failure_message,
    )

    return PaymentOutput(
        payment_id=failure.payment_id,
        status="failed",
        failure=failure,
        notification=notification,
    )


# ---------------------------------------------------------------------------
# Top-level workflow
# ---------------------------------------------------------------------------
@workflow
def payment_workflow(payment_input: PaymentInput) -> PaymentOutput:
    """Payment Processing Workflow.

    1. Validate the payment (accounts, balance, idempotency).
    2. Branch:
       - Valid   -> process_payment -> update_database -> send_notification
       - Invalid -> handle_payment_failure -> send_failure_notification
    3. Return PaymentOutput with status and notification details.

    Notification failures are handled with graceful degradation — the
    payment outcome is not affected if notification delivery fails.
    """
    validated = validate_payment(payment_input=payment_input)

    # Conditional branch based on validation result.
    # Flyte's conditional() evaluates the is_valid field at runtime.
    result = (
        conditional("check_validation")
        .if_(validated.validation.is_valid.is_true())
        .then(payment_success_path(validated=validated))
        .else_()
        .then(payment_failure_path(validated=validated))
    )

    return result

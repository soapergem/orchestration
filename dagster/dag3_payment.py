"""
DAG 3 -- Payment Processing (Dagster)

Workflow:
  1. validate_payment      -- DB checks (account exists, balance, idempotency)
  2. Branching logic        -- graph-level conditional: valid -> process, invalid -> failure
  3. process_payment        -- Call (simulated) payment gateway with RetryPolicy + exponential backoff
  4. update_database        -- Idempotent debit/credit + transaction record
  5. send_notification      -- Best-effort success notification
  6. handle_payment_failure -- Record failure in DB, send failure notification

Uses:
  - RetryPolicy(max_retries=3, delay=5, backoff=Backoff.EXPONENTIAL)
  - @failure_hook for logging / alerting on unexpected op failures
  - Conditional branching within the graph via output checks
"""

import json
import random
from datetime import datetime, timezone

from dagster import (
    Backoff,
    DagsterRunStatus,
    Failure,
    HookContext,
    In,
    Nothing,
    Out,
    Output,
    RetryPolicy,
    failure_hook,
    graph,
    job,
    op,
)

from .resources import PostgresResource

# ---------------------------------------------------------------------------
# Custom exceptions (match Step Functions typed errors)
# ---------------------------------------------------------------------------


class PaymentGatewayTimeout(Exception):
    pass


class PaymentGateway5xx(Exception):
    pass


class PaymentDeclined(Exception):
    pass


# ---------------------------------------------------------------------------
# Retry policies
# ---------------------------------------------------------------------------

RETRY_STANDARD = RetryPolicy(max_retries=3, delay=5)
RETRY_GATEWAY = RetryPolicy(max_retries=3, delay=5, backoff=Backoff.EXPONENTIAL)

# ---------------------------------------------------------------------------
# Hooks
# ---------------------------------------------------------------------------


@failure_hook
def payment_failure_alert(context: HookContext):
    """Log a structured alert when any op in the payment job fails unexpectedly."""
    context.log.error(
        f"PAYMENT FAILURE ALERT: op '{context.op.name}' failed in run "
        f"{context.run_id}.  Manual review may be required."
    )


# ---------------------------------------------------------------------------
# Ops
# ---------------------------------------------------------------------------


@op(
    description="Validate a payment request against DB (account checks, balance, idempotency).",
    retry_policy=RETRY_STANDARD,
    out={
        "valid_payment": Out(dict, is_required=False),
        "invalid_payment": Out(dict, is_required=False),
    },
    config_schema={
        "payment_id": str,
        "amount": float,
        "currency": str,
        "from_account": str,
        "to_account": str,
        "idempotency_key": str,
    },
)
def validate_payment(context, postgres: PostgresResource):
    """Check source/dest accounts, balance, and duplicate payment.

    Yields on exactly one of two outputs so the graph can branch.
    """
    cfg = context.op_config
    payment_id = cfg["payment_id"]
    amount = cfg["amount"]
    currency = cfg["currency"]
    from_account = cfg["from_account"]
    to_account = cfg["to_account"]
    idempotency_key = cfg.get("idempotency_key", payment_id)

    payment_data = {
        "payment_id": payment_id,
        "amount": amount,
        "currency": currency,
        "from_account": from_account,
        "to_account": to_account,
        "idempotency_key": idempotency_key,
    }

    with postgres.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT balance, status FROM accounts WHERE account_id = %s",
                (from_account,),
            )
            row = cur.fetchone()
            if row is None:
                reason = f"Source account {from_account} not found"
                context.log.warning(f"Validation failed: {reason}")
                yield Output(
                    {**payment_data, "validation": {"is_valid": False, "reason": reason}},
                    output_name="invalid_payment",
                )
                return

            balance, status = row
            if status != "active":
                reason = f"Source account {from_account} is {status}"
                context.log.warning(f"Validation failed: {reason}")
                yield Output(
                    {**payment_data, "validation": {"is_valid": False, "reason": reason}},
                    output_name="invalid_payment",
                )
                return

            if balance < amount:
                reason = f"Insufficient balance: {balance} < {amount}"
                context.log.warning(f"Validation failed: {reason}")
                yield Output(
                    {**payment_data, "validation": {"is_valid": False, "reason": reason}},
                    output_name="invalid_payment",
                )
                return

            cur.execute(
                "SELECT status FROM accounts WHERE account_id = %s",
                (to_account,),
            )
            row = cur.fetchone()
            if row is None:
                reason = f"Destination account {to_account} not found"
                context.log.warning(f"Validation failed: {reason}")
                yield Output(
                    {**payment_data, "validation": {"is_valid": False, "reason": reason}},
                    output_name="invalid_payment",
                )
                return

            if row[0] != "active":
                reason = f"Destination account {to_account} is {row[0]}"
                context.log.warning(f"Validation failed: {reason}")
                yield Output(
                    {**payment_data, "validation": {"is_valid": False, "reason": reason}},
                    output_name="invalid_payment",
                )
                return

            cur.execute(
                "SELECT status FROM transactions WHERE idempotency_key = %s",
                (idempotency_key,),
            )
            existing = cur.fetchone()
            if existing is not None:
                reason = f"Duplicate payment: existing transaction with status {existing[0]}"
                context.log.warning(f"Validation failed: {reason}")
                yield Output(
                    {**payment_data, "validation": {"is_valid": False, "reason": reason}},
                    output_name="invalid_payment",
                )
                return

    context.log.info(f"Payment {payment_id} passed validation")
    yield Output(
        {**payment_data, "validation": {"is_valid": True, "reason": None}},
        output_name="valid_payment",
    )


@op(
    description="Call the (simulated) payment gateway.  Uses exponential backoff retry.",
    retry_policy=RETRY_GATEWAY,
    out=Out(dict),
)
def process_payment(context, validated: dict) -> dict:
    """Simulated flaky payment gateway (matches Step Functions lambda logic).

    - 60 % success
    - 20 % timeout (retriable via RetryPolicy)
    - 15 % 5xx (retriable)
    - 5 % declined (raises Failure so Dagster does NOT retry)
    """
    payment_id = validated["payment_id"]
    amount = validated["amount"]
    currency = validated["currency"]
    idempotency_key = validated.get("idempotency_key", payment_id)

    roll = random.random()

    if roll < 0.05:
        raise Failure(
            description=f"Payment {payment_id} declined by issuing bank",
            metadata={
                "payment_id": payment_id,
                "decline_code": "insufficient_funds",
            },
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

    context.log.info(
        f"Payment {payment_id} processed: gateway_txn={gateway_transaction_id}"
    )
    return {
        **validated,
        "payment_result": {
            "status": "success",
            "gateway_transaction_id": gateway_transaction_id,
            "amount_charged": amount,
            "currency": currency,
        },
    }


@op(
    description="Record the successful payment in DB (debit/credit, transaction row).  Idempotent.",
    retry_policy=RETRY_STANDARD,
    out=Out(dict),
)
def update_database(context, payment_data: dict, postgres: PostgresResource) -> dict:
    """Debit source, credit dest, insert transaction record.  Skip if already applied."""
    payment_id = payment_data["payment_id"]
    amount = payment_data["amount"]
    from_account = payment_data["from_account"]
    to_account = payment_data["to_account"]
    idempotency_key = payment_data.get("idempotency_key", payment_id)
    payment_result = payment_data["payment_result"]

    with postgres.get_connection() as conn:
        with conn.cursor() as cur:
            now = datetime.now(timezone.utc).isoformat()

            cur.execute(
                "SELECT id FROM transactions WHERE idempotency_key = %s",
                (idempotency_key,),
            )
            if cur.fetchone() is not None:
                context.log.info(
                    f"Transaction {idempotency_key} already recorded -- idempotent skip"
                )
                return {
                    **payment_data,
                    "db_update": {"status": "skipped", "reason": "already recorded"},
                }

            cur.execute(
                "UPDATE accounts SET balance = balance - %s, updated_at = %s "
                "WHERE account_id = %s",
                (amount, now, from_account),
            )
            cur.execute(
                "UPDATE accounts SET balance = balance + %s, updated_at = %s "
                "WHERE account_id = %s",
                (amount, now, to_account),
            )
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
                    payment_data["currency"],
                    "completed",
                    payment_result["gateway_transaction_id"],
                    now,
                ),
            )
        conn.commit()

    context.log.info(f"Database updated for payment {payment_id}")
    return {**payment_data, "db_update": {"status": "success", "recorded_at": now}}


@op(
    description="Send a success notification (best-effort -- failures do not fail the job).",
    retry_policy=RETRY_STANDARD,
    out=Out(dict),
)
def send_notification(context, payment_data: dict) -> dict:
    """Simulated notification.  In production this calls SES/SNS/webhook."""
    payment_id = payment_data["payment_id"]
    amount = payment_data.get("amount")
    currency = payment_data.get("currency")
    gateway_txn = (
        payment_data.get("payment_result", {}).get("gateway_transaction_id", "N/A")
    )

    subject = f"Payment Successful: {payment_id}"
    body = (
        f"Payment {payment_id} for {amount} {currency} was processed successfully.\n"
        f"Gateway Transaction ID: {gateway_txn}"
    )

    context.log.info(f"NOTIFICATION: {subject}")
    context.log.info(f"BODY: {body}")

    return {
        "payment_id": payment_id,
        "notification": {"status": "sent", "subject": subject, "channel": "simulated"},
    }


@op(
    description="Record a payment failure in the DB and prepare failure notification data.",
    retry_policy=RETRY_STANDARD,
    out=Out(dict),
)
def handle_payment_failure(
    context, invalid_data: dict, postgres: PostgresResource
) -> dict:
    """Write a failed transaction row (idempotent) and return data for the failure notification."""
    payment_id = invalid_data["payment_id"]
    idempotency_key = invalid_data.get("idempotency_key", payment_id)
    error_info = invalid_data.get("validation", {})
    error_message = error_info.get("reason", "Unknown error")

    with postgres.get_connection() as conn:
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
                        invalid_data.get("from_account"),
                        invalid_data.get("to_account"),
                        invalid_data.get("amount"),
                        invalid_data.get("currency"),
                        "failed",
                        error_message,
                        now,
                    ),
                )
            conn.commit()

    context.log.info(f"Payment {payment_id} failure recorded: {error_message}")
    return {
        "payment_id": payment_id,
        "amount": invalid_data.get("amount"),
        "currency": invalid_data.get("currency"),
        "status": "failed",
        "failure_message": error_message,
    }


@op(
    description="Send a failure notification (best-effort).",
    retry_policy=RETRY_STANDARD,
    out=Out(dict),
)
def send_failure_notification(context, failure_data: dict) -> dict:
    """Simulated failure notification."""
    payment_id = failure_data["payment_id"]
    message = failure_data.get("failure_message", "Payment processing failed")
    amount = failure_data.get("amount")
    currency = failure_data.get("currency")

    subject = f"Payment Failed: {payment_id}"
    body = f"Payment {payment_id} for {amount} {currency} has failed.\nReason: {message}"

    context.log.info(f"NOTIFICATION: {subject}")
    context.log.info(f"BODY: {body}")

    return {
        "payment_id": payment_id,
        "notification": {"status": "sent", "subject": subject, "channel": "simulated"},
    }


# ---------------------------------------------------------------------------
# Graph / Job
# ---------------------------------------------------------------------------


@graph
def payment_processing_graph():
    valid, invalid = validate_payment()

    # Happy path: process -> update DB -> notify
    payment_result = process_payment(valid)
    db_result = update_database(payment_result)
    send_notification(db_result)

    # Failure path: record failure -> send failure notification
    failure_result = handle_payment_failure(invalid)
    send_failure_notification(failure_result)


payment_processing_job = payment_processing_graph.to_job(
    name="payment_processing_job",
    description=(
        "Payment Processing: validate, process with retries, update DB, "
        "send notification.  Branches to failure path on validation failure."
    ),
    resource_defs={
        "postgres": PostgresResource(
            host="postgres",
            port=5432,
            database="orchestration",
            user="orchestration",
            password="orchestration",
        ),
    },
    hooks={payment_failure_alert},
)

"""
DAG 3: Payment Processing -- Temporal Workflow

1. ValidatePayment activity
2. Branch (if/else on validation result)
3. ProcessPayment activity -- RetryPolicy with non_retryable_error_types for PaymentDeclined
4. UpdateDatabase activity -- idempotent
5. SendNotification activity -- swallow failures (graceful degradation)
6. HandlePaymentFailure activity -- in except block
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from activities import (
        HandlePaymentFailureInput,
        HandlePaymentFailureOutput,
        PaymentDeclined,
        ProcessPaymentInput,
        ProcessPaymentOutput,
        SendNotificationInput,
        SendNotificationOutput,
        UpdateDatabaseInput,
        UpdateDatabaseOutput,
        ValidatePaymentInput,
        ValidatePaymentOutput,
        handle_payment_failure,
        process_payment,
        send_payment_notification,
        update_payment_database,
        validate_payment,
    )


# Retry policies
VALIDATION_RETRY = RetryPolicy(
    maximum_attempts=3,
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
)

# ProcessPayment: retry on gateway timeouts/5xx, but NOT on PaymentDeclined
PAYMENT_RETRY = RetryPolicy(
    maximum_attempts=5,
    initial_interval=timedelta(seconds=3),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    non_retryable_error_types=["PaymentDeclined"],
)

DB_RETRY = RetryPolicy(
    maximum_attempts=3,
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
)

NOTIFICATION_RETRY = RetryPolicy(
    maximum_attempts=3,
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
)


@dataclass
class PaymentInput:
    """Input for the payment processing workflow."""
    payment_id: str
    amount: float
    currency: str
    from_account: str
    to_account: str
    idempotency_key: str | None = None


@dataclass
class PaymentOutput:
    """Final output of the payment workflow."""
    payment_id: str
    status: str  # "success" or "failed"
    gateway_transaction_id: str | None = None
    failure_message: str | None = None
    notification_sent: bool = False


@workflow.defn
class PaymentWorkflow:
    """
    Payment Processing workflow.

    Validates, processes, records, and notifies.  On any processing failure
    the workflow branches to the failure-handling path, which records the
    failure and sends a failure notification before raising.
    """

    @workflow.run
    async def run(self, input: PaymentInput) -> PaymentOutput:
        idempotency_key = input.idempotency_key or input.payment_id
        workflow.logger.info(
            "Starting payment workflow: %s for %.2f %s",
            input.payment_id,
            input.amount,
            input.currency,
        )

        # ---- Step 1: Validate the payment ---------------------------------
        validation: ValidatePaymentOutput = await workflow.execute_activity(
            validate_payment,
            ValidatePaymentInput(
                payment_id=input.payment_id,
                amount=input.amount,
                currency=input.currency,
                from_account=input.from_account,
                to_account=input.to_account,
                idempotency_key=idempotency_key,
            ),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=VALIDATION_RETRY,
        )

        # ---- Step 2: Branch on validation result --------------------------
        if not validation.validation.is_valid:
            workflow.logger.warn(
                "Payment %s failed validation: %s",
                input.payment_id,
                validation.validation.reason,
            )
            # Jump to failure handling
            return await self._handle_failure(
                payment_id=input.payment_id,
                amount=input.amount,
                currency=input.currency,
                from_account=input.from_account,
                to_account=input.to_account,
                idempotency_key=idempotency_key,
                error_message=validation.validation.reason or "Validation failed",
            )

        # ---- Step 3: Process the payment ----------------------------------
        try:
            payment_result: ProcessPaymentOutput = await workflow.execute_activity(
                process_payment,
                ProcessPaymentInput(
                    payment_id=input.payment_id,
                    amount=input.amount,
                    currency=input.currency,
                    from_account=input.from_account,
                    to_account=input.to_account,
                    idempotency_key=idempotency_key,
                ),
                start_to_close_timeout=timedelta(seconds=60),
                retry_policy=PAYMENT_RETRY,
            )
        except Exception as exc:
            workflow.logger.error(
                "Payment %s processing failed: %s", input.payment_id, exc
            )
            return await self._handle_failure(
                payment_id=input.payment_id,
                amount=input.amount,
                currency=input.currency,
                from_account=input.from_account,
                to_account=input.to_account,
                idempotency_key=idempotency_key,
                error_message=str(exc),
            )

        gateway_txn_id = payment_result.payment_result.gateway_transaction_id

        # ---- Step 4: Update database (idempotent) -------------------------
        try:
            db_result: UpdateDatabaseOutput = await workflow.execute_activity(
                update_payment_database,
                UpdateDatabaseInput(
                    payment_id=input.payment_id,
                    amount=input.amount,
                    currency=input.currency,
                    from_account=input.from_account,
                    to_account=input.to_account,
                    idempotency_key=idempotency_key,
                    gateway_transaction_id=gateway_txn_id,
                ),
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=DB_RETRY,
            )
        except Exception as exc:
            workflow.logger.error(
                "Payment %s DB update failed: %s", input.payment_id, exc
            )
            return await self._handle_failure(
                payment_id=input.payment_id,
                amount=input.amount,
                currency=input.currency,
                from_account=input.from_account,
                to_account=input.to_account,
                idempotency_key=idempotency_key,
                error_message=f"Database update failed: {exc}",
            )

        # ---- Step 5: Send success notification (swallow failures) ---------
        notification_sent = False
        try:
            notif_result: SendNotificationOutput = await workflow.execute_activity(
                send_payment_notification,
                SendNotificationInput(
                    payment_id=input.payment_id,
                    status="success",
                    amount=input.amount,
                    currency=input.currency,
                    gateway_transaction_id=gateway_txn_id,
                ),
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=NOTIFICATION_RETRY,
            )
            notification_sent = True
        except Exception as exc:
            # Graceful degradation: payment succeeded, notification is best-effort
            workflow.logger.warn(
                "Payment %s notification failed (non-fatal): %s",
                input.payment_id,
                exc,
            )

        workflow.logger.info(
            "Payment %s completed successfully (gateway_txn=%s)",
            input.payment_id,
            gateway_txn_id,
        )

        return PaymentOutput(
            payment_id=input.payment_id,
            status="success",
            gateway_transaction_id=gateway_txn_id,
            notification_sent=notification_sent,
        )

    # -- Failure-handling helper -------------------------------------------

    async def _handle_failure(
        self,
        *,
        payment_id: str,
        amount: float,
        currency: str,
        from_account: str,
        to_account: str,
        idempotency_key: str,
        error_message: str,
    ) -> PaymentOutput:
        """Record the failure, send failure notification, then raise."""

        # Step 6a: Record failure in DB
        failure_result: HandlePaymentFailureOutput = await workflow.execute_activity(
            handle_payment_failure,
            HandlePaymentFailureInput(
                payment_id=payment_id,
                amount=amount,
                currency=currency,
                from_account=from_account,
                to_account=to_account,
                idempotency_key=idempotency_key,
                error_message=error_message,
            ),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=DB_RETRY,
        )

        # Step 6b: Send failure notification (best-effort)
        try:
            await workflow.execute_activity(
                send_payment_notification,
                SendNotificationInput(
                    payment_id=payment_id,
                    status="failed",
                    amount=amount,
                    currency=currency,
                    message=error_message,
                ),
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=NOTIFICATION_RETRY,
            )
        except Exception as notif_exc:
            workflow.logger.warn(
                "Payment %s failure notification failed (non-fatal): %s",
                payment_id,
                notif_exc,
            )

        # Raise so the workflow completes as failed
        raise workflow.ApplicationError(
            f"Payment {payment_id} failed: {error_message}",
            type="PaymentProcessingFailed",
        )

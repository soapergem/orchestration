"""
DAG 3: Payment Processing — Luigi Implementation

Pipeline: ValidatePayment -> ProcessPayment (with retry) -> UpdateDatabase -> SendNotification

Mirrors the Step Functions implementation in step-functions/dag3-payment/.

MAJOR DIVERGENCES FROM STEP FUNCTIONS:
- Luigi provides no framework-level error handling, retry policies, or compensation
  mechanisms. Step Functions has declarative Retry (with IntervalSeconds, MaxAttempts,
  BackoffRate, JitterStrategy, MaxDelaySeconds) and Catch (routing errors to specific
  states). In Luigi, all of this must be hand-coded in Python within run() methods.
- Step Functions has a dedicated HandlePaymentFailure state that routes ALL errors
  through a common failure handler, then sends a failure notification. Luigi has no
  failure callbacks — if a task raises an exception, it fails and dependents never run.
  There is no way to trigger compensating actions on failure without wrapping the
  entire pipeline in external error handling.
- Step Functions supports structured error metadata (Error, Cause) propagated through
  the state machine. Luigi errors are Python exceptions in logs with no structured routing.
- Step Functions' NotificationFailed state shows graceful degradation: the payment
  succeeded even if notification fails. In Luigi, this requires try/except in the
  notification task.

Run with:
    luigi --module dag3_payment SendNotification \
        --payment-id PAY-001 \
        --amount 150.00 \
        --currency USD \
        --from-account ACC-001 \
        --to-account ACC-002 \
        --run-id my-run-001
"""

import json
import os
import random
import time
from datetime import datetime, timezone

import luigi
import psycopg2


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

MARKER_DIR = os.environ.get("LUIGI_MARKER_DIR", "/tmp/luigi-markers/dag3")


def get_db_connection():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        database=DB_CONFIG["database"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )


# ---------------------------------------------------------------------------
# Custom exceptions (mirrors step-functions/dag3-payment/lambdas/process_payment.py)
# ---------------------------------------------------------------------------


class PaymentGatewayTimeout(Exception):
    pass


class PaymentGateway5xx(Exception):
    pass


class PaymentDeclined(Exception):
    pass


# ---------------------------------------------------------------------------
# Task 1: ValidatePayment
# ---------------------------------------------------------------------------


class ValidatePayment(luigi.Task):
    """
    Validates a payment request: checks accounts exist, sufficient balance,
    duplicate detection.

    Mirrors step-functions/dag3-payment/lambdas/validate_payment.py.
    """

    payment_id = luigi.Parameter()
    amount = luigi.FloatParameter()
    currency = luigi.Parameter()
    from_account = luigi.Parameter()
    to_account = luigi.Parameter()
    run_id = luigi.Parameter()
    idempotency_key = luigi.Parameter(default="")

    def output(self):
        return luigi.LocalTarget(
            os.path.join(MARKER_DIR, self.run_id, "validate_payment.json")
        )

    def run(self):
        idem_key = self.idempotency_key or self.payment_id

        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # Check source account exists and has sufficient balance
                cur.execute(
                    "SELECT balance, status FROM accounts WHERE account_id = %s",
                    (self.from_account,),
                )
                row = cur.fetchone()

                if row is None:
                    result = {
                        "validation": {
                            "is_valid": False,
                            "reason": f"Source account {self.from_account} not found",
                        }
                    }
                elif row[1] != "active":
                    result = {
                        "validation": {
                            "is_valid": False,
                            "reason": f"Source account {self.from_account} is {row[1]}",
                        }
                    }
                elif row[0] < self.amount:
                    result = {
                        "validation": {
                            "is_valid": False,
                            "reason": f"Insufficient balance: {row[0]} < {self.amount}",
                        }
                    }
                else:
                    # Check destination account
                    cur.execute(
                        "SELECT status FROM accounts WHERE account_id = %s",
                        (self.to_account,),
                    )
                    dest_row = cur.fetchone()

                    if dest_row is None:
                        result = {
                            "validation": {
                                "is_valid": False,
                                "reason": f"Destination account {self.to_account} not found",
                            }
                        }
                    elif dest_row[0] != "active":
                        result = {
                            "validation": {
                                "is_valid": False,
                                "reason": f"Destination account {self.to_account} is {dest_row[0]}",
                            }
                        }
                    else:
                        # Check for duplicate payment
                        cur.execute(
                            "SELECT status FROM transactions WHERE idempotency_key = %s",
                            (idem_key,),
                        )
                        existing = cur.fetchone()

                        if existing is not None:
                            result = {
                                "validation": {
                                    "is_valid": False,
                                    "reason": f"Duplicate payment: existing transaction with status {existing[0]}",
                                }
                            }
                        else:
                            result = {"validation": {"is_valid": True, "reason": None}}
        finally:
            conn.close()

        # Include payment metadata for downstream tasks
        result["payment_id"] = self.payment_id
        result["amount"] = self.amount
        result["currency"] = self.currency
        result["from_account"] = self.from_account
        result["to_account"] = self.to_account
        result["idempotency_key"] = idem_key

        os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
        with self.output().open("w") as f:
            json.dump(result, f)


# ---------------------------------------------------------------------------
# Task 2: ProcessPayment (with manual retry)
# ---------------------------------------------------------------------------


class ProcessPayment(luigi.Task):
    """
    Calls a simulated external payment gateway API.

    DIVERGENCE: Step Functions provides declarative retry:
        PaymentGatewayTimeout/5xx: IntervalSeconds=3, MaxAttempts=5,
            BackoffRate=2.0, MaxDelaySeconds=30, JitterStrategy=FULL
        TooManyRequests: IntervalSeconds=1, MaxAttempts=6, BackoffRate=2.0
        Lambda errors: IntervalSeconds=2, MaxAttempts=6, BackoffRate=2.0

    Luigi has no built-in retry with backoff. We implement a simple retry loop
    with exponential backoff in run(). This is less sophisticated than Step
    Functions' jitter strategy and error-type-specific retry policies.

    Step Functions also catches PaymentDeclined separately and routes it to
    HandlePaymentFailure. In Luigi, a PaymentDeclined exception simply fails
    the task with no automated failure handling.
    """

    payment_id = luigi.Parameter()
    amount = luigi.FloatParameter()
    currency = luigi.Parameter()
    from_account = luigi.Parameter()
    to_account = luigi.Parameter()
    run_id = luigi.Parameter()
    idempotency_key = luigi.Parameter(default="")
    max_retries = luigi.IntParameter(default=5)

    def requires(self):
        return ValidatePayment(
            payment_id=self.payment_id,
            amount=self.amount,
            currency=self.currency,
            from_account=self.from_account,
            to_account=self.to_account,
            run_id=self.run_id,
            idempotency_key=self.idempotency_key,
        )

    def output(self):
        return luigi.LocalTarget(
            os.path.join(MARKER_DIR, self.run_id, "process_payment.json")
        )

    def run(self):
        # Read validation result
        with self.input().open("r") as f:
            validation_data = json.load(f)

        # Check validation passed — mirrors CheckValidation Choice state
        if not validation_data["validation"]["is_valid"]:
            # DIVERGENCE: Step Functions routes this to HandlePaymentFailure via
            # a Choice state -> ValidationFailed -> HandlePaymentFailure chain.
            # Luigi has no error routing — we record the failure and raise.
            self._record_failure(
                validation_data["payment_id"],
                validation_data.get("idempotency_key", validation_data["payment_id"]),
                f"Validation failed: {validation_data['validation']['reason']}",
            )
            raise Exception(
                f"Payment validation failed: {validation_data['validation']['reason']}"
            )

        idem_key = validation_data.get("idempotency_key", self.payment_id)

        # --- Retry loop with exponential backoff ---
        # DIVERGENCE: This is a poor approximation of Step Functions' retry.
        # Step Functions retries happen at the orchestrator level (the Lambda
        # is re-invoked fresh). Here, we retry within a single task execution,
        # which means transient process-level issues won't be recovered.
        last_exception = None
        for attempt in range(self.max_retries):
            try:
                result = self._call_payment_gateway(validation_data, idem_key)

                os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
                with self.output().open("w") as f:
                    json.dump(result, f)
                return

            except PaymentDeclined as e:
                # Not retriable — record failure and raise immediately
                self._record_failure(self.payment_id, idem_key, str(e))
                raise

            except (PaymentGatewayTimeout, PaymentGateway5xx) as e:
                last_exception = e
                if attempt < self.max_retries - 1:
                    # Exponential backoff: 3s, 6s, 12s, 24s (capped at 30s)
                    delay = min(3 * (2 ** attempt), 30)
                    # Add jitter (approximation of FULL jitter strategy)
                    delay = random.uniform(0, delay)
                    time.sleep(delay)

        # All retries exhausted
        self._record_failure(self.payment_id, idem_key, str(last_exception))
        raise last_exception

    def _call_payment_gateway(self, validation_data, idem_key):
        """
        Simulated payment gateway call.
        Mirrors step-functions/dag3-payment/lambdas/process_payment.py.
        """
        roll = random.random()

        if roll < 0.05:
            raise PaymentDeclined(
                json.dumps(
                    {
                        "payment_id": self.payment_id,
                        "reason": "Card declined by issuing bank",
                        "decline_code": "insufficient_funds",
                    }
                )
            )
        elif roll < 0.20:
            raise PaymentGateway5xx(
                f"Payment gateway returned 500 for payment {self.payment_id}"
            )
        elif roll < 0.40:
            raise PaymentGatewayTimeout(
                f"Payment gateway timed out for payment {self.payment_id}"
            )

        gateway_transaction_id = (
            f"gw-txn-{self.payment_id}-{random.randint(10000, 99999)}"
        )

        return {
            **validation_data,
            "payment_result": {
                "status": "success",
                "gateway_transaction_id": gateway_transaction_id,
                "amount_charged": self.amount,
                "currency": self.currency,
            },
        }

    def _record_failure(self, payment_id, idem_key, error_message):
        """
        Records a payment failure in the database.

        DIVERGENCE: In Step Functions, HandlePaymentFailure is a dedicated
        state that runs automatically when any upstream state fails via Catch.
        In Luigi, we must call this manually in the except block. This is
        fragile — if the developer forgets the call, failures go unrecorded.
        """
        try:
            conn = get_db_connection()
            try:
                with conn.cursor() as cur:
                    now = datetime.now(timezone.utc).isoformat()
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
                                payment_id,
                                idem_key,
                                self.from_account,
                                self.to_account,
                                self.amount,
                                self.currency,
                                "failed",
                                error_message[:1000],
                                now,
                            ),
                        )
                conn.commit()
            finally:
                conn.close()
        except Exception:
            # Best-effort failure recording — don't mask the original error
            pass


# ---------------------------------------------------------------------------
# Task 3: UpdateDatabase
# ---------------------------------------------------------------------------


class UpdateDatabase(luigi.Task):
    """
    Records the payment result in the database: debits/credits accounts,
    writes transaction record.

    Mirrors step-functions/dag3-payment/lambdas/update_database.py.
    """

    payment_id = luigi.Parameter()
    amount = luigi.FloatParameter()
    currency = luigi.Parameter()
    from_account = luigi.Parameter()
    to_account = luigi.Parameter()
    run_id = luigi.Parameter()
    idempotency_key = luigi.Parameter(default="")

    def requires(self):
        return ProcessPayment(
            payment_id=self.payment_id,
            amount=self.amount,
            currency=self.currency,
            from_account=self.from_account,
            to_account=self.to_account,
            run_id=self.run_id,
            idempotency_key=self.idempotency_key,
        )

    def output(self):
        return luigi.LocalTarget(
            os.path.join(MARKER_DIR, self.run_id, "update_database.json")
        )

    def run(self):
        with self.input().open("r") as f:
            payment_data = json.load(f)

        payment_result = payment_data["payment_result"]
        idem_key = payment_data.get("idempotency_key", self.payment_id)

        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                now = datetime.now(timezone.utc).isoformat()

                # Check idempotency
                cur.execute(
                    "SELECT id FROM transactions WHERE idempotency_key = %s",
                    (idem_key,),
                )
                if cur.fetchone() is not None:
                    result = {
                        **payment_data,
                        "db_update": {
                            "status": "skipped",
                            "reason": "Transaction already recorded (idempotent)",
                        },
                    }
                else:
                    # Debit source account
                    cur.execute(
                        "UPDATE accounts SET balance = balance - %s, updated_at = %s "
                        "WHERE account_id = %s",
                        (self.amount, now, self.from_account),
                    )

                    # Credit destination account
                    cur.execute(
                        "UPDATE accounts SET balance = balance + %s, updated_at = %s "
                        "WHERE account_id = %s",
                        (self.amount, now, self.to_account),
                    )

                    # Record transaction
                    cur.execute(
                        """INSERT INTO transactions
                           (payment_id, idempotency_key, from_account, to_account,
                            amount, currency, status, gateway_transaction_id, created_at)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        (
                            self.payment_id,
                            idem_key,
                            self.from_account,
                            self.to_account,
                            self.amount,
                            self.currency,
                            "completed",
                            payment_result["gateway_transaction_id"],
                            now,
                        ),
                    )

                    result = {
                        **payment_data,
                        "db_update": {"status": "success", "recorded_at": now},
                    }

            conn.commit()
        finally:
            conn.close()

        os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
        with self.output().open("w") as f:
            json.dump(result, f)


# ---------------------------------------------------------------------------
# Task 4: SendNotification
# ---------------------------------------------------------------------------


class SendNotification(luigi.Task):
    """
    Sends a payment notification (success or failure) via simulated email/webhook.

    DIVERGENCE: In Step Functions, if SendNotification fails, it routes to
    NotificationFailed (a Pass state) — the payment is still considered
    successful. This graceful degradation is a first-class feature of the
    state machine's Catch mechanism.

    Luigi has no Catch mechanism. We wrap the notification in try/except to
    achieve the same graceful degradation, but this is ad-hoc and the
    "notification failed but payment succeeded" outcome is not visible
    in Luigi's task status (the task appears successful either way).
    """

    payment_id = luigi.Parameter()
    amount = luigi.FloatParameter()
    currency = luigi.Parameter()
    from_account = luigi.Parameter()
    to_account = luigi.Parameter()
    run_id = luigi.Parameter()
    idempotency_key = luigi.Parameter(default="")

    def requires(self):
        return UpdateDatabase(
            payment_id=self.payment_id,
            amount=self.amount,
            currency=self.currency,
            from_account=self.from_account,
            to_account=self.to_account,
            run_id=self.run_id,
            idempotency_key=self.idempotency_key,
        )

    def output(self):
        return luigi.LocalTarget(
            os.path.join(MARKER_DIR, self.run_id, "send_notification.json")
        )

    def run(self):
        with self.input().open("r") as f:
            payment_data = json.load(f)

        notification_result = {}

        try:
            # Mirrors step-functions/dag3-payment/lambdas/send_notification.py
            payment_result = payment_data.get("payment_result", {})
            gateway_txn = payment_result.get("gateway_transaction_id", "N/A")

            subject = f"Payment Successful: {self.payment_id}"
            body = (
                f"Payment {self.payment_id} for {self.amount} {self.currency} "
                f"was processed successfully.\n"
                f"Gateway Transaction ID: {gateway_txn}"
            )

            # In production, this would call SES, SNS, or a webhook.
            print(f"NOTIFICATION: {subject}")
            print(f"BODY: {body}")

            notification_result = {
                "payment_id": self.payment_id,
                "notification": {
                    "status": "sent",
                    "subject": subject,
                    "channel": "simulated",
                },
            }

        except Exception as e:
            # DIVERGENCE: Graceful degradation via try/except.
            # Step Functions handles this with Catch -> NotificationFailed Pass state.
            # The payment succeeded; we just couldn't send the notification.
            print(
                f"WARNING: Notification failed for payment {self.payment_id}: {e}. "
                f"Payment was still processed successfully."
            )
            notification_result = {
                "payment_id": self.payment_id,
                "notification": {
                    "status": "failed",
                    "error": str(e),
                    "note": "Payment succeeded but notification failed.",
                },
            }

        os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
        with self.output().open("w") as f:
            json.dump(notification_result, f)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    luigi.run()

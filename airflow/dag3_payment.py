"""
DAG 3: Payment Processing (Classic Operator Style with set_upstream/set_downstream)

Validates a payment request against the database, processes it through a
simulated (flaky) payment gateway with typed exceptions, updates debit/credit
records idempotently, and sends best-effort notifications.  The failure path
records the failed transaction and sends a failure notification.

Airflow idioms used:
- Classic operator instantiation (PythonOperator, BranchPythonOperator)
- set_upstream() / set_downstream() for dependency wiring
- Typed exception handling with retries and exponential backoff
- on_failure_callback for error recording
- trigger_rule for best-effort notification and joining branches
"""

from __future__ import annotations

import json
import os
import random
from datetime import datetime, timedelta, timezone

import psycopg2
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DB_CONN_PARAMS = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": int(os.environ.get("POSTGRES_PORT", "5432")),
    "dbname": os.environ.get("POSTGRES_DB", "orchestration"),
    "user": os.environ.get("POSTGRES_USER", "orchestration"),
    "password": os.environ.get("POSTGRES_PASSWORD", "orchestration"),
}


# ---------------------------------------------------------------------------
# Typed exceptions
# ---------------------------------------------------------------------------

class PaymentGatewayTimeout(AirflowException):
    """Retriable: the gateway did not respond in time."""


class PaymentGateway5xx(AirflowException):
    """Retriable: the gateway returned a server error."""


class PaymentDeclined(AirflowException):
    """Non-retriable: the payment was actively declined."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(**DB_CONN_PARAMS)


def _on_payment_failure(context: dict) -> None:
    """
    on_failure_callback attached to the process_payment task.
    Records the failed transaction in the database so the failure path
    has a record even if handle_payment_failure never runs.
    """
    ti = context["task_instance"]
    dag_run = context["dag_run"]
    conf = dag_run.conf or {}

    payment_id = conf.get("payment_id", "unknown")
    idempotency_key = conf.get("idempotency_key", payment_id)
    exception = context.get("exception")
    error_message = str(exception) if exception else "Unknown error"

    try:
        conn = _get_connection()
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
                        conf.get("from_account"),
                        conf.get("to_account"),
                        conf.get("amount"),
                        conf.get("currency"),
                        "failed",
                        error_message,
                        now,
                    ),
                )
            conn.commit()
        conn.close()
    except Exception as exc:
        ti.log.error("on_failure_callback could not record failure: %s", exc)


# ---------------------------------------------------------------------------
# Callable functions for PythonOperators
# ---------------------------------------------------------------------------

def validate_payment_fn(**context) -> dict:
    """Check account existence, status, balance, and duplicate payment."""
    conf = context["dag_run"].conf or context["params"]
    payment_id = conf["payment_id"]
    amount = float(conf["amount"])
    from_account = conf["from_account"]
    to_account = conf["to_account"]

    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT balance, status FROM accounts WHERE account_id = %s",
                (from_account,),
            )
            row = cur.fetchone()
            if row is None:
                return {"is_valid": False, "reason": f"Source account {from_account} not found"}

            balance, status = float(row[0]), row[1]
            if status != "active":
                return {"is_valid": False, "reason": f"Source account {from_account} is {status}"}
            if balance < amount:
                return {"is_valid": False, "reason": f"Insufficient balance: {balance} < {amount}"}

            cur.execute(
                "SELECT status FROM accounts WHERE account_id = %s",
                (to_account,),
            )
            row = cur.fetchone()
            if row is None:
                return {"is_valid": False, "reason": f"Destination account {to_account} not found"}
            if row[0] != "active":
                return {"is_valid": False, "reason": f"Destination account {to_account} is {row[0]}"}

            idempotency_key = conf.get("idempotency_key", payment_id)
            cur.execute(
                "SELECT status FROM transactions WHERE idempotency_key = %s",
                (idempotency_key,),
            )
            existing = cur.fetchone()
            if existing is not None:
                return {"is_valid": False, "reason": f"Duplicate payment: existing transaction with status {existing[0]}"}
    finally:
        conn.close()

    return {"is_valid": True, "reason": None}


def check_validation_fn(**context) -> str:
    """Branch based on validation result."""
    ti = context["task_instance"]
    validation = ti.xcom_pull(task_ids="validate_payment")
    if validation and validation.get("is_valid"):
        return "process_payment"
    return "handle_payment_failure"


def process_payment_fn(**context) -> dict:
    """Call simulated payment gateway. Raises typed exceptions."""
    conf = context["dag_run"].conf or context["params"]
    payment_id = conf["payment_id"]
    amount = float(conf["amount"])
    currency = conf["currency"]

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
        raise PaymentGateway5xx(f"Payment gateway returned 500 for payment {payment_id}")
    elif roll < 0.40:
        raise PaymentGatewayTimeout(f"Payment gateway timed out for payment {payment_id}")

    gateway_transaction_id = f"gw-txn-{payment_id}-{random.randint(10000, 99999)}"
    return {
        "status": "success",
        "gateway_transaction_id": gateway_transaction_id,
        "amount_charged": amount,
        "currency": currency,
    }


def update_database_fn(**context) -> dict:
    """Debit source, credit destination, record transaction."""
    conf = context["dag_run"].conf or context["params"]
    ti = context["task_instance"]
    payment_result = ti.xcom_pull(task_ids="process_payment")

    payment_id = conf["payment_id"]
    amount = float(conf["amount"])
    currency = conf["currency"]
    from_account = conf["from_account"]
    to_account = conf["to_account"]
    idempotency_key = conf.get("idempotency_key", payment_id)

    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            now = datetime.now(timezone.utc).isoformat()

            cur.execute(
                "SELECT id FROM transactions WHERE idempotency_key = %s",
                (idempotency_key,),
            )
            if cur.fetchone() is not None:
                conn.rollback()
                return {"status": "skipped", "reason": "Transaction already recorded (idempotent)"}

            cur.execute(
                "UPDATE accounts SET balance = balance - %s, updated_at = %s WHERE account_id = %s",
                (amount, now, from_account),
            )
            cur.execute(
                "UPDATE accounts SET balance = balance + %s, updated_at = %s WHERE account_id = %s",
                (amount, now, to_account),
            )
            cur.execute(
                """INSERT INTO transactions
                   (payment_id, idempotency_key, from_account, to_account,
                    amount, currency, status, gateway_transaction_id, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    payment_id, idempotency_key, from_account, to_account,
                    amount, currency, "completed",
                    payment_result["gateway_transaction_id"], now,
                ),
            )
        conn.commit()
    finally:
        conn.close()

    return {"status": "success", "recorded_at": now}


def send_notification_fn(**context) -> dict:
    """Best-effort success notification."""
    conf = context["dag_run"].conf or context["params"]
    payment_id = conf["payment_id"]
    amount = conf["amount"]
    currency = conf["currency"]

    subject = f"Payment Successful: {payment_id}"
    body = f"Payment {payment_id} for {amount} {currency} was processed successfully."
    print(f"NOTIFICATION: {subject}")
    print(f"BODY: {body}")

    return {
        "payment_id": payment_id,
        "notification": {"status": "sent", "subject": subject, "channel": "simulated"},
    }


def handle_payment_failure_fn(**context) -> dict:
    """Record the failed transaction in the database."""
    conf = context["dag_run"].conf or context["params"]
    payment_id = conf["payment_id"]
    idempotency_key = conf.get("idempotency_key", payment_id)

    ti = context["task_instance"]
    validation = ti.xcom_pull(task_ids="validate_payment")
    error_message = (
        validation.get("reason", "Unknown error") if validation else "Unknown error"
    )

    conn = _get_connection()
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
                        payment_id, idempotency_key,
                        conf.get("from_account"), conf.get("to_account"),
                        conf.get("amount"), conf.get("currency"),
                        "failed", error_message, now,
                    ),
                )
            conn.commit()
    finally:
        conn.close()

    return {"payment_id": payment_id, "status": "failed", "failure_message": error_message}


def send_failure_notification_fn(**context) -> dict:
    """Send a failure notification (best-effort)."""
    ti = context["task_instance"]
    failure_result = ti.xcom_pull(task_ids="handle_payment_failure")
    conf = context["dag_run"].conf or context["params"]
    payment_id = failure_result["payment_id"]

    subject = f"Payment Failed: {payment_id}"
    body = (
        f"Payment {payment_id} for {conf.get('amount')} {conf.get('currency')} has failed.\n"
        f"Reason: {failure_result.get('failure_message', 'Unknown')}"
    )
    print(f"NOTIFICATION: {subject}")
    print(f"BODY: {body}")

    return {
        "payment_id": payment_id,
        "notification": {"status": "sent", "subject": subject, "channel": "simulated"},
    }


# ---------------------------------------------------------------------------
# DAG definition using set_upstream() / set_downstream()
# ---------------------------------------------------------------------------

with DAG(
    dag_id="dag3_payment",
    description="Payment Processing: validate, process with retries, update DB, notify",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "orchestration",
        "retries": 0,
        "retry_delay": timedelta(seconds=5),
    },
    params={
        "payment_id": "PAY-001",
        "amount": 100.00,
        "currency": "USD",
        "from_account": "ACC-001",
        "to_account": "ACC-002",
        "idempotency_key": "PAY-001",
    },
    tags=["payment", "retries", "branching"],
) as dag:

    validate_payment = PythonOperator(
        task_id="validate_payment",
        python_callable=validate_payment_fn,
    )

    check_validation = BranchPythonOperator(
        task_id="check_validation",
        python_callable=check_validation_fn,
    )

    process_payment = PythonOperator(
        task_id="process_payment",
        python_callable=process_payment_fn,
        retries=3,
        retry_delay=timedelta(seconds=3),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(seconds=30),
        on_failure_callback=_on_payment_failure,
    )

    update_database = PythonOperator(
        task_id="update_database",
        python_callable=update_database_fn,
        retries=3,
        retry_delay=timedelta(seconds=2),
        retry_exponential_backoff=True,
    )

    send_notification = PythonOperator(
        task_id="send_notification",
        python_callable=send_notification_fn,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    handle_payment_failure = PythonOperator(
        task_id="handle_payment_failure",
        python_callable=handle_payment_failure_fn,
    )

    send_failure_notification = PythonOperator(
        task_id="send_failure_notification",
        python_callable=send_failure_notification_fn,
    )

    # Wire dependencies using set_upstream() / set_downstream()

    # validate_payment -> check_validation
    check_validation.set_upstream(validate_payment)

    # check_validation branches to either process_payment or handle_payment_failure
    process_payment.set_upstream(check_validation)
    handle_payment_failure.set_upstream(check_validation)

    # Success path: process_payment -> update_database -> send_notification
    update_database.set_upstream(process_payment)
    send_notification.set_upstream(update_database)

    # Failure path: handle_payment_failure -> send_failure_notification
    send_failure_notification.set_upstream(handle_payment_failure)

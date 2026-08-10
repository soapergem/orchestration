"""
DAG 3: Payment Processing (Conductor)

Validate -> flaky gateway call with backoff -> idempotent DB update -> best-effort
notification, with a separate failure path.

Conductor idioms demonstrated:
- `NonRetryableException` (conductor.client.worker.exception): the SDK maps it
  to task status FAILED_WITH_TERMINAL_ERROR, which the engine honours by
  skipping the remaining `retryCount` entirely. This is the retriable /
  non-retriable split the spec asks for, and it is first-class -- contrast
  Kestra, whose `retry:` block has no error-type predicate at all and happily
  retried a non-retriable error twelve times.
- Retry policy as *data*: `retryCount: 4`, `retryLogic: EXPONENTIAL_BACKOFF`,
  `retryDelaySeconds: 2` live in taskdefs.json, not in this file. The gateway's
  own jitter is added in-task because Conductor's backoff has none.
- `optional: true` on the notification task in the workflow JSON: the task may
  fail, is marked COMPLETED_WITH_ERRORS, and the workflow proceeds. That is
  graceful degradation declared rather than coded.
"""

import os
import random
import uuid

import psycopg2
from conductor.client.worker.exception import NonRetryableException
from conductor.client.worker.worker_task import worker_task

DB_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": int(os.environ.get("POSTGRES_PORT", "5432")),
    "dbname": os.environ.get("POSTGRES_DB", "orchestration"),
    "user": os.environ.get("POSTGRES_USER", "orchestration"),
    "password": os.environ.get("POSTGRES_PASSWORD", "orchestration"),
}

BAKEOFF_NS = os.environ.get("BAKEOFF_NS", "conductor")
SCHEMA = f"{BAKEOFF_NS}_dag3"

# Gateway simulation rates, per the spec: 60% success, 20% timeout, 15% 5xx,
# 5% declined. The first three are retriable; declined is not.
#
# Read at CALL time, not import time, so overriding them to force a specific
# branch takes effect without reasoning about when each of the SDK's 26 spawned
# task processes happened to import this module.
def _rates() -> tuple[float, float, float]:
    return (
        float(os.environ.get("GATEWAY_SUCCESS_RATE", "0.60")),
        float(os.environ.get("GATEWAY_TIMEOUT_RATE", "0.20")),
        float(os.environ.get("GATEWAY_SERVER_ERROR_RATE", "0.15")),
    )


def get_db_connection() -> psycopg2.extensions.connection:
    """Connect with search_path pinned to this runner's DAG 3 schema.

    DAG 3 does NOT create its schema: it needs the seeded accounts fixture, so
    a missing schema is a setup error and must fail loudly rather than silently
    operating on an empty database. Run `just seed conductor` first.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name = %s", (SCHEMA,))
        if cur.fetchone() is None:
            conn.close()
            raise RuntimeError(
                f'schema "{SCHEMA}" is missing -- run `just seed {BAKEOFF_NS}` first'
            )
        cur.execute(f'SET search_path TO "{SCHEMA}"')
    return conn


# ---- error types -----------------------------------------------------------


class PaymentDeclined(NonRetryableException):
    """The gateway said no. Retrying cannot change that answer."""


class ValidationFailed(NonRetryableException):
    """The request is malformed or the account is ineligible."""


# ---- tasks -----------------------------------------------------------------


@worker_task(task_definition_name="validate_payment")
def validate_payment(
    payment_id: str,
    from_account: str,
    to_account: str,
    amount: float,
    idempotency_key: str = "",
    currency: str = "USD",
) -> dict:
    """Account existence, active status, sufficient balance, duplicate check.

    Returns a *result* rather than raising on business-rule failure: the
    workflow SWITCHes on `validation_status`, so an invalid payment is a normal
    branch, not an engine-level error.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            if idempotency_key:
                cur.execute(
                    "SELECT payment_id, status FROM transactions WHERE idempotency_key = %s",
                    (idempotency_key,),
                )
                existing = cur.fetchone()
                if existing:
                    return {
                        "validation_status": "duplicate",
                        "reason": f"idempotency_key already used by payment {existing[0]}",
                        "payment_id": payment_id,
                    }

            cur.execute(
                "SELECT account_id, balance, status FROM accounts WHERE account_id = ANY(%s)",
                ([from_account, to_account],),
            )
            accounts = {r[0]: {"balance": float(r[1]), "status": r[2]} for r in cur.fetchall()}

        for acct in (from_account, to_account):
            if acct not in accounts:
                return {
                    "validation_status": "invalid",
                    "reason": f"account {acct} does not exist",
                    "payment_id": payment_id,
                }
            if accounts[acct]["status"] != "active":
                return {
                    "validation_status": "invalid",
                    "reason": f"account {acct} is {accounts[acct]['status']}",
                    "payment_id": payment_id,
                }

        if accounts[from_account]["balance"] < float(amount):
            return {
                "validation_status": "invalid",
                "reason": (
                    f"insufficient balance: {accounts[from_account]['balance']} < {amount}"
                ),
                "payment_id": payment_id,
            }

        # Basic fraud heuristic, per the spec's "basic fraud checks via DB queries".
        if float(amount) > 10000:
            return {
                "validation_status": "invalid",
                "reason": f"amount {amount} exceeds the single-payment fraud threshold",
                "payment_id": payment_id,
            }

        return {
            "validation_status": "valid",
            "payment_id": payment_id,
            "from_account": from_account,
            "to_account": to_account,
            "amount": float(amount),
            "currency": currency,
            "from_balance": accounts[from_account]["balance"],
        }
    finally:
        conn.close()


@worker_task(task_definition_name="process_payment")
def process_payment(
    payment_id: str, amount: float, idempotency_key: str = "", attempt_hint: str = ""
) -> dict:
    """Call the simulated gateway. Flaky by design.

    The two failure classes are what this task exists to demonstrate:
      * timeout / 5xx  -> raise a plain Exception. The engine retries per the
        task definition's EXPONENTIAL_BACKOFF.
      * declined       -> raise PaymentDeclined (a NonRetryableException). The
        SDK reports FAILED_WITH_TERMINAL_ERROR and the engine abandons the
        remaining retries immediately.
    """
    success_rate, timeout_rate, server_error_rate = _rates()
    roll = random.random()

    if roll < success_rate:
        return {
            "gateway_status": "succeeded",
            "gateway_transaction_id": f"GW-{uuid.uuid4().hex[:12].upper()}",
            "payment_id": payment_id,
            "amount": float(amount),
            "idempotency_key": idempotency_key,
        }

    if roll < success_rate + timeout_rate:
        raise TimeoutError(f"payment gateway timed out for {payment_id}")

    if roll < success_rate + timeout_rate + server_error_rate:
        raise RuntimeError(f"payment gateway returned 503 for {payment_id}")

    raise PaymentDeclined(f"payment {payment_id} was declined by the issuer")


@worker_task(task_definition_name="update_payment_database")
def update_payment_database(
    payment_id: str,
    from_account: str,
    to_account: str,
    amount: float,
    gateway_transaction_id: str,
    idempotency_key: str = "",
    currency: str = "USD",
) -> dict:
    """Debit, credit and record -- idempotently, in one transaction.

    Idempotency rides on the UNIQUE constraint over `idempotency_key`: the
    INSERT ... ON CONFLICT DO NOTHING tells us whether this is a replay, and the
    balance updates are skipped entirely if it is. That keeps a retried task
    from double-charging, which is the entire point of the step.
    """
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                key = idempotency_key or payment_id
                cur.execute(
                    """
                    INSERT INTO transactions
                        (payment_id, idempotency_key, from_account, to_account,
                         amount, currency, status, gateway_transaction_id)
                    VALUES (%s, %s, %s, %s, %s, %s, 'completed', %s)
                    ON CONFLICT (idempotency_key) DO NOTHING
                    RETURNING id
                    """,
                    (
                        payment_id,
                        key,
                        from_account,
                        to_account,
                        float(amount),
                        currency,
                        gateway_transaction_id,
                    ),
                )
                inserted = cur.fetchone()
                if inserted is None:
                    return {
                        "db_status": "already_applied",
                        "payment_id": payment_id,
                        "idempotent_skip": True,
                    }

                cur.execute(
                    "UPDATE accounts SET balance = balance - %s, updated_at = NOW() "
                    "WHERE account_id = %s RETURNING balance",
                    (float(amount), from_account),
                )
                from_balance = float(cur.fetchone()[0])
                cur.execute(
                    "UPDATE accounts SET balance = balance + %s, updated_at = NOW() "
                    "WHERE account_id = %s RETURNING balance",
                    (float(amount), to_account),
                )
                to_balance = float(cur.fetchone()[0])

        return {
            "db_status": "applied",
            "payment_id": payment_id,
            "transaction_row_id": inserted[0],
            "from_balance": from_balance,
            "to_balance": to_balance,
            "idempotent_skip": False,
        }
    finally:
        conn.close()


@worker_task(task_definition_name="send_payment_notification")
def send_payment_notification(payment_id: str, amount: float, to_account: str) -> dict:
    """Best-effort receipt. Declared `optional: true` in the workflow JSON, so a
    failure here degrades to COMPLETED_WITH_ERRORS instead of failing the run."""
    # Simulated: a real deployment would POST a webhook or enqueue an email.
    return {
        "notification_status": "sent",
        "channel": "email",
        "payment_id": payment_id,
        "message": f"Receipt: {amount} credited to {to_account}",
    }


@worker_task(task_definition_name="handle_payment_failure")
def handle_payment_failure(
    payment_id: str,
    from_account: str = "",
    to_account: str = "",
    amount: float = 0,
    reason: str = "",
    idempotency_key: str = "",
    currency: str = "USD",
) -> dict:
    """Record the failed transaction and notify. Runs on both the validation
    branch and the gateway-failure branch."""
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO transactions
                        (payment_id, idempotency_key, from_account, to_account,
                         amount, currency, status, error_message)
                    VALUES (%s, %s, %s, %s, %s, %s, 'failed', %s)
                    ON CONFLICT (idempotency_key) DO NOTHING
                    """,
                    (
                        payment_id,
                        f"failed-{idempotency_key or payment_id}",
                        from_account or None,
                        to_account or None,
                        float(amount or 0),
                        currency,
                        reason,
                    ),
                )
    finally:
        conn.close()

    return {
        "failure_recorded": True,
        "payment_id": payment_id,
        "reason": reason,
        "notification_status": "sent",
    }

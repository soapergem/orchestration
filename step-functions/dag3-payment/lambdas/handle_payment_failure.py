"""
Lambda: HandlePaymentFailure
Records a payment failure in the database and prepares failure notification data.
"""

from datetime import datetime, timezone

from db import get_db_connection


def handler(event, context):
    payment_id = event["payment_id"]
    idempotency_key = event.get("idempotency_key", payment_id)
    error_info = event.get("error", {})

    error_message = error_info.get("Cause", error_info.get("reason", "Unknown error"))

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
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

    return {
        "payment_id": payment_id,
        "amount": event.get("amount"),
        "currency": event.get("currency"),
        "status": "failed",
        "failure_message": error_message,
    }

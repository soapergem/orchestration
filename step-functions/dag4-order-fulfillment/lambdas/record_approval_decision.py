"""
Lambda: RecordApprovalDecision
Persists the approval decision to the database after the callback arrives.
"""

from datetime import datetime, timezone

from db import get_db_connection


def handler(event, context):
    decision = event.get("decision", "unknown")
    approver = event.get("approver")
    reason = event.get("reason", "")
    order_id = event.get("order_id")
    approval_request_id = event.get("approval_request_id")

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        now = datetime.now(timezone.utc)

        if approval_request_id:
            cur.execute(
                """
                UPDATE approval_requests
                SET status = %s, approver = %s, reason = %s, decided_at = %s
                WHERE approval_request_id = %s
                """,
                (decision, approver, reason, now, approval_request_id),
            )

        if order_id:
            new_status = "approved" if decision == "approved" else "rejected"
            cur.execute(
                "UPDATE orders SET status = %s, updated_at = %s WHERE order_id = %s",
                (new_status, now, order_id),
            )

        conn.commit()

        return {
            "decision": decision,
            "approver": approver,
            "reason": reason,
            "decided_at": now.isoformat(),
        }
    finally:
        conn.close()

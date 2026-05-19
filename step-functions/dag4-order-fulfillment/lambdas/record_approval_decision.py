"""
Lambda: RecordApprovalDecision
Persists the approval decision to the database after the callback arrives.
"""

import json
from datetime import datetime, timezone

import boto3
import psycopg2


secrets = boto3.client("secretsmanager")


def get_connection(db_config):
    secret = secrets.get_secret_value(SecretId=db_config["secret_arn"])
    creds = json.loads(secret["SecretString"])
    return psycopg2.connect(
        host=creds["host"],
        port=creds.get("port", 5432),
        dbname=creds["dbname"],
        user=creds["username"],
        password=creds["password"],
    )


def handler(event, context):
    decision = event.get("decision", "unknown")
    approver = event.get("approver")
    reason = event.get("reason", "")
    order_id = event.get("order_id")
    approval_request_id = event.get("approval_request_id")
    db_config = event.get("db_config")

    if not db_config:
        return {
            "decision": decision,
            "approver": approver,
            "reason": reason,
            "decided_at": datetime.now(timezone.utc).isoformat(),
        }

    conn = get_connection(db_config)
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

"""
Lambda: RequestApproval
Registers an approval request with the Approval Service, handing it the
sub-workflow's task token as the provider-specific resume handle.

The sub-workflow uses .waitForTaskToken, so it suspends until a decision is
delivered -- the approval service calls SendTaskSuccess/SendTaskFailure with
this token itself (via /decide or /resume); there is no relay Lambda.
"""

import json
import os
import uuid

import urllib3

from db import get_db_connection

http = urllib3.PoolManager()


def handler(event, context):
    order_id = event["order_id"]
    customer_id = event["customer_id"]
    total_amount = event["total_amount"]
    items = event["items"]
    task_token = event["task_token"]

    approval_request_id = f"APR-{uuid.uuid4().hex[:12].upper()}"
    approval_service_url = os.environ.get("APPROVAL_SERVICE_URL", "http://approval-service:8091")

    items_summary = ", ".join(
        f"{item['quantity']}x {item['sku']}" for item in items
    )

    payload = {
        "approval_request_id": approval_request_id,
        "order_id": order_id,
        "total_amount": total_amount,
        "customer_id": customer_id,
        "items_summary": items_summary,
        "provider": "stepfunctions",
        "resume_data": {
            "task_token": task_token,
            "region": os.environ.get("AWS_REGION"),
        },
    }

    response = http.request(
        "POST",
        f"{approval_service_url}/approval-requests",
        body=json.dumps(payload),
        headers={"Content-Type": "application/json"},
        timeout=10.0,
    )

    if response.status != 201:
        raise Exception(
            f"Approval Service returned {response.status}: "
            f"{response.data.decode('utf-8')[:500]}"
        )

    # Record the approval request in the database
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO approval_requests (approval_request_id, order_id, total_amount, status)
            VALUES (%s, %s, %s, 'pending')
            ON CONFLICT (approval_request_id) DO NOTHING
            """,
            (approval_request_id, order_id, total_amount),
        )
        cur.execute(
            "UPDATE orders SET status = 'pending_approval', updated_at = NOW() WHERE order_id = %s",
            (order_id,),
        )
        conn.commit()
    finally:
        conn.close()

    # Lambda exits here. The state machine stays suspended until the approval service
    # calls the callback URL, which triggers the relay to call SendTaskSuccess.
    return {"approval_request_id": approval_request_id, "status": "submitted"}

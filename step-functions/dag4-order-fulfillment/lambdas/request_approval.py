"""
Lambda: RequestApproval
Sends an approval request to the Approval Service and provides a callback URL
that will call SendTaskSuccess/SendTaskFailure with the task token.

The sub-workflow uses .waitForTaskToken, so it suspends until the approval arrives.
"""

import json
import os
import uuid

import boto3
import urllib3

sfn = boto3.client("stepfunctions")
http = urllib3.PoolManager()

APPROVAL_RELAY_URL = os.environ.get("APPROVAL_RELAY_URL")


def handler(event, context):
    order_id = event["order_id"]
    customer_id = event["customer_id"]
    total_amount = event["total_amount"]
    items = event["items"]
    db_config = event["db_config"]
    task_token = event["task_token"]

    approval_request_id = f"APR-{uuid.uuid4().hex[:12].upper()}"
    approval_service_url = os.environ.get("APPROVAL_SERVICE_URL", "http://approval-service:8091")

    callback_url = f"{APPROVAL_RELAY_URL}?task_token={task_token}"

    items_summary = ", ".join(
        f"{item['quantity']}x {item['sku']}" for item in items
    )

    payload = {
        "approval_request_id": approval_request_id,
        "order_id": order_id,
        "total_amount": total_amount,
        "customer_id": customer_id,
        "callback_url": callback_url,
        "items_summary": items_summary,
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
    secret = boto3.client("secretsmanager").get_secret_value(SecretId=db_config["secret_arn"])
    creds = json.loads(secret["SecretString"])

    import psycopg2
    conn = psycopg2.connect(
        host=creds["host"],
        port=creds.get("port", 5432),
        dbname=creds["dbname"],
        user=creds["username"],
        password=creds["password"],
    )
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

"""
Lambda: SubmitAsyncFetch
Sends the fetch request to the Callback Fetch Service and provides a callback URL
that will call SendTaskSuccess with the task token when the fetch completes.

The state machine uses .waitForTaskToken, so it suspends until the callback arrives.
"""

import json
import os
import uuid

import boto3
import urllib3

sfn = boto3.client("stepfunctions")
http = urllib3.PoolManager()

CALLBACK_RELAY_URL = os.environ.get("CALLBACK_RELAY_URL")


def handler(event, context):
    url = event["url"]
    request_config = event.get("request_config", {})
    task_token = event["task_token"]

    fetch_service_url = request_config.get(
        "callback_fetch_service_url", "http://callback-fetch-service:8090"
    )
    correlation_id = str(uuid.uuid4())

    # The callback URL points to a relay endpoint (API Gateway + Lambda or similar)
    # that receives the fetch result and calls SendTaskSuccess/SendTaskFailure
    # with the embedded task token.
    callback_url = f"{CALLBACK_RELAY_URL}?task_token={task_token}"

    headers = {"Content-Type": "application/json", "User-Agent": "orchestration-bakeoff/1.0"}

    # Build request headers for the actual fetch (API key if configured)
    fetch_headers = {}
    if "api_key_secret_arn" in request_config:
        secrets = boto3.client("secretsmanager")
        secret = secrets.get_secret_value(SecretId=request_config["api_key_secret_arn"])
        api_key = json.loads(secret["SecretString"])["api_key"]
        fetch_headers["Authorization"] = f"Bearer {api_key}"

    payload = {
        "url": url,
        "headers": fetch_headers,
        "callback_url": callback_url,
        "correlation_id": correlation_id,
    }

    response = http.request(
        "POST",
        f"{fetch_service_url}/fetch-async",
        body=json.dumps(payload),
        headers=headers,
        timeout=10.0,
    )

    if response.status != 202:
        raise Exception(
            f"Callback Fetch Service returned {response.status}: "
            f"{response.data.decode('utf-8')[:500]}"
        )

    # The Lambda returns here, but the state machine stays suspended.
    # It will resume when the callback relay calls SendTaskSuccess.
    return {"correlation_id": correlation_id, "status": "submitted"}

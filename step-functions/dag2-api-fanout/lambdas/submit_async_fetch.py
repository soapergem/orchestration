"""
Lambda: SubmitAsyncFetch
Registers the fetch request with the Callback Fetch Service, handing it the
state machine's task token as the provider-specific resume handle.

The state machine uses .waitForTaskToken, so it suspends until something calls
POST /resume/<correlation_id> on the fetch service, at which point the service
calls SendTaskSuccess/SendTaskFailure with this token itself -- no relay Lambda
in between.
"""

import json
import os
import uuid

import urllib3

http = urllib3.PoolManager()


def handler(event, context):
    url = event["url"]
    request_config = event.get("request_config", {})
    task_token = event["task_token"]

    fetch_service_url = (
        request_config.get("callback_fetch_service_url")
        or os.environ.get("CALLBACK_FETCH_SERVICE_URL", "http://callback-fetch-service:8090")
    )
    # Allow the caller to pin a correlation_id so an external trigger knows which
    # request to /resume; otherwise generate one.
    correlation_id = event.get("correlation_id") or str(uuid.uuid4())

    headers = {"Content-Type": "application/json", "User-Agent": "orchestration-bakeoff/1.0"}

    # Build request headers for the actual fetch (API key if configured)
    fetch_headers = {}
    if "api_key_secret_arn" in request_config:
        import boto3

        secrets = boto3.client("secretsmanager")
        secret = secrets.get_secret_value(SecretId=request_config["api_key_secret_arn"])
        api_key = json.loads(secret["SecretString"])["api_key"]
        fetch_headers["Authorization"] = f"Bearer {api_key}"

    payload = {
        "url": url,
        "headers": fetch_headers,
        "correlation_id": correlation_id,
        "provider": "stepfunctions",
        "resume_data": {
            "task_token": task_token,
            "region": os.environ.get("AWS_REGION"),
        },
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
    # It resumes when the fetch service's /resume endpoint calls SendTaskSuccess.
    return {"correlation_id": correlation_id, "status": "submitted"}

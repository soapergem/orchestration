"""
Lambda: CallbackRelay
Receives the HTTP callback from the Callback Fetch Service and translates it into
a SendTaskSuccess or SendTaskFailure call to resume the Step Functions execution.

Deployed behind an API Gateway endpoint. The task_token is passed as a query parameter.
"""

import json

import boto3

sfn = boto3.client("stepfunctions")


def handler(event, context):
    # task_token comes from the query string
    query_params = event.get("queryStringParameters", {}) or {}
    task_token = query_params.get("task_token")

    if not task_token:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Missing task_token query parameter"}),
        }

    body = json.loads(event.get("body", "{}"))
    status = body.get("status")

    if status == "completed":
        sfn.send_task_success(
            taskToken=task_token,
            output=json.dumps(body),
        )
    else:
        sfn.send_task_failure(
            taskToken=task_token,
            error="FetchFailed",
            cause=body.get("error", "Unknown error from fetch service"),
        )

    return {"statusCode": 200, "body": json.dumps({"status": "relayed"})}

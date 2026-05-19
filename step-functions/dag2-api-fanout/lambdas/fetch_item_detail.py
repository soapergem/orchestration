"""
Lambda: FetchItemDetail
Fetches detailed information for a single item from the fan-out.
"""

import json

import boto3
import urllib3


secrets = boto3.client("secretsmanager")
http = urllib3.PoolManager()


def handler(event, context):
    item = event["item"]
    request_config = event.get("request_config", {})

    detail_url = item["detail_url"]

    # Build headers
    headers = {"User-Agent": "orchestration-bakeoff/1.0"}

    if "api_key_secret_arn" in request_config:
        secret = secrets.get_secret_value(SecretId=request_config["api_key_secret_arn"])
        api_key = json.loads(secret["SecretString"])["api_key"]
        headers["Authorization"] = f"Bearer {api_key}"

    # Make the detail API request
    response = http.request("GET", detail_url, headers=headers, timeout=30.0)

    if response.status != 200:
        raise Exception(
            f"Detail API request for {item['id']} failed with status {response.status}: "
            f"{response.data.decode('utf-8')[:500]}"
        )

    detail = json.loads(response.data.decode("utf-8"))

    return {
        "id": item["id"],
        "name": item["name"],
        "detail": detail,
    }

"""
Lambda: FetchInitialContent
Calls an initial API endpoint and returns a list of items to fan out over.

Example: Fetch a GitHub org's repos, then fan out to get details for each repo.
"""

import json

import boto3
import urllib3


secrets = boto3.client("secretsmanager")
http = urllib3.PoolManager()


def handler(event, context):
    url = event["url"]
    request_config = event.get("request_config", {})

    # Build headers (optionally fetch API key from Secrets Manager)
    headers = {"User-Agent": "orchestration-bakeoff/1.0"}

    if "api_key_secret_arn" in request_config:
        secret = secrets.get_secret_value(SecretId=request_config["api_key_secret_arn"])
        api_key = json.loads(secret["SecretString"])["api_key"]
        headers["Authorization"] = f"Bearer {api_key}"

    # Make the initial API request
    response = http.request("GET", url, headers=headers, timeout=30.0)

    if response.status != 200:
        raise Exception(
            f"Initial API request failed with status {response.status}: "
            f"{response.data.decode('utf-8')[:500]}"
        )

    data = json.loads(response.data.decode("utf-8"))

    # Extract items to fan out over.
    # This is API-specific. For a GitHub repos response, each item has a "url" field.
    # Adapt this parsing logic to your specific API.
    items = []
    for item in data:
        items.append(
            {
                "id": item.get("id"),
                "name": item.get("name", item.get("id")),
                "detail_url": item.get("url"),
            }
        )

    return {
        "source_url": url,
        "item_count": len(items),
        "items": items,
        "request_config": request_config,
    }

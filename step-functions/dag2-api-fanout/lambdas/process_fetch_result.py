"""
Lambda: ProcessFetchResult
Normalizes the callback payload from the Callback Fetch Service into the standard
format expected by the downstream fan-out steps (CheckItemsExist, FanOutAPIRequests).

This ensures the rest of the DAG works identically regardless of whether the fetch
was synchronous or async.
"""

import json


def handler(event, context):
    # The event is the raw callback payload from the fetch service,
    # forwarded by SendTaskSuccess.
    callback_status = event.get("status")

    if callback_status != "completed":
        raise Exception(
            f"Fetch service returned status '{callback_status}': "
            f"{event.get('error', 'unknown error')}"
        )

    body = event.get("body")
    if body is None:
        raise Exception("Fetch service callback contained no body")

    # The body is the raw API response (e.g., a list of GitHub repos).
    # Parse it into the standard items format.
    if isinstance(body, str):
        body = json.loads(body)

    items = []
    if isinstance(body, list):
        for item in body:
            items.append(
                {
                    "id": item.get("id"),
                    "name": item.get("name", item.get("id")),
                    "detail_url": item.get("url"),
                }
            )

    source_url = event.get("url", "unknown")

    return {
        "source_url": source_url,
        "item_count": len(items),
        "items": items,
        "request_config": event.get("request_config", {}),
    }

"""
DAG 2: API Fan-Out with Async Callback

Submits an async fetch request to the callback-fetch-service, waits for the
result via a durable event wait, processes the result, fans out child workflows
to fetch item details in parallel, and combines all results.

Hatchet features used:
- Durable event waits (durable_task with aio_wait_for_event)
- Child workflow spawning for fan-out
- Task-level retries with backoff
- DAG-style sequential dependencies
"""

import json
import os
import uuid

import httpx

from hatchet_sdk import Context, DurableContext, Hatchet

hatchet = Hatchet()

CALLBACK_FETCH_SERVICE_URL = os.environ.get(
    "CALLBACK_FETCH_SERVICE_URL", "http://callback-fetch-service:8090"
)
HATCHET_EVENT_API_URL = os.environ.get(
    "HATCHET_EVENT_API_URL", "http://localhost:8080/api/v1/events"
)


# ---------------------------------------------------------------------------
# Child workflow: fetch detail for a single item
# ---------------------------------------------------------------------------

fetch_item_detail_wf = hatchet.workflow(
    name="FetchItemDetail", on_events=["item:fetch_detail"]
)


@fetch_item_detail_wf.task(
    name="fetch_detail",
    retries=3,
    backoff_factor=2.0,
    backoff_max_seconds=2,
)
async def fetch_detail(input: dict, context: Context) -> dict:
    """Fetches detailed information for a single item."""
    input = input.model_dump()
    item = input["item"]
    request_config = input.get("request_config", {})

    detail_url = item["detail_url"]

    headers = {"User-Agent": "orchestration-bakeoff/1.0"}
    if request_config.get("api_key"):
        headers["Authorization"] = f"Bearer {request_config['api_key']}"

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(detail_url, headers=headers)

    if response.status_code != 200:
        raise Exception(
            f"Detail API request for {item['id']} failed with status "
            f"{response.status_code}: {response.text[:500]}"
        )

    detail = response.json()

    return {
        "id": item["id"],
        "name": item["name"],
        "detail": detail,
    }


# ---------------------------------------------------------------------------
# Main API fan-out workflow
# ---------------------------------------------------------------------------
#
# API Fan-Out Pipeline:
# 1. Submit async fetch request to callback-fetch-service
# 2. Wait for callback via durable event wait
# 3. Process the fetch result into normalized items
# 4. Fan-out child workflows to fetch details for each item
# 5. Combine all results into a summary

api_fanout_wf = hatchet.workflow(name="APIFanOut", on_events=["api:fanout"])


@api_fanout_wf.task(
    name="submit_async_fetch",
    retries=3,
    backoff_factor=2.0,
    backoff_max_seconds=2,
)
async def submit_async_fetch(input: dict, context: Context) -> dict:
    """
    POST to callback-fetch-service with a callback_url pointing to
    Hatchet's event API. The service will POST the fetch result as a
    Hatchet event when the async fetch completes.
    """
    input = input.model_dump()
    url = input["url"]
    request_config = input.get("request_config", {})

    correlation_id = str(uuid.uuid4())

    fetch_service_url = request_config.get(
        "callback_fetch_service_url", CALLBACK_FETCH_SERVICE_URL
    )

    # The callback URL points to Hatchet's event ingestion endpoint.
    # The callback-fetch-service will POST the result here, which Hatchet
    # ingests as a "fetch_completed" event with our correlation_id.
    callback_url = (
        f"{HATCHET_EVENT_API_URL}"
        f"?event_type=fetch_completed"
        f"&correlation_id={correlation_id}"
    )

    # Build headers for the actual upstream fetch
    fetch_headers = {}
    if request_config.get("api_key"):
        fetch_headers["Authorization"] = f"Bearer {request_config['api_key']}"

    payload = {
        "url": url,
        "headers": fetch_headers,
        "callback_url": callback_url,
        "correlation_id": correlation_id,
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(
            f"{fetch_service_url}/fetch-async",
            json=payload,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "orchestration-bakeoff/1.0",
            },
        )

    if response.status_code not in (200, 202):
        raise Exception(
            f"Callback Fetch Service returned {response.status_code}: "
            f"{response.text[:500]}"
        )

    return {
        "correlation_id": correlation_id,
        "status": "submitted",
    }


@api_fanout_wf.durable_task(
    name="wait_for_callback",
    parents=[submit_async_fetch],
)
async def wait_for_callback(input: dict, context: DurableContext) -> dict:
    """
    Durable event wait: suspend this task until the callback-fetch-service
    pushes a 'fetch_completed' event with our correlation_id.
    """
    input = input.model_dump()
    submit_result = context.task_output(submit_async_fetch)
    correlation_id = submit_result["correlation_id"]

    # Durable event wait -- Hatchet suspends this task to disk and resumes
    # when the matching event arrives.
    event_data = await context.aio_wait_for_event(
        "fetch_completed",
        expression=f"{{{{ .correlation_id }}}} == '{correlation_id}'",
    )

    return {
        "callback_received": True,
        "correlation_id": correlation_id,
        "event_data": event_data,
    }


@api_fanout_wf.task(
    name="process_fetch_result",
    parents=[wait_for_callback],
    retries=3,
    backoff_factor=2.0,
    backoff_max_seconds=2,
)
async def process_fetch_result(input: dict, context: Context) -> dict:
    """
    Normalize the callback payload into the standard items format
    expected by the downstream fan-out.
    """
    input = input.model_dump()
    callback_result = context.task_output(wait_for_callback)
    event_data = callback_result["event_data"]

    callback_status = event_data.get("status")
    if callback_status != "completed":
        raise Exception(
            f"Fetch service returned status '{callback_status}': "
            f"{event_data.get('error', 'unknown error')}"
        )

    body = event_data.get("body")
    if body is None:
        raise Exception("Fetch service callback contained no body")

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

    source_url = input.get("url", "unknown")

    return {
        "source_url": source_url,
        "item_count": len(items),
        "items": items,
        "request_config": input.get("request_config", {}),
    }


@api_fanout_wf.task(
    name="fan_out",
    parents=[process_fetch_result],
    retries=3,
    backoff_factor=2.0,
    backoff_max_seconds=2,
)
async def fan_out(input: dict, context: Context) -> dict:
    """Spawn child workflows to fetch details for each item in parallel."""
    input = input.model_dump()
    process_result = context.task_output(process_fetch_result)
    items = process_result.get("items", [])
    request_config = process_result.get("request_config", {})

    if not items:
        return {
            "status": "no_items",
            "message": "No items found from initial content fetch.",
            "api_results": [],
        }

    # Bulk spawn child workflows
    spawn_refs = []
    for item in items:
        child_input = {
            "item": item,
            "request_config": request_config,
        }
        ref = await fetch_item_detail_wf.aio_run_no_wait(
            child_input,
            child_key=f"fetch-detail-{item['id']}",
        )
        spawn_refs.append(ref)

    # Collect all child results. aio_result() returns the child workflow's
    # output keyed by task name, so unwrap the single fetch_detail task.
    api_results = []
    for ref in spawn_refs:
        try:
            result = await ref.aio_result()
            api_results.append(result["fetch_detail"])
        except Exception as e:
            api_results.append({"error": str(e)})

    return {"api_results": api_results}


@api_fanout_wf.task(
    name="combine_results",
    parents=[fan_out],
    retries=3,
    backoff_factor=2.0,
    backoff_max_seconds=2,
)
async def combine_results(input: dict, context: Context) -> dict:
    """Merge all fan-out API results into a single summary."""
    input = input.model_dump()
    fan_out_result = context.task_output(fan_out)

    if fan_out_result.get("status") == "no_items":
        return fan_out_result

    api_results = fan_out_result["api_results"]
    process_result = context.task_output(process_fetch_result)
    source_url = process_result.get("source_url", "unknown")

    combined = []
    errors = []

    for result in api_results:
        if "error" in result:
            errors.append({"id": result.get("id"), "error": result["error"]})
        else:
            combined.append(
                {
                    "id": result["id"],
                    "name": result["name"],
                    "detail": result.get("detail", {}),
                }
            )

    return {
        "status": "success",
        "source_url": source_url,
        "total_items": len(api_results),
        "successful": len(combined),
        "failed": len(errors),
        "results": combined,
        "errors": errors,
    }

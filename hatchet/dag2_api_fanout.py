"""
DAG 2: API Fan-Out with Async Callback

Submits an async fetch request to the callback-fetch-service, waits for the
result via a durable event wait, processes the result, fans out child workflows
to fetch item details in parallel, and combines all results.

Hatchet features used:
- Durable event waits (context.event() with filter and timeout)
- Child workflow spawning for fan-out
- Task-level retries with backoff
- DAG-style sequential dependencies
"""

import json
import os
import uuid

import httpx

from hatchet_sdk import Context, Hatchet

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

@hatchet.workflow(name="FetchItemDetail", on_events=["item:fetch_detail"])
class FetchItemDetailWorkflow:
    """Fetches detailed information for a single item."""

    @hatchet.task(
        name="fetch_detail",
        retries=3,
        backoff_factor=2.0,
        backoff_base_seconds=2,
    )
    async def fetch_detail(self, context: Context) -> dict:
        input_data = context.workflow_input()
        item = input_data["item"]
        request_config = input_data.get("request_config", {})

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

@hatchet.workflow(name="APIFanOut", on_events=["api:fanout"])
class APIFanOutWorkflow:
    """
    API Fan-Out Pipeline:
    1. Submit async fetch request to callback-fetch-service
    2. Wait for callback via durable event wait
    3. Process the fetch result into normalized items
    4. Fan-out child workflows to fetch details for each item
    5. Combine all results into a summary
    """

    @hatchet.task(
        name="submit_async_fetch",
        retries=3,
        backoff_factor=2.0,
        backoff_base_seconds=2,
    )
    async def submit_async_fetch(self, context: Context) -> dict:
        """
        POST to callback-fetch-service with a callback_url pointing to
        Hatchet's event API. The service will POST the fetch result as a
        Hatchet event when the async fetch completes.
        """
        input_data = context.workflow_input()
        url = input_data["url"]
        request_config = input_data.get("request_config", {})

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

    @hatchet.task(
        name="wait_for_callback",
        parents=["submit_async_fetch"],
    )
    async def wait_for_callback(self, context: Context) -> dict:
        """
        Durable event wait: suspend this task until the callback-fetch-service
        pushes a 'fetch_completed' event with our correlation_id, or timeout
        after 60 seconds.
        """
        submit_result = (await context.task_output("submit_async_fetch"))
        correlation_id = submit_result["correlation_id"]

        # Durable event wait -- Hatchet suspends this task to disk and resumes
        # when the matching event arrives, or raises on timeout.
        event_data = await (
            context.event("fetch_completed")
            .with_filter(correlation_id=correlation_id)
            .with_timeout(60)
        )

        return {
            "callback_received": True,
            "correlation_id": correlation_id,
            "event_data": event_data,
        }

    @hatchet.task(
        name="process_fetch_result",
        parents=["wait_for_callback"],
        retries=3,
        backoff_factor=2.0,
        backoff_base_seconds=2,
    )
    async def process_fetch_result(self, context: Context) -> dict:
        """
        Normalize the callback payload into the standard items format
        expected by the downstream fan-out.
        """
        callback_result = (await context.task_output("wait_for_callback"))
        event_data = callback_result["event_data"]
        input_data = context.workflow_input()

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

        source_url = input_data.get("url", "unknown")

        return {
            "source_url": source_url,
            "item_count": len(items),
            "items": items,
            "request_config": input_data.get("request_config", {}),
        }

    @hatchet.task(
        name="fan_out",
        parents=["process_fetch_result"],
        retries=3,
        backoff_factor=2.0,
        backoff_base_seconds=2,
    )
    async def fan_out(self, context: Context) -> dict:
        """Spawn child workflows to fetch details for each item in parallel."""
        process_result = (await context.task_output("process_fetch_result"))
        items = process_result.get("items", [])
        request_config = process_result.get("request_config", {})

        if not items:
            return {
                "status": "no_items",
                "message": "No items found from initial content fetch.",
                "api_results": [],
            }

        # Bulk spawn child workflows
        spawn_futures = []
        for item in items:
            child_input = {
                "item": item,
                "request_config": request_config,
            }
            future = context.spawn_workflow(
                "FetchItemDetail",
                child_input,
                key=f"fetch-detail-{item['id']}",
            )
            spawn_futures.append(future)

        # Collect all child results
        api_results = []
        for future in spawn_futures:
            try:
                result = await future.result()
                api_results.append(result)
            except Exception as e:
                api_results.append({"error": str(e)})

        return {"api_results": api_results}

    @hatchet.task(
        name="combine_results",
        parents=["fan_out"],
        retries=3,
        backoff_factor=2.0,
        backoff_base_seconds=2,
    )
    async def combine_results(self, context: Context) -> dict:
        """Merge all fan-out API results into a single summary."""
        fan_out_result = (await context.task_output("fan_out"))

        if fan_out_result.get("status") == "no_items":
            return fan_out_result

        api_results = fan_out_result["api_results"]
        process_result = (await context.task_output("process_fetch_result"))
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

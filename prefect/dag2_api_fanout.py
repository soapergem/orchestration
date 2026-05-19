"""
DAG 2: API Fan-Out with Async Callback
========================================
Submit an async fetch request to the callback-fetch-service, poll until the
result is ready, normalise the response, fan out to fetch item details in
parallel, and combine the results.

Prefect 3.x implementation using @flow, @task, .map(), and a polling-based
wait (see note below on pause_flow_run).

NOTE ON ASYNC WAIT STRATEGY
----------------------------
In production, Prefect's native ``pause_flow_run(timeout=60)`` combined with
the resume API (``POST /api/flow_runs/<run_id>/resume``) would allow the flow
to fully suspend and free the worker while waiting for the external callback.
The callback-fetch-service would POST to that resume endpoint when the fetch
completes.

For this bake-off we use a **polling approach** instead: after submitting the
async fetch, a task polls ``GET /status/<correlation_id>`` on the callback-
fetch-service every 5 seconds until the result is available or a timeout is
reached.  This avoids requiring the callback service to have network access to
the Prefect server's API.
"""

import json
import os
import time
import uuid

import httpx
from prefect import flow, get_run_logger, task

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CALLBACK_FETCH_SERVICE_URL = os.environ.get(
    "CALLBACK_FETCH_SERVICE_URL", "http://callback-fetch-service:8090"
)

DEFAULT_POLL_INTERVAL = 5   # seconds
DEFAULT_POLL_TIMEOUT = 60   # seconds


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(
    retries=3,
    retry_delay_seconds=[2, 4, 8],
    name="submit_async_fetch",
)
def submit_async_fetch(
    url: str,
    request_config: dict | None = None,
) -> dict:
    """
    POST to the callback-fetch-service's ``/fetch-async`` endpoint.

    Returns the correlation_id used to poll for the result.  The
    ``callback_url`` we provide is a no-op placeholder; in production it would
    point to the Prefect resume endpoint.
    """
    logger = get_run_logger()
    cfg = request_config or {}
    service_url = cfg.get("callback_fetch_service_url", CALLBACK_FETCH_SERVICE_URL)
    correlation_id = str(uuid.uuid4())

    # Build optional auth headers for the upstream API the service will call
    fetch_headers: dict[str, str] = {}
    if "api_key" in cfg:
        fetch_headers["Authorization"] = f"Bearer {cfg['api_key']}"

    payload = {
        "url": url,
        "headers": fetch_headers,
        # In production this would be the Prefect resume URL.  For the polling
        # approach we still supply a callback_url (the service may ignore it).
        "callback_url": cfg.get("callback_url", "http://localhost:0/noop"),
        "correlation_id": correlation_id,
    }

    with httpx.Client(timeout=10.0) as client:
        response = client.post(
            f"{service_url}/fetch-async",
            json=payload,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "orchestration-bakeoff/1.0",
            },
        )

    if response.status_code not in (200, 202):
        raise RuntimeError(
            f"callback-fetch-service returned {response.status_code}: "
            f"{response.text[:500]}"
        )

    logger.info(
        "Submitted async fetch for %s — correlation_id=%s",
        url,
        correlation_id,
    )

    return {
        "correlation_id": correlation_id,
        "service_url": service_url,
        "status": "submitted",
    }


@task(
    retries=2,
    retry_delay_seconds=5,
    name="poll_for_fetch_result",
)
def poll_for_fetch_result(
    correlation_id: str,
    service_url: str,
    poll_interval: int = DEFAULT_POLL_INTERVAL,
    poll_timeout: int = DEFAULT_POLL_TIMEOUT,
) -> dict:
    """
    Poll ``GET /status/<correlation_id>`` until the fetch is complete or the
    timeout expires.

    In production this would be replaced by ``pause_flow_run(timeout=60)`` with
    the callback-fetch-service POSTing to the Prefect resume endpoint.
    """
    logger = get_run_logger()
    deadline = time.monotonic() + poll_timeout

    with httpx.Client(timeout=10.0) as client:
        while time.monotonic() < deadline:
            resp = client.get(f"{service_url}/status/{correlation_id}")

            if resp.status_code == 200:
                body = resp.json()
                status = body.get("status")
                if status == "completed":
                    logger.info("Fetch completed for correlation_id=%s", correlation_id)
                    return body
                if status == "failed":
                    raise RuntimeError(
                        f"Fetch failed for {correlation_id}: "
                        f"{body.get('error', 'unknown error')}"
                    )
                # Still pending — keep polling
                logger.debug(
                    "Fetch still pending for %s (status=%s)",
                    correlation_id,
                    status,
                )
            elif resp.status_code == 404:
                logger.debug("Correlation %s not found yet, retrying...", correlation_id)
            else:
                logger.warning(
                    "Unexpected status %d polling %s",
                    resp.status_code,
                    correlation_id,
                )

            time.sleep(poll_interval)

    raise TimeoutError(
        f"Fetch for correlation_id={correlation_id} did not complete "
        f"within {poll_timeout}s"
    )


@task(name="process_fetch_result")
def process_fetch_result(fetch_response: dict) -> dict:
    """
    Normalise the callback payload into the standard items format expected by
    the downstream fan-out.
    """
    logger = get_run_logger()

    callback_status = fetch_response.get("status")
    if callback_status != "completed":
        raise RuntimeError(
            f"Fetch service returned status '{callback_status}': "
            f"{fetch_response.get('error', 'unknown error')}"
        )

    body = fetch_response.get("body")
    if body is None:
        raise RuntimeError("Fetch service callback contained no body")

    if isinstance(body, str):
        body = json.loads(body)

    items: list[dict] = []
    if isinstance(body, list):
        for item in body:
            items.append(
                {
                    "id": item.get("id"),
                    "name": item.get("name", item.get("id")),
                    "detail_url": item.get("url"),
                }
            )

    source_url = fetch_response.get("url", "unknown")
    logger.info("Processed fetch result: %d items from %s", len(items), source_url)

    return {
        "source_url": source_url,
        "item_count": len(items),
        "items": items,
        "request_config": fetch_response.get("request_config", {}),
    }


@task(
    retries=3,
    retry_delay_seconds=[2, 4, 8],
    name="fetch_item_detail",
)
def fetch_item_detail(item: dict, request_config: dict | None = None) -> dict:
    """Fetch detailed information for a single item."""
    logger = get_run_logger()
    cfg = request_config or {}

    detail_url = item["detail_url"]
    headers: dict[str, str] = {"User-Agent": "orchestration-bakeoff/1.0"}

    if "api_key" in cfg:
        headers["Authorization"] = f"Bearer {cfg['api_key']}"

    with httpx.Client(timeout=30.0) as client:
        response = client.get(detail_url, headers=headers)

    if response.status_code != 200:
        raise RuntimeError(
            f"Detail request for item {item['id']} failed with "
            f"status {response.status_code}: {response.text[:500]}"
        )

    detail = response.json()
    logger.info("Fetched detail for item %s", item["id"])

    return {
        "id": item["id"],
        "name": item["name"],
        "detail": detail,
    }


@task(name="combine_results")
def combine_results(api_results: list[dict], source_url: str = "unknown") -> dict:
    """Merge all fan-out results into a single summary."""
    combined: list[dict] = []
    errors: list[dict] = []

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


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="api_fanout_pipeline", log_prints=True)
def api_fanout_pipeline(
    url: str,
    request_config: dict | None = None,
    poll_interval: int = DEFAULT_POLL_INTERVAL,
    poll_timeout: int = DEFAULT_POLL_TIMEOUT,
) -> dict:
    """
    API fan-out pipeline:
      1. Submit async fetch to the callback-fetch-service
      2. Poll until the fetch completes (or timeout)
      3. Normalise the result
      4. Fan out to fetch detail for each item (parallel via .map())
      5. Combine all results
    """
    logger = get_run_logger()
    cfg = request_config or {}

    # Step 1: Submit async fetch
    submission = submit_async_fetch(url=url, request_config=cfg)

    # Step 2: Poll for result
    fetch_response = poll_for_fetch_result(
        correlation_id=submission["correlation_id"],
        service_url=submission["service_url"],
        poll_interval=poll_interval,
        poll_timeout=poll_timeout,
    )

    # Step 3: Normalise
    processed = process_fetch_result(fetch_response)

    items = processed["items"]
    source_url = processed["source_url"]

    if not items:
        logger.info("No items found from initial fetch — nothing to fan out")
        return {
            "status": "no_items",
            "message": "No items found from initial content fetch.",
        }

    # Step 4: Fan-out — fetch detail for each item in parallel
    detail_futures = fetch_item_detail.map(
        items,
        request_config=processed.get("request_config"),
    )

    detail_results = [f.result() for f in detail_futures]

    # Step 5: Combine
    combined = combine_results(api_results=detail_results, source_url=source_url)

    logger.info(
        "Pipeline complete: %d/%d items succeeded",
        combined["successful"],
        combined["total_items"],
    )

    return combined


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Example invocation
    result = api_fanout_pipeline(
        url="https://api.github.com/orgs/PrefectHQ/repos",
        request_config={},
    )
    print(json.dumps(result, indent=2))

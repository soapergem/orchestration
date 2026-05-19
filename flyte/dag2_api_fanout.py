"""
DAG 2: API Fan-Out with Async Callback — Flyte Implementation

Pipeline:
  1. submit_async_fetch   — POST to callback-fetch-service
  2. poll_for_callback    — Poll GET /status/<correlation_id> every 5s
                            (production: use wait_for_input)
  3. process_fetch_result — Normalize the response into FanOutItems
  4. fan_out_items        — @dynamic mapping fetch_item_detail over items
  5. combine_results      — Merge all detail responses into CombinedResult

Equivalent Step Functions workflow:
  step-functions/dag2-api-fanout/state-machine.asl.json

Key Flyte features demonstrated:
  - @dynamic for runtime fan-out over items discovered at step 3
  - Polling fallback for async callback pattern (with documentation of
    wait_for_input for production use)
  - RetryStrategy on HTTP tasks
  - Strong typing on every task boundary

Production note on wait_for_input:
  Flyte 1.10+ supports ``wait_for_input(name=..., expected_type=...,
  timeout=...)`` which suspends the workflow until an external system POSTs
  the value to the FlyteAdmin API. In a production deployment the
  callback-fetch-service would call FlyteAdmin to provide the input. For
  the bake-off we use a polling fallback instead, since it does not require
  the callback service to have FlyteAdmin API access.
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import timedelta
from typing import List

import urllib3
from flytekit import ImageSpec, dynamic, task, workflow

from .types import (
    CombinedResult,
    FanOutInput,
    FanOutItem,
    FetchResult,
    ItemDetail,
    ItemError,
    RequestConfig,
)

# ---------------------------------------------------------------------------
# Container image spec
# ---------------------------------------------------------------------------
fanout_image = ImageSpec(
    name="api-fanout",
    packages=[
        "urllib3",
        "flytekit",
    ],
    python_version="3.11",
)

_http = urllib3.PoolManager()


# ---------------------------------------------------------------------------
# Task 1: Submit async fetch to callback-fetch-service
# ---------------------------------------------------------------------------
@task(
    retries=3,
    container_image=fanout_image,
)
def submit_async_fetch(url: str, request_config: RequestConfig) -> FetchResult:
    """POST to the callback-fetch-service to kick off an async fetch.

    The service will begin fetching *url* in the background. We provide a
    ``correlation_id`` so we can poll for the result later.

    In production with Flyte wait_for_input, we would instead provide a
    ``callback_url`` pointing at the FlyteAdmin signal endpoint. The service
    would POST the result there, resuming the workflow without polling.
    """
    correlation_id = str(uuid.uuid4())
    service_url = request_config.callback_fetch_service_url

    headers = {
        "Content-Type": "application/json",
        "User-Agent": request_config.user_agent,
    }

    fetch_headers = {}
    if request_config.api_key:
        fetch_headers["Authorization"] = f"Bearer {request_config.api_key}"

    payload = {
        "url": url,
        "headers": fetch_headers,
        "callback_url": "",  # Not used in polling mode
        "correlation_id": correlation_id,
    }

    response = _http.request(
        "POST",
        f"{service_url}/fetch-async",
        body=json.dumps(payload),
        headers=headers,
        timeout=10.0,
    )

    if response.status != 202:
        raise RuntimeError(
            f"callback-fetch-service returned {response.status}: "
            f"{response.data.decode('utf-8')[:500]}"
        )

    return FetchResult(
        status="submitted",
        correlation_id=correlation_id,
        body="",
        url=url,
    )


# ---------------------------------------------------------------------------
# Task 2: Poll for callback result
#
# PRODUCTION ALTERNATIVE — wait_for_input:
#   In a real Flyte deployment you would replace this polling task with
#   wait_for_input in the workflow definition:
#
#       fetch_result = wait_for_input(
#           name="fetch_result",
#           expected_type=FetchResult,
#           timeout=timedelta(seconds=60),
#       )
#
#   The callback-fetch-service would POST the result to the FlyteAdmin API:
#       PUT /api/v1/signals/{execution_id}/{name}
#   which resumes the workflow without any polling.
# ---------------------------------------------------------------------------
@task(
    retries=3,
    container_image=fanout_image,
    timeout=timedelta(seconds=120),
)
def poll_for_callback(
    submit_result: FetchResult,
    request_config: RequestConfig,
) -> FetchResult:
    """Poll GET /status/<correlation_id> every 5 seconds until the fetch completes.

    This is the bake-off fallback. In production, use Flyte's
    ``wait_for_input`` to avoid polling entirely.
    """
    service_url = request_config.callback_fetch_service_url
    correlation_id = submit_result.correlation_id
    max_attempts = 12  # 12 * 5s = 60s total

    headers = {"User-Agent": request_config.user_agent}

    for attempt in range(max_attempts):
        response = _http.request(
            "GET",
            f"{service_url}/status/{correlation_id}",
            headers=headers,
            timeout=10.0,
        )

        if response.status == 200:
            data = json.loads(response.data.decode("utf-8"))
            status = data.get("status", "")

            if status == "completed":
                body = data.get("body", data.get("result", ""))
                if not isinstance(body, str):
                    body = json.dumps(body)
                return FetchResult(
                    status="completed",
                    correlation_id=correlation_id,
                    body=body,
                    url=submit_result.url,
                )

            if status == "failed":
                raise RuntimeError(
                    f"Async fetch {correlation_id} failed: "
                    f"{data.get('error', 'unknown error')}"
                )

        # Still pending — wait and retry
        if attempt < max_attempts - 1:
            time.sleep(5)

    raise TimeoutError(
        f"Async fetch {correlation_id} did not complete within "
        f"{max_attempts * 5} seconds"
    )


# ---------------------------------------------------------------------------
# Task 3: Normalize the fetch result into fan-out items
# ---------------------------------------------------------------------------
@task(
    retries=3,
    container_image=fanout_image,
)
def process_fetch_result(
    fetch_result: FetchResult,
) -> List[FanOutItem]:
    """Parse the callback body into a list of FanOutItems for the fan-out step.

    Mirrors the Step Functions ``ProcessFetchResult`` Lambda. The body is
    expected to be a JSON array of objects with ``id``, ``name``/``id``, and
    ``url`` fields (e.g. a list of GitHub repos).
    """
    if fetch_result.status != "completed":
        raise RuntimeError(
            f"Fetch returned status '{fetch_result.status}' — expected 'completed'"
        )

    body = fetch_result.body
    if not body:
        raise RuntimeError("Fetch callback contained no body")

    data = json.loads(body) if isinstance(body, str) else body

    items: List[FanOutItem] = []
    if isinstance(data, list):
        for item in data:
            items.append(
                FanOutItem(
                    id=str(item.get("id", "")),
                    name=str(item.get("name", item.get("id", ""))),
                    detail_url=str(item.get("url", "")),
                )
            )

    return items


# ---------------------------------------------------------------------------
# Task 4a: Fetch detail for a single item
# ---------------------------------------------------------------------------
@task(
    retries=3,
    container_image=fanout_image,
)
def fetch_item_detail(
    item: FanOutItem,
    request_config: RequestConfig,
) -> ItemDetail:
    """GET the detail URL for a single fan-out item.

    Mirrors the Step Functions ``FetchItemDetail`` Lambda.
    """
    headers = {"User-Agent": request_config.user_agent}
    if request_config.api_key:
        headers["Authorization"] = f"Bearer {request_config.api_key}"

    response = _http.request(
        "GET",
        item.detail_url,
        headers=headers,
        timeout=30.0,
    )

    if response.status != 200:
        raise RuntimeError(
            f"Detail API for {item.id} returned {response.status}: "
            f"{response.data.decode('utf-8')[:500]}"
        )

    detail_json = response.data.decode("utf-8")

    return ItemDetail(
        id=item.id,
        name=item.name,
        detail=detail_json,
    )


# ---------------------------------------------------------------------------
# Task 4b: Dynamic fan-out over all items
# ---------------------------------------------------------------------------
@dynamic(container_image=fanout_image)
def fan_out_items(
    items: List[FanOutItem],
    request_config: RequestConfig,
) -> List[ItemDetail]:
    """Dynamically map ``fetch_item_detail`` over every FanOutItem.

    Equivalent to Step Functions' Map state with MaxConcurrency=20.
    Flyte's @dynamic creates one task node per item at runtime.
    """
    details: List[ItemDetail] = []
    for item in items:
        detail = fetch_item_detail(item=item, request_config=request_config)
        details.append(detail)
    return details


# ---------------------------------------------------------------------------
# Task 5: Combine all results
# ---------------------------------------------------------------------------
@task(
    retries=3,
    container_image=fanout_image,
)
def combine_results(
    source_url: str,
    details: List[ItemDetail],
) -> CombinedResult:
    """Merge all fan-out detail responses into a single summary.

    Mirrors the Step Functions ``CombineResults`` Lambda.
    """
    successful: List[ItemDetail] = []
    errors: List[ItemError] = []

    for detail in details:
        if not detail.detail:
            errors.append(ItemError(id=detail.id, error="Empty detail response"))
        else:
            successful.append(detail)

    return CombinedResult(
        status="success",
        source_url=source_url,
        total_items=len(details),
        successful=len(successful),
        failed=len(errors),
        results=successful,
        errors=errors,
    )


# ---------------------------------------------------------------------------
# Top-level workflow
# ---------------------------------------------------------------------------
@workflow
def api_fanout_pipeline(fanout_input: FanOutInput) -> CombinedResult:
    """API Fan-Out Pipeline.

    1. Submit an async fetch to the callback-fetch-service.
    2. Poll for completion (production: wait_for_input).
    3. Normalize the response into a list of items.
    4. Fan out: fetch detail for every item in parallel.
    5. Combine all detail results into a single summary.

    If no items are found, the pipeline returns a CombinedResult with
    total_items=0.
    """
    submit_result = submit_async_fetch(
        url=fanout_input.url,
        request_config=fanout_input.request_config,
    )

    fetch_result = poll_for_callback(
        submit_result=submit_result,
        request_config=fanout_input.request_config,
    )

    items = process_fetch_result(fetch_result=fetch_result)

    details = fan_out_items(
        items=items,
        request_config=fanout_input.request_config,
    )

    result = combine_results(
        source_url=fanout_input.url,
        details=details,
    )

    return result

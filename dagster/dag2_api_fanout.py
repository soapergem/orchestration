"""
DAG 2 -- API Fan-Out with Async Callback (Dagster)

Architecture divergence
-----------------------
Dagster cannot suspend a running op to wait for an external callback.  The
Step Functions version uses ``.waitForTaskToken`` to park the execution until
the Callback Fetch Service POSTs a result back.

In Dagster the workflow is split across **two job runs** with a **sensor**
bridging them:

  Job 1 (``submit_fetch_job``):
      submit_async_fetch  -- POST to the Callback Fetch Service, persist the
                             ``correlation_id`` to a file so the sensor can
                             find it.

  Sensor (``fetch_completion_sensor`` in sensors.py):
      Polls GET /status/<correlation_id> on the Callback Fetch Service.
      When status == "completed", triggers Job 2 with the result payload
      as run config.

  Job 2 (``process_and_fanout_job``):
      process_fetch_result --> fan_out_api_requests (DynamicOutput) --> combine_results

This is the idiomatic Dagster pattern for bridging an async external system
that communicates via polling or callbacks.
"""

import json
import os
import uuid

from dagster import (
    DynamicOut,
    DynamicOutput,
    In,
    Out,
    Output,
    RetryPolicy,
    graph,
    job,
    op,
)

from .resources import HttpClientResource, PostgresResource

# ---------------------------------------------------------------------------
# Shared paths for correlation data
# ---------------------------------------------------------------------------

CORRELATION_DIR = os.environ.get(
    "DAG2_CORRELATION_DIR", "/tmp/dagster_dag2_correlations"
)

RETRY = RetryPolicy(max_retries=3, delay=5)


def _correlation_path(correlation_id: str) -> str:
    return os.path.join(CORRELATION_DIR, f"{correlation_id}.json")


# ---------------------------------------------------------------------------
# Job 1 ops
# ---------------------------------------------------------------------------


@op(
    description=(
        "POST an async fetch request to the Callback Fetch Service.  "
        "Writes the correlation_id to a shared directory for the sensor."
    ),
    retry_policy=RETRY,
    out=Out(dict),
    config_schema={
        "url": str,
        "api_key": str,
    },
)
def submit_async_fetch(context, http_client: HttpClientResource) -> dict:
    """Submit an async fetch and record the correlation_id on disk."""
    url = context.op_config["url"]
    api_key = context.op_config.get("api_key", "")

    correlation_id = str(uuid.uuid4())
    fetch_service_url = http_client.callback_fetch_service_url

    fetch_headers = {}
    if api_key:
        fetch_headers["Authorization"] = f"Bearer {api_key}"

    payload = {
        "url": url,
        "headers": fetch_headers,
        "callback_url": "",
        "correlation_id": correlation_id,
    }

    response = http_client.post(
        f"{fetch_service_url}/fetch-async",
        json_body=payload,
        timeout=10.0,
    )

    if response.status_code != 202:
        raise Exception(
            f"Callback Fetch Service returned {response.status_code}: "
            f"{response.text[:500]}"
        )

    os.makedirs(CORRELATION_DIR, exist_ok=True)
    record = {
        "correlation_id": correlation_id,
        "url": url,
        "dagster_run_id": context.run_id,
        "status": "submitted",
    }
    with open(_correlation_path(correlation_id), "w") as f:
        json.dump(record, f)

    context.log.info(
        f"Submitted async fetch, correlation_id={correlation_id}"
    )
    return record


# ---------------------------------------------------------------------------
# Job 1 graph / job
# ---------------------------------------------------------------------------


@graph
def submit_fetch_graph():
    submit_async_fetch()


submit_fetch_job = submit_fetch_graph.to_job(
    name="submit_fetch_job",
    description="Job 1 of DAG2: submit an async fetch request.",
    resource_defs={
        "http_client": HttpClientResource(),
    },
)


# ---------------------------------------------------------------------------
# Job 2 ops
# ---------------------------------------------------------------------------


@op(
    description="Normalize the callback payload into the standard items format.",
    retry_policy=RETRY,
    out=Out(dict),
    config_schema={"fetch_result": dict},
)
def process_fetch_result(context) -> dict:
    """Parse the raw fetch-service response into ``{items: [...], ...}``."""
    event = context.op_config["fetch_result"]

    callback_status = event.get("status")
    if callback_status != "completed":
        raise Exception(
            f"Fetch service returned status '{callback_status}': "
            f"{event.get('error', 'unknown error')}"
        )

    body = event.get("body")
    if body is None:
        raise Exception("Fetch service callback contained no body")

    if isinstance(body, str):
        body = json.loads(body)

    items = []
    if isinstance(body, list):
        for item in body:
            items.append({
                "id": item.get("id"),
                "name": item.get("name", item.get("id")),
                "detail_url": item.get("url"),
            })

    source_url = event.get("url", "unknown")
    context.log.info(f"Processed fetch result: {len(items)} items from {source_url}")

    return {
        "source_url": source_url,
        "item_count": len(items),
        "items": items,
        "request_config": event.get("request_config", {}),
    }


@op(
    description="Fan out over each item and fetch its detail URL.",
    retry_policy=RETRY,
    out=DynamicOut(dict),
)
def fan_out_api_requests(
    context, processed: dict, http_client: HttpClientResource
) -> DynamicOutput:
    """Yield one DynamicOutput per item so downstream fetches run in parallel."""
    items = processed.get("items", [])
    request_config = processed.get("request_config", {})

    if not items:
        context.log.warning("No items to fan out over")
        return

    for idx, item in enumerate(items):
        detail_url = item.get("detail_url")
        if not detail_url:
            context.log.warning(f"Item {item.get('id')} has no detail_url -- skipping")
            continue

        headers = {"User-Agent": "orchestration-bakeoff/dagster"}
        api_key = request_config.get("api_key", "")
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        try:
            response = http_client.get(detail_url, timeout=30.0)
            if response.status_code != 200:
                raise Exception(
                    f"Detail API request for {item['id']} failed with status "
                    f"{response.status_code}: {response.text[:500]}"
                )
            detail = response.json()
        except Exception as exc:
            context.log.error(f"Error fetching detail for item {item.get('id')}: {exc}")
            detail = {"error": str(exc)}

        mapping_key = f"item_{str(item.get('id', idx)).replace('-', '_')}"
        yield DynamicOutput(
            {
                "id": item["id"],
                "name": item["name"],
                "detail": detail,
            },
            mapping_key=mapping_key,
        )


@op(
    description="Collect all fan-out results into a single combined summary.",
    retry_policy=RETRY,
    ins={"api_results": In(list)},
    out=Out(dict),
    config_schema={"source_url": str},
)
def combine_results(context, api_results: list) -> dict:
    """Merge individual item detail responses into one payload."""
    source_url = context.op_config.get("source_url", "unknown")

    combined = []
    errors = []

    for result in api_results:
        if "error" in result.get("detail", {}):
            errors.append({"id": result.get("id"), "error": result["detail"]["error"]})
        else:
            combined.append({
                "id": result["id"],
                "name": result["name"],
                "detail": result.get("detail", {}),
            })

    context.log.info(
        f"Combined {len(combined)} successes and {len(errors)} errors "
        f"from {len(api_results)} total items"
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
# Job 2 graph / job
# ---------------------------------------------------------------------------


@graph
def process_and_fanout_graph():
    processed = process_fetch_result()
    results = fan_out_api_requests(processed).collect()
    combine_results(results)


process_and_fanout_job = process_and_fanout_graph.to_job(
    name="process_and_fanout_job",
    description=(
        "Job 2 of DAG2: process a completed fetch result, fan out detail "
        "API requests, combine results.  Triggered by fetch_completion_sensor."
    ),
    resource_defs={
        "http_client": HttpClientResource(),
    },
)

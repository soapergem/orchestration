"""
DAG 2: API Fan-Out with Async Callback -- Temporal Workflow

Uses Temporal's first-class signal support as the native callback pattern:
1. SubmitAsyncFetch activity -- POST to callback-fetch-service
2. WaitForFetchCallback -- workflow.wait_condition on a signal
3. ProcessFetchResult -- transform in workflow code
4. FanOutAPIRequests -- asyncio.gather() over fetch_item_detail activities
5. CombineResults activity -- merge all results

The HTTP callback from the external service is received by signal_server.py,
which relays it as a Temporal signal to this workflow.
"""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from activities import (
        CombineResultsInput,
        CombineResultsOutput,
        FetchItemDetailInput,
        FetchItemDetailOutput,
        SubmitAsyncFetchInput,
        SubmitAsyncFetchOutput,
        combine_results,
        fetch_item_detail,
        submit_async_fetch,
    )


SIGNAL_SERVER_URL = os.environ.get("SIGNAL_SERVER_URL", "http://localhost:8095")

FANOUT_RETRY_POLICY = RetryPolicy(
    maximum_attempts=3,
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
)


@dataclass
class APIFanOutInput:
    """Input for the API Fan-Out workflow."""
    url: str
    headers: dict[str, str] = field(default_factory=dict)


@dataclass
class APIFanOutOutput:
    """Final combined output of the fan-out."""
    status: str
    source_url: str
    total_items: int
    successful: int
    failed: int
    results: list[dict[str, Any]]
    errors: list[dict[str, Any]]


@workflow.defn
class APIFanOutWorkflow:
    """
    API Fan-Out workflow with async callback via Temporal signals.

    The callback-fetch-service POSTs results to signal_server.py, which
    sends the ``fetch_completed`` signal to this workflow instance.
    """

    def __init__(self) -> None:
        self._fetch_result: dict[str, Any] | None = None

    # -- Signal handler: receives the async callback payload ----------------
    @workflow.signal
    async def fetch_completed(self, result: dict[str, Any]) -> None:
        """Signal handler invoked by the signal relay server."""
        workflow.logger.info("Received fetch_completed signal")
        self._fetch_result = result

    # -- Query handler: lets callers inspect the current state ---------------
    @workflow.query
    def get_fetch_result(self) -> dict[str, Any] | None:
        return self._fetch_result

    # -- Main workflow logic ------------------------------------------------
    @workflow.run
    async def run(self, input: APIFanOutInput) -> APIFanOutOutput:
        workflow.logger.info("Starting API Fan-Out for URL: %s", input.url)

        # Build the callback URL that points to our signal relay server
        wf_info = workflow.info()
        callback_url = (
            f"{SIGNAL_SERVER_URL}/fetch-callback"
            f"?workflow_id={wf_info.workflow_id}"
            f"&run_id={wf_info.run_id}"
        )

        # Step 1: Submit the async fetch request
        submit_result: SubmitAsyncFetchOutput = await workflow.execute_activity(
            submit_async_fetch,
            SubmitAsyncFetchInput(
                url=input.url,
                callback_url=callback_url,
                headers=input.headers,
            ),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=FANOUT_RETRY_POLICY,
        )

        workflow.logger.info(
            "Async fetch submitted: correlation_id=%s", submit_result.correlation_id
        )

        # Step 2: Wait for the callback signal (with timeout)
        try:
            await workflow.wait_condition(
                lambda: self._fetch_result is not None,
                timeout=timedelta(seconds=60),
            )
        except asyncio.TimeoutError:
            raise workflow.ApplicationError(
                "Timed out waiting for fetch callback after 60 seconds",
                type="FetchCallbackTimeout",
            )

        fetch_data = self._fetch_result
        assert fetch_data is not None

        # Step 3: Process the fetch result (pure workflow logic -- deterministic)
        callback_status = fetch_data.get("status")
        if callback_status != "completed":
            raise workflow.ApplicationError(
                f"Fetch service returned status '{callback_status}': "
                f"{fetch_data.get('error', 'unknown error')}",
                type="FetchFailed",
            )

        body = fetch_data.get("body")
        if body is None:
            raise workflow.ApplicationError(
                "Fetch service callback contained no body",
                type="FetchEmptyBody",
            )

        if isinstance(body, str):
            body = json.loads(body)

        items: list[dict[str, Any]] = []
        if isinstance(body, list):
            for item in body:
                items.append({
                    "id": item.get("id"),
                    "name": item.get("name", item.get("id")),
                    "detail_url": item.get("url"),
                })

        workflow.logger.info("Processed fetch result: %d items found", len(items))

        if not items:
            return APIFanOutOutput(
                status="no_items",
                source_url=input.url,
                total_items=0,
                successful=0,
                failed=0,
                results=[],
                errors=[],
            )

        # Step 4: Fan out -- fetch detail for each item in parallel
        detail_tasks = [
            workflow.execute_activity(
                fetch_item_detail,
                FetchItemDetailInput(
                    item_id=str(item["id"]),
                    name=str(item["name"]),
                    detail_url=item["detail_url"],
                    headers=input.headers,
                ),
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=FANOUT_RETRY_POLICY,
            )
            for item in items
        ]

        detail_results: list[FetchItemDetailOutput] = await asyncio.gather(
            *detail_tasks, return_exceptions=True
        )

        # Convert results for the combine step, handling exceptions gracefully
        results_for_combine: list[dict[str, Any]] = []
        for i, result in enumerate(detail_results):
            if isinstance(result, Exception):
                results_for_combine.append({
                    "id": str(items[i]["id"]),
                    "name": str(items[i]["name"]),
                    "error": str(result),
                })
            else:
                results_for_combine.append({
                    "id": result.id,
                    "name": result.name,
                    "detail": result.detail,
                })

        # Step 5: Combine results
        combined: CombineResultsOutput = await workflow.execute_activity(
            combine_results,
            CombineResultsInput(
                source_url=input.url,
                results=results_for_combine,
            ),
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=FANOUT_RETRY_POLICY,
        )

        workflow.logger.info(
            "Fan-out complete: %d successful, %d failed out of %d",
            combined.successful,
            combined.failed,
            combined.total_items,
        )

        return APIFanOutOutput(
            status=combined.status,
            source_url=combined.source_url,
            total_items=combined.total_items,
            successful=combined.successful,
            failed=combined.failed,
            results=combined.results,
            errors=combined.errors,
        )

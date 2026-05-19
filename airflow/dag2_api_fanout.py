"""
DAG 2: API Fan-Out with Async Callback

Submits an async fetch request to the callback-fetch-service, then uses a
**deferrable operator** with a custom trigger to poll for completion -- freeing
the Airflow worker while the triggerer process handles the polling.  Once
the fetch completes, the result is normalised, checked for items, and fanned
out via dynamic task mapping to fetch detail for each item in parallel.

Airflow idioms used:
- Custom deferrable operator (AsyncFetchOperator) with custom trigger
- TaskFlow API (@task, @task.branch)
- Dynamic task mapping (.expand())
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timedelta
from typing import Any

import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.trigger_rule import TriggerRule

from triggers.fetch_callback_trigger import FetchCallbackTrigger

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FETCH_SERVICE_URL = os.environ.get(
    "CALLBACK_FETCH_SERVICE_URL", "http://callback-fetch-service:8090"
)


# ---------------------------------------------------------------------------
# Deferrable operator: submit fetch, then defer to polling trigger
# ---------------------------------------------------------------------------

class AsyncFetchOperator(BaseOperator):
    """
    1. POST to callback-fetch-service /fetch-async with a callback_url.
    2. Defer execution to ``FetchCallbackTrigger`` which polls /status/<id>.
    3. When the trigger fires, ``execute_complete()`` processes the result.
    """

    template_fields = ("url", "fetch_service_url")

    def __init__(
        self,
        url: str,
        fetch_service_url: str = FETCH_SERVICE_URL,
        poll_interval: float = 5.0,
        fetch_timeout: float = 120.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.url = url
        self.fetch_service_url = fetch_service_url
        self.poll_interval = poll_interval
        self.fetch_timeout = fetch_timeout

    def execute(self, context: Any) -> None:
        """Submit the async fetch request, then defer to the trigger."""
        correlation_id = str(uuid.uuid4())

        # The callback_url is informational -- the trigger polls /status instead
        callback_url = (
            f"{self.fetch_service_url}/callback/{correlation_id}"
        )

        payload = {
            "url": self.url,
            "headers": {},
            "callback_url": callback_url,
            "correlation_id": correlation_id,
        }

        self.log.info(
            "Submitting async fetch for %s (correlation_id=%s)",
            self.url,
            correlation_id,
        )

        resp = requests.post(
            f"{self.fetch_service_url}/fetch-async",
            json=payload,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "orchestration-bakeoff/1.0",
            },
            timeout=10,
        )

        if resp.status_code not in (200, 202):
            raise AirflowException(
                f"Callback Fetch Service returned {resp.status_code}: "
                f"{resp.text[:500]}"
            )

        self.log.info("Fetch submitted, deferring to trigger for polling")

        self.defer(
            trigger=FetchCallbackTrigger(
                correlation_id=correlation_id,
                fetch_service_url=self.fetch_service_url,
                poll_interval=self.poll_interval,
                timeout=self.fetch_timeout,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Any, event: dict) -> dict:
        """Called by the triggerer when the polling trigger fires."""
        status = event.get("status")

        if status == "completed":
            self.log.info(
                "Fetch completed for correlation_id=%s",
                event.get("correlation_id"),
            )
            return event

        if status == "failed":
            raise AirflowException(
                f"Fetch failed for correlation_id={event.get('correlation_id')}: "
                f"{event.get('error')}"
            )

        if status == "timeout":
            raise AirflowException(event.get("message", "Fetch timed out"))

        raise AirflowException(f"Unexpected trigger event status: {status}")


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    dag_id="dag2_api_fanout",
    description="API Fan-Out: async fetch via deferrable operator, fan-out detail requests, combine",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "orchestration",
        "retries": 3,
        "retry_delay": timedelta(seconds=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=1),
    },
    params={
        "url": "https://api.github.com/orgs/apache/repos",
    },
    tags=["api", "fanout", "deferrable", "async"],
)
def api_fanout_pipeline():

    # ------------------------------------------------------------------
    # Step 1+2: Submit async fetch and wait via deferrable operator
    # ------------------------------------------------------------------
    submit_and_wait = AsyncFetchOperator(
        task_id="submit_and_wait_for_fetch",
        url="{{ params.url }}",
        fetch_service_url=FETCH_SERVICE_URL,
        poll_interval=5.0,
        fetch_timeout=120.0,
    )

    # ------------------------------------------------------------------
    # Step 3: Normalise the fetch result
    # ------------------------------------------------------------------
    @task()
    def process_fetch_result(fetch_event: dict) -> dict:
        """Normalise the callback payload into a list of items."""
        status = fetch_event.get("status")
        if status != "completed":
            raise AirflowException(
                f"Fetch returned status '{status}': {fetch_event.get('error', 'unknown')}"
            )

        body = fetch_event.get("body")
        if body is None:
            raise AirflowException("Fetch callback contained no body")

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

        return {
            "source_url": fetch_event.get("url", "unknown"),
            "item_count": len(items),
            "items": items,
        }

    # ------------------------------------------------------------------
    # Step 4: Branch -- are there items to process?
    # ------------------------------------------------------------------
    @task.branch()
    def check_items_exist(processed: dict) -> str:
        """Branch: fan out if items exist, otherwise skip."""
        if processed.get("items"):
            return "fan_out_api_requests"
        return "no_items_to_process"

    # ------------------------------------------------------------------
    # Step 5: Fan-out -- fetch detail for each item in parallel
    # ------------------------------------------------------------------
    @task()
    def fan_out_api_requests(item: dict) -> dict:
        """Fetch detail for a single item."""
        detail_url = item.get("detail_url")
        if not detail_url:
            return {
                "id": item.get("id"),
                "name": item.get("name"),
                "error": "No detail_url provided",
            }

        resp = requests.get(
            detail_url,
            headers={"User-Agent": "orchestration-bakeoff/1.0"},
            timeout=30,
        )

        if resp.status_code != 200:
            raise AirflowException(
                f"Detail request for {item.get('id')} failed with {resp.status_code}: "
                f"{resp.text[:500]}"
            )

        return {
            "id": item.get("id"),
            "name": item.get("name"),
            "detail": resp.json(),
        }

    # ------------------------------------------------------------------
    # Step 6: Combine all fan-out results
    # ------------------------------------------------------------------
    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def combine_results(api_results: list[dict], processed: dict) -> dict:
        """Merge all fan-out results into a summary."""
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
            "source_url": processed.get("source_url", "unknown"),
            "total_items": len(api_results),
            "successful": len(combined),
            "failed": len(errors),
            "results": combined,
            "errors": errors,
        }

    @task()
    def no_items_to_process() -> dict:
        """Terminal task when the fetch returned no items."""
        return {
            "status": "no_items",
            "message": "No items found from initial content fetch.",
        }

    # ------------------------------------------------------------------
    # Wire the DAG
    # ------------------------------------------------------------------
    fetch_event = submit_and_wait.output
    processed = process_fetch_result(fetch_event)
    branch = check_items_exist(processed)

    # Fan-out path
    items_list = processed["items"]
    detail_results = fan_out_api_requests.expand(item=items_list)
    final = combine_results(api_results=detail_results, processed=processed)

    # No-items path
    empty = no_items_to_process()

    branch >> [detail_results, empty]


api_fanout_pipeline()

"""
DAG 2: API Fan-Out with Async Callback — Luigi Implementation

Pipeline: FetchContent (poll) -> FanOutAPIRequests (one FetchItemDetail per item) -> CombineResults

Mirrors the Step Functions implementation in step-functions/dag2-api-fanout/.

MAJOR DIVERGENCES FROM STEP FUNCTIONS:
- Luigi has no suspend/resume mechanism. Step Functions uses .waitForTaskToken
  to suspend the execution (zero cost, no resources held) until a callback
  arrives. Luigi must poll in a blocking loop, holding a worker thread for
  the entire duration.
- Luigi has no native callback/webhook support. The polling loop in FetchContent
  is a workaround that is fundamentally less efficient than Step Functions'
  event-driven callback pattern.
- Step Functions Map state supports MaxConcurrency=20 with automatic per-item
  retry (IntervalSeconds=2, MaxAttempts=3, BackoffRate=2.0, JitterStrategy=FULL).
  Luigi fan-out is controlled by --workers N and has no per-task retry.
- Step Functions Choice state (CheckItemsExist) is handled here by conditional
  logic in Python rather than a declarative state.

Run with:
    luigi --module dag2_api_fanout CombineResults \
        --url https://api.example.com/items \
        --run-id my-run-001 \
        --workers 8

    Use --workers to control how many FetchItemDetail tasks run in parallel.
"""

import json
import os
import time
import uuid

import luigi
import urllib3


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CALLBACK_FETCH_SERVICE_URL = os.environ.get(
    "CALLBACK_FETCH_SERVICE_URL", "http://callback-fetch-service:8090"
)

MARKER_DIR = os.environ.get("LUIGI_MARKER_DIR", "/tmp/luigi-markers/dag2")

http = urllib3.PoolManager()


# ---------------------------------------------------------------------------
# Task 1: FetchContent (blocking poll — no callback support)
# ---------------------------------------------------------------------------


class FetchContent(luigi.Task):
    """
    POSTs a fetch request to the callback-fetch-service, then polls for
    completion.

    DIVERGENCE: Luigi has no suspend/resume mechanism. Step Functions uses
    .waitForTaskToken to suspend the execution at zero cost until a callback
    arrives from the fetch service. Here, the Luigi worker is BLOCKED for the
    entire polling duration (up to 60 seconds). This means:
      - A worker thread is consumed doing nothing but sleeping and polling.
      - If --workers is low, this blocks other tasks from running.
      - There is no heartbeat mechanism to detect stale executions.
      - In production with many concurrent fetch requests, this wastes
        significant resources compared to the event-driven Step Functions model.
    """

    url = luigi.Parameter(description="URL to fetch content from")
    run_id = luigi.Parameter(description="Unique run identifier")
    api_key = luigi.Parameter(default="", description="Optional API key for the fetch")
    poll_interval = luigi.IntParameter(default=5, description="Seconds between polls")
    poll_timeout = luigi.IntParameter(default=60, description="Max seconds to wait")

    def output(self):
        return luigi.LocalTarget(
            os.path.join(MARKER_DIR, self.run_id, "fetch_content.json")
        )

    def run(self):
        correlation_id = str(uuid.uuid4())

        # Build the fetch request — mirrors submit_async_fetch.py
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "orchestration-bakeoff/1.0",
        }

        fetch_headers = {}
        if self.api_key:
            fetch_headers["Authorization"] = f"Bearer {self.api_key}"

        # POST the async fetch request.
        # NOTE: We do NOT provide a callback_url because Luigi cannot receive
        # callbacks. Instead we will poll the status endpoint.
        payload = {
            "url": self.url,
            "headers": fetch_headers,
            "correlation_id": correlation_id,
        }

        response = http.request(
            "POST",
            f"{CALLBACK_FETCH_SERVICE_URL}/fetch-async",
            body=json.dumps(payload),
            headers=headers,
            timeout=10.0,
        )

        if response.status != 202:
            raise Exception(
                f"Callback Fetch Service returned {response.status}: "
                f"{response.data.decode('utf-8')[:500]}"
            )

        # ---------------------------------------------------------------
        # DIVERGENCE: Blocking poll loop.
        # Step Functions suspends at zero cost here. Luigi blocks a worker.
        # ---------------------------------------------------------------
        start_time = time.time()

        while True:
            elapsed = time.time() - start_time
            if elapsed > self.poll_timeout:
                raise TimeoutError(
                    f"Fetch did not complete within {self.poll_timeout}s "
                    f"(correlation_id={correlation_id})"
                )

            time.sleep(self.poll_interval)

            status_response = http.request(
                "GET",
                f"{CALLBACK_FETCH_SERVICE_URL}/status/{correlation_id}",
                headers={"User-Agent": "orchestration-bakeoff/1.0"},
                timeout=10.0,
            )

            if status_response.status == 200:
                status_data = json.loads(status_response.data.decode("utf-8"))

                if status_data.get("status") == "completed":
                    break
                elif status_data.get("status") == "failed":
                    raise Exception(
                        f"Fetch failed: {status_data.get('error', 'unknown')}"
                    )
                # else: still in progress, continue polling

        # ---------------------------------------------------------------
        # Process the fetch result — mirrors process_fetch_result.py
        # ---------------------------------------------------------------
        body = status_data.get("body")
        if body is None:
            raise Exception("Fetch service returned no body")

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

        result = {
            "source_url": self.url,
            "item_count": len(items),
            "items": items,
            "correlation_id": correlation_id,
        }

        os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
        with self.output().open("w") as f:
            json.dump(result, f)


# ---------------------------------------------------------------------------
# Task 2a: FetchItemDetail (one per item)
# ---------------------------------------------------------------------------


class FetchItemDetail(luigi.Task):
    """
    Fetches detailed information for a single item.

    DIVERGENCE: Step Functions retries this with IntervalSeconds=2,
    MaxAttempts=3, BackoffRate=2.0, JitterStrategy=FULL. Luigi has no
    built-in retry mechanism. We implement a simple manual retry loop.
    """

    item_json = luigi.Parameter(description="JSON-serialized item dict")
    run_id = luigi.Parameter()
    api_key = luigi.Parameter(default="")
    max_retries = luigi.IntParameter(default=3)

    def output(self):
        item = json.loads(self.item_json)
        item_id = str(item.get("id", "unknown"))
        # Sanitize the item ID for use in filenames
        safe_id = "".join(c if c.isalnum() or c in "-_" else "_" for c in item_id)
        return luigi.LocalTarget(
            os.path.join(MARKER_DIR, self.run_id, f"item_detail_{safe_id}.json")
        )

    def run(self):
        item = json.loads(self.item_json)
        detail_url = item["detail_url"]

        headers = {"User-Agent": "orchestration-bakeoff/1.0"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        # Manual retry loop — Luigi has no built-in retry with backoff.
        last_exception = None
        for attempt in range(self.max_retries):
            try:
                response = http.request(
                    "GET", detail_url, headers=headers, timeout=30.0
                )

                if response.status == 200:
                    detail = json.loads(response.data.decode("utf-8"))
                    result = {
                        "id": item["id"],
                        "name": item["name"],
                        "detail": detail,
                    }

                    os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
                    with self.output().open("w") as f:
                        json.dump(result, f)
                    return

                raise Exception(
                    f"Detail API for {item['id']} returned {response.status}: "
                    f"{response.data.decode('utf-8')[:500]}"
                )

            except Exception as e:
                last_exception = e
                if attempt < self.max_retries - 1:
                    # Simple exponential backoff: 2s, 4s, 8s...
                    time.sleep(2 ** (attempt + 1))

        raise last_exception


# ---------------------------------------------------------------------------
# Task 2b: FanOutAPIRequests (dynamic fan-out over items)
# ---------------------------------------------------------------------------


class FanOutAPIRequests(luigi.Task):
    """
    Reads the item list from FetchContent and creates one FetchItemDetail
    task per item.

    DIVERGENCE: Step Functions Map state handles this declaratively with
    MaxConcurrency=20 and per-item error isolation. In Luigi, if any single
    FetchItemDetail fails, this entire task fails (there is no partial
    success / error collection like Step Functions provides with
    ToleratedFailurePercentage). Parallelism depends on --workers.
    """

    url = luigi.Parameter()
    run_id = luigi.Parameter()
    api_key = luigi.Parameter(default="")

    def requires(self):
        return FetchContent(
            url=self.url, run_id=self.run_id, api_key=self.api_key
        )

    def output(self):
        return luigi.LocalTarget(
            os.path.join(MARKER_DIR, self.run_id, "fan_out_complete.json")
        )

    def run(self):
        with self.input().open("r") as f:
            fetch_result = json.load(f)

        items = fetch_result.get("items", [])

        # Handle no-items case — mirrors CheckItemsExist Choice state
        if not items:
            os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
            with self.output().open("w") as f:
                json.dump(
                    {
                        "status": "no_items",
                        "message": "No items found from initial content fetch.",
                    },
                    f,
                )
            return

        # Yield one FetchItemDetail per item — Luigi schedules them
        # and runs up to --workers in parallel.
        detail_tasks = [
            FetchItemDetail(
                item_json=json.dumps(item),
                run_id=self.run_id,
                api_key=self.api_key,
            )
            for item in items
        ]
        yield detail_tasks

        # Collect results
        api_results = []
        for task in detail_tasks:
            with task.output().open("r") as f:
                api_results.append(json.load(f))

        result = {
            "source_url": fetch_result.get("source_url"),
            "item_count": len(items),
            "api_results": api_results,
        }

        os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
        with self.output().open("w") as f:
            json.dump(result, f)


# ---------------------------------------------------------------------------
# Task 3: CombineResults
# ---------------------------------------------------------------------------


class CombineResults(luigi.Task):
    """
    Merges all fan-out API results into a single summary.

    Mirrors step-functions/dag2-api-fanout/lambdas/combine_results.py.
    """

    url = luigi.Parameter()
    run_id = luigi.Parameter()
    api_key = luigi.Parameter(default="")

    def requires(self):
        return FanOutAPIRequests(
            url=self.url, run_id=self.run_id, api_key=self.api_key
        )

    def output(self):
        return luigi.LocalTarget(
            os.path.join(MARKER_DIR, self.run_id, "combined_results.json")
        )

    def run(self):
        with self.input().open("r") as f:
            fan_out_result = json.load(f)

        # Short-circuit if there were no items
        if fan_out_result.get("status") == "no_items":
            os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
            with self.output().open("w") as f:
                json.dump(fan_out_result, f)
            return

        api_results = fan_out_result.get("api_results", [])
        source_url = fan_out_result.get("source_url", "unknown")

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

        output = {
            "status": "success",
            "source_url": source_url,
            "total_items": len(api_results),
            "successful": len(combined),
            "failed": len(errors),
            "results": combined,
            "errors": errors,
        }

        os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
        with self.output().open("w") as f:
            json.dump(output, f)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    luigi.run()

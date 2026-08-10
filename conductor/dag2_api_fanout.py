"""
DAG 2: API Fan-Out with Async Callback (Conductor)

Delegate a fetch to an external service, SUSPEND until it calls back, branch on
whether any items came back, fan out one detail request per item, and combine.

Conductor idioms demonstrated:
- `WAIT`: a system task that occupies no worker and no thread. The workflow sits
  in IN_PROGRESS until something outside completes the task by reference name.
  This is the whole point of the DAG and Conductor does it with one JSON stanza.
- Resume is a plain, unauthenticated HTTP POST to
  `/api/tasks/{workflowId}/{taskRefName}/COMPLETED`, whose body becomes the
  task's output. No SDK, no token, no relay process -- the mock service does it
  directly (`shared-services/callback-fetch-service/app.py`, `conductor`
  provider). Contrast Kestra, which needed auth + multipart, and Hatchet, which
  needed a whole relay process because it can't be an HTTP callback target.
- `SWITCH` on a worker-computed value for the conditional branch.
- `FORK_JOIN_DYNAMIC` again for the per-item fan-out.

The workflow id IS the resume handle: `${workflow.workflowId}` is passed to the
fetch service at registration time, so the service can address the exact task.
"""

import os
import random
import time

import requests
from conductor.client.worker.worker_task import worker_task

CALLBACK_FETCH_SERVICE_URL = os.environ.get(
    "CALLBACK_FETCH_SERVICE_URL", "http://callback-fetch-service:8090"
)
# Where the *fetch service container* should send its resume. It is a container,
# so this is a compose DNS name -- NOT the localhost:8000 the host workers use.
CONDUCTOR_INTERNAL_URL = os.environ.get(
    "CONDUCTOR_INTERNAL_URL", "http://conductor-server:8080"
)

# The collection is fetched BY THE FETCH SERVICE (a container), so the default
# host is the compose DNS name. See CLAUDE.md: the detail URLs are fetched by
# our own workers instead, and those run on the host -- which is why
# `?base=` gets appended below.
FIXTURE_INTERNAL_URL = os.environ.get(
    "FIXTURE_INTERNAL_URL", "http://fixture-service:8099"
)
FIXTURE_SERVICE_URL = os.environ.get("FIXTURE_SERVICE_URL", "http://localhost:8099")


# ---- tasks -----------------------------------------------------------------


@worker_task(task_definition_name="submit_async_fetch")
def submit_async_fetch(
    workflow_id: str,
    correlation_id: str,
    fetch_url: str = "",
    per_page: int = 5,
    auto_resume: bool = True,
) -> dict:
    """POST to the fetch service, handing it the handle it needs to resume us.

    `resume_data` is the provider-shaped blob described in RUNNING.md §2b. For
    Conductor it is simply (workflow id, task reference name) -- the pair that
    uniquely addresses a blocked task. There is no capability token, because the
    API needs no credentials at all.
    """
    url = fetch_url or f"{FIXTURE_INTERNAL_URL}/books?per_page={per_page}"

    resp = requests.post(
        f"{CALLBACK_FETCH_SERVICE_URL}/fetch-async",
        json={
            "url": url,
            "correlation_id": correlation_id,
            "provider": "conductor",
            "resume_data": {
                "workflow_id": workflow_id,
                "task_ref_name": "wait_for_callback",
                "base_url": CONDUCTOR_INTERNAL_URL,
            },
            "auto_resume": auto_resume,
        },
        timeout=15,
    )
    resp.raise_for_status()
    return {"correlation_id": correlation_id, "submitted": True, "fetch_url": url}


@worker_task(task_definition_name="process_fetch_result")
def process_fetch_result(callback_payload: dict) -> dict:
    """Normalise whatever the callback delivered into a flat list of items.

    `/books` returns a BARE ARRAY (with X-Total-Count / Link headers) precisely
    so `isinstance(body, list)` normalisers keep working, but accept the
    enveloped shapes too so the DAG survives a different fixture.
    """
    body = callback_payload.get("body")

    if isinstance(body, list):
        raw = body
    elif isinstance(body, dict):
        raw = body.get("items") or body.get("docs") or body.get("results") or []
    else:
        raw = []

    items = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        detail_url = entry.get("url") or entry.get("detail_url")
        if not detail_url:
            continue
        items.append(
            {
                # Every tool's normaliser reads this same fallback chain, so the
                # fixture can change shape without breaking any of them.
                "name": entry.get("title") or entry.get("name") or entry.get("id"),
                "url": detail_url,
            }
        )

    return {
        "items": items,
        "item_count": len(items),
        # SWITCH cannot evaluate `len(...)`; it switches on a value. So the
        # branch key is computed here, as a string, and the JSON just routes on
        # it. Conductor's expression language is deliberately thin -- decisions
        # get made in workers.
        "has_items": "yes" if items else "no",
        "fetch_status": callback_payload.get("status"),
    }


@worker_task(task_definition_name="prepare_detail_fanout")
def prepare_detail_fanout(items: list[dict]) -> dict:
    """Build the FORK_JOIN_DYNAMIC payload for one detail fetch per item."""
    dynamic_tasks = []
    dynamic_inputs = {}

    for i, item in enumerate(items):
        ref = f"fetch_detail_{i}"
        dynamic_tasks.append(
            {
                "name": "fetch_item_detail",
                "taskReferenceName": ref,
                "type": "SIMPLE",
            }
        )
        dynamic_inputs[ref] = {"item_name": item["name"], "detail_url": item["url"]}

    return {
        "dynamic_tasks": dynamic_tasks,
        "dynamic_inputs": dynamic_inputs,
        "fanout_width": len(dynamic_tasks),
    }


@worker_task(task_definition_name="fetch_item_detail")
def fetch_item_detail(item_name: str, detail_url: str) -> dict:
    """Fetch one item's detail record, with jittered retries.

    Conductor's own EXPONENTIAL_BACKOFF (taskdefs.json) has no jitter, so the
    in-task loop adds it. A thundering herd of 20 concurrent tasks retrying in
    lockstep is exactly what the spec's "jitter-based retries" guards against.

    Returns a failure record rather than raising, so one dead item cannot fail
    the whole fan-out -- the spec wants success/failure *counts*, not an abort.
    """
    # The fan-out runs on the HOST, so `fixture-service` will not resolve. The
    # service derives detail URLs from the request it received -- which came
    # from the fetch service *container* -- so they point at the compose name
    # and must be rewritten. See CLAUDE.md; this is the easy thing to get wrong.
    url = detail_url.replace(FIXTURE_INTERNAL_URL, FIXTURE_SERVICE_URL)

    last_error = None
    for attempt in range(3):
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            return {"name": item_name, "ok": True, "detail": resp.json()}
        except Exception as e:
            last_error = str(e)
            if attempt < 2:
                time.sleep((2**attempt) + random.uniform(0, 0.5))

    return {"name": item_name, "ok": False, "error": last_error}


@worker_task(task_definition_name="combine_results")
def combine_results(detail_results: dict, item_count: int = 0) -> dict:
    """Merge the fan-out results into one summary.

    A JOIN task's output is a map keyed by each joined task's reference name,
    so this unwraps that map rather than taking a list.
    """
    results = []
    for ref in sorted(detail_results or {}):
        value = detail_results[ref]
        if isinstance(value, dict):
            results.append(value)

    succeeded = [r for r in results if r.get("ok")]
    failed = [r for r in results if not r.get("ok")]

    return {
        "requested": item_count,
        "returned": len(results),
        "success_count": len(succeeded),
        "failure_count": len(failed),
        "titles": [r.get("name") for r in succeeded],
        "failures": [
            {"name": r.get("name"), "error": r.get("error")} for r in failed
        ],
    }

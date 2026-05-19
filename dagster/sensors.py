"""
Sensors for the orchestration bake-off.

fetch_completion_sensor (DAG 2):
    Polls correlation files written by ``submit_async_fetch``.  For each
    pending correlation, GETs /status/<correlation_id> on the Callback Fetch
    Service.  When the status is "completed", triggers ``process_and_fanout_job``
    with the result payload.

approval_sensor (DAG 4):
    Polls approval-request files written by ``check_approval_and_route``.
    For each pending request, GETs /approval-requests/<id> on the Approval
    Service.  When a decision arrives:
      - approved  -> triggers ``order_post_approval_job``
      - rejected/expired -> triggers ``compensation_job``
"""

import json
import os

import requests
from dagster import (
    RunConfig,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)

from .dag2_api_fanout import CORRELATION_DIR, process_and_fanout_job
from .dag4_order_fulfillment import (
    APPROVAL_DIR,
    compensation_job,
    order_post_approval_job,
)
from .resources import HttpClientResource

# ---------------------------------------------------------------------------
# Configuration -- service base URLs (match docker-compose defaults)
# ---------------------------------------------------------------------------

CALLBACK_FETCH_SERVICE_URL = os.environ.get(
    "CALLBACK_FETCH_SERVICE_URL", "http://callback-fetch-service:8090"
)
APPROVAL_SERVICE_URL = os.environ.get(
    "APPROVAL_SERVICE_URL", "http://approval-service:8091"
)

DEFAULT_TIMEOUT = 5.0


# ---------------------------------------------------------------------------
# DAG 2 sensor: fetch completion
# ---------------------------------------------------------------------------


@sensor(
    job=process_and_fanout_job,
    minimum_interval_seconds=10,
    description=(
        "Polls the Callback Fetch Service for completed async fetches.  "
        "When a fetch completes, triggers process_and_fanout_job with the "
        "result payload.  "
        "DIVERGENCE: Dagster cannot suspend a running op to wait for an "
        "external callback.  The workflow is split across two job runs "
        "with this sensor bridging them."
    ),
)
def fetch_completion_sensor(context: SensorEvaluationContext):
    """Scan correlation files and poll the fetch service for each."""
    if not os.path.isdir(CORRELATION_DIR):
        yield SkipReason("No correlation directory found yet")
        return

    files = [
        f
        for f in os.listdir(CORRELATION_DIR)
        if f.endswith(".json")
    ]

    if not files:
        yield SkipReason("No pending correlations")
        return

    triggered = 0

    for filename in files:
        filepath = os.path.join(CORRELATION_DIR, filename)
        try:
            with open(filepath, "r") as f:
                record = json.load(f)
        except (json.JSONDecodeError, IOError) as exc:
            context.log.warning(f"Could not read {filepath}: {exc}")
            continue

        if record.get("status") != "submitted":
            continue

        correlation_id = record["correlation_id"]
        url = record.get("url", "unknown")

        try:
            resp = requests.get(
                f"{CALLBACK_FETCH_SERVICE_URL}/status/{correlation_id}",
                timeout=DEFAULT_TIMEOUT,
            )
        except requests.RequestException as exc:
            context.log.warning(
                f"Could not poll fetch service for {correlation_id}: {exc}"
            )
            continue

        if resp.status_code != 200:
            context.log.debug(
                f"Fetch service returned {resp.status_code} for {correlation_id}"
            )
            continue

        status_data = resp.json()

        if status_data.get("status") != "completed":
            context.log.debug(
                f"Correlation {correlation_id} not yet complete: "
                f"{status_data.get('status')}"
            )
            continue

        context.log.info(
            f"Fetch completed for correlation {correlation_id} -- triggering job"
        )

        # Mark as processed so we don't trigger again
        record["status"] = "processed"
        with open(filepath, "w") as f:
            json.dump(record, f)

        # Build run config for process_and_fanout_job
        fetch_result = {
            "status": "completed",
            "body": status_data.get("body", status_data.get("result", [])),
            "url": url,
            "request_config": record.get("request_config", {}),
        }

        yield RunRequest(
            run_key=f"fetch-{correlation_id}",
            run_config={
                "ops": {
                    "process_fetch_result": {
                        "config": {
                            "fetch_result": fetch_result,
                        }
                    },
                    "combine_results": {
                        "config": {
                            "source_url": url,
                        }
                    },
                }
            },
        )
        triggered += 1

    if triggered == 0:
        yield SkipReason("No completed fetches found this tick")


# ---------------------------------------------------------------------------
# DAG 4 sensor: manager approval
# ---------------------------------------------------------------------------


@sensor(
    jobs=[order_post_approval_job, compensation_job],
    minimum_interval_seconds=10,
    description=(
        "Polls the Approval Service for manager decisions on pending orders.  "
        "When a decision arrives, triggers either the post-approval shipping "
        "job (approved) or the compensation job (rejected/expired).  "
        "DIVERGENCE: Dagster cannot suspend a running op to wait for an "
        "external callback.  The workflow is split across two job runs "
        "with this sensor bridging them."
    ),
)
def approval_sensor(context: SensorEvaluationContext):
    """Scan approval files and poll the approval service for each."""
    if not os.path.isdir(APPROVAL_DIR):
        yield SkipReason("No approval directory found yet")
        return

    files = [f for f in os.listdir(APPROVAL_DIR) if f.endswith(".json")]

    if not files:
        yield SkipReason("No pending approval requests")
        return

    triggered = 0

    for filename in files:
        filepath = os.path.join(APPROVAL_DIR, filename)
        try:
            with open(filepath, "r") as f:
                record = json.load(f)
        except (json.JSONDecodeError, IOError) as exc:
            context.log.warning(f"Could not read {filepath}: {exc}")
            continue

        if record.get("status") != "pending":
            continue

        approval_request_id = record["approval_request_id"]
        order_id = record["order_id"]
        order_data = record.get("order_data", {})

        try:
            resp = requests.get(
                f"{APPROVAL_SERVICE_URL}/approval-requests/{approval_request_id}",
                timeout=DEFAULT_TIMEOUT,
            )
        except requests.RequestException as exc:
            context.log.warning(
                f"Could not poll approval service for {approval_request_id}: {exc}"
            )
            continue

        if resp.status_code != 200:
            context.log.debug(
                f"Approval service returned {resp.status_code} for "
                f"{approval_request_id}"
            )
            continue

        approval_data = resp.json()
        decision = approval_data.get("decision", approval_data.get("status"))

        if decision == "pending":
            context.log.debug(
                f"Approval {approval_request_id} still pending"
            )
            continue

        context.log.info(
            f"Approval decision for {approval_request_id}: {decision}"
        )

        # Mark as processed
        record["status"] = "processed"
        record["decision"] = decision
        with open(filepath, "w") as f:
            json.dump(record, f)

        if decision == "approved":
            # Trigger post-approval job (shipping -> update -> notify)
            items = order_data.get("items", [])
            shipping_address = order_data.get("shipping_address", {})

            yield RunRequest(
                run_key=f"approval-{approval_request_id}-ship",
                job_name="order_post_approval_job",
                run_config={
                    "ops": {
                        "call_shipping_api": {
                            "config": {
                                "order_id": order_id,
                                "items": items,
                                "shipping_address": shipping_address,
                            }
                        },
                        "update_order_status": {
                            "config": {
                                "order_id": order_id,
                                "status": "shipped",
                                "shipment_id": "",
                                "tracking_number": "",
                                "failure_reason": "",
                            }
                        },
                        "send_order_notification": {
                            "config": {
                                "order_id": order_id,
                                "status": "shipped",
                                "tracking_number": "",
                                "carrier": "",
                                "estimated_delivery": "",
                                "failure_reason": "",
                            }
                        },
                    }
                },
            )
        else:
            # Rejected or expired: trigger compensation
            failure_reason = approval_data.get(
                "reason", f"Order {decision} by manager"
            )

            yield RunRequest(
                run_key=f"approval-{approval_request_id}-compensate",
                job_name="compensation_job",
                run_config={
                    "ops": {
                        "release_inventory": {
                            "config": {
                                "order_id": order_id,
                            }
                        },
                        "update_order_cancelled": {
                            "config": {
                                "order_id": order_id,
                                "failure_reason": failure_reason,
                            }
                        },
                        "send_cancellation_notification": {
                            "config": {
                                "order_id": order_id,
                                "failure_reason": failure_reason,
                            }
                        },
                    }
                },
            )

        triggered += 1

    if triggered == 0:
        yield SkipReason("No approval decisions found this tick")

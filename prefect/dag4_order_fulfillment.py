"""
DAG 4: Order Fulfillment with Saga Compensation
=================================================
Validate an order, reserve inventory (sub-flow), optionally request manager
approval (sub-flow with polling wait), ship the order (sub-flow with retries),
and notify.  On any failure after inventory is reserved, run saga compensation
to release inventory and cancel the order.

Prefect 3.x implementation using @flow (parent + sub-flows), @task,
try/except for saga compensation, and a compensations list executed in
reverse order.

NOTE ON ASYNC WAIT STRATEGY
----------------------------
In production, the manager-approval sub-flow would use Prefect's native
``pause_flow_run(timeout=120)`` so the worker is freed while waiting for the
human decision.  The approval-service callback would POST to
``/api/flow_runs/<run_id>/resume``.

For this bake-off we use a **polling approach**: the sub-flow polls
``GET /approval-requests/<id>/status`` on the approval-service every 5 seconds
until a decision arrives or the timeout expires.
"""

import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import httpx
import psycopg2
from prefect import flow, get_run_logger, task
from prefect.deployments import run_deployment
from prefect.exceptions import FlowPauseTimeout
from prefect.flow_runs import pause_flow_run, suspend_flow_run
from prefect.runtime import flow_run as runtime_flow_run

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": int(os.environ.get("POSTGRES_PORT", "5432")),
    "database": os.environ.get("POSTGRES_DB", "orchestration"),
    "user": os.environ.get("POSTGRES_USER", "orchestration"),
    "password": os.environ.get("POSTGRES_PASSWORD", "orchestration"),
}

APPROVAL_SERVICE_URL = os.environ.get(
    "APPROVAL_SERVICE_URL", "http://approval-service:8091"
)
SHIPPING_SERVICE_URL = os.environ.get(
    "SHIPPING_SERVICE_URL", "http://shipping-service:8092"
)

# Per-(runner, DAG) schema isolation -- see shared-services/init-db.sql.
BAKEOFF_NS = os.environ.get("BAKEOFF_NS", "prefect")
BAKEOFF_SCHEMA = f"{BAKEOFF_NS}_dag4"

# How the manager-approval wait is implemented:
#   "suspend" -- suspend_flow_run(): the approval flow's PROCESS EXITS while
#                waiting and is rescheduled on resume. Zero resource cost, but
#                requires (a) a deployment and (b) the approval flow to be a
#                TOP-LEVEL run, because Prefect raises "Cannot suspend subflows".
#                So this mode invokes it via run_deployment() instead of as an
#                in-process subflow. See serve_dag4.py.
#   "pause"   -- pause_flow_run(): process stays alive polling for the resume.
#                Native suspend/resume semantics, no deployment needed.
#   "poll"    -- loop on GET /approval-requests/<id>. Kept for A/B comparison.
APPROVAL_WAIT_MODE = os.environ.get("APPROVAL_WAIT_MODE", "pause").lower()

# Deployment invoked for the approval step when APPROVAL_WAIT_MODE=suspend.
MANAGER_APPROVAL_DEPLOYMENT = os.environ.get(
    "MANAGER_APPROVAL_DEPLOYMENT", "manager_approval_flow/dag4-manager-approval"
)

# Base Prefect API URL as seen FROM the approval-service CONTAINER, used to build
# the resume callback. This is NOT localhost: localhost inside the container is
# the container. Use the runtime's host-gateway hostname (RUNNING.md §2) --
# host.containers.internal on Podman, host.docker.internal on Docker/finch. The
# server must also be bound to 0.0.0.0, not 127.0.0.1, to accept it.
PREFECT_RESUME_API_URL = os.environ.get(
    "PREFECT_RESUME_API_URL", "http://host.containers.internal:4200/api"
)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class OrderValidationFailed(Exception):
    """Order did not pass validation checks."""


class OrderRejected(Exception):
    """Manager rejected the order."""


class ApprovalTimeout(Exception):
    """Approval was not received within the timeout window."""


class ShippingError(Exception):
    """Non-retriable shipping error (e.g. invalid address)."""


class ShippingTransientError(Exception):
    """Retriable shipping error (timeout / 5xx)."""


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _get_db_connection(db_config: dict | None = None):
    """Connection scoped to this runner's DAG 4 schema (``<BAKEOFF_NS>_dag4``),
    which holds the seeded customers/inventory this DAG validates against."""
    cfg = db_config or DB_CONFIG
    conn = psycopg2.connect(
        host=cfg["host"],
        port=cfg.get("port", 5432),
        dbname=cfg["database"],
        user=cfg["user"],
        password=cfg["password"],
    )
    schema = cfg.get("schema", BAKEOFF_SCHEMA)
    with conn.cursor() as cur:
        # Seeded schema, so it must already exist. SET search_path to a missing
        # schema succeeds silently, so check up front and name the fix.
        cur.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name = %s", (schema,))
        if cur.fetchone() is None:
            raise RuntimeError(
                f'schema "{schema}" does not exist -- seed it with: '
                f"psql -c \"SELECT bootstrap_bakeoff('{BAKEOFF_NS}');\""
            )
        cur.execute(f'SET search_path TO "{schema}"')
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(
    retries=3,
    retry_delay_seconds=[2, 4, 8],
    name="validate_order",
)
def validate_order(
    order_id: str,
    customer_id: str,
    items: list[dict],
    approval_threshold: float = 500.00,
    db_config: dict | None = None,
) -> dict:
    """
    Validate the order: customer active, all SKUs exist with enough stock.
    Returns the computed total_amount and validation result.
    Read-only -- no compensation needed on failure.
    """
    logger = get_run_logger()
    conn = _get_db_connection(db_config)
    try:
        with conn.cursor() as cur:
            # Customer check
            cur.execute(
                "SELECT status FROM customers WHERE customer_id = %s",
                (customer_id,),
            )
            row = cur.fetchone()
            if not row:
                return {"is_valid": False, "reason": f"Customer {customer_id} not found"}
            if row[0] != "active":
                return {"is_valid": False, "reason": f"Customer {customer_id} is {row[0]}"}

            # SKU & stock check
            total_amount = 0.0
            for item in items:
                sku = item["sku"]
                quantity = item["quantity"]
                cur.execute(
                    "SELECT available_quantity, unit_price FROM inventory WHERE sku = %s",
                    (sku,),
                )
                row = cur.fetchone()
                if not row:
                    return {"is_valid": False, "reason": f"SKU {sku} not found"}
                available, unit_price = row
                if available < quantity:
                    return {
                        "is_valid": False,
                        "reason": (
                            f"Insufficient stock for {sku}: "
                            f"requested {quantity}, available {available}"
                        ),
                    }
                total_amount += float(unit_price) * quantity

    finally:
        conn.close()

    logger.info("Order %s validated — total_amount=%.2f", order_id, total_amount)
    return {
        "is_valid": True,
        "reason": None,
        "total_amount": total_amount,
        "approval_threshold": approval_threshold,
    }


@task(
    retries=3,
    retry_delay_seconds=[2, 4, 8],
    name="reserve_inventory_task",
)
def reserve_inventory_task(
    order_id: str,
    customer_id: str,
    items: list[dict],
    db_config: dict | None = None,
) -> dict:
    """Atomically reserve inventory for all order items."""
    logger = get_run_logger()
    reservation_id = f"RES-{uuid.uuid4().hex[:12].upper()}"

    conn = _get_db_connection(db_config)
    try:
        with conn.cursor() as cur:
            # Idempotency check
            cur.execute(
                "SELECT reservation_id FROM inventory_reservations "
                "WHERE order_id = %s AND status = 'reserved' LIMIT 1",
                (order_id,),
            )
            existing = cur.fetchone()
            if existing:
                logger.info("Reservation already exists for order %s (idempotent)", order_id)
                return {
                    "reservation_id": existing[0],
                    "items_reserved": [i["sku"] for i in items],
                    "reserved_at": datetime.now(timezone.utc).isoformat(),
                    "idempotent": True,
                }

            # Upsert the order row FIRST: inventory_reservations.order_id has a
            # non-deferrable FK to orders(order_id), so the reservation rows
            # below cannot be inserted until the order exists.
            total = sum(
                i["quantity"] * i.get("unit_price", 0) for i in items
            )
            cur.execute(
                """
                INSERT INTO orders (order_id, customer_id, total_amount, status)
                VALUES (%s, %s, %s, 'reserved')
                ON CONFLICT (order_id) DO UPDATE
                    SET status = 'reserved', updated_at = NOW()
                """,
                (order_id, customer_id, total),
            )

            items_reserved = []
            for item in items:
                sku = item["sku"]
                quantity = item["quantity"]

                cur.execute(
                    """
                    UPDATE inventory
                    SET available_quantity = available_quantity - %s,
                        reserved_quantity = reserved_quantity + %s
                    WHERE sku = %s AND available_quantity >= %s
                    RETURNING sku
                    """,
                    (quantity, quantity, sku, quantity),
                )
                if cur.fetchone() is None:
                    conn.rollback()
                    raise RuntimeError(
                        f"InsufficientStock: Cannot reserve {quantity} of {sku}"
                    )

                cur.execute(
                    """
                    INSERT INTO inventory_reservations
                        (reservation_id, order_id, sku, quantity, status)
                    VALUES (%s, %s, %s, %s, 'reserved')
                    """,
                    (f"{reservation_id}-{sku}", order_id, sku, quantity),
                )
                items_reserved.append(sku)

        conn.commit()
        logger.info(
            "Reserved inventory for order %s — reservation_id=%s",
            order_id,
            reservation_id,
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return {
        "reservation_id": reservation_id,
        "items_reserved": items_reserved,
        "reserved_at": datetime.now(timezone.utc).isoformat(),
    }


@task(
    retries=5,
    retry_delay_seconds=[3, 6, 12, 24, 48],
    name="release_inventory",
)
def release_inventory(
    order_id: str,
    db_config: dict | None = None,
) -> dict:
    """Saga compensation: release all reserved inventory for the order. Idempotent."""
    logger = get_run_logger()
    conn = _get_db_connection(db_config)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT reservation_id, sku, quantity
                FROM inventory_reservations
                WHERE order_id = %s AND status = 'reserved'
                """,
                (order_id,),
            )
            reservations = cur.fetchall()

            if not reservations:
                logger.info("No reservations to release for order %s", order_id)
                return {"order_id": order_id, "released": 0, "status": "no_reservations_to_release"}

            released = 0
            for reservation_id, sku, quantity in reservations:
                cur.execute(
                    """
                    UPDATE inventory
                    SET available_quantity = available_quantity + %s,
                        reserved_quantity = reserved_quantity - %s
                    WHERE sku = %s
                    """,
                    (quantity, quantity, sku),
                )
                cur.execute(
                    """
                    UPDATE inventory_reservations
                    SET status = 'released', released_at = %s
                    WHERE reservation_id = %s AND status = 'reserved'
                    """,
                    (datetime.now(timezone.utc), reservation_id),
                )
                released += 1

        conn.commit()
        logger.info("Released %d reservations for order %s", released, order_id)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return {"order_id": order_id, "released": released, "status": "inventory_released"}


@task(
    retries=3,
    retry_delay_seconds=[2, 4, 8],
    name="submit_approval_request",
)
def submit_approval_request(
    order_id: str,
    customer_id: str,
    total_amount: float,
    items: list[dict],
    db_config: dict | None = None,
    resume_callback_url: str | None = None,
    approval_request_id: str | None = None,
) -> str:
    """POST an approval request to the approval-service. Returns the request id.

    ``approval_request_id`` may be supplied by the caller so the id is known
    before the approval flow starts -- needed in "suspend" mode, where the parent
    invokes the approval flow as a separate deployment run and must be able to
    read the decision back afterwards. It also makes a re-executed submit
    (which happens when a suspended run resumes) reuse the same id.

    ``resume_callback_url`` is registered as the ``http_callback`` provider's
    resume handle: when the manager decides, the service POSTs the decision
    there. In "pause" mode it is Prefect's own resume endpoint, so the decision
    un-pauses this flow run directly.
    """
    logger = get_run_logger()
    approval_request_id = approval_request_id or f"APR-{uuid.uuid4().hex[:12].upper()}"

    items_summary = ", ".join(f"{i['quantity']}x {i['sku']}" for i in items)

    payload = {
        "approval_request_id": approval_request_id,
        "order_id": order_id,
        "total_amount": total_amount,
        "customer_id": customer_id,
        # No resume URL in "poll" mode: the flow polls instead, so nothing should
        # be resumed. The broker still requires a callback_url to infer the
        # provider, hence the deliberately dead placeholder.
        "callback_url": resume_callback_url or "http://localhost:0/noop",
        "items_summary": items_summary,
    }

    with httpx.Client(timeout=10.0) as client:
        response = client.post(
            f"{APPROVAL_SERVICE_URL}/approval-requests",
            json=payload,
            headers={"Content-Type": "application/json"},
        )

    if response.status_code not in (200, 201):
        raise RuntimeError(
            f"Approval service returned {response.status_code}: "
            f"{response.text[:500]}"
        )

    # Record in DB
    conn = _get_db_connection(db_config)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO approval_requests
                    (approval_request_id, order_id, total_amount, status)
                VALUES (%s, %s, %s, 'pending')
                ON CONFLICT (approval_request_id) DO NOTHING
                """,
                (approval_request_id, order_id, total_amount),
            )
            cur.execute(
                "UPDATE orders SET status = 'pending_approval', updated_at = NOW() "
                "WHERE order_id = %s",
                (order_id,),
            )
        conn.commit()
    finally:
        conn.close()

    logger.info(
        "Submitted approval request %s for order %s",
        approval_request_id,
        order_id,
    )
    return approval_request_id


@task(name="read_approval_decision", retries=3, retry_delay_seconds=[1, 2, 4])
def read_approval_decision(approval_request_id: str) -> dict:
    """Single read of the recorded decision, used after a pause-based resume.

    The resume only tells us *that* a decision happened; the decision itself is
    read back from the service. (The alternative -- pause_flow_run(
    wait_for_input=...) -- would carry the decision in the resume payload, but
    requires the caller to POST a Prefect-shaped RunInput, which would couple the
    approval service to Prefect's schema.)
    """
    logger = get_run_logger()
    with httpx.Client(timeout=10.0) as client:
        resp = client.get(
            f"{APPROVAL_SERVICE_URL}/approval-requests/{approval_request_id}"
        )
    resp.raise_for_status()
    body = resp.json()
    status = body.get("status")

    if status not in ("approved", "rejected"):
        # Resumed but nothing decided -- treat as expired rather than hanging.
        logger.warning(
            "Approval %s resumed with status=%s; treating as expired",
            approval_request_id,
            status,
        )
        return {
            "decision": "expired",
            "approver": None,
            "reason": f"Resumed without a decision (status={status})",
            "decided_at": datetime.now(timezone.utc).isoformat(),
        }

    logger.info(
        "Approval %s decided: %s by %s",
        approval_request_id,
        status,
        body.get("approver", "unknown"),
    )
    return {
        "decision": status,
        "approver": body.get("approver"),
        "reason": body.get("reason", ""),
        "decided_at": body.get("decided_at", datetime.now(timezone.utc).isoformat()),
    }


@task(name="poll_approval_decision")
def poll_approval_decision(
    approval_request_id: str,
    poll_interval: int = 5,
    poll_timeout: int = 120,
) -> dict:
    """
    Poll the approval-service for a decision.

    In production this would be replaced by ``pause_flow_run(timeout=120)``
    with the approval-service POSTing to the Prefect resume endpoint.
    """
    logger = get_run_logger()
    deadline = time.monotonic() + poll_timeout

    with httpx.Client(timeout=10.0) as client:
        while time.monotonic() < deadline:
            resp = client.get(
                f"{APPROVAL_SERVICE_URL}/approval-requests/{approval_request_id}"
            )

            if resp.status_code == 200:
                body = resp.json()
                status = body.get("status")
                if status in ("approved", "rejected"):
                    logger.info(
                        "Approval %s decided: %s by %s",
                        approval_request_id,
                        status,
                        body.get("approver", "unknown"),
                    )
                    return {
                        "decision": status,
                        "approver": body.get("approver"),
                        "reason": body.get("reason", ""),
                        "decided_at": body.get("decided_at", datetime.now(timezone.utc).isoformat()),
                    }
                # Still pending
                logger.debug("Approval %s still pending...", approval_request_id)
            else:
                logger.warning(
                    "Unexpected status %d polling approval %s",
                    resp.status_code,
                    approval_request_id,
                )

            time.sleep(poll_interval)

    # Timeout — treat as expired
    logger.warning("Approval %s timed out after %ds", approval_request_id, poll_timeout)
    return {
        "decision": "expired",
        "approver": None,
        "reason": "Approval request timed out",
        "decided_at": datetime.now(timezone.utc).isoformat(),
    }


@task(
    retries=3,
    retry_delay_seconds=[2, 4, 8],
    name="record_approval_decision",
)
def record_approval_decision(
    approval_request_id: str,
    order_id: str,
    decision: dict,
    db_config: dict | None = None,
) -> dict:
    """Persist the approval decision to the database."""
    logger = get_run_logger()
    conn = _get_db_connection(db_config)
    try:
        with conn.cursor() as cur:
            now = datetime.now(timezone.utc)

            cur.execute(
                """
                UPDATE approval_requests
                SET status = %s, approver = %s, reason = %s, decided_at = %s
                WHERE approval_request_id = %s
                """,
                (
                    decision["decision"],
                    decision.get("approver"),
                    decision.get("reason", ""),
                    now,
                    approval_request_id,
                ),
            )

            new_status = "approved" if decision["decision"] == "approved" else "rejected"
            cur.execute(
                "UPDATE orders SET status = %s, updated_at = %s WHERE order_id = %s",
                (new_status, now, order_id),
            )

        conn.commit()
        logger.info("Recorded approval decision for %s: %s", order_id, decision["decision"])
    finally:
        conn.close()

    return decision


@task(
    retries=3,
    retry_delay_seconds=[3, 6, 12],
    name="call_shipping_api",
)
def call_shipping_api(
    order_id: str,
    items: list[dict],
    shipping_address: dict,
) -> dict:
    """Call the shipping service. Raises typed exceptions for error routing."""
    logger = get_run_logger()
    idempotency_key = f"{order_id}-ship"

    payload = {
        "order_id": order_id,
        "items": items,
        "shipping_address": shipping_address,
        "idempotency_key": idempotency_key,
    }

    with httpx.Client(timeout=30.0) as client:
        response = client.post(
            f"{SHIPPING_SERVICE_URL}/shipments",
            json=payload,
            headers={"Content-Type": "application/json"},
        )

    body = response.json()

    if response.status_code == 200:
        logger.info(
            "Shipment created for order %s — tracking=%s",
            order_id,
            body.get("tracking_number"),
        )
        return body

    # Parse error type from the response
    detail = body.get("detail", {})
    if isinstance(detail, dict):
        error_type = detail.get("error_type", "Unknown")
        message = detail.get("message", str(body))
    else:
        error_type = "Unknown"
        message = str(body)

    if error_type == "InvalidAddress":
        raise ShippingError(f"Invalid address: {message}")
    elif error_type == "ShippingTimeout" or response.status_code == 504:
        raise ShippingTransientError(f"Shipping timeout: {message}")
    elif error_type == "ShippingServiceError" or response.status_code >= 500:
        raise ShippingTransientError(f"Shipping service error: {message}")
    else:
        raise ShippingError(f"Unexpected shipping error ({response.status_code}): {message}")


@task(
    retries=3,
    retry_delay_seconds=[2, 4, 8],
    name="update_order_status",
)
def update_order_status(
    order_id: str,
    status: str,
    shipment_id: str | None = None,
    tracking_number: str | None = None,
    failure_reason: str | None = None,
    db_config: dict | None = None,
) -> dict:
    """Update the order record. Used for both success and compensation paths."""
    logger = get_run_logger()
    conn = _get_db_connection(db_config)
    try:
        with conn.cursor() as cur:
            now = datetime.now(timezone.utc)
            cur.execute(
                """
                UPDATE orders
                SET status = %s,
                    shipment_id = COALESCE(%s, shipment_id),
                    tracking_number = COALESCE(%s, tracking_number),
                    failure_reason = COALESCE(%s, failure_reason),
                    updated_at = %s
                WHERE order_id = %s
                RETURNING order_id, status
                """,
                (status, shipment_id, tracking_number, failure_reason, now, order_id),
            )
            result = cur.fetchone()
            conn.commit()

            if not result:
                raise RuntimeError(f"Order {order_id} not found")

            logger.info("Order %s status updated to %s", order_id, status)
    finally:
        conn.close()

    return {"order_id": result[0], "status": result[1], "updated_at": now.isoformat()}


@task(
    retries=3,
    retry_delay_seconds=[2, 4, 8],
    name="send_order_notification",
)
def send_order_notification(
    order_id: str,
    status: str,
    tracking_number: str | None = None,
    carrier: str | None = None,
    estimated_delivery: str | None = None,
    failure_reason: str | None = None,
) -> dict:
    """Send a simulated notification for order status changes."""
    logger = get_run_logger()

    sent_at = datetime.now(timezone.utc).isoformat()

    if status == "shipped":
        message = (
            f"Your order {order_id} has been shipped! "
            f"Tracking: {tracking_number or 'N/A'} via {carrier or 'N/A'}. "
            f"Estimated delivery: {estimated_delivery or 'N/A'}."
        )
    elif status == "cancelled":
        message = (
            f"Your order {order_id} has been cancelled. "
            f"Reason: {failure_reason or 'N/A'}."
        )
    else:
        message = f"Order {order_id} status update: {status}."

    logger.info("NOTIFICATION: %s", message)

    return {
        "notification_sent": True,
        "order_id": order_id,
        "status": status,
        "sent_at": sent_at,
    }


def run_approval_deployment(
    approval_params: dict,
    approval_request_id: str,
    order_id: str,
    timeout: int,
    db_config: dict | None = None,
) -> dict:
    """Run the manager-approval deployment and return its decision.

    Used only in "suspend" mode. ``run_deployment`` blocks until the child run
    finishes -- including across its suspension -- so the CHILD's process exits
    while waiting but this parent process does not. Making the whole chain
    zero-cost would mean suspending the parent too, or splitting it into
    event-triggered deployments.

    The decision is read back from the approval service rather than from the
    child run's return value, so no result-serialization round trip is needed.
    """
    logger = get_run_logger()

    # as_subflow=False is REQUIRED, not cosmetic. run_deployment() defaults to
    # linking the child via parent_task_run_id, and Prefect rejects suspending
    # any run with a parent ("Cannot suspend subflows") -- being a deployment run
    # is not sufficient. The cost is real: severing that link also removes the
    # parent/child nesting from the UI, so DAG 4's sub-workflow lineage is no
    # longer visible as a tree. Zero-cost suspension and composition lineage are
    # mutually exclusive here.
    #
    # +30s so the child's own timeout fires first and it can record "expired"
    # itself, rather than this wait giving up on a still-running child.
    flow_run = run_deployment(
        name=MANAGER_APPROVAL_DEPLOYMENT,
        parameters=approval_params,
        timeout=timeout + 30,
        as_subflow=False,
    )

    state = flow_run.state
    logger.info(
        "Approval deployment run %s finished in state %s",
        flow_run.id,
        state.type if state else "UNKNOWN",
    )

    if state and state.is_completed():
        return read_approval_decision(approval_request_id=approval_request_id)

    # Crashed, timed out, or was cancelled -- fail closed so the saga compensates
    # rather than shipping an order whose approval status is unknown.
    logger.warning(
        "Approval deployment run %s did not complete (state=%s) — treating as expired",
        flow_run.id,
        state.type if state else "UNKNOWN",
    )
    decision = {
        "decision": "expired",
        "approver": None,
        "reason": f"Approval flow did not complete (state={state.type if state else 'UNKNOWN'})",
        "decided_at": datetime.now(timezone.utc).isoformat(),
    }
    # The child records its own decision on the happy path; on this path it
    # could not, so record it here.
    return record_approval_decision(
        approval_request_id=approval_request_id,
        order_id=order_id,
        decision=decision,
        db_config=db_config,
    )


# ---------------------------------------------------------------------------
# Sub-flows
# ---------------------------------------------------------------------------

@flow(name="reserve_inventory_flow")
def reserve_inventory_flow(
    order_id: str,
    customer_id: str,
    items: list[dict],
    db_config: dict | None = None,
) -> dict:
    """Sub-flow: Reserve inventory. Wraps the task for workflow composition."""
    return reserve_inventory_task(
        order_id=order_id,
        customer_id=customer_id,
        items=items,
        db_config=db_config,
    )


# persist_result is required for suspend_flow_run(): the run is rescheduled on
# resume and re-executes, so completed task states must be retrievable.
@flow(name="manager_approval_flow", persist_result=True)
def manager_approval_flow(
    order_id: str,
    customer_id: str,
    total_amount: float,
    items: list[dict],
    db_config: dict | None = None,
    poll_interval: int = 5,
    poll_timeout: int = 120,
    approval_request_id: str | None = None,
) -> dict:
    """
    Sub-flow: Request manager approval and wait for the decision.

    Three wait strategies, selected by ``APPROVAL_WAIT_MODE`` (see module header):

    "suspend" -- ``suspend_flow_run()``: this process EXITS while waiting and is
        rescheduled when the approval service hits the resume endpoint. Zero
        resource cost. Only valid when this flow is a top-level deployment run
        (Prefect: "Cannot suspend subflows"), so the parent invokes it via
        ``run_deployment()``.

    "pause" (default) -- ``pause_flow_run()``: process stays alive polling for
        the resume, so a slot is still held. No deployment required.

    "poll" -- the original loop against GET /approval-requests/<id>.
    """
    logger = get_run_logger()

    use_suspend = APPROVAL_WAIT_MODE == "suspend"
    use_pause = APPROVAL_WAIT_MODE == "pause"

    # For both native modes the resume target is THIS flow run. Read the id from
    # the runtime rather than threading it in, so it is correct whether this flow
    # is an in-process subflow or its own deployment run.
    resume_callback_url = None
    if use_suspend or use_pause:
        this_run_id = runtime_flow_run.id
        resume_callback_url = (
            f"{PREFECT_RESUME_API_URL.rstrip('/')}/flow_runs/{this_run_id}/resume"
        )
        logger.info("Approval will resume flow run %s", this_run_id)

    # Submit the approval request. On a suspend-resume the flow function
    # re-executes from the top; the cached task state means this does not
    # re-submit, and the caller-supplied id keeps it stable regardless.
    approval_request_id = submit_approval_request(
        order_id=order_id,
        customer_id=customer_id,
        total_amount=total_amount,
        items=items,
        db_config=db_config,
        resume_callback_url=resume_callback_url,
        approval_request_id=approval_request_id,
    )

    if use_suspend:
        # Process exits here and is rescheduled on resume. No poll_interval:
        # nothing is polling, the run is simply not running.
        suspend_flow_run(timeout=poll_timeout)
        decision = read_approval_decision(approval_request_id=approval_request_id)
    elif use_pause:
        try:
            # Blocks until the approval service POSTs to the resume endpoint.
            # poll_interval bounds how quickly the resume is noticed.
            pause_flow_run(timeout=poll_timeout, poll_interval=poll_interval)
        except FlowPauseTimeout:
            logger.warning(
                "Approval %s not decided within %ds — treating as expired",
                approval_request_id,
                poll_timeout,
            )
            decision = {
                "decision": "expired",
                "approver": None,
                "reason": "Approval request timed out",
                "decided_at": datetime.now(timezone.utc).isoformat(),
            }
        else:
            decision = read_approval_decision(
                approval_request_id=approval_request_id
            )
    else:
        decision = poll_approval_decision(
            approval_request_id=approval_request_id,
            poll_interval=poll_interval,
            poll_timeout=poll_timeout,
        )

    # Record the decision in the database
    recorded = record_approval_decision(
        approval_request_id=approval_request_id,
        order_id=order_id,
        decision=decision,
        db_config=db_config,
    )

    logger.info("Approval flow complete for order %s: %s", order_id, recorded["decision"])
    return recorded


@flow(
    name="shipping_flow",
    retries=3,
    retry_delay_seconds=5,
)
def shipping_flow(
    order_id: str,
    items: list[dict],
    shipping_address: dict,
) -> dict:
    """Sub-flow: Call shipping API. Flow-level retries for transient errors."""
    return call_shipping_api(
        order_id=order_id,
        items=items,
        shipping_address=shipping_address,
    )


# ---------------------------------------------------------------------------
# Main flow — saga orchestration
# ---------------------------------------------------------------------------

@flow(name="order_fulfillment", log_prints=True)
def order_fulfillment(
    order_id: str | None = None,
    customer_id: str = "CUST-42",
    items: list[dict] | None = None,
    shipping_address: dict | None = None,
    approval_threshold: float = 500.00,
    db_config: dict | None = None,
    approval_timeout: int = 120,
    approval_poll_interval: int = 5,
) -> dict:
    """
    Order fulfillment pipeline with saga compensation:
      1. Validate order (read-only, no compensation needed)
      2. Reserve inventory (sub-flow) — compensation: release_inventory
      3. If total >= threshold: manager approval (sub-flow; pauses or polls)
      4. Ship order (sub-flow with retries)
      5. Update order status & notify

    On failure after inventory is reserved, compensations are executed in
    reverse order to ensure consistency.

    ``order_id`` is the reservation's idempotency key, so it defaults to a fresh
    generated id: a deployment has static parameters, and a constant id would make
    every run after the first an idempotent no-op. ``items`` defaults to
    GADGET-B + WIDGET-A ($529.98) from the bootstrap_bakeoff() seed, which clears
    the $500 threshold and so exercises the approval path.
    """
    logger = get_run_logger()
    cfg = db_config or DB_CONFIG
    order_id = order_id or f"ORD-{uuid.uuid4().hex[:12].upper()}"
    if items is None:
        items = [
            {"sku": "GADGET-B", "quantity": 1, "unit_price": 499.99},
            {"sku": "WIDGET-A", "quantity": 1, "unit_price": 29.99},
        ]
    if shipping_address is None:
        shipping_address = {
            "street": "123 Main St",
            "city": "Springfield",
            "state": "IL",
            "zip": "62701",
            "country": "US",
        }
    compensations: list[tuple[str, Any]] = []

    # ------------------------------------------------------------------
    # Step 1: Validate order (no side effects -> no compensation needed)
    # ------------------------------------------------------------------
    validation = validate_order(
        order_id=order_id,
        customer_id=customer_id,
        items=items,
        approval_threshold=approval_threshold,
        db_config=cfg,
    )

    if not validation["is_valid"]:
        raise OrderValidationFailed(
            f"Order {order_id} failed validation: {validation['reason']}"
        )

    total_amount = validation["total_amount"]

    try:
        # --------------------------------------------------------------
        # Step 2: Reserve inventory (sub-flow)
        # --------------------------------------------------------------
        reservation = reserve_inventory_flow(
            order_id=order_id,
            customer_id=customer_id,
            items=items,
            db_config=cfg,
        )
        compensations.append(
            ("release_inventory", lambda: release_inventory(order_id=order_id, db_config=cfg))
        )

        # --------------------------------------------------------------
        # Step 3: Manager approval (if amount >= threshold)
        # --------------------------------------------------------------
        if total_amount >= approval_threshold:
            logger.info(
                "Order %s total (%.2f) >= threshold (%.2f) — requesting approval",
                order_id,
                total_amount,
                approval_threshold,
            )
            approval_params = {
                "order_id": order_id,
                "customer_id": customer_id,
                "total_amount": total_amount,
                "items": items,
                "db_config": cfg,
                "poll_interval": approval_poll_interval,
                "poll_timeout": approval_timeout,
            }

            if APPROVAL_WAIT_MODE == "suspend":
                # The approval flow must be a top-level run to suspend, so invoke
                # its deployment instead of calling it as a subflow. Its id is
                # fixed up front so the decision can be read back here.
                approval_request_id = f"APR-{uuid.uuid4().hex[:12].upper()}"
                decision = run_approval_deployment(
                    approval_params={
                        **approval_params,
                        "approval_request_id": approval_request_id,
                    },
                    approval_request_id=approval_request_id,
                    order_id=order_id,
                    timeout=approval_timeout,
                    db_config=cfg,
                )
            else:
                decision = manager_approval_flow(**approval_params)

            if decision["decision"] == "rejected":
                raise OrderRejected(
                    f"Order rejected by {decision.get('approver', 'unknown')}: "
                    f"{decision.get('reason', 'no reason given')}"
                )
            elif decision["decision"] == "expired":
                raise ApprovalTimeout("Approval request timed out")

            logger.info("Order %s approved by %s", order_id, decision.get("approver"))
        else:
            logger.info(
                "Order %s total (%.2f) < threshold (%.2f) — no approval needed",
                order_id,
                total_amount,
                approval_threshold,
            )

        # --------------------------------------------------------------
        # Step 4: Ship (sub-flow with retries)
        # --------------------------------------------------------------
        shipment = shipping_flow(
            order_id=order_id,
            items=items,
            shipping_address=shipping_address,
        )

    except Exception as e:
        # ----------------------------------------------------------
        # Saga compensation: execute in reverse order
        # ----------------------------------------------------------
        logger.error("Order %s failed: %s — running compensations", order_id, e)

        for comp_name, comp_fn in reversed(compensations):
            try:
                logger.info("Running compensation: %s", comp_name)
                comp_fn()
            except Exception as comp_err:
                logger.error(
                    "Compensation '%s' failed for order %s: %s",
                    comp_name,
                    order_id,
                    comp_err,
                )

        # Update order status to cancelled
        try:
            update_order_status(
                order_id=order_id,
                status="cancelled",
                failure_reason=str(e),
                db_config=cfg,
            )
        except Exception as status_err:
            logger.error("Failed to update order status to cancelled: %s", status_err)

        # Send cancellation notification (best-effort)
        try:
            send_order_notification(
                order_id=order_id,
                status="cancelled",
                failure_reason=str(e),
            )
        except Exception as notif_err:
            logger.warning("Failed to send cancellation notification: %s", notif_err)

        raise

    # ------------------------------------------------------------------
    # Happy path: Update order & notify
    # ------------------------------------------------------------------
    update_order_status(
        order_id=order_id,
        status="shipped",
        shipment_id=shipment.get("shipment_id"),
        tracking_number=shipment.get("tracking_number"),
        db_config=cfg,
    )

    try:
        send_order_notification(
            order_id=order_id,
            status="shipped",
            tracking_number=shipment.get("tracking_number"),
            carrier=shipment.get("carrier"),
            estimated_delivery=shipment.get("estimated_delivery"),
        )
    except Exception as notif_err:
        # Shipment succeeded — notification failure does not invalidate it
        logger.warning(
            "Order %s shipped successfully but notification failed: %s",
            order_id,
            notif_err,
        )

    return {
        "status": "shipped",
        "order_id": order_id,
        "reservation": reservation,
        "shipment": shipment,
    }


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # All parameters fall back to flow defaults: a generated order_id (so this is
    # re-runnable) for CUST-42 buying GADGET-B + WIDGET-A = $529.98, which clears
    # the $500 threshold and therefore exercises the manager-approval wait.
    #
    # To take other branches, pass explicit values:
    #   items=[{"sku": "THING-C", "quantity": 3, "unit_price": 9.99}]
    #                             -> $29.97, below threshold, skips approval
    #   customer_id="CUST-99"     -> inactive customer, validation fails
    #   items=[{"sku": "RARE-D", "quantity": 2, ...}]
    #                             -> only 2 units exist; last-unit contention
    #   approval_timeout=5        -> beats the service's 10s auto-decide, so the
    #                                approval expires and the saga compensates
    result = order_fulfillment()
    print(json.dumps(result, indent=2, default=str))

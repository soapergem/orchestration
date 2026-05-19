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
    cfg = db_config or DB_CONFIG
    return psycopg2.connect(
        host=cfg["host"],
        port=cfg.get("port", 5432),
        dbname=cfg["database"],
        user=cfg["user"],
        password=cfg["password"],
    )


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

            # Upsert order record
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
) -> str:
    """POST an approval request to the approval-service. Returns the request id."""
    logger = get_run_logger()
    approval_request_id = f"APR-{uuid.uuid4().hex[:12].upper()}"

    items_summary = ", ".join(f"{i['quantity']}x {i['sku']}" for i in items)

    payload = {
        "approval_request_id": approval_request_id,
        "order_id": order_id,
        "total_amount": total_amount,
        "customer_id": customer_id,
        # In production, callback_url would point to Prefect's resume endpoint.
        "callback_url": "http://localhost:0/noop",
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


@flow(name="manager_approval_flow")
def manager_approval_flow(
    order_id: str,
    customer_id: str,
    total_amount: float,
    items: list[dict],
    db_config: dict | None = None,
    poll_interval: int = 5,
    poll_timeout: int = 120,
) -> dict:
    """
    Sub-flow: Request manager approval and wait for the decision.

    Uses polling against the approval-service status endpoint.  In production
    this would use ``pause_flow_run(timeout=120)`` with the approval-service
    callback hitting the Prefect resume API.
    """
    logger = get_run_logger()

    # Submit the approval request
    approval_request_id = submit_approval_request(
        order_id=order_id,
        customer_id=customer_id,
        total_amount=total_amount,
        items=items,
        db_config=db_config,
    )

    # Poll for the decision
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
    order_id: str,
    customer_id: str,
    items: list[dict],
    shipping_address: dict,
    approval_threshold: float = 500.00,
    db_config: dict | None = None,
) -> dict:
    """
    Order fulfillment pipeline with saga compensation:
      1. Validate order (read-only, no compensation needed)
      2. Reserve inventory (sub-flow) — compensation: release_inventory
      3. If total >= threshold: manager approval (sub-flow with polling wait)
      4. Ship order (sub-flow with retries)
      5. Update order status & notify

    On failure after inventory is reserved, compensations are executed in
    reverse order to ensure consistency.
    """
    logger = get_run_logger()
    cfg = db_config or DB_CONFIG
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
            decision = manager_approval_flow(
                order_id=order_id,
                customer_id=customer_id,
                total_amount=total_amount,
                items=items,
                db_config=cfg,
            )

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
    result = order_fulfillment(
        order_id="ORD-001",
        customer_id="CUST-001",
        items=[
            {"sku": "SKU-A", "quantity": 2, "unit_price": 150.00},
            {"sku": "SKU-B", "quantity": 1, "unit_price": 300.00},
        ],
        shipping_address={
            "street": "123 Main St",
            "city": "Springfield",
            "state": "IL",
            "zip": "62701",
            "country": "US",
        },
        approval_threshold=500.00,
    )
    print(json.dumps(result, indent=2, default=str))

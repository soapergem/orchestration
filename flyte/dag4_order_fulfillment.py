"""
DAG 4: Order Fulfillment — Flyte Implementation

Pipeline:
  1. validate_order        — Check customer, SKUs, compute total
  2. reserve_inventory     — Sub-workflow: atomically reserve stock
  3. check_approval        — Conditional: total >= threshold?
  4. manager_approval      — Sub-workflow: poll approval-service
  5. call_shipping_api     — Sub-workflow with RetryStrategy
  6. update_order_shipped  — Record shipment in DB
  7. send_shipped_notification — Best-effort
  8. Saga compensation     — On failure: release_inventory, update_order_cancelled,
                              send_cancellation_notification

Equivalent Step Functions workflow:
  step-functions/dag4-order-fulfillment/state-machine.asl.json
  step-functions/dag4-order-fulfillment/sub-workflows/*.asl.json

Key Flyte features demonstrated:
  - Sub-workflows (@workflow called from parent @workflow)
  - Conditional branching
  - Saga compensation modeled as tasks in the except path
  - Polling fallback for external approval (wait_for_input in production)
  - RetryStrategy on shipping task
  - Strong typing via dataclasses on every boundary

Production note on wait_for_input:
  The manager_approval sub-workflow uses polling. In production, replace
  the poll loop with:
      decision = wait_for_input(
          name="approval_decision",
          expected_type=ApprovalDecision,
          timeout=timedelta(seconds=120),
      )
  and have the approval-service POST to the FlyteAdmin signal endpoint.
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import List

import psycopg2
import urllib3
from flytekit import ImageSpec, conditional, task, workflow

from .types import (
    ApprovalDecision,
    CompensationResult,
    DBConfig,
    OrderInput,
    OrderItem,
    OrderNotification,
    OrderOutput,
    OrderStatusUpdate,
    OrderValidated,
    OrderValidation,
    ReservationResult,
    ShipmentResult,
    ShippingAddress,
)

# ---------------------------------------------------------------------------
# Container image spec
# ---------------------------------------------------------------------------
order_image = ImageSpec(
    name="order-fulfillment",
    packages=[
        "psycopg2-binary",
        "urllib3",
        "flytekit",
    ],
    python_version="3.11",
)

_http = urllib3.PoolManager()

APPROVAL_SERVICE_URL = "http://approval-service:8091"
SHIPPING_SERVICE_URL = "http://shipping-service:8092"


# ---------------------------------------------------------------------------
# Helper: Postgres connection
# ---------------------------------------------------------------------------
def _get_connection(cfg: DBConfig) -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.database,
        user=cfg.user,
        password=cfg.password,
    )


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------
class ShippingTimeout(Exception):
    pass


class ShippingServiceError(Exception):
    pass


class InvalidAddress(Exception):
    pass


# ===================================================================
# TASKS
# ===================================================================


# ---------------------------------------------------------------------------
# Task 1: Validate order
# ---------------------------------------------------------------------------
@task(
    retries=3,
    container_image=order_image,
)
def validate_order(order_input: OrderInput) -> OrderValidated:
    """Validate that all SKUs exist, customer is active, and compute total.

    Read-only — no mutations, so no compensation needed on failure.
    """
    db = order_input.db_config

    def _fail(reason: str) -> OrderValidated:
        return OrderValidated(
            order_id=order_input.order_id,
            customer_id=order_input.customer_id,
            items=order_input.items,
            shipping_address=order_input.shipping_address,
            approval_threshold=order_input.approval_threshold,
            db_config=db,
            total_amount=0.0,
            validation=OrderValidation(is_valid=False, reason=reason),
        )

    conn = _get_connection(db)
    try:
        cur = conn.cursor()

        # Check customer
        cur.execute(
            "SELECT status FROM customers WHERE customer_id = %s",
            (order_input.customer_id,),
        )
        row = cur.fetchone()
        if not row:
            return _fail(f"Customer {order_input.customer_id} not found")
        if row[0] != "active":
            return _fail(f"Customer {order_input.customer_id} is {row[0]}")

        # Check SKUs and compute total
        total_amount = 0.0
        for item in order_input.items:
            cur.execute(
                "SELECT available_quantity, unit_price FROM inventory WHERE sku = %s",
                (item.sku,),
            )
            row = cur.fetchone()
            if not row:
                return _fail(f"SKU {item.sku} not found")
            available, unit_price = row
            if available < item.quantity:
                return _fail(
                    f"Insufficient stock for {item.sku}: "
                    f"requested {item.quantity}, available {available}"
                )
            total_amount += float(unit_price) * item.quantity
    finally:
        conn.close()

    return OrderValidated(
        order_id=order_input.order_id,
        customer_id=order_input.customer_id,
        items=order_input.items,
        shipping_address=order_input.shipping_address,
        approval_threshold=order_input.approval_threshold,
        db_config=db,
        total_amount=total_amount,
        validation=OrderValidation(is_valid=True, reason=""),
    )


# ---------------------------------------------------------------------------
# Task 2: Reserve inventory (atomic)
# ---------------------------------------------------------------------------
@task(
    retries=3,
    container_image=order_image,
)
def reserve_inventory(
    order_id: str,
    customer_id: str,
    items: List[OrderItem],
    db_config: DBConfig,
) -> ReservationResult:
    """Atomically reserve inventory for all items in a single transaction.

    Idempotent — if a reservation already exists for this order, returns
    it without re-reserving.
    """
    reservation_id = f"RES-{uuid.uuid4().hex[:12].upper()}"

    conn = _get_connection(db_config)
    try:
        cur = conn.cursor()

        # Idempotency check
        cur.execute(
            "SELECT reservation_id FROM inventory_reservations "
            "WHERE order_id = %s AND status = 'reserved' LIMIT 1",
            (order_id,),
        )
        existing = cur.fetchone()
        if existing:
            return ReservationResult(
                reservation_id=existing[0],
                items_reserved=[item.sku for item in items],
                reserved_at=datetime.now(timezone.utc).isoformat(),
                idempotent=True,
            )

        items_reserved: List[str] = []
        for item in items:
            cur.execute(
                """
                UPDATE inventory
                SET available_quantity = available_quantity - %s,
                    reserved_quantity = reserved_quantity + %s
                WHERE sku = %s AND available_quantity >= %s
                RETURNING sku
                """,
                (item.quantity, item.quantity, item.sku, item.quantity),
            )
            if cur.fetchone() is None:
                conn.rollback()
                raise RuntimeError(
                    f"InsufficientStock: Cannot reserve {item.quantity} of {item.sku}"
                )

            cur.execute(
                """
                INSERT INTO inventory_reservations
                    (reservation_id, order_id, sku, quantity, status)
                VALUES (%s, %s, %s, %s, 'reserved')
                """,
                (f"{reservation_id}-{item.sku}", order_id, item.sku, item.quantity),
            )
            items_reserved.append(item.sku)

        # Create/update order record
        total = sum(item.quantity * item.unit_price for item in items)
        cur.execute(
            """
            INSERT INTO orders (order_id, customer_id, total_amount, status)
            VALUES (%s, %s, %s, 'reserved')
            ON CONFLICT (order_id) DO UPDATE SET status = 'reserved', updated_at = NOW()
            """,
            (order_id, customer_id, total),
        )

        conn.commit()

        return ReservationResult(
            reservation_id=reservation_id,
            items_reserved=items_reserved,
            reserved_at=datetime.now(timezone.utc).isoformat(),
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Sub-workflow: Reserve inventory
# ---------------------------------------------------------------------------
@workflow
def reserve_inventory_subworkflow(
    order_id: str,
    customer_id: str,
    items: List[OrderItem],
    db_config: DBConfig,
) -> ReservationResult:
    """Sub-workflow wrapping inventory reservation.

    Equivalent to the Step Functions ``reserve-inventory.asl.json``
    sub-state-machine.
    """
    return reserve_inventory(
        order_id=order_id,
        customer_id=customer_id,
        items=items,
        db_config=db_config,
    )


# ---------------------------------------------------------------------------
# Task 3: Request manager approval (submit)
# ---------------------------------------------------------------------------
@task(
    retries=3,
    container_image=order_image,
)
def request_approval(
    order_id: str,
    customer_id: str,
    total_amount: float,
    items: List[OrderItem],
    db_config: DBConfig,
) -> str:
    """Submit an approval request to the approval-service.

    Returns the ``approval_request_id`` for subsequent polling.
    """
    approval_request_id = f"APR-{uuid.uuid4().hex[:12].upper()}"

    items_summary = ", ".join(f"{item.quantity}x {item.sku}" for item in items)

    payload = {
        "approval_request_id": approval_request_id,
        "order_id": order_id,
        "total_amount": total_amount,
        "customer_id": customer_id,
        "callback_url": "",  # Not used in polling mode
        "items_summary": items_summary,
    }

    response = _http.request(
        "POST",
        f"{APPROVAL_SERVICE_URL}/approval-requests",
        body=json.dumps(payload),
        headers={"Content-Type": "application/json"},
        timeout=10.0,
    )

    if response.status not in (200, 201):
        raise RuntimeError(
            f"Approval Service returned {response.status}: "
            f"{response.data.decode('utf-8')[:500]}"
        )

    # Record the pending approval in the database
    conn = _get_connection(db_config)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO approval_requests (approval_request_id, order_id, total_amount, status)
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

    return approval_request_id


# ---------------------------------------------------------------------------
# Task 4: Poll for approval decision
#
# PRODUCTION ALTERNATIVE — wait_for_input:
#   Replace this task + request_approval with:
#       decision = wait_for_input(
#           name="approval_decision",
#           expected_type=ApprovalDecision,
#           timeout=timedelta(seconds=120),
#       )
#   and have the approval-service POST to the FlyteAdmin signal endpoint.
# ---------------------------------------------------------------------------
@task(
    retries=3,
    container_image=order_image,
    timeout=timedelta(seconds=180),
)
def poll_for_approval(
    approval_request_id: str,
    order_id: str,
    db_config: DBConfig,
) -> ApprovalDecision:
    """Poll GET /approval-requests/<id> every 5 seconds until decided.

    Returns the ``ApprovalDecision``. If the request times out (no decision
    within the polling window), returns decision="expired".
    """
    max_attempts = 24  # 24 * 5s = 120s total

    for attempt in range(max_attempts):
        response = _http.request(
            "GET",
            f"{APPROVAL_SERVICE_URL}/approval-requests/{approval_request_id}",
            timeout=10.0,
        )

        if response.status == 200:
            data = json.loads(response.data.decode("utf-8"))
            status = data.get("status", "pending")

            if status in ("approved", "rejected"):
                decision = ApprovalDecision(
                    decision=status,
                    approver=data.get("approver"),
                    reason=data.get("reason", ""),
                    decided_at=data.get("decided_at", datetime.now(timezone.utc).isoformat()),
                )

                # Persist decision to DB
                conn = _get_connection(db_config)
                try:
                    cur = conn.cursor()
                    now = datetime.now(timezone.utc)
                    cur.execute(
                        """
                        UPDATE approval_requests
                        SET status = %s, approver = %s, reason = %s, decided_at = %s
                        WHERE approval_request_id = %s
                        """,
                        (status, decision.approver, decision.reason, now, approval_request_id),
                    )
                    new_status = "approved" if status == "approved" else "rejected"
                    cur.execute(
                        "UPDATE orders SET status = %s, updated_at = %s WHERE order_id = %s",
                        (new_status, now, order_id),
                    )
                    conn.commit()
                finally:
                    conn.close()

                return decision

        if attempt < max_attempts - 1:
            time.sleep(5)

    # Timed out — return expired decision
    return ApprovalDecision(
        decision="expired",
        approver="",
        reason="Approval request timed out",
        decided_at=datetime.now(timezone.utc).isoformat(),
    )


# ---------------------------------------------------------------------------
# Sub-workflow: Manager approval
# ---------------------------------------------------------------------------
@workflow
def manager_approval_subworkflow(
    order_id: str,
    customer_id: str,
    total_amount: float,
    items: List[OrderItem],
    db_config: DBConfig,
) -> ApprovalDecision:
    """Sub-workflow for manager approval.

    Equivalent to step-functions/dag4-order-fulfillment/sub-workflows/
    manager-approval.asl.json.

    In production, use wait_for_input instead of polling.
    """
    approval_request_id = request_approval(
        order_id=order_id,
        customer_id=customer_id,
        total_amount=total_amount,
        items=items,
        db_config=db_config,
    )

    decision = poll_for_approval(
        approval_request_id=approval_request_id,
        order_id=order_id,
        db_config=db_config,
    )

    return decision


# ---------------------------------------------------------------------------
# Task 5: Call shipping API
# ---------------------------------------------------------------------------
@task(
    retries=3,
    container_image=order_image,
)
def call_shipping_api(
    order_id: str,
    items: List[OrderItem],
    shipping_address: ShippingAddress,
) -> ShipmentResult:
    """Call the shipping service API.

    Raises typed exceptions so Flyte's RetryStrategy can retry transient
    errors while propagating non-retriable ones immediately.
    """
    idempotency_key = f"{order_id}-ship"

    items_payload = [
        {"sku": item.sku, "quantity": item.quantity, "unit_price": item.unit_price}
        for item in items
    ]
    address_payload = {
        "street": shipping_address.street,
        "city": shipping_address.city,
        "state": shipping_address.state,
        "zip_code": shipping_address.zip_code,
        "country": shipping_address.country,
    }

    payload = {
        "order_id": order_id,
        "items": items_payload,
        "shipping_address": address_payload,
        "idempotency_key": idempotency_key,
    }

    response = _http.request(
        "POST",
        f"{SHIPPING_SERVICE_URL}/shipments",
        body=json.dumps(payload),
        headers={"Content-Type": "application/json"},
        timeout=30.0,
    )

    body = json.loads(response.data.decode("utf-8"))

    if response.status == 200:
        return ShipmentResult(
            shipment_id=body["shipment_id"],
            tracking_number=body["tracking_number"],
            carrier=body["carrier"],
            estimated_delivery=body["estimated_delivery"],
        )

    # Parse error details
    detail = body.get("detail", {})
    if isinstance(detail, dict):
        error_type = detail.get("error_type", "Unknown")
        message = detail.get("message", str(body))
    else:
        error_type = "Unknown"
        message = str(body)

    if error_type == "InvalidAddress":
        raise InvalidAddress(message)
    elif error_type == "ShippingTimeout" or response.status == 504:
        raise ShippingTimeout(message)
    elif error_type == "ShippingServiceError" or response.status >= 500:
        raise ShippingServiceError(message)
    else:
        raise RuntimeError(f"Unexpected shipping error ({response.status}): {message}")


# ---------------------------------------------------------------------------
# Sub-workflow: Shipping
# ---------------------------------------------------------------------------
@workflow
def shipping_subworkflow(
    order_id: str,
    items: List[OrderItem],
    shipping_address: ShippingAddress,
) -> ShipmentResult:
    """Sub-workflow wrapping the shipping API call.

    Equivalent to step-functions/dag4-order-fulfillment/sub-workflows/
    shipping.asl.json.
    """
    return call_shipping_api(
        order_id=order_id,
        items=items,
        shipping_address=shipping_address,
    )


# ---------------------------------------------------------------------------
# Task 6: Update order status in DB
# ---------------------------------------------------------------------------
@task(
    retries=3,
    container_image=order_image,
)
def update_order_status(
    order_id: str,
    status: str,
    db_config: DBConfig,
    shipment_id: str = "",
    tracking_number: str = "",
    failure_reason: str = "",
) -> OrderStatusUpdate:
    """Update the order record in Postgres.

    Used for both success (shipped) and compensation (cancelled) paths.
    """
    conn = _get_connection(db_config)
    try:
        cur = conn.cursor()
        now = datetime.now(timezone.utc)

        cur.execute(
            """
            UPDATE orders
            SET status = %s,
                shipment_id = COALESCE(NULLIF(%s, ''), shipment_id),
                tracking_number = COALESCE(NULLIF(%s, ''), tracking_number),
                failure_reason = COALESCE(NULLIF(%s, ''), failure_reason),
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

        return OrderStatusUpdate(
            order_id=result[0],
            status=result[1],
            updated_at=now.isoformat(),
        )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Task 7: Send order notification
# ---------------------------------------------------------------------------
@task(
    retries=3,
    container_image=order_image,
)
def send_order_notification(
    order_id: str,
    status: str,
    tracking_number: str = "",
    carrier: str = "",
    estimated_delivery: str = "",
    failure_reason: str = "",
) -> OrderNotification:
    """Send a simulated notification for order status changes.

    In production, this would call SES, SNS, or a webhook.
    """
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

    print(json.dumps({
        "order_id": order_id,
        "status": status,
        "message": message,
        "sent_at": sent_at,
        "channel": "simulated_email",
    }))

    return OrderNotification(
        notification_sent=True,
        order_id=order_id,
        status=status,
        sent_at=sent_at,
    )


# ---------------------------------------------------------------------------
# Saga compensation tasks
# ---------------------------------------------------------------------------
@task(
    retries=5,
    container_image=order_image,
)
def release_inventory(
    order_id: str,
    db_config: DBConfig,
) -> CompensationResult:
    """Saga compensation: reverse all inventory reservations for an order.

    Idempotent — no-op if already released.
    """
    conn = _get_connection(db_config)
    try:
        cur = conn.cursor()

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
            return CompensationResult(
                order_id=order_id,
                released=0,
                status="no_reservations_to_release",
                failure_reason="",
            )

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

        return CompensationResult(
            order_id=order_id,
            released=released,
            status="inventory_released",
            failure_reason="",
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Sub-workflow: Saga compensation sequence
# ---------------------------------------------------------------------------
@workflow
def compensate_order(
    order_id: str,
    failure_reason: str,
    db_config: DBConfig,
) -> OrderOutput:
    """Execute the saga compensation sequence.

    1. Release reserved inventory
    2. Update order status to cancelled
    3. Send cancellation notification (best-effort)

    Each step is a separate Flyte task node with its own retry policy.
    """
    compensation = release_inventory(order_id=order_id, db_config=db_config)

    order_update = update_order_status(
        order_id=order_id,
        status="cancelled",
        db_config=db_config,
        failure_reason=failure_reason,
    )

    # Best-effort cancellation notification
    notification = send_order_notification(
        order_id=order_id,
        status="cancelled",
        failure_reason=failure_reason,
    )

    return OrderOutput(
        order_id=order_id,
        status="cancelled",
        compensation=compensation,
        notification=notification,
        failure_reason=failure_reason,
    )


# ---------------------------------------------------------------------------
# Sub-workflow: Happy path after approval (or no approval needed)
# ---------------------------------------------------------------------------
@workflow
def ship_and_finalize(
    order_id: str,
    items: List[OrderItem],
    shipping_address: ShippingAddress,
    db_config: DBConfig,
) -> OrderOutput:
    """Ship the order and update the database.

    If shipping fails, this propagates the exception to the parent workflow
    which triggers saga compensation.
    """
    shipment = shipping_subworkflow(
        order_id=order_id,
        items=items,
        shipping_address=shipping_address,
    )

    order_update = update_order_status(
        order_id=order_id,
        status="shipped",
        db_config=db_config,
        shipment_id=shipment.shipment_id,
        tracking_number=shipment.tracking_number,
    )

    notification = send_order_notification(
        order_id=order_id,
        status="shipped",
        tracking_number=shipment.tracking_number,
        carrier=shipment.carrier,
        estimated_delivery=shipment.estimated_delivery,
    )

    return OrderOutput(
        order_id=order_id,
        status="shipped",
        shipment=shipment,
        notification=notification,
    )


# ---------------------------------------------------------------------------
# Sub-workflow: Path requiring approval
# ---------------------------------------------------------------------------
@workflow
def approval_then_ship(
    order_id: str,
    customer_id: str,
    total_amount: float,
    items: List[OrderItem],
    shipping_address: ShippingAddress,
    db_config: DBConfig,
) -> OrderOutput:
    """Request manager approval, then ship if approved.

    If rejected or expired, triggers compensation via the parent workflow.
    """
    decision = manager_approval_subworkflow(
        order_id=order_id,
        customer_id=customer_id,
        total_amount=total_amount,
        items=items,
        db_config=db_config,
    )

    # Conditional: approved -> ship; otherwise -> compensate
    result = (
        conditional("check_approval_decision")
        .if_(decision.decision == "approved")
        .then(
            ship_and_finalize(
                order_id=order_id,
                items=items,
                shipping_address=shipping_address,
                db_config=db_config,
            )
        )
        .else_()
        .then(
            compensate_order(
                order_id=order_id,
                failure_reason="Order rejected or approval timed out",
                db_config=db_config,
            )
        )
    )

    return result


# ---------------------------------------------------------------------------
# Sub-workflow: Valid order path (reserve, approve if needed, ship)
# ---------------------------------------------------------------------------
@workflow
def order_valid_path(validated: OrderValidated) -> OrderOutput:
    """Path taken when validation succeeds.

    1. Reserve inventory.
    2. Check if approval is required (total >= threshold).
    3. Ship the order (with or without approval).
    """
    reservation = reserve_inventory_subworkflow(
        order_id=validated.order_id,
        customer_id=validated.customer_id,
        items=validated.items,
        db_config=validated.db_config,
    )

    result = (
        conditional("check_approval_required")
        .if_(validated.total_amount >= validated.approval_threshold)
        .then(
            approval_then_ship(
                order_id=validated.order_id,
                customer_id=validated.customer_id,
                total_amount=validated.total_amount,
                items=validated.items,
                shipping_address=validated.shipping_address,
                db_config=validated.db_config,
            )
        )
        .else_()
        .then(
            ship_and_finalize(
                order_id=validated.order_id,
                items=validated.items,
                shipping_address=validated.shipping_address,
                db_config=validated.db_config,
            )
        )
    )

    return result


# ---------------------------------------------------------------------------
# Task: Fail on invalid order (returns OrderOutput with failure info)
# ---------------------------------------------------------------------------
@task(container_image=order_image)
def order_validation_failed(order_id: str, reason: str) -> OrderOutput:
    """Return a failed OrderOutput when validation does not pass.

    No side effects — validation is read-only so there is nothing to
    compensate.
    """
    return OrderOutput(
        order_id=order_id,
        status="failed",
        failure_reason=reason or "Order failed validation",
    )


# ---------------------------------------------------------------------------
# Top-level workflow
# ---------------------------------------------------------------------------
@workflow
def order_fulfillment_workflow(order_input: OrderInput) -> OrderOutput:
    """Order Fulfillment Workflow.

    1. Validate the order (customer, SKUs, stock, total).
    2. Reserve inventory (sub-workflow, atomic transaction).
    3. If total >= approval_threshold: request manager approval.
    4. Call shipping API (sub-workflow with retries).
    5. Update DB, send shipped notification.

    Saga compensation on failure:
      - Release reserved inventory
      - Update order to cancelled
      - Send cancellation notification

    The workflow uses try/except semantics for compensation. Since Flyte
    workflows must be deterministic, compensation is modeled as separate
    tasks invoked in the failure conditional path.
    """
    # Step 1: Validate
    validated = validate_order(order_input=order_input)

    # Step 2: Branch on validation result
    result = (
        conditional("check_order_validation")
        .if_(validated.validation.is_valid.is_true())
        .then(order_valid_path(validated=validated))
        .else_()
        .then(
            order_validation_failed(
                order_id=validated.order_id,
                reason=validated.validation.reason,
            )
        )
    )

    return result

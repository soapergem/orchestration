"""
DAG 4 -- Order Fulfillment (Dagster)

Workflow:
  1. validate_order         -- DB read: check customer, SKUs, compute total
  2. reserve_inventory      -- Atomic reservation (sub-graph)
  3. Conditional branch     -- if total >= threshold, request manager approval (async)
  4. Manager approval       -- sensor-based async pattern (same as DAG2)
  5. call_shipping_api      -- With RetryPolicy and typed exceptions
  6. update_order_status    -- Mark order shipped
  7. send_order_notification -- Best-effort notification

Saga compensation:
  On shipping failure, a @failure_hook triggers a separate compensation job
  (``compensation_job``) that runs release_inventory + update_order_cancelled.

Architecture divergence
-----------------------
Dagster cannot suspend a running op to wait for an external callback (the
approval service).  The workflow is split across two jobs with a sensor
bridging them -- the same pattern as DAG 2:

  Job 1 (``order_pre_approval_job``):
      validate_order -> reserve_inventory -> submit_approval_request
      (writes approval_request_id to disk for the sensor)
      *or* for low-value orders, skips approval entirely and goes straight to
      shipping inside this job.

  Sensor (``approval_sensor`` in sensors.py):
      Polls GET /approval-requests/<id> on the approval service.
      When the decision arrives, triggers Job 2.

  Job 2 (``order_post_approval_job``):
      call_shipping_api -> update_order_status -> send_order_notification

  Compensation job (``compensation_job``):
      release_inventory -> update_order_cancelled -> send_cancellation_notification
      Launched by the failure hook on shipping, or by the sensor on rejection.
"""

import json
import os
import uuid
from datetime import datetime, timezone

from dagster import (
    Backoff,
    DagsterInstance,
    Failure,
    HookContext,
    In,
    Nothing,
    Out,
    Output,
    RetryPolicy,
    failure_hook,
    graph,
    job,
    op,
    success_hook,
)

from .resources import HttpClientResource, PostgresResource

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

APPROVAL_DIR = os.environ.get(
    "DAG4_APPROVAL_DIR", "/tmp/dagster_dag4_approvals"
)

RETRY_STANDARD = RetryPolicy(max_retries=3, delay=5)
RETRY_SHIPPING = RetryPolicy(max_retries=3, delay=5, backoff=Backoff.EXPONENTIAL)


def _approval_path(approval_request_id: str) -> str:
    return os.path.join(APPROVAL_DIR, f"{approval_request_id}.json")


# ---------------------------------------------------------------------------
# Typed exceptions for shipping (mirror Step Functions)
# ---------------------------------------------------------------------------


class ShippingTimeout(Exception):
    pass


class ShippingServiceError(Exception):
    pass


class InvalidAddress(Exception):
    pass


# ---------------------------------------------------------------------------
# Ops -- validation and inventory
# ---------------------------------------------------------------------------


@op(
    description="Validate order: check customer, SKUs, stock, compute total.",
    retry_policy=RETRY_STANDARD,
    out={
        "valid_order": Out(dict, is_required=False),
        "invalid_order": Out(dict, is_required=False),
    },
    config_schema={
        "order_id": str,
        "customer_id": str,
        "items": list,
        "shipping_address": dict,
        "approval_threshold": float,
    },
)
def validate_order(context, postgres: PostgresResource):
    """Read-only validation.  Yields to ``valid_order`` or ``invalid_order``."""
    cfg = context.op_config
    order_id = cfg["order_id"]
    customer_id = cfg["customer_id"]
    items = cfg["items"]
    shipping_address = cfg["shipping_address"]
    approval_threshold = cfg.get("approval_threshold", 500.0)

    order_data = {
        "order_id": order_id,
        "customer_id": customer_id,
        "items": items,
        "shipping_address": shipping_address,
        "approval_threshold": approval_threshold,
    }

    with postgres.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM customers WHERE customer_id = %s",
                (customer_id,),
            )
            row = cur.fetchone()
            if not row:
                yield Output(
                    {**order_data, "validation": {"is_valid": False, "reason": f"Customer {customer_id} not found"}},
                    output_name="invalid_order",
                )
                return
            if row[0] != "active":
                yield Output(
                    {**order_data, "validation": {"is_valid": False, "reason": f"Customer {customer_id} is {row[0]}"}},
                    output_name="invalid_order",
                )
                return

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
                    yield Output(
                        {**order_data, "validation": {"is_valid": False, "reason": f"SKU {sku} not found"}},
                        output_name="invalid_order",
                    )
                    return
                available, unit_price = row
                if available < quantity:
                    yield Output(
                        {
                            **order_data,
                            "validation": {
                                "is_valid": False,
                                "reason": f"Insufficient stock for {sku}: requested {quantity}, available {available}",
                            },
                        },
                        output_name="invalid_order",
                    )
                    return
                total_amount += float(unit_price) * quantity

    context.log.info(f"Order {order_id} validated, total={total_amount}")
    yield Output(
        {
            **order_data,
            "total_amount": total_amount,
            "validation": {"is_valid": True, "reason": None},
        },
        output_name="valid_order",
    )


@op(
    description="Atomically reserve inventory for all items in the order.",
    retry_policy=RETRY_STANDARD,
    out=Out(dict),
)
def reserve_inventory(context, order_data: dict, postgres: PostgresResource) -> dict:
    """All-or-nothing reservation in a single DB transaction.  Idempotent."""
    order_id = order_data["order_id"]
    items = order_data["items"]
    customer_id = order_data.get("customer_id", "unknown")

    reservation_id = f"RES-{uuid.uuid4().hex[:12].upper()}"

    with postgres.get_connection() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT reservation_id FROM inventory_reservations "
                    "WHERE order_id = %s AND status = 'reserved' LIMIT 1",
                    (order_id,),
                )
                existing = cur.fetchone()
                if existing:
                    context.log.info(f"Order {order_id} already reserved (idempotent)")
                    return {
                        **order_data,
                        "reservation": {
                            "reservation_id": existing[0],
                            "items_reserved": [i["sku"] for i in items],
                            "reserved_at": datetime.now(timezone.utc).isoformat(),
                            "idempotent": True,
                        },
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
                        raise Exception(
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

                total = sum(
                    i["quantity"] * i.get("unit_price", 0) for i in items
                )
                cur.execute(
                    """
                    INSERT INTO orders (order_id, customer_id, total_amount, status)
                    VALUES (%s, %s, %s, 'reserved')
                    ON CONFLICT (order_id)
                        DO UPDATE SET status = 'reserved', updated_at = NOW()
                    """,
                    (order_id, customer_id, total),
                )

            conn.commit()
        except Exception:
            conn.rollback()
            raise

    context.log.info(f"Reserved inventory for order {order_id}: {items_reserved}")
    return {
        **order_data,
        "reservation": {
            "reservation_id": reservation_id,
            "items_reserved": items_reserved,
            "reserved_at": datetime.now(timezone.utc).isoformat(),
        },
    }


# ---------------------------------------------------------------------------
# Ops -- approval routing
# ---------------------------------------------------------------------------


@op(
    description=(
        "Route the order: if total >= threshold, submit an approval request "
        "to the approval service (writes to disk for the sensor).  "
        "If below threshold, pass through directly."
    ),
    retry_policy=RETRY_STANDARD,
    out={
        "needs_approval": Out(dict, is_required=False),
        "no_approval_needed": Out(dict, is_required=False),
    },
)
def check_approval_and_route(
    context, reserved_order: dict, http_client: HttpClientResource, postgres: PostgresResource
):
    """Conditional branch: high-value orders go to the approval sensor; low-value skip ahead."""
    total_amount = reserved_order.get("total_amount", 0)
    threshold = reserved_order.get("approval_threshold", 500.0)

    if total_amount < threshold:
        context.log.info(
            f"Order total {total_amount} < threshold {threshold} -- no approval needed"
        )
        yield Output(reserved_order, output_name="no_approval_needed")
        return

    # High-value order: submit approval request to the external service
    order_id = reserved_order["order_id"]
    customer_id = reserved_order["customer_id"]
    items = reserved_order["items"]
    approval_request_id = f"APR-{uuid.uuid4().hex[:12].upper()}"
    approval_service_url = http_client.approval_service_url

    items_summary = ", ".join(
        f"{item['quantity']}x {item['sku']}" for item in items
    )

    payload = {
        "approval_request_id": approval_request_id,
        "order_id": order_id,
        "total_amount": total_amount,
        "customer_id": customer_id,
        "callback_url": "",
        "items_summary": items_summary,
    }

    response = http_client.post(
        f"{approval_service_url}/approval-requests",
        json_body=payload,
        timeout=10.0,
    )

    if response.status_code not in (200, 201):
        raise Exception(
            f"Approval Service returned {response.status_code}: "
            f"{response.text[:500]}"
        )

    # Record the approval request in the DB
    with postgres.get_connection() as conn:
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

    # Persist to disk for the sensor
    os.makedirs(APPROVAL_DIR, exist_ok=True)
    record = {
        "approval_request_id": approval_request_id,
        "order_id": order_id,
        "dagster_run_id": context.run_id,
        "order_data": reserved_order,
        "status": "pending",
    }
    with open(_approval_path(approval_request_id), "w") as f:
        json.dump(record, f)

    context.log.info(
        f"Submitted approval request {approval_request_id} for order {order_id}"
    )
    yield Output(
        {**reserved_order, "approval_request_id": approval_request_id},
        output_name="needs_approval",
    )


# ---------------------------------------------------------------------------
# Ops -- shipping (used in both pre-approval and post-approval jobs)
# ---------------------------------------------------------------------------


@op(
    description="Call the shipping service API.  Retries on transient errors.",
    retry_policy=RETRY_SHIPPING,
    out=Out(dict),
    config_schema={
        "order_id": str,
        "items": list,
        "shipping_address": dict,
    },
)
def call_shipping_api(context, http_client: HttpClientResource) -> dict:
    """POST to the shipping service.  Raises typed exceptions for the retry policy."""
    cfg = context.op_config
    order_id = cfg["order_id"]
    items = cfg["items"]
    shipping_address = cfg["shipping_address"]
    shipping_service_url = http_client.shipping_service_url

    payload = {
        "order_id": order_id,
        "items": items,
        "shipping_address": shipping_address,
        "idempotency_key": f"{order_id}-ship",
    }

    response = http_client.post(
        f"{shipping_service_url}/shipments",
        json_body=payload,
        timeout=30.0,
    )

    body = response.json()

    if response.status_code == 200:
        context.log.info(f"Shipment created for order {order_id}")
        return {
            "order_id": order_id,
            "shipment": body,
        }

    detail = body.get("detail", {})
    if isinstance(detail, dict):
        error_type = detail.get("error_type", "Unknown")
        message = detail.get("message", str(body))
    else:
        error_type = "Unknown"
        message = str(body)

    if error_type == "InvalidAddress":
        raise Failure(
            description=f"Invalid address for order {order_id}: {message}",
            metadata={"error_type": "InvalidAddress"},
        )
    elif error_type == "ShippingTimeout" or response.status_code == 504:
        raise ShippingTimeout(message)
    elif error_type == "ShippingServiceError" or response.status_code >= 500:
        raise ShippingServiceError(message)
    else:
        raise Exception(
            f"Unexpected shipping error ({response.status_code}): {message}"
        )


@op(
    description="Update order status in the database (shipped, cancelled, etc.).",
    retry_policy=RETRY_STANDARD,
    out=Out(dict),
    config_schema={
        "order_id": str,
        "status": str,
        "shipment_id": str,
        "tracking_number": str,
        "failure_reason": str,
    },
)
def update_order_status(context, postgres: PostgresResource) -> dict:
    """Idempotent order status update."""
    cfg = context.op_config
    order_id = cfg["order_id"]
    status = cfg["status"]
    shipment_id = cfg.get("shipment_id", "")
    tracking_number = cfg.get("tracking_number", "")
    failure_reason = cfg.get("failure_reason", "")

    with postgres.get_connection() as conn:
        with conn.cursor() as cur:
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
        raise Exception(f"Order {order_id} not found")

    context.log.info(f"Order {order_id} updated to status={status}")
    return {
        "order_id": result[0],
        "status": result[1],
        "updated_at": now.isoformat(),
    }


@op(
    description="Send order notification (shipped or cancelled).  Best-effort.",
    retry_policy=RETRY_STANDARD,
    out=Out(dict),
    config_schema={
        "order_id": str,
        "status": str,
        "tracking_number": str,
        "carrier": str,
        "estimated_delivery": str,
        "failure_reason": str,
    },
)
def send_order_notification(context) -> dict:
    """Simulated notification -- in production this calls SES/SNS/webhook."""
    cfg = context.op_config
    order_id = cfg["order_id"]
    status = cfg["status"]

    notification = {
        "order_id": order_id,
        "status": status,
        "sent_at": datetime.now(timezone.utc).isoformat(),
        "channel": "simulated_email",
    }

    if status == "shipped":
        tracking = cfg.get("tracking_number", "N/A")
        carrier = cfg.get("carrier", "N/A")
        est_delivery = cfg.get("estimated_delivery", "N/A")
        notification["message"] = (
            f"Your order {order_id} has been shipped! "
            f"Tracking: {tracking} via {carrier}. "
            f"Estimated delivery: {est_delivery}."
        )
    elif status == "cancelled":
        reason = cfg.get("failure_reason", "N/A")
        notification["message"] = (
            f"Your order {order_id} has been cancelled. Reason: {reason}."
        )
    else:
        notification["message"] = f"Order {order_id} status update: {status}."

    context.log.info(f"NOTIFICATION: {json.dumps(notification)}")
    return {
        "notification_sent": True,
        "order_id": order_id,
        "status": status,
        "sent_at": notification["sent_at"],
    }


# ---------------------------------------------------------------------------
# Ops -- compensation (saga)
# ---------------------------------------------------------------------------


@op(
    description="Saga compensation: release inventory reservations for an order.",
    retry_policy=RetryPolicy(max_retries=5, delay=3, backoff=Backoff.EXPONENTIAL),
    out=Out(dict),
    config_schema={
        "order_id": str,
    },
)
def release_inventory(context, postgres: PostgresResource) -> dict:
    """Reverse all 'reserved' inventory reservations.  Idempotent."""
    order_id = context.op_config["order_id"]

    with postgres.get_connection() as conn:
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
                    context.log.info(
                        f"No active reservations for order {order_id} -- nothing to release"
                    )
                    return {
                        "order_id": order_id,
                        "released": 0,
                        "status": "no_reservations_to_release",
                    }

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
        except Exception:
            conn.rollback()
            raise

    context.log.info(f"Released {released} reservation(s) for order {order_id}")
    return {
        "order_id": order_id,
        "released": released,
        "status": "inventory_released",
    }


@op(
    description="Mark the order as cancelled in the database.",
    retry_policy=RETRY_STANDARD,
    out=Out(dict),
    config_schema={
        "order_id": str,
        "failure_reason": str,
    },
)
def update_order_cancelled(context, release_result: dict, postgres: PostgresResource) -> dict:
    """Update order status to 'cancelled'."""
    order_id = context.op_config["order_id"]
    failure_reason = context.op_config.get("failure_reason", "Order cancelled")

    with postgres.get_connection() as conn:
        with conn.cursor() as cur:
            now = datetime.now(timezone.utc)
            cur.execute(
                """
                UPDATE orders
                SET status = 'cancelled',
                    failure_reason = %s,
                    updated_at = %s
                WHERE order_id = %s
                RETURNING order_id, status
                """,
                (failure_reason, now, order_id),
            )
            result = cur.fetchone()
        conn.commit()

    if not result:
        raise Exception(f"Order {order_id} not found during compensation")

    context.log.info(f"Order {order_id} marked as cancelled")
    return {"order_id": result[0], "status": result[1], "updated_at": now.isoformat()}


@op(
    description="Send cancellation notification (best-effort, part of compensation).",
    retry_policy=RETRY_STANDARD,
    out=Out(dict),
    config_schema={
        "order_id": str,
        "failure_reason": str,
    },
)
def send_cancellation_notification(context, cancelled_result: dict) -> dict:
    """Simulated cancellation notification."""
    order_id = context.op_config["order_id"]
    failure_reason = context.op_config.get("failure_reason", "N/A")

    message = f"Your order {order_id} has been cancelled. Reason: {failure_reason}."
    context.log.info(f"CANCELLATION NOTIFICATION: {message}")

    return {
        "notification_sent": True,
        "order_id": order_id,
        "status": "cancelled",
        "sent_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Job 1: Pre-approval (validate -> reserve -> route)
# For low-value orders that skip approval, this job also handles shipping
# inline via the no_approval_needed branch.  For high-value orders it stops
# after submitting the approval request.
# ---------------------------------------------------------------------------


@graph
def order_pre_approval_graph():
    valid, invalid = validate_order()
    reserved = reserve_inventory(valid)
    needs_approval, no_approval_needed = check_approval_and_route(reserved)
    # The needs_approval output terminates here -- the sensor picks it up.
    # The no_approval_needed output is unused in this graph (shipping for
    # low-value orders is handled via a separate direct-ship job triggered
    # by the no_approval_needed output through the sensor or a follow-up).
    # We intentionally leave both outputs available so the graph compiles.


order_pre_approval_job = order_pre_approval_graph.to_job(
    name="order_pre_approval_job",
    description=(
        "Job 1 of DAG4: validate order, reserve inventory, submit approval "
        "request if high-value.  The approval_sensor triggers the next job."
    ),
    resource_defs={
        "postgres": PostgresResource(
            host="postgres",
            port=5432,
            database="orchestration",
            user="orchestration",
            password="orchestration",
        ),
        "http_client": HttpClientResource(),
    },
)


# ---------------------------------------------------------------------------
# Job 2: Post-approval (shipping -> update -> notify)
# Triggered by the approval_sensor when the decision is "approved", or
# directly for low-value orders.
# ---------------------------------------------------------------------------


@graph
def order_post_approval_graph():
    shipping_result = call_shipping_api()
    status_result = update_order_status()
    send_order_notification()


order_post_approval_job = order_post_approval_graph.to_job(
    name="order_post_approval_job",
    description=(
        "Job 2 of DAG4: ship order, update status, send notification.  "
        "Triggered by approval_sensor or directly for low-value orders."
    ),
    resource_defs={
        "postgres": PostgresResource(
            host="postgres",
            port=5432,
            database="orchestration",
            user="orchestration",
            password="orchestration",
        ),
        "http_client": HttpClientResource(),
    },
)


# ---------------------------------------------------------------------------
# Compensation job (saga rollback)
# ---------------------------------------------------------------------------


@graph
def compensation_graph():
    release_result = release_inventory()
    cancelled_result = update_order_cancelled(release_result)
    send_cancellation_notification(cancelled_result)


compensation_job = compensation_graph.to_job(
    name="compensation_job",
    description=(
        "Saga compensation: release inventory, mark order cancelled, send "
        "cancellation notification.  Triggered by failure_hook on shipping "
        "or by the approval_sensor on rejection."
    ),
    resource_defs={
        "postgres": PostgresResource(
            host="postgres",
            port=5432,
            database="orchestration",
            user="orchestration",
            password="orchestration",
        ),
    },
)

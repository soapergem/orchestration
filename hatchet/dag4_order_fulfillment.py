"""
DAG 4: Order Fulfillment

Validates an order, reserves inventory (child workflow), conditionally requests
manager approval (child workflow with durable event wait), calls the shipping
API (child workflow), updates the order, and sends notifications. Implements
saga compensation via on_failure handler.

Hatchet features used:
- Child workflow spawning (context.spawn_workflow)
- Durable event waits (context.event() with filter and timeout)
- on_failure handler for saga compensation
- NonRetryableException for non-retriable errors
- Task-level retries with backoff
- Conditional branching in durable task code
"""

import json
import os
import uuid
from datetime import datetime, timezone

import httpx
import psycopg2

from hatchet_sdk import Context, Hatchet, NonRetryableException

hatchet = Hatchet()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": int(os.environ.get("POSTGRES_PORT", "5432")),
    "dbname": os.environ.get("POSTGRES_DB", "orchestration"),
    "user": os.environ.get("POSTGRES_USER", "orchestration"),
    "password": os.environ.get("POSTGRES_PASSWORD", "orchestration"),
}

APPROVAL_SERVICE_URL = os.environ.get(
    "APPROVAL_SERVICE_URL", "http://approval-service:8091"
)
SHIPPING_SERVICE_URL = os.environ.get(
    "SHIPPING_SERVICE_URL", "http://shipping-service:8092"
)
HATCHET_EVENT_API_URL = os.environ.get(
    "HATCHET_EVENT_API_URL", "http://localhost:8080/api/v1/events"
)


def get_db_connection(db_config: dict | None = None) -> psycopg2.extensions.connection:
    cfg = db_config or DB_CONFIG
    return psycopg2.connect(
        host=cfg["host"],
        port=cfg.get("port", 5432),
        dbname=cfg.get("dbname", cfg.get("database", "orchestration")),
        user=cfg["user"],
        password=cfg["password"],
    )


# ---------------------------------------------------------------------------
# Child Workflow: Reserve Inventory
# ---------------------------------------------------------------------------

@hatchet.workflow(name="ReserveInventory", on_events=["inventory:reserve"])
class ReserveInventoryWorkflow:
    """Atomically reserves inventory for all items in the order."""

    @hatchet.task(
        name="reserve_items",
        retries=3,
        backoff_factor=2.0,
        backoff_base_seconds=2,
    )
    async def reserve_items(self, context: Context) -> dict:
        input_data = context.workflow_input()
        order_id = input_data["order_id"]
        items = input_data["items"]
        customer_id = input_data.get("customer_id", "unknown")
        db_config = input_data.get("db_config") or DB_CONFIG

        reservation_id = f"RES-{uuid.uuid4().hex[:12].upper()}"

        conn = get_db_connection(db_config)
        try:
            cur = conn.cursor()

            # Idempotency: check for existing reservation
            cur.execute(
                "SELECT reservation_id FROM inventory_reservations "
                "WHERE order_id = %s AND status = 'reserved' LIMIT 1",
                (order_id,),
            )
            existing = cur.fetchone()
            if existing:
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

            # Create / update the order record
            total = sum(i["quantity"] * i["unit_price"] for i in items)
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

            return {
                "reservation_id": reservation_id,
                "items_reserved": items_reserved,
                "reserved_at": datetime.now(timezone.utc).isoformat(),
            }
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Child Workflow: Manager Approval (with durable event wait)
# ---------------------------------------------------------------------------

@hatchet.workflow(name="ManagerApproval", on_events=["approval:request"])
class ManagerApprovalWorkflow:
    """
    Requests manager approval and waits for the decision via a durable event.
    The approval-service posts an 'approval_decision' event to Hatchet when
    the manager decides.
    """

    @hatchet.task(
        name="request_approval",
        retries=3,
        backoff_factor=2.0,
        backoff_base_seconds=2,
    )
    async def request_approval(self, context: Context) -> dict:
        """POST the approval request to the approval-service."""
        input_data = context.workflow_input()
        order_id = input_data["order_id"]
        customer_id = input_data["customer_id"]
        total_amount = input_data["total_amount"]
        items = input_data["items"]
        db_config = input_data.get("db_config") or DB_CONFIG

        approval_request_id = f"APR-{uuid.uuid4().hex[:12].upper()}"

        # Callback URL points to Hatchet's event API
        callback_url = (
            f"{HATCHET_EVENT_API_URL}"
            f"?event_type=approval_decision"
            f"&order_id={order_id}"
            f"&approval_request_id={approval_request_id}"
        )

        items_summary = ", ".join(
            f"{item['quantity']}x {item['sku']}" for item in items
        )

        payload = {
            "approval_request_id": approval_request_id,
            "order_id": order_id,
            "total_amount": total_amount,
            "customer_id": customer_id,
            "callback_url": callback_url,
            "items_summary": items_summary,
        }

        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                f"{APPROVAL_SERVICE_URL}/approval-requests",
                json=payload,
                headers={"Content-Type": "application/json"},
            )

        if response.status_code not in (200, 201):
            raise Exception(
                f"Approval Service returned {response.status_code}: "
                f"{response.text[:500]}"
            )

        # Record the approval request in the database
        try:
            conn = get_db_connection(db_config)
            try:
                cur = conn.cursor()
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
        except Exception as db_err:
            print(f"WARNING: Could not record approval request: {db_err}")

        return {
            "approval_request_id": approval_request_id,
            "order_id": order_id,
            "status": "submitted",
        }

    @hatchet.task(
        name="wait_for_approval",
        parents=["request_approval"],
    )
    async def wait_for_approval(self, context: Context) -> dict:
        """
        Durable event wait: suspend until the approval-service pushes an
        'approval_decision' event with our order_id, or timeout after 120s.
        """
        request_result = (await context.task_output("request_approval"))
        order_id = request_result["order_id"]
        approval_request_id = request_result["approval_request_id"]

        try:
            event_data = await (
                context.event("approval_decision")
                .with_filter(order_id=order_id)
                .with_timeout(120)
            )
        except TimeoutError:
            # Approval timed out -- treat as expired
            return {
                "decision": "expired",
                "approver": None,
                "reason": "Approval request timed out",
                "decided_at": None,
                "approval_request_id": approval_request_id,
                "order_id": order_id,
            }

        return {
            "decision": event_data.get("decision", "unknown"),
            "approver": event_data.get("approver"),
            "reason": event_data.get("reason", ""),
            "decided_at": event_data.get("decided_at", datetime.now(timezone.utc).isoformat()),
            "approval_request_id": approval_request_id,
            "order_id": order_id,
        }

    @hatchet.task(
        name="record_decision",
        parents=["wait_for_approval"],
        retries=3,
        backoff_factor=2.0,
        backoff_base_seconds=2,
    )
    async def record_decision(self, context: Context) -> dict:
        """Persist the approval decision to the database."""
        approval_result = (await context.task_output("wait_for_approval"))
        input_data = context.workflow_input()
        db_config = input_data.get("db_config") or DB_CONFIG

        decision = approval_result["decision"]
        approver = approval_result.get("approver")
        reason = approval_result.get("reason", "")
        order_id = approval_result["order_id"]
        approval_request_id = approval_result["approval_request_id"]

        conn = get_db_connection(db_config)
        try:
            cur = conn.cursor()
            now = datetime.now(timezone.utc)

            if approval_request_id:
                cur.execute(
                    """
                    UPDATE approval_requests
                    SET status = %s, approver = %s, reason = %s, decided_at = %s
                    WHERE approval_request_id = %s
                    """,
                    (decision, approver, reason, now, approval_request_id),
                )

            if order_id:
                new_status = "approved" if decision == "approved" else "rejected"
                cur.execute(
                    "UPDATE orders SET status = %s, updated_at = %s WHERE order_id = %s",
                    (new_status, now, order_id),
                )

            conn.commit()

            return {
                "decision": decision,
                "approver": approver,
                "reason": reason,
                "decided_at": now.isoformat(),
            }
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Child Workflow: Shipping
# ---------------------------------------------------------------------------

@hatchet.workflow(name="ShipOrder", on_events=["shipping:ship"])
class ShipOrderWorkflow:
    """Calls the shipping service API with retries."""

    @hatchet.task(
        name="call_shipping_api",
        retries=3,
        backoff_factor=2.0,
        backoff_base_seconds=3,
    )
    async def call_shipping_api(self, context: Context) -> dict:
        input_data = context.workflow_input()
        order_id = input_data["order_id"]
        items = input_data["items"]
        shipping_address = input_data["shipping_address"]

        idempotency_key = f"{order_id}-ship"

        payload = {
            "order_id": order_id,
            "items": items,
            "shipping_address": shipping_address,
            "idempotency_key": idempotency_key,
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{SHIPPING_SERVICE_URL}/shipments",
                json=payload,
                headers={"Content-Type": "application/json"},
            )

        body = response.json()

        if response.status_code == 200:
            return body

        error_type = "Unknown"
        message = str(body)
        if isinstance(body.get("detail"), dict):
            error_type = body["detail"].get("error_type", "Unknown")
            message = body["detail"].get("message", str(body))

        if error_type == "InvalidAddress":
            # Non-retriable
            raise NonRetryableException(f"InvalidAddress: {message}")
        else:
            # Retriable (ShippingTimeout, ShippingServiceError, etc.)
            raise Exception(
                f"Shipping error ({error_type}, status {response.status_code}): {message}"
            )


# ---------------------------------------------------------------------------
# Main Order Fulfillment Workflow
# ---------------------------------------------------------------------------

@hatchet.workflow(name="OrderFulfillment", on_events=["order:fulfill"])
class OrderFulfillmentWorkflow:
    """
    Order Fulfillment Pipeline:
    1. validate_order -- check customer, SKUs, compute total
    2. reserve_inventory -- child workflow (atomic reservation)
    3. check_approval / manager_approval -- conditional child workflow with event wait
    4. call_shipping_api -- child workflow with retries
    5. update_order_shipped -- record shipment in DB
    6. send_shipped_notification -- notify customer

    Saga compensation via on_failure handler:
    - release_inventory
    - update_order_cancelled
    - send_cancellation_notification
    """

    @hatchet.task(
        name="validate_order",
        retries=3,
        backoff_factor=2.0,
        backoff_base_seconds=2,
    )
    async def validate_order(self, context: Context) -> dict:
        """Validate order: customer active, SKUs exist, sufficient stock, compute total."""
        input_data = context.workflow_input()
        order_id = input_data["order_id"]
        customer_id = input_data["customer_id"]
        items = input_data["items"]
        db_config = input_data.get("db_config") or DB_CONFIG
        approval_threshold = input_data.get("approval_threshold", 500.00)

        conn = get_db_connection(db_config)
        try:
            cur = conn.cursor()

            cur.execute(
                "SELECT status FROM customers WHERE customer_id = %s",
                (customer_id,),
            )
            row = cur.fetchone()
            if not row:
                raise NonRetryableException(
                    f"Order validation failed: Customer {customer_id} not found"
                )
            if row[0] != "active":
                raise NonRetryableException(
                    f"Order validation failed: Customer {customer_id} is {row[0]}"
                )

            total_amount = 0
            for item in items:
                sku = item["sku"]
                quantity = item["quantity"]

                cur.execute(
                    "SELECT available_quantity, unit_price FROM inventory WHERE sku = %s",
                    (sku,),
                )
                row = cur.fetchone()
                if not row:
                    raise NonRetryableException(
                        f"Order validation failed: SKU {sku} not found"
                    )

                available, unit_price = row
                if available < quantity:
                    raise NonRetryableException(
                        f"Order validation failed: Insufficient stock for {sku}: "
                        f"requested {quantity}, available {available}"
                    )
                total_amount += float(unit_price) * quantity

            return {
                "total_amount": total_amount,
                "approval_threshold": approval_threshold,
                "validation": {"is_valid": True},
            }
        finally:
            conn.close()

    @hatchet.task(
        name="reserve_inventory",
        parents=["validate_order"],
        retries=3,
        backoff_factor=2.0,
        backoff_base_seconds=2,
    )
    async def reserve_inventory(self, context: Context) -> dict:
        """Spawn the ReserveInventory child workflow and wait for completion."""
        input_data = context.workflow_input()
        order_id = input_data["order_id"]

        child_input = {
            "order_id": order_id,
            "items": input_data["items"],
            "customer_id": input_data.get("customer_id", "unknown"),
            "db_config": input_data.get("db_config") or DB_CONFIG,
        }

        child = context.spawn_workflow(
            "ReserveInventory",
            child_input,
            key=f"reserve-inventory-{order_id}",
        )
        result = await child.result()

        return {
            "reservation_id": result["reservation_id"],
            "items_reserved": result["items_reserved"],
            "reserved_at": result["reserved_at"],
        }

    @hatchet.task(
        name="check_and_approve",
        parents=["reserve_inventory"],
        retries=3,
        backoff_factor=2.0,
        backoff_base_seconds=2,
    )
    async def check_and_approve(self, context: Context) -> dict:
        """
        Conditional: if total_amount >= approval_threshold, spawn the
        ManagerApproval child workflow (which uses a durable event wait).
        Otherwise, auto-approve.
        """
        input_data = context.workflow_input()
        validate_result = (await context.task_output("validate_order"))
        total_amount = validate_result["total_amount"]
        approval_threshold = validate_result["approval_threshold"]

        if total_amount < approval_threshold:
            return {
                "approval_required": False,
                "decision": "auto_approved",
                "reason": (
                    f"Total {total_amount} below threshold {approval_threshold}"
                ),
            }

        # Spawn ManagerApproval child workflow
        order_id = input_data["order_id"]
        child_input = {
            "order_id": order_id,
            "customer_id": input_data["customer_id"],
            "total_amount": total_amount,
            "items": input_data["items"],
            "db_config": input_data.get("db_config") or DB_CONFIG,
        }

        child = context.spawn_workflow(
            "ManagerApproval",
            child_input,
            key=f"manager-approval-{order_id}",
        )
        result = await child.result()

        decision = result.get("decision", "unknown")

        if decision not in ("approved", "auto_approved"):
            reason = result.get("reason", "Order rejected or approval timed out")
            raise NonRetryableException(
                f"Order {order_id} not approved: decision={decision}, reason={reason}"
            )

        return {
            "approval_required": True,
            "decision": decision,
            "approver": result.get("approver"),
            "reason": result.get("reason", ""),
        }

    @hatchet.task(
        name="ship_order",
        parents=["check_and_approve"],
        retries=3,
        backoff_factor=2.0,
        backoff_base_seconds=2,
    )
    async def ship_order(self, context: Context) -> dict:
        """Spawn the ShipOrder child workflow and wait for the shipment result."""
        input_data = context.workflow_input()
        order_id = input_data["order_id"]

        child_input = {
            "order_id": order_id,
            "items": input_data["items"],
            "shipping_address": input_data["shipping_address"],
        }

        child = context.spawn_workflow(
            "ShipOrder",
            child_input,
            key=f"ship-order-{order_id}",
        )
        result = await child.result()

        return {
            "shipment_id": result.get("shipment_id"),
            "tracking_number": result.get("tracking_number"),
            "carrier": result.get("carrier"),
            "estimated_delivery": result.get("estimated_delivery"),
        }

    @hatchet.task(
        name="update_order_shipped",
        parents=["ship_order"],
        retries=3,
        backoff_factor=2.0,
        backoff_base_seconds=2,
    )
    async def update_order_shipped(self, context: Context) -> dict:
        """Record the shipment details in the orders table."""
        input_data = context.workflow_input()
        order_id = input_data["order_id"]
        db_config = input_data.get("db_config") or DB_CONFIG

        ship_result = (await context.task_output("ship_order"))
        shipment_id = ship_result.get("shipment_id")
        tracking_number = ship_result.get("tracking_number")

        conn = get_db_connection(db_config)
        try:
            cur = conn.cursor()
            now = datetime.now(timezone.utc)

            cur.execute(
                """
                UPDATE orders
                SET status = 'shipped',
                    shipment_id = COALESCE(%s, shipment_id),
                    tracking_number = COALESCE(%s, tracking_number),
                    updated_at = %s
                WHERE order_id = %s
                RETURNING order_id, status
                """,
                (shipment_id, tracking_number, now, order_id),
            )
            result = cur.fetchone()
            conn.commit()

            if not result:
                raise Exception(f"Order {order_id} not found")

            return {
                "order_id": result[0],
                "status": result[1],
                "updated_at": now.isoformat(),
            }
        finally:
            conn.close()

    @hatchet.task(
        name="send_shipped_notification",
        parents=["update_order_shipped"],
        retries=3,
        backoff_factor=2.0,
        backoff_base_seconds=2,
    )
    async def send_shipped_notification(self, context: Context) -> dict:
        """
        Send a shipped notification. Uses try/except so a notification failure
        does not fail the entire order workflow.
        """
        input_data = context.workflow_input()
        order_id = input_data["order_id"]

        try:
            ship_result = (await context.task_output("ship_order"))

            notification = {
                "order_id": order_id,
                "status": "shipped",
                "sent_at": datetime.now(timezone.utc).isoformat(),
                "channel": "simulated_email",
                "message": (
                    f"Your order {order_id} has been shipped! "
                    f"Tracking: {ship_result.get('tracking_number', 'N/A')} "
                    f"via {ship_result.get('carrier', 'N/A')}. "
                    f"Estimated delivery: {ship_result.get('estimated_delivery', 'N/A')}."
                ),
            }
            print(json.dumps(notification))

            return {
                "notification_sent": True,
                "order_id": order_id,
                "status": "shipped",
                "sent_at": notification["sent_at"],
            }
        except Exception as e:
            # Graceful degradation: order shipped, notification failed
            print(f"WARNING: Shipped notification failed for order {order_id}: {e}")
            return {
                "notification_sent": False,
                "order_id": order_id,
                "status": "shipped",
                "error": str(e),
            }

    # -------------------------------------------------------------------
    # Saga Compensation: on_failure handler
    # -------------------------------------------------------------------

    @hatchet.on_failure_task(name="compensate_order")
    async def compensate_order(self, context: Context) -> dict:
        """
        Saga compensation handler. Runs when any task in the workflow fails
        after exhausting retries. Performs three compensation steps:
        1. Release inventory
        2. Update order to cancelled
        3. Send cancellation notification

        Each step is best-effort with its own error handling.
        """
        input_data = context.workflow_input()
        order_id = input_data.get("order_id", "unknown")
        db_config = input_data.get("db_config") or DB_CONFIG

        failure_reason = str(context.task_run_error()) if hasattr(context, "task_run_error") else "Unknown error"

        compensation_results = {
            "order_id": order_id,
            "failure_reason": failure_reason,
            "release_inventory": "skipped",
            "update_order": "skipped",
            "send_notification": "skipped",
        }

        # Step 1: Release inventory
        try:
            conn = get_db_connection(db_config)
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

                if reservations:
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
                    compensation_results["release_inventory"] = f"released_{released}_items"
                else:
                    compensation_results["release_inventory"] = "no_reservations"
            finally:
                conn.close()
        except Exception as e:
            compensation_results["release_inventory"] = f"failed: {e}"
            print(f"COMPENSATION ERROR: release_inventory failed for {order_id}: {e}")

        # Step 2: Update order status to cancelled
        try:
            conn = get_db_connection(db_config)
            try:
                cur = conn.cursor()
                now = datetime.now(timezone.utc)

                cur.execute(
                    """
                    UPDATE orders
                    SET status = 'cancelled',
                        failure_reason = COALESCE(%s, failure_reason),
                        updated_at = %s
                    WHERE order_id = %s
                    RETURNING order_id, status
                    """,
                    (failure_reason, now, order_id),
                )
                result = cur.fetchone()
                conn.commit()

                if result:
                    compensation_results["update_order"] = "cancelled"
                else:
                    compensation_results["update_order"] = "order_not_found"
            finally:
                conn.close()
        except Exception as e:
            compensation_results["update_order"] = f"failed: {e}"
            print(f"COMPENSATION ERROR: update_order failed for {order_id}: {e}")

        # Step 3: Send cancellation notification
        try:
            notification = {
                "order_id": order_id,
                "status": "cancelled",
                "sent_at": datetime.now(timezone.utc).isoformat(),
                "channel": "simulated_email",
                "message": (
                    f"Your order {order_id} has been cancelled. "
                    f"Reason: {failure_reason}."
                ),
            }
            print(json.dumps(notification))
            compensation_results["send_notification"] = "sent"
        except Exception as e:
            compensation_results["send_notification"] = f"failed: {e}"
            print(
                f"COMPENSATION ERROR: send_cancellation_notification failed "
                f"for {order_id}: {e}"
            )

        return compensation_results

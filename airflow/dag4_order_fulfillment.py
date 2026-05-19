"""
DAG 4: Order Fulfillment with Saga Compensation

Validates an order, reserves inventory (TaskGroup), optionally routes through
manager approval (deferrable operator polling the approval-service), calls the
shipping API with typed retries, and updates the order.  On failure at any
post-reservation step, saga compensation releases the inventory.

Airflow idioms used:
- TaskFlow API (@task, @task.branch)
- TaskGroup for sub-workflow organisation
- Deferrable operator (ManagerApprovalOperator) with custom ApprovalTrigger
- Typed exceptions (InvalidAddress = non-retriable)
- on_failure_callback for saga compensation
- trigger_rule for best-effort notification
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg2
import requests
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.trigger_rule import TriggerRule

from triggers.approval_trigger import ApprovalTrigger

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DB_CONN_PARAMS = {
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


# ---------------------------------------------------------------------------
# Typed exceptions
# ---------------------------------------------------------------------------

class ShippingTimeout(AirflowException):
    """Retriable: shipping service timed out."""


class ShippingServiceError(AirflowException):
    """Retriable: shipping service returned 5xx."""


class InvalidAddress(AirflowException):
    """Non-retriable: the shipping address is invalid."""


class ApprovalRejected(AirflowException):
    """Non-retriable: manager rejected the order."""


class ApprovalExpired(AirflowException):
    """Non-retriable: approval request timed out."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(**DB_CONN_PARAMS)


def _release_inventory(order_id: str, log: Any = None) -> dict:
    """
    Saga compensation: reverse all inventory reservations for an order.
    Idempotent -- safe to call multiple times.
    """
    conn = _get_connection()
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
        if log:
            log.info("Released %d reservation(s) for order %s", released, order_id)
        return {"order_id": order_id, "released": released, "status": "inventory_released"}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _on_shipping_failure(context: dict) -> None:
    """on_failure_callback: release inventory when shipping fails."""
    dag_run = context["dag_run"]
    conf = dag_run.conf or {}
    order_id = conf.get("order_id", "unknown")
    log = context["task_instance"].log
    log.warning("Shipping failed for order %s -- triggering inventory release", order_id)
    try:
        _release_inventory(order_id, log=log)
    except Exception as exc:
        log.error("Saga compensation (inventory release) failed: %s", exc)


# ---------------------------------------------------------------------------
# Deferrable operator: Manager Approval
# ---------------------------------------------------------------------------

class ManagerApprovalOperator(BaseOperator):
    """
    1. POST to approval-service /approval-requests to create the request.
    2. Defer to ApprovalTrigger which polls /approval-requests/<id>.
    3. execute_complete() processes the decision.

    On rejection or timeout, this operator releases inventory (saga
    compensation) before raising the exception.
    """

    template_fields = ("order_id", "customer_id")

    def __init__(
        self,
        order_id: str,
        customer_id: str,
        approval_service_url: str = APPROVAL_SERVICE_URL,
        poll_interval: float = 5.0,
        approval_timeout: float = 180.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.order_id = order_id
        self.customer_id = customer_id
        self.approval_service_url = approval_service_url
        self.poll_interval = poll_interval
        self.approval_timeout = approval_timeout

    def execute(self, context: Any) -> None:
        """Submit approval request, then defer to trigger."""
        # Read runtime values from XCom / conf
        ti = context["task_instance"]
        validation = ti.xcom_pull(task_ids="validate_order") or {}
        total_amount = validation.get("total_amount", 0)

        conf = context["dag_run"].conf or context["params"]
        items = conf.get("items", [])
        items_summary = ", ".join(
            f"{item['quantity']}x {item['sku']}" for item in items
        )

        approval_request_id = f"APR-{uuid.uuid4().hex[:12].upper()}"

        payload = {
            "approval_request_id": approval_request_id,
            "order_id": self.order_id,
            "total_amount": total_amount,
            "customer_id": self.customer_id,
            "items_summary": items_summary,
        }

        self.log.info(
            "Submitting approval request %s for order %s ($%.2f)",
            approval_request_id,
            self.order_id,
            total_amount,
        )

        resp = requests.post(
            f"{self.approval_service_url}/approval-requests",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )

        if resp.status_code not in (200, 201):
            raise AirflowException(
                f"Approval Service returned {resp.status_code}: {resp.text[:500]}"
            )

        # Record in DB
        conn = _get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO approval_requests
                        (approval_request_id, order_id, total_amount, status)
                    VALUES (%s, %s, %s, 'pending')
                    ON CONFLICT (approval_request_id) DO NOTHING
                    """,
                    (approval_request_id, self.order_id, total_amount),
                )
                cur.execute(
                    "UPDATE orders SET status = 'pending_approval', updated_at = NOW() "
                    "WHERE order_id = %s",
                    (self.order_id,),
                )
            conn.commit()
        finally:
            conn.close()

        self.log.info("Deferring to ApprovalTrigger for %s", approval_request_id)

        self.defer(
            trigger=ApprovalTrigger(
                approval_request_id=approval_request_id,
                approval_service_url=self.approval_service_url,
                poll_interval=self.poll_interval,
                timeout=self.approval_timeout,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Any, event: dict) -> dict:
        """Process the approval trigger event."""
        status = event.get("status")
        decision = event.get("decision")

        # Record the decision in the DB
        approval_request_id = event.get("approval_request_id")
        conn = _get_connection()
        try:
            with conn.cursor() as cur:
                now = datetime.now(timezone.utc)
                if approval_request_id:
                    cur.execute(
                        """
                        UPDATE approval_requests
                        SET status = %s, approver = %s, reason = %s, decided_at = %s
                        WHERE approval_request_id = %s
                        """,
                        (
                            decision,
                            event.get("approver"),
                            event.get("reason", ""),
                            now,
                            approval_request_id,
                        ),
                    )
                new_order_status = "approved" if decision == "approved" else "rejected"
                cur.execute(
                    "UPDATE orders SET status = %s, updated_at = %s WHERE order_id = %s",
                    (new_order_status, now, self.order_id),
                )
            conn.commit()
        finally:
            conn.close()

        if decision == "approved":
            self.log.info("Order %s approved by %s", self.order_id, event.get("approver"))
            return event

        if decision == "rejected":
            self.log.warning("Order %s rejected -- releasing inventory", self.order_id)
            _release_inventory(self.order_id, log=self.log)
            raise ApprovalRejected(
                f"Order {self.order_id} rejected by {event.get('approver')}: "
                f"{event.get('reason', 'no reason given')}"
            )

        if status == "timeout" or decision == "expired":
            self.log.warning("Approval for order %s expired -- releasing inventory", self.order_id)
            _release_inventory(self.order_id, log=self.log)
            raise ApprovalExpired(
                f"Approval for order {self.order_id} timed out"
            )

        raise AirflowException(f"Unexpected approval event: {event}")


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    dag_id="dag4_order_fulfillment",
    description="Order Fulfillment: validate, reserve inventory, approve, ship, with saga compensation",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "orchestration",
        "retries": 0,
        "retry_delay": timedelta(seconds=5),
    },
    params={
        "order_id": "ORD-001",
        "customer_id": "CUST-001",
        "items": [
            {"sku": "SKU-A", "quantity": 2, "unit_price": 50.00},
            {"sku": "SKU-B", "quantity": 1, "unit_price": 75.00},
        ],
        "shipping_address": {
            "street": "123 Main St",
            "city": "Springfield",
            "state": "IL",
            "zip": "62701",
            "country": "US",
        },
        "approval_threshold": 500.00,
    },
    tags=["order", "fulfillment", "saga", "deferrable", "approval"],
)
def order_fulfillment():

    # ------------------------------------------------------------------
    # Step 1: Validate Order
    # ------------------------------------------------------------------
    @task()
    def validate_order(**context) -> dict:
        """Validate SKUs exist, customer is active, compute total."""
        conf = context["dag_run"].conf or context["params"]
        order_id = conf["order_id"]
        customer_id = conf["customer_id"]
        items = conf["items"]
        approval_threshold = conf.get("approval_threshold", 500.00)

        conn = _get_connection()
        try:
            cur = conn.cursor()

            cur.execute(
                "SELECT status FROM customers WHERE customer_id = %s",
                (customer_id,),
            )
            row = cur.fetchone()
            if not row:
                return {"is_valid": False, "reason": f"Customer {customer_id} not found"}
            if row[0] != "active":
                return {"is_valid": False, "reason": f"Customer {customer_id} is {row[0]}"}

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

            return {
                "is_valid": True,
                "reason": None,
                "total_amount": total_amount,
                "approval_threshold": approval_threshold,
            }
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Step 2: Reserve Inventory (TaskGroup)
    # ------------------------------------------------------------------
    @task_group(group_id="reserve_inventory")
    def reserve_inventory_group():

        @task()
        def reserve_items(**context) -> dict:
            """Atomically reserve all items. Idempotent."""
            conf = context["dag_run"].conf or context["params"]
            order_id = conf["order_id"]
            customer_id = conf["customer_id"]
            items = conf["items"]

            reservation_id = f"RES-{uuid.uuid4().hex[:12].upper()}"

            conn = _get_connection()
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
                        raise AirflowException(
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

                # Create/update order record
                total = sum(i["quantity"] * i["unit_price"] for i in items)
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

        return reserve_items()

    # ------------------------------------------------------------------
    # Step 3: Check if manager approval is required
    # ------------------------------------------------------------------
    @task.branch()
    def check_approval_required(validation: dict) -> str:
        """Branch: require manager approval for high-value orders."""
        total = validation.get("total_amount", 0)
        threshold = validation.get("approval_threshold", 500.00)

        if total >= threshold:
            return "manager_approval"
        return "call_shipping_api"

    # ------------------------------------------------------------------
    # Step 4: Manager Approval (deferrable operator)
    # ------------------------------------------------------------------
    manager_approval_op = ManagerApprovalOperator(
        task_id="manager_approval",
        order_id="{{ (dag_run.conf or params).order_id }}",
        customer_id="{{ (dag_run.conf or params).customer_id }}",
        approval_service_url=APPROVAL_SERVICE_URL,
        poll_interval=5.0,
        approval_timeout=180.0,
    )

    # ------------------------------------------------------------------
    # Step 5: Call Shipping API
    # ------------------------------------------------------------------
    @task(
        task_id="call_shipping_api",
        retries=3,
        retry_delay=timedelta(seconds=3),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(seconds=15),
        on_failure_callback=_on_shipping_failure,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    def call_shipping_api(**context) -> dict:
        """Call the shipping service. Typed exceptions for retry routing."""
        conf = context["dag_run"].conf or context["params"]
        order_id = conf["order_id"]
        items = conf["items"]
        shipping_address = conf["shipping_address"]

        idempotency_key = f"{order_id}-ship"

        payload = {
            "order_id": order_id,
            "items": items,
            "shipping_address": shipping_address,
            "idempotency_key": idempotency_key,
        }

        resp = requests.post(
            f"{SHIPPING_SERVICE_URL}/shipments",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )

        body = resp.json()

        if resp.status_code == 200:
            return body

        error_type = "Unknown"
        message = str(body)
        if isinstance(body.get("detail"), dict):
            error_type = body["detail"].get("error_type", "Unknown")
            message = body["detail"].get("message", str(body))

        if error_type == "InvalidAddress":
            raise InvalidAddress(message)
        elif error_type == "ShippingTimeout" or resp.status_code == 504:
            raise ShippingTimeout(message)
        elif error_type == "ShippingServiceError" or resp.status_code >= 500:
            raise ShippingServiceError(message)
        else:
            raise AirflowException(
                f"Unexpected shipping error ({resp.status_code}): {message}"
            )

    # ------------------------------------------------------------------
    # Step 6: Update order status
    # ------------------------------------------------------------------
    @task(
        retries=3,
        retry_delay=timedelta(seconds=2),
        retry_exponential_backoff=True,
    )
    def update_order_status(shipment: dict, **context) -> dict:
        """Mark the order as shipped in the database."""
        conf = context["dag_run"].conf or context["params"]
        order_id = conf["order_id"]

        conn = _get_connection()
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
                (
                    shipment.get("shipment_id"),
                    shipment.get("tracking_number"),
                    now,
                    order_id,
                ),
            )
            result = cur.fetchone()
            conn.commit()

            if not result:
                raise AirflowException(f"Order {order_id} not found")

            return {
                "order_id": result[0],
                "status": result[1],
                "updated_at": now.isoformat(),
                "shipment_id": shipment.get("shipment_id"),
                "tracking_number": shipment.get("tracking_number"),
                "carrier": shipment.get("carrier"),
                "estimated_delivery": shipment.get("estimated_delivery"),
            }
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Step 7: Send notification (best-effort)
    # ------------------------------------------------------------------
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def send_notification(order_result: dict, **context) -> dict:
        """Best-effort order notification. Runs even if upstream had issues."""
        conf = context["dag_run"].conf or context["params"]
        order_id = conf["order_id"]
        status = order_result.get("status", "unknown")

        notification = {
            "order_id": order_id,
            "status": status,
            "sent_at": datetime.now(timezone.utc).isoformat(),
            "channel": "simulated_email",
        }

        if status == "shipped":
            notification["message"] = (
                f"Your order {order_id} has been shipped! "
                f"Tracking: {order_result.get('tracking_number', 'N/A')} "
                f"via {order_result.get('carrier', 'N/A')}. "
                f"Estimated delivery: {order_result.get('estimated_delivery', 'N/A')}."
            )
        elif status == "cancelled":
            notification["message"] = (
                f"Your order {order_id} has been cancelled. "
                f"Reason: {order_result.get('failure_reason', 'N/A')}."
            )
        else:
            notification["message"] = f"Order {order_id} status update: {status}."

        print(json.dumps(notification))

        return {
            "notification_sent": True,
            "order_id": order_id,
            "status": status,
            "sent_at": notification["sent_at"],
        }

    # ------------------------------------------------------------------
    # Step 8: Saga compensation (safety net)
    # ------------------------------------------------------------------
    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def release_inventory_compensation(**context) -> dict:
        """
        Explicit saga compensation task. Triggered when any upstream task
        in the success path fails. The on_failure_callback on shipping
        provides immediate compensation; this task is a safety net that
        also marks the order as cancelled.
        """
        conf = context["dag_run"].conf or context["params"]
        order_id = conf["order_id"]

        result = _release_inventory(order_id, log=context["task_instance"].log)

        # Also update order status to cancelled
        conn = _get_connection()
        try:
            cur = conn.cursor()
            now = datetime.now(timezone.utc)
            cur.execute(
                """
                UPDATE orders
                SET status = 'cancelled',
                    failure_reason = 'Saga compensation triggered',
                    updated_at = %s
                WHERE order_id = %s AND status != 'shipped'
                """,
                (now, order_id),
            )
            conn.commit()
        finally:
            conn.close()

        result["order_status"] = "cancelled"
        return result

    # ------------------------------------------------------------------
    # Wire the DAG
    # ------------------------------------------------------------------

    # Linear flow: validate -> reserve -> branch
    validation = validate_order()
    reservation = reserve_inventory_group()
    branch = check_approval_required(validation)

    validation >> reservation >> branch

    # Branch targets: either approval or straight to shipping
    branch >> manager_approval_op
    branch >> call_shipping_api

    # After approval, go to shipping
    manager_approval_op >> call_shipping_api

    # Shipping -> update order -> notification (success path)
    shipment = call_shipping_api()
    order_result = update_order_status(shipment)
    notification = send_notification(order_result)

    # Saga compensation: triggered if shipping or approval fails
    compensation = release_inventory_compensation()
    [manager_approval_op, call_shipping_api] >> compensation


order_fulfillment()

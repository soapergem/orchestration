"""
DAG 4: Order Fulfillment — Luigi Implementation

Pipeline:
    ValidateOrder -> ReserveInventory -> [ManagerApproval if high-value]
    -> CallShippingAPI -> UpdateOrderStatus -> SendOrderNotification

    Compensation (saga): If shipping fails, release inventory and cancel order.

Mirrors the Step Functions implementation in step-functions/dag4-order-fulfillment/.

MAJOR DIVERGENCES FROM STEP FUNCTIONS:
- Luigi has no saga, compensation, or on-failure mechanism. Step Functions uses
  Catch blocks to route failures to compensation states (ReleaseInventory ->
  UpdateOrderCancelled -> SendCancellationNotification). In Luigi, compensation
  is ad-hoc Python code in the task's run() method via try/except. This is
  fragile: if the process crashes mid-compensation, there is no automatic
  recovery — the inventory stays reserved permanently.
- Luigi has no sub-workflow concept. Step Functions composes sub-state-machines
  (manager-approval.asl.json, reserve-inventory.asl.json, shipping.asl.json)
  via nested execution. In Luigi, these are just regular tasks.
- Luigi blocks a worker for the entire approval wait. Step Functions uses
  .waitForTaskToken to suspend at zero cost. For a real 72-hour SLA approval,
  Luigi would block a worker for 72 hours — completely impractical.
- Luigi has no Choice state equivalent. Conditional branching (e.g., "skip
  approval for low-value orders") is handled by conditional requires() logic
  in Python, which is less visible than Step Functions' declarative routing.
- Luigi provides no framework-level error handling, retry policies, or
  compensation mechanisms.

Run with:
    luigi --module dag4_order_fulfillment SendOrderNotification \
        --order-id ORD-001 \
        --customer-id CUST-001 \
        --items-json '[{"sku":"SKU-A","quantity":2,"unit_price":100.00}]' \
        --shipping-address-json '{"street":"123 Main St","city":"NYC","zip":"10001"}' \
        --run-id my-run-001 \
        --workers 1
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

import luigi
import psycopg2
import urllib3


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

MARKER_DIR = os.environ.get("LUIGI_MARKER_DIR", "/tmp/luigi-markers/dag4")

http = urllib3.PoolManager()


def get_db_connection():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        database=DB_CONFIG["database"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )


# ---------------------------------------------------------------------------
# Task 1: ValidateOrder
# ---------------------------------------------------------------------------


class ValidateOrder(luigi.Task):
    """
    Validates that all SKUs exist, customer is active, and computes total amount.
    Read-only — no mutations, so no compensation needed on failure.

    Mirrors step-functions/dag4-order-fulfillment/lambdas/validate_order.py.
    """

    order_id = luigi.Parameter()
    customer_id = luigi.Parameter()
    items_json = luigi.Parameter(description="JSON array of order items")
    shipping_address_json = luigi.Parameter(description="JSON object of shipping address")
    run_id = luigi.Parameter()
    approval_threshold = luigi.FloatParameter(default=500.00)

    def output(self):
        return luigi.LocalTarget(
            os.path.join(MARKER_DIR, self.run_id, "validate_order.json")
        )

    def run(self):
        items = json.loads(self.items_json)

        conn = get_db_connection()
        try:
            cur = conn.cursor()

            # Check customer exists and is active
            cur.execute(
                "SELECT status FROM customers WHERE customer_id = %s",
                (self.customer_id,),
            )
            row = cur.fetchone()
            if not row:
                result = {
                    "validation": {
                        "is_valid": False,
                        "reason": f"Customer {self.customer_id} not found",
                    }
                }
                self._write_output(result)
                return
            if row[0] != "active":
                result = {
                    "validation": {
                        "is_valid": False,
                        "reason": f"Customer {self.customer_id} is {row[0]}",
                    }
                }
                self._write_output(result)
                return

            # Check inventory for each item
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
                    self._write_output(
                        {
                            "validation": {
                                "is_valid": False,
                                "reason": f"SKU {sku} not found",
                            }
                        }
                    )
                    return

                available, unit_price = row
                if available < quantity:
                    self._write_output(
                        {
                            "validation": {
                                "is_valid": False,
                                "reason": (
                                    f"Insufficient stock for {sku}: "
                                    f"requested {quantity}, available {available}"
                                ),
                            }
                        }
                    )
                    return

                total_amount += float(unit_price) * quantity

        finally:
            conn.close()

        result = {
            "order_id": self.order_id,
            "customer_id": self.customer_id,
            "items": items,
            "shipping_address": json.loads(self.shipping_address_json),
            "total_amount": total_amount,
            "approval_threshold": self.approval_threshold,
            "validation": {"is_valid": True, "reason": None},
        }
        self._write_output(result)

    def _write_output(self, result):
        result.setdefault("order_id", self.order_id)
        result.setdefault("customer_id", self.customer_id)
        result.setdefault("items", json.loads(self.items_json))
        result.setdefault("shipping_address", json.loads(self.shipping_address_json))
        result.setdefault("approval_threshold", self.approval_threshold)

        os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
        with self.output().open("w") as f:
            json.dump(result, f)


# ---------------------------------------------------------------------------
# Task 2: ReserveInventory
# ---------------------------------------------------------------------------


class ReserveInventory(luigi.Task):
    """
    Atomically reserves inventory for all items in the order.

    DIVERGENCE: In Step Functions, this is a nested sub-state-machine
    (reserve-inventory.asl.json) invoked via startExecution.sync:2. Luigi has
    no sub-workflow concept — this is just a regular task. The Step Functions
    sub-workflow has its own Retry (MaxAttempts=3, BackoffRate=2.0). Here we
    rely on Postgres transaction atomicity but have no retry.
    """

    order_id = luigi.Parameter()
    customer_id = luigi.Parameter()
    items_json = luigi.Parameter()
    shipping_address_json = luigi.Parameter()
    run_id = luigi.Parameter()
    approval_threshold = luigi.FloatParameter(default=500.00)

    def requires(self):
        return ValidateOrder(
            order_id=self.order_id,
            customer_id=self.customer_id,
            items_json=self.items_json,
            shipping_address_json=self.shipping_address_json,
            run_id=self.run_id,
            approval_threshold=self.approval_threshold,
        )

    def output(self):
        return luigi.LocalTarget(
            os.path.join(MARKER_DIR, self.run_id, "reserve_inventory.json")
        )

    def run(self):
        with self.input().open("r") as f:
            order_data = json.load(f)

        if not order_data["validation"]["is_valid"]:
            raise Exception(
                f"Order validation failed: {order_data['validation']['reason']}"
            )

        items = order_data["items"]
        reservation_id = f"RES-{uuid.uuid4().hex[:12].upper()}"

        conn = get_db_connection()
        try:
            cur = conn.cursor()

            # Check for existing reservation (idempotency)
            cur.execute(
                "SELECT reservation_id FROM inventory_reservations "
                "WHERE order_id = %s AND status = 'reserved' LIMIT 1",
                (self.order_id,),
            )
            existing = cur.fetchone()
            if existing:
                result = {
                    **order_data,
                    "reservation": {
                        "reservation_id": existing[0],
                        "items_reserved": [i["sku"] for i in items],
                        "reserved_at": datetime.now(timezone.utc).isoformat(),
                        "idempotent": True,
                    },
                }
                self._write_output(result)
                return

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
                    (f"{reservation_id}-{sku}", self.order_id, sku, quantity),
                )
                items_reserved.append(sku)

            # Create the order record
            total = sum(i["quantity"] * i["unit_price"] for i in items)
            cur.execute(
                """
                INSERT INTO orders (order_id, customer_id, total_amount, status)
                VALUES (%s, %s, %s, 'reserved')
                ON CONFLICT (order_id) DO UPDATE SET status = 'reserved', updated_at = NOW()
                """,
                (self.order_id, self.customer_id, total),
            )

            conn.commit()

            result = {
                **order_data,
                "reservation": {
                    "reservation_id": reservation_id,
                    "items_reserved": items_reserved,
                    "reserved_at": datetime.now(timezone.utc).isoformat(),
                },
            }

        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        self._write_output(result)

    def _write_output(self, result):
        os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
        with self.output().open("w") as f:
            json.dump(result, f)


# ---------------------------------------------------------------------------
# Task 3: ManagerApproval (blocking poll)
# ---------------------------------------------------------------------------


class ManagerApproval(luigi.Task):
    """
    Submits an approval request to the approval-service and polls until
    a decision is made.

    DIVERGENCE: Luigi blocks a worker for the entire approval wait. Step
    Functions uses .waitForTaskToken to suspend the execution at zero cost
    until the approval-service calls back. For a real 72-hour approval SLA,
    Luigi would block a worker for 72 hours — this is completely impractical.
    The Step Functions model is fundamentally better here: it suspends with
    no resource consumption and resumes only when the callback arrives.

    We use a 120-second timeout for testing. In production, this blocking
    approach is not viable for human-in-the-loop workflows.
    """

    order_id = luigi.Parameter()
    customer_id = luigi.Parameter()
    items_json = luigi.Parameter()
    shipping_address_json = luigi.Parameter()
    run_id = luigi.Parameter()
    approval_threshold = luigi.FloatParameter(default=500.00)
    poll_interval = luigi.IntParameter(default=5)
    poll_timeout = luigi.IntParameter(default=120)

    def requires(self):
        return ReserveInventory(
            order_id=self.order_id,
            customer_id=self.customer_id,
            items_json=self.items_json,
            shipping_address_json=self.shipping_address_json,
            run_id=self.run_id,
            approval_threshold=self.approval_threshold,
        )

    def output(self):
        return luigi.LocalTarget(
            os.path.join(MARKER_DIR, self.run_id, "manager_approval.json")
        )

    def run(self):
        with self.input().open("r") as f:
            order_data = json.load(f)

        total_amount = order_data.get("total_amount", 0)
        threshold = order_data.get("approval_threshold", self.approval_threshold)

        # If below threshold, skip approval — mirrors CheckApprovalRequired Choice state
        if total_amount < threshold:
            result = {
                **order_data,
                "approval": {
                    "decision": "not_required",
                    "reason": (
                        f"Order total {total_amount} below threshold {threshold}"
                    ),
                },
            }
            self._write_output(result)
            return

        # Submit approval request
        items = order_data["items"]
        approval_request_id = f"APR-{uuid.uuid4().hex[:12].upper()}"

        items_summary = ", ".join(
            f"{item['quantity']}x {item['sku']}" for item in items
        )

        payload = {
            "approval_request_id": approval_request_id,
            "order_id": self.order_id,
            "total_amount": total_amount,
            "customer_id": self.customer_id,
            "items_summary": items_summary,
        }

        response = http.request(
            "POST",
            f"{APPROVAL_SERVICE_URL}/approval-requests",
            body=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10.0,
        )

        if response.status != 201:
            raise Exception(
                f"Approval Service returned {response.status}: "
                f"{response.data.decode('utf-8')[:500]}"
            )

        # Record the approval request in DB
        try:
            conn = get_db_connection()
            try:
                cur = conn.cursor()
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
        except Exception:
            pass  # Best-effort DB update

        # ---------------------------------------------------------------
        # DIVERGENCE: Blocking poll loop.
        # Step Functions suspends at zero cost via .waitForTaskToken.
        # Luigi blocks a worker for the entire duration. For a real 72-hour
        # approval SLA, this is completely impractical — a worker thread
        # sits idle for days. Use 120s timeout for testing.
        # ---------------------------------------------------------------
        start_time = time.time()

        while True:
            elapsed = time.time() - start_time
            if elapsed > self.poll_timeout:
                # Timeout — treat as expired (mirrors ApprovalExpired state)
                result = {
                    **order_data,
                    "approval": {
                        "decision": "expired",
                        "approver": None,
                        "reason": "Approval request timed out",
                        "decided_at": None,
                        "approval_request_id": approval_request_id,
                    },
                }
                self._write_output(result)
                return

            time.sleep(self.poll_interval)

            status_response = http.request(
                "GET",
                f"{APPROVAL_SERVICE_URL}/approval-requests/{approval_request_id}",
                headers={"Content-Type": "application/json"},
                timeout=10.0,
            )

            if status_response.status == 200:
                status_data = json.loads(status_response.data.decode("utf-8"))
                decision = status_data.get("decision") or status_data.get("status")

                if decision in ("approved", "rejected"):
                    # Record the decision in DB
                    try:
                        conn = get_db_connection()
                        try:
                            cur = conn.cursor()
                            now = datetime.now(timezone.utc)
                            cur.execute(
                                """
                                UPDATE approval_requests
                                SET status = %s, approver = %s, reason = %s, decided_at = %s
                                WHERE approval_request_id = %s
                                """,
                                (
                                    decision,
                                    status_data.get("approver"),
                                    status_data.get("reason", ""),
                                    now,
                                    approval_request_id,
                                ),
                            )
                            new_status = "approved" if decision == "approved" else "rejected"
                            cur.execute(
                                "UPDATE orders SET status = %s, updated_at = %s WHERE order_id = %s",
                                (new_status, now, self.order_id),
                            )
                            conn.commit()
                        finally:
                            conn.close()
                    except Exception:
                        pass  # Best-effort

                    result = {
                        **order_data,
                        "approval": {
                            "decision": decision,
                            "approver": status_data.get("approver"),
                            "reason": status_data.get("reason"),
                            "decided_at": datetime.now(timezone.utc).isoformat(),
                            "approval_request_id": approval_request_id,
                        },
                    }
                    self._write_output(result)
                    return

    def _write_output(self, result):
        os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
        with self.output().open("w") as f:
            json.dump(result, f)


# ---------------------------------------------------------------------------
# Task 4: CallShippingAPI (with manual retry and saga compensation)
# ---------------------------------------------------------------------------


class CallShippingAPI(luigi.Task):
    """
    Calls the shipping-service API. Implements manual retry with backoff and
    saga compensation on failure.

    DIVERGENCE: Luigi has no saga, compensation, or on-failure mechanism.
    Compensation is ad-hoc Python code in the task's run() method. If shipping
    fails after all retries, we manually call release_inventory() and
    update_order_status() in the except block. This is fragile:
      - If the process crashes during compensation, inventory stays reserved.
      - If compensation itself fails, there is no automatic retry of the
        compensation (Step Functions retries ReleaseInventory with MaxAttempts=5).
      - There is no CompensationFailed terminal state — we can only log and raise.

    Step Functions handles this with a declarative Catch -> CompensateFromShipping
    -> ReleaseInventory -> UpdateOrderCancelled -> SendCancellationNotification
    chain, with each step having its own retry policy.
    """

    order_id = luigi.Parameter()
    customer_id = luigi.Parameter()
    items_json = luigi.Parameter()
    shipping_address_json = luigi.Parameter()
    run_id = luigi.Parameter()
    approval_threshold = luigi.FloatParameter(default=500.00)
    max_retries = luigi.IntParameter(default=3)

    def requires(self):
        return ManagerApproval(
            order_id=self.order_id,
            customer_id=self.customer_id,
            items_json=self.items_json,
            shipping_address_json=self.shipping_address_json,
            run_id=self.run_id,
            approval_threshold=self.approval_threshold,
        )

    def output(self):
        return luigi.LocalTarget(
            os.path.join(MARKER_DIR, self.run_id, "call_shipping.json")
        )

    def run(self):
        with self.input().open("r") as f:
            order_data = json.load(f)

        # Check approval decision — mirrors CheckApprovalDecision Choice state
        approval = order_data.get("approval", {})
        decision = approval.get("decision", "not_required")

        if decision == "rejected":
            # Compensation path: release inventory and cancel order
            self._compensate(
                order_data, "Order rejected by manager approval"
            )
            raise Exception("Order rejected by manager approval")

        if decision == "expired":
            self._compensate(
                order_data, "Approval request timed out"
            )
            raise Exception("Approval request timed out")

        # Proceed with shipping (approved or not_required)
        items = order_data["items"]
        shipping_address = order_data["shipping_address"]
        idempotency_key = f"{self.order_id}-ship"

        payload = {
            "order_id": self.order_id,
            "items": items,
            "shipping_address": shipping_address,
            "idempotency_key": idempotency_key,
        }

        # Manual retry loop with exponential backoff
        last_exception = None
        for attempt in range(self.max_retries):
            try:
                response = http.request(
                    "POST",
                    f"{SHIPPING_SERVICE_URL}/shipments",
                    body=json.dumps(payload),
                    headers={"Content-Type": "application/json"},
                    timeout=30.0,
                )

                body = json.loads(response.data.decode("utf-8"))

                if response.status == 200:
                    result = {
                        **order_data,
                        "shipment": {
                            "shipment_id": body.get("shipment_id"),
                            "tracking_number": body.get("tracking_number"),
                            "carrier": body.get("carrier"),
                            "estimated_delivery": body.get("estimated_delivery"),
                        },
                    }
                    self._write_output(result)
                    return

                # Parse error type
                error_detail = body.get("detail", {})
                if isinstance(error_detail, dict):
                    error_type = error_detail.get("error_type", "Unknown")
                    message = error_detail.get("message", str(body))
                else:
                    error_type = "Unknown"
                    message = str(body)

                # InvalidAddress is not retriable
                if error_type == "InvalidAddress":
                    self._compensate(order_data, f"Invalid address: {message}")
                    raise Exception(f"InvalidAddress: {message}")

                # Retriable errors
                raise Exception(
                    f"Shipping error ({error_type}, status {response.status}): {message}"
                )

            except Exception as e:
                last_exception = e
                # Don't retry non-retriable errors
                if "InvalidAddress" in str(e):
                    raise

                if attempt < self.max_retries - 1:
                    delay = min(3 * (2 ** attempt), 15)
                    delay = random.uniform(0, delay)
                    time.sleep(delay)

        # ---------------------------------------------------------------
        # All retries exhausted — execute saga compensation.
        #
        # DIVERGENCE: Luigi has no saga, compensation, or on-failure
        # mechanism. Compensation is ad-hoc Python code in the task's
        # run() method. If the process crashes here, inventory stays
        # reserved forever.
        # ---------------------------------------------------------------
        self._compensate(
            order_data,
            f"Shipping failed after {self.max_retries} retries: {last_exception}",
        )
        raise last_exception

    def _compensate(self, order_data, failure_reason):
        """
        Saga compensation: release inventory and cancel order.

        DIVERGENCE: In Step Functions, this is a chain of dedicated states:
            CompensateFromShipping -> ReleaseInventory -> UpdateOrderCancelled
            -> SendCancellationNotification
        Each with its own retry policy (ReleaseInventory: MaxAttempts=5,
        BackoffRate=2.0). If compensation fails, it routes to CompensationFailed.

        Here, compensation is ad-hoc code in the except path. If release_inventory
        or update_order fail, we log the error but cannot automatically retry.
        There is no CompensationFailed terminal state — we can only log.
        """
        reservation = order_data.get("reservation", {})
        reservation_id = reservation.get("reservation_id")

        # Step 1: Release inventory
        try:
            self._release_inventory(order_data, failure_reason)
        except Exception as e:
            # DIVERGENCE: Step Functions would route to CompensationFailed here.
            # We can only log the failure — manual intervention required.
            print(
                f"CRITICAL: Saga compensation failed for order {self.order_id}. "
                f"Inventory reservation {reservation_id} may be stuck. "
                f"Error: {e}. Manual intervention required."
            )
            return

        # Step 2: Update order status to cancelled
        try:
            self._update_order_cancelled(failure_reason)
        except Exception as e:
            print(
                f"WARNING: Could not update order {self.order_id} status to cancelled: {e}"
            )

        # Step 3: Send cancellation notification (best-effort)
        try:
            self._send_cancellation_notification(failure_reason)
        except Exception as e:
            print(
                f"WARNING: Could not send cancellation notification for {self.order_id}: {e}"
            )

    def _release_inventory(self, order_data, failure_reason):
        """
        Reverses inventory reservations for the order.
        Mirrors step-functions/dag4-order-fulfillment/lambdas/release_inventory.py.
        """
        conn = get_db_connection()
        try:
            cur = conn.cursor()

            cur.execute(
                """
                SELECT reservation_id, sku, quantity
                FROM inventory_reservations
                WHERE order_id = %s AND status = 'reserved'
                """,
                (self.order_id,),
            )
            reservations = cur.fetchall()

            if not reservations:
                return

            for res_id, sku, quantity in reservations:
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
                    (datetime.now(timezone.utc), res_id),
                )

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _update_order_cancelled(self, failure_reason):
        """Update order status to cancelled."""
        conn = get_db_connection()
        try:
            cur = conn.cursor()
            now = datetime.now(timezone.utc)
            cur.execute(
                """
                UPDATE orders
                SET status = 'cancelled',
                    failure_reason = %s,
                    updated_at = %s
                WHERE order_id = %s
                """,
                (failure_reason[:1000], now, self.order_id),
            )
            conn.commit()
        finally:
            conn.close()

    def _send_cancellation_notification(self, failure_reason):
        """Send a simulated cancellation notification."""
        notification = {
            "order_id": self.order_id,
            "status": "cancelled",
            "message": (
                f"Your order {self.order_id} has been cancelled. "
                f"Reason: {failure_reason}"
            ),
            "sent_at": datetime.now(timezone.utc).isoformat(),
            "channel": "simulated_email",
        }
        print(f"CANCELLATION NOTIFICATION: {json.dumps(notification)}")

    def _write_output(self, result):
        os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
        with self.output().open("w") as f:
            json.dump(result, f)


# ---------------------------------------------------------------------------
# Task 5: UpdateOrderStatus
# ---------------------------------------------------------------------------


class UpdateOrderStatus(luigi.Task):
    """
    Updates the order record in the database to 'shipped'.

    Mirrors step-functions/dag4-order-fulfillment/lambdas/update_order_status.py.
    """

    order_id = luigi.Parameter()
    customer_id = luigi.Parameter()
    items_json = luigi.Parameter()
    shipping_address_json = luigi.Parameter()
    run_id = luigi.Parameter()
    approval_threshold = luigi.FloatParameter(default=500.00)

    def requires(self):
        return CallShippingAPI(
            order_id=self.order_id,
            customer_id=self.customer_id,
            items_json=self.items_json,
            shipping_address_json=self.shipping_address_json,
            run_id=self.run_id,
            approval_threshold=self.approval_threshold,
        )

    def output(self):
        return luigi.LocalTarget(
            os.path.join(MARKER_DIR, self.run_id, "update_order_status.json")
        )

    def run(self):
        with self.input().open("r") as f:
            order_data = json.load(f)

        shipment = order_data.get("shipment", {})

        conn = get_db_connection()
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
                    self.order_id,
                ),
            )
            result_row = cur.fetchone()
            conn.commit()

            if not result_row:
                raise Exception(f"Order {self.order_id} not found")

            result = {
                **order_data,
                "order_status_update": {
                    "order_id": result_row[0],
                    "status": result_row[1],
                    "updated_at": now.isoformat(),
                },
            }
        finally:
            conn.close()

        os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
        with self.output().open("w") as f:
            json.dump(result, f)


# ---------------------------------------------------------------------------
# Task 6: SendOrderNotification
# ---------------------------------------------------------------------------


class SendOrderNotification(luigi.Task):
    """
    Sends a simulated notification for the shipped order.

    DIVERGENCE: In Step Functions, if SendShippedNotification fails, the
    Catch routes to NotificationFailed (a Pass state that succeeds anyway).
    The order is still considered shipped. In Luigi, we use try/except for
    the same graceful degradation.
    """

    order_id = luigi.Parameter()
    customer_id = luigi.Parameter()
    items_json = luigi.Parameter()
    shipping_address_json = luigi.Parameter()
    run_id = luigi.Parameter()
    approval_threshold = luigi.FloatParameter(default=500.00)

    def requires(self):
        return UpdateOrderStatus(
            order_id=self.order_id,
            customer_id=self.customer_id,
            items_json=self.items_json,
            shipping_address_json=self.shipping_address_json,
            run_id=self.run_id,
            approval_threshold=self.approval_threshold,
        )

    def output(self):
        return luigi.LocalTarget(
            os.path.join(MARKER_DIR, self.run_id, "send_notification.json")
        )

    def run(self):
        with self.input().open("r") as f:
            order_data = json.load(f)

        shipment = order_data.get("shipment", {})
        notification_result = {}

        try:
            notification = {
                "order_id": self.order_id,
                "status": "shipped",
                "message": (
                    f"Your order {self.order_id} has been shipped! "
                    f"Tracking: {shipment.get('tracking_number', 'N/A')} "
                    f"via {shipment.get('carrier', 'N/A')}. "
                    f"Estimated delivery: {shipment.get('estimated_delivery', 'N/A')}."
                ),
                "sent_at": datetime.now(timezone.utc).isoformat(),
                "channel": "simulated_email",
            }

            print(f"ORDER NOTIFICATION: {json.dumps(notification)}")

            notification_result = {
                "notification_sent": True,
                "order_id": self.order_id,
                "status": "shipped",
                "sent_at": notification["sent_at"],
            }

        except Exception as e:
            # DIVERGENCE: Graceful degradation via try/except.
            # Step Functions Catch -> NotificationFailed Pass state handles this
            # declaratively. The order is still shipped even if notification fails.
            print(
                f"WARNING: Notification failed for order {self.order_id}: {e}. "
                f"Order was still shipped successfully."
            )
            notification_result = {
                "notification_sent": False,
                "order_id": self.order_id,
                "status": "shipped",
                "notification_error": str(e),
                "note": "Order shipped successfully but notification failed.",
            }

        os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
        with self.output().open("w") as f:
            json.dump(notification_result, f)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    luigi.run()

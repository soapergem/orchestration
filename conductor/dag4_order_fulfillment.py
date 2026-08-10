"""
DAG 4: Order Fulfillment with Human Approval and Saga Compensation (Conductor)

Reserve inventory -> (maybe) human approval, suspending -> ship -> with true
rollback of the reservation on rejection, timeout, or shipping failure.

Conductor idioms demonstrated:
- `SUB_WORKFLOW` composition: the three sub-workflows the spec asks for are
  separate registered definitions (`workflows/dag4_*.json`), independently
  versioned and independently startable. A sub-workflow failure surfaces as a
  failed task in the parent.
- `failureWorkflow`: the saga trigger for *unplanned* failure. When the main
  workflow dies -- shipping exhausted its retries, the approval WAIT timed out --
  Conductor starts `dag4_compensation` and hands it the failed workflow's ENTIRE
  input map plus `reason` / `failureStatus` / `workflowId`. That inherited input
  is what makes the compensation addressable: it already knows the order_id.
- `SWITCH` + explicit compensation SUB_WORKFLOW for *planned* rollback (an
  approval rejection), so the graph shows the compensation rather than hiding it
  behind an error handler.
- `WAIT` + `timeoutSeconds` on the task definition for the human approval, so
  the approval costs no worker while it is pending.

Both saga triggers are exercised deliberately -- see conductor/README.md.
Compensation is idempotent by construction: `release_inventory` only acts on
reservations still in `reserved` status, so a double compensation is a no-op.
"""

import os
import uuid

import psycopg2
import requests
from conductor.client.worker.exception import NonRetryableException
from conductor.client.worker.worker_task import worker_task

DB_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": int(os.environ.get("POSTGRES_PORT", "5432")),
    "dbname": os.environ.get("POSTGRES_DB", "orchestration"),
    "user": os.environ.get("POSTGRES_USER", "orchestration"),
    "password": os.environ.get("POSTGRES_PASSWORD", "orchestration"),
}

BAKEOFF_NS = os.environ.get("BAKEOFF_NS", "conductor")
SCHEMA = f"{BAKEOFF_NS}_dag4"

APPROVAL_SERVICE_URL = os.environ.get("APPROVAL_SERVICE_URL", "http://approval-service:8091")
SHIPPING_SERVICE_URL = os.environ.get("SHIPPING_SERVICE_URL", "http://shipping-service:8092")
CONDUCTOR_INTERNAL_URL = os.environ.get(
    "CONDUCTOR_INTERNAL_URL", "http://conductor-server:8080"
)

APPROVAL_THRESHOLD = float(os.environ.get("APPROVAL_THRESHOLD", "500"))


def get_db_connection() -> psycopg2.extensions.connection:
    """Connect with search_path pinned to this runner's DAG 4 schema.

    Like DAG 3, this fails fast rather than self-creating: the customers and
    inventory fixtures must already be seeded (`just seed conductor`).
    """
    conn = psycopg2.connect(**DB_CONFIG)
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name = %s", (SCHEMA,))
        if cur.fetchone() is None:
            conn.close()
            raise RuntimeError(
                f'schema "{SCHEMA}" is missing -- run `just seed {BAKEOFF_NS}` first'
            )
        cur.execute(f'SET search_path TO "{SCHEMA}"')
    return conn


class InvalidAddress(NonRetryableException):
    """The shipping address is wrong. Retrying will not fix it."""


class OrderValidationFailed(NonRetryableException):
    """The order references a missing SKU or an inactive customer."""


# ---- main-line tasks -------------------------------------------------------


@worker_task(task_definition_name="validate_order")
def validate_order(order_id: str, customer_id: str, items: list[dict]) -> dict:
    """Check SKUs exist, the customer is active, and compute total_amount.

    Read-only: nothing here needs compensating if a later step fails. Also
    creates the order row, ordered BEFORE the reservation rows because
    inventory_reservations carries an FK to orders -- the same FK-ordering trap
    that bit Prefect and Argo.
    """
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT status FROM customers WHERE customer_id = %s", (customer_id,)
                )
                row = cur.fetchone()
                if row is None:
                    raise OrderValidationFailed(f"customer {customer_id} does not exist")
                if row[0] != "active":
                    raise OrderValidationFailed(f"customer {customer_id} is {row[0]}")

                skus = [i["sku"] for i in items]
                cur.execute(
                    "SELECT sku, available_quantity, unit_price FROM inventory "
                    "WHERE sku = ANY(%s)",
                    (skus,),
                )
                inventory = {r[0]: {"available": r[1], "price": float(r[2])} for r in cur.fetchall()}

                total = 0.0
                for item in items:
                    sku = item["sku"]
                    if sku not in inventory:
                        raise OrderValidationFailed(f"unknown SKU {sku}")
                    if inventory[sku]["available"] < item["quantity"]:
                        raise OrderValidationFailed(
                            f"insufficient stock for {sku}: "
                            f"{inventory[sku]['available']} < {item['quantity']}"
                        )
                    total += inventory[sku]["price"] * item["quantity"]

                total = round(total, 2)
                cur.execute(
                    """
                    INSERT INTO orders (order_id, customer_id, total_amount, status)
                    VALUES (%s, %s, %s, 'validated')
                    ON CONFLICT (order_id) DO UPDATE
                        SET total_amount = EXCLUDED.total_amount,
                            status = 'validated',
                            updated_at = NOW()
                    """,
                    (order_id, customer_id, total),
                )

        return {
            "order_id": order_id,
            "customer_id": customer_id,
            "total_amount": total,
            "items": items,
            # Computed here as a branch key, because SWITCH compares values and
            # cannot evaluate `total >= threshold` itself.
            "approval_required": "yes" if total >= APPROVAL_THRESHOLD else "no",
            "approval_threshold": APPROVAL_THRESHOLD,
        }
    finally:
        conn.close()


@worker_task(task_definition_name="reserve_inventory")
def reserve_inventory(order_id: str, items: list[dict]) -> dict:
    """Atomically decrement available_quantity and create reservation rows.

    The conditional UPDATE (`WHERE available_quantity >= %s`) plus RETURNING is
    what makes the concurrent last-unit case safe: two workflows racing for the
    final RARE-D unit cannot both win, because the second UPDATE matches no row.
    """
    conn = get_db_connection()
    reservations = []
    try:
        with conn:
            with conn.cursor() as cur:
                for item in items:
                    sku, qty = item["sku"], item["quantity"]
                    cur.execute(
                        """
                        UPDATE inventory
                           SET available_quantity = available_quantity - %s,
                               reserved_quantity  = reserved_quantity  + %s
                         WHERE sku = %s AND available_quantity >= %s
                        RETURNING available_quantity
                        """,
                        (qty, qty, sku, qty),
                    )
                    if cur.fetchone() is None:
                        raise RuntimeError(
                            f"could not reserve {qty} of {sku}: insufficient stock"
                        )

                    reservation_id = f"RES-{uuid.uuid4().hex[:10].upper()}"
                    cur.execute(
                        """
                        INSERT INTO inventory_reservations
                            (reservation_id, order_id, sku, quantity, status)
                        VALUES (%s, %s, %s, %s, 'reserved')
                        """,
                        (reservation_id, order_id, sku, qty),
                    )
                    reservations.append(
                        {"reservation_id": reservation_id, "sku": sku, "quantity": qty}
                    )

                cur.execute(
                    "UPDATE orders SET status = 'inventory_reserved', updated_at = NOW() "
                    "WHERE order_id = %s",
                    (order_id,),
                )
    finally:
        conn.close()

    return {
        "order_id": order_id,
        "reservations": reservations,
        "reserved_count": len(reservations),
    }


@worker_task(task_definition_name="request_manager_approval")
def request_manager_approval(
    order_id: str, customer_id: str, total_amount: float, workflow_id: str
) -> dict:
    """Record the approval request and hand the approval service our resume handle.

    `task_ref_name` is "wait_for_approval" -- the WAIT task inside THIS
    sub-workflow. Note `workflow_id` must therefore be the *sub-workflow's* id,
    not the parent's, which is why the sub-workflow passes
    `${workflow.workflowId}` from its own scope.
    """
    approval_request_id = f"APR-{uuid.uuid4().hex[:10].upper()}"

    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO approval_requests
                        (approval_request_id, order_id, total_amount, status)
                    VALUES (%s, %s, %s, 'pending')
                    """,
                    (approval_request_id, order_id, float(total_amount)),
                )
                cur.execute(
                    "UPDATE orders SET status = 'awaiting_approval', updated_at = NOW() "
                    "WHERE order_id = %s",
                    (order_id,),
                )
    finally:
        conn.close()

    resp = requests.post(
        f"{APPROVAL_SERVICE_URL}/approval-requests",
        json={
            "approval_request_id": approval_request_id,
            "order_id": order_id,
            "total_amount": float(total_amount),
            "customer_id": customer_id,
            "items_summary": f"Order {order_id}",
            "provider": "conductor",
            "resume_data": {
                "workflow_id": workflow_id,
                "task_ref_name": "wait_for_approval",
                "base_url": CONDUCTOR_INTERNAL_URL,
            },
        },
        timeout=15,
    )
    resp.raise_for_status()

    return {
        "approval_request_id": approval_request_id,
        "order_id": order_id,
        "status": "pending",
    }


@worker_task(task_definition_name="record_approval_decision")
def record_approval_decision(
    approval_request_id: str, order_id: str, decision: str, approver: str = "", reason: str = ""
) -> dict:
    """Persist the manager's decision.

    Guarded against the late/duplicate-callback edge case: the UPDATE only
    matches a row still in `pending`, so a second resume after the first
    decision changes nothing and reports `duplicate`.
    """
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE approval_requests
                       SET status = %s, approver = %s, reason = %s, decided_at = NOW()
                     WHERE approval_request_id = %s AND status = 'pending'
                    RETURNING approval_request_id
                    """,
                    (decision, approver or None, reason or None, approval_request_id),
                )
                applied = cur.fetchone() is not None
    finally:
        conn.close()

    return {
        "approval_request_id": approval_request_id,
        "order_id": order_id,
        "decision": decision,
        "recorded": applied,
        "duplicate": not applied,
    }


DEFAULT_SHIPPING_ADDRESS = {
    "street": "1 Test Street",
    "city": "Springfield",
    "state": "IL",
    "zip": "62701",
}


@worker_task(task_definition_name="call_shipping_api")
def call_shipping_api(
    order_id: str,
    customer_id: str,
    items: list[dict],
    shipping_address: dict | None = None,
) -> dict:
    """Call the flaky shipping API.

    Error classification is driven by the service's own `error_type` field, not
    by the HTTP status. That distinction matters: the service returns **422 for
    two different things** -- a genuinely undeliverable address (`InvalidAddress`,
    non-retriable) and FastAPI's own request-validation rejection when a field
    is missing. Keying off `status_code == 422` conflates a permanent business
    outcome with a bug in the caller, and the first version of this task did
    exactly that: it omitted `shipping_address` entirely, got FastAPI's 422, and
    reported a clean "InvalidAddress" -- a malformed request masquerading as a
    successful saga trigger.

    Retriable (ordinary exception, engine retries with backoff):
        ShippingTimeout (504), ShippingServiceError (503), transport errors
    Non-retriable (NonRetryableException -> FAILED_WITH_TERMINAL_ERROR):
        InvalidAddress (422)
    """
    forced = os.environ.get("FORCE_SHIPPING_FAILURE", "").lower() in ("1", "true", "yes")
    if forced:
        if os.environ.get("FORCE_SHIPPING_FAILURE_MODE") == "invalid_address":
            raise InvalidAddress(f"forced InvalidAddress for {order_id}")
        raise RuntimeError(f"forced retriable shipping failure for {order_id}")

    try:
        resp = requests.post(
            f"{SHIPPING_SERVICE_URL}/shipments",
            json={
                "order_id": order_id,
                "customer_id": customer_id,
                "items": items,
                "shipping_address": shipping_address or DEFAULT_SHIPPING_ADDRESS,
                # Same key on every retry, so the service's own idempotency
                # cache returns the first shipment rather than creating a second.
                "idempotency_key": f"ship-{order_id}",
            },
            timeout=20,
        )
    except requests.Timeout as e:
        raise RuntimeError(f"shipping API timed out for {order_id}") from e

    if resp.status_code < 300:
        body = resp.json()
        return {
            "shipment_status": "created",
            "order_id": order_id,
            "shipment_id": body.get("shipment_id"),
            "tracking_number": body.get("tracking_number"),
            "carrier": body.get("carrier"),
        }

    try:
        detail = resp.json().get("detail", {})
    except Exception:
        detail = resp.text[:300]

    error_type = detail.get("error_type") if isinstance(detail, dict) else None
    message = detail.get("message") if isinstance(detail, dict) else str(detail)

    if error_type == "InvalidAddress":
        raise InvalidAddress(f"shipping rejected {order_id}: {message}")

    if error_type is None and resp.status_code == 422:
        # FastAPI request-validation failure: our payload is wrong, and no
        # number of retries will fix it. Terminal, but say so accurately.
        raise InvalidAddress(
            f"malformed shipping request for {order_id} (not a business "
            f"rejection): {str(detail)[:300]}"
        )

    raise RuntimeError(
        f"shipping API returned {resp.status_code} for {order_id}: "
        f"{error_type or 'unknown'}: {message}"
    )


@worker_task(task_definition_name="update_order_status")
def update_order_status(
    order_id: str, shipment_id: str = "", tracking_number: str = ""
) -> dict:
    """Mark the order shipped with its tracking info."""
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE orders
                       SET status = 'shipped', shipment_id = %s,
                           tracking_number = %s, updated_at = NOW()
                     WHERE order_id = %s
                    """,
                    (shipment_id or None, tracking_number or None, order_id),
                )
    finally:
        conn.close()

    return {"order_id": order_id, "status": "shipped", "tracking_number": tracking_number}


@worker_task(task_definition_name="send_order_notification")
def send_order_notification(
    order_id: str, customer_id: str = "", tracking_number: str = ""
) -> dict:
    """Best-effort shipment notification. `optional: true` in the workflow."""
    return {
        "notification_status": "sent",
        "order_id": order_id,
        "message": f"Order {order_id} shipped, tracking {tracking_number}",
    }


# ---- saga compensation -----------------------------------------------------


@worker_task(task_definition_name="release_inventory")
def release_inventory(order_id: str, reason: str = "") -> dict:
    """Reverse the reservation. Idempotent by construction.

    Only rows still in `reserved` are touched, and the inventory adjustment is
    driven by those same rows, so calling this twice releases nothing the second
    time. That is what makes double-compensation safe -- the spec's
    "idempotent: no-op if already released".

    FORCE_COMPENSATION_FAILURE=1 makes this fail every attempt, which is the
    only practical way to reach the dead-letter: it exhausts release_inventory's
    retries, fails dag4_compensation, and so triggers *its* failureWorkflow,
    dag4_compensation_dead_letter. A saga whose compensation can itself fail is
    the case people design for and never test.
    """
    if os.environ.get("FORCE_COMPENSATION_FAILURE", "").lower() in ("1", "true", "yes"):
        raise RuntimeError(f"forced compensation failure for {order_id}")

    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE inventory_reservations
                       SET status = 'released', released_at = NOW()
                     WHERE order_id = %s AND status = 'reserved'
                    RETURNING sku, quantity
                    """,
                    (order_id,),
                )
                released = cur.fetchall()

                for sku, qty in released:
                    cur.execute(
                        """
                        UPDATE inventory
                           SET available_quantity = available_quantity + %s,
                               reserved_quantity  = reserved_quantity  - %s
                         WHERE sku = %s
                        """,
                        (qty, qty, sku),
                    )
    finally:
        conn.close()

    return {
        "order_id": order_id,
        "released_count": len(released),
        "released": [{"sku": s, "quantity": q} for s, q in released],
        "already_released": len(released) == 0,
        "reason": reason,
    }


@worker_task(task_definition_name="update_order_cancelled")
def update_order_cancelled(
    order_id: str, reason: str = "", status: str | None = "cancelled"
) -> dict:
    """Set the order to cancelled/failed with a reason.

    `status` is coerced rather than defaulted, because a Python default does not
    help here: when dag4_compensation runs as a *failureWorkflow* it inherits
    the failed workflow's input, which has no `cancel_status` key, so
    `${workflow.input.cancel_status}` resolves to an explicit null and is passed
    as None -- overriding the default and hitting the column's NOT NULL.
    """
    status = status or "cancelled"
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE orders
                       SET status = %s, failure_reason = %s, updated_at = NOW()
                     WHERE order_id = %s
                    """,
                    (status, reason[:500] if reason else None, order_id),
                )
    finally:
        conn.close()

    return {"order_id": order_id, "status": status, "reason": reason}


@worker_task(task_definition_name="send_cancellation_notification")
def send_cancellation_notification(order_id: str, reason: str = "") -> dict:
    """Best-effort cancellation notice. `optional: true` in the workflow."""
    return {
        "notification_status": "sent",
        "order_id": order_id,
        "message": f"Order {order_id} cancelled: {reason}",
    }


@worker_task(task_definition_name="compensation_failed_dead_letter")
def compensation_failed_dead_letter(order_id: str, reason: str = "") -> dict:
    """Terminal dead-letter: compensation itself failed after its retries.

    Deliberately does no cleanup. Its only job is to leave an unmistakable row
    behind for a human, because at this point the system's state is known to be
    inconsistent and guessing further would make it worse.
    """
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE orders
                       SET status = 'compensation_failed',
                           failure_reason = %s, updated_at = NOW()
                     WHERE order_id = %s
                    """,
                    (f"MANUAL INTERVENTION REQUIRED: {reason}"[:500], order_id),
                )
    finally:
        conn.close()

    return {
        "order_id": order_id,
        "status": "compensation_failed",
        "requires_manual_intervention": True,
        "reason": reason,
    }



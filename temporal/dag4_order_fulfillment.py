"""
DAG 4: Order Fulfillment -- Temporal Workflow

Saga pattern with child workflows and signals:
1. ValidateOrder activity
2. ReserveInventory -- child workflow
3. CheckApprovalRequired -- if/else in workflow code
4. ManagerApproval -- child workflow with signal for approval decision
5. CallShippingAPI -- child workflow with retry
6. Saga compensation: reversed compensations stack on failure

This is where Temporal truly shines -- the saga pattern is expressed as
straightforward Python try/except with a compensations list.
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from activities import (
        InvalidAddress,
        OrderItem,
        RecordApprovalDecisionInput,
        ReleaseInventoryInput,
        ReleaseInventoryOutput,
        RequestApprovalInput,
        ReserveInventoryInput,
        ReserveInventoryOutput,
        SendOrderNotificationInput,
        SendOrderNotificationOutput,
        ShippingInput,
        ShippingOutput,
        UpdateOrderStatusInput,
        UpdateOrderStatusOutput,
        ValidateOrderInput,
        ValidateOrderOutput,
        call_shipping_api,
        record_approval_decision,
        release_inventory,
        request_approval,
        reserve_inventory,
        send_order_notification,
        update_order_status,
        validate_order,
    )


SIGNAL_SERVER_URL = os.environ.get("SIGNAL_SERVER_URL", "http://localhost:8095")

ACTIVITY_RETRY = RetryPolicy(
    maximum_attempts=3,
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
)

COMPENSATION_RETRY = RetryPolicy(
    maximum_attempts=5,
    initial_interval=timedelta(seconds=3),
    backoff_coefficient=2.0,
)

SHIPPING_RETRY = RetryPolicy(
    maximum_attempts=3,
    initial_interval=timedelta(seconds=3),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=15),
    non_retryable_error_types=["InvalidAddress"],
)


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------


class OrderRejectedException(Exception):
    """Raised when manager approval is rejected."""
    pass


class OrderValidationFailed(Exception):
    """Raised when order validation fails."""
    pass


# ---------------------------------------------------------------------------
# Data classes for workflow I/O
# ---------------------------------------------------------------------------


@dataclass
class OrderFulfillmentInput:
    order_id: str
    customer_id: str
    items: list[OrderItem]
    shipping_address: dict[str, str] = field(default_factory=dict)
    approval_threshold: float = 500.00


@dataclass
class OrderFulfillmentOutput:
    order_id: str
    status: str  # "shipped" or "cancelled"
    shipment_id: str | None = None
    tracking_number: str | None = None
    carrier: str | None = None
    estimated_delivery: str | None = None
    failure_reason: str | None = None


@dataclass
class ApprovalDecision:
    decision: str  # "approved" | "rejected" | "expired"
    approver: str | None = None
    reason: str = ""


# ---------------------------------------------------------------------------
# Child Workflow: ReserveInventoryWorkflow
# ---------------------------------------------------------------------------


@workflow.defn
class ReserveInventoryWorkflow:
    """Child workflow that atomically reserves inventory for an order."""

    @workflow.run
    async def run(self, input: ReserveInventoryInput) -> ReserveInventoryOutput:
        workflow.logger.info("Reserving inventory for order %s", input.order_id)

        result = await workflow.execute_activity(
            reserve_inventory,
            input,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=ACTIVITY_RETRY,
        )

        workflow.logger.info(
            "Inventory reserved: %s (items: %s)",
            result.reservation_id,
            result.items_reserved,
        )
        return result


# ---------------------------------------------------------------------------
# Child Workflow: ManagerApprovalWorkflow (with signal)
# ---------------------------------------------------------------------------


@workflow.defn
class ManagerApprovalWorkflow:
    """
    Child workflow that requests manager approval and waits for the decision
    via a Temporal signal.

    The approval service calls the signal_server.py relay, which sends the
    ``approval_decision`` signal to this child workflow.
    """

    def __init__(self) -> None:
        self._decision: ApprovalDecision | None = None

    @workflow.signal
    async def approval_decision(self, decision: ApprovalDecision) -> None:
        """Signal handler: receives the approval/rejection decision."""
        workflow.logger.info("Received approval decision: %s", decision.decision)
        self._decision = decision

    @workflow.query
    def get_decision(self) -> ApprovalDecision | None:
        return self._decision

    @workflow.run
    async def run(self, input: RequestApprovalInput) -> ApprovalDecision:
        workflow.logger.info(
            "Requesting manager approval for order %s (amount=%.2f)",
            input.order_id,
            input.total_amount,
        )

        # Step 1: Send the approval request to the Approval Service
        approval_request_id: str = await workflow.execute_activity(
            request_approval,
            input,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=ACTIVITY_RETRY,
        )

        workflow.logger.info(
            "Approval request submitted: %s. Waiting for decision signal...",
            approval_request_id,
        )

        # Step 2: Wait for the approval signal (with timeout)
        try:
            await workflow.wait_condition(
                lambda: self._decision is not None,
                timeout=timedelta(seconds=120),
            )
        except asyncio.TimeoutError:
            workflow.logger.warn(
                "Approval request %s timed out for order %s",
                approval_request_id,
                input.order_id,
            )
            self._decision = ApprovalDecision(
                decision="expired",
                approver=None,
                reason="Approval request timed out",
            )

        decision = self._decision
        assert decision is not None

        # Step 3: Record the decision in the database
        await workflow.execute_activity(
            record_approval_decision,
            RecordApprovalDecisionInput(
                approval_request_id=approval_request_id,
                order_id=input.order_id,
                decision=decision.decision,
                approver=decision.approver,
                reason=decision.reason,
            ),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=ACTIVITY_RETRY,
        )

        return decision


# ---------------------------------------------------------------------------
# Child Workflow: ShippingWorkflow
# ---------------------------------------------------------------------------


@workflow.defn
class ShippingWorkflow:
    """Child workflow that calls the Shipping Service API with retries."""

    @workflow.run
    async def run(self, input: ShippingInput) -> ShippingOutput:
        workflow.logger.info("Shipping order %s", input.order_id)

        result = await workflow.execute_activity(
            call_shipping_api,
            input,
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=SHIPPING_RETRY,
        )

        workflow.logger.info(
            "Shipment created: %s (tracking: %s, carrier: %s)",
            result.shipment_id,
            result.tracking_number,
            result.carrier,
        )
        return result


# ---------------------------------------------------------------------------
# Main Workflow: OrderFulfillmentWorkflow (Saga pattern)
# ---------------------------------------------------------------------------


@workflow.defn
class OrderFulfillmentWorkflow:
    """
    Order Fulfillment workflow implementing the saga compensation pattern.

    After each successful step we push a compensation onto the stack.
    On failure, compensations are executed in reverse order.
    """

    @workflow.run
    async def run(self, input: OrderFulfillmentInput) -> OrderFulfillmentOutput:
        workflow.logger.info("Starting order fulfillment for %s", input.order_id)

        # ---- Step 1: Validate the order -----------------------------------
        validation: ValidateOrderOutput = await workflow.execute_activity(
            validate_order,
            ValidateOrderInput(
                order_id=input.order_id,
                customer_id=input.customer_id,
                items=input.items,
                shipping_address=input.shipping_address,
                approval_threshold=input.approval_threshold,
            ),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=ACTIVITY_RETRY,
        )

        if not validation.validation.is_valid:
            raise workflow.ApplicationError(
                f"Order validation failed: {validation.validation.reason}",
                type="OrderValidationFailed",
            )

        # Use enriched items (with unit_price) from validation
        enriched_items = validation.items
        total_amount = validation.total_amount
        approval_threshold = validation.approval_threshold

        # ---- Saga compensation stack --------------------------------------
        compensations: list[tuple[str, Any]] = []

        try:
            # ---- Step 2: Reserve inventory (child workflow) ---------------
            reservation: ReserveInventoryOutput = await workflow.execute_child_workflow(
                ReserveInventoryWorkflow.run,
                ReserveInventoryInput(
                    order_id=input.order_id,
                    customer_id=input.customer_id,
                    items=enriched_items,
                ),
                id=f"reserve-inventory-{input.order_id}",
                execution_timeout=timedelta(seconds=60),
            )

            # Push compensation: release the reserved inventory
            compensations.append(("release_inventory", reservation.reservation_id))

            workflow.logger.info(
                "Inventory reserved: %s (items: %s)",
                reservation.reservation_id,
                reservation.items_reserved,
            )

            # ---- Step 3: Check if approval is required --------------------
            needs_approval = total_amount >= approval_threshold
            workflow.logger.info(
                "Total=%.2f, threshold=%.2f -> needs_approval=%s",
                total_amount,
                approval_threshold,
                needs_approval,
            )

            if needs_approval:
                # ---- Step 4: Manager approval (child workflow with signal) --
                wf_info = workflow.info()

                decision: ApprovalDecision = await workflow.execute_child_workflow(
                    ManagerApprovalWorkflow.run,
                    RequestApprovalInput(
                        order_id=input.order_id,
                        customer_id=input.customer_id,
                        total_amount=total_amount,
                        items=enriched_items,
                        workflow_id=f"manager-approval-{input.order_id}",
                        run_id="",  # Will be set by Temporal
                    ),
                    id=f"manager-approval-{input.order_id}",
                    execution_timeout=timedelta(seconds=180),
                )

                if decision.decision != "approved":
                    raise OrderRejectedException(
                        f"Order rejected by {decision.approver or 'system'}: "
                        f"{decision.reason or decision.decision}"
                    )

                workflow.logger.info(
                    "Order approved by %s", decision.approver or "system"
                )

            # ---- Step 5: Call shipping API (child workflow) ----------------
            shipment: ShippingOutput = await workflow.execute_child_workflow(
                ShippingWorkflow.run,
                ShippingInput(
                    order_id=input.order_id,
                    items=enriched_items,
                    shipping_address=input.shipping_address,
                ),
                id=f"shipping-{input.order_id}",
                execution_timeout=timedelta(seconds=120),
            )

            workflow.logger.info(
                "Shipment created: %s (tracking: %s)",
                shipment.shipment_id,
                shipment.tracking_number,
            )

        except Exception as exc:
            # ---- Saga compensation: reverse all completed steps -----------
            workflow.logger.error(
                "Order %s failed: %s. Running %d compensations...",
                input.order_id,
                exc,
                len(compensations),
            )

            for comp_type, comp_data in reversed(compensations):
                try:
                    if comp_type == "release_inventory":
                        await workflow.execute_activity(
                            release_inventory,
                            ReleaseInventoryInput(
                                order_id=input.order_id,
                                reservation_id=comp_data,
                            ),
                            start_to_close_timeout=timedelta(seconds=30),
                            retry_policy=COMPENSATION_RETRY,
                        )
                        workflow.logger.info(
                            "Compensation: released inventory for order %s",
                            input.order_id,
                        )
                except Exception as comp_exc:
                    workflow.logger.error(
                        "Compensation failed for %s: %s. Manual intervention required.",
                        comp_type,
                        comp_exc,
                    )

            # Update order to cancelled
            try:
                await workflow.execute_activity(
                    update_order_status,
                    UpdateOrderStatusInput(
                        order_id=input.order_id,
                        status="cancelled",
                        failure_reason=str(exc),
                    ),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=COMPENSATION_RETRY,
                )
            except Exception as update_exc:
                workflow.logger.error(
                    "Failed to update order %s to cancelled: %s",
                    input.order_id,
                    update_exc,
                )

            # Send cancellation notification (best-effort)
            try:
                await workflow.execute_activity(
                    send_order_notification,
                    SendOrderNotificationInput(
                        order_id=input.order_id,
                        status="cancelled",
                        failure_reason=str(exc),
                    ),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=ACTIVITY_RETRY,
                )
            except Exception as notif_exc:
                workflow.logger.warn(
                    "Cancellation notification failed for order %s: %s",
                    input.order_id,
                    notif_exc,
                )

            raise workflow.ApplicationError(
                f"Order {input.order_id} cancelled: {exc}",
                type="OrderCancelled",
            )

        # ---- Happy path: update order to shipped and notify ---------------
        await workflow.execute_activity(
            update_order_status,
            UpdateOrderStatusInput(
                order_id=input.order_id,
                status="shipped",
                shipment_id=shipment.shipment_id,
                tracking_number=shipment.tracking_number,
            ),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=ACTIVITY_RETRY,
        )

        # Send shipped notification (best-effort)
        try:
            await workflow.execute_activity(
                send_order_notification,
                SendOrderNotificationInput(
                    order_id=input.order_id,
                    status="shipped",
                    tracking_number=shipment.tracking_number,
                    carrier=shipment.carrier,
                    estimated_delivery=shipment.estimated_delivery,
                ),
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=ACTIVITY_RETRY,
            )
        except Exception as notif_exc:
            workflow.logger.warn(
                "Shipped notification failed for order %s (non-fatal): %s",
                input.order_id,
                notif_exc,
            )

        workflow.logger.info("Order %s fulfilled successfully", input.order_id)

        return OrderFulfillmentOutput(
            order_id=input.order_id,
            status="shipped",
            shipment_id=shipment.shipment_id,
            tracking_number=shipment.tracking_number,
            carrier=shipment.carrier,
            estimated_delivery=shipment.estimated_delivery,
        )

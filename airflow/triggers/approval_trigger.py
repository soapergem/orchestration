"""
Custom Airflow trigger that polls the approval-service /approval-requests/<id>
endpoint until the approval decision is made or the request times out.

Used by DAG 4 (Order Fulfillment) for the ManagerApproval deferrable operator.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, AsyncIterator

import aiohttp
from airflow.triggers.base import BaseTrigger, TriggerEvent


class ApprovalTrigger(BaseTrigger):
    """
    Polls GET /approval-requests/<approval_request_id> on the approval-service
    every ``poll_interval`` seconds.  Fires a TriggerEvent when the decision
    field changes from "pending" to "approved" / "rejected" / "expired", or
    when ``timeout`` seconds elapse.
    """

    def __init__(
        self,
        approval_request_id: str,
        approval_service_url: str = "http://approval-service:8091",
        poll_interval: float = 5.0,
        timeout: float = 180.0,
    ) -> None:
        super().__init__()
        self.approval_request_id = approval_request_id
        self.approval_service_url = approval_service_url.rstrip("/")
        self.poll_interval = poll_interval
        self.timeout = timeout

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "triggers.approval_trigger.ApprovalTrigger",
            {
                "approval_request_id": self.approval_request_id,
                "approval_service_url": self.approval_service_url,
                "poll_interval": self.poll_interval,
                "timeout": self.timeout,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Poll the approval service until a decision is rendered or timeout."""
        url = f"{self.approval_service_url}/approval-requests/{self.approval_request_id}"
        start_time = datetime.now(timezone.utc)

        async with aiohttp.ClientSession() as session:
            while True:
                elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
                if elapsed >= self.timeout:
                    yield TriggerEvent(
                        {
                            "status": "timeout",
                            "approval_request_id": self.approval_request_id,
                            "decision": "expired",
                            "reason": (
                                f"Approval request {self.approval_request_id} "
                                f"timed out after {self.timeout}s"
                            ),
                        }
                    )
                    return

                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        if resp.status == 200:
                            payload = await resp.json()
                            decision = payload.get("decision", payload.get("status", "pending"))

                            if decision in ("approved", "rejected"):
                                yield TriggerEvent(
                                    {
                                        "status": "completed",
                                        "approval_request_id": self.approval_request_id,
                                        "decision": decision,
                                        "approver": payload.get("approver"),
                                        "reason": payload.get("reason", ""),
                                        "decided_at": payload.get(
                                            "decided_at",
                                            datetime.now(timezone.utc).isoformat(),
                                        ),
                                    }
                                )
                                return

                            if decision == "expired":
                                yield TriggerEvent(
                                    {
                                        "status": "timeout",
                                        "approval_request_id": self.approval_request_id,
                                        "decision": "expired",
                                        "reason": "Approval expired on server side",
                                    }
                                )
                                return

                            # Still "pending" -- keep polling

                        elif resp.status == 404:
                            self.log.warning(
                                "Approval request %s not found yet, retrying",
                                self.approval_request_id,
                            )
                        else:
                            self.log.warning(
                                "Unexpected status %s from %s", resp.status, url
                            )

                except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                    self.log.warning(
                        "Error polling %s: %s -- will retry", url, exc
                    )

                await asyncio.sleep(self.poll_interval)

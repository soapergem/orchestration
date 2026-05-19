"""
Signal Relay Server -- FastAPI app

Receives HTTP POST callbacks from external services (callback-fetch-service,
approval-service) and relays them as Temporal signals to the appropriate
workflow instances.

This bridges the gap between HTTP callbacks and Temporal's native signal
mechanism.  In the Step Functions implementation, this role is played by
API Gateway + Lambda relay functions.

Run with:
    uvicorn signal_server:app --host 0.0.0.0 --port 8095
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from temporalio.client import Client

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Temporal Signal Relay Server",
    description="Relays HTTP callbacks to Temporal workflow signals",
)

# ---------------------------------------------------------------------------
# Temporal client (lazy-initialised on first request)
# ---------------------------------------------------------------------------

TEMPORAL_ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
TEMPORAL_NAMESPACE = os.environ.get("TEMPORAL_NAMESPACE", "default")

_temporal_client: Client | None = None


async def get_temporal_client() -> Client:
    """Return a cached Temporal client, connecting on first use."""
    global _temporal_client
    if _temporal_client is None:
        _temporal_client = await Client.connect(
            TEMPORAL_ADDRESS, namespace=TEMPORAL_NAMESPACE
        )
        logger.info(
            "Connected to Temporal at %s (namespace=%s)",
            TEMPORAL_ADDRESS,
            TEMPORAL_NAMESPACE,
        )
    return _temporal_client


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# DAG 2: Fetch callback relay
# ---------------------------------------------------------------------------


@app.post("/fetch-callback")
async def fetch_callback(
    request: Request,
    workflow_id: str = Query(..., description="Target workflow ID"),
    run_id: str = Query("", description="Target workflow run ID"),
) -> dict[str, str]:
    """
    Receives the HTTP callback from the callback-fetch-service and sends a
    ``fetch_completed`` signal to the API Fan-Out workflow.

    The callback-fetch-service POSTs the fetch result here.  The workflow_id
    and run_id are embedded in the callback URL that was sent with the
    original fetch request.
    """
    body = await request.json()
    logger.info(
        "Received fetch callback for workflow %s (run=%s): status=%s",
        workflow_id,
        run_id,
        body.get("status"),
    )

    client = await get_temporal_client()
    handle = client.get_workflow_handle(workflow_id, run_id=run_id or None)

    try:
        await handle.signal("fetch_completed", body)
    except Exception as exc:
        logger.error("Failed to signal workflow %s: %s", workflow_id, exc)
        raise HTTPException(
            status_code=502,
            detail=f"Failed to signal Temporal workflow: {exc}",
        )

    return {"status": "relayed", "workflow_id": workflow_id}


# ---------------------------------------------------------------------------
# DAG 4: Approval callback relay
# ---------------------------------------------------------------------------


@dataclass
class ApprovalDecisionPayload:
    """Shape of the body POSTed by the approval service."""
    decision: str  # "approved" | "rejected"
    approver: str | None = None
    reason: str = ""


@app.post("/approval-callback")
async def approval_callback(
    request: Request,
    workflow_id: str = Query(..., description="Target child workflow ID"),
    run_id: str = Query("", description="Target child workflow run ID"),
) -> dict[str, str]:
    """
    Receives the HTTP callback from the approval-service and sends an
    ``approval_decision`` signal to the ManagerApprovalWorkflow child workflow.
    """
    body = await request.json()
    decision = body.get("decision", "unknown")
    logger.info(
        "Received approval callback for workflow %s (run=%s): decision=%s",
        workflow_id,
        run_id,
        decision,
    )

    if decision not in ("approved", "rejected"):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid decision value: {decision}. Expected 'approved' or 'rejected'.",
        )

    client = await get_temporal_client()
    handle = client.get_workflow_handle(workflow_id, run_id=run_id or None)

    # Build the signal payload matching the ApprovalDecision dataclass
    signal_payload = {
        "decision": decision,
        "approver": body.get("approver"),
        "reason": body.get("reason", ""),
    }

    try:
        await handle.signal("approval_decision", signal_payload)
    except Exception as exc:
        logger.error("Failed to signal workflow %s: %s", workflow_id, exc)
        raise HTTPException(
            status_code=502,
            detail=f"Failed to signal Temporal workflow: {exc}",
        )

    return {"status": "relayed", "workflow_id": workflow_id, "decision": decision}


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host="0.0.0.0", port=8095)

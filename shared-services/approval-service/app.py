import asyncio
import os
from datetime import datetime, timezone
from enum import Enum

import httpx
from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Approval Service")

AUTO_DECIDE_DELAY = int(os.getenv("AUTO_DECIDE_DELAY_SECONDS", "0"))
AUTO_DECIDE_ACTION = os.getenv("AUTO_DECIDE_ACTION", "none")


class ApprovalStatus(str, Enum):
    pending = "pending"
    approved = "approved"
    rejected = "rejected"


class ApprovalRequest(BaseModel):
    approval_request_id: str
    order_id: str
    total_amount: float
    customer_id: str
    callback_url: str
    items_summary: str = ""


class DecisionRequest(BaseModel):
    decision: ApprovalStatus
    approver: str
    reason: str = ""


class ApprovalRecord(BaseModel):
    approval_request_id: str
    order_id: str
    total_amount: float
    customer_id: str
    callback_url: str
    items_summary: str
    status: ApprovalStatus
    approver: str | None = None
    reason: str | None = None
    requested_at: str
    decided_at: str | None = None


approvals_store: dict[str, ApprovalRecord] = {}


async def deliver_decision(record: ApprovalRecord):
    payload = {
        "approval_request_id": record.approval_request_id,
        "order_id": record.order_id,
        "decision": record.status.value,
        "approver": record.approver,
        "reason": record.reason,
        "decided_at": record.decided_at,
    }
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            await client.post(record.callback_url, json=payload)
        except Exception:
            pass


async def auto_decide(record: ApprovalRecord):
    await asyncio.sleep(AUTO_DECIDE_DELAY)
    if record.status != ApprovalStatus.pending:
        return
    record.status = ApprovalStatus.approved if AUTO_DECIDE_ACTION == "approved" else ApprovalStatus.rejected
    record.approver = "auto-decider"
    record.reason = f"Auto-{record.status.value} after {AUTO_DECIDE_DELAY}s delay"
    record.decided_at = datetime.now(timezone.utc).isoformat()
    await deliver_decision(record)


@app.post("/approval-requests", status_code=201)
async def create_approval(req: ApprovalRequest, background_tasks: BackgroundTasks):
    if req.approval_request_id in approvals_store:
        raise HTTPException(status_code=409, detail="Approval request already exists")
    record = ApprovalRecord(
        approval_request_id=req.approval_request_id,
        order_id=req.order_id,
        total_amount=req.total_amount,
        customer_id=req.customer_id,
        callback_url=req.callback_url,
        items_summary=req.items_summary,
        status=ApprovalStatus.pending,
        requested_at=datetime.now(timezone.utc).isoformat(),
    )
    approvals_store[req.approval_request_id] = record
    if AUTO_DECIDE_ACTION in ("approved", "rejected") and AUTO_DECIDE_DELAY > 0:
        background_tasks.add_task(auto_decide, record)
    return {"approval_request_id": record.approval_request_id, "status": "pending"}


@app.post("/approval-requests/{approval_request_id}/decide")
async def decide(approval_request_id: str, req: DecisionRequest, background_tasks: BackgroundTasks):
    record = approvals_store.get(approval_request_id)
    if not record:
        raise HTTPException(status_code=404, detail="Approval request not found")
    if record.status != ApprovalStatus.pending:
        raise HTTPException(status_code=409, detail=f"Already decided: {record.status.value}")
    if req.decision == ApprovalStatus.pending:
        raise HTTPException(status_code=422, detail="Decision must be 'approved' or 'rejected'")
    record.status = req.decision
    record.approver = req.approver
    record.reason = req.reason
    record.decided_at = datetime.now(timezone.utc).isoformat()
    background_tasks.add_task(deliver_decision, record)
    return {
        "approval_request_id": record.approval_request_id,
        "decision": record.status.value,
        "decided_at": record.decided_at,
    }


@app.get("/approval-requests/{approval_request_id}")
async def get_approval(approval_request_id: str):
    record = approvals_store.get(approval_request_id)
    if not record:
        raise HTTPException(status_code=404, detail="Approval request not found")
    return record.model_dump()

import asyncio
import json
from datetime import datetime, timezone
from enum import Enum
from typing import Literal

import boto3
import httpx
from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import AliasChoices, BaseModel, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

app = FastAPI(title="Approval Service")


class Settings(BaseSettings):
    """Service configuration, sourced from environment variables."""

    model_config = SettingsConfigDict(case_sensitive=False)

    auto_decide_delay_seconds: int = 0
    auto_decide_action: Literal["none", "approved", "rejected"] = "none"
    aws_region: str | None = Field(
        default=None, validation_alias=AliasChoices("AWS_REGION", "AWS_DEFAULT_REGION")
    )


settings = Settings()


class ApprovalStatus(str, Enum):
    pending = "pending"
    approved = "approved"
    rejected = "rejected"


class Provider(str, Enum):
    """How this approval is resumed once decided."""

    stepfunctions = "stepfunctions"  # resume_data: {task_token, region?}
    http_callback = "http_callback"  # resume_data: {callback_url}


class ApprovalRequest(BaseModel):
    """Register an approval plus the provider-specific handle used to resume.

    ``resume_data`` is stored verbatim and only interpreted when a decision is
    delivered. ``callback_url`` is accepted as a top-level shorthand for the
    http_callback provider.
    """

    approval_request_id: str
    order_id: str
    total_amount: float
    customer_id: str
    items_summary: str = ""
    provider: Provider | None = None
    resume_data: dict = {}
    callback_url: str | None = None

    @model_validator(mode="after")
    def _normalize(self):
        if self.callback_url and not self.resume_data.get("callback_url"):
            self.resume_data = {**self.resume_data, "callback_url": self.callback_url}
        if self.provider is None:
            if self.resume_data.get("task_token"):
                self.provider = Provider.stepfunctions
            elif self.resume_data.get("callback_url"):
                self.provider = Provider.http_callback
            else:
                raise ValueError(
                    "cannot infer provider: supply `provider` or a resume_data "
                    "with task_token / callback_url"
                )
        return self


class DecisionRequest(BaseModel):
    decision: ApprovalStatus
    approver: str
    reason: str = ""


class ApprovalRecord(BaseModel):
    approval_request_id: str
    order_id: str
    total_amount: float
    customer_id: str
    items_summary: str
    provider: Provider
    resume_data: dict
    status: ApprovalStatus
    approver: str | None = None
    reason: str | None = None
    requested_at: str
    decided_at: str | None = None
    resumed_at: str | None = None
    resume_count: int = 0


approvals_store: dict[str, ApprovalRecord] = {}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _decision_payload(record: ApprovalRecord) -> dict:
    return {
        "approval_request_id": record.approval_request_id,
        "order_id": record.order_id,
        "decision": record.status.value,
        "approver": record.approver,
        "reason": record.reason,
        "decided_at": record.decided_at,
    }


async def _dispatch_resume(record: ApprovalRecord) -> dict:
    """Deliver the decision to the provider that registered this approval."""
    payload = _decision_payload(record)

    if record.provider == Provider.stepfunctions:
        task_token = record.resume_data.get("task_token")
        if not task_token:
            raise HTTPException(400, "resume_data missing task_token")
        region = record.resume_data.get("region") or settings.aws_region

        def _resume():
            sfn = boto3.client("stepfunctions", region_name=region)
            if record.status in (ApprovalStatus.approved, ApprovalStatus.rejected):
                # The state machine branches on decision, so both are a success.
                sfn.send_task_success(taskToken=task_token, output=json.dumps(payload))
                return "send_task_success"
            sfn.send_task_failure(
                taskToken=task_token,
                error="InvalidDecision",
                cause=f"Unexpected decision value: {record.status.value}",
            )
            return "send_task_failure"

        # boto3 is synchronous -- keep it off the event loop.
        action = await asyncio.to_thread(_resume)
        return {"action": action}

    if record.provider == Provider.http_callback:
        callback_url = record.resume_data.get("callback_url")
        if not callback_url:
            raise HTTPException(400, "resume_data missing callback_url")
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(callback_url, json=payload)
        return {"action": "http_callback", "callback_status": resp.status_code}

    raise HTTPException(400, f"unsupported provider: {record.provider}")


async def _apply_decision(
    record: ApprovalRecord, decision: ApprovalStatus, approver: str, reason: str
) -> dict:
    """Record a decision and resume the registered provider."""
    record.status = decision
    record.approver = approver
    record.reason = reason
    record.decided_at = _now()
    result = await _dispatch_resume(record)
    record.resumed_at = _now()
    record.resume_count += 1
    return result


async def auto_decide(record: ApprovalRecord):
    """Automated-test path: decide after a delay, best-effort dispatch."""
    await asyncio.sleep(settings.auto_decide_delay_seconds)
    if record.status != ApprovalStatus.pending:
        return
    decision = (
        ApprovalStatus.approved
        if settings.auto_decide_action == "approved"
        else ApprovalStatus.rejected
    )
    try:
        await _apply_decision(
            record,
            decision,
            approver="auto-decider",
            reason=f"Auto-{decision.value} after {settings.auto_decide_delay_seconds}s delay",
        )
    except Exception:
        # Mirror the fetch service: if we can't resume, the workflow's own
        # timeout takes over.
        pass


@app.post("/approval-requests", status_code=201)
async def create_approval(req: ApprovalRequest, background_tasks: BackgroundTasks):
    if req.approval_request_id in approvals_store:
        raise HTTPException(status_code=409, detail="Approval request already exists")
    record = ApprovalRecord(
        approval_request_id=req.approval_request_id,
        order_id=req.order_id,
        total_amount=req.total_amount,
        customer_id=req.customer_id,
        items_summary=req.items_summary,
        provider=req.provider,
        resume_data=req.resume_data,
        status=ApprovalStatus.pending,
        requested_at=_now(),
    )
    approvals_store[req.approval_request_id] = record
    if settings.auto_decide_action in ("approved", "rejected") and settings.auto_decide_delay_seconds > 0:
        background_tasks.add_task(auto_decide, record)
    return {
        "approval_request_id": record.approval_request_id,
        "status": "pending",
        "provider": record.provider.value,
    }


@app.post("/approval-requests/{approval_request_id}/decide")
async def decide(approval_request_id: str, req: DecisionRequest):
    record = approvals_store.get(approval_request_id)
    if not record:
        raise HTTPException(status_code=404, detail="Approval request not found")
    if record.status != ApprovalStatus.pending:
        raise HTTPException(status_code=409, detail=f"Already decided: {record.status.value}")
    if req.decision == ApprovalStatus.pending:
        raise HTTPException(status_code=422, detail="Decision must be 'approved' or 'rejected'")
    result = await _apply_decision(record, req.decision, req.approver, req.reason)
    return {
        "approval_request_id": record.approval_request_id,
        "decision": record.status.value,
        "decided_at": record.decided_at,
        **result,
    }


@app.post("/approval-requests/{approval_request_id}/resume")
async def resume(approval_request_id: str):
    """Re-fire the resume for an already-decided approval.

    Lets the bake-off exercise duplicate and late-decision edge cases; the
    orchestrator is responsible for ignoring the extra signal.
    """
    record = approvals_store.get(approval_request_id)
    if not record:
        raise HTTPException(status_code=404, detail="Approval request not found")
    if record.status == ApprovalStatus.pending:
        raise HTTPException(status_code=409, detail="not decided yet; nothing to resume")
    result = await _dispatch_resume(record)
    record.resumed_at = _now()
    record.resume_count += 1
    return {
        "approval_request_id": record.approval_request_id,
        "decision": record.status.value,
        "resume_count": record.resume_count,
        **result,
    }


@app.get("/approval-requests")
async def list_approvals(status: ApprovalStatus | None = None):
    """List approvals, newest first. Filter by ``status`` (e.g. pending)."""
    records = sorted(
        approvals_store.values(), key=lambda r: r.requested_at, reverse=True
    )
    if status is not None:
        records = [r for r in records if r.status == status]
    return {
        "count": len(records),
        "approval_requests": [
            {
                "approval_request_id": r.approval_request_id,
                "order_id": r.order_id,
                "provider": r.provider.value,
                "status": r.status.value,
                "requested_at": r.requested_at,
                "decided_at": r.decided_at,
                "resume_count": r.resume_count,
            }
            for r in records
        ],
    }


@app.get("/approval-requests/{approval_request_id}")
async def get_approval(approval_request_id: str):
    record = approvals_store.get(approval_request_id)
    if not record:
        raise HTTPException(status_code=404, detail="Approval request not found")
    return record.model_dump()

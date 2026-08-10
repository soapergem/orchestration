import asyncio
import json
from datetime import datetime, timezone
from enum import Enum
from typing import Literal

import boto3
import google.auth
import google.auth.transport.requests
import httpx
from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import AliasChoices, BaseModel, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

app = FastAPI(title="Approval Service")


class Settings(BaseSettings):
    """Service configuration, sourced from environment variables."""

    auto_decide_delay_seconds: int = 0
    auto_decide_action: Literal["none", "approved", "rejected"] = "none"
    aws_region: str | None = Field(
        default=None, validation_alias=AliasChoices("AWS_REGION", "AWS_DEFAULT_REGION")
    )
    # Kestra resume credentials -- see the matching block in
    # callback-fetch-service. Kestra OSS offers only the shared admin account.
    kestra_url: str = "http://kestra:8080"
    kestra_tenant: str = "main"
    kestra_user: str | None = None
    kestra_password: SecretStr | None = None
    # Conductor OSS has no authentication at all, so no credentials here.
    conductor_url: str = "http://conductor-server:8080"

    model_config = SettingsConfigDict(case_sensitive=False)


settings = Settings()


class ApprovalStatus(str, Enum):
    pending = "pending"
    approved = "approved"
    rejected = "rejected"


class Provider(str, Enum):
    """How this approval is resumed once decided."""

    stepfunctions = "stepfunctions"  # resume_data: {task_token, region?}
    http_callback = "http_callback"  # resume_data: {callback_url}
    kestra = "kestra"  # resume_data: {execution_id, tenant?}
    conductor = "conductor"  # resume_data: {workflow_id, task_ref_name, base_url?}
    google_workflows = "google_workflows"  # resume_data: {callback_url}, needs a GCP token


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
            elif self.resume_data.get("execution_id"):
                self.provider = Provider.kestra
            elif self.resume_data.get("workflow_id"):
                self.provider = Provider.conductor
            elif self.resume_data.get("callback_url"):
                self.provider = Provider.http_callback
            else:
                raise ValueError(
                    "cannot infer provider: supply `provider` or a resume_data "
                    "with task_token / execution_id / workflow_id / callback_url"
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


async def _resume_kestra(resume_data: dict, payload: dict) -> dict:
    """Resume a paused Kestra execution.

    Kestra's resume endpoint requires authentication (401 otherwise) and accepts
    only multipart/form-data (415 on JSON or urlencoded). The form field name
    must match an input declared in the flow's Pause task ``onResume`` block.
    """
    execution_id = resume_data.get("execution_id")
    if not execution_id:
        raise HTTPException(400, "resume_data missing execution_id")
    if not settings.kestra_user or not settings.kestra_password:
        raise HTTPException(500, "KESTRA_USER / KESTRA_PASSWORD are not configured")

    tenant = resume_data.get("tenant") or settings.kestra_tenant
    field = resume_data.get("on_resume_field", "payload")
    url = (
        f"{settings.kestra_url.rstrip('/')}/api/v1/{tenant}"
        f"/executions/{execution_id}/resume"
    )
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(
            url,
            # files=, not data=: httpx sends data= as urlencoded, which Kestra
            # rejects with 415. A (None, value) tuple is a plain multipart field.
            files={field: (None, json.dumps(payload))},
            auth=(settings.kestra_user, settings.kestra_password.get_secret_value()),
        )
    return {"action": "kestra_resume", "callback_status": resp.status_code}


async def _resume_conductor(resume_data: dict, payload: dict) -> dict:
    """Complete a blocked Conductor WAIT task by workflow id + task reference name.

    ``POST /api/tasks/{workflowId}/{taskRefName}/{status}`` with the task output
    as the JSON body -- verified against TaskResource.java in 3.31.0, not the
    ``/api/queue/update/...`` path older docs still give.

    Both approved and rejected resume as COMPLETED: the decision travels in the
    task *output* and DAG 4 branches on it with a SWITCH, exactly as the
    stepfunctions provider sends send_task_success for both. Failing the task
    instead would collapse rejection into an engine-level error and lose the
    distinction between "manager said no" and "the approval step broke".
    """
    workflow_id = resume_data.get("workflow_id")
    task_ref_name = resume_data.get("task_ref_name")
    if not workflow_id or not task_ref_name:
        raise HTTPException(400, "resume_data missing workflow_id / task_ref_name")

    base = (resume_data.get("base_url") or settings.conductor_url).rstrip("/")
    url = f"{base}/api/tasks/{workflow_id}/{task_ref_name}/COMPLETED"
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(url, json=payload)
    return {
        "action": "conductor_task_update",
        "task_status": "COMPLETED",
        "callback_status": resp.status_code,
    }


# ---------------------------------------------------------------------------
# Google Workflows resume support
# ---------------------------------------------------------------------------

_google_creds = None


def _google_access_token() -> str:
    """Mint an OAuth2 access token for resuming a Google Workflows execution.

    A callback endpoint minted by ``events.create_callback_endpoint`` lives on
    workflowexecutions.googleapis.com, so the POST must carry a Google *access*
    token (scope cloud-platform) -- not an OIDC ID token, which is what callers
    of a private Cloud Run service use. Sending the wrong kind yields an opaque
    401.

    Credentials come from GOOGLE_APPLICATION_CREDENTIALS / ADC, so on GKE or
    Cloud Run this needs no key file; off-GCP (this repo's K3s/OCI clusters) it
    is the mounted service-account key.
    """
    global _google_creds
    if _google_creds is None:
        creds, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        _google_creds = creds
    if not _google_creds.valid:
        _google_creds.refresh(google.auth.transport.requests.Request())
    return _google_creds.token


async def _resume_google_workflows(resume_data: dict, payload: dict) -> dict:
    callback_url = resume_data.get("callback_url")
    if not callback_url:
        raise HTTPException(400, "resume_data missing callback_url")
    try:
        token = _google_access_token()
    except Exception as exc:
        # No credentials on this deployment. Say so plainly -- the workflow will
        # otherwise just sit suspended until its own timeout fires.
        raise HTTPException(
            500,
            "google_workflows resume needs application default credentials "
            f"(mount a service-account key): {exc}",
        ) from exc
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            callback_url,
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
        )
    return {"action": "google_workflows", "callback_status": resp.status_code}


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

    if record.provider == Provider.kestra:
        return await _resume_kestra(record.resume_data, payload)

    if record.provider == Provider.conductor:
        return await _resume_conductor(record.resume_data, payload)

    if record.provider == Provider.google_workflows:
        return await _resume_google_workflows(record.resume_data, payload)

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

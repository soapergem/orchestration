import asyncio
import json
import random
import uuid
from datetime import datetime, timezone
from enum import Enum

import boto3
import google.auth
import google.auth.transport.requests
import httpx
from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import AliasChoices, BaseModel, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

app = FastAPI(title="Callback Fetch Service")


class Settings(BaseSettings):
    """Service configuration, sourced from environment variables."""

    fetch_delay_min_seconds: int = 2
    fetch_delay_max_seconds: int = 10
    fetch_timeout_seconds: int = 30
    # When on, fire the resume as soon as the fetch completes (hands-off DAG 2).
    # Off by default so resume stays a manual, externally-triggered step. A
    # per-request `auto_resume` overrides this default.
    auto_resume: bool = False
    auto_resume_delay_seconds: int = 0
    aws_region: str | None = Field(
        default=None, validation_alias=AliasChoices("AWS_REGION", "AWS_DEFAULT_REGION")
    )
    # Kestra resume credentials. Unlike Step Functions -- where the task token is
    # itself the handle -- Kestra's execution id is only an identifier, so the
    # resume call needs separate credentials. Kestra OSS has no service accounts
    # or API tokens (Enterprise only) and basic auth is mandatory since 0.24.0,
    # so this is necessarily the single shared admin account.
    kestra_url: str = "http://kestra:8080"
    kestra_tenant: str = "main"
    kestra_user: str | None = None
    kestra_password: SecretStr | None = None
    # Conductor needs no credentials at all: OSS ships with authentication
    # entirely absent, so any caller that can reach the API can complete any
    # task. Convenient here, disqualifying in production.
    conductor_url: str = "http://conductor-server:8080"

    model_config = SettingsConfigDict(case_sensitive=False)


settings = Settings()


class RequestStatus(str, Enum):
    pending = "pending"
    completed = "completed"
    failed = "failed"


class Provider(str, Enum):
    """How this pending request is resumed when /resume is called."""

    stepfunctions = "stepfunctions"  # resume_data: {task_token, region?}
    http_callback = "http_callback"  # resume_data: {callback_url}
    kestra = "kestra"  # resume_data: {execution_id, tenant?}
    conductor = "conductor"  # resume_data: {workflow_id, task_ref_name, base_url?}
    google_workflows = "google_workflows"  # resume_data: {callback_url}, needs a GCP token


class FetchRequest(BaseModel):
    """Register an async fetch plus the provider-specific handle used to resume.

    ``resume_data`` is an opaque, provider-shaped blob stored verbatim and only
    interpreted when /resume is called. ``callback_url`` is accepted as a
    top-level shorthand for the http_callback provider.
    """

    url: str
    headers: dict[str, str] = {}
    correlation_id: str | None = None
    provider: Provider | None = None
    resume_data: dict = {}
    callback_url: str | None = None
    auto_resume: bool | None = None  # overrides the AUTO_RESUME global default

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


class FetchRecord(BaseModel):
    correlation_id: str
    provider: Provider
    resume_data: dict
    status: RequestStatus
    requested_at: str
    completed_at: str | None = None
    http_status: int | None = None
    body: dict | list | None = None
    error: str | None = None
    resumed_at: str | None = None
    resume_count: int = 0


requests_store: dict[str, FetchRecord] = {}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _perform_fetch(record: FetchRecord, url: str, headers: dict[str, str]):
    """Simulate a slow external fetch, caching the outcome on ``record``.

    Does NOT resume the caller -- resume is a separate, manually-triggered step.
    """
    delay = random.uniform(settings.fetch_delay_min_seconds, settings.fetch_delay_max_seconds)
    await asyncio.sleep(delay)

    async with httpx.AsyncClient(timeout=settings.fetch_timeout_seconds) as client:
        try:
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            record.status = RequestStatus.completed
            record.http_status = resp.status_code
            record.body = resp.json()
        except httpx.HTTPStatusError as e:
            record.status = RequestStatus.failed
            record.http_status = e.response.status_code
            record.error = f"HTTP {e.response.status_code}: {e.response.text[:200]}"
        except Exception as e:
            record.status = RequestStatus.failed
            record.error = str(e)

    record.completed_at = _now()


def _result_payload(record: FetchRecord) -> dict:
    """Standard result envelope delivered to whichever provider is resumed."""
    payload: dict = {
        "correlation_id": record.correlation_id,
        "status": record.status.value,
        "http_status": record.http_status,
    }
    if record.status == RequestStatus.completed:
        payload["body"] = record.body
    else:
        payload["error"] = record.error
    return payload


async def _fetch_then_maybe_resume(
    record: FetchRecord, url: str, headers: dict[str, str], auto_resume: bool
):
    """Background task: run the fetch, and auto-fire the resume if enabled."""
    await _perform_fetch(record, url, headers)
    if not auto_resume:
        return
    if settings.auto_resume_delay_seconds:
        await asyncio.sleep(settings.auto_resume_delay_seconds)
    try:
        await _dispatch_resume(record)
        record.resumed_at = _now()
        record.resume_count += 1
    except Exception:
        # Best-effort, same contract as a manual resume failure: the
        # orchestrator's own timeout takes over.
        pass


async def _resume_kestra(resume_data: dict, payload: dict) -> dict:
    """Resume a paused Kestra execution.

    Kestra's resume endpoint is fussy in two ways that no generic webhook sender
    satisfies: it requires authentication (401 otherwise) and accepts only
    multipart/form-data (415 on JSON). The form field name must match an input
    declared in the flow's Pause task ``onResume`` block.
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


async def _resume_conductor(resume_data: dict, payload: dict, ok: bool) -> dict:
    """Complete a blocked Conductor WAIT task by workflow id + task reference name.

    ``POST /api/tasks/{workflowId}/{taskRefName}/{status}`` with the task output
    as the JSON body. Verified against TaskResource.java in 3.31.0 -- note this
    is NOT the ``/api/queue/update/...`` path that older Conductor docs and
    blog posts still give, which 404s on current servers.

    Unlike Kestra this needs no credentials, because Conductor OSS has no
    authentication to satisfy. The endpoint ``produces=text/plain``, so the
    response body is a bare task id string rather than JSON.
    """
    workflow_id = resume_data.get("workflow_id")
    task_ref_name = resume_data.get("task_ref_name")
    if not workflow_id or not task_ref_name:
        raise HTTPException(400, "resume_data missing workflow_id / task_ref_name")

    base = (resume_data.get("base_url") or settings.conductor_url).rstrip("/")
    status = "COMPLETED" if ok else "FAILED"
    url = f"{base}/api/tasks/{workflow_id}/{task_ref_name}/{status}"
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(url, json=payload)
    return {
        "action": "conductor_task_update",
        "task_status": status,
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


async def _dispatch_resume(record: FetchRecord) -> dict:
    """Perform the provider-specific resume using the cached result."""
    payload = _result_payload(record)

    if record.provider == Provider.stepfunctions:
        task_token = record.resume_data.get("task_token")
        if not task_token:
            raise HTTPException(400, "resume_data missing task_token")
        region = record.resume_data.get("region") or settings.aws_region

        def _resume():
            sfn = boto3.client("stepfunctions", region_name=region)
            if record.status == RequestStatus.completed:
                sfn.send_task_success(taskToken=task_token, output=json.dumps(payload))
                return "send_task_success"
            sfn.send_task_failure(
                taskToken=task_token,
                error="FetchFailed",
                cause=record.error or "Unknown error from fetch service",
            )
            return "send_task_failure"

        # boto3 is synchronous -- keep it off the event loop.
        action = await asyncio.to_thread(_resume)
        return {"action": action}

    if record.provider == Provider.kestra:
        return await _resume_kestra(record.resume_data, payload)

    if record.provider == Provider.conductor:
        return await _resume_conductor(
            record.resume_data, payload, record.status == RequestStatus.completed
        )

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


@app.post("/fetch-async", status_code=202)
async def submit_fetch(req: FetchRequest, background_tasks: BackgroundTasks):
    correlation_id = req.correlation_id or str(uuid.uuid4())
    record = FetchRecord(
        correlation_id=correlation_id,
        provider=req.provider,
        resume_data=req.resume_data,
        status=RequestStatus.pending,
        requested_at=_now(),
    )
    requests_store[correlation_id] = record
    auto_resume = settings.auto_resume if req.auto_resume is None else req.auto_resume
    background_tasks.add_task(
        _fetch_then_maybe_resume, record, req.url, req.headers, auto_resume
    )
    return {
        "correlation_id": correlation_id,
        "status": "accepted",
        "provider": record.provider.value,
        "auto_resume": auto_resume,
    }


@app.post("/resume/{correlation_id}")
async def resume(correlation_id: str):
    """Fire the provider-specific resume for a completed (or failed) fetch.

    Idempotency, duplicate resumes, and late resumes after a workflow timeout
    are intentionally allowed here -- enforcing them is the orchestrator's job,
    and leaving them open lets the bake-off exercise those edge cases.
    """
    record = requests_store.get(correlation_id)
    if not record:
        raise HTTPException(404, "Unknown correlation_id")
    if record.status == RequestStatus.pending:
        raise HTTPException(409, "fetch not complete yet; nothing to resume")

    result = await _dispatch_resume(record)
    record.resumed_at = _now()
    record.resume_count += 1
    return {
        "correlation_id": correlation_id,
        "status": "resumed",
        "provider": record.provider.value,
        "resume_count": record.resume_count,
        **result,
    }


@app.get("/requests")
async def list_requests(status: RequestStatus | None = None, resumed: bool | None = None):
    """List tracked requests, newest first. Filter by ``status`` (e.g. pending)
    and/or ``resumed`` to find what's awaiting a /resume call."""
    records = sorted(
        requests_store.values(), key=lambda r: r.requested_at, reverse=True
    )
    if status is not None:
        records = [r for r in records if r.status == status]
    if resumed is not None:
        records = [r for r in records if (r.resume_count > 0) == resumed]
    return {
        "count": len(records),
        "requests": [
            {
                "correlation_id": r.correlation_id,
                "provider": r.provider.value,
                "status": r.status.value,
                "requested_at": r.requested_at,
                "completed_at": r.completed_at,
                "resume_count": r.resume_count,
                "resumed_at": r.resumed_at,
            }
            for r in records
        ],
    }


@app.get("/status/{correlation_id}")
async def get_status(correlation_id: str):
    record = requests_store.get(correlation_id)
    if not record:
        raise HTTPException(status_code=404, detail="Unknown correlation_id")
    result: dict = {
        "correlation_id": record.correlation_id,
        "status": record.status.value,
        "provider": record.provider.value,
        "resume_count": record.resume_count,
    }
    if record.status == RequestStatus.completed:
        result["http_status"] = record.http_status
        result["body"] = record.body
    elif record.status == RequestStatus.failed:
        result["http_status"] = record.http_status
        result["error"] = record.error
    return result

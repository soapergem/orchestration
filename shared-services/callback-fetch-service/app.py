import asyncio
import json
import random
import uuid
from datetime import datetime, timezone
from enum import Enum

import boto3
import httpx
from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import AliasChoices, BaseModel, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

app = FastAPI(title="Callback Fetch Service")


class Settings(BaseSettings):
    """Service configuration, sourced from environment variables."""

    model_config = SettingsConfigDict(case_sensitive=False)

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


settings = Settings()


class RequestStatus(str, Enum):
    pending = "pending"
    completed = "completed"
    failed = "failed"


class Provider(str, Enum):
    """How this pending request is resumed when /resume is called."""

    stepfunctions = "stepfunctions"  # resume_data: {task_token, region?}
    http_callback = "http_callback"  # resume_data: {callback_url}


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
            elif self.resume_data.get("callback_url"):
                self.provider = Provider.http_callback
            else:
                raise ValueError(
                    "cannot infer provider: supply `provider` or a resume_data "
                    "with task_token / callback_url"
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

import asyncio
import os
import uuid
from datetime import datetime, timezone
from enum import Enum

import httpx
from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Callback Fetch Service")

FETCH_DELAY_MIN = int(os.getenv("FETCH_DELAY_MIN_SECONDS", "2"))
FETCH_DELAY_MAX = int(os.getenv("FETCH_DELAY_MAX_SECONDS", "10"))
FETCH_TIMEOUT = int(os.getenv("FETCH_TIMEOUT_SECONDS", "30"))


class RequestStatus(str, Enum):
    pending = "pending"
    completed = "completed"
    failed = "failed"


class FetchRequest(BaseModel):
    url: str
    headers: dict[str, str] = {}
    callback_url: str
    correlation_id: str | None = None


class FetchRecord(BaseModel):
    correlation_id: str
    status: RequestStatus
    requested_at: str
    completed_at: str | None = None
    http_status: int | None = None
    body: dict | list | None = None
    error: str | None = None


requests_store: dict[str, FetchRecord] = {}


async def perform_fetch_and_callback(record: FetchRecord, url: str, headers: dict[str, str], callback_url: str):
    import random

    delay = random.uniform(FETCH_DELAY_MIN, FETCH_DELAY_MAX)
    await asyncio.sleep(delay)

    async with httpx.AsyncClient(timeout=FETCH_TIMEOUT) as client:
        try:
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            body = resp.json()
            record.status = RequestStatus.completed
            record.http_status = resp.status_code
            record.body = body
        except httpx.HTTPStatusError as e:
            record.status = RequestStatus.failed
            record.http_status = e.response.status_code
            record.error = f"HTTP {e.response.status_code}: {e.response.text[:200]}"
        except Exception as e:
            record.status = RequestStatus.failed
            record.error = str(e)

        record.completed_at = datetime.now(timezone.utc).isoformat()

        callback_payload = {
            "correlation_id": record.correlation_id,
            "status": record.status.value,
            "http_status": record.http_status,
        }
        if record.status == RequestStatus.completed:
            callback_payload["body"] = record.body
        else:
            callback_payload["error"] = record.error

        try:
            await client.post(callback_url, json=callback_payload, timeout=10)
        except Exception:
            pass


@app.post("/fetch-async", status_code=202)
async def submit_fetch(req: FetchRequest, background_tasks: BackgroundTasks):
    correlation_id = req.correlation_id or str(uuid.uuid4())
    record = FetchRecord(
        correlation_id=correlation_id,
        status=RequestStatus.pending,
        requested_at=datetime.now(timezone.utc).isoformat(),
    )
    requests_store[correlation_id] = record
    background_tasks.add_task(perform_fetch_and_callback, record, req.url, req.headers, req.callback_url)
    return {"correlation_id": correlation_id, "status": "accepted"}


@app.get("/status/{correlation_id}")
async def get_status(correlation_id: str):
    record = requests_store.get(correlation_id)
    if not record:
        raise HTTPException(status_code=404, detail="Unknown correlation_id")
    result: dict = {
        "correlation_id": record.correlation_id,
        "status": record.status.value,
    }
    if record.status == RequestStatus.completed:
        result["http_status"] = record.http_status
        result["body"] = record.body
    elif record.status == RequestStatus.failed:
        result["http_status"] = record.http_status
        result["error"] = record.error
    return result

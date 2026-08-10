"""
Hatchet event relay -- HTTP bridge between the mock services and Hatchet events.

DAG 2 and DAG 4 suspend on a durable event wait (``aio_wait_for_event``). The
callback-fetch and approval services resume them by POSTing a plain JSON result
to a ``callback_url``. Hatchet cannot be that URL directly:

  - its event endpoint is ``POST /api/v1/tenants/{tenant}/events``, which
    requires a bearer token the mock services do not carry, and
  - it expects a ``{"key": ..., "data": ...}`` envelope, not the bare result body.

So this relay accepts the mock services' POST, merges the query-string
correlation fields into the payload (the workflows' CEL expressions match on
them), and pushes a properly-formed Hatchet event with the SDK's credentials.

This is the Hatchet analogue of ``temporal/signal_server.py``.

Usage:
    HATCHET_CLIENT_TOKEN=... uvicorn event_relay:app --host 0.0.0.0 --port 8096
"""

import logging
from typing import Any

from fastapi import FastAPI, Request
from hatchet_sdk import Hatchet

logger = logging.getLogger(__name__)

app = FastAPI(title="Hatchet Event Relay")
hatchet = Hatchet()


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


# ---- DAG 2: callback-fetch-service -> fetch_completed --------------------


@app.post("/fetch-callback")
async def fetch_callback(request: Request) -> dict[str, Any]:
    """Relay a completed async fetch as a ``fetch_completed`` event.

    ``correlation_id`` arrives as a query parameter (the workflow builds the
    callback URL) and must end up *inside* the event payload, because the
    workflow's wait expression matches on ``.correlation_id``.
    """
    return await _relay(request, default_key="fetch_completed")


# ---- DAG 4: approval-service -> approval_decision ------------------------


@app.post("/approval-callback")
async def approval_callback(request: Request) -> dict[str, Any]:
    """Relay an approval decision as an ``approval_decision`` event."""
    return await _relay(request, default_key="approval_decision")


# ---- Shared ---------------------------------------------------------------


async def _relay(request: Request, default_key: str) -> dict[str, Any]:
    params = dict(request.query_params)
    event_key = params.pop("event_type", None) or default_key

    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {"body": body}

    # Query params are the correlation handles the CEL expressions match on;
    # the body wins on conflict so a real decision field is never overwritten.
    payload = {**params, **body}

    event = await hatchet.event.aio_push(event_key, payload)
    logger.info(
        "relayed %s (id=%s) correlation=%s",
        event_key,
        getattr(event, "eventId", None),
        {k: params.get(k) for k in ("correlation_id", "order_id")},
    )
    return {"status": "relayed", "event_key": event_key}


if __name__ == "__main__":
    import uvicorn

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    uvicorn.run(app, host="0.0.0.0", port=8096)

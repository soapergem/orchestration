"""
Custom Airflow trigger that polls the callback-fetch-service /status/<correlation_id>
endpoint until the async fetch completes or times out.

Used by DAG 2 (API Fan-Out) to implement deferrable async waiting -- the worker
is freed while this trigger polls in the triggerer process.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, AsyncIterator

import aiohttp
from airflow.triggers.base import BaseTrigger, TriggerEvent


class FetchCallbackTrigger(BaseTrigger):
    """
    Polls GET /status/<correlation_id> on the callback-fetch-service every
    ``poll_interval`` seconds. Fires a TriggerEvent when the status is
    "completed" or "failed", or when ``timeout`` seconds have elapsed.
    """

    def __init__(
        self,
        correlation_id: str,
        fetch_service_url: str = "http://callback-fetch-service:8090",
        poll_interval: float = 5.0,
        timeout: float = 120.0,
    ) -> None:
        super().__init__()
        self.correlation_id = correlation_id
        self.fetch_service_url = fetch_service_url.rstrip("/")
        self.poll_interval = poll_interval
        self.timeout = timeout

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "triggers.fetch_callback_trigger.FetchCallbackTrigger",
            {
                "correlation_id": self.correlation_id,
                "fetch_service_url": self.fetch_service_url,
                "poll_interval": self.poll_interval,
                "timeout": self.timeout,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Poll the fetch service status endpoint until completion or timeout."""
        status_url = f"{self.fetch_service_url}/status/{self.correlation_id}"
        start_time = datetime.now(timezone.utc)

        async with aiohttp.ClientSession() as session:
            while True:
                elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
                if elapsed >= self.timeout:
                    yield TriggerEvent(
                        {
                            "status": "timeout",
                            "correlation_id": self.correlation_id,
                            "message": (
                                f"Fetch callback timed out after {self.timeout}s "
                                f"for correlation_id={self.correlation_id}"
                            ),
                        }
                    )
                    return

                try:
                    async with session.get(status_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        if resp.status == 200:
                            payload = await resp.json()
                            status = payload.get("status", "unknown")

                            if status == "completed":
                                yield TriggerEvent(
                                    {
                                        "status": "completed",
                                        "correlation_id": self.correlation_id,
                                        "body": payload.get("body"),
                                        "url": payload.get("url"),
                                    }
                                )
                                return

                            if status == "failed":
                                yield TriggerEvent(
                                    {
                                        "status": "failed",
                                        "correlation_id": self.correlation_id,
                                        "error": payload.get("error", "Unknown fetch error"),
                                    }
                                )
                                return

                            # status is still "pending" -- keep polling
                        elif resp.status == 404:
                            # Correlation ID not yet registered; keep polling
                            pass
                        else:
                            self.log.warning(
                                "Unexpected status %s from %s", resp.status, status_url
                            )

                except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                    self.log.warning(
                        "Error polling %s: %s -- will retry", status_url, exc
                    )

                await asyncio.sleep(self.poll_interval)

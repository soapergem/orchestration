import random
import uuid
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

app = FastAPI(title="Shipping Service")


class Settings(BaseSettings):
    """Service configuration, sourced from environment variables."""

    model_config = SettingsConfigDict(case_sensitive=False)

    shipping_success_rate: float = 0.70
    shipping_timeout_rate: float = 0.15
    shipping_server_error_rate: float = 0.10
    # Remaining probability is InvalidAddress (non-retriable)


settings = Settings()


class ShipmentRequest(BaseModel):
    order_id: str
    items: list[dict]
    shipping_address: dict
    idempotency_key: str | None = None


class ShipmentError(Exception):
    def __init__(self, error_type: str, message: str, status_code: int):
        self.error_type = error_type
        self.message = message
        self.status_code = status_code


completed_shipments: dict[str, dict] = {}


@app.post("/shipments")
async def create_shipment(req: ShipmentRequest):
    if req.idempotency_key and req.idempotency_key in completed_shipments:
        return completed_shipments[req.idempotency_key]

    address = req.shipping_address
    required_fields = ("street", "city", "state", "zip")
    missing = [f for f in required_fields if not address.get(f)]
    if missing:
        raise HTTPException(
            status_code=422,
            detail={"error_type": "InvalidAddress", "message": f"Missing address fields: {', '.join(missing)}"},
        )

    roll = random.random()
    if roll < settings.shipping_success_rate:
        result = {
            "shipment_id": f"SHIP-{uuid.uuid4().hex[:12].upper()}",
            "order_id": req.order_id,
            "tracking_number": f"1Z{uuid.uuid4().hex[:16].upper()}",
            "carrier": "simulated-carrier",
            "estimated_delivery": (datetime.now(timezone.utc) + timedelta(days=random.randint(3, 7))).strftime(
                "%Y-%m-%d"
            ),
            "status": "shipped",
        }
        if req.idempotency_key:
            completed_shipments[req.idempotency_key] = result
        return result

    if roll < settings.shipping_success_rate + settings.shipping_timeout_rate:
        raise HTTPException(
            status_code=504,
            detail={"error_type": "ShippingTimeout", "message": "Carrier API timed out"},
        )

    if roll < settings.shipping_success_rate + settings.shipping_timeout_rate + settings.shipping_server_error_rate:
        raise HTTPException(
            status_code=503,
            detail={"error_type": "ShippingServiceError", "message": "Carrier API returned 503"},
        )

    raise HTTPException(
        status_code=422,
        detail={
            "error_type": "InvalidAddress",
            "message": "Address validation failed: undeliverable address",
        },
    )

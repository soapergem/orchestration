"""
Lambda: CallShippingAPI
Calls the simulated Shipping Service API. Raises typed exceptions for
retriable vs non-retriable errors so Step Functions can route them correctly.
"""

import json
import os

import urllib3

http = urllib3.PoolManager()

SHIPPING_SERVICE_URL = os.environ.get("SHIPPING_SERVICE_URL", "http://shipping-service:8092")


class ShippingTimeout(Exception):
    pass


class ShippingServiceError(Exception):
    pass


class InvalidAddress(Exception):
    pass


def handler(event, context):
    order_id = event["order_id"]
    items = event["items"]
    shipping_address = event["shipping_address"]

    idempotency_key = f"{order_id}-ship"

    payload = {
        "order_id": order_id,
        "items": items,
        "shipping_address": shipping_address,
        "idempotency_key": idempotency_key,
    }

    response = http.request(
        "POST",
        f"{SHIPPING_SERVICE_URL}/shipments",
        body=json.dumps(payload),
        headers={"Content-Type": "application/json"},
        timeout=30.0,
    )

    body = json.loads(response.data.decode("utf-8"))

    if response.status == 200:
        return body

    error_type = body.get("detail", {}).get("error_type", "Unknown") if isinstance(body.get("detail"), dict) else "Unknown"
    message = body.get("detail", {}).get("message", str(body)) if isinstance(body.get("detail"), dict) else str(body)

    if error_type == "InvalidAddress":
        raise InvalidAddress(message)
    elif error_type == "ShippingTimeout" or response.status == 504:
        raise ShippingTimeout(message)
    elif error_type == "ShippingServiceError" or response.status >= 500:
        raise ShippingServiceError(message)
    else:
        raise Exception(f"Unexpected shipping error ({response.status}): {message}")

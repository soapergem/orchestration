"""
Calls the Shipping Service API.
Exits with non-zero status on failure so Argo's retryStrategy can kick in.
"""

import json
import os
import sys

import urllib3

http = urllib3.PoolManager()

SHIPPING_SERVICE_URL = os.environ.get("SHIPPING_SERVICE_URL", "http://shipping-service:8092")


def main():
    event = json.loads(os.environ["INPUT"])

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
        json.dump(body, sys.stdout)
        return

    error_type = "Unknown"
    message = str(body)
    if isinstance(body.get("detail"), dict):
        error_type = body["detail"].get("error_type", "Unknown")
        message = body["detail"].get("message", str(body))

    if error_type == "InvalidAddress":
        print(f"InvalidAddress (non-retriable): {message}", file=sys.stderr)
        sys.exit(2)
    elif error_type == "ShippingTimeout" or response.status == 504:
        print(f"ShippingTimeout (retriable): {message}", file=sys.stderr)
        sys.exit(1)
    elif error_type == "ShippingServiceError" or response.status >= 500:
        print(f"ShippingServiceError (retriable): {message}", file=sys.stderr)
        sys.exit(1)
    else:
        print(f"Unexpected shipping error ({response.status}): {message}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

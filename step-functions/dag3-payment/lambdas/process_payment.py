"""
Lambda: ProcessPayment
Calls a simulated external payment gateway API.
Must be idempotent -- uses an idempotency key to prevent double-charging.
"""

import json
import random

import urllib3


http = urllib3.PoolManager()


class PaymentGatewayTimeout(Exception):
    pass


class PaymentGateway5xx(Exception):
    pass


class PaymentDeclined(Exception):
    pass


def handler(event, context):
    payment_id = event["payment_id"]
    amount = event["amount"]
    currency = event["currency"]
    from_account = event["from_account"]
    to_account = event["to_account"]
    idempotency_key = event.get("idempotency_key", payment_id)

    # --- Simulated payment gateway call ---
    # In a real implementation, this would call an external API like Stripe, PayPal, etc.
    # The idempotency_key ensures that retries don't cause duplicate charges.
    #
    # For the bake-off, we simulate a flaky gateway:
    #   - 60% success
    #   - 20% timeout (retriable)
    #   - 15% 5xx error (retriable)
    #   - 5% declined (not retriable)

    roll = random.random()

    if roll < 0.05:
        raise PaymentDeclined(
            json.dumps(
                {
                    "payment_id": payment_id,
                    "reason": "Card declined by issuing bank",
                    "decline_code": "insufficient_funds",
                }
            )
        )
    elif roll < 0.20:
        raise PaymentGateway5xx(
            f"Payment gateway returned 500 for payment {payment_id}"
        )
    elif roll < 0.40:
        raise PaymentGatewayTimeout(
            f"Payment gateway timed out for payment {payment_id}"
        )

    # Simulate successful payment
    gateway_transaction_id = f"gw-txn-{payment_id}-{random.randint(10000, 99999)}"

    return {
        **event,
        "payment_result": {
            "status": "success",
            "gateway_transaction_id": gateway_transaction_id,
            "amount_charged": amount,
            "currency": currency,
        },
    }

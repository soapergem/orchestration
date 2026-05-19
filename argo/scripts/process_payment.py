"""
Calls a simulated external payment gateway API.
Must be idempotent -- uses an idempotency key to prevent double-charging.
"""

import json
import os
import random
import sys


def main():
    event = json.loads(os.environ["INPUT"])

    payment_id = event["payment_id"]
    amount = event["amount"]
    currency = event["currency"]
    from_account = event["from_account"]
    to_account = event["to_account"]
    idempotency_key = event.get("idempotency_key", payment_id)

    # Simulated payment gateway call.
    # The idempotency_key ensures that retries don't cause duplicate charges.
    #
    # Simulation:
    #   - 60% success
    #   - 20% timeout (retriable)
    #   - 15% 5xx error (retriable)
    #   - 5% declined (not retriable)
    roll = random.random()

    if roll < 0.05:
        print(
            json.dumps({
                "error": "PaymentDeclined",
                "payment_id": payment_id,
                "reason": "Card declined by issuing bank",
                "decline_code": "insufficient_funds",
            }),
            file=sys.stderr,
        )
        # Exit code 2 signals a non-retriable error
        sys.exit(2)
    elif roll < 0.20:
        print(f"PaymentGateway5xx: Payment gateway returned 500 for payment {payment_id}", file=sys.stderr)
        sys.exit(1)
    elif roll < 0.40:
        print(f"PaymentGatewayTimeout: Payment gateway timed out for payment {payment_id}", file=sys.stderr)
        sys.exit(1)

    # Simulate successful payment
    gateway_transaction_id = f"gw-txn-{payment_id}-{random.randint(10000, 99999)}"

    result = {
        **event,
        "payment_result": {
            "status": "success",
            "gateway_transaction_id": gateway_transaction_id,
            "amount_charged": amount,
            "currency": currency,
        },
    }
    json.dump(result, sys.stdout)


if __name__ == "__main__":
    main()

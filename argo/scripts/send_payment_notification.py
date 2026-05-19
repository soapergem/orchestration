"""
Sends a payment notification (success or failure) via simulated email/webhook.
"""

import json
import os
import sys


def main():
    event = json.loads(os.environ["INPUT"])

    payment_id = event["payment_id"]
    status = event.get("status", "success")
    amount = event.get("amount")
    currency = event.get("currency")

    if status == "failed":
        message = event.get("message", "Payment processing failed")
        subject = f"Payment Failed: {payment_id}"
        body = f"Payment {payment_id} for {amount} {currency} has failed.\nReason: {message}"
    else:
        gateway_txn = event.get("payment_result", {}).get("gateway_transaction_id", "N/A")
        subject = f"Payment Successful: {payment_id}"
        body = (
            f"Payment {payment_id} for {amount} {currency} was processed successfully.\n"
            f"Gateway Transaction ID: {gateway_txn}"
        )

    # Simulated notification -- log and return
    print(f"NOTIFICATION: {subject}", file=sys.stderr)
    print(f"BODY: {body}", file=sys.stderr)

    result = {
        "payment_id": payment_id,
        "notification": {
            "status": "sent",
            "subject": subject,
            "channel": "simulated",
        },
    }
    json.dump(result, sys.stdout)


if __name__ == "__main__":
    main()

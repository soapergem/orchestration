"""
Lambda: SendNotification
Sends a payment notification (success or failure) via simulated email/webhook.
"""

import json

import urllib3


http = urllib3.PoolManager()


def handler(event, context):
    payment_id = event["payment_id"]
    status = event.get("status", "success")
    amount = event.get("amount")
    currency = event.get("currency")

    if status == "failed":
        message = event.get("message", "Payment processing failed")
        subject = f"Payment Failed: {payment_id}"
        body = f"Payment {payment_id} for {amount} {currency} has failed.\nReason: {message}"
    else:
        gateway_txn = event.get("payment_result", {}).get(
            "gateway_transaction_id", "N/A"
        )
        subject = f"Payment Successful: {payment_id}"
        body = (
            f"Payment {payment_id} for {amount} {currency} was processed successfully.\n"
            f"Gateway Transaction ID: {gateway_txn}"
        )

    # --- Simulated notification ---
    # In production, this would call SES, SNS, or a webhook endpoint.
    # For the bake-off, we just log the notification.
    print(f"NOTIFICATION: {subject}")
    print(f"BODY: {body}")

    return {
        "payment_id": payment_id,
        "notification": {
            "status": "sent",
            "subject": subject,
            "channel": "simulated",
        },
    }

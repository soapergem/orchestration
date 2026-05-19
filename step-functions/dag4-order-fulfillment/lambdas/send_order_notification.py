"""
Lambda: SendOrderNotification
Sends a simulated notification (email/webhook) for order status changes.
Used for both success (shipped) and cancellation paths.
"""

import json
from datetime import datetime, timezone


def handler(event, context):
    order_id = event.get("order_id")
    status = event.get("status")

    notification = {
        "order_id": order_id,
        "status": status,
        "sent_at": datetime.now(timezone.utc).isoformat(),
        "channel": "simulated_email",
    }

    if status == "shipped":
        notification["message"] = (
            f"Your order {order_id} has been shipped! "
            f"Tracking: {event.get('tracking_number', 'N/A')} "
            f"via {event.get('carrier', 'N/A')}. "
            f"Estimated delivery: {event.get('estimated_delivery', 'N/A')}."
        )
    elif status == "cancelled":
        notification["message"] = (
            f"Your order {order_id} has been cancelled. "
            f"Reason: {event.get('failure_reason', 'N/A')}."
        )
    else:
        notification["message"] = f"Order {order_id} status update: {status}."

    # In a real system, this would call SES, SNS, or a webhook.
    # For the bake-off, we just return the notification payload.
    print(json.dumps(notification))

    return {
        "notification_sent": True,
        "order_id": order_id,
        "status": status,
        "sent_at": notification["sent_at"],
    }

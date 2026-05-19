"""
Sends a simulated notification (email/webhook) for order status changes.
Used for both success (shipped) and cancellation paths.
"""

import json
import os
import sys
from datetime import datetime, timezone


def main():
    event = json.loads(os.environ["INPUT"])

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

    print(json.dumps(notification), file=sys.stderr)

    result = {
        "notification_sent": True,
        "order_id": order_id,
        "status": status,
        "sent_at": notification["sent_at"],
    }
    json.dump(result, sys.stdout)


if __name__ == "__main__":
    main()

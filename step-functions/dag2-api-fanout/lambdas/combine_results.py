"""
Lambda: CombineResults
Receives the array of all fan-out API results and merges them into a summary.
"""

import json


def handler(event, context):
    api_results = event["api_results"]
    source_url = event.get("source_url", "unknown")

    # Build a summary from all the detail responses
    combined = []
    errors = []

    for result in api_results:
        if "error" in result:
            errors.append({"id": result.get("id"), "error": result["error"]})
        else:
            combined.append(
                {
                    "id": result["id"],
                    "name": result["name"],
                    "detail": result.get("detail", {}),
                }
            )

    return {
        "status": "success",
        "source_url": source_url,
        "total_items": len(api_results),
        "successful": len(combined),
        "failed": len(errors),
        "results": combined,
        "errors": errors,
    }

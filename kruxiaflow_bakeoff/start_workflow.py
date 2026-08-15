#!/usr/bin/env python3
"""Submit a bake-off workflow to Kruxia Flow and follow it to a terminal state.

    source kruxiaflow_bakeoff/env.sh
    ./kruxiaflow_bakeoff/start_workflow.py dag3
    ./kruxiaflow_bakeoff/start_workflow.py dag3 --force-outcome decline
    ./kruxiaflow_bakeoff/start_workflow.py dag3 --amount 100 --from ACC-001

Kruxia Flow has no CLI equivalent of `temporal workflow start` for this, so
submitting is a plain POST -- which is itself the point: the whole surface is
HTTP, and nothing here imports the SDK.

Every id defaults to a *generated* value. That matters: DAG 3 and DAG 4 are
idempotent by refusal, so re-running with a fixed id reports success while doing
nothing and silently tests the duplicate path instead of the happy path (see
CLAUDE.md). Pass --payment-id explicitly when you want that behaviour.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

API = os.environ.get("KRUXIAFLOW_API_URL", "http://localhost:8100").rstrip("/")
NS = os.environ.get("BAKEOFF_NS", "kruxiaflow")
# The engine's built-in worker runs inside the container, so postgres is
# `postgres:5432` from its point of view -- NOT localhost:54321. See env.sh.
DB_URL = os.environ.get(
    "KF_DB_URL_PLAIN",
    "postgres://orchestration:orchestration@postgres:5432/orchestration",
)

TERMINAL = {"completed", "failed", "cancelled", "timed_out"}


def _post(path: str, body: dict) -> dict:
    req = urllib.request.Request(
        f"{API}{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    if os.environ.get("KRUXIAFLOW_TOKEN"):
        req.add_header("Authorization", f"Bearer {os.environ['KRUXIAFLOW_TOKEN']}")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def _get(path: str) -> dict:
    req = urllib.request.Request(f"{API}{path}")
    if os.environ.get("KRUXIAFLOW_TOKEN"):
        req.add_header("Authorization", f"Bearer {os.environ['KRUXIAFLOW_TOKEN']}")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def build_input(args: argparse.Namespace, stamp: str) -> dict:
    if args.dag == "dag3":
        return {
            "schema": f"{NS}_dag3",
            "db_url": DB_URL,
            "payment_id": args.payment_id or f"PAY-{stamp}",
            "idempotency_key": args.idempotency_key or f"IDEM-{stamp}",
            "amount": args.amount,
            "currency": args.currency,
            "from_account": getattr(args, "from"),
            "to_account": args.to,
            # Empty string rather than absent: an unset template variable is a
            # different failure mode from an empty one, and the worker treats
            # "" as "roll randomly".
            "force_outcome": args.force_outcome or "",
            "force_notification_failure": args.force_notification_failure,
        }
    raise SystemExit(f"unknown dag: {args.dag}")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("dag", choices=["dag3"])
    p.add_argument("--payment-id")
    p.add_argument("--idempotency-key")
    p.add_argument("--amount", type=float, default=100.0)
    p.add_argument("--currency", default="USD")
    p.add_argument("--from", default="ACC-001")
    p.add_argument("--to", default="ACC-003")
    p.add_argument(
        "--force-outcome",
        choices=["decline", "5xx", "timeout", "success"],
        help="make the flaky gateway deterministic, for edge-case runs",
    )
    p.add_argument("--force-notification-failure", action="store_true")
    p.add_argument("--timeout", type=int, default=300)
    args = p.parse_args()

    stamp = datetime.now(timezone.utc).strftime("%H%M%S")
    definition = f"kruxiaflow_{args.dag}_payment" if args.dag == "dag3" else ""
    payload = build_input(args, stamp)

    print(f"definition  {definition}")
    print(f"input       {json.dumps(payload, indent=12)[1:-1].strip()}")

    try:
        started = _post(
            "/api/v1/workflows", {"definition_name": definition, "input": payload}
        )
    except urllib.error.HTTPError as e:
        print(f"submit failed: HTTP {e.code}\n{e.read().decode()[:500]}", file=sys.stderr)
        return 1

    wf_id = started["workflow_id"]
    print(f"workflow    {wf_id}")

    deadline = time.time() + args.timeout
    last = None
    while time.time() < deadline:
        state = _get(f"/api/v1/workflows/{wf_id}")
        status = state["status"]
        if status != last:
            print(f"  [{time.strftime('%H:%M:%S')}] {status}")
            last = status
        if status in TERMINAL:
            break
        time.sleep(2)
    else:
        print(f"still running after {args.timeout}s", file=sys.stderr)
        return 2

    print(f"\nfinal       {state['status']}")
    if state.get("error_message"):
        print(f"error       {state['error_message'][:300]}")
    print("\nactivities:")
    for a in state["activities"]:
        err = f"  <- {a['error'][:90]}" if a.get("error") else ""
        print(f"  {a['activity_key']:<28} {a['status']}{err}")

    # `failed` is the expected terminal state for the compensation paths: an
    # activity failing makes the WORKFLOW fail even when its failure-path
    # dependents all completed. Exit 0 for those so scripted edge-case runs
    # don't read as broken -- see README finding on unhandleable failure.
    handled = any(
        a["activity_key"].startswith(("record_", "notify_")) and a["status"] == "completed"
        for a in state["activities"]
    )
    if state["status"] == "completed" or handled:
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())

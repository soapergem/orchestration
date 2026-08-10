"""
Start any of the four bake-off workflows and (optionally) follow it to a
terminal state.

    source conductor/env.sh
    uv run python conductor/start_workflow.py dag1 --wait
    uv run python conductor/start_workflow.py dag2 --wait
    uv run python conductor/start_workflow.py dag3 --amount 250 --wait
    uv run python conductor/start_workflow.py dag4 --order-total high --wait

Edge-case switches used by the verification runs in conductor/README.md:
    dag2 --no-auto-resume     callback never arrives -> WAIT times out
    dag4 --reject             approval rejected      -> saga via SWITCH
    dag4 --no-decide          approval times out     -> saga via failureWorkflow
"""

import argparse
import json
import os
import time
import uuid

import requests

SERVER = os.environ.get("CONDUCTOR_SERVER_URL", "http://localhost:8000/api")
BASE = SERVER.rstrip("/").removesuffix("/api")
APPROVAL_SERVICE_URL = os.environ.get("APPROVAL_SERVICE_URL", "http://localhost:8091")

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def start(name: str, payload: dict) -> str:
    resp = requests.post(
        f"{BASE}/api/workflow/{name}",
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    if resp.status_code >= 300:
        raise SystemExit(f"start failed {resp.status_code}: {resp.text[:400]}")
    return resp.text.strip().strip('"')


def poll(workflow_id: str, timeout: int = 600) -> dict:
    """Follow a run to a terminal state, printing each task transition."""
    deadline = time.time() + timeout
    seen: dict[str, str] = {}
    while time.time() < deadline:
        resp = requests.get(f"{BASE}/api/workflow/{workflow_id}", timeout=30)
        resp.raise_for_status()
        wf = resp.json()

        for task in wf.get("tasks", []):
            ref, status = task["referenceTaskName"], task["status"]
            if seen.get(ref) != status:
                seen[ref] = status
                retries = task.get("retryCount", 0)
                suffix = f" (retry {retries})" if retries else ""
                print(f"  {status:<22} {ref}{suffix}")

        if wf["status"] in ("COMPLETED", "FAILED", "TERMINATED", "TIMED_OUT"):
            return wf
        time.sleep(2)

    raise SystemExit(f"timed out after {timeout}s waiting for {workflow_id}")


def report(wf: dict) -> int:
    print(f"\nstatus : {wf['status']}")
    if wf.get("reasonForIncompletion"):
        print(f"reason : {wf['reasonForIncompletion']}")
    print(f"output : {json.dumps(wf.get('output', {}), indent=2, default=str)[:1500]}")
    print(f"UI     : {os.environ.get('CONDUCTOR_UI_SERVER_URL', 'http://localhost:8127')}"
          f"/execution/{wf['workflowId']}")
    return 0 if wf["status"] == "COMPLETED" else 1


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("dag", choices=["dag1", "dag2", "dag3", "dag4"])
    p.add_argument("--wait", action="store_true", help="follow the run to completion")
    p.add_argument("--timeout", type=int, default=600)
    # dag1
    p.add_argument("--zip-path", default=os.environ.get(
        "ETL_ZIP_PATH", os.path.join(REPO_ROOT, "test-data", "sample-data.zip")))
    p.add_argument("--output-dir", default=os.environ.get(
        "ETL_OUTPUT_DIR", "/tmp/conductor_etl_output"))
    # dag2
    p.add_argument("--per-page", type=int, default=5)
    p.add_argument("--no-auto-resume", action="store_true",
                   help="dag2: the callback never fires, so the WAIT task times out")
    p.add_argument("--empty", action="store_true",
                   help="dag2: fetch succeeds but returns zero items -> SWITCH defaultCase")
    # dag3
    p.add_argument("--amount", type=float, default=250.0)
    p.add_argument("--from-account", default="ACC-001")
    p.add_argument("--to-account", default="ACC-003")
    # dag4
    p.add_argument("--order-total", choices=["high", "low"], default="high",
                   help="high (>= $500) routes through approval; low skips it")
    p.add_argument("--reject", action="store_true",
                   help="dag4: flip the approval service to reject, exercising the saga")
    p.add_argument("--no-decide", action="store_true",
                   help="dag4: nobody decides, so the approval WAIT times out")
    p.add_argument("--bad-address", action="store_true",
                   help="dag4: undeliverable address -> non-retriable InvalidAddress -> saga")
    args = p.parse_args()

    if args.dag == "dag1":
        wf_id = start("dag1_csv_etl", {
            "zip_path": args.zip_path,
            "extract_dir": "/tmp/conductor_etl_extracted",
            "output_dir": args.output_dir,
        })

    elif args.dag == "dag2":
        payload = {
            "correlation_id": f"conductor-dag2-{uuid.uuid4().hex[:8]}",
            "per_page": args.per_page,
            "auto_resume": not args.no_auto_resume,
        }
        if args.empty:
            # A page past the end of the corpus: HTTP 200 with a bare []. Note
            # `per_page=0` is NOT the way to do this -- fixture-service 422s,
            # which exercises the error-payload edge case instead.
            fixture = os.environ.get("FIXTURE_INTERNAL_URL", "http://fixture-service:8099")
            payload["fetch_url"] = f"{fixture}/books?page=99999&per_page=5"
        wf_id = start("dag2_api_fanout", payload)

    elif args.dag == "dag3":
        payment_id = f"PAY-{uuid.uuid4().hex[:10].upper()}"
        wf_id = start("dag3_payment", {
            "payment_id": payment_id,
            "from_account": args.from_account,
            "to_account": args.to_account,
            "amount": args.amount,
            "currency": "USD",
            # Generated, never a literal: a fixed idempotency key would make
            # every re-run a no-op skip. (CLAUDE.md flags this for deployments.)
            "idempotency_key": f"idem-{payment_id}",
        })

    else:
        if args.reject or args.no_decide:
            print("NOTE: set the approval service's behaviour before starting:")
            print("  reject : AUTO_DECIDE_ACTION=rejected")
            print("  timeout: AUTO_DECIDE_ACTION=none")
            print("  then `just rebuild` or recreate the approval-service container.\n")
        items = (
            [{"sku": "GADGET-B", "quantity": 2}]          # 2 x 499.99 = 999.98 -> approval
            if args.order_total == "high"
            else [{"sku": "THING-C", "quantity": 3}]      # 3 x 9.99 = 29.97 -> skip approval
        )
        # An address missing the required fields, which the shipping service
        # rejects with error_type=InvalidAddress (non-retriable), as opposed to
        # omitting shipping_address entirely -- that is a FastAPI validation
        # error, a different thing that must not be reported as a business
        # rejection. See call_shipping_api's docstring.
        address = (
            {"street": "", "city": "", "state": "", "zip": ""}
            if args.bad_address
            else {"street": "1 Test Street", "city": "Springfield",
                  "state": "IL", "zip": "62701"}
        )
        wf_id = start("dag4_order_fulfillment", {
            "order_id": f"ORD-{uuid.uuid4().hex[:10].upper()}",
            "customer_id": "CUST-42",
            "items": items,
            "shipping_address": address,
        })

    print(f"started {args.dag}: {wf_id}")
    if not args.wait:
        print(f"UI: {os.environ.get('CONDUCTOR_UI_SERVER_URL', 'http://localhost:8127')}"
              f"/execution/{wf_id}")
        return 0

    return report(poll(wf_id, timeout=args.timeout))


if __name__ == "__main__":
    raise SystemExit(main())

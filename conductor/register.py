"""
Push the task definitions and workflow definitions to the Conductor server.

    source conductor/env.sh && uv run python conductor/register.py

This is Conductor's whole deployment model, and it is a category the bake-off's
other tools mostly do not occupy: definitions are *data*, PUT over HTTP into the
server's metadata store, versioned there, and edited independently of the
workers that execute them. Nothing scans a folder (Airflow), nothing is
registered by a connecting worker (Temporal, Hatchet), and the workers do not
need restarting when a definition changes -- only when a task *body* changes.

Endpoint cardinality is inconsistent between the two resources, which is worth
knowing before you debug a 500:

    POST /api/metadata/workflow   <- ONE WorkflowDef   (a list 500s)
    PUT  /api/metadata/workflow   <- LIST of WorkflowDef (upsert; used here)
    POST /api/metadata/taskdefs   <- LIST of TaskDef   (used here)
    PUT  /api/metadata/taskdefs   <- ONE TaskDef
"""

import argparse
import json
import os
import sys
from pathlib import Path

import requests

HERE = Path(__file__).parent
WORKFLOW_DIR = HERE / "workflows"
TASKDEFS_FILE = HERE / "taskdefs.json"

# env.sh exports this with the /api suffix the SDK wants; strip it here because
# these are raw REST calls and we build the paths ourselves.
SERVER = os.environ.get("CONDUCTOR_SERVER_URL", "http://localhost:8000/api")
BASE = SERVER.rstrip("/").removesuffix("/api")


def register_taskdefs() -> int:
    taskdefs = json.loads(TASKDEFS_FILE.read_text())
    resp = requests.post(
        f"{BASE}/api/metadata/taskdefs",
        json=taskdefs,
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    if resp.status_code >= 300:
        print(f"  taskdefs FAILED {resp.status_code}: {resp.text[:400]}", file=sys.stderr)
        return 0
    print(f"  registered {len(taskdefs)} task definitions")
    return len(taskdefs)


def register_workflows() -> int:
    defs = []
    for path in sorted(WORKFLOW_DIR.glob("*.json")):
        defs.append(json.loads(path.read_text()))

    # PUT takes the list and upserts, so re-running this is how you deploy a
    # change. Same version number = overwrite in place; bump `version` in the
    # JSON to keep the old one addressable by running workflows.
    resp = requests.put(
        f"{BASE}/api/metadata/workflow",
        json=defs,
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    if resp.status_code >= 300:
        print(f"  workflows FAILED {resp.status_code}: {resp.text[:400]}", file=sys.stderr)
        return 0

    for d in defs:
        print(f"  registered {d['name']} v{d['version']}")
    return len(defs)


def verify() -> bool:
    """Read the definitions back. Registration returning 200 is not proof the
    server kept what you sent -- bulk upsert reports partial success as 200."""
    ok = True
    expected = {json.loads(p.read_text())["name"] for p in WORKFLOW_DIR.glob("*.json")}
    resp = requests.get(f"{BASE}/api/metadata/workflow", timeout=30)
    resp.raise_for_status()
    present = {w["name"] for w in resp.json()}
    missing = expected - present
    if missing:
        print(f"  MISSING workflows after registration: {sorted(missing)}", file=sys.stderr)
        ok = False

    expected_tasks = {t["name"] for t in json.loads(TASKDEFS_FILE.read_text())}
    resp = requests.get(f"{BASE}/api/metadata/taskdefs", timeout=30)
    resp.raise_for_status()
    present_tasks = {t["name"] for t in resp.json()}
    missing_tasks = expected_tasks - present_tasks
    if missing_tasks:
        print(f"  MISSING task defs after registration: {sorted(missing_tasks)}", file=sys.stderr)
        ok = False

    if ok:
        print(f"  verified {len(expected)} workflows and {len(expected_tasks)} task defs on the server")
    return ok


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--only",
        choices=["taskdefs", "workflows"],
        help="register just one half (default: both)",
    )
    args = parser.parse_args()

    print(f"Conductor server: {BASE}")
    if args.only != "workflows":
        register_taskdefs()
    if args.only != "taskdefs":
        register_workflows()

    return 0 if verify() else 1


if __name__ == "__main__":
    raise SystemExit(main())

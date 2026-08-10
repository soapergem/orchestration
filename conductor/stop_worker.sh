#!/usr/bin/env bash
# Stop the Conductor worker AND every task-runner child it spawned.
#
#   ./conductor/stop_worker.sh
#
# Why this exists: `pkill -f worker.py` is not enough. The Python SDK's
# TaskHandler runs each task type in its own spawned child process, and those
# children have the cmdline
#
#     python -c from multiprocessing.spawn import spawn_main; ...
#
# which does not mention worker.py. Killing the supervisor therefore orphans all
# 26 of them; they keep polling, keep claiming tasks, and keep executing them
# with stale code, so the next worker you start silently competes with the last
# one. See conductor/README.md §Findings.
set -uo pipefail

VENV_PY="${VENV_PY:-orchestration/.venv/bin/python}"

# 1. Ask the supervisor to shut down cleanly -- its SIGTERM handler calls
#    TaskHandler.stop_processes(), which reaps the children properly.
mapfile -t SUPERVISORS < <(pgrep -f 'conductor/worker\.py' 2>/dev/null || true)
if ((${#SUPERVISORS[@]})); then
    echo "SIGTERM -> supervisor(s): ${SUPERVISORS[*]}"
    kill -TERM "${SUPERVISORS[@]}" 2>/dev/null || true
    for _ in {1..10}; do
        pgrep -f 'conductor/worker\.py' >/dev/null 2>&1 || break
        sleep 1
    done
fi

# 2. Sweep up anything that survived, scoped to this repo's venv so we cannot
#    touch unrelated Python on the machine.
mapfile -t ORPHANS < <(pgrep -f "${VENV_PY}3? -c from multiprocessing.spawn" 2>/dev/null || true)
if ((${#ORPHANS[@]})); then
    echo "reaping ${#ORPHANS[@]} orphaned task-runner process(es)"
    kill -TERM "${ORPHANS[@]}" 2>/dev/null || true
    sleep 2
    kill -KILL "${ORPHANS[@]}" 2>/dev/null || true
fi

remaining=$(pgrep -cf 'conductor/worker\.py' 2>/dev/null || echo 0)
echo "done -- ${remaining} supervisor process(es) remaining"

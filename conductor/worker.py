"""
Conductor worker process: hosts every `@worker_task` across all four DAGs.

    source conductor/env.sh && uv run python conductor/worker.py

Conductor's execution model in one paragraph: workers POLL. This process opens
no port and accepts no inbound connection; it long-polls
`GET /api/tasks/poll/{taskType}` for each task type it has registered and pushes
results back with `POST /api/tasks`. That is why the engine can live in a
container while the workers live on the host with no gateway wiring, and why a
worker behind NAT works with no tunnel -- a genuine operational advantage over
engines that must dial the worker.

`TaskHandler(scan_for_annotated_workers=True)` discovers every function
decorated with `@worker_task` that has been IMPORTED by the time it is
constructed. The imports below are therefore load-bearing, not stylistic: a DAG
module that is not imported here contributes no workers, its tasks are never
polled, and its workflows hang in SCHEDULED forever with nothing in any log.
Hatchet has the identical failure mode with `hatchet.worker(workflows=[...])`.

STOPPING THIS WORKER: use Ctrl-C, or SIGTERM to THIS pid. Do not `pkill -f
worker.py`. TaskHandler runs each task type in its own spawned child process,
and those children have the cmdline

    python -c from multiprocessing.spawn import spawn_main; ...

with no mention of worker.py, so a `pkill -f worker.py` kills only the
supervisor and orphans all 26 children. They keep polling the server forever,
keep claiming tasks, and silently execute them with whatever code and
environment they were started with -- so a "restarted" worker quietly competes
with every previous generation. (114 orphans accumulated across three restarts
while this was being written, which is how it was found.) The SIGTERM handler
below exists to make the ordinary case safe.
"""

import logging
import os
import signal
import sys

# Importing for the decorator side effect: each module registers its
# @worker_task functions into the SDK's global worker registry. Do not "clean
# up" these imports -- see the module docstring.
import dag1_csv_etl  # noqa: F401
import dag2_api_fanout  # noqa: F401
import dag3_payment  # noqa: F401
import dag4_order_fulfillment  # noqa: F401
from conductor.client.automator.task_handler import TaskHandler
from conductor.client.configuration.configuration import Configuration

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
logger = logging.getLogger("conductor.bakeoff.worker")


def main() -> int:
    server_url = os.environ.get("CONDUCTOR_SERVER_URL")
    if not server_url:
        # The SDK would otherwise default to http://localhost:8080/api, which on
        # this host is AIRFLOW. Refuse rather than silently talk to it.
        print(
            "CONDUCTOR_SERVER_URL is unset -- the SDK would default to "
            "http://localhost:8080/api, which is Airflow on this host.\n"
            "Run: source conductor/env.sh",
            file=sys.stderr,
        )
        return 1

    configuration = Configuration(server_api_url=server_url)
    logger.info("polling %s", server_url)
    logger.info("BAKEOFF_NS=%s", os.environ.get("BAKEOFF_NS", "conductor"))

    from conductor.client.automator.task_handler import get_registered_worker_names

    names = sorted(get_registered_worker_names())
    logger.info("hosting %d task types: %s", len(names), ", ".join(names))

    handler = TaskHandler(configuration=configuration, scan_for_annotated_workers=True)
    handler.start_processes()
    logger.info("worker pid %d -- stop with Ctrl-C or `kill %d`, NOT pkill", os.getpid(), os.getpid())

    def _shutdown(signum, _frame):
        # Without this, SIGTERM kills only the supervisor and leaves every
        # spawned task-runner child polling the server indefinitely. See the
        # module docstring.
        logger.info("signal %s -- stopping %d task runners", signum, len(names))
        handler.stop_processes()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        handler.join_processes()
    except KeyboardInterrupt:
        logger.info("shutting down")
        handler.stop_processes()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

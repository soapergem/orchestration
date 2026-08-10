"""
Deploy the bake-off DAGs to a `docker`-type work pool: one container per flow run.

This is the variant that actually exercises ../comparison.md's claim that Prefect
gets dependency isolation "via Docker/K8s work pools (isolation is per flow run,
not per task)". Contrast serve_all.py, where every run is a subprocess of one
long-lived runner sharing a single Python environment.

Prerequisites
-------------
1. Build the image (code is baked in, so rebuild after editing a DAG):

       podman build -t localhost/prefect-bakeoff:latest -f Dockerfile .

2. Create the pool once:

       prefect work-pool create --type docker bakeoff-docker

   (Names starting with "prefect" are rejected as reserved.)

3. Register the deployments:

       PREFECT_API_URL=http://127.0.0.1:4200/api python deploy_docker.py

4. Run a worker. It needs DOCKER_HOST pointing at your runtime's
   Docker-compatible API socket -- see CONTAINER RUNTIMES below:

       DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock \
       PREFECT_API_URL=http://127.0.0.1:4200/api \
         prefect worker start --pool bakeoff-docker

5. Trigger:

       prefect deployment run 'payment_processing/dag3-payment-docker'

CONTAINER RUNTIMES
------------------
Prefect's docker worker speaks the **Docker Engine API** through docker-py, so
what matters is whether your runtime exposes that API -- not whether it has a
`docker` CLI.

  podman  -- works. Enable the user socket once:
                 systemctl --user enable --now podman.socket
             then DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock
             Verified here: docker-py reports "Podman Engine" at API 1.41.
             Host gateway inside containers: host.containers.internal

  docker  -- works out of the box; DOCKER_HOST usually unset.
             Host gateway: host.docker.internal

  finch   -- NOT VERIFIED. The finch CLI alone does not serve the Docker Engine
             API (it is nerdctl + containerd in a Lima VM), but the separate
             `runfinch/finch-daemon` project does: it exposes a dockerd-like
             socket implementing a SUBSET of Docker Engine API v1.43, typically
             at unix:///tmp/nerdctl/nerdctl.sock. Two open questions on that
             machine:
               1. Whether the endpoints prefect-docker needs are in that subset.
               2. On macOS, finch runs in a Lima VM, so the socket lives inside
                  the VM and must be forwarded to the host for DOCKER_HOST to
                  reach it.
             Set HOST_GATEWAY=host.docker.internal there. If it does not work,
             fall back to serve_all.py (process isolation) on that machine --
             the DAG code is identical either way.

Set HOST_GATEWAY / DOCKER_IMAGE via env to retarget without editing this file.
"""

import os

from dag1_csv_etl import csv_etl_pipeline
from dag2_api_fanout import api_fanout_pipeline
from dag3_payment import payment_processing
from dag4_order_fulfillment import manager_approval_flow, order_fulfillment

# Hostname a CONTAINER uses to reach services published on the host.
#   podman -> host.containers.internal   |   docker/finch -> host.docker.internal
HOST_GATEWAY = os.environ.get("HOST_GATEWAY", "host.containers.internal")

IMAGE = os.environ.get("DOCKER_IMAGE", "localhost/prefect-bakeoff:latest")
WORK_POOL = os.environ.get("DOCKER_WORK_POOL", "bakeoff-docker")

# Host directory bind-mounted for DAG 1's ZIP input and Parquet output. The
# container paths are fixed by the Dockerfile's ETL_* vars.
LOCAL_DATA_DIR = os.environ.get(
    "LOCAL_DATA_DIR",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".local-data")),
)

# Every flow run container gets these. Note nothing points at "localhost":
# inside the container that is the container itself.
COMMON_ENV = {
    "PREFECT_API_URL": f"http://{HOST_GATEWAY}:4200/api",
    "POSTGRES_HOST": HOST_GATEWAY,
    "POSTGRES_PORT": "54321",
    "CALLBACK_FETCH_SERVICE_URL": f"http://{HOST_GATEWAY}:8090",
    "APPROVAL_SERVICE_URL": f"http://{HOST_GATEWAY}:8091",
    "SHIPPING_SERVICE_URL": f"http://{HOST_GATEWAY}:8092",
    # DAG 4: the approval service (a container) POSTs the resume to the Prefect
    # API on the host, so this is the gateway host too -- not localhost.
    "PREFECT_RESUME_API_URL": f"http://{HOST_GATEWAY}:4200/api",
    "APPROVAL_WAIT_MODE": os.environ.get("APPROVAL_WAIT_MODE", "pause"),
    "BAKEOFF_NS": os.environ.get("BAKEOFF_NS", "prefect"),
}

# image_pull_policy=Never is REQUIRED for a locally built image: the default
# tries to pull localhost/prefect-bakeoff from a registry and fails.
BASE_JOB_VARIABLES = {
    "env": COMMON_ENV,
    "image_pull_policy": "Never",
    "auto_remove": True,
}


def job_variables(**overrides) -> dict:
    jv = {**BASE_JOB_VARIABLES, **overrides}
    if "env" in overrides:
        jv["env"] = {**COMMON_ENV, **overrides["env"]}
    return jv


if __name__ == "__main__":
    common = {
        "work_pool_name": WORK_POOL,
        "image": IMAGE,
        # The image already contains the flow code, so there is nothing to build
        # or push here. Rebuild the image by hand after editing a DAG.
        "build": False,
        "push": False,
    }

    csv_etl_pipeline.deploy(
        name="dag1-csv-etl-docker",
        tags=["bakeoff", "dag1", "docker"],
        # DAG 1 is the only DAG with filesystem I/O, so it needs the bind mount.
        job_variables=job_variables(volumes=[f"{LOCAL_DATA_DIR}:/data"]),
        **common,
    )
    api_fanout_pipeline.deploy(
        name="dag2-api-fanout-docker",
        parameters={"url": "http://fixture-service:8099/books"},
        tags=["bakeoff", "dag2", "docker"],
        job_variables=job_variables(),
        **common,
    )
    payment_processing.deploy(
        name="dag3-payment-docker",
        tags=["bakeoff", "dag3", "docker"],
        job_variables=job_variables(),
        **common,
    )
    order_fulfillment.deploy(
        name="dag4-order-fulfillment-docker",
        tags=["bakeoff", "dag4", "docker"],
        # The PARENT is what calls run_deployment(), so the pointer to the
        # approval deployment belongs here -- and must name the -docker variant,
        # or a suspend-mode run would dispatch to the process-pool deployment.
        job_variables=job_variables(
            env={
                "MANAGER_APPROVAL_DEPLOYMENT": (
                    "manager_approval_flow/dag4-manager-approval-docker"
                )
            }
        ),
        **common,
    )
    # DAG 4 in suspend mode invokes this as a separate deployment run; it must
    # therefore also exist on this pool, and MANAGER_APPROVAL_DEPLOYMENT must
    # name it. Suspension needs persist_result with a filesystem the *next*
    # container can read -- see the README; a bind-mounted result path or a
    # remote result store is required, since a fresh container has no access to
    # the previous one's local storage.
    manager_approval_flow.deploy(
        name="dag4-manager-approval-docker",
        tags=["bakeoff", "dag4", "approval", "docker"],
        # Results are bind-mounted to a HOST path: suspend_flow_run() resumes in
        # a BRAND NEW container, which cannot see the previous container's local
        # storage. Without shared result storage the resumed run cannot rehydrate
        # its persisted task states. See the README for what was observed.
        job_variables=job_variables(
            env={"PREFECT_LOCAL_STORAGE_PATH": "/results"},
            volumes=[f"{LOCAL_DATA_DIR}/results:/results"],
        ),
        **common,
    )
    print("Deployed 5 deployments to work pool:", WORK_POOL)

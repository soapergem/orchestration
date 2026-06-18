# Running the Bake-Off Locally (Podman)

How to stand up each orchestrator against the shared services. Scope: the
**local** orchestrators. Argo and Flyte run on Kubernetes (separate setup); Step
Functions and Google Workflows are out of scope for now.

> **Container runtime:** this environment uses **Podman** (4.9.x) with
> `podman-compose`. `podman compose <...>` delegates to it and parses the
> compose file cleanly. Wherever Docker docs say `docker compose`, use
> `podman compose`.

---

## 1. The shared backbone (always first)

```bash
cd shared-services
podman compose up -d            # postgres + callback-fetch + approval + shipping
```

This starts:

| Service | Host port | Purpose |
|---|---|---|
| postgres | 5432 | DB for all DAGs (+ empty `hatchet`/`kestra` DBs) |
| callback-fetch-service | 8090 | DAG 2 async fetch + callback |
| approval-service | 8091 | DAG 4 human approval |
| shipping-service | 8092 | DAG 4 flaky shipping API |

Each orchestrator engine is behind a **compose profile** so you run one at a
time: `podman compose --profile <name> up -d`.

---

## 2. The Podman callback-networking rule (read once)

DAG 2 and DAG 4 use an **async callback**: the orchestrator hands a
`callback_url` to a mock service (a container), and the service later POSTs the
result *back*. So the host in `callback_url` must be resolvable **from inside
the mock-service container**:

- **Callback target is itself a container** (Hatchet engine, Kestra server) →
  use the **compose service name** (`hatchet-engine`, `kestra`). Same network,
  just works.
- **Callback target runs on the HOST** (the Temporal signal server) → use
  **`host.containers.internal`** (Podman's host gateway), *not* `localhost`.
  `localhost` inside a container is the container itself, so the callback
  silently times out.

There are **no hard-coded `host.docker.internal`** values anywhere — every host
is an env var or (for Kestra) server config, so this is pure configuration.

The **polling** orchestrators — **Airflow, Dagster, Prefect, Luigi** — don't use
callbacks at all (they poll `GET /status` on the services), so none of this
applies to them. Host→published-port over `localhost` is fine.

---

## 3. Python-native orchestrators (no engine container)

Install the project deps once: `uv sync`. Then run each from its own directory.

> **Python version caveat:** `pyproject.toml` pins `requires-python >=3.14`.
> If `temporalio`/`hatchet-sdk`/`luigi` don't yet ship 3.14 wheels, create a
> throwaway 3.12 venv for those workers: `uv venv --python 3.12 .venv-workers`
> and `uv pip install temporalio hatchet-sdk luigi httpx psycopg[binary] pyarrow`.

### Airflow
```bash
cd airflow
AIRFLOW__CORE__DAGS_FOLDER=$PWD uv run airflow standalone   # UI on :8080
```
Polls the fetch/approval services — no callback wiring needed.

### Dagster
```bash
cd dagster
uv run dagster dev                                          # UI on :3000
```
DAG 2/4 waits are handled by the sensors in `sensors.py` polling `/status`.

### Prefect
```bash
cd prefect
uv run prefect server start                                 # UI on :4200, separate shell
uv run python dag1_csv_etl.py                               # run a flow
```
`callback_url` is a no-op placeholder; the flows poll.

### Luigi
```bash
cd luigi
uv run python dag1_csv_etl.py
```
No callback support by design (DAG 2 polls synchronously).

All four reach Postgres at `localhost:5432` and the mock services at
`localhost:8090–8092` (published ports). No overrides required.

---

## 4. Temporal (server in compose, worker on host)

```bash
cd shared-services
podman compose --profile temporal up -d     # temporal:7233, UI on :8233
```

Run the **worker** and **signal-relay server** on the host. The signal server
is the callback target, and it runs on the host — so the mock-service
containers must reach it via `host.containers.internal`:

```bash
cd temporal

# Worker
TEMPORAL_ADDRESS=localhost:7233 \
POSTGRES_HOST=localhost \
CALLBACK_FETCH_SERVICE_URL=http://localhost:8090 \
APPROVAL_SERVICE_URL=http://localhost:8091 \
SHIPPING_SERVICE_URL=http://localhost:8092 \
SIGNAL_SERVER_URL=http://host.containers.internal:8095 \
  uv run python worker.py

# Signal relay server (separate shell)
TEMPORAL_ADDRESS=localhost:7233 \
  uv run uvicorn signal_server:app --host 0.0.0.0 --port 8095
```

Why `SIGNAL_SERVER_URL=http://host.containers.internal:8095`: the worker bakes
this host into the `callback_url` it gives the fetch/approval **containers**,
which then POST back to your host's signal server. The worker itself reaches
everything else over `localhost` (published ports), hence the other overrides
(the code defaults to compose DNS names, which don't resolve on the host).

---

## 5. Hatchet (engine in compose, worker on host)

```bash
cd shared-services
podman compose --profile hatchet up -d      # engine API on :8888, gRPC on :7077
```

Hatchet needs a **client token** that can only be minted after the engine is
up, so it can't be baked into compose. Generate one, then run the worker:

```bash
# 1. Mint a token (exact subcommand may vary by hatchet-lite version — check
#    `podman compose exec hatchet-engine /hatchet-admin --help`):
podman compose exec hatchet-engine \
  /hatchet-admin token create --config /config --tenant-id <tenant> > hatchet.token

# 2. Run the worker on the host:
cd ../hatchet
HATCHET_CLIENT_TOKEN="$(cat ../shared-services/hatchet.token)" \
HATCHET_CLIENT_TLS_STRATEGY=none \
HATCHET_CLIENT_HOST_PORT=localhost:7077 \
POSTGRES_HOST=localhost \
CALLBACK_FETCH_SERVICE_URL=http://localhost:8090 \
APPROVAL_SERVICE_URL=http://localhost:8091 \
SHIPPING_SERVICE_URL=http://localhost:8092 \
HATCHET_EVENT_API_URL=http://hatchet-engine:8888/api/v1/events \
  uv run python worker.py
```

`HATCHET_EVENT_API_URL` is the callback target. It's an env-ingestion endpoint
on the **engine container**, so it must be the in-network service name
`hatchet-engine` (the code default `localhost:8080` is wrong on two counts:
hatchet-lite serves the API on **8888**, and `localhost` won't resolve from the
mock-service container). **Verify the API port/path against your hatchet-lite
version** — this is the most likely thing to need adjustment.

---

## 6. Kestra (everything in the container)

Kestra runs the flow YAMLs itself — no separate worker.

```bash
cd shared-services
podman compose --profile kestra up -d       # UI on :8080
```

Load the flows (mounted read-only at `/flows`) — via the UI importer, or:

```bash
# DAG flows
podman compose exec kestra kestra flow namespace update orchestration.api /flows
# Subflows (manager approval, shipping, inventory)
podman compose exec kestra kestra flow namespace update orchestration.api /flows/subflows
```

The callback wiring is already handled by `kestra.url: http://kestra:8080/` in
the compose config: Kestra builds `execution.resumeUrl` from that base, so the
URL it hands the callback/approval containers points at `kestra:8080` on the
shared network — reachable. If you change `kestra.url` to `localhost`, DAG 2
and DAG 4 will time out.

---

## Quick reference: callback target per orchestrator

| Orchestrator | Wait mechanism | Callback target | Host to use |
|---|---|---|---|
| Airflow / Dagster / Prefect / Luigi | polling | n/a (orchestrator polls) | — |
| Temporal | signal relay (host process) | signal server | `host.containers.internal:8095` |
| Hatchet | event ingestion | engine container | `hatchet-engine:8888` |
| Kestra | pause/resume webhook | server container | `kestra:8080` (via `kestra.url`) |

---

## Status / caveats

- Compose **network + env wiring** is the verified-by-analysis part. Engine
  **image tags, hatchet-lite port layout, the token subcommand, and Kestra flow
  loading** are best-effort from docs and should be shaken out on a first real
  run — they're flagged inline above and in `docker-compose.yml`.
- `init-engines.sql` (the empty `hatchet`/`kestra` DBs) only runs on a **fresh**
  `pgdata` volume. On an existing volume, create them manually (see that file).

---

## 7. Argo Workflows (Kubernetes)

Installed on EKS (Fargate) via the upstream manifest, not Helm.

### Installation

```bash
# Create namespace
kubectl create namespace argo

# Install Argo Workflows (server-side apply required for large CRDs)
kubectl apply -n argo --server-side \
  -f https://github.com/argoproj/argo-workflows/releases/latest/download/install.yaml

# Grant the default service account permission to report task results
kubectl apply -n argo -f - <<'EOF'
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: executor
  namespace: argo
rules:
- apiGroups:
  - argoproj.io
  resources:
  - workflowtaskresults
  verbs:
  - create
  - patch
  - get
  - list
  - watch
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: executor-default
  namespace: argo
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: executor
subjects:
- kind: ServiceAccount
  name: default
  namespace: argo
EOF

# Disable HTTPS and fix readiness probe for port-forward access
kubectl patch deployment argo-server -n argo --type json -p='[
  {"op":"replace","path":"/spec/template/spec/containers/0/args","value":["server","--auth-mode=server","--secure=false"]},
  {"op":"replace","path":"/spec/template/spec/containers/0/readinessProbe/httpGet/scheme","value":"HTTP"}
]'
```

### Accessing the UI

```bash
kubectl port-forward -n argo svc/argo-server 2746:2746
# Visit http://localhost:2746
```

### Submitting workflows

```bash
kubectl create -n argo -f argo/dag1-csv-etl.yaml
kubectl create -n argo -f argo/dag2-api-fanout.yaml
kubectl create -n argo -f argo/dag3-payment.yaml
```

Submitted workflows appear in the **Workflows** tab (not Workflow Templates).

### Notes

- The `--server-side` flag is required because some CRDs exceed the 262KB annotation limit for client-side apply.
- The default executor is emissary (the only option in Argo v4+), which is compatible with Fargate.
- HTTPS was disabled for local port-forward convenience. Do not expose this without TLS in a real environment.

---

## 8. Flyte (Kubernetes)

Installed on EKS (Fargate) via Helm using the `flyte-core` chart with a standalone Postgres deployment.

### Installation

The `flyte-binary` chart bundles Postgres as a sidecar container, which does not work on Fargate due to init container ordering issues. Use `flyte-core` with a standalone Postgres instead.

```bash
# Add Helm repo
helm repo add flyteorg https://flyteorg.github.io/flyte
helm repo update

# Create namespace and deploy Postgres
kubectl create namespace flyte

kubectl apply -n flyte -f - <<'EOF'
apiVersion: v1
kind: Service
metadata:
  name: postgres
  namespace: flyte
spec:
  ports:
  - port: 5432
    targetPort: 5432
  selector:
    app: postgres
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres
  namespace: flyte
spec:
  replicas: 1
  selector:
    matchLabels:
      app: postgres
  template:
    metadata:
      labels:
        app: postgres
    spec:
      containers:
      - name: postgres
        image: postgres:15-alpine
        env:
        - name: POSTGRES_USER
          value: postgres
        - name: POSTGRES_PASSWORD
          value: postgres
        - name: POSTGRES_DB
          value: flyteadmin
        ports:
        - containerPort: 5432
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
EOF

kubectl rollout status deployment/postgres -n flyte --timeout=120s

# Install Flyte
helm install flyte flyteorg/flyte-core -n flyte \
  --set postgres.enabled=false \
  --set common.ingress.enabled=false \
  --set db.admin.database.host=postgres \
  --set db.admin.database.port=5432 \
  --set db.admin.database.dbname=flyteadmin \
  --set db.admin.database.username=postgres \
  --set db.admin.database.passwordPath="" \
  --set db.datacatalog.database.host=postgres \
  --set db.datacatalog.database.port=5432 \
  --set db.datacatalog.database.dbname=flyteadmin \
  --set db.datacatalog.database.username=postgres \
  --set db.datacatalog.database.passwordPath="" \
  --timeout 10m \
  --wait
```

### Accessing the UI

```bash
kubectl port-forward -n flyte svc/flyteconsole 8080:80
# Visit http://localhost:8080
```

### Notes

- Postgres is running on Fargate with ephemeral storage (no PV). Data is lost if the pod restarts. This is acceptable for bake-off evaluation.
- Estimated Fargate cost for the Postgres pod: ~$9/month.
- For production use, replace with RDS or a Postgres deployment with persistent volumes.
- Credentials are hardcoded (`postgres`/`postgres`). Do not use in a real environment.

---

## Teardown

### Local (Podman)

```bash
cd shared-services

# Stop a specific profile
podman compose --profile temporal down
podman compose --profile hatchet down
podman compose --profile kestra down

# Stop shared services and remove volumes
podman compose down -v
```

### Kubernetes

```bash
# Argo Workflows
kubectl delete -n argo -f https://github.com/argoproj/argo-workflows/releases/latest/download/install.yaml
kubectl delete namespace argo

# Flyte
helm uninstall flyte -n flyte
kubectl delete deployment postgres -n flyte
kubectl delete service postgres -n flyte
kubectl delete namespace flyte
```

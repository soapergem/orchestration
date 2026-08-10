#!/usr/bin/env bash
#
# Deploy the bake-off backbone (Postgres + the three mock services) into a
# Kubernetes cluster, for the K8s-only orchestrators (Argo, Flyte).
#
# This is the in-cluster equivalent of `just up`. It exists because Argo and
# Flyte DAG steps resolve the *compose* DNS names -- `postgres`,
# `callback-fetch-service`, `approval-service`, `shipping-service` -- so those
# names have to resolve inside the cluster too. See RUNNING.md 7c.
#
# Registry-free by design: the mock services are pure-Python FastAPI apps, so
# they run on the public python image with app.py mounted from a ConfigMap and
# dependencies installed by an init container. That means no image build, no
# registry credentials, and no cross-architecture build step -- the same
# manifests work on amd64 and arm64. (The alternative, shared-services/deploy's
# Helm chart, needs per-arch images in a registry the cluster can pull from, plus
# a pull secret that stays refreshed.)
#
# Usage:
#   KCTX=my-arm64-cluster ./deploy-backbone.sh
#   KCTX=my-amd64-cluster STORAGE_CLASS="" ./deploy-backbone.sh      # no PV (Fargate)
#
# Env:
#   KCTX            kube context (required -- never defaults, two clusters are in play)
#   ORCH_NS         namespace                     (default: orchestrators)
#   STORAGE_CLASS   PVC storage class; "" = emptyDir, data lost on restart
#   PG_STORAGE      PVC size                     (default: 50Gi)
#   IMAGE_PREFIX    registry prefix for public images (default: docker.io/library)
#   SEED_NS         space-separated bakeoff namespaces to seed (default: "argo flyte")
#   PUBLIC_DOMAIN   if set, also create ingress + TLS at orch-<svc>.<domain>
#                   (needed by the OFF-cluster orchestrators: Step Functions and
#                   Google Workflows). Unset = in-cluster only.
#   CLUSTER_ISSUER  cert-manager ClusterIssuer   (default: letsencrypt-prod)
#   INGRESS_CLASS   ingress class                (default: traefik)

set -euo pipefail

: "${KCTX:?set KCTX to the kube context (e.g. KCTX=my-arm64-cluster)}"
ORCH_NS="${ORCH_NS:-orchestrators}"
STORAGE_CLASS="${STORAGE_CLASS-oci-bv}"
PG_STORAGE="${PG_STORAGE:-50Gi}"
# cri-o (Oracle Linux, K3s with certain configs) runs with short-name mode
# "enforcing", which rejects `postgres:16` as ambiguous. Always fully qualify.
IMAGE_PREFIX="${IMAGE_PREFIX:-docker.io/library}"
SEED_NS="${SEED_NS:-argo flyte}"
CLUSTER_ISSUER="${CLUSTER_ISSUER:-letsencrypt-prod}"
INGRESS_CLASS="${INGRESS_CLASS:-traefik}"

SVC_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
K="kubectl --context $KCTX -n $ORCH_NS"

echo "==> context=$KCTX namespace=$ORCH_NS storage_class=${STORAGE_CLASS:-<emptyDir>}"

kubectl --context "$KCTX" create namespace "$ORCH_NS" \
  --dry-run=client -o yaml | kubectl --context "$KCTX" apply -f -

# ---- Postgres -------------------------------------------------------------
# init-db.sql defines bootstrap_bakeoff(ns) and runs only on a FRESH volume,
# exactly like the compose postgres. On an existing volume, the seeding step at
# the bottom reloads the function first.
echo "==> postgres"
$K create configmap bakeoff-init-db \
  --from-file=00-init-db.sql="$SVC_ROOT/init-db.sql" \
  --dry-run=client -o yaml | kubectl --context "$KCTX" apply -f -

if [[ -n "$STORAGE_CLASS" ]]; then
  $K apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: bakeoff-pgdata
spec:
  accessModes: ["ReadWriteOnce"]
  storageClassName: $STORAGE_CLASS
  resources:
    requests:
      storage: $PG_STORAGE
EOF
  PG_VOLUME='persistentVolumeClaim: {claimName: bakeoff-pgdata}'
else
  echo "    (no storage class -- using emptyDir, data is lost on pod restart)"
  PG_VOLUME='emptyDir: {}'
fi

$K apply -f - <<EOF
apiVersion: v1
kind: Service
metadata:
  name: postgres
spec:
  ports:
  - port: 5432
    targetPort: 5432
  selector:
    app: bakeoff-postgres
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: bakeoff-postgres
spec:
  replicas: 1
  selector:
    matchLabels:
      app: bakeoff-postgres
  template:
    metadata:
      labels:
        app: bakeoff-postgres
    spec:
      containers:
      - name: postgres
        image: $IMAGE_PREFIX/postgres:16
        env:
        - {name: POSTGRES_DB, value: orchestration}
        - {name: POSTGRES_USER, value: orchestration}
        - {name: POSTGRES_PASSWORD, value: orchestration}
        # Subdirectory, so a PV with a lost+found does not break initdb.
        - {name: PGDATA, value: /var/lib/postgresql/data/pgdata}
        ports:
        - containerPort: 5432
        volumeMounts:
        - {name: pgdata, mountPath: /var/lib/postgresql/data}
        - {name: init, mountPath: /docker-entrypoint-initdb.d}
        readinessProbe:
          exec:
            command: ["pg_isready", "-U", "orchestration", "-d", "orchestration"]
          initialDelaySeconds: 10
          periodSeconds: 5
        resources:
          requests: {cpu: 200m, memory: 256Mi}
      volumes:
      - name: pgdata
        $PG_VOLUME
      - name: init
        configMap: {name: bakeoff-init-db}
EOF

# ---- Mock services ------------------------------------------------------
# Service names must match the compose names: the Argo DAG YAML hard-codes them
# as literal env values, so a rename means editing every manifest.
deploy_mock() {
  local name="$1" port="$2" deps="$3" src="$4"; shift 4
  echo "==> $name (:$port)"
  $K create configmap "${name}-src" --from-file=app.py="$src" \
    --dry-run=client -o yaml | kubectl --context "$KCTX" apply -f -

  local env_yaml=""
  for kv in "$@"; do
    env_yaml+="        - {name: ${kv%%=*}, value: \"${kv#*=}\"}"$'\n'
  done

  # Google Workflows resume (DAG 2 / DAG 4): a callback endpoint on
  # workflowexecutions.googleapis.com only accepts an authenticated POST, so the
  # two broker services need GCP credentials. Mounted key rather than workload
  # identity, because neither of these clusters federates into the GCP project --
  # the same trade-off as the AWS key the stepfunctions provider uses.
  # Only the two resume-capable services get it; shipping and fixture never resume.
  local gcp_env="" gcp_mount="" gcp_volume=""
  if [[ -n "${GOOGLE_RESUME_SECRET:-}" ]] &&
     [[ "$name" == "callback-fetch-service" || "$name" == "approval-service" ]]; then
    gcp_env="        - {name: GOOGLE_APPLICATION_CREDENTIALS, value: /var/secrets/google/key.json}"$'\n'
    gcp_mount="        - {name: gcp-creds, mountPath: /var/secrets/google, readOnly: true}"$'\n'
    gcp_volume="      - name: gcp-creds"$'\n'"        secret: {secretName: $GOOGLE_RESUME_SECRET}"$'\n'
  fi

  $K apply -f - <<EOF
apiVersion: v1
kind: Service
metadata:
  name: $name
spec:
  ports:
  - port: $port
    targetPort: $port
  selector:
    app: $name
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: $name
spec:
  replicas: 1
  selector:
    matchLabels:
      app: $name
  template:
    metadata:
      labels:
        app: $name
      annotations:
        # Roll the pods when app.py changes -- a ConfigMap update alone does not
        # restart anything, and these are mounted read-only at /app.
        bakeoff/src-checksum: "$(sha256sum "$src" | cut -c1-16)"
    spec:
      initContainers:
      # Dependencies are installed here rather than baked into an image, which
      # is what keeps this registry-free. --target + PYTHONPATH instead of a
      # venv so the runtime container stays a stock python image.
      - name: deps
        image: $IMAGE_PREFIX/python:3.12-slim
        command: ["pip", "install", "--no-cache-dir", "--target=/deps", "--quiet"]
        args: [$deps]
        volumeMounts:
        - {name: deps, mountPath: /deps}
        resources:
          requests: {cpu: 100m, memory: 128Mi}
      containers:
      - name: app
        image: $IMAGE_PREFIX/python:3.12-slim
        workingDir: /app
        command: ["python", "-m", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "$port"]
        env:
        - {name: PYTHONPATH, value: /deps}
$env_yaml$gcp_env        ports:
        - containerPort: $port
        volumeMounts:
        - {name: src, mountPath: /app, readOnly: true}
        - {name: deps, mountPath: /deps, readOnly: true}
$gcp_mount
        # A TCP probe, not an HTTP one: none of the three services exposes a
        # /health route, and shipping-service has no GET endpoint at all
        # (only POST /shipments). Probing a path would 404 forever.
        readinessProbe:
          tcpSocket: {port: $port}
          initialDelaySeconds: 3
          periodSeconds: 5
          failureThreshold: 6
        resources:
          requests: {cpu: 50m, memory: 128Mi}
      volumes:
      - name: deps
        emptyDir: {}
      - name: src
        configMap: {name: ${name}-src}
$gcp_volume
EOF
}

# Google Workflows resume credentials, if supplied. Get the key from
# terraform/gcp:
#   terraform -chdir=../../terraform/gcp output -raw resume_service_account_key \
#     | base64 -d > /tmp/gcp-resume-key.json
#   KCTX=my-arm64-cluster GOOGLE_SA_KEY_FILE=/tmp/gcp-resume-key.json ./deploy-backbone.sh
# Without it the services still start; a google_workflows resume then returns a
# 500 that says credentials are missing, rather than hanging the workflow.
if [[ -n "${GOOGLE_SA_KEY_FILE:-}" ]]; then
  GOOGLE_RESUME_SECRET="${GOOGLE_RESUME_SECRET:-google-resume-creds}"
  echo "==> google resume credentials -> secret/$GOOGLE_RESUME_SECRET"
  $K create secret generic "$GOOGLE_RESUME_SECRET" \
    --from-file=key.json="$GOOGLE_SA_KEY_FILE" \
    --dry-run=client -o yaml | kubectl --context "$KCTX" apply -f -
fi

# Behaviour env mirrors shared-services/docker-compose.yml so DAG 2 and DAG 4
# run hands-off (see RUNNING.md 2b). Turn AUTO_* off to exercise the
# timeout / duplicate / late-callback edge cases.
deploy_mock callback-fetch-service 8090 \
  '"fastapi","uvicorn","httpx","boto3","pydantic-settings","google-auth","requests"' \
  "$SVC_ROOT/callback-fetch-service/app.py" \
  FETCH_DELAY_MIN_SECONDS=2 FETCH_DELAY_MAX_SECONDS=10 \
  FETCH_TIMEOUT_SECONDS=30 AUTO_RESUME=true AUTO_RESUME_DELAY_SECONDS=0

deploy_mock approval-service 8091 \
  '"fastapi","uvicorn","httpx","boto3","pydantic-settings","google-auth","requests"' \
  "$SVC_ROOT/approval-service/app.py" \
  AUTO_DECIDE_DELAY_SECONDS=10 AUTO_DECIDE_ACTION=approved

deploy_mock shipping-service 8092 \
  '"fastapi","uvicorn","pydantic-settings"' \
  "$SVC_ROOT/shipping-service/app.py" \
  SHIPPING_SUCCESS_RATE=0.70 SHIPPING_TIMEOUT_RATE=0.15 SHIPPING_SERVER_ERROR_RATE=0.10

# fixture-service ships only app.py in its ConfigMap. Both data files -- the Open
# Library corpus and DAG 1's archive -- are uncommitted build artefacts fetched
# from S3 on boot, which also sidesteps the 1 MiB ConfigMap limit the corpus would
# otherwise blow through.
#
# Export these to provision it (from `terraform output`):
#   FIXTURE_BOOKS_URL, FIXTURE_SAMPLE_ZIP_URL, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
# Without FIXTURE_BOOKS_URL the pod starts but reports 503 on /health and /books,
# so a missing corpus is loud rather than silent.
: "${FIXTURE_BOOKS_URL:=}"
: "${FIXTURE_SAMPLE_ZIP_URL:=}"
: "${AWS_ACCESS_KEY_ID:=}"
: "${AWS_SECRET_ACCESS_KEY:=}"
deploy_fixture() {
  local name=fixture-service port=8099
  local src="$SVC_ROOT/fixture-service/app.py"
  echo "==> $name (:$port)"

  if [ -z "$FIXTURE_BOOKS_URL" ]; then
    echo "    NOTE: FIXTURE_BOOKS_URL unset -- DAG 2 will 503 until a corpus is provided." >&2
  fi

  $K create configmap "${name}-src" --from-file=app.py="$src" \
    --dry-run=client -o yaml | kubectl --context "$KCTX" apply -f -

  local data_env=""
  [ -n "$FIXTURE_BOOKS_URL" ] && \
    data_env+="        - {name: FIXTURE_BOOKS_URL, value: \"$FIXTURE_BOOKS_URL\"}"$'\n'
  [ -n "$FIXTURE_SAMPLE_ZIP_URL" ] && \
    data_env+="        - {name: FIXTURE_SAMPLE_ZIP_URL, value: \"$FIXTURE_SAMPLE_ZIP_URL\"}"$'\n'
  # Item detail URLs are derived from FIXTURE_BASE_URL, else the request host.
  # Keep the default in-cluster: Argo and Flyte pods are the in-cluster consumers
  # and must get `http://fixture-service:8099/...` back. Off-cluster callers
  # (Google Workflows, Step Functions) pass `?base=https://orch-fixture.<domain>`
  # per request instead -- the same idiom airflow/dag2 uses locally with
  # `?base=http://localhost:8099`. Deliberately NOT derived from PUBLIC_DOMAIN:
  # that would hairpin every in-cluster fan-out out through the public ingress.
  [ -n "${FIXTURE_BASE_URL:-}" ] && \
    data_env+="        - {name: FIXTURE_BASE_URL, value: \"$FIXTURE_BASE_URL\"}"$'\n'

  if [ -n "$FIXTURE_BOOKS_URL$FIXTURE_SAMPLE_ZIP_URL" ]; then
    data_env+="        - {name: AWS_REGION, value: \"${AWS_REGION:-us-east-1}\"}"$'\n'
    if [ -n "$AWS_ACCESS_KEY_ID" ]; then
      $K create secret generic fixture-s3-creds \
        --from-literal=AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
        --from-literal=AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
        --dry-run=client -o yaml | kubectl --context "$KCTX" apply -f -
      data_env+="        - {name: AWS_ACCESS_KEY_ID, valueFrom: {secretKeyRef: {name: fixture-s3-creds, key: AWS_ACCESS_KEY_ID}}}"$'\n'
      data_env+="        - {name: AWS_SECRET_ACCESS_KEY, valueFrom: {secretKeyRef: {name: fixture-s3-creds, key: AWS_SECRET_ACCESS_KEY}}}"$'\n'
    fi
  fi

  $K apply -f - <<EOF
apiVersion: v1
kind: Service
metadata:
  name: $name
spec:
  ports:
  - port: $port
    targetPort: $port
  selector:
    app: $name
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: $name
spec:
  replicas: 1
  selector:
    matchLabels:
      app: $name
  template:
    metadata:
      labels:
        app: $name
      annotations:
        bakeoff/src-checksum: "$(sha256sum "$src" | cut -c1-16)"
    spec:
      initContainers:
      - name: deps
        image: $IMAGE_PREFIX/python:3.12-slim
        command: ["pip", "install", "--no-cache-dir", "--target=/deps", "--quiet"]
        args: ["fastapi","uvicorn","pydantic-settings","boto3"]
        volumeMounts:
        - {name: deps, mountPath: /deps}
        resources:
          requests: {cpu: 100m, memory: 128Mi}
      containers:
      - name: app
        image: $IMAGE_PREFIX/python:3.12-slim
        workingDir: /app
        command: ["python", "-m", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "$port"]
        env:
        - {name: PYTHONPATH, value: /deps}
        - {name: FIXTURE_DEFAULT_PER_PAGE, value: "5"}
$data_env        ports:
        - containerPort: $port
        volumeMounts:
        - {name: src, mountPath: /app, readOnly: true}
        - {name: deps, mountPath: /deps, readOnly: true}
        # Unlike the other three mocks this one HAS a /health route -- and it
        # returns 503 until a corpus is loaded, so the probe gates on real
        # readiness rather than just a listening socket.
        readinessProbe:
          httpGet: {path: /health, port: $port}
          initialDelaySeconds: 5
          periodSeconds: 5
          failureThreshold: 12
        resources:
          requests: {cpu: 50m, memory: 128Mi}
      volumes:
      - name: deps
        emptyDir: {}
      - name: src
        configMap: {name: ${name}-src}
EOF
}

deploy_fixture

# ---- Optional: public ingress + TLS ---------------------------------------
# In-cluster DNS is enough for Argo and Flyte, whose pods run beside these
# services. It is NOT enough for the two off-cluster orchestrators: Step
# Functions lambdas and Google Workflows executions both call the mock services
# from outside, and Google Workflows additionally needs a *publicly* fetchable
# items API for DAG 2 (the callback-fetch service is the one that fetches it).
#
# Enable with PUBLIC_DOMAIN; hostnames are orch-<svc>.<PUBLIC_DOMAIN>, matching
# shared-services/deploy's Helm chart so the URLs do not change when the services
# move between clusters.
#
#   KCTX=my-arm64-cluster PUBLIC_DOMAIN=example.com ./deploy-backbone.sh
#
# Certificates are issued by cert-manager. On a DNS-01 issuer the cert is minted
# without traffic reaching this cluster, so this can be applied BEFORE the A
# records are moved; on an HTTP-01 issuer the order stays pending until they are.
deploy_ingress() {
  local domain="$1"
  echo "==> ingress for *.${domain} (issuer=$CLUSTER_ISSUER, class=$INGRESS_CLASS)"

  # Traefik-specific: redirect :80 to :443. Skipped for other controllers, which
  # is why the annotation below is applied conditionally.
  if [[ "$INGRESS_CLASS" == "traefik" ]]; then
    $K apply -f - <<EOF
apiVersion: traefik.io/v1alpha1
kind: Middleware
metadata:
  name: mock-force-https
spec:
  redirectScheme:
    scheme: https
    permanent: true
EOF
  fi

  local ann=""
  if [[ "$INGRESS_CLASS" == "traefik" ]]; then
    ann="    traefik.ingress.kubernetes.io/router.middlewares: ${ORCH_NS}-mock-force-https@kubernetescrd"
  fi

  # One host per service, one TLS secret per host (rather than a single SAN cert)
  # so a failure to issue one certificate cannot take the others down with it.
  $K apply -f - <<EOF
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: mock-services
  annotations:
    cert-manager.io/cluster-issuer: ${CLUSTER_ISSUER}
${ann}
spec:
  ingressClassName: ${INGRESS_CLASS}
  rules:
  - host: orch-callback-fetch.${domain}
    http:
      paths:
      - {path: /, pathType: Prefix, backend: {service: {name: callback-fetch-service, port: {number: 8090}}}}
  - host: orch-approval.${domain}
    http:
      paths:
      - {path: /, pathType: Prefix, backend: {service: {name: approval-service, port: {number: 8091}}}}
  - host: orch-shipping.${domain}
    http:
      paths:
      - {path: /, pathType: Prefix, backend: {service: {name: shipping-service, port: {number: 8092}}}}
  - host: orch-fixture.${domain}
    http:
      paths:
      - {path: /, pathType: Prefix, backend: {service: {name: fixture-service, port: {number: 8099}}}}
  tls:
  - hosts: [orch-callback-fetch.${domain}]
    secretName: callback-fetch-tls
  - hosts: [orch-approval.${domain}]
    secretName: approval-tls
  - hosts: [orch-shipping.${domain}]
    secretName: shipping-tls
  - hosts: [orch-fixture.${domain}]
    secretName: fixture-tls
EOF
}

if [[ -n "${PUBLIC_DOMAIN:-}" ]]; then
  deploy_ingress "$PUBLIC_DOMAIN"
fi

# ---- Wait + seed ---------------------------------------------------------
echo "==> waiting for rollouts"
for d in bakeoff-postgres callback-fetch-service approval-service shipping-service fixture-service; do
  $K rollout status "deploy/$d" --timeout=300s
done

echo "==> seeding bake-off schemas: $SEED_NS"
# Reload the function first: init-db.sql only auto-runs on a fresh volume.
$K exec deploy/bakeoff-postgres -- \
  psql -q -U orchestration -d orchestration -f /docker-entrypoint-initdb.d/00-init-db.sql
for ns in $SEED_NS; do
  $K exec deploy/bakeoff-postgres -- \
    psql -q -U orchestration -d orchestration -c "SELECT bootstrap_bakeoff('$ns');"
  echo "    seeded ${ns}_dag1 / _dag3 / _dag4"
done

echo
echo "==> backbone ready in namespace $ORCH_NS on context $KCTX"
$K get deploy,svc
cat <<'NOTE'

Argo and Flyte resolve bare names like `postgres`, so unless the workflow pods
run in THIS namespace, alias the services into the workflow namespace:

  ORCH_NS=orchestrators WORKFLOW_NS=argo ./alias-backbone.sh

NOTE

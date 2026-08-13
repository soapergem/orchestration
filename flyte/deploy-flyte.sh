#!/usr/bin/env bash
#
# Install Flyte (flyte-core) on a Kubernetes cluster.
#
# Three pieces the upstream chart does NOT give you, and which the original
# RUNNING.md instructions omitted:
#
#  1. A metadata Postgres. `flyte-binary` bundles one as a sidecar, whose
#     init-container ordering broke here, so flyte-core + standalone Postgres it is.
#  2. A blob store. The chart's default `storage.type: sandbox` writes a config
#     pointing at http://minio.<ns>.svc.cluster.local:9000 -- but the chart has no
#     `minio` key at all and deploys nothing. `--set minio.enabled=true` is
#     silently ignored. Flyte stores every task input/output there, so without it
#     the install comes up "Running" and cannot execute a single workflow. That is
#     exactly the state an earlier install was left in: flyteadmin healthy, storage
#     endpoint dangling, `kubectl get flyteworkflows -A` empty.
#  3. The bucket itself. minio starts empty; flyte does not create the container.
#
# Usage:
#   KCTX=my-arm64-cluster ./deploy-flyte.sh
#
# Env:
#   KCTX            kube context (required)
#   FLYTE_NS        namespace                        (default: flyte)
#   STORAGE_CLASS   PVC storage class; "" = emptyDir (default: oci-bv)
#   IMAGE_PREFIX    registry prefix for docker.io images -- cri-o rejects short
#                   names, see RUNNING.md 7c        (default: docker.io/library)
#   CHART_VERSION   pin the chart, e.g. v1.16.8     (default: latest)
#   FLYTE_PROJECT   the one seeded project           (default: bakeoff)

set -euo pipefail

: "${KCTX:?set KCTX to the kube context (e.g. KCTX=my-arm64-cluster)}"
FLYTE_NS="${FLYTE_NS:-flyte}"
STORAGE_CLASS="${STORAGE_CLASS-oci-bv}"
IMAGE_PREFIX="${IMAGE_PREFIX:-docker.io/library}"
CHART_VERSION="${CHART_VERSION:-}"
FLYTE_PROJECT="${FLYTE_PROJECT:-bakeoff}"
# Prebuilt task image; must match what register.sh/run.sh use.
: "${ECR:?set ECR to your registry, e.g. <account-id>.dkr.ecr.<region>.amazonaws.com (see .envrc.example)}"
TASK_IMAGE="${TASK_IMAGE:-$ECR/orch-bakeoff-flyte:latest}"

K="kubectl --context $KCTX -n $FLYTE_NS"

# Must match the chart's sandbox storage defaults exactly -- flyteadmin and
# flytepropeller read these from flyte-admin-base-config/storage.yaml.
MINIO_USER=minio
MINIO_PASS=miniostorage
MINIO_BUCKET=my-s3-bucket

echo "==> context=$KCTX namespace=$FLYTE_NS storage_class=${STORAGE_CLASS:-<emptyDir>}"

kubectl --context "$KCTX" create namespace "$FLYTE_NS" \
  --dry-run=client -o yaml | kubectl --context "$KCTX" apply -f -

pvc() {  # name size
  if [[ -z "$STORAGE_CLASS" ]]; then return 0; fi
  $K apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: $1
spec:
  accessModes: ["ReadWriteOnce"]
  storageClassName: $STORAGE_CLASS
  resources:
    requests:
      storage: $2
EOF
}
vol() {  # claim-name -> volume stanza
  if [[ -n "$STORAGE_CLASS" ]]; then echo "persistentVolumeClaim: {claimName: $1}";
  else echo "emptyDir: {}"; fi
}

# ---- 1. Metadata Postgres --------------------------------------------------
# Separate from the bake-off DB in shared-services -- unrelated data, and the
# chart's defaults want two databases (flyteadmin + datacatalog).
echo "==> metadata postgres"
pvc flyte-pgdata 10Gi

$K create configmap flyte-pg-init \
  --from-literal=00-datacatalog.sql='CREATE DATABASE datacatalog;' \
  --dry-run=client -o yaml | kubectl --context "$KCTX" apply -f -

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
    app: postgres
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres
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
        image: $IMAGE_PREFIX/postgres:15-alpine
        env:
        - {name: POSTGRES_USER, value: postgres}
        - {name: POSTGRES_PASSWORD, value: postgres}
        - {name: POSTGRES_DB, value: flyteadmin}
        - {name: PGDATA, value: /var/lib/postgresql/data/pgdata}
        # The chart sets db.*.passwordPath="", so flyteadmin/datacatalog connect
        # with NO password. Postgres' default host auth would reject that, so
        # allow trust. Evaluation-grade only -- never do this in production.
        - {name: POSTGRES_HOST_AUTH_METHOD, value: trust}
        ports:
        - containerPort: 5432
        volumeMounts:
        - {name: pgdata, mountPath: /var/lib/postgresql/data}
        - {name: init, mountPath: /docker-entrypoint-initdb.d}
        readinessProbe:
          exec: {command: ["pg_isready", "-U", "postgres"]}
          initialDelaySeconds: 10
          periodSeconds: 5
        resources:
          requests: {cpu: 100m, memory: 256Mi}
      volumes:
      - name: pgdata
        $(vol flyte-pgdata)
      - name: init
        configMap: {name: flyte-pg-init}
EOF

# ---- 2. minio (the piece the chart omits) ---------------------------------
echo "==> minio blob store"
pvc flyte-minio 20Gi

$K apply -f - <<EOF
apiVersion: v1
kind: Service
metadata:
  name: minio
spec:
  ports:
  - {name: api, port: 9000, targetPort: 9000}
  - {name: console, port: 9001, targetPort: 9001}
  selector:
    app: minio
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
spec:
  replicas: 1
  selector:
    matchLabels:
      app: minio
  template:
    metadata:
      labels:
        app: minio
    spec:
      containers:
      - name: minio
        # Fully qualified: quay.io, so cri-o's short-name enforcement is moot.
        image: quay.io/minio/minio:latest
        args: ["server", "/data", "--console-address", ":9001"]
        env:
        - {name: MINIO_ROOT_USER, value: $MINIO_USER}
        - {name: MINIO_ROOT_PASSWORD, value: $MINIO_PASS}
        ports:
        - {containerPort: 9000}
        - {containerPort: 9001}
        volumeMounts:
        - {name: data, mountPath: /data}
        readinessProbe:
          httpGet: {path: /minio/health/live, port: 9000}
          initialDelaySeconds: 5
          periodSeconds: 5
        resources:
          requests: {cpu: 100m, memory: 256Mi}
      volumes:
      - name: data
        $(vol flyte-minio)
EOF

$K rollout status deploy/minio --timeout=300s
$K rollout status deploy/postgres --timeout=300s

# ---- 3. The bucket -------------------------------------------------------
# minio comes up empty; flyte will not create its own container.
echo "==> bucket $MINIO_BUCKET"
$K delete job flyte-minio-mb --ignore-not-found >/dev/null
$K apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: flyte-minio-mb
spec:
  backoffLimit: 4
  template:
    spec:
      restartPolicy: OnFailure
      containers:
      - name: mc
        image: quay.io/minio/mc:latest
        command: ["sh", "-c"]
        args:
        - |
          set -e
          until mc alias set fl http://minio:9000 $MINIO_USER $MINIO_PASS; do sleep 3; done
          mc mb --ignore-existing fl/$MINIO_BUCKET
          mc ls fl
EOF
$K wait --for=condition=complete job/flyte-minio-mb --timeout=180s

# ---- 4. flyte-core ------------------------------------------------------
echo "==> flyte-core"
helm repo add flyteorg https://flyteorg.github.io/flyte >/dev/null 2>&1 || true
helm repo update flyteorg >/dev/null

VERSION_ARG=()
[[ -n "$CHART_VERSION" ]] && VERSION_ARG=(--version "$CHART_VERSION")

# Task pods need blob-store credentials of their own. flytepropeller's
# `plugins.k8s.default-env-vars` defaults to [], so without this every task pod
# starts, tries to download its own code package from minio, and dies with
# "Unable to locate credentials" -- retried to exhaustion, so the workflow fails
# with no hint that the cause is configuration rather than the DAG. flytekit
# inside the pod reads exactly these three FLYTE_AWS_* names.
#
# FLYTE_TASK_IMAGE must be here too, not just in the registration client. A
# @dynamic task builds its sub-graph AT RUN TIME inside its own pod, so it
# re-evaluates the DAG module's ImageSpec there -- and without the override it
# emits the ImageSpec-derived name (`csv-etl:<hash>`) for every sub-node, which
# does not exist in any registry. On cri-o that surfaces as "short name mode is
# enforcing ... returns ambiguous list"; elsewhere it is a failed pull.
ENV_JSON='configmap.k8s.plugins.k8s.default-env-vars=[
  {"FLYTE_AWS_ENDPOINT":"http://minio.'"$FLYTE_NS"'.svc.cluster.local:9000"},
  {"FLYTE_AWS_ACCESS_KEY_ID":"'"$MINIO_USER"'"},
  {"FLYTE_AWS_SECRET_ACCESS_KEY":"'"$MINIO_PASS"'"},
  {"FLYTE_TASK_IMAGE":"'"$TASK_IMAGE"'"}
]'

# The chart seeds three projects (flytesnacks, flytetester, flyteexamples) via a
# `flyteadmin migrate seed-projects` init container, and `configmap.domain.domains`
# defaults to development/staging/production. The syncresources loop then walks
# every pair and creates a `<project>-<domain>` Namespace + ResourceQuota, so the
# stock install lands TEN namespaces for a bake-off that uses one. Seed only ours.
#
# Note this only ADDS on upgrade -- seed-projects never removes. Dropping a
# project from an existing install means archiving it (flyteadmin's clusterresource
# provider filters `state != ARCHIVED`, so the sync loop stops recreating its
# namespaces) and then deleting the namespaces by hand. See RUNNING.md 9f.
helm --kube-context "$KCTX" upgrade --install flyte flyteorg/flyte-core -n "$FLYTE_NS" \
  "${VERSION_ARG[@]}" \
  --set-json "$ENV_JSON" \
  --set-json 'flyteadmin.initialProjects=["'"$FLYTE_PROJECT"'"]' \
  --set postgres.enabled=false \
  --set common.ingress.enabled=false \
  --set db.admin.database.host=postgres \
  --set db.admin.database.port=5432 \
  --set db.admin.database.dbname=flyteadmin \
  --set db.admin.database.username=postgres \
  --set db.admin.database.passwordPath="" \
  --set db.datacatalog.database.host=postgres \
  --set db.datacatalog.database.port=5432 \
  --set db.datacatalog.database.dbname=datacatalog \
  --set db.datacatalog.database.username=postgres \
  --set db.datacatalog.database.passwordPath="" \
  --timeout 15m --wait

echo
echo "==> installed:"
helm --kube-context "$KCTX" list -n "$FLYTE_NS"
$K get deploy
cat <<NOTE

Sanity checks that would have caught that state:

  # the storage endpoint must name a Service that EXISTS
  $K get cm flyte-admin-base-config -o jsonpath='{.data.storage\.yaml}'
  $K get svc minio

  # UI
  kubectl --context $KCTX -n $FLYTE_NS port-forward svc/flyteconsole 8080:80

Task images still need building for the cluster's architecture:
  export FLYTE_IMAGE_PLATFORM=linux/arm64      # see RUNNING.md 7b
NOTE

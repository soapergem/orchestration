#!/usr/bin/env bash
#
# Register the Flyte DAGs with flyteadmin, from INSIDE the cluster.
#
# Why in-cluster: `pyflyte register` uploads the code package to the blob store
# using a signed URL that flyteadmin generates, and that URL names the in-cluster
# endpoint (http://minio.flyte.svc.cluster.local:9000). A client on the
# workstation cannot resolve it, so registration dies with a NameResolutionError
# after having done all the real work. Running the client in a pod means the
# name resolves and no Flyte reconfiguration is needed.
#
# The task image doubles as the registration client -- it already has flytekit
# plus every DAG dependency.
#
# Usage:
#   KCTX=my-arm64-cluster ./register.sh                      # all four DAGs
#   KCTX=my-arm64-cluster ./register.sh dag3_payment.py      # just one
#
# Env:
#   KCTX          kube context (required)
#   FLYTE_NS      namespace running flyteadmin      (default: flyte)
#   TASK_IMAGE    prebuilt task image               (default: $ECR/orch-bakeoff-flyte:latest)
#   PROJECT       flyte project                     (default: flytesnacks)
#   DOMAIN        flyte domain                      (default: development)

set -euo pipefail

: "${KCTX:?set KCTX to the kube context (e.g. KCTX=my-arm64-cluster)}"
FLYTE_NS="${FLYTE_NS:-flyte}"
: "${ECR:?set ECR to your registry, e.g. <account-id>.dkr.ecr.<region>.amazonaws.com (see .envrc.example)}"
TASK_IMAGE="${TASK_IMAGE:-$ECR/orch-bakeoff-flyte:latest}"
PROJECT="${PROJECT:-flytesnacks}"
DOMAIN="${DOMAIN:-development}"

SRC_DIR="$(cd "$(dirname "$0")" && pwd)"
TARGETS="${*:-dag1_csv_etl.py dag2_api_fanout.py dag3_payment.py dag4_order_fulfillment.py}"
K="kubectl --context $KCTX -n $FLYTE_NS"

echo "==> registering [$TARGETS] into $PROJECT/$DOMAIN with image $TASK_IMAGE"

# The DAG source travels as a ConfigMap. 85 KB total, well inside the 1 MB limit.
$K create configmap flyte-dag-src \
  --from-file=__init__.py="$SRC_DIR/__init__.py" \
  --from-file=types.py="$SRC_DIR/types.py" \
  --from-file=dag1_csv_etl.py="$SRC_DIR/dag1_csv_etl.py" \
  --from-file=dag2_api_fanout.py="$SRC_DIR/dag2_api_fanout.py" \
  --from-file=dag3_payment.py="$SRC_DIR/dag3_payment.py" \
  --from-file=dag4_order_fulfillment.py="$SRC_DIR/dag4_order_fulfillment.py" \
  --dry-run=client -o yaml | kubectl --context "$KCTX" apply -f - >/dev/null

$K delete job flyte-register --ignore-not-found >/dev/null
# Wait for the old pod to go, or the new Job adopts nothing and logs are confusing.
$K wait --for=delete pod -l job-name=flyte-register --timeout=60s >/dev/null 2>&1 || true

$K apply -f - <<EOF >/dev/null
apiVersion: batch/v1
kind: Job
metadata:
  name: flyte-register
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      imagePullSecrets:
      - name: ecr-bakeoff
      containers:
      - name: register
        image: $TASK_IMAGE
        env:
        # Bypasses ImageSpec entirely -- see the note in the DAG files. Without
        # this, registration tries to BUILD an image, which needs a local
        # container builder and the right target architecture.
        - {name: FLYTE_TASK_IMAGE, value: "$TASK_IMAGE"}
        # flytekit reaches for the OS keyring for credential storage; there is
        # none in a container (or in WSL2), and it fails with
        # "Failed to create the collection: Prompt dismissed".
        - {name: PYTHON_KEYRING_BACKEND, value: "keyring.backends.null.Keyring"}
        - {name: FLYTE_PLATFORM_URL, value: "flyteadmin.$FLYTE_NS.svc.cluster.local:81"}
        - {name: FLYTE_PLATFORM_INSECURE, value: "true"}
        command: ["sh", "-c"]
        args:
        - |
          set -e
          # Copy only *.py, dereferencing the ConfigMap's internal
          # ..2026_.../ versioned symlink dir. A plain \`cp -r\` of the mount
          # leaks that path into the module name and pyflyte then fails with
          # "No module named 'flyte.'".
          mkdir -p /src/flyte
          cp /dagsrc/*.py /src/flyte/
          cd /src
          for f in $TARGETS; do
            echo "--- registering \$f"
            pyflyte register --project $PROJECT --domain $DOMAIN \\
              --image "\$FLYTE_TASK_IMAGE" "flyte/\$f"
          done
          echo REGISTER_OK
        volumeMounts:
        - {name: dagsrc, mountPath: /dagsrc, readOnly: true}
        - {name: src, mountPath: /src}
        resources:
          requests: {cpu: 200m, memory: 512Mi}
      volumes:
      - name: dagsrc
        configMap: {name: flyte-dag-src}
      - name: src
        emptyDir: {}
EOF

for _ in $(seq 1 60); do
  s=$($K get job flyte-register -o jsonpath='{.status.succeeded}/{.status.failed}' 2>/dev/null || true)
  case "$s" in
    1/*) echo "==> REGISTER OK"; $K logs job/flyte-register 2>&1 | grep -E '^\[✔\]|^---' | tail -40; exit 0 ;;
    */1) echo "==> REGISTER FAILED"; $K logs job/flyte-register 2>&1 | tail -30; exit 1 ;;
  esac
  sleep 5
done
echo "==> timed out waiting for the registration job"
$K logs job/flyte-register 2>&1 | tail -30
exit 1

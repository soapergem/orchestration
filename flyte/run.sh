#!/usr/bin/env bash
#
# Launch a registered Flyte workflow and wait for it to finish.
#
# Runs the client in-cluster for the same reason register.sh does: flyteadmin and
# minio are only addressable by their in-cluster DNS names.
#
# Usage:
#   KCTX=my-arm64-cluster ./run.sh dag3
#   KCTX=my-arm64-cluster ./run.sh dag3 PAY-MYID-1        # explicit idempotency key
#
# Env:
#   KCTX / FLYTE_NS / TASK_IMAGE / PROJECT / DOMAIN  -- as in register.sh
#   BAKEOFF_NS   bake-off schema namespace  (default: flyte -> flyte_dag3 etc.)

set -euo pipefail

: "${KCTX:?set KCTX to the kube context}"
FLYTE_NS="${FLYTE_NS:-flyte}"
: "${ECR:?set ECR to your registry, e.g. <account-id>.dkr.ecr.<region>.amazonaws.com (see .envrc.example)}"
TASK_IMAGE="${TASK_IMAGE:-$ECR/orch-bakeoff-flyte:latest}"
PROJECT="${PROJECT:-flytesnacks}"
DOMAIN="${DOMAIN:-development}"
BAKEOFF_NS="${BAKEOFF_NS:-flyte}"
DAG="${1:?usage: run.sh dag3 [id]}"
RUN_ID="${2:-}"

K="kubectl --context $KCTX -n $FLYTE_NS"
# Unique per invocation: a single shared job name means launching a second DAG
# deletes the first one's launcher mid-flight, and that DAG never starts.
JOB="flyte-run-$DAG"

DB='{"host": "postgres", "port": 5432, "database": "orchestration", "user": "orchestration", "password": "orchestration", "namespace": "'"$BAKEOFF_NS"'"}'
ADDR='{"street": "1 Main St", "city": "Springfield", "state": "IL", "zip_code": "62701", "country": "US"}'

# Every host below is the ExternalName ALIAS that alias-backbone.sh puts in the
# task namespace: task pods run in <project>-<domain>, not in the flyte
# namespace, so the bare compose names have to resolve there.
case "$DAG" in
  dag1)
    WF=flyte.dag1_csv_etl.csv_etl_pipeline
    IN_NAME=etl_input; IN_TYPE=ETLInput
    [[ -z "$RUN_ID" ]] && RUN_ID="etl"
    INPUTS='{
        "zip_file_path": "http://fixture-service:8099/sample-data.zip",
        "extract_dir": "/tmp/csv_extract",
        "output_dir": "/tmp/parquet_output",
        "db_config": '"$DB"'}'
    ;;
  dag2)
    WF=flyte.dag2_api_fanout.api_fanout_pipeline
    IN_NAME=fanout_input; IN_TYPE=FanOutInput
    [[ -z "$RUN_ID" ]] && RUN_ID="fanout"
    INPUTS='{
        "url": "http://fixture-service:8099/books?per_page=30",
        "request_config": {"callback_fetch_service_url": "http://callback-fetch-service:8090",
                           "api_key": "", "user_agent": "orchestration-bakeoff/1.0"}}'
    ;;
  dag3)
    WF=flyte.dag3_payment.payment_workflow
    IN_NAME=payment_input; IN_TYPE=PaymentInput
    [[ -z "$RUN_ID" ]] && RUN_ID="PAY-FLYTE-$(date +%H%M%S)"
    INPUTS='{
        "payment_id": "'"$RUN_ID"'", "amount": 100.0, "currency": "USD",
        "from_account": "ACC-001", "to_account": "ACC-003",
        "idempotency_key": "'"$RUN_ID"'",
        "db_config": '"$DB"'}'
    ;;
  dag4)
    WF=flyte.dag4_order_fulfillment.order_fulfillment_workflow
    IN_NAME=order_input; IN_TYPE=OrderInput
    [[ -z "$RUN_ID" ]] && RUN_ID="ORD-FLYTE-$(date +%H%M%S)"
    # 559.97 total clears the 500 threshold, so this exercises the approval path.
    INPUTS='{
        "order_id": "'"$RUN_ID"'", "customer_id": "CUST-42",
        "items": [{"sku": "WIDGET-A", "quantity": 2, "unit_price": 29.99},
                  {"sku": "GADGET-B", "quantity": 1, "unit_price": 499.99}],
        "shipping_address": '"$ADDR"',
        "approval_threshold": 500.0,
        "db_config": '"$DB"'}'
    ;;
  *) echo "unknown dag: $DAG (expected dag1|dag2|dag3|dag4)"; exit 1 ;;
esac

# Collapse to one line: the JSON is embedded in a YAML flow mapping
# (`{name: WF_INPUTS, value: '...'}`), which a literal newline would break.
INPUTS=$(printf '%s' "$INPUTS" | tr -d '\n' | tr -s ' ')

echo "==> launching $WF (id=$RUN_ID) in $PROJECT/$DOMAIN"

$K delete job "$JOB" --ignore-not-found >/dev/null
$K wait --for=delete pod -l job-name="$JOB" --timeout=60s >/dev/null 2>&1 || true

$K apply -f - <<EOF >/dev/null
apiVersion: batch/v1
kind: Job
metadata:
  name: $JOB
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      imagePullSecrets:
      - name: ecr-bakeoff
      containers:
      - name: run
        image: $TASK_IMAGE
        env:
        - {name: PYTHON_KEYRING_BACKEND, value: "keyring.backends.null.Keyring"}
        - {name: WF_NAME, value: "$WF"}
        - {name: WF_INPUTS, value: '$INPUTS'}
        - {name: WF_IN_NAME, value: "$IN_NAME"}
        - {name: WF_IN_TYPE, value: "$IN_TYPE"}
        command: ["sh", "-c"]
        args:
        - |
          set -e
          mkdir -p /src/flyte && cp /dagsrc/*.py /src/flyte/
          cd /src && python -c '
          import json, os
          from flytekit.remote import FlyteRemote
          from flytekit.configuration import Config, PlatformConfig
          from flyte import types as T
          r = FlyteRemote(
              config=Config(platform=PlatformConfig(
                  endpoint="flyteadmin.$FLYTE_NS.svc.cluster.local:81", insecure=True)),
              default_project="$PROJECT", default_domain="$DOMAIN")
          wf = r.fetch_workflow(name=os.environ["WF_NAME"])
          # Hydrate into the real dataclass rather than passing raw dicts.
          # flytekit will not coerce a nested list of dicts into List[OrderItem]
          # ("Type of Val <class list> is not an instance of typing.List[dict]"),
          # and every type in types.py is @dataclass_json, so from_dict handles
          # the nesting for free.
          cls = getattr(T, os.environ["WF_IN_TYPE"])
          arg = cls.from_dict(json.loads(os.environ["WF_INPUTS"]))
          ex = r.execute(wf, inputs={os.environ["WF_IN_NAME"]: arg}, wait=False)
          print("EXECUTION_ID", ex.id.name)
          '
        volumeMounts:
        - {name: dagsrc, mountPath: /dagsrc, readOnly: true}
        - {name: src, mountPath: /src}
        resources:
          requests: {cpu: 100m, memory: 256Mi}
      volumes:
      - name: dagsrc
        configMap: {name: flyte-dag-src}
      - name: src
        emptyDir: {}
EOF

for _ in $(seq 1 24); do
  s=$($K get job "$JOB" -o jsonpath='{.status.succeeded}/{.status.failed}' 2>/dev/null || true)
  case "$s" in 1/*|*/1) break ;; esac
  sleep 5
done
EX=$($K logs job/"$JOB" 2>/dev/null | awk '/EXECUTION_ID/{print $2}')
[[ -z "$EX" ]] && { echo "==> launch failed"; $K logs job/"$JOB" 2>&1 | tail -20; exit 1; }
echo "==> execution $EX"

TASK_NS="$PROJECT-$DOMAIN"
# FlyteWorkflow status.phase: 3=Success 5=Failed 6=Aborted
for _ in $(seq 1 100); do
  ph=$(kubectl --context "$KCTX" -n "$TASK_NS" get flyteworkflow "$EX" \
        -o jsonpath='{.status.phase}' 2>/dev/null || true)
  case "$ph" in 3) echo "==> SUCCEEDED"; break ;; 5) echo "==> FAILED"; break ;; 6) echo "==> ABORTED"; break ;; esac
  sleep 6
done
kubectl --context "$KCTX" -n "$TASK_NS" get pods --no-headers 2>/dev/null | grep "^$EX" | sed 's/^/    /'
echo
echo "Console:  kubectl --context $KCTX -n $FLYTE_NS port-forward svc/flyteconsole 8083:80"
echo "          http://localhost:8083/console/projects/$PROJECT/domains/$DOMAIN/executions/$EX"
[[ "$ph" == "3" ]] || exit 1

#!/usr/bin/env bash
# Execute one of the four workflows with a ready-made input.
#
#   ./run-workflow.sh dag1
#   ./run-workflow.sh dag3 --force-outcome declined
#   ./run-workflow.sh dag4 --order-id ORD-123
#
# DAG 3 and DAG 4 take db_config as an execution *input*, so the Neon DSN is
# needed here rather than at deploy time:
#   export NEON_DATABASE_URL='postgresql://user:pass@host/orchestration?sslmode=require'
#
# The DSN never reaches Terraform state, but it DOES land in the execution's
# recorded input, visible in the console. Evaluation-grade, like the rest of the
# credentials in this repo.
set -euo pipefail

DAG="${1:-}"
shift || true
[[ -z "$DAG" ]] && { echo "usage: $0 dag1|dag2|dag3|dag4 [options]" >&2; exit 2; }

TF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: "${PROJECT_ID:?set PROJECT_ID (see .envrc.example)}"
REGION="${REGION:-us-central1}"

name() { (cd "$TF_DIR" && terraform output -json workflow_names | python3 -c "import json,sys;print(json.load(sys.stdin)['$1'])"); }
tfout() { (cd "$TF_DIR" && terraform output -raw "$1"); }

FORCE_OUTCOME=""
ORDER_ID=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --force-outcome) FORCE_OUTCOME="$2"; shift 2 ;;
    --order-id)      ORDER_ID="$2"; shift 2 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

db_json() {
  [[ -z "${NEON_DATABASE_URL:-}" ]] && {
    echo "NEON_DATABASE_URL is not set -- DAG 3/4 need it for db_config" >&2
    exit 1
  }
  NEON_DATABASE_URL="$NEON_DATABASE_URL" python3 - <<'PY'
import json, os, urllib.parse
u = urllib.parse.urlparse(os.environ["NEON_DATABASE_URL"])
print(json.dumps({
    "host": u.hostname,
    "port": u.port or 5432,
    "database": (u.path or "/orchestration").lstrip("/") or "orchestration",
    "user": urllib.parse.unquote(u.username or ""),
    "password": urllib.parse.unquote(u.password or ""),
}))
PY
}

case "$DAG" in
  dag1)
    INPUT=$(python3 -c "
import json,sys
print(json.dumps({'zip_url': sys.argv[1], 'gcs_bucket': sys.argv[2], 'extract_prefix': 'extracted/', 'db_config': json.loads(sys.argv[3])}))
" "$(tfout zip_url)" "$(tfout data_bucket)" "$(db_json)")
    ;;
  dag2)
    INPUT='{}'
    ;;
  dag3)
    INPUT=$(python3 -c "
import json,sys,uuid
pid = 'PAY-GW-' + uuid.uuid4().hex[:8].upper()
d = {'payment_id': pid, 'idempotency_key': pid, 'amount': 100.0, 'currency': 'USD',
     'from_account': 'ACC-001', 'to_account': 'ACC-002', 'db_config': json.loads(sys.argv[1])}
if sys.argv[2]:
    d['gateway_force'] = sys.argv[2]
print(json.dumps(d))
" "$(db_json)" "$FORCE_OUTCOME")
    ;;
  dag4)
    INPUT=$(python3 -c "
import json,sys,uuid
oid = sys.argv[2] or ('ORD-GW-' + uuid.uuid4().hex[:8].upper())
print(json.dumps({
    'order_id': oid, 'customer_id': 'CUST-42',
    'items': [{'sku': 'GADGET-B', 'quantity': 1, 'unit_price': 499.99},
              {'sku': 'WIDGET-A', 'quantity': 1, 'unit_price': 29.99}],
    'shipping_address': {'street': '123 Main St', 'city': 'Springfield',
                         'state': 'IL', 'zip': '62701', 'country': 'US'},
    'approval_threshold': 500.0,
    'db_config': json.loads(sys.argv[1]),
}))
" "$(db_json)" "$ORDER_ID")
    ;;
  *) echo "unknown dag: $DAG" >&2; exit 2 ;;
esac

WF="$(name "$DAG")"
echo "==> executing $WF"
echo "$INPUT" | python3 -c "import json,sys; d=json.load(sys.stdin); d.pop('db_config',None); print('    input (db_config elided):', json.dumps(d))"

gcloud workflows execute "$WF" \
  --location="$REGION" \
  --data="$INPUT" \
  --project="$PROJECT_ID"

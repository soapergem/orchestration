#!/usr/bin/env bash
#
# Alias the backbone Services into a workflow namespace.
#
# Why this exists: the Argo DAG YAML sets PGHOST and the mock-service hostnames
# as *literal* env values (`postgres`, `shipping-service`), and a bare name only
# resolves within the pod's own namespace. Rather than deploying a second
# Postgres per orchestrator -- or editing every manifest to an FQDN -- point
# ExternalName Services at the real ones. CoreDNS returns a CNAME, so the bare
# name resolves and the connection lands on the single shared backbone.
#
# Flyte does not strictly need this: its DB settings are a typed workflow input
# (DBConfig), so it can take an FQDN directly. Argo does.
#
# Usage:
#   KCTX=my-arm64-cluster WORKFLOW_NS=argo ./alias-backbone.sh
#
# Env:
#   KCTX          kube context (required)
#   WORKFLOW_NS   namespace the workflow pods run in (required)
#   ORCH_NS       namespace the real backbone lives in (default: orchestrators)

set -euo pipefail

: "${KCTX:?set KCTX to the kube context}"
: "${WORKFLOW_NS:?set WORKFLOW_NS to the namespace the workflow pods run in}"
ORCH_NS="${ORCH_NS:-orchestrators}"

if [[ "$WORKFLOW_NS" == "$ORCH_NS" ]]; then
  echo "WORKFLOW_NS == ORCH_NS ($ORCH_NS): the names already resolve, nothing to do."
  exit 0
fi

kubectl --context "$KCTX" create namespace "$WORKFLOW_NS" \
  --dry-run=client -o yaml | kubectl --context "$KCTX" apply -f -

# ExternalName carries no ports, so the port stays whatever the caller dials --
# which is fine, since every caller uses the same ports the real Services expose.
for entry in postgres:5432 callback-fetch-service:8090 approval-service:8091 \
             shipping-service:8092 fixture-service:8099; do
  name="${entry%%:*}"
  kubectl --context "$KCTX" apply -n "$WORKFLOW_NS" -f - <<EOF
apiVersion: v1
kind: Service
metadata:
  name: $name
  annotations:
    bakeoff/aliases: $name.$ORCH_NS.svc.cluster.local
spec:
  type: ExternalName
  externalName: $name.$ORCH_NS.svc.cluster.local
EOF
done

echo
echo "==> aliased into $WORKFLOW_NS (targets live in $ORCH_NS):"
kubectl --context "$KCTX" -n "$WORKFLOW_NS" get svc \
  -o custom-columns='NAME:.metadata.name,TYPE:.spec.type,TARGET:.spec.externalName'

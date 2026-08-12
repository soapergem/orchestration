#!/usr/bin/env bash
# Deploy the bake-off backbone (Postgres + the four mock services) into a
# Kubernetes cluster, for the orchestrators that RUN there -- Argo and Flyte.
#
#   KCTX=my-cluster BASE_DOMAIN=example.com ./deploy-backbone.sh
#
# This is now a THIN WRAPPER over the Helm chart in this directory. It used to be
# 508 lines of inline YAML -- a second, divergent implementation of what the
# chart already did -- and the two had drifted apart in both directions:
#
#   the script had  Postgres, and no AWS resume credentials
#   the chart had   AWS resume credentials, and no Postgres
#
# Neither gap announced itself. The missing credentials surfaced only as Step
# Functions DAG 2 reporting `FanOutError` and DAG 4 reporting "Order rejected or
# approval timed out" -- a Kubernetes Secret problem presented as a business
# decision two systems away (see step-functions/README.md). The chart's missing
# Postgres meant nobody could use it for Argo or Flyte, which is why the script
# existed at all.
#
# One chart, two presets:
#   values.yaml            AWS path -- prebuilt ECR images, Neon, no Postgres
#   values-incluster.yaml  this path -- source packaging, in-cluster Postgres
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="${ORCH_NS:-orchestrators}"
RELEASE="${RELEASE:-mock-services}"
BASE_DOMAIN="${BASE_DOMAIN:-${PUBLIC_DOMAIN:-}}"
PREFIX="${SUBDOMAIN_PREFIX:-orch-}"

if [[ -z "$BASE_DOMAIN" ]]; then
  echo "error: set BASE_DOMAIN (or PUBLIC_DOMAIN) -- e.g. BASE_DOMAIN=example.com" >&2
  echo "       ingress rules without it resolve nowhere and cert-manager retries forever." >&2
  exit 1
fi

kube=(--kube-context "${KCTX:-$(kubectl config current-context)}")

# The app source is mounted from ConfigMaps rather than baked into images: the
# arm64 cluster has no practical route to per-service images, because building
# arm64 from x86 needs qemu binfmt and rootless podman cannot register it
# (RUNNING.md 7b/9). The chart REFERENCES these; it does not template them,
# since app.py is far too large and churn-prone to live in values.yaml.
echo "==> source ConfigMaps"
for svc in callback-fetch-service approval-service shipping-service fixture-service; do
  kubectl "${kube[@]}" create configmap "${svc}-src" -n "$NAMESPACE" \
    --from-file=app.py="$here/../${svc}/app.py" \
    --dry-run=client -o yaml | kubectl "${kube[@]}" apply -f - >/dev/null
done

# init-db.sql defines bootstrap_bakeoff(ns) and runs ONLY on a fresh volume. On
# an existing one use `just seed <runner>` / `just reset <runner>`.
kubectl "${kube[@]}" create configmap bakeoff-init-db -n "$NAMESPACE" \
  --from-file=init-db.sql="$here/../init-db.sql" \
  --dry-run=client -o yaml | kubectl "${kube[@]}" apply -f - >/dev/null

echo "==> helm upgrade --install $RELEASE -n $NAMESPACE"
# --take-ownership adopts resources created before this chart existed. Harmless
# once adopted; required the first time on a cluster that ran the old script.
helm upgrade --install "$RELEASE" "$here" -n "$NAMESPACE" "${kube[@]}" \
  --create-namespace \
  -f "$here/values-incluster.yaml" \
  --take-ownership --wait --timeout 9m \
  --set "callbackFetch.domain=${PREFIX}callback-fetch.${BASE_DOMAIN}" \
  --set "approval.domain=${PREFIX}approval.${BASE_DOMAIN}" \
  --set "shipping.domain=${PREFIX}shipping.${BASE_DOMAIN}" \
  --set "fixture.domain=${PREFIX}fixture.${BASE_DOMAIN}" \
  "$@"

echo
echo "==> deployed. Credentials are NOT created here -- they are per-cluster:"
echo "    aws-resume-creds   states:SendTaskSuccess, for Step Functions DAG 2/4."
echo "                       Without it those DAGs fail in misleading ways."
echo "    google-resume-creds  Google Workflows callbacks."
echo "    fixture-s3-creds   read-only S3 for DAG 1's ZIP and DAG 2's corpus."
echo "    All three come from \`terraform -chdir=terraform/aws output\` (and"
echo "    terraform/gcp for the Google one); see README.md."

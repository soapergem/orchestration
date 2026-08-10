#!/usr/bin/env bash
#
# Deploy the mock-services Helm chart to K3s. Fills the per-service image repos
# and the aws-resume-creds Secret from Terraform outputs, so a single command
# takes you from "TF applied + images pushed" to "deployed".
#
# Prereqs:
#   - terraform applied in terraform/aws (ECR repos + resume IAM user)
#   - images pushed: terraform/aws/scripts/build-push-mock-services.sh
#   - helm + kubectl context pointing at the cluster
#   - ECR pull secret present in the namespace (managed out-of-band by
#     k8s-ecr-login-renew); a warning is printed if it's missing
#
# Usage: ./deploy.sh [-n namespace] [-r release] [-- extra helm args...]
#   e.g. ./deploy.sh -- --set ingress.enabled=false
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tf="${here}/../../terraform/aws"
namespace="orchestrators"
release="mock-services"
pull_secret="k8s-ecr-login-renew-docker-secret"

while [ $# -gt 0 ]; do
  case "$1" in
    -n) namespace="$2"; shift 2 ;;
    -r) release="$2"; shift 2 ;;
    --) shift; break ;;
    *) break ;;
  esac
done

command -v helm >/dev/null || { echo "ERROR: helm not found (brew install helm)"; exit 1; }
command -v kubectl >/dev/null || { echo "ERROR: kubectl not found"; exit 1; }

tfout() { terraform -chdir="${tf}" output -raw "$1"; }
img() {
  terraform -chdir="${tf}" output -json ecr_repository_urls \
    | python3 -c "import sys,json;print(json.load(sys.stdin)['$1'])"
}

echo "==> Context: $(kubectl config current-context)   namespace: ${namespace}"

# Namespace + resume-creds secret (idempotent).
kubectl create namespace "${namespace}" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "${namespace}" create secret generic aws-resume-creds \
  --from-literal=AWS_ACCESS_KEY_ID="$(tfout callback_resume_access_key_id)" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$(tfout callback_resume_secret_access_key)" \
  --dry-run=client -o yaml | kubectl apply -f -

# Read-only S3 creds for fixture-service, which fetches DAG 1's archive on boot.
# A separate, least-privilege user: GetObject on one prefix, nothing else.
kubectl -n "${namespace}" create secret generic fixture-s3-creds \
  --from-literal=AWS_ACCESS_KEY_ID="$(tfout fixture_reader_access_key_id)" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$(tfout fixture_reader_secret_access_key)" \
  --dry-run=client -o yaml | kubectl apply -f -

# Warn (don't fail) if the out-of-band ECR pull secret isn't there yet.
if ! kubectl -n "${namespace}" get secret "${pull_secret}" >/dev/null 2>&1; then
  echo "WARNING: pull secret '${pull_secret}' not found in ${namespace} — image pulls will fail until it exists (or pass -- --set image.pullSecretName=...)."
fi

# Ingress hostnames. values.yaml ships example.com placeholders, which resolve
# nowhere and would get cert-manager stuck retrying; set BASE_DOMAIN (or
# PUBLIC_DOMAIN) to a domain whose DNS you control and the four hosts are
# derived here. See .envrc.example.
domain="${BASE_DOMAIN:-${PUBLIC_DOMAIN:-}}"
domain_args=()
if [ -n "$domain" ]; then
  echo "==> ingress hostnames under ${domain}"
  domain_args=(
    --set callbackFetch.domain="orch-callback-fetch.${domain}"
    --set approval.domain="orch-approval.${domain}"
    --set shipping.domain="orch-shipping.${domain}"
    --set fixture.domain="orch-fixture.${domain}"
  )
else
  echo "WARNING: BASE_DOMAIN/PUBLIC_DOMAIN unset — using the example.com placeholders in values.yaml."
  echo "         Ingress will not resolve and cert-manager will not issue certificates."
fi

helm upgrade --install "${release}" "${here}" -n "${namespace}" --create-namespace \
  --set callbackFetch.repository="$(img callback-fetch)" \
  --set approval.repository="$(img approval)" \
  --set shipping.repository="$(img shipping)" \
  --set fixture.repository="$(img fixture-service)" \
  --set fixture.sampleZipUrl="$(tfout fixture_sample_zip_url)" \
  --set fixture.booksUrl="$(tfout fixture_books_url)" \
  "${domain_args[@]}" \
  "$@"

echo "==> Done. helm status ${release} -n ${namespace}"

#!/usr/bin/env bash
#
# Build the mock-service images for linux/arm64 (K3s nodes) and push them to
# ECR. Run from terraform/aws AFTER `terraform apply` (the ECR repos must exist;
# repo URLs are read from `terraform output`). Detects finch or docker.
#
# Usage: ./scripts/build-push-mock-services.sh [region] [profile]
set -euo pipefail

region="${1:-us-east-1}"
profile="${2:-soapergem}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"   # terraform/aws
services_root="${here}/../../shared-services"

runner="$(command -v finch || command -v docker || true)"
[ -n "${runner}" ] || { echo "ERROR: neither finch nor docker found"; exit 1; }

repos_json="$(terraform -chdir="${here}" output -json ecr_repository_urls)"
get_repo() { echo "${repos_json}" | python3 -c "import sys,json;print(json.load(sys.stdin)['$1'])"; }

registry="$(get_repo callback-fetch)"; registry="${registry%%/*}"
aws ecr get-login-password --region "${region}" --profile "${profile}" \
  | "${runner}" login --username AWS --password-stdin "${registry}"

for svc in callback-fetch approval shipping; do
  repo="$(get_repo "${svc}")"
  echo "==> ${svc} -> ${repo}:latest"
  "${runner}" build --platform linux/arm64 -t "${repo}:latest" "${services_root}/${svc}-service"
  "${runner}" push "${repo}:latest"
done

echo "Pushed callback-fetch, approval, shipping (linux/arm64) to ECR."

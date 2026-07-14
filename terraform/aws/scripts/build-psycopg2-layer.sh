#!/usr/bin/env bash
#
# Build a Lambda layer containing psycopg2-binary for the Lambda runtime.
# Wheels are fetched for the Lambda platform (manylinux/x86_64), not the host,
# so this works from macOS. Run before `terraform apply`.
#
# Usage: ./scripts/build-psycopg2-layer.sh [python_version]
#        (default python_version: 3.12 -- must match var.lambda_runtime)
set -euo pipefail

py_version="${1:-3.12}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
dest="${here}/build/psycopg2-layer/python"

rm -rf "${dest}"
mkdir -p "${dest}"

pip3 install \
  --platform manylinux2014_x86_64 \
  --implementation cp \
  --python-version "${py_version}" \
  --only-binary=:all: \
  --target "${dest}" \
  "psycopg2-binary==2.9.10"

echo "Built psycopg2 layer -> ${dest}"

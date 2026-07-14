#!/usr/bin/env bash
#
# Build a Lambda layer containing pyarrow for the Lambda runtime (used by
# DAG 1's convert_to_parquet). Fetches manylinux/x86_64 wheels, so this works
# from macOS. Run before `terraform apply`.
#
# Usage: ./scripts/build-pyarrow-layer.sh [python_version]   (default 3.12)
set -euo pipefail

py_version="${1:-3.12}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
dest="${here}/build/pyarrow-layer/python"

rm -rf "${dest}"
mkdir -p "${dest}"

pip3 install \
  --platform manylinux2014_x86_64 \
  --implementation cp \
  --python-version "${py_version}" \
  --only-binary=:all: \
  --target "${dest}" \
  "pyarrow==20.0.0"

echo "Built pyarrow layer -> ${dest}"

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

# pip3 is not always on PATH (this repo standardises on uv). Resolve an
# installer that can fetch wheels for a *foreign* platform -- Lambda's
# manylinux/x86_64 -- rather than the host's, so this works from macOS and from
# a uv-only Linux box alike.
if command -v uv >/dev/null 2>&1; then
  install() {
    uv pip install \
      --python-platform x86_64-manylinux2014 \
      --python-version "${py_version}" \
      --only-binary :all: \
      --target "${dest}" \
      "$1"
  }
elif command -v pip3 >/dev/null 2>&1; then
  install() {
    pip3 install \
      --platform manylinux2014_x86_64 \
      --implementation cp \
      --python-version "${py_version}" \
      --only-binary=:all: \
      --target "${dest}" \
      "$1"
  }
else
  echo "error: neither uv nor pip3 found on PATH" >&2
  exit 1
fi

install "pyarrow==20.0.0"

echo "Built pyarrow layer -> ${dest}"

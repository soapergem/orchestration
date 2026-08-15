#!/usr/bin/env bash
# Push every workflow definition to the Kruxia Flow control plane.
#
#   source kruxiaflow_bakeoff/env.sh
#   ./kruxiaflow_bakeoff/deploy.sh            # all of them
#   ./kruxiaflow_bakeoff/deploy.sh dag3       # just the ones matching a glob
#
# This is Model 3 deployment (deployment.md): the definition is DATA pushed into
# the server's registry and versioned there, so the orchestration graph changes
# without redeploying the code that executes it. Same family as Conductor and
# Kestra, opposite of Temporal/Hatchet where the worker IS the deployment.
#
# Deploys are idempotent by content hash: re-POSTing an unchanged definition is
# a no-op rather than a new version, so running this repeatedly is free.
# Sub-workflows are ordinary peer definitions -- Kruxia Flow has no nesting
# construct -- so subflows/ is deployed exactly like the top-level DAGs.

set -euo pipefail

cd "$(dirname "$0")"

API="${KRUXIAFLOW_API_URL:-http://localhost:8100}"
pattern="${1:-*}"

# `--insecure-dev` means no Authorization header is needed. Kept as a variable
# so switching the engine back to its default (OAuth2 on every request) is a
# one-line change here rather than an edit per call.
AUTH_HEADER=()
if [ -n "${KRUXIAFLOW_TOKEN:-}" ]; then
    AUTH_HEADER=(-H "Authorization: Bearer ${KRUXIAFLOW_TOKEN}")
fi

if ! curl -sf -m 10 "${API}/health" >/dev/null; then
    echo "error: no Kruxia Flow at ${API} -- run \`just up kruxiaflow\` first." >&2
    exit 1
fi

deployed=0
failed=0

# Sub-workflows first: a parent that starts a child by name is easier to reason
# about when the child already exists. The engine does not enforce this (it
# resolves the name at run time, not at deploy time), which is itself worth
# knowing -- a parent referencing a typo'd child deploys clean and fails at run.
for f in subflows/${pattern}.yaml ${pattern}.yaml; do
    [ -e "$f" ] || continue
    name=$(basename "$f" .yaml)
    # ${arr[@]+"${arr[@]}"} rather than "${arr[@]}": macOS ships bash 3.2, where
    # expanding an empty array under `set -u` is an "unbound variable" error.
    resp=$(curl -s -m 30 -w '\n%{http_code}' -X POST "${API}/api/v1/workflow_definitions" \
        -H "Content-Type: text/yaml" ${AUTH_HEADER[@]+"${AUTH_HEADER[@]}"} \
        --data-binary @"$f")
    code=$(printf '%s' "$resp" | tail -n1)
    body=$(printf '%s' "$resp" | sed '$d')
    if [ "$code" = "200" ] || [ "$code" = "201" ]; then
        version=$(printf '%s' "$body" | sed -n 's/.*"version":"\([^"]*\)".*/\1/p')
        printf '  ok    %-28s %s\n' "$name" "${version:-unchanged}"
        deployed=$((deployed + 1))
    else
        printf '  FAIL  %-28s HTTP %s\n    %s\n' "$name" "$code" "$body" >&2
        failed=$((failed + 1))
    fi
done

echo "deployed ${deployed}, failed ${failed}"
[ "$failed" -eq 0 ]

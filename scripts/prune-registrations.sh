#!/usr/bin/env bash
#
# Delete stale workflow registrations from the Hatchet and Kestra engines.
#
# Why this exists: two of the twelve UIs list definitions that are not in this
# repo, which makes "what is deployed?" unanswerable by looking.
#
#   Hatchet  19 workflows for 9 real ones. Registrations are keyed by NAME, and
#            the name carries HATCHET_CLIENT_NAMESPACE as a prefix -- so running
#            the worker once without the namespace set and once with it registers
#            two independent copies of all nine. Nothing reaps the orphans: a
#            registration outlives the worker that made it. `EventProbe` is a
#            debug workflow that no longer exists in the repo at all.
#
#   Kestra   13 flows for 7 real ones. The other six are `tutorial.*` samples
#            shipped inside the server image and auto-loaded on first boot.
#
# Neither is a code defect -- both are engines treating registration as durable
# server state rather than a projection of your source tree. That is exactly the
# "control-plane push" packaging model in deployment.md, and this script is the
# garbage collector that model requires you to write yourself.
#
# DRY RUN BY DEFAULT. Pass --apply to actually delete.
#
# Usage:
#   ./scripts/prune-registrations.sh hatchet
#   ./scripts/prune-registrations.sh kestra --apply
#   ./scripts/prune-registrations.sh all --apply
#
# Env:
#   HATCHET_URL          engine API        (default: http://localhost:8888)
#   HATCHET_EMAIL/_PASS  dashboard login   (default: the hatchet-lite seeded pair)
#   HATCHET_KEEP_PREFIX  registrations to KEEP  (default: bakeoff_ -- must match
#                        HATCHET_CLIENT_NAMESPACE in hatchet/env.sh)
#   KESTRA_URL           server            (default: http://localhost:8081)
#   KESTRA_USER/_PASS    admin login       (default: docker-compose.yml's pair)
#   KESTRA_TENANT        tenant path segment (default: main)
#   KESTRA_PRUNE_NS      namespace prefix to DELETE (default: tutorial)

set -euo pipefail

HATCHET_URL="${HATCHET_URL:-http://localhost:8888}"
HATCHET_EMAIL="${HATCHET_EMAIL:-admin@example.com}"
HATCHET_PASS="${HATCHET_PASS:-Admin123!!}"
HATCHET_KEEP_PREFIX="${HATCHET_KEEP_PREFIX:-bakeoff_}"

KESTRA_URL="${KESTRA_URL:-http://localhost:8081}"
KESTRA_USER="${KESTRA_USER:-admin@orchestration.local}"
KESTRA_PASS="${KESTRA_PASS:-Orchestration_123}"
KESTRA_TENANT="${KESTRA_TENANT:-main}"
KESTRA_PRUNE_NS="${KESTRA_PRUNE_NS:-tutorial}"

TARGET="${1:-}"
APPLY=false
[[ "${2:-}" == "--apply" ]] && APPLY=true

case "$TARGET" in
  hatchet|kestra|all) ;;
  *) echo "usage: $0 <hatchet|kestra|all> [--apply]" >&2; exit 2 ;;
esac

$APPLY || echo "== DRY RUN (pass --apply to delete) =="

# ---- hatchet ---------------------------------------------------------------
prune_hatchet() {
  echo
  echo "== hatchet ($HATCHET_URL) -- keeping names prefixed '$HATCHET_KEEP_PREFIX'"
  local jar; jar="$(mktemp)"
  trap 'rm -f "$jar"' RETURN

  if ! curl -sf -c "$jar" -X POST "$HATCHET_URL/api/v1/users/login" \
       -H 'Content-Type: application/json' \
       -d "{\"email\":\"$HATCHET_EMAIL\",\"password\":\"$HATCHET_PASS\"}" -o /dev/null; then
    echo "  ERROR: login failed -- is \`just up hatchet\` running?" >&2; return 1
  fi

  local tenant
  tenant=$(curl -sf -b "$jar" "$HATCHET_URL/api/v1/users/memberships" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["rows"][0]["tenant"]["metadata"]["id"])')

  # id<TAB>name for everything NOT carrying the keep-prefix.
  local doomed
  doomed=$(curl -sf -b "$jar" "$HATCHET_URL/api/v1/tenants/$tenant/workflows?limit=200" \
    | KEEP="$HATCHET_KEEP_PREFIX" python3 -c '
import json, os, sys
keep = os.environ["KEEP"]
for w in json.load(sys.stdin).get("rows") or []:
    if not w["name"].startswith(keep):
        print(w["metadata"]["id"], w["name"], sep="\t")')

  if [[ -z "$doomed" ]]; then echo "  nothing to prune"; return 0; fi

  while IFS=$'\t' read -r id name; do
    if $APPLY; then
      local code
      code=$(curl -s -o /dev/null -w '%{http_code}' -b "$jar" \
             -X DELETE "$HATCHET_URL/api/v1/workflows/$id")
      echo "  [$code] deleted $name"
    else
      echo "  would delete $name ($id)"
    fi
  done <<< "$doomed"
}

# ---- kestra ----------------------------------------------------------------
prune_kestra() {
  echo
  echo "== kestra ($KESTRA_URL) -- deleting namespace prefix '$KESTRA_PRUNE_NS'"
  local auth="$KESTRA_USER:$KESTRA_PASS"

  local doomed
  doomed=$(curl -sf -u "$auth" "$KESTRA_URL/api/v1/$KESTRA_TENANT/flows/search?size=500" \
    | NS="$KESTRA_PRUNE_NS" python3 -c '
import json, os, sys
ns = os.environ["NS"]
for f in json.load(sys.stdin).get("results") or []:
    if f["namespace"] == ns or f["namespace"].startswith(ns + "."):
        print(f["namespace"], f["id"], sep="\t")') || {
    echo "  ERROR: flow search failed -- is \`just up kestra\` running?" >&2; return 1; }

  if [[ -z "$doomed" ]]; then echo "  nothing to prune"; return 0; fi

  while IFS=$'\t' read -r ns id; do
    if $APPLY; then
      local code
      code=$(curl -s -o /dev/null -w '%{http_code}' -u "$auth" \
             -X DELETE "$KESTRA_URL/api/v1/$KESTRA_TENANT/flows/$ns/$id")
      echo "  [$code] deleted $ns.$id"
    else
      echo "  would delete $ns.$id"
    fi
  done <<< "$doomed"
}

[[ "$TARGET" == "hatchet" || "$TARGET" == "all" ]] && prune_hatchet
[[ "$TARGET" == "kestra"  || "$TARGET" == "all" ]] && prune_kestra
echo
$APPLY || echo "== nothing was deleted (dry run) =="
exit 0

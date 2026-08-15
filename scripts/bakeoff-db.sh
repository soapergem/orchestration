#!/usr/bin/env bash
# Seed / reset / inspect a runner's bake-off schemas on whichever database that
# runner actually uses.
#
#   ./scripts/bakeoff-db.sh status <runner>
#   ./scripts/bakeoff-db.sh seed   <runner>
#   ./scripts/bakeoff-db.sh reset  <runner> [--yes]
#
# Normally invoked as `just seed <runner>` / `just reset <runner>` /
# `just db-status <runner>`.
#
# WHY THIS EXISTS
#
# The 13 runners are spread across THREE databases, and the earlier recipes
# hardcoded `compose exec postgres`, so they silently operated on the local pod
# no matter which runner you named:
#
#   local pod :54321        airflow dagster prefect luigi temporal hatchet
#                           kestra conductor kruxiaflow
#   in-cluster postgres     argo flyte        (bakeoff-postgres, ns orchestrators)
#   Neon (public)           stepfunctions google_workflows
#
# For the four non-local runners that was worse than a no-op: `reset argo` ran
# bootstrap_bakeoff('argo') against the LOCAL pod, whose CREATE SCHEMA IF NOT
# EXISTS recreated stray argo_dag* schemas there while Argo's real data sat
# untouched in the cluster -- and reported success. Those strays are what made
# the local database look like it hosted runners it never hosted.
#
# Unknown runner names are now a hard error for the same reason: `reset typo`
# used to create typo_dag1/_dag3/_dag4 rather than fail.
#
# WHAT RESET MEANS
#
# `seed` is NOT a reset: bootstrap_bakeoff is CREATE TABLE IF NOT EXISTS plus
# INSERT ... ON CONFLICT DO NOTHING, so it restores missing structure and leaves
# drifted balances and inventory exactly as it found them. `reset` drops the
# three schemas first. Only bake-off schemas are touched -- engine metadata
# (Temporal's databases, Kestra's storage) is in separate databases entirely.
#
# Neon is shared by stepfunctions and google_workflows, so a reset there is a
# destructive operation against a real cloud database. Namespace isolation keeps
# the two apart -- dropping stepfunctions_dag* cannot affect google_workflows_dag*
# -- but it still prompts unless you pass --yes.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT" || exit 1

INIT_SQL="shared-services/init-db.sql"
# Path init-db.sql is mounted at inside the local compose container.
INIT_SQL_MOUNTED="/docker-entrypoint-initdb.d/00-init-db.sql"
ORCH_NS="${ORCH_NS:-orchestrators}"
CLUSTER_PG_DEPLOY="deploy/bakeoff-postgres"

LOCAL_RUNNERS="airflow dagster prefect luigi temporal hatchet kestra conductor kruxiaflow"
CLUSTER_RUNNERS="argo flyte"
NEON_RUNNERS="stepfunctions google_workflows"
ALL_RUNNERS="$LOCAL_RUNNERS $CLUSTER_RUNNERS $NEON_RUNNERS"

# --------------------------------------------------------------- transport

transport_for() {
    local r="$1"
    for x in $LOCAL_RUNNERS;   do [ "$r" = "$x" ] && { echo local;   return; }; done
    for x in $CLUSTER_RUNNERS; do [ "$r" = "$x" ] && { echo cluster; return; }; done
    for x in $NEON_RUNNERS;    do [ "$r" = "$x" ] && { echo neon;    return; }; done
    echo unknown
}

compose_cmd() {
    # Detection lives in the Justfile; honour what it passes, else re-detect.
    local runner="${CONTAINER_RUNNER:-}"
    if [ -z "$runner" ]; then
        if command -v finch >/dev/null 2>&1; then runner=finch
        elif command -v podman >/dev/null 2>&1; then runner=podman
        else runner=docker; fi
    fi
    echo "$runner compose -f shared-services/docker-compose.yml"
}

kubectl_cmd() {
    if [ -n "${KCTX:-}" ]; then
        echo "kubectl --context $KCTX"
    else
        echo "kubectl"
    fi
}

# psql_run <transport> [extra psql args...]  -- SQL may also arrive on stdin.
psql_run() {
    local transport="$1"; shift
    case "$transport" in
        local)
            # shellcheck disable=SC2046
            $(compose_cmd) exec -T postgres psql -U orchestration -d orchestration "$@"
            ;;
        cluster)
            # shellcheck disable=SC2046
            $(kubectl_cmd) exec -i -n "$ORCH_NS" "$CLUSTER_PG_DEPLOY" -- \
                psql -U orchestration -d orchestration "$@"
            ;;
        neon)
            psql "$NEON_DATABASE_URL" "$@"
            ;;
    esac
}

require_neon_url() {
    if [ -z "${NEON_DATABASE_URL:-}" ]; then
        echo "error: NEON_DATABASE_URL is not set -- required for this runner." >&2
        echo "       It lives in .envrc (gitignored); see .envrc.example." >&2
        exit 1
    fi
}

# --------------------------------------------------------------- validation

validate_runner() {
    validate_runner_name "${1:-}"
    # A runner's HOME database must be reachable for seed/reset/status. prune is
    # the exception -- it targets a database the runner does not live on, so it
    # calls validate_runner_name directly and checks only the --from target.
    [ "$(transport_for "$1")" = neon ] && require_neon_url
    return 0
}

validate_runner_name() {
    local r="${1:-}"
    if [ -z "$r" ]; then
        echo "error: no runner given" >&2
        usage
        exit 2
    fi
    if [ "$(transport_for "$r")" = unknown ]; then
        echo "error: '$r' is not a known runner." >&2
        echo "       Creating schemas for a typo is how stray namespaces appear," >&2
        echo "       so this is a hard failure rather than a silent bootstrap." >&2
        echo >&2
        echo "  local   $LOCAL_RUNNERS" >&2
        echo "  cluster $CLUSTER_RUNNERS" >&2
        echo "  neon    $NEON_RUNNERS" >&2
        exit 2
    fi
    return 0
}

describe_target() {
    case "$(transport_for "$1")" in
        local)   echo "local compose postgres (:54321)" ;;
        cluster) echo "in-cluster bakeoff-postgres (ns $ORCH_NS)" ;;
        neon)    echo "Neon (shared with the other cloud runner)" ;;
    esac
}

# --------------------------------------------------------------- commands

cmd_seed() {
    local runner="$1" transport
    validate_runner "$runner"
    transport="$(transport_for "$runner")"

    echo "==> seeding '$runner' on $(describe_target "$runner")"
    # init-db.sql only runs automatically on a FRESH volume, so (re)load the
    # bootstrap function first. Idempotent.
    if [ "$transport" = local ]; then
        psql_run "$transport" -q -f "$INIT_SQL_MOUNTED" || exit 1
    else
        psql_run "$transport" -q -f - < "$INIT_SQL" || exit 1
    fi
    psql_run "$transport" -c "SELECT bootstrap_bakeoff('$runner');" || exit 1
}

cmd_reset() {
    local runner="$1" assume_yes="${2:-}" transport
    validate_runner "$runner"
    transport="$(transport_for "$runner")"

    if [ "$transport" = neon ] && [ "$assume_yes" != "--yes" ]; then
        echo "About to DROP ${runner}_dag1, ${runner}_dag3, ${runner}_dag4 on NEON."
        echo "That is a real cloud database, shared with the other cloud runner."
        printf 'Type the runner name to confirm: '
        read -r reply
        if [ "$reply" != "$runner" ]; then
            echo "aborted."
            exit 1
        fi
    fi

    echo "==> dropping ${runner}_dag{1,3,4} on $(describe_target "$runner")"
    psql_run "$transport" -q -v ON_ERROR_STOP=1 \
        -c "DROP SCHEMA IF EXISTS \"${runner}_dag1\", \"${runner}_dag3\", \"${runner}_dag4\" CASCADE" \
        || exit 1
    cmd_seed "$runner"
}

# Remove a runner's schemas from a database that is NOT that runner's home.
#
# Distinct from `reset`, and deliberately so: `reset temporal` drops
# temporal_dag* on the LOCAL pod, which is Temporal's real data. This drops them
# somewhere they should never have existed -- so the target is named explicitly
# with --from, and naming the runner's own home is a hard error rather than a
# silent `reset`.
#
# The guard matters more than the drop. Every mutable table must be empty; a
# single transaction, order, approval or reservation means the schema is in use
# and the drop is refused. Seeded fixtures (accounts, inventory, customers) are
# expected and ignored -- bootstrap_bakeoff creates those, so their presence is
# not evidence of anything.
cmd_prune() {
    local runner="${1:-}" from="" force=""
    shift 2>/dev/null || true
    while [ $# -gt 0 ]; do
        case "$1" in
            --from) from="${2:-}"; shift 2 ;;
            --force) force=1; shift ;;
            *) echo "error: unexpected argument '$1'" >&2; usage; exit 2 ;;
        esac
    done

    validate_runner_name "$runner"
    local home; home="$(transport_for "$runner")"

    case "$from" in
        local|cluster|neon) ;;
        "") echo "error: prune requires --from <local|cluster|neon>." >&2
            echo "       '$runner' lives on '$home'; name the database to clean explicitly." >&2
            exit 2 ;;
        *)  echo "error: unknown --from '$from' (expected local, cluster or neon)" >&2; exit 2 ;;
    esac

    if [ "$from" = "$home" ]; then
        echo "error: '$from' IS $runner's own database -- refusing." >&2
        echo "       Dropping a runner's schemas where they belong is \`reset $runner\`," >&2
        echo "       which re-seeds afterwards. prune is for strays only." >&2
        exit 2
    fi
    [ "$from" = neon ] && require_neon_url

    # Refuse if anything ever ran here. Missing tables count as empty: a schema
    # with no transactions table cannot hold transactions.
    if [ -z "$force" ]; then
        local used
        used=$(psql_run "$from" -tAq -c "
            SELECT COALESCE(sum(n), 0) FROM (
              SELECT (SELECT count(*) FROM \"${runner}_dag3\".transactions) AS n
              WHERE to_regclass('\"${runner}_dag3\".transactions') IS NOT NULL
              UNION ALL
              SELECT (SELECT count(*) FROM \"${runner}_dag4\".orders)
              WHERE to_regclass('\"${runner}_dag4\".orders') IS NOT NULL
              UNION ALL
              SELECT (SELECT count(*) FROM \"${runner}_dag4\".approval_requests)
              WHERE to_regclass('\"${runner}_dag4\".approval_requests') IS NOT NULL
              UNION ALL
              SELECT (SELECT count(*) FROM \"${runner}_dag4\".inventory_reservations)
              WHERE to_regclass('\"${runner}_dag4\".inventory_reservations') IS NOT NULL
            ) t;" 2>/dev/null | tr -d '[:space:]')

        if [ -z "$used" ]; then
            echo "error: could not inspect ${runner}_dag* on '$from' -- refusing to drop blind." >&2
            exit 1
        fi
        if [ "$used" != "0" ]; then
            echo "error: ${runner}_dag* on '$from' holds $used run artifact(s) -- refusing." >&2
            echo "       That is not a stray. Inspect it before dropping anything:" >&2
            echo "         $(basename "$0") status $runner   # (routes to $home, not $from)" >&2
            echo "       Pass --force only if you are certain." >&2
            exit 1
        fi
    fi

    echo "==> pruning ${runner}_dag{1,3,4} from '$from' (not $runner's home, which is '$home')"
    psql_run "$from" -q -v ON_ERROR_STOP=1 \
        -c "DROP SCHEMA IF EXISTS \"${runner}_dag1\", \"${runner}_dag3\", \"${runner}_dag4\" CASCADE" \
        || exit 1
    echo "    done -- no re-seed (that is the point)"
}

cmd_status() {
    local runner="$1" transport
    validate_runner "$runner"
    transport="$(transport_for "$runner")"

    echo "runner:   $runner"
    echo "database: $(describe_target "$runner")"
    psql_run "$transport" -P pager=off -c "
        SELECT n.nspname AS schema,
               count(c.oid) AS tables
        FROM pg_namespace n
        LEFT JOIN pg_class c ON c.relnamespace = n.oid AND c.relkind = 'r'
        WHERE n.nspname LIKE '${runner}\\_dag%'
        GROUP BY 1 ORDER BY 1;"
}

usage() {
    cat >&2 <<EOF
usage: $(basename "$0") {status|seed|reset} <runner> [--yes]
       $(basename "$0") prune <runner> --from <local|cluster|neon> [--force]

runners, by the database they actually use:
  local    $LOCAL_RUNNERS
  cluster  $CLUSTER_RUNNERS
  neon     $NEON_RUNNERS

  --yes    skip the confirmation prompt on a Neon reset
  prune    drop a runner's schemas from a database that is NOT its home --
           for strays. Refuses the runner's own database (use reset), and
           refuses any schema set holding run artifacts unless --force.
EOF
}

action="${1:-}"
shift 2>/dev/null || true

case "$action" in
    seed)   cmd_seed "${1:-}" ;;
    reset)  cmd_reset "${1:-}" "${2:-}" ;;
    prune)  cmd_prune "$@" ;;
    status) cmd_status "${1:-}" ;;
    ""|-h|--help) usage; exit 2 ;;
    *) echo "error: unknown action '$action'" >&2; usage; exit 2 ;;
esac

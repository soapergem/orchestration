#!/usr/bin/env bash
# Hold open the cluster port-forwards this repo needs, until Ctrl-C.
#
#   ./scripts/port-forwards.sh              # argo + in-cluster postgres
#   ./scripts/port-forwards.sh argo         # just one
#   KCTX=oracle ./scripts/port-forwards.sh
#
# Normally invoked as `just port-forwards`.
#
# WHY THIS EXISTS
#
# Two reasons, and the second is the one that bites.
#
# 1. `kubectl port-forward` never returns -- it blocks until killed. A Justfile
#    recipe runs each line as its OWN shell, sequentially, so
#
#        port-forwards:
#            kubectl port-forward ... 2746:2746
#            kubectl port-forward ... 54322:5432
#
#    hangs on the first line and never reaches the second. Nothing is wrong with
#    just here; foreground processes simply cannot be listed one per line.
#    Backgrounding with `&` does not fix it either: each line's shell exits
#    immediately, orphaning the forward with no way to stop it from the recipe.
#
# 2. A port-forward is not durable. It is a single connection to a single pod,
#    and it dies on pod restart, node drain, laptop sleep, or an idle timeout --
#    printing an error to a terminal nobody is reading. The forward looks up
#    (the recipe is "still running") while every connection through it fails.
#    So each one is SUPERVISED here and restarted when it drops.
#
# Ctrl-C stops everything: each kubectl gets its own process group via setsid,
# and the trap signals the group, matching py-procs.sh. Killing the parent alone
# would orphan the forwards, which then keep holding their host ports -- the same
# trap py-procs.sh documents for Airflow's 13 surviving processes.

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT" || exit 1

# host_port | namespace | service | target_port | label
# Host ports are reservations in RUNNING.md 0; add new ones there and to
# shared-services/check-ports.sh at the same time.
FORWARDS="
argo|2746|argo|svc/argo-server|2746|Argo UI/API
postgres|54322|orchestrators|svc/postgres|5432|in-cluster bake-off DB
"

KCTX="${KCTX:-}"
RESTART_DELAY="${RESTART_DELAY:-2}"
PIDS=""

kube() {
    if [ -n "$KCTX" ]; then kubectl --context "$KCTX" "$@"; else kubectl "$@"; fi
}

cleanup() {
    trap - INT TERM EXIT
    echo
    echo "==> stopping port-forwards"
    for pid in $PIDS; do
        # Negative PID: signal the whole process group, not just the supervisor.
        kill -TERM "-$pid" 2>/dev/null
    done
    wait 2>/dev/null
    exit 0
}

# Restart-on-exit supervisor for one forward. Runs until killed.
supervise() {
    local label="$1" ns="$2" svc="$3" hostport="$4" target="$5"
    while :; do
        kube port-forward -n "$ns" "$svc" "$hostport:$target" >/dev/null 2>&1
        local rc=$?
        # 0 also means "dropped" here -- kubectl exits 0 when the connection is
        # closed from the far side, which is the common pod-restart case and
        # exactly what must NOT be mistaken for a clean shutdown.
        echo "    [$label] forward exited (rc=$rc); reconnecting in ${RESTART_DELAY}s"
        sleep "$RESTART_DELAY"
    done
}

port_busy() { ss -tlnH "sport = :$1" 2>/dev/null | grep -q .; }

# ---- select which forwards to run -----------------------------------------
WANTED="${*:-}"
selected=""
while IFS='|' read -r name hostport ns svc target label; do
    [ -z "${name:-}" ] && continue
    if [ -n "$WANTED" ]; then
        case " $WANTED " in *" $name "*) ;; *) continue ;; esac
    fi
    selected="${selected}${name}|${hostport}|${ns}|${svc}|${target}|${label}
"
done <<< "$FORWARDS"

if [ -z "$selected" ]; then
    echo "error: nothing selected. Known targets:" >&2
    echo "$FORWARDS" | awk -F'|' 'NF{printf "  %-9s -> localhost:%s  (%s)\n", $1, $2, $6}' >&2
    exit 2
fi

# ---- pre-flight: fail loudly before backgrounding anything ----------------
fatal=0
while IFS='|' read -r name hostport ns svc target label; do
    [ -z "${name:-}" ] && continue
    if port_busy "$hostport"; then
        echo "error: port $hostport is already bound -- is this already running?" >&2
        echo "       ./shared-services/check-ports.sh shows what owns it." >&2
        fatal=1
    fi
    if ! kube get -n "$ns" "$svc" >/dev/null 2>&1; then
        echo "error: no $svc in namespace $ns${KCTX:+ (context $KCTX)}." >&2
        [ -z "$KCTX" ] && echo "       KCTX is unset, so this used the CURRENT context: $(kubectl config current-context 2>/dev/null || echo none)" >&2
        fatal=1
    fi
done <<< "$selected"
[ "$fatal" -eq 1 ] && exit 1

# ---- run -------------------------------------------------------------------
trap cleanup INT TERM EXIT

echo "==> port-forwards${KCTX:+ (context $KCTX)} -- Ctrl-C to stop"
while IFS='|' read -r name hostport ns svc target label; do
    [ -z "${name:-}" ] && continue
    printf '    %-9s localhost:%-6s -> %s/%s:%s  (%s)\n' \
        "$name" "$hostport" "$ns" "${svc#svc/}" "$target" "$label"
    setsid bash -c "$(declare -f supervise kube); KCTX='$KCTX' RESTART_DELAY='$RESTART_DELAY' \
        supervise '$name' '$ns' '$svc' '$hostport' '$target'" &
    PIDS="$PIDS $!"
done <<< "$selected"

wait

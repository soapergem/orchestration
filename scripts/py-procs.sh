#!/usr/bin/env bash
# Start / stop / inspect the host-run orchestrator UIs.
#
#   ./scripts/py-procs.sh up [targets...]      # default: all four
#   ./scripts/py-procs.sh down [targets...]
#   ./scripts/py-procs.sh status
#
# Normally invoked as `just py-up` / `just py-down` / `just py-status`.
#
# WHY THIS EXISTS
#
# Airflow, Dagster, Prefect and Luigi have no engine container -- they are
# libraries executed in your own venv (CLAUDE.md, Layout). `just up-all` cannot
# start them: it acts on compose profiles, and only temporal, hatchet, kestra and
# conductor have one. RUNNING.md §0's port table is a *reservation* map, not a
# status map, which is why 3000/4200/8080/8082 read as unowned until you start
# these yourself. `./shared-services/check-ports.sh` shows what is really bound.
#
# Two differences from the commands RUNNING.md documents:
#
#   1. Everything binds 0.0.0.0 rather than 127.0.0.1, so a browser outside this
#      VM (a WSL2 host, another machine on the LAN) can reach the UIs. Airflow
#      already defaults there; Dagster needs -h and Prefect needs
#      PREFECT_SERVER_API_HOST.
#   2. Each process gets its own session via setsid, so `down` can signal the
#      whole process group. `dagster dev` and `airflow standalone` are
#      supervisors: killing the parent alone orphans the webserver, daemon,
#      scheduler and triggerer, which keep holding their ports (measured: 13
#      surviving processes for Airflow). Same trap as Conductor's 26 orphaned
#      task-runner children -- see CLAUDE.md, "Watch out".
#
# Workers and relay servers are deliberately excluded: each needs its engine
# already up, Hatchet's needs a token minted at runtime, and Conductor's needs
# conductor/stop_worker.sh rather than a plain kill. Start those per RUNNING.md
# when you test that specific tool.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HOST_LOGS="$REPO_ROOT/.hostlogs"
ALL_TARGETS="dagster prefect airflow luigi"

port_for() {
    case "$1" in
        dagster) echo 3000 ;;
        prefect) echo 4200 ;;
        airflow) echo 8080 ;;
        luigi)   echo 8082 ;;
        *)       echo '' ;;
    esac
}

# The command each target runs, as a string exec'd inside its own session.
command_for() {
    case "$1" in
        dagster)
            echo 'source dagster_bakeoff/env.sh && exec uv run dagster dev -m dagster_bakeoff.repository -h "${DAGSTER_HOST:-0.0.0.0}" -p "${DAGSTER_PORT:-3000}"'
            ;;
        # PREFECT_UI_API_URL is not optional once the server binds 0.0.0.0.
        # Prefect derives the URL it hands the browser from the bind host, so it
        # advertises http://0.0.0.0:4200/api -- an address no browser can dial.
        # The API is fine (/api/health returns true); only the UI is broken, and
        # it fails with "Unable to connect to Prefect server", which reads like
        # the server is down. Override it with a *reachable* host: localhost
        # suits a local or WSL2-forwarded browser; export PREFECT_UI_API_URL
        # yourself with this machine's LAN address to reach it from elsewhere.
        prefect)
            echo 'export PREFECT_SERVER_API_HOST=0.0.0.0 PREFECT_SERVER_API_PORT=4200 PREFECT_UI_API_URL="${PREFECT_UI_API_URL:-http://localhost:4200/api}" && exec uv run prefect server start'
            ;;
        airflow)
            echo 'cd airflow && source env.sh && exec uv run --project .. airflow standalone'
            ;;
        # luigid needs `setuptools<81`: luigi 3.7.1's server.py still imports
        # pkg_resources, which setuptools removed in 82, so the daemon dies on
        # import before logging anything. Scoped here rather than added to the
        # project, since nothing else wants it.
        #
        # --state-path is pinned for a quieter reason: the default is
        # /var/lib/luigi-server/state.pickle, a directory that does not exist and
        # that a non-root user cannot create. luigid starts anyway, serves
        # normally, and then fails to persist on shutdown -- so every restart
        # looks like a fresh install. See luigi/README.md.
        luigi)
            echo "exec uv run --with 'setuptools<81' luigid --address 0.0.0.0 --port 8082 --state-path '$HOST_LOGS/luigi-state.pickle'"
            ;;
        *)
            echo ''
            ;;
    esac
}

cmd_up() {
    local targets=${*:-$ALL_TARGETS}
    cd "$REPO_ROOT" || exit 1
    mkdir -p "$HOST_LOGS"

    for t in $targets; do
        local pidfile="$HOST_LOGS/$t.pid"
        if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
            echo "$t: already running (pgid $(cat "$pidfile"))"
            continue
        fi

        local cmd
        cmd="$(command_for "$t")"
        if [ -z "$cmd" ]; then
            echo "$t: unknown target (want: $ALL_TARGETS)"
            continue
        fi

        # setsid makes the inner bash a session leader, so its pid IS the process
        # group id. Recorded here, signalled as a group by `down`.
        setsid bash -c 'echo $$ > "'"$pidfile"'"; '"$cmd" > "$HOST_LOGS/$t.log" 2>&1 &
        sleep 1
        echo "$t: starting (pgid $(cat "$pidfile" 2>/dev/null || echo '?')), log .hostlogs/$t.log"
    done
}

cmd_down() {
    local targets=${*:-$ALL_TARGETS}

    for t in $targets; do
        local pidfile="$HOST_LOGS/$t.pid"
        if [ ! -f "$pidfile" ]; then
            echo "$t: no pidfile, not running"
            continue
        fi

        local pid
        pid="$(cat "$pidfile")"
        # A negative pid signals the whole process group, so supervised children
        # die with their parent instead of being orphaned onto their ports.
        if kill -TERM -- "-$pid" 2>/dev/null; then
            echo "$t: stopped (pgid $pid)"
        elif kill -TERM "$pid" 2>/dev/null; then
            echo "$t: stopped (pid $pid)"
        else
            echo "$t: already gone"
        fi
        rm -f "$pidfile"
    done
}

cmd_status() {
    printf '%-9s %-8s %-7s %s\n' NAME PGID PORT STATE

    for t in $ALL_TARGETS; do
        local port pidfile pid alive state
        port="$(port_for "$t")"
        pidfile="$HOST_LOGS/$t.pid"
        pid='-'
        [ -f "$pidfile" ] && pid="$(cat "$pidfile")"

        alive=no
        [ "$pid" != '-' ] && kill -0 "$pid" 2>/dev/null && alive=yes

        if curl -sf -m 2 -o /dev/null "http://localhost:$port"; then
            state=LISTENING
        elif [ "$alive" = yes ]; then
            state=starting
        else
            state='-'
        fi
        printf '%-9s %-8s %-7s %s\n' "$t" "$pid" "$port" "$state"
    done
}

action="${1:-}"
shift 2>/dev/null || true

case "$action" in
    up)     cmd_up "$@" ;;
    down)   cmd_down "$@" ;;
    status) cmd_status ;;
    *)
        echo "usage: $(basename "$0") {up|down|status} [targets...]" >&2
        echo "targets: $ALL_TARGETS" >&2
        exit 2
        ;;
esac

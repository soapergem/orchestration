#!/usr/bin/env bash
# Check the repo's host-port map (RUNNING.md §0) against what is actually
# listening. Reports the owner of each bound port and flags anything listening
# on a port the map doesn't account for.
#
#   ./shared-services/check-ports.sh
set -uo pipefail

# port|owner|what
MAP="
54321|shared|postgres
8090|shared|callback-fetch-service
8091|shared|approval-service
8092|shared|shipping-service
8099|shared|fixture-service (Open Library Books API + DAG 1 zip)
3000|Dagster|dagster dev UI
4200|Prefect|server UI + API
8080|Airflow|api_server UI
8793|Airflow|serve-logs (worker log server)
8794|Airflow|serve-logs (triggerer log server)
8081|Kestra|UI (container listens on 8080)
8082|Luigi|luigid central scheduler
8083|Flyte|flyteconsole port-forward
8095|Temporal|signal-relay server
8096|Hatchet|event-relay server
8233|Temporal|Web UI
8888|Hatchet|engine REST API
7233|Temporal|engine gRPC
7077|Hatchet|engine gRPC (container 7070)
2746|Argo|argo-server port-forward
8000|Conductor|server REST API (/api)
8127|Conductor|UI (container listens on 5000)
"

listening() { ss -tlnH 2>/dev/null | awk '{print $4}' | sed 's/.*://' | sort -un; }

# comm needs both inputs in the SAME collation; use plain lexical sort for it.
mapped_ports=$(echo "$MAP" | awk -F'|' 'NF{print $1}' | sort -u)
live=$(listening | sort -u)

printf '%-7s %-10s %-38s %s\n' PORT OWNER WHAT STATE
echo "$MAP" | while IFS='|' read -r port owner what; do
    [ -z "$port" ] && continue
    if echo "$live" | grep -qx "$port"; then state="LISTENING"; else state="-"; fi
    printf '%-7s %-10s %-38s %s\n' "$port" "$owner" "$what" "$state"
done

echo
unmapped=$(comm -23 <(echo "$live") <(echo "$mapped_ports"))
# Ephemeral/system ports aren't interesting; only flag the ranges we allocate from.
flagged=$(echo "$unmapped" | awk '$1>=2000 && $1<=9999')
if [ -n "$flagged" ]; then
    echo "Listening but NOT in the port map (add to RUNNING.md §0 or move):"
    echo "$flagged" | while read -r p; do
        [ -z "$p" ] && continue
        proc=$(ss -tlnpH "sport = :$p" 2>/dev/null | grep -oE 'users:\(\("[^"]+' | head -1 | sed 's/.*"//')
        printf '  %-7s %s\n' "$p" "${proc:-unknown}"
    done
else
    echo "No unmapped listeners in 2000-9999. Port map is clean."
fi

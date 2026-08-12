#!/usr/bin/env bash
# Show the login for every local service that has one, and name the ones that
# don't.
#
#   ./scripts/creds.sh
#
# Normally invoked as `just creds`.
#
# WHY THIS IS A SCRIPT AND NOT A DOC
#
# The credentials come from four different mechanisms, so a static list drifts:
#
#   generated at first run   Airflow  (written to AIRFLOW_HOME on the FIRST
#                                     `standalone` run only -- later starts log
#                                     "previously generated ... Not echoing it
#                                     here", so that file is the only copy)
#   seeded by the image      Hatchet  (hatchet-lite, not our compose file)
#   set in docker-compose    Kestra, Postgres
#   absent entirely          Conductor, Temporal, Dagster, Prefect, luigid
#
# All of it is evaluation-grade and local-only, which is the point: it is safe to
# print precisely because none of it protects anything. Do not reuse the pattern
# anywhere real. Argo, Flyte, Step Functions and Google Workflows are omitted --
# those authenticate through the cluster or cloud IAM, not a password here.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT" || exit 1

echo "SERVICE     URL                       CREDENTIALS"

# Airflow -- generated. Delete the file and restart to rotate.
PW_FILE=airflow/.airflow-home/simple_auth_manager_passwords.json.generated
if [ -f "$PW_FILE" ]; then
    pw="$(python3 -c "import json;print(json.load(open('$PW_FILE'))['admin'])" 2>/dev/null || echo '?')"
    echo "Airflow     http://localhost:8080     admin / $pw"
else
    echo "Airflow     http://localhost:8080     (not generated yet -- run 'just py-up airflow')"
fi

# Hatchet -- seeded by the hatchet-lite image. Verified against
# POST /api/v1/users/login (HTTP 200), 2026-08-12.
echo "Hatchet     http://localhost:8888     admin@example.com / Admin123!!"

# Kestra -- OSS has exactly one credential, the shared admin account; service
# accounts are Enterprise-only. Same values the mock services use to drive
# Kestra's authenticated resume endpoint.
echo "Kestra      http://localhost:8081     ${KESTRA_USER:-admin@orchestration.local} / ${KESTRA_PASSWORD:-Orchestration_123}"

echo "Postgres    localhost:54321           orchestration / orchestration (db: orchestration)"
echo
echo "No authentication at all:"
echo "  Conductor  http://localhost:8127    Conductor OSS has NO auth -- anyone who can reach"
echo "                                      the API can rewrite definitions and complete tasks."
echo "  Temporal   http://localhost:8233    dev server, no auth"
echo "  Dagster    http://localhost:3000    no auth"
echo "  Prefect    http://localhost:4200    no auth (self-hosted)"
echo "  luigid     http://localhost:8082    no auth"
echo

# The Hatchet SDK token is a JWT minted at runtime and is NOT a login -- it is
# what the host worker authenticates with. Report where it lives rather than
# dumping it; RUNNING.md §5 has the mint command.
if [ -n "${HATCHET_CLIENT_TOKEN:-}" ]; then
    echo "HATCHET_CLIENT_TOKEN: set in env (${#HATCHET_CLIENT_TOKEN} chars) -- worker auth, not a UI login"
elif [ -f shared-services/hatchet.token ]; then
    echo "HATCHET_CLIENT_TOKEN: not exported; token file at shared-services/hatchet.token"
else
    echo "HATCHET_CLIENT_TOKEN: absent -- mint one per RUNNING.md §5 before starting the Hatchet worker"
fi

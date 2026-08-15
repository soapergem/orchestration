#!/bin/bash
# Onboard this database's runners, on a FRESH volume only.
#
# Runs from /docker-entrypoint-initdb.d after init-db.sql has defined
# bootstrap_bakeoff() -- the postgres entrypoint executes that directory in
# lexical order, which is why the mounted filenames carry numeric prefixes.
#
# WHY THIS IS A SEPARATE FILE
#
# The runner list is a property of the *database instance*, not of the schema
# definitions, and the two instances host different runners:
#
#   local compose pod    the 8 host-run tools (airflow dagster prefect luigi
#                        temporal hatchet kestra conductor)
#   in-cluster postgres  argo, flyte
#   Neon                 stepfunctions, google_workflows -- not initialised here
#
# init-db.sql is shared by both instances *and* is re-run by `bakeoff-db.sh seed`
# to refresh the function, so anything with side effects in there gets replayed
# onto whichever database you happen to be seeding. That is exactly how
# temporal_* and prefect_* kept reappearing in the cluster database. Side
# effects live here instead; init-db.sql is now inert and safe to reload.
#
# Set BAKEOFF_RUNNERS to a space-separated list. Empty means onboard nothing,
# which is a legitimate choice -- `just seed <runner>` works on a live database.

set -euo pipefail

if [ -z "${BAKEOFF_RUNNERS:-}" ]; then
  echo "init-runners: BAKEOFF_RUNNERS is empty -- onboarding no runners."
  echo "init-runners: use \`just seed <runner>\` to add one later."
  return 0 2>/dev/null || exit 0
fi

for ns in $BAKEOFF_RUNNERS; do
  echo "init-runners: bootstrap_bakeoff('$ns')"
  psql -v ON_ERROR_STOP=1 \
       --username "${POSTGRES_USER:-orchestration}" \
       --dbname "${POSTGRES_DB:-orchestration}" \
       -c "SELECT bootstrap_bakeoff('$ns');"
done

echo "init-runners: onboarded -- $BAKEOFF_RUNNERS"

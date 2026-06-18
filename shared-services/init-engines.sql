-- Empty databases for the orchestrator engines that use Postgres as their
-- backing store. Each engine runs its own migrations on first start; we only
-- need the (empty) database to exist.
--
-- NOTE: docker-entrypoint-initdb.d scripts run ONLY on a fresh data volume.
-- If `pgdata` already exists, create these manually:
--   podman compose exec postgres psql -U orchestration -c 'CREATE DATABASE hatchet;'
--   podman compose exec postgres psql -U orchestration -c 'CREATE DATABASE kestra;'
--
-- Temporal's auto-setup image creates its own `temporal` and
-- `temporal_visibility` databases, so they are intentionally absent here.

CREATE DATABASE hatchet;
CREATE DATABASE kestra;

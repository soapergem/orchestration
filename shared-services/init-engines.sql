-- Empty databases for the orchestrator engines that use Postgres as their
-- backing store. Each engine runs its own migrations on first start; we only
-- need the (empty) database to exist.
--
-- NOTE: docker-entrypoint-initdb.d scripts run ONLY on a fresh data volume.
-- If `pgdata` already exists, create these manually:
--   podman compose exec postgres psql -U orchestration -c 'CREATE DATABASE hatchet;'
--   podman compose exec postgres psql -U orchestration -c 'CREATE DATABASE kestra;'
--   podman compose exec postgres psql -U orchestration -c 'CREATE DATABASE conductor;'
--   podman compose exec postgres psql -U orchestration -c 'CREATE DATABASE kruxiaflow;'
--
-- Temporal's auto-setup image creates its own `temporal` and
-- `temporal_visibility` databases, so they are intentionally absent here.

CREATE DATABASE hatchet;
CREATE DATABASE kestra;
-- Conductor stores metadata, the task queues AND the search index here: setting
-- conductor.indexing.type=postgres means it needs no Elasticsearch at all.
CREATE DATABASE conductor;
-- Kruxia Flow's event store, activity queue, workflow-definition registry and
-- Large-Object workflow storage all live here: Postgres is its ONLY external
-- dependency, which is the whole architectural claim. The container's default
-- command is `serve --migrate`, so it creates its own tables on first start.
CREATE DATABASE kruxiaflow;

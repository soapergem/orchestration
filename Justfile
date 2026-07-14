# Auto-detect the container runner: finch > podman > docker. Override with
# `just --set container_runner <name>` or `CONTAINER_RUNNER=<name> just ...`.
container_runner := env("CONTAINER_RUNNER", `if command -v finch >/dev/null 2>&1; then echo finch; elif command -v podman >/dev/null 2>&1; then echo podman; else echo docker; fi`)
compose := container_runner + " compose -f shared-services/docker-compose.yml"

default:
    @just --list | grep -v "^    default$"

# Start the shared backbone (postgres + mock services). Pass an engine profile
# to also start it, e.g. `just up temporal` / `just up hatchet` / `just up kestra`.
up profile="":
    {{ compose }} {{ if profile == "" { "" } else { "--profile " + profile } }} up -d

# Stop the shared services. Pass the same profile you started with to stop it too.
down profile="":
    {{ compose }} {{ if profile == "" { "" } else { "--profile " + profile } }} down

# Stop everything and delete volumes (postgres data, kestra storage).
down-clean:
    {{ compose }} down -v

# Follow logs for the running services.
logs:
    {{ compose }} logs -f


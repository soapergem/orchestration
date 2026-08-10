# Source before any Hatchet command:  source hatchet/env.sh
#
# Host-side environment for the Hatchet worker, relay, and starter client.
# Everything here exists because the code's defaults are compose-internal.

# --- Client -----------------------------------------------------------------
# The token is minted after the engine is up (see RUNNING.md §5). The FILE WINS
# over any inherited HATCHET_CLIENT_TOKEN on purpose: `.envrc` exports a
# long-lived JWT from an older engine, and a stale token fails with a bare
# "invalid auth token" from gRPC that looks nothing like an expiry.
_hatchet_token_file="${HATCHET_TOKEN_FILE:-$PWD/shared-services/hatchet.token}"
if [ -s "$_hatchet_token_file" ]; then
    HATCHET_CLIENT_TOKEN="$(cat "$_hatchet_token_file")"
fi
export HATCHET_CLIENT_TOKEN
unset _hatchet_token_file
export HATCHET_CLIENT_TLS_STRATEGY=none

# gRPC. The token's embedded `grpc_broadcast_address` is `hatchet-engine:7070`,
# which only resolves inside compose; from the host it's the published port.
export HATCHET_CLIENT_HOST_PORT=localhost:7077

# REST. Compose now sets SERVER_URL on the engine so freshly minted tokens carry
# the right `server_url` claim, but this stays as a belt-and-braces override for
# tokens minted before that change. The generated default was
# `http://localhost:8080` -- which on this host is AIRFLOW, so SDK calls came
# back with Airflow's error JSON ("/api/v1 has been removed in Airflow 3"), a
# deeply confusing way to learn about a port collision. See RUNNING.md §0.
export HATCHET_CLIENT_SERVER_URL=http://localhost:8888

# Namespace prefixes every workflow/action/event key. Set it because a worker
# killed with SIGKILL stays ACTIVE in the engine (no graceful deregistration)
# and keeps being handed tasks it will never run -- durable tasks especially,
# which then sit RUNNING forever with nothing in any log. A namespace gives a
# restarted worker its own action names, so stale registrations can't claim
# them. The relay must use the SAME namespace: the SDK applies it to event keys
# on both the push and the wait side.
export HATCHET_CLIENT_NAMESPACE="${HATCHET_CLIENT_NAMESPACE:-bakeoff}"

# --- Backbone ---------------------------------------------------------------
export POSTGRES_HOST=localhost
export POSTGRES_PORT=54321
export POSTGRES_DB=orchestration
export POSTGRES_USER=orchestration
export POSTGRES_PASSWORD=orchestration

# Per-(runner, DAG) schema isolation -> hatchet_dag1 / _dag3 / _dag4
export BAKEOFF_NS=hatchet

export CALLBACK_FETCH_SERVICE_URL=http://localhost:8090
export APPROVAL_SERVICE_URL=http://localhost:8091
export SHIPPING_SERVICE_URL=http://localhost:8092

# --- Event relay ------------------------------------------------------------
# Baked into the callback_url handed to the mock-service CONTAINERS, so it must
# be the host gateway as they see it. Podman: host.containers.internal.
export HATCHET_EVENT_RELAY_URL=http://host.containers.internal:8096

# DAG 4 approval wait before compensating (seconds).
export APPROVAL_TIMEOUT_SECONDS="${APPROVAL_TIMEOUT_SECONDS:-120}"

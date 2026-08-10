#!/usr/bin/env bash
# Build and push the Cloud Run task-service image to Artifact Registry.
#
# Must run BEFORE the first `terraform apply`: Cloud Run validates that the image
# exists, so applying against an empty repository fails. Re-run after editing
# shared-services/gcp-task-service/app.py -- the Cloud Run resource hashes that
# file, so the next apply rolls a new revision.
#
# Runtime-agnostic like the rest of the repo (finch > podman > docker); override
# with CONTAINER_RUNNER.
set -euo pipefail

: "${PROJECT_ID:?set PROJECT_ID (see .envrc.example) -- no default, so this cannot push into the wrong project}"
REGION="${REGION:-us-central1}"
NAME_PREFIX="${NAME_PREFIX:-orch}"
REPO="${NAME_PREFIX}-images"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/task-service:latest"

if [[ -n "${CONTAINER_RUNNER:-}" ]]; then
  RUNNER="$CONTAINER_RUNNER"
elif command -v finch >/dev/null 2>&1; then
  RUNNER=finch
elif command -v podman >/dev/null 2>&1; then
  RUNNER=podman
else
  RUNNER=docker
fi

CTX="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../shared-services/gcp-task-service" && pwd)"

echo "==> runner:  $RUNNER"
echo "==> context: $CTX"
echo "==> image:   $IMAGE"

# Cloud Run is linux/amd64. Build for it explicitly, because this repo also runs
# on arm64 hosts and a silently-arm64 image fails to start with an exec format
# error (the same trap RUNNING.md 7b documents for Flyte's ImageSpec).
echo "==> ensuring Artifact Registry repo exists"
gcloud artifacts repositories describe "$REPO" \
  --location="$REGION" --project="$PROJECT_ID" >/dev/null 2>&1 || \
gcloud artifacts repositories create "$REPO" \
  --repository-format=docker --location="$REGION" --project="$PROJECT_ID" \
  --description="Bake-off task service images"

echo "==> configuring registry auth for $RUNNER"
if [[ "$RUNNER" == "podman" ]]; then
  # podman does not read docker credential helpers, so log in directly.
  gcloud auth print-access-token --project="$PROJECT_ID" | \
    "$RUNNER" login -u oauth2accesstoken --password-stdin "${REGION}-docker.pkg.dev"
else
  gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet --project="$PROJECT_ID"
fi

echo "==> build"
"$RUNNER" build --platform linux/amd64 -t "$IMAGE" "$CTX"

echo "==> push"
"$RUNNER" push "$IMAGE"

echo "==> done: $IMAGE"

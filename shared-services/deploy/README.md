# Mock services Helm chart

**One chart, two presets.** This is the single source of truth for the
bake-off backbone in Kubernetes -- Postgres plus the four mock services
(`callback-fetch`, `approval`, `shipping`, `fixture`), with ingress and TLS.

| Preset | For | Packaging | Database |
|---|---|---|---|
| `values.yaml` (default) | **Step Functions** — Lambdas call in over the public ingress | prebuilt ECR images | Neon (none deployed) |
| `values-incluster.yaml` | **Argo, Flyte** — DAG steps run as pods here | source-mounted (registry-free) | in-cluster Postgres |

Until 2026-08-12 the in-cluster case was a *separate* 508-line imperative
script, and the two implementations had drifted in both directions:

    the script had  Postgres, and no AWS resume credentials
    the chart had   AWS resume credentials, and no Postgres

Neither gap announced itself. The missing credentials surfaced only as Step
Functions DAG 2 reporting `FanOutError` and DAG 4 reporting *"Order rejected or
approval timed out"* -- a Kubernetes Secret problem presented as a business
decision two systems away (`step-functions/README.md`). `deploy-backbone.sh` is
now a 78-line wrapper over this chart.

## Two things the chart does NOT create

Both are per-cluster and deliberately out of band:

1. **The `*-src` ConfigMaps** (source packaging only). `app.py` is far too large
   and churn-prone to live in `values.yaml`, so the chart *references* them and
   `deploy-backbone.sh` renders them. In source mode `helm install` alone is
   therefore not sufficient -- use the wrapper.
2. **The three credential Secrets.** See below; a missing one is the single
   costliest failure mode in this repo.

## Credentials (the step everyone forgets)

| Secret | Used by | Source | If absent |
|---|---|---|---|
| `aws-resume-creds` | callback-fetch, approval | `terraform -chdir=terraform/aws output callback_resume_*` | **Step Functions DAG 2 and DAG 4 fail with misleading errors** |
| `google-resume-creds` | callback-fetch, approval | `terraform -chdir=terraform/gcp output -raw resume_service_account_key` | Google Workflows resume returns a 500 saying credentials are missing |
| `fixture-s3-creds` | fixture | `terraform -chdir=terraform/aws output fixture_reader_*` | fixture `/health` returns 503 and never becomes ready |

`aws.resumeSecretName` and `gcp.resumeSecretName` may each be set to `""` on a
cluster that never runs that cloud's DAGs -- the env vars are then omitted
entirely rather than the pod failing on a missing Secret.

Use `s3://` URLs for `fixture.booksUrl`/`sampleZipUrl`, **not presigned ones**.
The pre-2026-08-12 deployment used presigned URLs with `X-Amz-Expires=604800`,
a seven-day fuse that would have silently taken DAG 2's corpus with it.

Leave `fixture.baseUrl` **empty**. Setting it pins every per-item detail URL to
one host; the old deployment pinned them to `http://fixture-service:8099`, which
a Lambda cannot resolve -- that is exactly why Step Functions' DAG 2 fan-out died
with `NameResolutionError` on every iteration. Empty means fixture-service
derives them per request, so in-cluster and public callers both work.

## Service naming

`nameSuffix` defaults to `-service` and **must stay that way** on any cluster
running Argo or Flyte: their DAGs carry `approval-service:8091`,
`callback-fetch-service:8090`, `shipping-service:8092` and `fixture-service:8099`
as *literal* env values, matching the compose DNS names. The chart rendered bare
`approval` / `callback-fetch` / `shipping` until 2026-08-12, which would have
broken every in-cluster DAG while the AWS path kept working -- Lambdas reach the
services through the public ingress, so nothing there would have noticed.

Likewise `postgres.serviceName` is `postgres`, separate from the workload name,
because DAG steps hardcode `PGHOST: "postgres"`.

## Adopting resources created before the chart

`--take-ownership` (Helm >= 3.17) adopts pre-existing resources. Three things bit
during the 2026-08-12 adoption and will bite again:

- **Probe handlers are mutually exclusive and a patch MERGES.** fixture-service
  had an httpGet `/health` probe; a chart specifying `tcpSocket` produced
  *"may not specify more than 1 handler type"*. Hence per-service `healthPath`.
- **Removals do not propagate on first adopt.** Helm's three-way merge has no
  prior manifest for an adopted resource, so it patches what the chart declares
  and leaves unknown keys alone. `FIXTURE_BASE_URL` survived an upgrade that no
  longer rendered it; pruning needed `kubectl set env deploy/x FIXTURE_BASE_URL-`.
- **A PVC cannot shrink.** Match `postgres.storage`/`storageClass` to what is
  live (the OCI cluster: 50Gi on `oci-bv`) or the upgrade is rejected.

## Prerequisites

- **Terraform applied** (`terraform/aws`): creates the ECR repos + the resume
  IAM user. `./scripts/build-push-mock-services.sh` builds and pushes the images.
- **ECR image-pull secret** `k8s-ecr-login-renew-docker-secret` already present
  in the `orchestrators` namespace. The cluster manages/refreshes it out-of-band
  (via `k8s-ecr-login-renew`); this chart only references it. Override the name
  with `--set image.pullSecretName=...`, or `--set image.pullSecretName=` to omit
  it entirely.
- **cert-manager** with a `ClusterIssuer` named `letsencrypt-prod` (override via
  `--set ingress.clusterIssuer=...`), and Traefik as the ingress controller
  (K3s default).
- **helm** and a kubeconfig pointing at the cluster.

## Quick deploy

Two entry points, by preset:

```bash
# AWS path (ECR images, Neon): reads ECR repo URLs and resume credentials from
# the Terraform outputs, ensures the namespace + aws-resume-creds, then installs.
./deploy.sh
./deploy.sh -- --set ingress.enabled=false   # extra flags after --

# In-cluster path (Argo/Flyte): renders the *-src + init-db ConfigMaps, then
# helm upgrade --install with values-incluster.yaml.
KCTX=my-cluster BASE_DOMAIN=example.com ./deploy-backbone.sh
```

The steps below are the manual breakdown (and the reference for what the script
does).

## 1. AWS resume-credentials secret

Not chart-managed — create it once from the Terraform outputs:

```bash
cd ../../terraform/aws
kubectl create namespace orchestrators --dry-run=client -o yaml | kubectl apply -f -
kubectl -n orchestrators create secret generic aws-resume-creds \
  --from-literal=AWS_ACCESS_KEY_ID="$(terraform output -raw callback_resume_access_key_id)" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$(terraform output -raw callback_resume_secret_access_key)"
cd -
```

## 2. Install / upgrade

The per-service image repositories come from Terraform. From this directory:

```bash
TF=../../terraform/aws
img() { terraform -chdir="$TF" output -json ecr_repository_urls \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['$1'])"; }

helm upgrade --install mock-services . -n orchestrators --create-namespace \
  --set callbackFetch.repository="$(img callback-fetch)" \
  --set approval.repository="$(img approval)" \
  --set shipping.repository="$(img shipping)"
```

Preview the rendered manifests first with `helm template mock-services . -n orchestrators --set ...`.

## 3. DNS

Point the three hostnames at the Traefik ingress (LoadBalancer/node IP). They
must match the Terraform `mock_service_base_domain` — the SFN lambdas call these
exact hosts:

| Service | Host | Service port |
|---|---|---|
| callback-fetch | `orch-callback-fetch.example.com` | 8090 |
| approval | `orch-approval.example.com` | 8091 |
| shipping | `orch-shipping.example.com` | 8092 |

cert-manager issues a per-host TLS cert (`<service>-tls`) once DNS resolves.

## Configuration

All values live in `values.yaml`, grouped by concern:

| Key | Purpose |
|---|---|
| `image.pullSecretName` | Existing ECR pull secret (default `k8s-ecr-login-renew-docker-secret`; `""` to omit) |
| `aws.region`, `aws.resumeSecretName` | Region + existing Secret holding the SFN resume creds |
| `callbackFetch.*`, `approval.*`, `shipping.*` | Per-service repo/tag, replicas, port, domain, and behavior env |
| `ingress.enabled` | Set `false` to render only Deployments + Services (bring your own ingress) |
| `ingress.className`, `.clusterIssuer`, `.middlewareName` | Traefik + cert-manager wiring |

Behavior env baked into the chart: `callback-fetch` runs with `AUTO_RESUME=true`
(resume the SFN task as soon as the fetch completes); `approval` runs with
`AUTO_DECIDE_*` so DAG 4 decides and resumes hands-off.

## Uninstall

```bash
helm uninstall mock-services -n orchestrators
```

The `aws-resume-creds` and pull secrets are not chart-managed, so they remain.

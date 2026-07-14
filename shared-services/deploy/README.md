# Mock services Helm chart (K3s)

Deploys the shared mock services (`callback-fetch`, `approval`, `shipping`) to
the `orchestrators` namespace on K3s, pulling per-service arm64 images from ECR.
`callback-fetch` and `approval` resume AWS Step Functions via `SendTaskSuccess`
using a dedicated IAM user's credentials; `shipping` needs no AWS access.

Ingress + TLS are included (Traefik + cert-manager), so the services are
reachable over HTTPS at `orch-*.gemovationlabs.com` — the hostnames the SFN
lambdas call.

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
| callback-fetch | `orch-callback-fetch.gemovationlabs.com` | 8090 |
| approval | `orch-approval.gemovationlabs.com` | 8091 |
| shipping | `orch-shipping.gemovationlabs.com` | 8092 |

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

# Mock services on K3s

Deploys the shared mock services (`callback-fetch`, `approval`, `shipping`) to
the `orchestrators` namespace, pulling arm64 images from ECR. You expose them at
`*.gemovationlabs.com` with your own ingress/DNS/TLS (Deployment + Service only
here). `callback-fetch` and `approval` resume AWS Step Functions via
`SendTaskSuccess`, using a dedicated IAM user's credentials from a Secret.

Prereqs: `terraform apply` (creates the ECR repos + IAM user) and
`./scripts/build-push-mock-services.sh` (builds + pushes the images), both under
`terraform/aws/`.

## 1. Namespace

```bash
kubectl apply -f namespace.yaml
```

## 2. ECR image-pull secret

K3s isn't on EKS, so it needs a docker-registry secret to pull from private ECR.
The ECR token expires every ~12h — refresh this secret (e.g. a CronJob) if pulls
start failing.

```bash
REGION=us-east-1 PROFILE=soapergem
REGISTRY="$(cd ../../terraform/aws && terraform output -json ecr_repository_urls | python3 -c 'import sys,json;print(next(iter(json.load(sys.stdin).values())).split("/")[0])')"
kubectl -n orchestrators create secret docker-registry ecr-creds \
  --docker-server="$REGISTRY" \
  --docker-username=AWS \
  --docker-password="$(aws ecr get-login-password --region $REGION --profile $PROFILE)"
```

## 3. AWS resume credentials secret

```bash
cd ../../terraform/aws
kubectl -n orchestrators create secret generic aws-resume-creds \
  --from-literal=AWS_ACCESS_KEY_ID="$(terraform output -raw callback_resume_access_key_id)" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$(terraform output -raw callback_resume_secret_access_key)"
cd -
```

## 4. Deploy (images via envsubst)

```bash
cd ../../terraform/aws
export CALLBACK_FETCH_IMAGE="$(terraform output -json ecr_repository_urls | python3 -c 'import sys,json;print(json.load(sys.stdin)["callback-fetch"])'):latest"
export APPROVAL_IMAGE="$(terraform output -json ecr_repository_urls | python3 -c 'import sys,json;print(json.load(sys.stdin)["approval"])'):latest"
export SHIPPING_IMAGE="$(terraform output -json ecr_repository_urls | python3 -c 'import sys,json;print(json.load(sys.stdin)["shipping"])'):latest"
cd -

for f in callback-fetch approval shipping; do
  envsubst < "$f.yaml" | kubectl apply -f -
done
```

## 5. Expose

Point your ingress + DNS at the Services (ports 8090/8091/8092) so they resolve at:

| Service | Host | Port |
|---|---|---|
| callback-fetch | `callback-fetch.gemovationlabs.com` | 8090 |
| approval | `approval.gemovationlabs.com` | 8091 |
| shipping | `shipping.gemovationlabs.com` | 8092 |

These hostnames must match the Terraform `mock_service_base_domain` (the SFN
lambdas call `https://callback-fetch.<domain>`, etc.).

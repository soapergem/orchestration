# AWS Step Functions — Terraform

Deploys the Step Functions bake-off DAGs to real AWS. **No VPC**: lambdas run on
default networking and reach Neon over public TLS. All DB work (DAGs 1/3/4) uses
Neon; S3 is used only for DAG 1 file I/O (ZIP input, Parquet output). The Neon
DSN is stored as an SSM Parameter Store SecureString.

Currently scaffolded: **DAG 1 (CSV ETL)**, **DAG 2 (API fan-out)**, and **DAG 3
(Payment)**. DAG 4 comes next.

DAG 2's mock service (callback-fetch) is **not** hosted in AWS — no long-running
container. Terraform creates the ECR repos + a least-privilege IAM user; you
build/push arm64 images (`scripts/build-push-mock-services.sh`) and deploy the
shared services to your **K3s** cluster (`shared-services/k8s/`, namespace
`orchestrators`), exposed at `*.gemovationlabs.com`. The service calls
`SendTaskSuccess` from K3s using the IAM user's key (a K8s Secret), resuming the
suspended `.waitForTaskToken` state. The SFN lambda reaches it at
`https://callback-fetch.<mock_service_base_domain>`.

DAG 1 isolates its dynamic tables (`orders`/`customers`/`products`/
`combined_report`) in a dedicated `dag1_etl` Postgres schema (set in its
`db.py`) so they don't collide with DAG 3/4's transactional tables in the same
Neon database.

## Prerequisites

- Terraform >= 1.9, an AWS account, and the `soapergem` profile configured
  (`aws configure --profile soapergem`).
- `NEON_DATABASE_URL` exported (it already is, via `.envrc`).
- Expose it to Terraform and build the psycopg2 layer:

  ```bash
  export TF_VAR_neon_database_url="$NEON_DATABASE_URL"   # add to .envrc
  ./scripts/build-psycopg2-layer.sh                      # -> build/psycopg2-layer/
  ./scripts/build-pyarrow-layer.sh                       # -> build/pyarrow-layer/ (DAG 1)
  ```

## Deploy

```bash
cd terraform/aws
terraform init
terraform apply
```

## Run DAG 3

```bash
aws stepfunctions start-execution \
  --profile soapergem \
  --state-machine-arn "$(terraform output -raw dag3_state_machine_arn)" \
  --input '{"payment_id":"PAY-1","amount":100,"currency":"USD","from_account":"ACC-001","to_account":"ACC-003"}'
```

The lambdas read the Neon DSN from SSM (`NEON_DB_PARAM` env var) — no `db_config`
is needed in the input anymore.

## Run DAG 1

The sample ZIP is seeded into the bucket at apply time.

```bash
aws stepfunctions start-execution \
  --profile soapergem \
  --state-machine-arn "$(terraform output -raw dag1_state_machine_arn)" \
  --input "{\"s3_bucket\":\"$(terraform output -raw dag1_bucket)\",\"zip_key\":\"$(terraform output -raw dag1_sample_zip_key)\"}"
```

Result: CSVs loaded into the `dag1_etl` schema in Neon, joined into
`dag1_etl.combined_report`, and written to `s3://<bucket>/output/combined_report.parquet`.

## Run DAG 2

Requires the callback-fetch service deployed on K3s (see
`shared-services/k8s/README.md`) and reachable at
`https://callback-fetch.<mock_service_base_domain>`. Point the DAG at any URL
returning a JSON list of items with `url` fields (each item's `url` is then
fetched in the fan-out). Keep the list small to avoid rate-limiting.

```bash
aws stepfunctions start-execution --profile soapergem \
  --state-machine-arn "$(terraform output -raw dag2_state_machine_arn)" \
  --input '{"url":"https://api.github.com/orgs/argoproj/repos?per_page=5","request_config":{}}'
```

The submit lambda registers a task token with the K3s callback-fetch service;
with `AUTO_RESUME=true` the service fetches the URL and calls `SendTaskSuccess`
(using the IAM-user creds), resuming the workflow into the fan-out + combine steps.

## Schema note

The `transactions` table in `shared-services/init-db.sql` was reconciled to match
the DAG 3 code contract (`id`, `payment_id`, `from_account`/`to_account`,
`gateway_transaction_id`, `error_message`). If you seeded Neon before that fix,
re-apply it (the table has no seed data, so a drop is safe):

```bash
psql "$NEON_DATABASE_URL" -c "DROP TABLE IF EXISTS transactions CASCADE;"
psql "$NEON_DATABASE_URL" -f ../../shared-services/init-db.sql
```

## Teardown

```bash
terraform destroy
```

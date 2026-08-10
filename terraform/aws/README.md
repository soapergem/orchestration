# AWS Step Functions — Terraform

Deploys the Step Functions bake-off DAGs to real AWS. **No VPC**: lambdas run on
default networking and reach Neon over public TLS. All DB work (DAGs 1/3/4) uses
Neon; S3 is used only for DAG 1 file I/O (ZIP input, Parquet output). The Neon
DSN is stored as an SSM Parameter Store SecureString.

Scaffolded: **all four DAGs** — DAG 1 (CSV ETL), DAG 2 (API fan-out), DAG 3
(Payment), DAG 4 (order fulfillment + saga compensation).

DAG 2's mock service (callback-fetch) is **not** hosted in AWS — no long-running
container. Terraform creates the ECR repos + a least-privilege IAM user; you
build/push arm64 images (`scripts/build-push-mock-services.sh`) and deploy the
shared services to your **K3s** cluster (`shared-services/deploy/`, namespace
`orchestrators`), exposed at `*.example.com`. The service calls
`SendTaskSuccess` from K3s using the IAM user's key (a K8s Secret), resuming the
suspended `.waitForTaskToken` state. The SFN lambda reaches it at
`https://<mock_service_subdomain_prefix>callback-fetch.<mock_service_base_domain>`.

DAG 1 isolates its dynamic tables (`orders`/`customers`/`products`/
`combined_report`) in a dedicated `<BAKEOFF_NS>_dag1` Postgres schema (set in its
`db.py`) so they don't collide with DAG 3/4's transactional tables in the same
Neon database.

## Prerequisites

- Terraform >= 1.9, an AWS account, and a named AWS profile configured
  (`aws configure --profile <name>`). Export it as `AWS_PROFILE` — `.envrc.example`
  has a slot for it — and set `aws_profile` in `terraform.tfvars` to match.
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
  --profile "$AWS_PROFILE" \
  --state-machine-arn "$(terraform output -raw dag3_state_machine_arn)" \
  --input '{"payment_id":"PAY-1","amount":100,"currency":"USD","from_account":"ACC-001","to_account":"ACC-003"}'
```

The lambdas read the Neon DSN from SSM (`NEON_DB_PARAM` env var) — no `db_config`
is needed in the input anymore.

## Run DAG 1

The sample ZIP is seeded into the bucket at apply time.

```bash
aws stepfunctions start-execution \
  --profile "$AWS_PROFILE" \
  --state-machine-arn "$(terraform output -raw dag1_state_machine_arn)" \
  --input "{\"s3_bucket\":\"$(terraform output -raw dag1_bucket)\",\"zip_key\":\"$(terraform output -raw dag1_sample_zip_key)\"}"
```

Result: CSVs loaded into the `stepfunctions_dag1` schema in Neon, joined into
`stepfunctions_dag1.combined_report`, and written to
`s3://<bucket>/output/combined_report.parquet`.

## Run DAG 2

Requires the callback-fetch service deployed on K3s (see
`shared-services/deploy/README.md`) and reachable at
`https://<mock_service_subdomain_prefix>callback-fetch.<mock_service_base_domain>`. Point the DAG at any URL
returning a JSON list of items with `url` fields (each item's `url` is then
fetched in the fan-out). Keep the list small to avoid rate-limiting.

```bash
aws stepfunctions start-execution --profile "$AWS_PROFILE" \
  --state-machine-arn "$(terraform output -raw dag2_state_machine_arn)" \
  --input '{"url":"https://orch-fixture.example.com/books?per_page=5","request_config":{}}'
```

The submit lambda registers a task token with the K3s callback-fetch service;
with `AUTO_RESUME=true` the service fetches the URL and calls `SendTaskSuccess`
(using the IAM-user creds), resuming the workflow into the fan-out + combine steps.

## Run DAG 4

Requires the `approval` and `shipping` services on K3s. Use a unique `order_id`
each run (it's the orders PK). Items need `sku`, `quantity`, `unit_price`. A total
`>=` `approval_threshold` routes through manager approval (the approval service
auto-approves after ~10s via `AUTO_DECIDE_ACTION=approved`); below it skips
straight to shipping.

```bash
aws stepfunctions start-execution --profile "$AWS_PROFILE" \
  --state-machine-arn "$(terraform output -raw dag4_state_machine_arn)" \
  --name "dag4-$(uuidgen)" \
  --input '{"order_id":"ORD-'"$(uuidgen | cut -c1-8)"'","customer_id":"CUST-42","items":[{"sku":"GADGET-B","quantity":2,"unit_price":499.99}],"shipping_address":{"street":"123 Main St","city":"Springfield","state":"IL","zip":"01234"},"approval_threshold":500}'
```

Flow: ValidateOrder → ReserveInventory (sub) → ManagerApproval (sub, suspends on
task token until the approval service resumes it) → CallShippingAPI (sub) →
UpdateOrderShipped. Rejection/timeout or shipping failure triggers the saga
compensation path (ReleaseInventory → UpdateOrderCancelled). `shipping` is flaky
(70% success), so some runs exercise retries or `InvalidAddress` → compensation.

## Schema note

**Seeding Neon is a prerequisite** (2026-08-06). The lambdas now follow the
repo-wide `BAKEOFF_NS` convention: every connection pins `search_path` to
`<BAKEOFF_NS>_dagN`, defaulting to `stepfunctions`. DAG 1 self-creates its schema;
**DAG 3 and DAG 4 fail fast** with a `bootstrap_bakeoff` hint, because they need
seeded fixtures.

```bash
psql "$NEON_DATABASE_URL" -f ../../shared-services/init-db.sql   # defines bootstrap_bakeoff
psql "$NEON_DATABASE_URL" -c "SELECT bootstrap_bakeoff('stepfunctions');"
```

Why it matters here more than elsewhere: **Neon is shared with the Google
Workflows implementation**, which is the only other orchestrator needing a
publicly reachable Postgres. `stepfunctions_dag3.accounts` and
`google_workflows_dag3.accounts` hold independent balances; before this change
DAG 3/4 wrote flat `public.*` tables and the two would have debited the same rows.

Two leftovers from the migration, both harmless: the old `dag1_etl` schema is now
orphaned (DAG 1 uses `stepfunctions_dag1`), and the flat `public.*` tables DAG 3/4
used are no longer read. Drop them when you are confident nothing else wants them.

The `transactions` table in `shared-services/init-db.sql` was also reconciled to
match the DAG 3 code contract (`id`, `payment_id`, `from_account`/`to_account`,
`gateway_transaction_id`, `error_message`). A Neon seeded before that fix needs it
re-applied; the table has no seed data, so a drop is safe.

## Teardown

```bash
terraform destroy
```

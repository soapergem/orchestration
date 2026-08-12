# AWS Step Functions

Amazon States Language (ASL) JSON per DAG, with the step bodies as Lambda
functions. DAG 4 composes three sub-workflows, each its own state machine.

**Status: all four DAGs verified end-to-end on real AWS** (2026-08-12,
`us-east-1`, account `602436928406`), against Neon Postgres and the mock
services running on the arm64 OCI cluster. An earlier campaign on 2026-07-14 had
run all four as well; this is the first one with its evidence written down,
which is why this file exists. `CLAUDE.md` described Step Functions as "still
untested" until 2026-08-12 — it was the only orchestrator with no README, so
nothing in the repo contradicted the claim.

```
step-functions/
  dag1-csv-etl/          statemachine.asl.json + lambdas/
  dag2-api-fanout/
  dag3-payment/
  dag4-order-fulfillment/   + sub-workflows/ (reserve-inventory, manager-approval, shipping)
```

Infrastructure is `terraform/aws/` — Lambdas, IAM, S3, SSM, ECR. See that
README for what gets created.

---

## Launch

There is **no local path**. Step Functions is a managed service with no emulator
for these DAGs, so everything below runs against real AWS, a publicly reachable
Postgres (Neon), and publicly reachable mock services.

### 1. What has to exist first

| | Where | Notes |
|---|---|---|
| Lambdas + state machines | AWS | `terraform -chdir=terraform/aws apply` |
| Postgres | Neon | shared with Google Workflows; `$NEON_DATABASE_URL` |
| Mock services | K8s, publicly reachable | `RUNNING.md` §7c-i, `shared-services/deploy/` |
| `aws-resume-creds` Secret | same cluster as the mocks | **the step everyone forgets — see Findings** |

The lambdas read the Neon DSN from SSM (`NEON_DB_PARAM`), so no `db_config` goes
in the execution input.

### 2. Run them

`terraform output` is the documented way to get ARNs, but works only if you hold
the state (see Findings). These commands read from AWS directly and need
nothing but credentials:

```bash
SM() { aws stepfunctions list-state-machines \
  --query "stateMachines[?name=='orch-bakeoff-$1'].stateMachineArn" --output text; }

# DAG 1 -- ZIP is already seeded in the bucket
aws stepfunctions start-execution --state-machine-arn "$(SM dag1-csv-etl)" \
  --name "dag1-$(date +%s)" \
  --input '{"s3_bucket":"orch-bakeoff-dag1-602436928406","zip_key":"input/sample-data.zip"}'

# DAG 2 -- note BOTH the public host AND the base= override; see Findings
aws stepfunctions start-execution --state-machine-arn "$(SM dag2-api-fanout)" \
  --name "dag2-$(date +%s)" \
  --input '{"url":"https://orch-fixture.gemovationlabs.com/books?per_page=5&base=https://orch-fixture.gemovationlabs.com","request_config":{}}'

# DAG 3 -- payment_id doubles as the idempotency key, so vary it per run
aws stepfunctions start-execution --state-machine-arn "$(SM dag3-payment)" \
  --name "dag3-$(date +%s)" \
  --input '{"payment_id":"PAY-001","amount":100,"currency":"USD","from_account":"ACC-001","to_account":"ACC-003"}'

# DAG 4 -- 999.98 clears the 500 threshold, so this takes the approval path
aws stepfunctions start-execution --state-machine-arn "$(SM dag4-order-fulfillment)" \
  --name "dag4-$(date +%s)" \
  --input '{"order_id":"ORD-001","customer_id":"CUST-42","items":[{"sku":"GADGET-B","quantity":2,"unit_price":499.99}],"shipping_address":{"street":"123 Main St","city":"Springfield","state":"IL","zip":"01234"},"approval_threshold":500}'
```

`order_id` and `payment_id` are primary keys **and** idempotency keys — reuse one
and the run short-circuits as a duplicate rather than exercising the happy path.

---

## What this implementation demonstrates

- **ASL as data.** `Task`, `Choice`, `Map`, `Parallel`, `Retry`/`Catch`, `Fail`.
  The graph is JSON, validated at deploy time.
- **`.waitForTaskToken`** for both suspend points (DAG 2's callback, DAG 4's
  approval) — the execution genuinely suspends, no compute is billed.
- **Nested state machines** via `states:startExecution.sync:2` for DAG 4's three
  sub-workflows.
- **`Map` with `MaxConcurrency`** for the spec's fan-out caps.
- **`Retry` with `BackoffRate`/`JitterStrategy`** and `Catch` for
  retriable-vs-terminal classification.
- **One Lambda per step**, so dependency isolation is inherent — the 10/10 the
  rubric gives it is architectural, not configured.

---

## Findings

### The resume credentials are a separate, silent deployment step

This is the one that cost the most time, and it is worth reading before
deploying anywhere new.

Step Functions suspends on a **task token**. Resuming means somebody calls
`SendTaskSuccess`, and that somebody is the mock service holding the token — so
the mock services need AWS credentials. Terraform creates them: an IAM user
`orch-bakeoff-callback-resume`, an access key, and an inline policy scoped to
exactly `states:SendTaskSuccess` / `SendTaskFailure` / `SendTaskHeartbeat`. It
then exposes them as outputs `callback_resume_access_key_id` and
`callback_resume_secret_access_key`, described as *"put in the K8s
aws-resume-creds Secret"*.

**Nothing enforces that last step.** The Helm chart in `shared-services/deploy/`
wires the Secret into `callback-fetch` and `approval` via `secretKeyRef`, but
the arm64 cluster's mocks were deployed by `deploy-backbone.sh` (the §7c
in-cluster path, written for Argo and Flyte, which never needed AWS resume) — so
they had `google-resume-creds` and no AWS credentials at all. `helm list -n
orchestrators` on that cluster returns **nothing**, which is the quickest way to
tell the two deployment paths apart.

The failure is silent and misleading in both DAGs:

| | Symptom | What it looks like |
|---|---|---|
| DAG 2 | `TaskTimedOut` → `FanOutError` | the fetch service is broken |
| DAG 4 | `OrderCancelled`, "Order rejected or approval timed out" | approval was **rejected** |

Neither is true. `boto3` in the pod raised `NoCredentialsError`, the resume never
happened, and the token aged out. DAG 4's message is the most misleading thing
here — a credentials problem in a *different system* is reported as a business
decision. The tell is that the mock service's own log shows the request arriving
(`POST /fetch-async → 202`) and then nothing.

Diagnose it directly, from inside the pod:

```bash
kubectl exec -n orchestrators deploy/callback-fetch-service -c app -- \
  python -c "import boto3; print(boto3.client('sts',region_name='us-east-1').get_caller_identity()['Arn'])"
# want: arn:aws:iam::<acct>:user/orch-bakeoff-callback-resume
```

Silver lining: DAG 4's timeout path is the saga, so the failed run *did* prove
compensation works — order `cancelled`, reservation `released`, inventory
restored. It exercised the thing it looked like it was failing at.

### DAG 2 needs `base=` — and Step Functions is a third case

`CLAUDE.md` documents two: a **host-run** fan-out needs
`?base=http://localhost:8099`, an **in-cluster** one needs no override. Step
Functions is neither. The *collection* is fetched by callback-fetch-service (in
the cluster) but the *detail* URLs are fetched by a **Lambda in AWS**, and
fixture-service derives those URLs from the collection request — so it returned
`http://fixture-service:8099/books/OL1001044W`, a name that resolves only inside
the cluster. Every one of the 5 map iterations died with:

```
NameResolutionError: Failed to resolve 'fixture-service'
```

The fix is `&base=https://orch-fixture.gemovationlabs.com` — the *public* host,
because the consumer is outside the cluster entirely. Same run went 5/5 with 0
failures immediately after. Worth noting the rule generalises to "whatever can
reach the detail URLs", not "wherever the collection was fetched".

### `BAKEOFF_NS` — applied 2026-08-12, and the trap it left behind

The code gained `BAKEOFF_NS` on 2026-08-06 but was **not applied to AWS until
2026-08-12**, so for six days the live state machines wrote Neon's `public.*`
tables while the namespaced schemas sat empty.

That gap is a trap for anyone auditing whether Step Functions has ever run:
counting rows in `stepfunctions_dag*` returned zeros and read as "never
executed". The evidence was in `public.*` and `dag1_etl` the whole time. If you
are checking an orchestrator's history, confirm *which schema its deployed code
actually writes* before concluding anything from a row count.

Now applied and verified: DAG 3 wrote `PAY-NS-144256` to
`stepfunctions_dag3.transactions` (ACC-001 5000→4900 from clean fixtures), DAG 4
wrote `ORD-NS-144256` `shipped` to `stepfunctions_dag4.orders`, and **`public.*`
was untouched** — 0 rows matching either id, counts unchanged at 13 orders / 3
transactions. Step Functions and Google Workflows now share Neon with real
namespace isolation.

The `public.*` and `dag1_etl` schemas are historical: nothing writes them any
more, and they can be dropped once you no longer want the July/August evidence.

### Terraform state was missing, and that was the root cause of the credentials gap

`terraform/aws/` had **no state file and no remote backend** — `.terraform/` held
the provider plugins, but `terraform.tfstate` never existed locally (only
`terraform/gcp/` had one). The stack had been applied from a machine whose state
was lost.

That single fact explains the whole chain: no state → `terraform output -raw
callback_resume_secret_access_key` returns nothing → `deploy.sh` cannot create
the `aws-resume-creds` Secret → resume fails silently → DAG 2 and DAG 4 fail in
ways that look like application bugs. A missing state file presented as a
business-logic failure two systems away.

**Resolved 2026-08-12.** `versions.tf` now declares a **partial**
`backend "s3" {}` configured at init from a gitignored `backend.hcl` (template:
`backend.hcl.example`), and the existing deployment was adopted with **67
declarative `import` blocks** — `terraform plan` now reports *"No changes. Your
infrastructure matches the configuration"* across 87 resources. State holds the
Neon DSN in plaintext, so the bucket must be private, encrypted and versioned.

Four things learned doing the adoption, all of which will recur:

- **An access key's secret cannot be read back from AWS**, so importing
  `aws_iam_access_key` yields a null secret and an *empty*
  `callback_resume_secret_access_key` output — which `deploy.sh` would then write
  into the K8s Secret as an empty string. Excluding it from the imports and
  letting Terraform mint a fresh key keeps the output real (verified: 40 chars).
  The old key was deleted after the new one was confirmed working in-cluster.
  Before that, the July credential survived on the `rpi-local` cluster, whose
  `aws-resume-creds` Secret still held the live key id — the only reason the
  original was recoverable at all.
- **Not everything in the config exists in AWS.** The `fixture_reader` user and
  `aws_s3_object.books_corpus` were added after the original deployment, so they
  had to be *created*, not imported. A blind "import everything the plan wants to
  create" would have failed on both.
- **The layer build scripts hardcoded `pip3`**, which does not exist on a
  uv-managed machine; they now resolve `uv` first, translating
  `--platform manylinux2014_x86_64` to `--python-platform x86_64-manylinux2014`.
  Rebuilding changed `source_code_hash`, which forces new **immutable** layer
  versions — expect 2 replacements on any first apply from a new machine.
- **An interrupted apply leaves a lock.** The 136 MB pyarrow upload blew a
  2-minute shell timeout mid-apply; recovery is
  `terraform force-unlock <id>` (the id is inside
  `s3://<bucket>/<key>.tflock`), then re-apply. State was intact and the second
  run finished the remaining 6 changes.

### Everything else worth knowing

- **`terraform output` is load-bearing in the docs.** `terraform/aws/README.md`
  and `shared-services/deploy/README.md` both drive their commands from it, which
  makes them unusable without state. The `SM()` helper above reads AWS instead.
- **Executions are immutable.** Redrive is limited to a 14-day window, Standard
  workflows only, and the same definition — the basis of the 4/10 on resume.
- **Lambda logs are where the real errors are.** The execution history gives you
  `error` and `cause`; the stack trace lives in the `taskFailedEventDetails.cause`
  JSON blob, which is a string containing escaped JSON containing a stack trace.

---

## Verified behaviour (2026-08-12)

Fresh runs, all four green:

- **DAG 1** `dag1-fresh-130841` **SUCCEEDED** — 3 CSVs unzipped from S3 and
  loaded, joined into `dag1_etl.combined_report` (10 rows), Parquet written to
  `s3://orch-bakeoff-dag1-602436928406/output/combined_report.parquet` (3,644 B,
  timestamp confirms it was rewritten).
- **DAG 2** `dag2-base-131934` **SUCCEEDED** — suspended on the task token,
  resumed by callback-fetch-service, fanned out 5 items, **5 successful / 0
  failed**.
- **DAG 3** `dag3-fresh-130841` **SUCCEEDED** — `PAY-FRESH-130841`, $100.00
  `completed`; ACC-001 5000→4650 and ACC-003 250→350 across the day's runs.
- **DAG 4** `dag4-fix-131822` **SUCCEEDED** — `ORD-FIX-131822` through the
  approval path, ending `shipped` with tracking `1ZEDC639E125864885`,
  reservation `reserved`, GADGET-B 40→38.
- **DAG 4 saga compensation** `dag4-fresh-130905` **FAILED as designed** —
  approval timed out (the credentials gap above), order `cancelled` with
  `failure_reason`, reservation `released`, inventory returned to 40. Accidental,
  but a real compensation run.

## Not yet exercised

- **DAG 3's decline and retry-exhaustion branches.** Only the success path ran;
  the gateway's 20% timeout / 15% 5xx / 5% declined splits were not forced.
- **Rejection-triggered compensation.** The compensation seen was a *timeout*;
  an explicit `AUTO_DECIDE_ACTION=rejected` run would exercise the same code by
  the other trigger.
- **Shipping-failure compensation**, duplicate approval decisions, and the
  concurrent last-unit race for `RARE-D`.
- **`BAKEOFF_NS` isolation**, which cannot be tested until the terraform apply
  lands. After that, DAG 3/4 read `stepfunctions_dag{3,4}` — freshly reset
  2026-08-12 — instead of the drifted `public.*` tables.
- **Redrive.** No failed execution was redriven, so the 14-day/Standard-only
  constraints are documented rather than measured.

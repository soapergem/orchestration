# Workflow Orchestration

I need to put together a presentation that compares the following workflow orchestration systems:

1. [AWS Step Functions](https://aws.amazon.com/step-functions/)
2. [Apache Airflow](https://airflow.apache.org/)
3. [Argo Workflows](https://argoproj.github.io/workflows/)
4. [Dagster](https://dagster.io/)
5. [Temporal](https://temporal.io/)
6. [Kestra](https://kestra.io/)
7. [Prefect](https://www.prefect.io/)
8. [Flyte](https://flyte.org/)
9. [Luigi](https://github.com/spotify/luigi)
10. [Hatchet](https://hatchet.run/)
11. [Google Workflows](https://cloud.google.com/workflows)

I ultimately want to speak authoritatively on why workflow orchestration is valuable and what the point of these tools are. I also need to be able to compare and contrast each of these tools on the following metrics (and more):

* Programming language
* Templating language
* Does it support the dynamic creation of tasks during runs?
* How they're hosted (Kubernetes, self-hosted, managed service, etc.)
* How easy (or complex) dependency management is
* How retries are handled (i.e. can you resume from failure or do you need to start over?)
* Support for error handling
* Provides audit trail of past runs
* If there's a way to do local development
* How parallelization is handled
* Whether individual tasks can be suspended and resumed externally by async processes
* Community support
* Licensing / fees

To begin, we should create a table of all these orchestrators and how they compare on these different criteria. Once that is done, we will also need to create a few sample workflows for each one which I desire to do end-to-end testing on. Finally, we will put together a slideshow presentation about these results and make recommendations.

## Bake-off

When comparing these different orchestrators, we should create some real world example DAGs in each of them to see how they do. Each DAG is designed to exercise a different set of orchestration features, so that together they cover the full comparison criteria.

### DAG 1: CSV ETL Pipeline

Unzip a ZIP file containing CSVs, then do some processing on each CSV in parallel, load them into a Postgres database, have a step that runs some SQL transform on the combined data, and eventually convert things to Parquet files.

**Steps:**
1. **UnzipFile** — Download ZIP from S3, extract CSVs, upload back to S3
2. **ProcessCSVs** (parallel Map) — For each CSV, load into a Postgres table (max concurrency: 10)
3. **RunSQLTransform** — SQL JOIN across loaded tables into a combined report
4. **ConvertToParquet** — Read report from Postgres, write to S3 as Parquet

**Features tested:** parallel fan-out with dynamic task count, sequential dependencies, retries with backoff, timeouts, database integration, data transformation.

### DAG 2: API Fan-Out with Async Callback

Delegate an initial content fetch to an external web service via an **async callback pattern**, then fan out multiple additional API requests based on the response, and combine the results together. This tests whether the orchestrator can suspend a running workflow and resume it when an external system calls back.

**Steps:**
1. **SubmitAsyncFetch** — POST to the Callback Fetch Service with a `callback_url` (orchestrator-specific) and `correlation_id`. Receives `202 Accepted` immediately.
2. **WaitForFetchCallback** — Workflow **suspends** and waits for the fetch service to call back with the result. 60-second timeout.
3. **ProcessFetchResult** — Normalizes the callback payload into the standard format for downstream steps.
4. **CheckItemsExist** — Conditional branch: if items exist, proceed to fan-out; otherwise, end gracefully.
5. **FanOutAPIRequests** (parallel Map) — For each item, fetch detail from the item's URL (max concurrency: 20, with jitter-based retries).
6. **CombineResults** — Merge all detail responses into a summary with success/failure counts.

**How each orchestrator handles the async wait (Step 2):**

| Support Level | Orchestrators | Mechanism |
|---|---|---|
| Native callback/signal (workflow suspends, zero resource cost) | Step Functions, Temporal, Hatchet, Kestra, Google Workflows | Task token, signal, durable event wait, webhook pause, HTTP callback |
| Native suspend + API resume | Argo, Prefect, Flyte | Suspend node + API call to resume, `pause_flow_run`, `wait_for_input` |
| Deferrable operator (worker freed, trigger polls) | Airflow | Deferrable operator with trigger polling `/status` endpoint |
| Sensor/polling (separate mechanism) | Dagster | Sensor polls `/status` endpoint, triggers downstream job |
| Blocking poll (worker blocked, no suspend) | Luigi | Synchronous polling loop within the task |

Orchestrators that lack native callback support fall back to polling the fetch service's `GET /status/<correlation_id>` endpoint. This divergence is intentional — it highlights a real capability gap.

**Features tested:** async callback / suspend-resume by external process, parallel fan-out, conditional branching, retries with jitter, dynamic task count, error aggregation.

**Edge cases:** callback never arrives (timeout), callback with error payload, duplicate callback (idempotency), callback after timeout (must not resurrect workflow), fetch service unavailable on initial POST.

### DAG 3: Payment Processing

A single payment processing workflow: receive a payment request (amount, from/to accounts), validate the payment (check account exists, sufficient balance, basic fraud checks via DB queries), process the payment via a simulated external API call (with retry logic since payment APIs are flaky and must be idempotent to avoid double-charging), update the database with the result (debit/credit accounts, record transaction), and send a notification (simulate emailing a receipt or posting to a webhook).

**Steps:**
1. **ValidatePayment** — DB checks: account exists, active status, sufficient balance, duplicate check via idempotency key
2. **CheckValidation** — Conditional branch on validation result
3. **ProcessPayment** — Call simulated external payment gateway (60% success, 20% timeout, 15% 5xx, 5% declined). Retries with exponential backoff and jitter for retriable errors; `PaymentDeclined` is non-retriable.
4. **UpdateDatabase** — Idempotent debit/credit of accounts and transaction record
5. **SendNotification** — Best-effort success notification (failure is non-fatal)
6. **HandlePaymentFailure** — Records failed transaction, sends failure notification

**Features tested:** sophisticated error handling with specific error types, non-retriable error classification, conditional branching, idempotency, retries with exponential backoff + jitter + max delay cap, database integration, graceful degradation (notification failure doesn't fail the workflow).

### DAG 4: Order Fulfillment with Human Approval and Saga Compensation

An e-commerce order fulfillment pipeline that exercises suspend/resume by external process, saga compensation (true rollback of completed side effects), and sub-workflow composition. Inventory is reserved **before** approval, so both approval rejection and shipping failure require compensation — creating two distinct saga trigger paths.

**Steps:**
1. **ValidateOrder** — DB read: check all SKUs exist in inventory, customer account active, compute `total_amount`. No mutation; no compensation needed on failure.
2. **ReserveInventory** *(sub-workflow: InventoryReservationWorkflow)* — Atomically decrement `available_quantity` and create reservation records. Registers `ReleaseInventory` as its compensation action.
3. **CheckApprovalRequired** — Conditional branch: if `total_amount >= approval_threshold` (default $500), route to manager approval; otherwise skip to shipping.
4. **ManagerApproval** *(sub-workflow: ManagerApprovalWorkflow)* — Records approval request in DB, sends notification to manager with callback URL, then **suspends** waiting for external signal. Configurable timeout (120s for automated testing, 72h conceptually).
   - On **approval**: continue to shipping.
   - On **rejection or timeout**: trigger saga compensation.
5. **CallShippingAPI** *(sub-workflow: ShippingWorkflow)* — Call simulated shipping API with 3 retries + exponential backoff. `InvalidAddress` is non-retriable and fails immediately.
   - On **failure after retries**: trigger saga compensation.
6. **UpdateOrderStatus** — Set order status to `shipped` with tracking info.
7. **SendNotification** — Best-effort customer notification with tracking info. Failure is non-fatal.

**Saga compensation path** (triggered by approval rejection/timeout OR shipping failure):
1. **ReleaseInventory** — Reverse the reservation (idempotent: no-op if already released)
2. **UpdateOrderCancelled** — Set order status to `cancelled` or `failed` with reason
3. **SendCancellationNotification** — Best-effort cancellation notification
4. If compensation itself fails after retries → **CompensationFailed** dead-letter state requiring manual intervention

**Sub-workflows:**

| Sub-workflow | Purpose | Key feature tested |
|---|---|---|
| InventoryReservationWorkflow | Reserve inventory + provide compensation function | Sub-workflow composition, DB writes, compensation registration |
| ManagerApprovalWorkflow | Request approval, wait for external signal, record decision | External suspend/resume, long-running wait, timeout handling |
| ShippingWorkflow | Call shipping API with retries | External API, retry policies, non-retriable error classification |

**Features tested:** suspend/resume by external process (human approval), long-running durable waits, saga pattern with compensating transactions, sub-workflow / workflow composition, conditional branching, retries with backoff, non-retriable errors, idempotency, graceful degradation, dead-letter handling for compensation failures.

**Edge cases:**
- *Approval:* happy path, rejection → compensation, timeout → compensation, late signal after timeout, low-value order skips approval
- *Shipping:* success on retry, failure after retries → compensation, non-retriable `InvalidAddress` → immediate compensation
- *Saga:* successful compensation, partial compensation failure (one step fails but others complete), idempotent double-compensation, cascading cancellation of sub-workflows
- *Data integrity:* concurrent reservation for last inventory unit, input validation failures

### Feature Coverage Matrix

| Feature | DAG 1 | DAG 2 | DAG 3 | DAG 4 |
|---|---|---|---|---|
| Sequential execution | Yes | Yes | Yes | Yes |
| Parallel fan-out | Yes | Yes | - | - |
| Conditional branching | - | Yes | Yes | Yes |
| External API calls | - | Yes | Yes | Yes |
| Database operations | Yes | - | Yes | Yes |
| Retry with backoff | Yes | Yes | Yes | Yes |
| Error handling / catch | Yes | Yes | Yes | Yes |
| Non-retriable errors | - | - | Yes | Yes |
| Async callback / signal | - | Yes | - | Yes |
| Long-running external wait | - | - | - | Yes |
| Saga / compensation | - | - | - | Yes |
| Sub-workflow composition | - | - | - | Yes |
| Dynamic task creation (Map) | Yes | Yes | - | - |
| Idempotency | - | - | Yes | Yes |
| Graceful degradation | - | - | Yes | Yes |

## Shared Services

All DAGs rely on shared infrastructure that runs locally via Docker Compose. One `docker compose up` starts everything.

```
shared-services/
  callback-fetch-service/     # DAG 2: async content fetching with callback
    app.py                    # FastAPI app (~80-120 lines)
    Dockerfile
  approval-service/           # DAG 4: human approval simulation
    app.py                    # FastAPI app (~100-150 lines)
    Dockerfile
  shipping-service/           # DAG 4: flaky shipping API simulation
    app.py                    # FastAPI app
    Dockerfile
  docker-compose.yml          # All services + Postgres
  init-db.sql                 # All tables for DAGs 1-4
```

**Callback Fetch Service** (DAG 2):
- `POST /fetch-async` — Accepts URL + callback_url + correlation_id. Returns `202 Accepted`. Performs the actual HTTP fetch in a background task after a configurable delay (2-10s), then POSTs the result to the callback_url.
- `GET /status/<correlation_id>` — Polling fallback for orchestrators without native callback support. Returns `pending`, `completed` (with body), or `failed` (with error).

**Approval Service** (DAG 4):
- `POST /approval-requests` — Registers an approval request with callback_url.
- `POST /approval-requests/<id>/decide` — Simulates manager clicking Approve/Reject. POSTs decision to the callback_url.
- `GET /approval-requests/<id>` — Polling fallback.
- Auto-decide mode for automated testing: set `AUTO_DECIDE_DELAY_SECONDS=10` and `AUTO_DECIDE_ACTION=approved|rejected|none` via environment variables.

**Shipping Service** (DAG 4):
- `POST /shipments` — Simulated flaky shipping API. 70% success (returns tracking number), 15% timeout, 10% 5xx error, 5% `InvalidAddress` (non-retriable). Supports idempotency keys.

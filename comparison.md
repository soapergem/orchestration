# Workflow Orchestration Comparison

## Why Workflow Orchestration Matters

Workflow orchestration is the automated coordination, management, and execution of complex tasks and processes across distributed systems. It is valuable because:

- **Reliability**: Ensures multi-step processes complete successfully even when individual components fail, with built-in retries and error handling.
- **Visibility**: Provides audit trails, logging, and monitoring so teams can understand what happened, when, and why.
- **Dependency Management**: Automatically resolves task dependencies so steps execute in the correct order.
- **Scalability**: Distributes work across workers/containers, enabling parallel execution and horizontal scaling.
- **Reproducibility**: Codified workflows are version-controlled and repeatable, eliminating "it worked on my machine" problems.
- **Reduced Toil**: Eliminates manual coordination of batch jobs, ETL pipelines, ML training, infrastructure provisioning, and business processes.

---

## Comparison Table

| Criteria | AWS Step Functions | Apache Airflow | Argo Workflows | Dagster | Temporal | Kestra | Prefect | Flyte | Luigi | Hatchet | Google Workflows |
|---|---|---|---|---|---|---|---|---|---|---|---|
| **Primary Language** | Any (service-based) | Python | Any (container-based) | Python | Go (engine); Java, Python, TypeScript, PHP, .NET, Ruby (SDKs) | Java (engine); any (tasks) | Python | Python (SDK); Go (engine) | Python | Go (engine); Python, TypeScript, Go, Ruby (SDKs) | Any (service-based) |
| **Workflow Definition** | JSON/YAML (ASL) | Python code (DAGs) | YAML (K8s CRDs) | Python code (assets/ops) | Code (native SDK) | YAML (declarative) | Python code (decorators) | Python code (decorators) | Python code (classes) | Code (native SDK, decorators) | YAML |
| **Templating Language** | JSONPath, Intrinsic Functions | Jinja2 | Go templates | Python (native) | Go, Java, Python, TypeScript, PHP, .NET (native code) | Pebble (Jinja-like) | Python (native) | Python (native) | Python (native) | Python, TypeScript, Go, or Ruby (native code); CEL for event filtering | YAML with built-in `${}` expression syntax (proprietary) |
| **Dynamic Task Creation** | No (state machine definition is immutable at runtime; Map state iterates over data but cannot introduce new step types) | Partial (Dynamic Task Mapping in 2.3+ resolves fan-out cardinality at runtime, but the task template and DAG structure are fixed at parse time; cannot yield structurally new tasks) | Yes (`withParam` from a prior step's output dynamically creates task instances at runtime; recursive template invocation also supported) | Yes (`DynamicOut`/`DynamicOutput` lets an op yield arbitrary outputs at runtime; downstream ops are cloned per output via `.map()`) | Yes (fundamental model: no static DAG; workflow is imperative code that spawns activities and child workflows based on arbitrary runtime logic) | No (ForEach/ForEachItem iterate over dynamic data, but all task definitions are fixed in YAML before execution) | Yes (fundamental model: no static DAG; flows are imperative Python that call tasks dynamically based on runtime logic) | Yes (`@dynamic` decorator creates a sub-DAG at runtime from arbitrary Python logic, submitted to the engine for execution) | Yes (`yield` in `run()` dynamically adds new task dependencies to the graph at runtime; the task type and parameters are determined during execution) | Yes (imperative code model: workflows spawn child tasks and sub-workflows dynamically based on runtime logic) | No (YAML-based state machine; `for` loops iterate over data but cannot create new step definitions at runtime) |
| **Hosting Model** | Fully managed (AWS) | Self-hosted; Managed (Astronomer, MWAA, Cloud Composer) | Self-hosted on Kubernetes | Self-hosted; Managed (Dagster+) | Self-hosted; Managed (Temporal Cloud) | Self-hosted (Docker/K8s); Enterprise; Cloud | Self-hosted; Managed (Prefect Cloud) | Self-hosted on Kubernetes; Managed (Union.ai) | Self-hosted (Python process) | Self-hosted (Docker Compose/K8s); Managed (Hatchet Cloud); BYOC (Enterprise) | Fully managed (GCP) |
| **Kubernetes Native** | No (serverless) | No (can run on K8s) | Yes (CRD-based) | No (can deploy on K8s) | No (can run on K8s) | No (can run on K8s) | No (can run on K8s) | Yes (K8s-native) | No | No (can run on K8s; KEDA integration for autoscaling) | No (serverless) |
| **Ecosystem Dependency Isolation** | Inherent (serverless; each Lambda/ECS task has its own deployment package) | None by default (tasks share the worker's Python env); opt-in via `@task.virtualenv`, `@task.docker`, `KubernetesPodOperator`, or `KubernetesExecutor` | Inherent (each step is its own container/K8s pod with its own image) | Shared Python env by default (process-per-step, same venv); opt-in via separate code locations, Dagster Pipes, or Docker/K8s executors | Shared Worker process by default; isolate via separate Workers on different Task Queues, each with its own env/container | Inherent for script tasks (Docker container per task by default); built-in tasks run on JVM worker | Shared worker Python env by default; opt-in via Docker, K8s, ECS, or Cloud Run work pools (isolation is per flow run, not per task) | Inherent (each task is its own K8s pod); per-task images via `ImageSpec` for different dependency sets | None (all tasks run in the same process with no built-in isolation; no virtualenv/container support) | Shared worker process by default; isolate via separate Workers with different container images | Inherent (serverless; each invoked Cloud Run/Cloud Function has its own deployment) |
| **Can Tasks Have Conflicting Deps?** | Yes (each service target is independent) | No, not by default (shared env); Yes with opt-in isolation operators | Yes (different image per step) | No, not by default (shared env); Yes with code locations or Pipes | No, not by default (shared Worker); Yes with separate Workers | Yes (different Docker image per script task) | No, not by default (shared worker env); Yes with container-based work pools | Yes (different image per task via ImageSpec) | No (single shared process, no isolation mechanism) | No, not by default (shared worker process); Yes with separate Workers per task type | Yes (each service target is independent) |
| **Retry Handling** | Built-in retry with configurable backoff; can resume from failed state | Configurable retries per task; restart from failed task | Retry policies per step; resume from failed node | Configurable retries per op/asset; re-execute from failure | Automatic retries with configurable policies; resume from exact failure point (durable execution) | Configurable retries per task; restart from failure | Automatic retries with configurable policies; resume from failure | Automatic retries and checkpointing; resume from failure | Re-run from failure (idempotent targets) | Configurable retries per task with exponential backoff; `NonRetryableException` for bypass; manual replay from UI | Configurable retry policies per step |
| **Resume from Failure** | Limited (Redrive feature resumes from failed state, but Standard Workflows only, within 14 days, same definition, and event history <25K) | Yes (from failed task) | Yes (from failed node) | Yes (re-materialize failed assets) | Yes (exact point of failure - durable execution) | Yes (from failed task) | Yes (from failed task) | Yes (checkpointed recovery) | Partial (re-runs skip completed targets) | Yes (checkpoint-based replay for durable tasks; manual replay from UI for DAGs) | Yes (from failed step) |
| **Execution Durability** | Per-state checkpoint (each state transition persisted; resumes from the failed state within the machine, but a failed state itself restarts from scratch) | Per-task checkpoint (completed tasks tracked in metadata DB; no automatic state persistence between tasks) | Per-step checkpoint (completed containers tracked; K8s handles pod-level recovery) | Per-op/asset checkpoint (completed materializations tracked in DB; no mid-op persistence) | Event-sourced durable execution (every activity completion persisted; deterministic replay reconstructs exact mid-workflow state; developer writes normal code; survives process/host/cluster failure transparently) | Per-task checkpoint (completed tasks tracked; no mid-task persistence) | Per-task checkpoint (task results cached; no mid-task persistence) | Per-task checkpoint (task outputs persisted to blob store; intra-task checkpointing available for some ML task types) | Target-based idempotency (if output file/target exists, task is skipped; no state persistence beyond target existence) | Checkpoint-based durable execution (state persisted to PostgreSQL; durable tasks checkpoint at each step; replay from last checkpoint without re-executing completed work; at-least-once guarantee) | Per-step checkpoint (each step checkpointed to Spanner; survives zone failures; no mid-step recovery) |
| **Error Handling** | Catch/Retry states in ASL | `on_failure_callback`, try/except in tasks | `onExit`, retry strategies, DAG-level error handling | `@op` error handling, `RetryPolicy`, sensors | Try/catch in code, compensations, saga pattern | Built-in error handling, `allowFailure`, `errors` blocks | Try/except, state handlers, automations | Exception handling in tasks, error propagation | Python try/except in tasks | On-failure tasks, configurable timeouts (execution + scheduling), `cancel_if`/`skip_if` conditions, `NonRetryableException`, cancellation signals | Try/except/retry in YAML |
| **Audit Trail** | Excellent for Standard Workflows (interactive visual graph with per-state input/output inspection, 90-day retention). Poor for Express Workflows (no built-in history; must query CloudWatch Logs) | Excellent (Grid/Graph/Gantt views, per-task logs + XCom + rendered templates, code version per run, unlimited retention by default) | Moderate (DAG graph with status in UI, but pod logs do NOT survive pod deletion unless you set up external log aggregation; Workflow Archive optional for metadata persistence) | Excellent (structured event log with asset lineage, Gantt chart, per-op I/O inspection, unlimited retention by default) | Excellent (complete event-sourced history with per-Activity input/output, immutable and replayable; retention configurable per namespace, default 30 days on Cloud) | Very good (Gantt + Topology views, per-task logs/outputs/metrics, expression debugger; unlimited retention by default) | Good (per-task logs in UI, DAG graph, timeline; but less emphasis on structured I/O inspection; Cloud free tier retains logs only 7 days) | Very good (strongly typed I/O inspection per task, DAG graph, persistent blob outputs; but pod logs vanish without external aggregation) | Poor (24-hour default retention, no I/O inspection, no structured event log; experimental Task History feature requires opt-in DB config) | Good (durable PostgreSQL persistence, per-task logs via SDK, OpenTelemetry + Prometheus; but 1,000 log lines/task limit; audit logs only on Business+ with 30-day retention) | Moderate (execution status + final result in GCP Console, 90-day retention; no per-step I/O without explicit `sys.log()` calls; no visual execution graph) |
| **Local Development** | Yes (SAM CLI, Step Functions Local) | Yes (`airflow standalone`) | Yes (Minikube/k3d) | Yes (`dagster dev`) | Yes (`temporal server start-dev`) | Yes (Docker Compose) | Yes (`prefect server start`) | Yes (local sandbox, `pyflyte run`) | Yes (run Python scripts directly) | Yes (Docker Compose or Hatchet Lite single-container image; workers are standard app processes) | Yes (Cloud Code emulator) |
| **Worker Scaling Model** | No workers; delegates to external AWS services (Lambda, ECS, Batch, etc.) which scale independently | CeleryExecutor: always-on worker pool, scale by adding nodes. KubernetesExecutor: ephemeral pod per task, scales with cluster. Both require pre-provisioning. | Ephemeral K8s pod per task; scales with cluster capacity; no persistent workers | Ephemeral per run (K8s Job, Docker, ECS); ops within a run can also be ephemeral pods via K8s Job executor | Always-on, user-managed Workers that long-poll for tasks; scale by adding Worker processes; each handles many concurrent workflows | Always-on Workers with configurable thread pools; scale by adding instances. Enterprise Task Runners can offload to ephemeral containers (Docker, K8s, cloud batch) | Always-on Workers poll for work; flow runs execute as ephemeral infra (K8s pods, Docker, ECS, Cloud Run) depending on work pool type | Ephemeral K8s pod per task; scales with cluster capacity; no persistent workers | No separate workers; the client process that launches the workflow executes all tasks in-process. "Not meant to scale beyond tens of thousands" of tasks | Always-on Workers connect via gRPC; slot-based concurrency (default 100 slots/worker); horizontal scaling by adding workers; KEDA autoscaling support; managed compute option with sub-100ms provisioning | No workers; delegates to external GCP services (Cloud Run, Cloud Functions, etc.) which scale independently |
| **Parallelization** | Parallel state, Map state, Distributed Map (up to 10K) | Task-level parallelism via executor (Celery, K8s, Local) | DAG-based parallelism, native K8s scheduling | Multi-asset parallelism, configurable concurrency | Goroutine-based parallelism, child workflows, task queues | Parallel task execution, `each` construct | Concurrent task runs, `.map()` | Dynamic fan-out, map tasks, K8s parallelism | Limited (worker-based) | Automatic DAG parallelism, dynamic fan-out via child spawning (no fixed limit), bulk spawn, concurrency controls with rate limiting and priority scheduling | Parallel branches in YAML |
| **Suspend/Resume by External Process** | Yes (callbacks, task tokens for human approval) | Yes (sensors, external triggers, deferrable operators) | Yes (suspend/resume nodes) | Yes (sensors, run status API) | Yes (signals, queries, updates - first-class support) | Yes (pause/resume, webhook triggers) | Yes (pause/resume, automations) | Yes (launch plan signals, wait-for-input) | No (not natively supported) | Yes (durable event waits, webhook triggers, CEL filtering, durable sleep, or-groups for timeout+event combos) | Yes (HTTP callbacks, connectors, wait up to 1 year) |
| **Community & Ecosystem** | Large (AWS ecosystem, re:Post) | Very Large (Apache Foundation, 35K+ GitHub stars, huge plugin ecosystem) | Large (CNCF graduated, 15K+ GitHub stars) | Growing (10K+ GitHub stars, active Slack) | Large (18K+ GitHub stars, active Slack, enterprise adoption) | Growing (12K+ GitHub stars, 750+ contributors) | Large (21K+ GitHub stars, active Slack community) | Medium (6K+ GitHub stars, CNCF member, active Slack) | Legacy/Stable (18K+ GitHub stars, limited new development) | Growing (7K+ GitHub stars, active Discord, YC W24; created Dec 2023) | Medium (GCP ecosystem, Google support) |
| **Licensing / Fees** | Proprietary (AWS); Free Tier: 4,000 state transitions/month; Standard: $0.025/1K transitions; Express: $1.00/1M requests + $0.0000025/100ms duration | Apache 2.0 (free); Managed: Astronomer from ~$300/mo, MWAA from ~$0.49/hr, Cloud Composer from ~$0.35/hr | Apache 2.0 (free); No official managed offering | Apache 2.0 (free); Dagster+ (managed) starts free, paid tiers for teams/enterprise | MIT (core); Temporal Cloud: usage-based pricing (actions + storage); self-hosted is free | Apache 2.0 (free); Enterprise and Cloud tiers with premium features (pricing not public) | Apache 2.0 (free); Prefect Cloud: Free tier available, Pro and Enterprise paid tiers | Apache 2.0 (free); Union.ai managed: free tier, then usage-based pricing | Apache 2.0 (free); No managed offering | MIT (free); Hatchet Cloud: Free (100K runs/mo), Team $500/mo, Scale $1,000/mo, Enterprise custom | Proprietary (GCP); Free Tier: 5,000 steps/month; then $0.01/1K steps (internal), $0.025/1K steps (external) |
| **Best Suited For** | AWS-centric serverless orchestration | Data engineering, ETL pipelines | Container-native CI/CD and data pipelines on K8s | Data platform teams (ETL, ML, data quality) | Durable microservice orchestration, long-running processes | Cross-team orchestration, language-agnostic pipelines | Python-first data engineering and ML | ML/AI pipelines at scale on K8s | Simple batch job pipelines | Background task processing, AI agent orchestration, durable long-running workflows | GCP-centric lightweight orchestration |
| **Primary Focus** | General-purpose serverless orchestration | Data pipeline scheduling | Container orchestration on K8s | Data orchestration (asset-centric) | Durable execution / application orchestration | Universal orchestration | Workflow orchestration for data | ML/AI workflow orchestration | Batch pipeline management | Durable task queue and workflow engine (PostgreSQL-backed) | Serverless service orchestration |
| **Score** | 58 | 63 | 71 | 67 | 89 | 64 | 66 | 69 | 36 | 73 | 57 |

---

## Scoring Rubric

Each orchestrator is scored out of 100 across 11 weighted categories. Higher is better.

| Category | Weight | 0 pts | Partial | Full |
|---|---|---|---|---|
| **Language Flexibility** | 10 | Single language only | 2-3 languages or any-via-containers | 4+ native SDK languages |
| **Dynamic Task Creation** | 10 | No (immutable workflow definition) | Partial (dynamic fan-out cardinality but fixed task types) | Yes (true runtime DAG modification or imperative code model) |
| **Dependency Isolation** | 10 | No isolation, no mechanism available | Shared by default but opt-in isolation available | Inherent per-task isolation by design |
| **Execution Durability** | 15 | Target-based idempotency only | Per-task/step checkpoint | Event-sourced durable execution with transparent replay |
| **Resume from Failure** | 10 | Must restart entire workflow | Resume from failed task with caveats | Resume from exact point of failure, no constraints |
| **Audit Trail** | 10 | <24h retention, no I/O inspection | Logs available but limited retention or no per-step I/O | Per-step I/O inspection, visual tools, unlimited retention |
| **Scalability** | 10 | Single-process, documented scaling limits | Always-on workers, horizontal scaling | Ephemeral/serverless, auto-scaling, no worker management |
| **Vendor Independence** | 10 | Proprietary, single-cloud lock-in | Open source but requires specific infra (e.g., K8s only) | Open source, runs anywhere, no lock-in |
| **Community & Maturity** | 5 | Very new (<2 yrs) or in maintenance mode | Growing community, moderate adoption | Large active community, battle-tested at scale |
| **Local Dev Experience** | 5 | Requires cloud services or K8s locally | Docker Compose or emulator | Single command (`dev` server or run scripts directly) |
| **Suspend/Resume by External Process** | 5 | Not supported | Basic pause/resume or sensors | First-class signals, callbacks, durable event waits |

## Score Breakdown

| Category (weight) | Step Functions | Airflow | Argo | Dagster | Temporal | Kestra | Prefect | Flyte | Luigi | Hatchet | Google Workflows |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Language Flexibility (10) | 8 | 3 | 8 | 3 | 10 | 8 | 3 | 3 | 3 | 7 | 8 |
| Dynamic Task Creation (10) | 0 | 4 | 8 | 8 | 10 | 0 | 10 | 8 | 8 | 10 | 0 |
| Dependency Isolation (10) | 10 | 4 | 10 | 4 | 4 | 8 | 4 | 10 | 0 | 4 | 10 |
| Execution Durability (15) | 9 | 7 | 7 | 7 | 15 | 7 | 7 | 8 | 3 | 12 | 9 |
| Resume from Failure (10) | 4 | 7 | 8 | 8 | 10 | 7 | 7 | 8 | 3 | 8 | 7 |
| Audit Trail (10) | 7 | 9 | 5 | 9 | 9 | 8 | 6 | 7 | 1 | 6 | 4 |
| Scalability (10) | 10 | 6 | 8 | 7 | 7 | 7 | 7 | 8 | 1 | 7 | 10 |
| Vendor Independence (10) | 0 | 10 | 8 | 10 | 10 | 9 | 10 | 8 | 10 | 9 | 0 |
| Community & Maturity (5) | 4 | 5 | 4 | 3 | 4 | 3 | 4 | 3 | 2 | 1 | 3 |
| Local Dev Experience (5) | 2 | 4 | 2 | 5 | 5 | 3 | 5 | 3 | 5 | 4 | 2 |
| Suspend/Resume (5) | 4 | 4 | 3 | 3 | 5 | 4 | 3 | 3 | 0 | 5 | 4 |
| **Total (100)** | **58** | **63** | **71** | **67** | **89** | **64** | **66** | **69** | **36** | **73** | **57** |

### Scoring Rationale (Notable Decisions)

- **Step Functions (58)**: Strong on scalability (serverless) and dependency isolation, but zeroed out on vendor independence (pure AWS lock-in) and dynamic task creation (immutable state machine). Resume from failure scored low due to Redrive constraints (14-day window, Standard only, same definition).
- **Airflow (63)**: Excellent audit trail and community, but weak on language flexibility (Python only), dependency isolation (shared env by default), and only partial dynamic task creation.
- **Dagster (67)**: Excellent audit trail and dev experience, but Python-only and shared-env by default pull it down.
- **Kestra (64)**: Good isolation for script tasks and language flexibility via YAML+containers, but no dynamic task creation and slightly younger community.
- **Prefect (66)**: Excellent dynamic tasks (imperative Python) and dev experience, but Python-only and shared-env by default.
- **Flyte (69)**: Strong isolation (K8s-native) and dynamic tasks via `@dynamic`, but requires Kubernetes and has a smaller community.
- **Argo (71)**: Strong isolation and dynamic tasks via `withParam`, but audit trail is a weak spot (logs lost when pods are GC'd) and requires Kubernetes.
- **Hatchet (73)**: Strong durability (checkpoint-based, PostgreSQL-backed), multi-language, dynamic tasks, and first-class suspend/resume. Deducted for very young project (Dec 2023), shared-env by default, and log line limits.
- **Temporal (89)**: Highest score. Event-sourced durable execution is unmatched (15/15). Multi-language SDKs, true dynamic tasks (imperative code), first-class signals, and open source. Main deductions: shared worker env by default, and requires some infra management.
- **Google Workflows (57)**: Similar to Step Functions - strong serverless scalability and isolation, but zeroed on vendor lock-in and dynamic tasks. Weaker audit trail than Step Functions (no per-step I/O without manual logging).
- **Luigi (36)**: Lowest score. No dependency isolation, no suspend/resume, poor audit trail, poor scalability, and in maintenance mode. Dynamic task creation via `yield` and vendor independence are its only real strengths.

---

## Key Differentiators Summary

### AWS Step Functions
- **Strengths**: Zero infrastructure management, deep AWS integration (220+ services), visual Workflow Studio, Distributed Map for massive parallelism.
- **Weaknesses**: Vendor lock-in to AWS, ASL can be verbose, limited local dev experience.

### Apache Airflow
- **Strengths**: Industry standard for data pipelines, massive plugin ecosystem, Python-native, battle-tested at scale, strong community.
- **Weaknesses**: Scheduler can be a bottleneck, DAGs are not truly dynamic at runtime (Airflow 2.x improved this), UI can feel dated, complex to operate self-hosted. Ecosystem dependency management can be painful -- by default all tasks share the worker's Python environment, so conflicting dependency versions across tasks are impossible without opting into heavier isolation mechanisms (`@task.virtualenv`, `@task.docker`, `KubernetesPodOperator`, or `KubernetesExecutor`).

### Argo Workflows
- **Strengths**: Kubernetes-native (CRD), container-per-step isolation, excellent for CI/CD and data pipelines on K8s, CNCF project.
- **Weaknesses**: Requires Kubernetes expertise, YAML-heavy, no built-in scheduling (needs Argo Events), steeper learning curve for non-K8s teams.

### Dagster
- **Strengths**: Asset-centric paradigm (data-aware), built-in data quality/lineage, excellent developer experience (`dagster dev`), type system for IO.
- **Weaknesses**: Newer ecosystem (fewer integrations than Airflow), asset model has a learning curve, smaller community.

### Temporal
- **Strengths**: Durable execution (survives any failure), multi-language SDKs, signals/queries for external interaction, ideal for microservice orchestration and long-running workflows.
- **Weaknesses**: Not data-pipeline focused (no built-in scheduling/sensors), requires understanding of deterministic constraints, operational complexity for self-hosted.

### Kestra
- **Strengths**: Declarative YAML (language-agnostic), 600+ plugins, built-in code editor, API-first, Terraform provider, good for cross-team adoption.
- **Weaknesses**: Younger project, smaller community than Airflow/Prefect, enterprise features require paid tier.

### Prefect
- **Strengths**: Pythonic (decorator-based), easy migration from scripts, excellent UI, hybrid execution model, strong community.
- **Weaknesses**: Python-only, breaking changes between v1 and v2/v3, some features cloud-only.

### Flyte
- **Strengths**: Kubernetes-native, strong typing with automatic serialization, built-in caching/versioning, excellent for ML pipelines, multi-tenancy.
- **Weaknesses**: Requires Kubernetes, smaller community, heavier infrastructure footprint, steep initial setup.

### Luigi
- **Strengths**: Simple and straightforward, battle-tested at Spotify, minimal dependencies, good for simple batch pipelines.
- **Weaknesses**: Limited features (no built-in retry/backfill UI, basic scheduler), largely in maintenance mode, no managed offering, lacks modern orchestration features.

### Hatchet
- **Strengths**: Simple PostgreSQL-only architecture (easy to self-host, no Cassandra/Redis/Kafka), durable checkpoint-based execution, one-off background tasks are first-class (no workflow boilerplate needed), multi-language SDKs (Python, TypeScript, Go, Ruby), built-in observability (OpenTelemetry, Prometheus, logging, UI), flexible concurrency controls (rate limiting, priority scheduling, GROUP_ROUND_ROBIN), MIT licensed with no source-available restrictions, no fixed fan-out limits.
- **Weaknesses**: Very young project (created Dec 2023, still pre-1.0), smaller community (~7K stars), no per-task dependency isolation without separate workers, PostgreSQL-only backend limits flexibility, not Kubernetes-native (no CRDs/operator), fewer SDKs than Temporal.

### Google Workflows
- **Strengths**: Fully managed serverless, deep GCP integration, pay-per-use, HTTP callbacks (wait up to 1 year), fast deploys.
- **Weaknesses**: Vendor lock-in to GCP, limited community, YAML-only syntax, fewer features than other orchestrators, no open-source option.

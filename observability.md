# Observability Deep Dive: Temporal, Hatchet, Argo Workflows, Step Functions

A focused comparison of auditing, alerting, and observability across our four shortlisted orchestrators.

---

## Audit Trail

| Aspect | Temporal | Hatchet | Argo Workflows | Step Functions |
|---|---|---|---|---|
| **What is recorded** | Complete event-sourced history: every Activity scheduled/started/completed/failed, every Signal, Timer, Child Workflow event, with full input/output payloads | Workflow and task execution state, inputs, outputs, retries, and state transitions in PostgreSQL | Workflow CR status with per-node phase, inputs, outputs, parameters, artifacts, timestamps; stored as K8s CRD in etcd | Per-state-transition event history with input/output JSON for every state; stored internally by the service |
| **Per-step I/O inspection** | Yes - Activity inputs (`ActivityTaskScheduled.input`) and outputs (`ActivityTaskCompleted.result`) visible in UI and via API | Data is stored in PostgreSQL and accessible via API; UI inspection granularity not prominently documented | Yes - node inputs/outputs/parameters visible in UI and via `argo get`; artifacts preserved if configured | Yes - interactive Step Details pane shows input/output per state with Advanced view showing data flow through InputPath/Parameters/ResultSelector/OutputPath |
| **Visual execution view** | Timeline/Compact/JSON views of event history; no DAG graph (imperative model) | DAG structure for workflow steps with status | DAG graph with color-coded node status; click into nodes for details | Interactive visual graph with color-coded states + Table view with Timeline column showing duration segments |
| **Who-did-what audit** | Principal Attribution on events (user email, service account, mTLS CN); available on Cloud and self-hosted with config | Audit logs on Business+ plans: actor, action, resource, IP, user agent, timestamp. Only covers token/member management and run creation; 30-day retention | No built-in principal attribution; relies on K8s RBAC audit logging | CloudTrail logs API-level calls (CreateStateMachine, StartExecution, etc.) with IAM principal, source IP, timestamp |
| **Default retention** | Configurable per namespace: 3 days (self-hosted default), 30 days (Cloud default), max 90 days on Cloud | 30 days for run data (self-hosted default, configurable); Cloud: 3 days (Team), 7 days (Scale) | Workflow CRDs: configurable TTL; Workflow Archive (if enabled): configurable, default forever | 90 days (Standard); not stored for Express (must use CloudWatch Logs with separate retention) |
| **Retention limits** | Self-hosted v1.18+: unlimited; Cloud: 1-90 days; 51,200 events / 50 MB per execution | Self-hosted: configurable; Cloud Team: 3 days; Cloud Scale: 7 days | No platform limit; bounded by etcd/DB | 90 days max (hard); 25,000 events per execution (hard) |

---

## Alerting

| Aspect | Temporal | Hatchet | Argo Workflows | Step Functions |
|---|---|---|---|---|
| **Built-in failure alerting** | No. No native alerting system. | No documented built-in alerting. | No. No native notification system. | No built-in alerting, but native integration with CloudWatch Alarms and EventBridge |
| **Recommended alerting path** | Prometheus metrics -> Alertmanager/Grafana Alerts. Alert on `temporal_workflow_failed` or `temporal_cloud_v1_workflow_failed_count` | Prometheus metrics -> Alertmanager/Grafana (Enterprise tier only on Cloud). Or implement on-failure task handlers in code | Exit handlers and LifecycleHooks invoke notification templates (HTTP calls, Slack webhooks). Or custom Prometheus metrics -> Alertmanager | CloudWatch Alarm on `ExecutionsFailed` -> SNS -> email/Slack/PagerDuty. Or EventBridge rule on `Step Functions Execution Status Change` with status `FAILED` |
| **Notification integrations** | Via Prometheus/Datadog/Grafana alerting stack (no native Slack/PagerDuty) | Via Prometheus alerting stack or DIY on-failure tasks (no native Slack/PagerDuty) | Exit handlers can call any HTTP endpoint (Slack incoming webhook, PagerDuty Events API). LifecycleHooks can fire during execution, not just at completion | CloudWatch Alarms -> SNS (email, SMS, Lambda, HTTP). EventBridge -> SNS, SQS, Lambda, and 20+ other targets. Native AWS ecosystem |
| **Granularity** | Per-workflow-type, per-task-queue, per-activity-type via metric labels | Per-tenant via tenant-scoped metrics | Per-workflow via exit handlers; per-template via LifecycleHooks; per-metric via custom Prometheus metrics | Per-state-machine via CloudWatch dimensions; per-execution via EventBridge event payload (includes execution ARN, error, cause) |
| **Time to alert** | Depends on Prometheus scrape interval + Alertmanager evaluation (typically 30s-2min) | Same as Temporal (Prometheus-based) | Exit handlers run immediately at workflow completion; Prometheus-based alerting has typical lag | CloudWatch Alarms: 1-5 min evaluation period. EventBridge: near real-time (seconds) |

---

## Metrics & Monitoring

| Aspect | Temporal | Hatchet | Argo Workflows | Step Functions |
|---|---|---|---|---|
| **Metrics backend** | Prometheus (native), StatsD, M3, Datadog, OpenTelemetry Collector | Prometheus (self-hosted: all tiers; Cloud: Enterprise only) | Prometheus (native), OpenTelemetry (native) | CloudWatch Metrics (native, automatic) |
| **Key failure metrics** | `temporal_workflow_failed`, `temporal_activity_execution_failed`, `temporal_cloud_v1_workflow_failed_count` | `hatchet_failed_tasks_total`, `hatchet_tenant_failed_tasks` | `total_count{phase="Failed"}`, `error_count`, custom per-workflow metrics | `ExecutionsFailed`, `ExecutionsTimedOut`, `ExecutionsAborted` (all in `AWS/States` namespace) |
| **Key latency metrics** | `temporal_workflow_endtoend_latency`, `temporal_activity_schedule_to_start_latency`, `temporal_activity_execution_latency` | `hatchet_queued_to_assigned_time_seconds`, `hatchet_tenant_workflow_duration_milliseconds` | `queue_latency`, `queue_duration`, `operation_duration_seconds`, `k8s_request_duration` | `ExecutionTime`, `ServiceIntegrationRunTime`, `ActivityRunTime` |
| **Capacity/saturation metrics** | `temporal_worker_task_slots_available/used`, `temporal_cloud_v1_approximate_backlog_count` | `hatchet_tenant_used_worker_slots`, `hatchet_tenant_available_worker_slots` | `workers_busy_count`, `queue_depth_gauge`, `workflow_condition` | `OpenExecutionCount`, `ConsumedCapacity`, `ProvisionedBucketSize`, `ExecutionThrottled` |
| **Custom metrics** | Via SDK metric tags (workflow_type, activity_type, task_queue) | Via additional metadata key-value pairs on runs | Yes - define custom counter/gauge/histogram inline in workflow YAML with `metrics.prometheus` | No custom metrics within Step Functions; add via Lambda/application-level instrumentation |
| **Pre-built dashboards** | Community Grafana dashboards at `github.com/temporalio/dashboards` | No pre-built dashboards; extensive example PromQL queries in docs | Official Grafana dashboard at `grafana.com/grafana/dashboards/21393` | CloudWatch automatic dashboards; custom dashboards via CloudWatch console |

---

## Distributed Tracing

| Aspect | Temporal | Hatchet | Argo Workflows | Step Functions |
|---|---|---|---|---|
| **Tracing support** | OpenTelemetry via SDK interceptors (`TracingInterceptor`). Spans created for Client calls, Activities, and Workflow invocations. One trace per Workflow Execution. | OpenTelemetry via `HatchetInstrumentor`. Auto-creates spans for task execution, workflow triggers, event pushes. W3C `traceparent` propagation built-in. | OpenTelemetry (beta). Spans for workflow reconciliation, node phases, pod creation, artifact operations. Configured via `OTEL_EXPORTER_OTLP_*` env vars. | AWS X-Ray (native). Service map, end-to-end traces per execution, per-state segments. 30-day trace retention. |
| **Setup complexity** | Low - add interceptor to Client constructor | Low - call `HatchetInstrumentor()` | Low - set environment variables on controller | Low - checkbox in console or API parameter |
| **Trace propagation** | Across Activities, Child Workflows, Signals via SDK interceptors | Across tasks and child workflows via `additionalMetadata` W3C traceparent injection | Across workflow nodes via controller; user workload pods need separate OTel instrumentation | Across state transitions natively; Lambda functions need X-Ray SDK for downstream tracing |

---

## Logging

| Aspect | Temporal | Hatchet | Argo Workflows | Step Functions |
|---|---|---|---|---|
| **Application logs in UI** | No. UI shows event history, not application logs. Logs must go to external system (ELK, Datadog, etc.) filtered by Workflow ID/Run ID. | Yes. `context.log()` and Python `logging` module logs appear in the dashboard per task run. **Limited to 1,000 log lines per task.** | Yes, while pods exist. Logs viewable in UI per node. **Logs lost when pods are garbage-collected** unless `archiveLogs` is enabled or external aggregation configured. | Standard: execution history is the log (event-by-event). Express: requires CloudWatch Logs with configurable log level (ALL/ERROR/FATAL/OFF). |
| **Log correlation** | SDK auto-attaches Workflow ID, Run ID, Namespace, Task Queue, Activity Type to log entries | SDK auto-associates logs with the specific task run context | Pod logs correlated by pod name (which maps to workflow node); no structured correlation beyond K8s labels | CloudWatch Logs entries include execution ARN and state name; X-Ray traces link to execution |
| **Log persistence** | Depends on your logging infrastructure (not managed by Temporal) | Stored in Hatchet engine (PostgreSQL); subject to data retention limits (30 days default) | Ephemeral by default; `archiveLogs` saves to artifact repo (S3/GCS); docs explicitly say "we do not recommend relying on Argo to archive logs" | Standard: execution history retained 90 days. Express: CloudWatch Logs retention configurable (1 day to indefinite). Cost scales with log volume. |
| **External log integration** | SDK context fields enable filtering in any log system | No documented log export/forwarding to external systems | ConfigMap `links` feature adds buttons in UI pointing to external logging (Kibana, Grafana/Loki, etc.) | Native CloudWatch Logs integration; can forward to S3, Elasticsearch, Datadog via subscription filters |

---

## Summary Comparison

| Capability | Temporal | Hatchet | Argo Workflows | Step Functions |
|---|---|---|---|---|
| **Audit trail quality** | Excellent (event-sourced, immutable, per-Activity I/O) | Good (durable PostgreSQL, but UI inspection and audit log scope are limited) | Moderate (workflow metadata excellent, but logs are fragile without extra setup) | Excellent for Standard (per-state I/O, visual graph); Poor for Express |
| **Alerting ease** | Requires Prometheus/Grafana stack setup | Requires Prometheus stack (Enterprise on Cloud) or DIY on-failure tasks | Flexible via exit handlers + LifecycleHooks, but requires implementation | Easiest - native CloudWatch Alarms + EventBridge, near-zero setup |
| **Metrics richness** | Very rich (3 sources: Cloud, SDK, Server; dozens of metrics with fine-grained labels) | Good (global + tenant + worker metrics; Cloud Enterprise only) | Good (controller metrics, custom per-workflow metrics, K8s API metrics) | Good (execution + service integration + activity metrics; no custom metrics) |
| **Tracing** | OpenTelemetry (first-class) | OpenTelemetry (first-class) | OpenTelemetry (beta) | X-Ray (native, mature) |
| **Log experience** | No logs in UI; must use external system | Logs in UI but 1,000 line limit; no export to external systems | Logs in UI but ephemeral; must set up external aggregation for persistence | Execution history IS the log for Standard; CloudWatch Logs for Express (cost scales with volume) |
| **Vendor lock-in for observability** | None (Prometheus/OTel are open standards) | Low (Prometheus/OTel are open standards; but Cloud metrics Enterprise-only) | None (Prometheus/OTel are open standards) | High (CloudWatch, X-Ray, EventBridge are all AWS-specific) |
| **Biggest observability gap** | No application logs in the UI | 1,000 log lines/task; Prometheus on Cloud is Enterprise-only; audit log scope is narrow | Pod logs lost on GC without external setup | Express Workflows have no built-in history; 25K event limit on Standard; 90-day retention |

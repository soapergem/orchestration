# Kruxia Flow engine defects — reported upstream

The defects found while implementing these DAGs are **Kruxia Flow's feedback,
not the bake-off's**, so the detailed reports — minimal reproductions, root
causes, suggested fixes and a suggested regression test — went to the Kruxia
Flow maintainers rather than into this repo. This file is the summary, so a
reader here knows what was found and why three of the four DAGs are unverified.

Each finding is traceable in the public source
([`kruxia/kruxiaflow`](https://github.com/kruxia/kruxiaflow) at `75f9a77`); the
lab notebook in [README.md](README.md) carries the full reproduction for each.

| # | Severity | Finding | Where it is in the source |
|---|---|---|---|
| 1 | **Blocker** | **`wait_for_signal` cannot complete** — a signalled activity always fails with `Subscription already exists`. The scheduling branch subscribes unconditionally and the signal path never consumes the subscription; `delete_subscription` is called only by the timeout sweeper. Reproduces on 0.8.0 and 0.8.3. | [`orchestrator.rs:1522-1535`](https://github.com/kruxia/kruxiaflow/blob/75f9a77/core/src/orchestrator/orchestrator.rs#L1522-L1535), [`:2880`](https://github.com/kruxia/kruxiaflow/blob/75f9a77/core/src/orchestrator/orchestrator.rs#L2880) |
| 2 | High | **`{{SIGNAL.*}}` in a waiting activity's own parameters hangs the workflow forever.** Params resolve before the wait, strict-mode templating errors, the `WorkflowCreated` event poisons after 5 retries — and carries no `activity_key`, so the failure is never published. Status stays `created` with an empty activity list and a null error. | [`template.rs:324`](https://github.com/kruxia/kruxiaflow/blob/75f9a77/core/src/workflow/template.rs#L324) |
| 3 | High | **`postgres_query` returns silent `null` for NUMERIC and TIMESTAMPTZ.** `row_to_json` is a try-cast ladder ending in `else { Value::Null }`, so an undecodable type is indistinguishable from a genuine SQL NULL. Money and timestamps, on a green run. | [`postgres.rs:186-282`](https://github.com/kruxia/kruxiaflow/blob/75f9a77/worker/src/activities/postgres.rs#L186-L282) |
| 4 | High | **`kruxia/kruxiaflow:latest` is 0.3.0** (2026-02-07) and predates `--insecure-dev`, so the project's own published quickstart cannot work and fails looking like an auth problem. `0.7.0`/`0.8.0`/`0.8.3` all exist. | — (Docker Hub tag) |
| 5 | Medium | **`POST /signal` answers HTTP 200 when the signal was dropped**, returning `{"signaled": false}`. Signals are not buffered, so one sent before the activity is `waiting` is discarded — and a caller checking the status code sees success. | [`postgres_subscription.rs:64`](https://github.com/kruxia/kruxiaflow/blob/75f9a77/core/src/subscription/postgres_subscription.rs#L64) |
| 6 | Feature | Declarative gaps: no OR-join, no per-activity `optional`/`allowFailure`, no retry jitter, `http_request` cannot participate in a retry policy, and custom-worker logs are captured nowhere. | [`definition.rs:874`](https://github.com/kruxia/kruxiaflow/blob/75f9a77/core/src/workflow/definition.rs#L874), [`http.rs:364`](https://github.com/kruxia/kruxiaflow/blob/75f9a77/worker/src/activities/http.rs#L364) |

## Why this matters to the bake-off

Finding 1 is why **DAG 2 could not be implemented at all** and **DAG 4 was never
run** — both depend on suspending a workflow and resuming it from an external
process. Finding 3 is why every money column in DAG 3 and DAG 4 is cast
`::float8` and every timestamp `::text`.

Findings 1, 2 and 5 are also the reason the provisional score in
[../kruxiaflow-comparison.md](../kruxiaflow-comparison.md) is given twice — as-is
and with the blocker fixed. Scoring a tool's best category at zero because of a
defect that is a few lines from fixed would be accurate about today and
misleading about the design.

## What stays in this repo

The *bake-off's* own findings — how Kruxia Flow scores, how it compares, and what
the implementation cost — are evaluation artefacts:

- **[README.md](README.md)** — the lab notebook: twelve findings with the
  evidence that produced them, the DAG 3 results table, and what was not
  verified and why.
- **[../kruxiaflow-comparison.md](../kruxiaflow-comparison.md)** — the
  head-to-head against Temporal and Conductor, with provisional scoring.

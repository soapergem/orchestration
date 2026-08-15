# Kruxia Flow vs. Temporal and Conductor

A head-to-head against two of the twelve orchestrators already evaluated in
`comparison.md`, rather than a full thirteenth column. **Temporal (88)** is the
*category* comparison — Kruxia Flow's own README places it "in the same category
as Temporal and Inngest" — and **Conductor (75)** is the *mechanism* comparison,
because architecturally the two are near-twins.

**Evaluated:** Kruxia Flow 0.8.3, source read at `75f9a77`, 2026-08-14.

> ## Status of this evaluation
>
> **DAG 3 is fully verified. DAG 2 and DAG 4 are blocked by an engine defect and
> DAG 1 was not reached.** `wait_for_signal` — the suspend/resume primitive — 
> cannot complete: a signalled activity always fails
> (filed upstream as `kruxiaflow-internal` →
> `docs/bugs/2026-08-14-wait-for-signal-resume-fails.md`). Testing stopped there rather
> than re-implementing DAG 2 and DAG 4 as polling workarounds, because that
> would have measured a workaround the tool does not advertise and converted its
> best category into its worst on the basis of a bug that is a few lines from
> fixed.
>
> Everything below is either **measured** (marked as such) or **read from
> source**. Nothing is inferred from marketing material. Scores are provisional
> and flagged where they depend on unrun code.

---

## What Kruxia Flow is

A single Rust binary well under 100 MB, plus PostgreSQL. The API server, orchestrator,
built-in worker pool and cost tracker are threads in one process; Postgres is the
event store, the activity queue, the definition registry and the blob store.
There is nothing else to run.

Workflows are YAML or JSON `POST`ed to `/api/v1/workflow_definitions` and
versioned server-side, idempotent by content hash. Activities form a directed
graph via `depends_on` / `dependency_of`; conditions are MiniJinja expressions
over prior activities' outputs. Custom code lives in external workers that poll
`/api/v1/workers/poll` — Rust and Python SDKs exist.

In this repo's taxonomy (`deployment.md`) it is **Model 3 — push a definition to
a control plane**, plus Model 2 for its workers. That is Conductor's shape
exactly.

Its actual differentiator is not in the bake-off rubric at all: **hard budgets
enforced before an LLM call, per-token cost tracking, and budget-aware model
fallback**. See "What the rubric cannot see" below.

---

## Against Conductor — the mechanism comparison

These two are the closest pair in the whole evaluation. Both push declarative
definitions into a server-side versioned metadata store; both use HTTP-polling
workers that need no inbound connectivity; both are Postgres-backed; both have
no authentication worth the name in their open-source form.

| | Kruxia Flow 0.8.3 | Conductor 3.31.0 |
|---|---|---|
| Definition | YAML/JSON, versioned server-side, idempotent by content hash | JSON, versioned server-side |
| Engine footprint | **one small static binary + Postgres**; "ready" logged at 0.996s | JVM container + Postgres; compose allows it a 60s healthcheck grace |
| Worker model | HTTP poll; Rust + Python SDKs | HTTP poll; six SDKs |
| Worker process cost | one process, asyncio | **one OS process per task type** (26 measured at 1.72 GB idle) |
| Sub-workflows | **none** — chaining is Deferred; fake it with HTTP + signal | `SUB_WORKFLOW` task, native |
| Dynamic fan-out | **none** — `parallel_for_each` is Proposed, unimplemented | `FORK_JOIN_DYNAMIC`, native |
| Suspend/resume | `wait_for_signal` + declarative `on_timeout` — **broken in 0.8.3** | `WAIT` task; one unauthenticated POST; works |
| Wait timeout | declarative `on_timeout: continue\|skip\|fail` — `continue` broken | sweeper-enforced, **fires late** (60s measured at 103s) |
| Retry classification | **worker-side `retryable` flag** — verified exact | `FAILED_WITH_TERMINAL_ERROR` |
| Retry jitter | **none** | none |
| Database work | **`postgres_query` / `postgres_transaction` with per-activity `db_url` and isolation level** | needs a worker |
| HTTP with retry | needs a custom worker (`http_request` never fails on non-2xx) | `HTTP` task |
| OR-join | **none** — `depends_on` is ANDed | `SWITCH` / `JOIN` |
| Optional step | **none** | `optional: true` |
| Auth (OSS) | OAuth2 by default, `--insecure-dev` to disable | **none whatsoever** |
| Audit trail | event-sourced in Postgres; no worker log capture | per-task I/O in UI; no worker stdout |

**Where Kruxia Flow is genuinely better.** The operational footprint is not a
close call: one static binary against a JVM, and a Python worker that is one
process rather than one per task type. Its **auth default is the right way
round** — Conductor OSS has no authentication at all, so anyone who can reach the
API can rewrite every definition; Kruxia Flow requires OAuth2 unless you opt out.
And the **database story is the best of any YAML-defined tool in the whole
evaluation**: DAG 3's five validation checks compiled to one `CASE` expression
and its debit/credit to one idempotent CTE under `serializable`, with zero custom
code. Conductor needs a worker for all of that.

**Where Conductor is ahead, decisively.** Two native constructs Kruxia Flow
simply lacks — `SUB_WORKFLOW` and `FORK_JOIN_DYNAMIC` — are the two that DAG 1,
DAG 2 and DAG 4 lean on hardest. Composition in Kruxia Flow means the parent
`POST`s to start a child, passes `{{WORKFLOW.id}}` so the child knows whom to
answer, and parks on a signal until the child signals back. That hand-built
mechanism loses four things a native construct gives free: the parent cannot see
the child's status, cannot cancel it, gets no cascading failure, and must be told
where to signal. And it is currently unusable anyway, because the return leg is
a signal.

**The honest summary:** Kruxia Flow is Conductor's architecture with a far better
runtime and a much thinner feature set, and right now with its central primitive
broken.

---

## Against Temporal — the category comparison

This is the comparison Kruxia Flow invites, and it is the harder one.

| | Kruxia Flow 0.8.3 | Temporal |
|---|---|---|
| Model | declarative graph, engine-evaluated | **imperative code, durably replayed** |
| Durability | event-sourced in Postgres; per-activity | **event-sourced with deterministic replay** — resumes mid-workflow |
| Dynamic tasks | **none** | inherent — the workflow *is* code |
| Sub-workflows | **none** | child workflows, native |
| Suspend/resume | `wait_for_signal` — **broken in 0.8.3** | signals; first-class; verified here incl. `kill -9` mid-approval |
| Error handling | condition on `{{dep.status == 'failed'}}`; workflow still ends `failed` | try/catch; a handled failure ends **`Completed`** |
| Languages | 2 SDKs (Rust, Python) | 6+ |
| Footprint | **1 binary + Postgres** | 3 compose services here (server, UI, worker) on the auto-setup image; heavier to self-host properly |
| Definition registry | **yes — you can ask "what is deployed?"** | **no registry at all** (0 in the visibility count) |
| Defects found here | 11 engine-level, 1 blocker | **0** |

**Where Kruxia Flow wins.** Operational footprint, again — Temporal self-hosted
is the heaviest control plane in this evaluation. And one genuine capability
Temporal lacks: **a definition registry**. `comparison.md` records that the
highest-scoring tool in the bake-off cannot answer "what workflows are deployed
here?", because the server learns a workflow type only when an execution of it
exists. Kruxia Flow can answer it, and versions the answer.

**Where Temporal wins, and it is not close.** Durable execution with
deterministic replay resumes mid-workflow at the exact point of failure;
Kruxia Flow checkpoints per activity. The workflow body being ordinary code means
dynamic fan-out and sub-workflows are not features to be implemented — they are
just code, and both are absent here. And the defect counts tell the real story:
Temporal produced **zero** defects across all four DAGs in this repo, against
eleven engine-level findings here including one blocker, in an evaluation that
did not even reach three of the four DAGs.

**The honest summary:** the categorical claim does not hold yet. Kruxia Flow is
in Temporal's *category* — durable execution with human-in-the-loop, not batch
scheduling — but its execution model is per-activity checkpointing over a static
graph, which is the same tier as Conductor and Argo, not Temporal's.

---

## Provisional scoring

Against the `comparison.md` rubric. **Provisional**: three of four DAGs were not
verified. The two figures reflect a real choice about how to score a defect.

| Category (weight) | As-is (0.8.3) | If issues 1-3 fixed | Note |
|---|---|---|---|
| Language Flexibility (10) | 4 | 4 | Rust + Python SDKs; any language could poll the worker API, but only two are supported |
| Dynamic Task Creation (10) | **0** | 0 | `parallel_for_each` Proposed, unimplemented; loops strictly sequential. Same as Step Functions and Kestra |
| Dependency Isolation (10) | 4 | 4 | Shared worker process; separate workers per type possible. Same as Temporal, Hatchet, Conductor |
| Execution Durability (10) | 6 | 6 | Event-sourced over Postgres, per-activity checkpoint. Not replay-based. Conductor's tier |
| Resume from Failure (10) | 7 | 7 | Resumes from the failed activity; no mid-activity persistence |
| Audit Trail (10) | 5 | 5 | Full event history in your own Postgres, unlimited retention — but no per-activity I/O view, no worker log capture, no UI beyond a cost dashboard |
| Scalability (10) | 6 | 6 | Always-on workers, horizontal; single-binary control plane unproven at scale |
| Vendor Independence (10) | **10** | 10 | Apache-2.0, one binary, your Postgres, runs air-gapped |
| Auth & SSO (5) | 2 | 2 | Built-in OAuth2 + own user table; no OIDC/SAML/SCIM delegation. Better than Conductor's nothing, same trap as Hatchet |
| Community & Maturity (5) | **1** | 1 | Pre-1.0, single steward, Discord-scale. The lowest here |
| Local Dev Experience (5) | 4 | 5 | One container, ready in <1s — would be 5 but the documented quickstart is broken by a stale `:latest` |
| Suspend/Resume (5) | **0** | **5** | Broken as shipped. Fixed, it would be the best in the field: declarative `on_timeout` is something neither Temporal nor Conductor offers |
| **Total (100)** | **49** | **55** | |

**How to read the two columns.** As-is, 49 places it above only Luigi (38). That
is a fair description of what ships today and an unfair description of the
design. Fixed, 55 puts it between Luigi and Step Functions (60) — still last
among serious tools, and the reasons are structural rather than buggy: no dynamic
task creation, no sub-workflows, two SDKs, and a pre-1.0 community.

**The rubric-independent observation** worth carrying into the deck: this is the
only tool in thirteen where the *headline feature broke in normal use*. Kestra
produced 18 defects and Conductor 15, but in both cases the primitives worked and
the defects were integration friction. Here, one of the four benchmark DAGs
cannot be written at all.

---

## Definition visibility (measured, not scored)

`comparison.md` counts, for each tool, how many workflows its UI/API lists after
deploying the same four DAGs. Kruxia Flow answers the question — which four of
the twelve cannot — but with the same caveat two others earned:

```
GET /api/v1/workflow_definitions  ->  18 distinct definitions
                                       5 real (DAG 3 + DAG 4 + its 3 children)
                                       1 smoke test
                                      12 throwaway probes
```

Registration is durable server state and **nothing garbage-collects it**. Every
experiment performed during this evaluation is still listed, exactly as Hatchet
showed 19 definitions for 9 real workflows and Kestra 13 for 7. Kruxia Flow ships
no reaper and the API exposes no delete for a definition, so the registry only
grows. Adopting it means owning that cleanup.

For the comparison table: a completed implementation would list **7** — four DAGs
plus DAG 4's three sub-workflows — the same as Step Functions and for the same
reason (no nesting construct, so every child is a top-level definition).

## What the rubric cannot see

Kruxia Flow's reason to exist is **cost-governed orchestration**: hard budgets
enforced *before* an LLM call (estimated against a published pricing catalogue),
per-token cost attribution per activity and per attempt, budget-aware model
fallback down an ordered list, and result caching. None of the four bake-off DAGs
has an LLM step, so **the rubric is structurally blind to all of it**, and the
score above says nothing about whether the tool is good at its actual job.

This matters for a recommendation in both directions:

- If you are orchestrating **LLM pipelines with a spend ceiling**, no other tool
  in this evaluation competes — you would build budget enforcement yourself on
  top of Temporal or Conductor, and the gateway alternatives cap API keys rather
  than workflows.
- If you are orchestrating **anything else**, the four DAGs are a fair test and
  the answer is currently no: dynamic fan-out and sub-workflow composition are
  table stakes that are absent, and the suspend primitive does not work.

The recommendation for the orchestration bake-off is therefore unchanged —
**Temporal (88)** — and Kruxia Flow should be presented as a promising but
pre-1.0 entrant evaluated at 0.8.3, with the reservation stated plainly.

---

## Reproducing this

```bash
just up kruxiaflow                    # engine on :8100
just seed kruxiaflow                  # kruxiaflow_dag{1,3,4} schemas
source kruxiaflow_bakeoff/env.sh
./kruxiaflow_bakeoff/deploy.sh
kruxiaflow_bakeoff/.venv/bin/python -u kruxiaflow_bakeoff/worker.py &
./kruxiaflow_bakeoff/start_workflow.py dag3 --force-outcome decline
```

On this machine `just` is not installed and podman has no VM, so each recipe was
run as its raw `docker compose` equivalent with `CONTAINER_RUNNER=docker`. Full
notes, all twelve findings, and the DAG 3 evidence table are in
`kruxiaflow_bakeoff/README.md`; the upstream-facing bug reports with minimal
reproductions were filed upstream in `kruxiaflow-internal` (branch
`bakeoff-findings-2026-08-14`); `kruxiaflow_bakeoff/UPSTREAM-ISSUES.md` indexes them.

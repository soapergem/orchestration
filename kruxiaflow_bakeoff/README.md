# Kruxia Flow — bake-off lab notebook

**Status: infrastructure and DAG 3 verified. DAG 2 and DAG 4 are BLOCKED by an
engine defect (finding 10) and DAG 1 was not reached. Evaluation stopped
deliberately at that point rather than measuring workarounds.**

> **The engine defects were reported upstream to the Kruxia Flow maintainers** —
> they are Kruxia Flow's feedback, not the bake-off's. See
> **[UPSTREAM-ISSUES.md](UPSTREAM-ISSUES.md)** for the summary. The head-to-head against Temporal and Conductor is in
> **[../kruxiaflow-comparison.md](../kruxiaflow-comparison.md)**.
>
> What stays in *this* file is the lab notebook: the findings as evidence for
> scoring, with the reproductions that produced them.

Kruxia Flow v0.8.3 — a Rust durable-execution engine, one binary plus PostgreSQL.
Source: [`kruxia/kruxiaflow`](https://github.com/kruxia/kruxiaflow) — every code
citation below links to that repo at commit `75f9a77`, the public HEAD when this
was written.

Unlike the other twelve entries, this one exists to be compared against **two**
tools rather than scored against all of them: **Temporal** (88 — the category
comparison, since Kruxia Flow's own README claims "the same category as Temporal
and Inngest") and **Conductor** (75 — the mechanism comparison, since the
architecture is nearly identical). See `../kruxiaflow-comparison.md` once Phase 5
lands.

## Launch

```bash
just up kruxiaflow                    # engine :8100 + backbone
just seed kruxiaflow                  # kruxiaflow_dag{1,3,4} schemas
source kruxiaflow_bakeoff/env.sh
./kruxiaflow_bakeoff/deploy.sh        # push definitions to the control plane
curl -s localhost:8100/health         # {"status":"ok"}
```

No token is needed: the engine runs with `KRUXIAFLOW_INSECURE_DEV=true`. That is
a local-evaluation choice — the product default is OAuth2 on every request.

## Tool idioms this implementation demonstrates

- **Model 3 deployment** — definitions are YAML *data* POSTed to
  `/api/v1/workflow_definitions` and versioned server-side, idempotent by content
  hash. The graph changes without redeploying the code that runs it.
- **`depends_on` / `dependency_of`** as the only graph vocabulary (never "edges"),
  normalised to `depends_on` at parse time; conditions are MiniJinja expressions.
- **`settings.wait_for_signal`** with a declarative `on_timeout: continue | skip |
  fail` — the suspend primitive, and the thing Conductor has no equivalent of.
- **`postgres_query` / `postgres_transaction`** with per-activity `db_url` and
  `isolation_level`, so transactional DB work needs no custom code at all.
- **Worker-side retriability** — `ActivityResult.error(retryable=False)` in a
  custom Python worker, because the engine's `RetryPolicy` has no error predicate.

## Findings

Numbered as they were established. 1-3 are Phase 0; the rest arrive with the DAGs.

### 1. `kruxia/kruxiaflow:latest` on Docker Hub is 0.3.0, six months stale

Verified 2026-08-14: `docker run --rm --entrypoint /kruxiaflow
kruxia/kruxiaflow:latest version` → **`Kruxia Flow 0.3.0 (2026-02-07)`**, against
a 0.8.3 source tree. The README states CI moves `:latest` only on release tags;
it evidently stopped moving after 0.3.0, while `0.7.0`, `0.8.0` and `0.8.3` tags
all exist.

This is not the usual "pin your images" hygiene note, because of what 0.3.0
lacks: **`--insecure-dev` does not exist in it** (absent from `serve --help`), so
it ignores `KRUXIAFLOW_INSECURE_DEV=true`, and every API call returns
`{"error":{"code":"UNAUTHORIZED"}}`. The project README's five-minute quickstart
— curl the compose file, `KRUXIAFLOW_INSECURE_DEV=true docker compose up -d`,
"that's it" — therefore **cannot work as written** for anyone starting today. The
failure presents as an auth problem, which points at credentials rather than at
the image tag.

0.3.0 is also amd64-only, so it runs under emulation on Apple Silicon. **0.8.3 is
a proper multi-arch index (arm64 + amd64)** and runs native — so the arch story is
good, and only the stale tag made it look otherwise.

*Report upstream.* The compose file here pins `0.8.3`.

### 2. `--insecure-dev` governs request auth, not boot requirements

`serve` hard-fails at startup without an OAuth RSA private key *and* a client
secret regardless of the flag ([`kruxiaflow/src/commands/serve.rs:298-306`](https://github.com/kruxia/kruxiaflow/blob/75f9a77/kruxiaflow/src/commands/serve.rs#L298-L306),
`.unwrap()` at :641). So the `keygen` one-shot is mandatory even in dev mode, and
"no auth needed" is true of the API and not of the deployment. The compose profile
mirrors upstream's `keygen` + `catalog` one-shots for this reason; both exit
immediately and are ordered by `service_completed_successfully`.

`--insecure-dev` additionally refuses a non-loopback bind unless
`--insecure-dev-allow-nonloopback` is *also* set — the supported container shape
is "bind 0.0.0.0 inside, publish to 127.0.0.1 outside", which is what the profile
does.

### 3. A declared `outputs:` name does not rename the activity's output

The smoke workflow declared `outputs: [{name: result}]` on an `echo` activity and
got back an output named **`echo`**, carrying the activity's own shape plus an
injected `_kruxiaflow_temp_dir` key. Declared output names appear to be a
*filter/contract* rather than a rename, so templates must reference the name the
activity actually emits. Minor, but it is the kind of thing that makes a
downstream `{{step.result}}` silently resolve to nothing.

### 4. `postgres_query` silently returns `null` for NUMERIC and TIMESTAMPTZ

The most serious finding so far, and the one to report first.

```
SELECT balance,            -- NUMERIC(12,2), actual value 5000.00
       balance::text,      -- works: "5000.00"
       balance::float8,    -- works: 5000.0
       1.5::numeric,       -- null
       NOW(),              -- null
       created_at,         -- TIMESTAMPTZ -> null
       42, true, 'x'       -- all fine
```

Verified against 0.8.3. **NUMERIC and TIMESTAMPTZ both come back as JSON
`null`** — no error, no warning, no log line.

Root cause is direct and structural: `row_to_json`
([`worker/src/activities/postgres.rs:186-282`](https://github.com/kruxia/kruxiaflow/blob/75f9a77/worker/src/activities/postgres.rs#L186-L282)) is a hand-written type ladder
trying `try_get::<T>` for UUID, String, i16, i32, i64, f32, f64, bool and
`serde_json::Value` in order, ending in

```rust
// Fallback: NULL or unsupported type
else { Value::Null }
```

That comment is the bug. **The fallback conflates "this column is SQL NULL"
with "I cannot decode this type"**, so an unsupported type is indistinguishable
from a genuine null at every layer above it. `sqlx` supports both types via
`rust_decimal`/`chrono` features; they simply are not in the ladder.

Why it matters more than the type list suggests: NUMERIC and TIMESTAMPTZ are
*money* and *time*. They are what a business workflow reads. A payment workflow
that reads `balance` to check sufficiency gets `null`, compares it, and takes
the wrong branch — silently, with a green run. This is precisely the failure
class this bake-off keeps finding across tools (Argo retrying a declined card,
Kestra's missing resume URL), and it is the worst-behaved instance so far
because it corrupts *data* rather than control flow.

The maintainers have previously fixed the same symptom on a different set of
types by extending this same ladder, so the fix pattern is established — the
ladder was simply never completed.

*Workaround used throughout this implementation*: cast every money column
`::float8` and every timestamp `::text` in the SQL, and do all arithmetic
server-side so a float never round-trips a stored balance.

*Report upstream.* Suggested fix: decode by `PgTypeInfo` rather than by
try-cast ladder, and make the fallback an **error** rather than `Null` — an
undecodable column should fail loudly, not look empty.

### 5. Failure paths are expressible, but a handled failure still fails the workflow

Probed directly, because the docs do not say. Given `A` that fails and two
dependents conditioned on `{{A.status == 'failed'}}` and `== 'completed'`:

| | |
|---|---|
| `A` | `failed` |
| failure-path dependent | **`completed`** — it runs |
| success-path dependent | `skipped` |
| **workflow** | **`failed`** |

So compensation and error handling *are* expressible — dependents of a failed
activity are scheduled, which is the thing that makes DAG 3's failure branch and
DAG 4's saga possible at all. Two consequences worth carrying into the
comparison:

1. **There is no way to handle a failure into success.** A saga that compensates
   perfectly still reports `failed`. "Compensated cleanly" and "broke and did
   nothing" are the same terminal status; distinguishing them means inspecting
   per-activity statuses or the database. Same shape as Conductor's
   `failureWorkflow`; the opposite of Step Functions' `Catch` (which can end
   `Succeeded`) and Temporal (catch the exception, return normally).
2. **A business rejection modelled as *data* ends `completed`.** DAG 3's
   validation branch returns a boolean rather than failing, so the rejected-payment
   run ends `completed` while the gateway-failure run ends `failed`. Both are
   correct; which one you get is an authoring choice, and the deck should say so.

### 6. No OR-join: multiple `depends_on` entries are ANDed

An activity with two `depends_on` entries waits for **both**. There is no
`any`/`or` join, so a single shared successor of two mutually exclusive branches
waits forever. The sanctioned answer is to write explicit `{{dep.status == ...}}`
conditions on each incoming edge — which guards an activity but does not
*converge* paths, so every convergence point must be duplicated per branch.

The practical cost in DAG 3: the failure notification exists **twice**
(`notify_validation_failure`, `notify_payment_failure`) because one shared
`notify_failure` could not be reached from either branch. Cheap here, but it
scales with the number of exclusive paths, and DAG 4 has more of them.

### 7. No per-activity `optional` / `allowFailure`

An activity either completes or fails, and failure propagates. There is no
equivalent of Conductor's `optional: true`, Kestra's `allowFailure` or Argo's
`continueOn`. The spec's graceful degradation ("a notification that fails must
not fail the payment") is therefore only achievable by **having the activity
return success while reporting failure in its output** — see
`worker.py::notify`, which returns `notification_status: failed` as a *value*.

Verified: forcing the notification to fail leaves the workflow `completed` and
the payment intact. But the mechanism is the activity lying about its outcome,
which means the engine's own history shows a clean run for something that
partially failed. That is a real audit-trail cost.

### 8. `RetryPolicy` has no jitter

`max_attempts`, `strategy: exponential|fixed`, `base_seconds`, `factor`,
`max_seconds` — and nothing else ([`core/src/workflow/definition.rs:874`](https://github.com/kruxia/kruxiaflow/blob/75f9a77/core/src/workflow/definition.rs#L874)). The
spec asks for backoff **plus jitter**; backoff here is deterministic, so N
workflows failing on the same downstream outage retry in lockstep. Luigi, of all
things, hand-rolls FULL jitter in application code and Kruxia Flow cannot express
it declaratively at all. Measured: 5 attempts took 48.6s, matching 3+6+12+24s
exactly, with no variance.

### 9. Custom-worker activity logs go nowhere by default

`ctx.logger.info(...)` inside a `py-bakeoff` activity produced **no output** —
the Python SDK installs no handler, and the engine does not capture worker stdout
into the workflow record either. The retry evidence in this notebook had to come
from querying `workflow_events` in the engine's own database. Same family as
Conductor ("worker stdout is not captured, only explicit task logs") but with no
task-log API to fall back on. Bears directly on the Audit Trail score.

### 10. `wait_for_signal` cannot complete: a signalled activity always fails

**The most serious finding in this evaluation.** Kruxia Flow's suspend/resume
primitive — the one the project README leads with ("Ask a human. Workflows
suspend on `wait_for_signal` … and resume days later, surviving restarts") —
does not work end to end.

Reproduced on the most vanilla case constructible, matching the README's own
example:

```yaml
activities:
  - key: await_approval
    settings:
      wait_for_signal: {event_name: approval, timeout_seconds: 600}
```

```
POST /api/v1/workflows/{id}/signal  ->  {"signaled": true, ...}
workflow status                     ->  failed
await_approval                      ->  failed
  "Failed to create signal subscription: Subscription already exists
   for workflow <id> activity await_approval"
```

Traced step by step against the database:

1. Activity parks in `waiting`; a row is created in
   `activity_event_subscriptions` with `signal_data = NULL`. Correct.
2. The signal is delivered: the API answers `signaled: true` and the row's
   `signal_data` becomes `{"approved": true}`. Correct.
3. The orchestrator re-evaluates the activity as ready and **re-enters the
   scheduling branch that creates the subscription**
   ([`core/src/orchestrator/orchestrator.rs:1522-1535`](https://github.com/kruxia/kruxiaflow/blob/75f9a77/core/src/orchestrator/orchestrator.rs#L1522-L1535)), which is an
   unconditional `if let Some(wait_settings) = …wait_for_signal { …
   create_subscription(…) }` with no check for an already-satisfied wait. The
   insert violates the uniqueness constraint and the activity fails.

**The signal-delivery path never consumes the subscription.**
`delete_subscription` is called from exactly one place — the *timeout* sweeper
at `orchestrator.rs:2880` — and never on the signal path. `get_signal_data`
exists on the trait but the scheduling branch never consults it.

Scope, tested rather than assumed:

| Variant | Result |
|---|---|
| `activity_name: echo` + `on_timeout` default (`fail`) | fails |
| gate with **no** `activity_name` (the README's shape) | fails |
| `on_timeout: skip`, then signalled | fails |
| `on_timeout: continue`, timing out | fails, same error |
| `on_timeout: skip`, timing out | **works** — activity `skipped` |
| `on_timeout: fail`, timing out | **works** — clean `SIGNAL_TIMEOUT` |
| engine **0.8.0** instead of 0.8.3 | fails identically — not a new regression |

So the only paths through `wait_for_signal` that work are the ones where the
activity **never runs**. Any actual resume fails.

Suggested fix: before creating a subscription, check for an existing one for
`(workflow_id, activity_key)`; if it exists and carries `signal_data`, delete it
and schedule the activity to the queue with that data attached, instead of
re-subscribing.

Consequences for this bake-off, which are severe:

* **DAG 2 cannot be implemented at all** — its entire purpose is suspend and
  resume by an external process.
* **DAG 4's approval step cannot work**, and neither can the chained
  sub-workflow shape, which signals the parent on child completion.
* The resume-broker `kruxiaflow` provider added to both mock services is correct
  and delivers (`signaled: true`); the engine then fails the activity anyway.

### 11. An activity that references `{{SIGNAL.*}}` in its own parameters hangs the workflow forever

Separate from finding 10, and a silent-hang of the worst kind.

A waiting activity's parameters are resolved **when it is scheduled — before the
wait**, so `{{SIGNAL.x}}` is undefined at that moment. MiniJinja runs in strict
mode, so resolution errors. The orchestrator retries the `WorkflowCreated` event
five times, declares it a poison message, and then logs:

```
Cannot publish ActivityFailed for poison event - no activity_key
  (event type: WorkflowCreated)
```

Because the poisoned event is `WorkflowCreated`, which carries no `activity_key`,
**no failure can be attributed to anything**. The workflow stays in status
`created` **forever**, with an empty `activities` array and `error_message:
null`. The API shows a workflow that looks like it simply has not started yet.
Only the engine's stderr reveals it.

Combined with finding 10 this is a catch-22: signal data is reachable only
through `{{SIGNAL.*}}`, referencing it in the waiting activity bricks the
workflow, and referencing it downstream is undefined there too — while the
resume that would populate it fails regardless.

## Verified so far

**Phase 0 — infrastructure (2026-08-14).** Engine 0.8.3 healthy on :8100; `serve
--migrate` created its own schema in a dedicated `kruxiaflow` database on the
shared Postgres; all seven built-in `std` activities registered (`echo`,
`http_request`, `postgres_query`, `postgres_transaction`, `llm_prompt`,
`embedding`, `email_send`); `kruxiaflow_dag{1,3,4}` schemas seeded and routed
correctly by `bakeoff-db.sh`; a workflow definition deployed, submitted and
completed with template substitution working (`{{INPUT.who}}`). Cold start to
"ready" was **under 1 second**.

**Phase 1 — DAG 3 payment (2026-08-14).** All six spec assertions verified
against engine 0.8.3, evidence from `workflow_events` in the engine database:

| Case | Result |
|---|---|
| Happy path | `completed` in ~2s; $100 moved ACC-001 → ACC-003, transaction recorded |
| Non-retriable decline | `PaymentDeclined` → **1 attempt**, `retryable=false`, whole run 2.1s |
| Retriable 5xx | **exactly 5 attempts**, `will_retry` true×4 then false, 48.6s = 3+6+12+24s |
| Validation rejection | suspended ACC-004 → validation branch, `process_payment` skipped, ends `completed` |
| Duplicate idempotency key | second run rejected as duplicate; **no second debit, gateway never called** |
| Graceful degradation | forced notification failure → workflow still `completed`, payment intact |

**Zero authoring defects in the DAG itself** — `dag3_payment.yaml` worked as
written on its first run, and every fix in this phase was either mine
(an empty-array expansion under `set -u` in `deploy.sh`, macOS bash 3.2) or
designed around a pre-existing engine defect found by probing first (finding 4).
That is a Temporal-like result, and the reason is the same: the parts that would
normally be subtly wrong — retry semantics, transactionality, branch evaluation —
are the engine's job here, and it does them.

The DAG is worth reading as evidence for one specific claim: **all of DAG 3's
database work is declarative**. Validation is a single `postgres_query` whose
CASE expression does all five checks the other implementations do as five Python
round trips, and the money movement is one idempotent CTE inside a
`serializable` `postgres_transaction` — the INSERT claims the idempotency key
and both balance updates are gated on it having happened, so a replay cannot
double-debit. No other YAML-defined tool in this bake-off can do that without a
custom worker.

## Not verified, and why

**DAG 2 — blocked, cannot be implemented.** Its entire purpose is suspending a
workflow and resuming it from an external process. Finding 10 means that path
cannot complete. There is no honest version of DAG 2 on stock 0.8.3.

**DAG 4 — written, blocked before running.** `dag4_order_fulfillment.yaml` and
all three `subflows/` are complete and deploy cleanly, but they have **never
been run**, because both the approval step and the child→parent return leg
depend on `wait_for_signal`. Treat those four files as a *design record* — they
show what composition and saga compensation have to look like in a tool with no
sub-workflow construct and no OR-join — not as verified code. Expect defects in
them; on the evidence of every other tool in this bake-off, four unrun workflow
definitions contain several.

The flat single-definition variant of DAG 4, which was to be built alongside the
chained one to measure what the missing sub-workflow construct costs, was not
started.

**DAG 1 — not reached.** No blocker known; it needs the `script` activity in a
`py-std` worker plus the in-process fan-out fallback (the engine has no dynamic
fan-out — see finding 12 below). It is the least interesting of the four for
this comparison because it exercises the fewest orchestration primitives.

### 12. No dynamic fan-out (established from source, not from a run)

There are zero references to any fan-out construct in the Rust tree and no such
field on `ActivityDefinition` ([`core/src/workflow/definition.rs:632`](https://github.com/kruxia/kruxiaflow/blob/75f9a77/core/src/workflow/definition.rs#L632)). Parallelism is static — sibling
activities with no dependency between them run concurrently — and back-edge
loops are strictly sequential, with **"Parallel iterations: Multiple iterations
executing simultaneously"** listed under Future Enhancements (Post-MVP) in
[`docs/loops-guide.md`](https://github.com/kruxia/kruxiaflow/blob/75f9a77/docs/loops-guide.md#future-enhancements-post-mvp).

So DAG 1's per-CSV map and DAG 2's 30-item fan-out have no orchestrator-level
expression at all. The honest fallback is one activity doing the fan-out
in-process under its own concurrency cap, which is a single opaque step to the
engine. This is the Step Functions / Kestra row of the rubric and it should be
scored 0 for Dynamic Task Creation.

## Stopping decision

The evaluation was stopped after finding 10 rather than continuing with polling
fallbacks. The reasoning, recorded because it affects how the scores should be
read: implementing DAG 2 and DAG 4 by polling would have measured a **workaround
this tool does not advertise**, and would have converted Kruxia Flow's single
best rubric category into its worst on the basis of a bug that is a few lines
from fixed. Neither owner is served by that number. The capability assessment
below is therefore made from source plus the primitives that *were* exercised,
and is explicitly marked provisional where it depends on unrun code.

## Environment notes

This machine has **no `just` and no initialised podman VM** (Docker Desktop is the
live runtime), so every command above was run as its raw `docker compose`
equivalent with `CONTAINER_RUNNER=docker`. The `Justfile` auto-detects
`finch > podman > docker` on *binary presence* alone, and podman is installed but
has no VM — so the detection picks a runtime that cannot connect. Export
`CONTAINER_RUNNER=docker` here.

Separately, `shared-services/init-runners.sh` was mode 644 and Docker Desktop
presents bind-mounted files as executable, so the Postgres entrypoint tried to
*exec* rather than *source* it and died with `/bin/bash: bad interpreter:
Permission denied`. The result was a database with **no bake-off schemas for any
runner** and no error surfaced by `up`. Fixed by making the file executable,
which is correct under both runtimes (it already ends `return 0 2>/dev/null ||
exit 0` to work either way). Pre-existing, not specific to Kruxia Flow.

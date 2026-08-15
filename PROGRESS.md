# PROGRESS — adding Kruxia Flow to the bake-off

Branch: `kruxiaflow`. Plan: `~/.claude/plans/breezy-petting-snail.md`.
Lab notebook: `kruxiaflow_bakeoff/README.md`.

Scope: implement all four bake-off DAGs in Kruxia Flow v0.8.3, then compare
head-to-head against **Temporal** (category) and **Conductor** (mechanism).
The other eleven tools are not re-run.

## 2026-08-14

```
[17:29] Completed: research -- read the kruxiaflow source + its docs, established
        6 capability findings from source | Next: plan review | Progress: -
[17:31] Completed: plan approved (both partners, all 4 DAGs, differentiator noted
        not built) | Next: Phase 0 infrastructure | Progress: 0/6 phases
[17:34] Completed: compose profile, Justfile teardown lists, init-engines.sql,
        BAKEOFF_RUNNERS, bakeoff-db.sh routing, kruxiaflow resume provider in both
        mock services | Next: bring the stack up | Progress: Phase 0 ~60%
[17:35] Completed: stack up on Docker Desktop; found + fixed pre-existing
        init-runners.sh exec-bit defect (no runner had schemas); seeded
        kruxiaflow_dag{1,3,4} | Next: smoke test | Progress: Phase 0 ~80%
[17:38] Completed: found kruxia/kruxiaflow:latest is 0.3.0 and lacks
        --insecure-dev entirely; pinned 0.8.3 (multi-arch) | Next: smoke test
        | Progress: Phase 0 ~90%
[17:40] Completed: PHASE 0 DONE -- definition deployed, workflow submitted and
        completed, template substitution verified. env.sh, deploy.sh, README
        written | Next: Phase 1 (DAG 3) | Progress: 1/6 phases
[18:05] Completed: probed engine semantics before writing any DAG -- found
        NUMERIC/TIMESTAMPTZ serialize as silent null (finding 4), and that
        failure-path branches DO run via {{dep.status == 'failed'}} (finding 5)
        | Next: write DAG 3 | Progress: Phase 1 ~20%
[18:10] Completed: py-bakeoff worker + dag3_payment.yaml; happy path green on
        first run, $100 moved, txn recorded | Next: edge cases | Phase 1 ~60%
[18:58] Completed: fixtures reset at user request; retry-exhaustion verified
        (exactly 5 attempts, 48.6s) | Next: remaining edge cases | Phase 1 ~80%
[19:02] Completed: PHASE 1 DONE -- all six DAG 3 assertions verified incl.
        duplicate-key no-double-charge and graceful degradation. Zero authoring
        defects in the DAG | Next: Phase 2 (DAG 4) | Progress: 2/6 phases
```

```
[19:20] BLOCKER FOUND: wait_for_signal cannot complete -- a signalled activity
        always fails with "Subscription already exists". Reproduced on 0.8.3
        AND 0.8.0, on every activity shape incl. the project README's own.
        Root cause pinned to orchestrator.rs:1522-1535 | Next: user decision
        | Progress: Phase 2 blocked
```

## Engine findings so far (report upstream to Kruxia Flow)

**0. `wait_for_signal` cannot complete — a signalled activity always fails.**
The flagship suspend/resume primitive. `delete_subscription` is never called on
the signal path, so re-scheduling the activity re-enters `create_subscription`
and hits the uniqueness constraint. Blocks DAG 2 entirely and DAG 4's approval
and sub-workflow chaining. Reproduced on 0.8.3 and 0.8.0. See README finding 10.


1. `kruxia/kruxiaflow:latest` is **0.3.0**, predates `--insecure-dev`; the
   project README's own quickstart cannot work. Pinned 0.8.3.
2. `--insecure-dev` still requires an RSA keypair + client secret at boot.
3. A declared `outputs:` name does not rename the activity's emitted output.
4. **`postgres_query` returns silent `null` for NUMERIC and TIMESTAMPTZ** — the
   `row_to_json` type ladder's fallback conflates "SQL NULL" with "type I cannot
   decode". Highest severity: it corrupts money and timestamps, not control flow.
5. A handled failure still ends the workflow `failed` — no catch-into-success.
6. No OR-join; mutually exclusive branches need duplicated successor activities.
7. No per-activity `optional`/`allowFailure`; graceful degradation requires the
   activity to report success while its output says it failed.
8. `RetryPolicy` has no jitter option.
9. Custom-worker `ctx.logger` output goes nowhere by default and the engine does
   not capture worker stdout.

## Phases

| # | Phase | Status |
|---|---|---|
| 0 | Infrastructure (compose, ports, DB, resume provider, deploy) | **done** |
| 1 | DAG 3 — payment | **done** — all 6 assertions verified |
| 2 | DAG 4 — order fulfillment + saga | **written, blocked** — never run; needs `wait_for_signal` |
| 3 | DAG 2 — API fan-out + async callback | **blocked, cannot be implemented** on stock 0.8.3 |
| 4 | DAG 1 — CSV ETL | not reached (no blocker known) |
| 5 | Comparison writeup | **done** — `kruxiaflow-comparison.md`; engine defects filed upstream |

Evaluation stopped deliberately at the blocker rather than measuring polling
workarounds. `comparison.md` was **not** given a 13th column: three of the four
DAGs are unverified, so a full column would imply a completeness this evaluation
does not have. The provisional score lives in `kruxiaflow-comparison.md` instead.

## Deliverables

| File | Audience |
|---|---|
| Reported upstream to the Kruxia Flow maintainers | **Kruxia Flow owner** — 5 bug reports + 1 feature request + an umbrella note, with reproductions and suggested fixes |
| `kruxiaflow_bakeoff/UPSTREAM-ISSUES.md` | pointer/index to the above, so the trail is not lost |
| `kruxiaflow-comparison.md` | **Orchestration repo owner** — head-to-head vs Temporal and Conductor, provisional scoring |
| `kruxiaflow_bakeoff/README.md` | lab notebook — 12 findings, DAG 3 evidence table |

## Left running / to clean up

- Stopped container `kf-v080-probe` and database `kruxiaflow_v080`, both from the
  version-regression test. Removing them needs `docker rm kf-v080-probe` and a
  `DROP DATABASE` run by hand (the tooling here blocks destructive commands).
- The compose stack and the `py-bakeoff` worker are still up.

## Open items for the repo owner

1. **`just` is not installed on this machine**, and podman is installed with no
   initialised VM while Docker Desktop is the live runtime. Every documented
   `just` recipe was run as its raw `docker compose` equivalent with
   `CONTAINER_RUNNER=docker`. The `Justfile` detects runtimes by binary presence
   only, so it picks podman and fails to connect.
2. **`shared-services/init-runners.sh` needed the exec bit** (fixed here). At 644
   under Docker Desktop the Postgres entrypoint exec'd it and it died, leaving a
   database with no bake-off schemas for *any* runner and no error at `up` time.

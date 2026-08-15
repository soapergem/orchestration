# Kruxia Flow issues — filed upstream, not here

The engine defects found while implementing these DAGs are **Kruxia Flow's
feedback, not the bake-off's**, so they live in the Kruxia Flow internal
repository rather than in this one. This file is a pointer so the trail is not
lost.

Filed in `kruxiaflow-internal` on branch `bakeoff-findings-2026-08-14`:

| Doc | Severity | One line |
|---|---|---|
| `docs/notes/2026-08-14-orchestration-bakeoff-findings.md` | — | Umbrella: context, index, and what worked well |
| `docs/bugs/2026-08-14-wait-for-signal-resume-fails.md` | **Blocker** | A signalled activity always fails — the resume path never consumes the subscription |
| `docs/bugs/2026-08-14-signal-template-resolution-poisons-workflow.md` | High | `{{SIGNAL.*}}` in a waiting activity's params hangs the workflow in `created` forever |
| `docs/bugs/2026-08-14-postgres-query-null-for-numeric-and-timestamptz.md` | High | Money and timestamps silently decode as `null` |
| `docs/bugs/2026-08-14-latest-tag-is-0.3.0-quickstart-broken.md` | High | The public quickstart cannot work; `latest` is six months stale |
| `docs/bugs/2026-08-14-signal-api-returns-200-when-dropped.md` | Medium | A dropped signal is reported as HTTP 200 |
| `docs/features/2026-08-14-declarative-workflow-gaps.md` | Feature | OR-join, `optional`, retry jitter, `http_request` retry classification, worker log capture |

**Nothing was filed on the public tracker.** `kruxia/kruxiaflow` has issues
enabled and has never received one (`totalCount: 0`), so the internal repo is
where this belongs first.

## What stays in this repo

The *bake-off's* own findings — how Kruxia Flow scores, how it compares, and
what the implementation cost — are evaluation artefacts and stay here:

- **`README.md`** — the lab notebook: twelve findings with the evidence, the DAG 3
  results table, and what was not verified and why.
- **`../kruxiaflow-comparison.md`** — the head-to-head against Temporal and
  Conductor, with provisional scoring.

The distinction matters when presenting: the Kruxia Flow owner should get the
upstream docs, which are written as actionable bug reports with root causes and
suggested fixes. The orchestration repo owner should get the comparison, which
is written as evidence for a recommendation. Neither is a good substitute for
the other.

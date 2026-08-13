# Orchest-Rated — Presentation Script

Spoken script for the deck. **The deck is `presentation/slides/slides.md`** (mkslides →
reveal.js; `just slides-serve` on :8084, `just slides-build`). Slides stay minimal
bullets; this file is the longer narration read alongside them. `[SLIDE n]` cues match
the slide order in `slides.md` exactly.

`OrchestRated.pptx` is **superseded** — its eight slides have been merged into
`slides.md` and it is kept only as history.

## Time budget — 45 minutes total

| Part | Content | Target |
|---|---|---|
| 1 | Concepts (slides 1–14) | **13.25 min** |
| 2 | The twelve tools, in five families (slides 15–34) | **14.75 min** |
| 3 | Scoring and results | **2.25 min** |
| 4 | What we learned by running them | **2.25 min** |
| 5 | Recommendation | **3 min** |
| — | Q&A / slack | **9.5 min** |

Speaking pace is roughly 140 words per minute, so the per-slide targets below are also
word budgets. If a section runs long in rehearsal, cut sentences rather than speeding up.

**28 of the 45 minutes are Parts 1 and 2.** That is the real shape of this talk: concepts
and the survey. Parts 3–5 are deliberately lean because the per-tool cards now carry the
scores, so Part 3 only has to do the side-by-side table and the movers.

**If rehearsal runs long, cut in this order.** Each is self-contained; nothing downstream
depends on it.

| Cut | Saves | Why it's safe |
|---|---|---|
| **Slide 14** control plane / data plane | 0.5 | Fold two sentences into slide 29 (family 4) instead |
| **Slide 13** what you can't do | 0.5 | The cycle point is a nice-to-have |
| **Slide 10** pull the control flow out | 0.75 | Overlaps slide 3's six jobs |
| **Slide 2** what is workflow orchestration | 0.75 | Overlaps slide 3 |
| Compress **Luigi, Hatchet, Kestra, Step Functions** to ~30s each | 1.0 | The four least contested cards |

That recovers 3.5 minutes. **Protect Temporal (24), Conductor (27), Flyte (31) and Google
Workflows (34)** — they carry the findings that only exist because we ran the code.

## Diagram assets

Editable Excalidraw scenes in the **Workflow Orchestration Deck** collection, all four
exported and in the deck. Labels use the bake-off's own workflows rather than A/B/C, so
the shape slides double as a preview of the four DAGs.

| Scene | Exported to | Slide |
|---|---|---|
| [Time and Prayers (problem)](https://app.excalidraw.com/s/AgOHaUIPH5l/9LF4rKU94mg) | `images/time-and-prayers.png` | 8 |
| [One Edge Per Item (answer)](https://app.excalidraw.com/s/AgOHaUIPH5l/2qNWEzqG2ff) | `images/one-edge-per-item.png` | 11 |
| [DAG Shapes — The Four Patterns](https://app.excalidraw.com/s/AgOHaUIPH5l/ARGsZHdIcFk) | `images/dag-shapes.png` | 12 |
| [DAG Shapes — What You Can't Do](https://app.excalidraw.com/s/AgOHaUIPH5l/513390HyVjC) | `images/dag-invalid.png` | 13 |

**Re-exporting after a scene edit** is automated — no manual clicking. Open the scene in
Chrome, hide the UI with `.layer-ui__wrapper { display: none }`, press `Shift+1` to
zoom-to-fit, screenshot to a file, then run
`scratchpad/crop.py <raw> <dest> 265 1922 24` to strip the 265px workspace sidebar and
trim white margins.

**Caution: the Excalidraw MCP's own screenshot is not a faithful preview of text width.**
It measures with different metrics than Excalifont, so text that looks fine there can
render clipped mid-word in the real app. Give standalone text generous width.

The problem/answer split is deliberate: slide 8 must not show slide 11's answer. A fifth
scene, *Time and Prayers vs. One Graph*, holds both panels combined and is now redundant —
delete it when convenient.

The six original PNGs (`simple`, `parallel`, `list`, `choice`, `loopback`, `sneaky`) are
superseded by the two composites and no longer referenced.

## Deck conventions

Per-tool cards (slides 19–34) all follow one shape, so the audience learns to read them
once:

1. **Metadata strip** — `Written in <engine language> · You write <authoring languages> ·
   Score n / 100`
2. **Syntax** — 5–8 real lines from this repo's implementation, chosen to show the
   *authoring model*
3. **Strengths / Weaknesses** — three bullets each, borderless two-column table

Conductor (27) and Kestra (28) are the two exceptions: they carry **two** code blocks each,
to show how Python hooks into a definition that isn't Python, so their tables are trimmed to
two bullets a side to fit. The dropped points are in the narration.

**Defect counts are deliberately absent from the cards.** They measure *our* implementation
cost, not tool quality — Luigi produced 6 and scores lowest, Kestra 18 and scores 63,
Temporal 0. Presented on a "Weaknesses" column they read as "this tool is buggy," which is
indefensible. They belong in Part 4, where the caveat sits beside them.

---

## Part 1 — Concepts

*Target: 13.25 minutes. Slide numbers match `presentation/slides/slides.md`.*

### [SLIDE 1] Orchest-Rated — Comparing Modern Workflow Orchestration Engines

*(0.75 min)*

We're looking at workflow orchestration today. Three things: what these tools actually
do and how that's different from writing a script, a survey of twelve specific
orchestrators, and a recommendation.

The survey isn't a documentation review. We built the same four workflows in all twelve
tools, deployed them, and ran them until they passed. So the comparison is backed by
code that actually executed, and I'll tell you where running them changed our minds.

---

### [SLIDE 2] What is workflow orchestration?

*(0.75 min)*

Workflow orchestration is the automated coordination of interdependent tasks —
enforcing the order they run in, holding the state between them, handling their
failures, and recording what happened.

Most of these tools include a scheduler; cron-style, event, and API triggers are
standard, and several are primarily used that way. But scheduling is about *starting*
work. Orchestration is about everything that happens after it starts — which is the
part you'd otherwise write yourself.

Workflows are usually modeled as a DAG, a directed acyclic graph. I'll come back to
what that means — and to the fact that the highest-scoring tool here doesn't make you
declare one at all.

---

### [SLIDE 3] What does an orchestrator do?

*(1.25 min — keep brisk; the next several slides make all six concrete)*

Whatever else they disagree about, all twelve do the same six things.

**Enforce order** — you declare that C needs A and B; the engine works out what's
runnable and when. You never write the sequencing.

**Remember where each run got to** — the engine persists that, and
persistence is what makes everything else on this list possible.

**Handle failures and retries** — backoff, jitter, timeouts, and error
*classification*: a declined card should fail immediately, a gateway 503 should retry
five times.

**Coordinate concurrency** — run independent steps in parallel, fan out over a
collection you can't size until runtime, and cap that fan-out.

**Record history** — a graph of the run with per-step status, timings, and in the good
ones the actual input and output of every step. It comes from the engine, so you get it
on workflows you didn't write.

**Suspend and resume** — hold a run while something outside it happens, without keeping
a process open. Some of these can wait a year.

Two of those six are where the twelve differ most: remembering where a run got to, and waiting.

---

### [SLIDE 4] Isn't that just programming?

*(1.25 min)*

Let me put the hardest objection up front, because it's the one I'd raise.

> *"Isn't that just programming? My language already has `if`. It has `for`, it has
> `try`/`except`. And retries are a decorator — `@retry(wait=wait_exponential(...))` and
> I'm done. You've just described control flow. I already have control flow."*

And the honest answer is: **yes. It is programming.** I'm not going to pretend
otherwise, and the best tools here don't either — Temporal's entire pitch is "write
ordinary code," and Prefect's is "your workflow should just be Python." The tools that
score highest in this comparison are the ones that look *most* like plain programming,
not least.

The `tenacity` example is worth taking seriously, because it's the strongest form of the
objection. That decorator genuinely gives you retries with exponential backoff in one
line. What it gives you is retries *inside one process*: the attempt count lives in a
local variable, nothing outside the process can see you're on attempt four, and if the
process dies the attempts die with it. It also can't tell a declined card from a gateway
503 unless you write that logic yourself. It's a real answer to "how do I retry," and no
answer at all to "what is the state of this run."

---

### [SLIDE 5] Yes, but…

*(1.25 min)*

So the question isn't whether it's programming. It's what happens to your control flow
in four specific situations.

**One: the process dies.** Your `if` lives in a stack frame, on one machine, for as long
as that process lives. Kill it and nothing knows which branch you took, how many retries
you'd done, or which iteration you were on.

**Two: somebody else needs to see it.** Your `if` is invisible to anyone without a
debugger attached to a live process. Three weeks later, "which path did order 12345
take?" has no answer — and that's for *one* execution. Now make it ten thousand
executions a night and ask which ones took the unusual branch.

**Three: the wait is long.** You cannot hold a stack frame open for three days while a
manager gets around to approving something.

**Four — and this is the one I'd actually lead with: the steps depend on each other.** In
a program, the connection between two steps is a side effect of the order you typed them
in. `report = transform(rows)` creates a dependency purely because you happened to use
`rows`. That edge is completely real, and it is written down *nowhere* — it exists as a
variable in a stack frame and as an accident of line ordering. So nothing can draw it,
nothing can resume into it, and nothing can tell you what else was waiting on it.

With three steps in a line, that costs you nothing. With thirty, "what is allowed to run
right now?" stops being obvious and becomes a computation — and when step twelve fails,
"what is safe to re-run?" is a question about the *graph*, which your `try`/`except`
cannot answer, because the exception has no idea the graph exists.

---

### [SLIDE 6] So what do people do? — one option: the "do everything" script

*(1.25 min)*

In practice people build one of two things. Here's the first.

**The massive "do everything" script, on a schedule.** You know this file. It's called
`etl - Copy (2).py`. Every step in one process, all the coupling implicit but real. Easy
to read, everything in one place — genuinely a virtue. Then three things happen to it.

*Step three of ten fails.* The process dies, so you re-run from the top, and now you have
to ask whether steps one and two are safe to run twice. If step two charged a card, they
aren't. And the expensive version isn't a failure at all — it's step three *hanging*,
because nothing had a timeout, and nobody noticing for six hours.

*You need to scale it.* One process on one machine is your ceiling. To go wider you
either thread it — and now you're writing coordination code — or you split the file, at
which point you've stopped having a script and started having the second option.

*You need a pause between steps six and seven.* Somebody has to approve something, or an
external system has to call you back. There is no version of a single script that handles
this well. You either block a process for three days, or you cut the file in half and glue
the halves together with a database flag and a webhook — which, again, is the second
option.

---

### [SLIDE 7] The do-it-yourself pitfall

*(0.75 min)*

And notice where that script ends up. When it hurts, you fix it — and the fixes are
always the same fixes. Backoff. Then jitter, because all your workers backed off in
lockstep and hit the service again simultaneously. Then a table tracking which steps
completed, so re-runs can skip them. Then timeouts. Then a concurrency cap. Then
alerting. Then somebody builds a little admin page so whoever is on call at 3am isn't
grepping logs.

That list is a workflow orchestrator. You've built one — undocumented, coupled to your
business logic, with exactly one user.

The argument was never that scripts don't work. It's that the requirements which push you
past a script are *generic* requirements, and generic requirements are worth buying
rather than building.

---

### [SLIDE 8] Another option: scheduling independent tasks

*(2 min)*

*Slide: `images/time-and-prayers.png`.*

So here's the second thing people build, and it's far more common — and far more
interesting, because it *looks* like a decoupled architecture. **Separate tasks,
connected by time and prayers.**

Here's the pattern. Task A runs at midnight and fans out over ten thousand data points.
Task B runs at 4am and fans out over the same ten thousand — one B for each A. B for item
4,271 needs A for item 4,271 to have succeeded.

So there is not one dependency here. There are **ten thousand** of them, one per item. And
every single one is expressed the same way: **4am.** Nobody wrote "B depends on A."
Somebody wrote a timestamp and a four-hour buffer, and that buffer is a guess about how
long A takes.

Watch what that actually costs:

- If A failed for some item, B for that item runs anyway, on missing or stale input. And
  it doesn't necessarily crash — it quietly produces a wrong answer for that one item.
- So you don't get a failure. You get nine thousand nine hundred and ninety-eight correct
  rows and two wrong ones, sitting in the same table, with nothing marking which is which.
- If A simply runs long, it's worse, because *which* items hadn't finished is arbitrary. A
  different couple of hundred every night.
- And at 9am, when the number is wrong: was it A, was it B, and **which of the ten
  thousand?** Two job runs, two log streams, and no shared identifier per item.

The fix everybody applies is to move B to 5am — buying correctness with wall-clock time.
It works until it doesn't.

And note precisely what a job queue does and doesn't give you here, because this is where
Celery gets unfairly blamed. Celery is genuinely good at the fan-out: run Task A across
ten thousand items with retries, backoff, and a concurrency cap. It has nothing whatsoever
to say about the relationship between A-for-item-N and B-for-item-N. And ten thousand
instances of that relationship were exactly the part you needed. They live in two crontab
lines and an assumption. **A task queue is the data plane without the control plane.**

---

### [SLIDE 9] There's a better way

*(0.5 min)*

So here's the framing, and it's the whole thesis of this section.

We all know how to write tasks. That was never the hard part, and nobody is selling you
that. Reliably gluing them together is the hard part.

**Your task code and your scripts are the nodes in the graph. Workflow orchestration
provides the edges.** That's the product: somewhere for the edges to live — declared,
persisted, and inspectable — instead of implied by statement order inside a process that
may not survive the afternoon.

---

### [SLIDE 10] Pull the control flow out

*(0.75 min)*

So what does *providing the edges* actually look like in practice? This.

Your `if`/`else` becomes a **choice** state. Your threads become a **parallel** state.
Your `for` loop over a collection becomes a **map** state. Your retry decorator becomes a
**retry policy** attached to the step.

The logic is identical. What changes is *who knows about it*. A choice state is a node in
a graph that the engine can show you, resume into, and tell you which way it went at 3am
three weeks ago. An `if` is a line of code that has already happened and left no trace.

That's the actual trade. You give up a little expressiveness, and you get your control
flow out of a single process and into something durable and inspectable.

*Callback to plant here — it pays off twice later. In Part 2, Flyte is the tool where this
intuition betrays you: a `@workflow` body looks like a program, but its edges come only
from data dependencies, so statement order implies nothing. And Luigi is the same problem
inverted — its branching and retries are real, but they live inside task bodies where the
orchestrator cannot see them, so none of them appear in the graph.*

---

### [SLIDE 11] Fixing the previous example

*(1 min)*

*Slide: `images/one-edge-per-item.png`.*

So back to midnight and 4am, with the edges made explicit.

An orchestrator makes each pairing an edge. B for an item cannot start until A for that
item succeeded. So the failure of item 4,271 blocks exactly one thing, and it blocks it
**visibly** — a task sitting in a blocked state with a name on it, instead of one silently
corrupted row. Nothing wrong gets published. And you retry item 4,271 by itself, not the
4am job.

Now notice what an orchestrator does *not* do here, because I had this wrong myself at
first: it does **not** make all ten thousand B's wait for the slowest A. There's no global
barrier, because the dependency was never global — it was per item. Item 1 goes straight
through while item 9,998 is still waiting. **That is where these tools stop being tidier
and start being load-bearing.**

---

### [SLIDE 12] Directed Acyclic Graphs

*(0.75 min)*

*Slide: `images/dag-shapes.png`.*

Quick vocabulary, since I promised to come back to it. **Directed** means edges have a
direction: A runs, then B. **Acyclic** means no cycles.

Four shapes cover most of what you'll build, and you'll see all four again in the next
section.

A **straight line** — extract, transform, load.

**Fan-out and fan-in** — validate an order, then reserve inventory and score fraud
concurrently, and the step that confirms waits for both.

**Fan-out over a list**, where the width is decided at runtime. You don't know if the ZIP
has three CSVs or three hundred. A real dividing line: some of these tools can size a
fan-out dynamically but can't introduce a new *kind* of step, and two can't do it at all.

And a **conditional branch**. In our order workflow that's "is this over five hundred
dollars, and does it therefore need a manager's approval."

---

### [SLIDE 13] What you can't do

*(0.5 min)*

*Slide: `images/dag-invalid.png`.*

What the graph can't contain is a cycle. The obvious version is two adjacent steps
pointing at each other — load waits for validate, validate waits for load, so neither can
ever start.

The second panel is the same mistake and it's the one that actually happens: four steps in
a line, a skip-ahead edge, and one edge running backwards. Publish depends on fetch, and
fetch depends on publish. Nothing about that is visible from any single step — you only
see it looking at the whole graph, which is one of the quieter arguments for having a tool
that draws the graph for you.

But "acyclic" doesn't mean you can't repeat work. Retries, `DO_WHILE` loops, and recursive
sub-workflows all give you repetition without a cycle in the dependency graph. The
constraint is on the graph, not on the behavior.

---

### [SLIDE 14] Control plane / data plane

*(0.5 min)*

One last piece of vocabulary, because it explains why the twelve differ as much as they
do.

Almost every one splits in two. The **control plane** — variously the scheduler, the
coordinator, or the decider — holds workflow state, works out which steps are now
eligible, and enqueues them. It does not run your code. The **data plane** is where your
code runs: workers, containers, pods, Lambdas.

The interesting variation is *how* those two connect. Some tools have long-lived workers
that poll for work. Some spin up an ephemeral container per step. Two of them have no data
plane at all, and just call HTTP services you deployed yourself. That single design choice
drives dependency isolation, scaling, cost, and how hard the thing is to operate — which
is most of what the next section is about.

---

## Part 2 — The Twelve Tools

*Target: 14.75 minutes. Twelve tool cards at roughly 45 seconds each — read these as
written; there is no room to elaborate. If you need to reclaim time, the four tools to
compress are Luigi, Hatchet, Kestra and Step Functions; the four worth protecting are
Temporal, Conductor, Flyte and Google Workflows.*

### [SLIDE 15] The 12-tool bake-off

*(0.75 min)*

*Slide: the logo wall. Don't read twelve names aloud — the grid does that, and each gets
its own slide in a moment.*

Here's the field. Twelve orchestrators, and we implemented the same four workflows in
every one — independently, in each tool's own idiomatic style, no shared code — then
deployed and ran them until they passed.

That last part is what makes this worth your time. Documentation tells you what a tool
claims; running the same four workflows tells you what it costs. Every single one of the
twelve had defects in code that looked correct before it was executed.

---

### [SLIDE 16] The four benchmark DAGs

*(1 min)*

**One — CSV ETL.** Unzip an archive, load each CSV into Postgres in parallel, SQL
transform across the loaded tables, write Parquet. Dynamic fan-out, retries, database
integration.

**Two — API fan-out with async callback.** Hand a fetch to an external service, *suspend
the workflow*, resume when that service calls back, branch on the results, fan out thirty
detail requests, combine. Tests suspend and resume driven from outside.

**Three — payment processing.** Validate, call a deliberately flaky gateway with backoff
and jitter, update the database idempotently, send a best-effort receipt. Tests
retriable-versus-terminal classification: a decline must fail immediately, a timeout must
retry, and a failed receipt email must not fail the payment.

**Four — order fulfillment with human approval and saga compensation.** Reserve inventory,
suspend for a manager's approval, ship. And if the manager rejects, or never answers, or
shipping fails — unwind it. Release the reservation, cancel the order, notify the
customer. Three sub-workflows. This is the hard one, and it's where the tools separate.

---

### [SLIDE 17] What it took to stand this up

*(1 min)*

One thing worth saying before we get to the tools, because it is the part nobody budgets for.

The four workflows don't just need an orchestrator. They need **an external service to call
back into a suspended workflow**, a **human-approval service**, a **deliberately flaky
shipping API** so retries have something to retry against, and a **fixture API** serving a
real book catalogue for the fan-out. None of those come with any orchestrator. All four had to
exist before a single workflow could run anywhere.

Then it multiplies by where the orchestrator lives. Locally that's a four-hundred-line compose
file: Postgres, those four services, and four engines. For Argo and Flyte it's a Kubernetes
cluster, an in-cluster Postgres, and the *same* four mocks redeployed as a Helm chart, because
a pod cannot reach my laptop. For the two cloud tools it's an AWS account and a GCP account
under Terraform, plus a Neon Postgres that is publicly reachable — because a Lambda cannot
reach my laptop either — and which those two share, kept apart only by a schema namespace.

And Google Workflows needed one more thing: a fourteen-route Cloud Run service, because that
engine executes none of your code.

*The line to land: the mocks had to be reachable from three network positions at once — a host
process, a pod in-cluster, and a Lambda in AWS. That sounds like a detail and it cost real
time: fixture-service hands back detail URLs derived from the request, so a Lambda in AWS got
`http://fixture-service:8099/...` and every map iteration died on DNS. Whatever runs the
fan-out is what has to resolve the URL.*

---

### [SLIDE 18] Twelve tools, five families

*(0.5 min)*

Twelve is too many to hold in your head, so I've grouped them by what actually determines
fit — which is usually not the feature list, it's what the tool demands of your
infrastructure.

The data pipeline lineage. Durable execution engines. Server-side declarative engines.
Kubernetes-native. And managed cloud serverless.

Notice these don't sort by quality. They sort by *what infrastructure you already run* —
which is most of the recommendation right there.

---

### [SLIDE 19] Family 1 — The data pipeline lineage

*(0.5 min)*

Luigi, Airflow, Prefect, Dagster. All Python, all authored as Python code, and all
descended from the same problem: a data team with a growing pile of batch jobs and
dependencies between them. These are the tools most people mean when they say
"orchestrator."

The family resemblance is strong and so are the family weaknesses. All four are
Python-only. And all four, by default, run every task in the same Python environment as
the worker — so two tasks needing conflicting library versions is something you solve by
opting into something heavier.

---

### [SLIDE 20] Luigi

*(0.75 min)*

From Spotify, the oldest tool here, and arguably the ancestor of the category.

Look at the syntax: the whole state model is the **Target**. If a task's output already
exists — a file, an S3 object — the task is done and gets skipped. No engine, no database,
no daemon. `python dag1.py` and you're running. It's the cheapest thing here to start.

The cost is that Luigi does almost nothing for you. No real retry policy, no suspend at
all, no dependency isolation — every task runs in the process that launched it. Retries,
branching, error classification and compensation all end up hand-written *inside* task
bodies, so the orchestrator can't see any of them. Its audit trail scored one out of ten:
ten-minute retention, and because Luigi's unit of state is a Target, the scheduler knows a
task is done but not what it produced.

Thirty-eight out of a hundred, and last place — but read that as a thin model rather than bad
software. Every capability the rubric rewards is present in a Luigi pipeline, as hand-written
application code the orchestrator never sees. Cheap to get running, expensive to own.

---

### [SLIDE 21] Apache Airflow

*(0.75 min)*

From Airbnb, now an Apache project, and the de facto industry standard for data
engineering — thirty-five thousand stars, an enormous plugin ecosystem, managed offerings
from Astronomer, AWS and Google, and a large pool of people who already know it.

The syntax is the thing to notice: operators instantiated as objects, chained with the
`>>` operator, and `.expand()` for fan-out. Declaring the edges is mandatory, which means
the whole class of mistake we'll see in Flyte simply cannot happen here.

Its standout is the audit trail — grid, graph and Gantt views, per-task logs, rendered
templates, the code version each run used, unlimited retention. Best in the field.

Against it: Python-only, shared worker environment, and only partly dynamic. The fan-out
*width* is decided at runtime, but the *shape* is fixed when the file is parsed — you
cannot decide at runtime that this run needs a different kind of step. Sixty-five.

---

### [SLIDE 22] Prefect

*(0.75 min)*

Founded by someone who'd worked on Airflow, and it reads as a direct response to it. The
pitch: your workflow should just be Python.

And look at the flow body — that's the whole point. It's ordinary, eagerly-executed
Python. `if` is `if`, a loop is a loop, a value is a value. The graph is inferred from what
actually happened.

That buys two things. Migrating an existing script is nearly trivial — add decorators to
what you already have. And, more importantly, **eager execution makes mistakes shallow.**
Because the body really runs, sequential code is sequential and a whole class of bug is
unwritable. Hold that thought for Flyte.

It also has genuine native suspend, including a mode that tears down the infrastructure
and rebuilds it on resume. Limits: Python-only, isolation is per flow run rather than per
task, and SSO is gated behind paid Cloud tiers. Sixty-seven.

---

### [SLIDE 23] Dagster

*(0.75 min)*

The most conceptually distinctive of the four. Its central abstraction isn't a task, it's
an **asset**. You don't declare "run this job" — you declare "this table exists, here's
how it's produced, and here's what from."

So the engine knows your data lineage, not just your execution order. Built-in data
quality checks, and "re-materialize this table and everything downstream" is a first-class
operation. For a data platform team that's a better mental model than a pile of scheduled
jobs. `dagster dev` gives a full local UI in one command, and its audit trail scores as
high as Airflow's — nine out of ten, joint best alongside Temporal — but earns it
differently: a structured event log with asset lineage, so "which run produced this table,
and what did it read" is a question you can actually ask.

Two caveats. The asset model has a real learning curve and the ecosystem is smaller. And a
structural gap: **no native suspend.** Both waiting workflows had to be split into separate
jobs bridged by a sensor that polls — which is why four workflows show up as seven.
Sixty-eight, the best of this family.

---

### [SLIDE 24] Family 2 — Durable execution engines

*(0.5 min)*

Different family, different problem. Temporal and Hatchet aren't aimed at data pipelines
at all — they're aimed at application and microservice workflows. Long-running business
processes, things that run for days, things where "the process died halfway through" has to
be a non-event.

The defining idea is the one everybody calls **durable execution**, which is a terrible name,
so here is what it means. Every tool in this comparison persists *something* — nine of the
twelve record which tasks finished and what they returned, so a crash resumes you at the last
completed step and the task that was in flight starts over.

These two go further: the workflow function itself survives. You write ordinary code — loops,
conditionals, local variables, a three-day wait — and when the machine dies, a fresh process
picks up on the same line with the same variables. Not "restart from the last checkpoint."
Continue mid-function.

One thing to be precise about, because it's easy to overstate: both of these schedule
perfectly well. Temporal has first-class Schedules with cron, calendar and interval specs,
pause, overlap policies and backfill; Hatchet has cron triggers and scheduled runs. What
they lack isn't the trigger, it's *data-awareness* around it. Three things, concretely:
nothing that waits for a file to land or an upstream table to be rebuilt — that's a
**sensor**, and Airflow ships a dozen of them. Nothing that knows a run represents Tuesday's
slice of data, so nothing can show you which days are filled and which are missing — those
are **partitions**. And nothing that knows which tables a run produced, so no dependency
graph between the datasets themselves — that's **lineage**, and it's Dagster's whole
premise.

You can hand-write all three, and Temporal is genuinely good at that: a durable-timer poll
loop survives restarts better than a sensor does. But it's your application code, not
something the engine understands or can show you.

---

### [SLIDE 25] Temporal

*(1.25 min — the one to spend on)*

The lineage is worth knowing: its founders built AWS Simple Workflow, then went to Uber
and built Cadence, then forked that into Temporal. Third generation of the same idea by
the same people.

Look at the code. There's no DAG and no YAML. The workflow is a function; it `await`s
activities, and it `await`s a *signal* when it needs a human. That `wait_condition` is a
genuine suspend — the workflow is not running, not polling, and not consuming a worker.

Event sourcing is what makes it work. Every activity result is persisted, so if the worker
dies a new worker replays the history to reconstruct the workflow's exact state — local
variables included — and continues from the precise point of failure.

We didn't take that on faith. We `kill -9`'d the worker mid-workflow while it waited on an
approval, decided the approval *while the worker was dead*, left it down over a minute,
then started a completely fresh process. It picked the workflow up and finished it. No lost
work, no duplicated side effects.

Highest score of the twelve, at eighty-eight, and the widest language support here — seven
native SDKs off a Go engine.

The honest weaknesses: it isn't a data pipeline tool — Schedules and backfill are there, but
sensors, partitions and lineage are not. The determinism constraint is real —
no clock reads, no randomness, no direct I/O in the workflow function, because it has to
replay identically. And one gap that surprised us: Temporal cannot tell you what workflows
are deployed. There is no definition registry; the server learns a workflow type exists
only when something runs one.

---

### [SLIDE 26] Hatchet

*(0.75 min)*

The newest thing here by a wide margin — created December 2023, a Y Combinator company.
Same durable-execution family, aimed at background tasks, AI agent orchestration, and
long-running jobs.

Its architectural argument is simplicity: Hatchet runs on **PostgreSQL and nothing else.**
No Cassandra, no Redis, no Kafka. If you already operate Postgres you can self-host this,
and that's a much smaller commitment than most engines in this space. MIT licensed.

The syntax shows the one real ergonomic wrinkle: the wait has to be expressed as an
`OrGroup` of an event condition and a sleep, because `aio_wait_for_event` takes no
timeout. Once you know that, it works.

Seventy. Held back by being very young and pre-1.0, a small community, and the worst auth
story of all twelve — a built-in user database that *cannot* be replaced by an external
identity provider. We scored that below tools with no auth at all, because "none" you can
front with your own SSO proxy.

And a warning if you try it: three separate engine defaults each cause a **silent infinite
hang**, with nothing in any log. Durable tasks are never dispatched unless the workflows are
passed to the worker at construction; `durable_task` caps a suspended wait at a one-minute
timeout; and a `SIGKILL`ed worker stays registered as active, keeps being handed durable
work, and survives an engine restart. Always stop workers with `SIGTERM`.

---

### [SLIDE 27] Family 3 — Server-side declarative engines

*(0.5 min)*

The unifying idea for Conductor and Kestra: **the workflow definition is data, not code** —
and it lives on the server rather than in your repository.

Both have a JVM engine and are language-agnostic about tasks. And both let you change the
shape of a workflow without redeploying the code that executes it, which is a genuinely
different operational model from everything else here. The flip side is that your repo
stops being the source of truth, and nothing detects drift.

---

### [SLIDE 28] Conductor

*(1 min)*

Out of Netflix, in production there and at Tesla, LinkedIn and J.P. Morgan. Thirty-two
thousand stars — second only to Airflow in this comparison. Netflix has since stepped back
to an internal fork; a company called Orkes is now the steward.

Look at the two halves on this slide, because the seam between them is the whole idea.

The top half is the graph: JSON, with a task type and a name. You don't deploy that as a
file — you POST it to the server, which stores and versions it. There is no code in it at
all.

The bottom half is the work: an ordinary Python function. And the *only* thing connecting
it to the graph is the string — `task_definition_name="load_csv_to_postgres"` matches the
`"name"` field above. There's no import, no reference, nothing compile-time. The worker is
a separate process that starts up, tells the server "I can do `load_csv_to_postgres`," and
polls for it. One call to `TaskHandler` discovers every decorated function in the process.

Two consequences. Because workers *poll*, they need no inbound port and work behind NAT.
And because the graph is server-side data, you can rewire a workflow through an API call
without redeploying a single worker, and in-flight executions stay pinned to the version
they started on. Notice also what is *not* in that Python: no retry policy. Retries,
timeouts and concurrency caps live on the task *definition*, so every workflow referencing
that task inherits one policy.

Note the `WAIT` task. It suspends at zero cost and resumes via a single HTTP POST — no SDK,
no token, no relay process. It was the only tool here that needed *no new infrastructure*
for the callback workflows. And it has the easiest local start of the twelve: one
container, SQLite, zero external dependencies.

Seventy-five — second place. Held back by three things: **no authentication whatsoever** in
open source, so a network boundary is mandatory rather than optional; an expression language
that only substitutes values, so every branch condition has to be precomputed by a worker;
and timeouts that fire late, because nothing sets a timer per task — a background process
sweeps periodically and expires whatever is overdue. So a timeout is a floor, not a
deadline: we measured a sixty-second one firing at a hundred and three seconds.

---

### [SLIDE 29] Kestra

*(0.75 min)*

The younger take on the same idea, with YAML instead of JSON. Twelve thousand stars, seven
hundred contributors, six hundred plugins.

Here the Python lives *inside* the YAML. The task declares a type of
`scripts.python.Script`, a Docker task runner, and then an indented block of ordinary
Python. That's Kestra's answer to language-agnosticism: your code is a string in the flow
definition, and it runs in its own container.

Two things worth pointing at. The `taskRunner` line means **a container per script task**,
so Kestra gets dependency isolation essentially for free — which none of the Python-native
four manage. And the handoff back out is explicit: `Kestra.outputs()` is how values re-enter
the flow, and downstream tasks read them as `outputs.<task-id>.vars.<key>`.

That `pip install kestra` line is not boilerplate — it's a fix. The `kestra` package is
preinstalled only in the *server's* own virtualenv, not in the task containers, so every one
of our thirty script tasks crashed on `from kestra import Kestra` until we installed it
per-task. The one flow that worked was the one using a process runner instead of Docker.

Native `Pause` with `onResume` handled both waiting workflows, and the timeout on that Pause
is what drives the saga path.

Sixty-three. Docked for two things: **no dynamic task creation at all** — the loops iterate
over runtime data, but every task is fixed in the YAML before the run starts. And the same
auth problem as Hatchet: one shared admin account in open source, SSO and service accounts
behind Enterprise.

One documentation caution worth carrying: `execution.resumeUrl`, which the callback pattern
appears to depend on, **does not exist in any Kestra version.** The docs were confidently
specific and wrong.

---

### [SLIDE 30] Family 4 — Kubernetes-native

*(0.5 min)*

Argo and Flyte are grouped because **they require a Kubernetes cluster.** Not "can run on
Kubernetes" — most of this list can. These two *are* Kubernetes applications. Without a
cluster there is no product.

What that buys is significant. **Dependency isolation, inherent and complete** — every step
is its own pod with its own image, so conflicting library versions aren't a problem you
solve, they're a problem that doesn't exist. Both scored a perfect ten there, alongside only
the two cloud services. Plus scaling with no workers to manage, and steps in any language,
because the unit of work is a container.

The cost is the prerequisite. If you already run Kubernetes you get things the other tools
have to work for. If you don't, nothing else about these two matters.

---

### [SLIDE 31] Argo Workflows

*(0.75 min)*

A CNCF graduated project, fifteen thousand stars. Workflows are YAML — Kubernetes custom
resources — and each step names a container image and a command. If you already run Argo CD,
adding Argo Workflows is a small step.

The thing I most want to highlight, because we didn't expect it: **Argo validates the entire
call tree at submission.** An unresolvable parameter three templates deep is rejected before
a single pod starts, and the error names the exact path. Of the twelve, only Argo and
Conductor catch structural errors before burning compute — and on Kubernetes each failed
iteration costs minutes of pod scheduling.

Seventy-four, with free OIDC SSO and group-based RBAC, better than most paid tiers here.

Its sharp weakness is the audit trail: **pod logs don't survive pod deletion.** We confirmed
that on a live cluster, repeatedly — completed step pods were garbage-collected before we
could read them. You can find *which* step failed; reading *why* needs external log
aggregation you set up yourself.

One more, because it silently defeated the thing DAG 3 exists to test: `retryPolicy: Always`
cannot classify errors. Argo retries on *pod* failure, and a pod that failed on purpose is
indistinguishable from one that hit a blip — so a declined credit card got retried all five
times, exactly like a gateway 5xx. The workflow still reached the right final state, just
slowly and after four wrong attempts, which is why nobody noticed.

---

### [SLIDE 32] Flyte

*(1 min)*

Out of Lyft, aimed at machine learning and data workflows at scale. You author in Python,
but it's Kubernetes-native underneath — one pod per task. Strong typing, built-in caching,
and real multi-tenancy: a project-and-domain pair materializes as an actual namespace with
its own resource quota.

Three things we verified that are genuinely good. One pod per task, including every element
of a dynamic fan-out. Typed inputs and outputs persisted to blob storage, so a finished
run's outputs are readable through the API long after its pods are gone — materially better
forensics than Argo. And configuration travels as *data*: the database settings are a typed
workflow input, so a run can be retargeted without touching workflow code.

Now look at that last line of the code, because this is the biggest trap we found anywhere
in the evaluation. **A Flyte `@workflow` body looks like Python and is not.** It builds a
graph, and Flyte derives edges *only* from data dependencies. Two statements in order, where
the second doesn't consume the first's output, run **in parallel.** In our code a SQL
transform ran concurrently with the unzip that was supposed to precede it, and died on a
missing table. Nothing warns you — not the type checker, not the linter, not registration.
That `>>` is mandatory.

Airflow and Argo make you declare edges, so the mistake is unexpressible. Prefect and
Temporal execute eagerly, so sequential code *is* sequential. Flyte is the only tool we
tested where correct-looking code is silently concurrent. Seventy.

---

### [SLIDE 33] Family 5 — Managed cloud serverless

*(0.5 min)*

Step Functions and Google Workflows, grouped for three reasons that all matter more than
their feature lists.

They require an account with **one specific cloud**, and can't run anywhere else. No
self-hosted option, and — we verified this — no usable local path for workflows of this
complexity.

**The billing model is different.** Everything else here is free software where you pay for
machines. These two bill per state transition or per step. It's the only category where a
chattier workflow has a line item.

And **the engine executes none of your code.** Every step calls out to something you
deployed separately. The upside is real — isolation is inherent, scaling is somebody else's
problem, both scored a perfect ten, and there's no infrastructure to operate. The downside
is that you build a service for every step body before you can run anything at all.

---

### [SLIDE 34] AWS Step Functions

*(0.75 min)*

Launched in 2016 as the successor to AWS Simple Workflow — which, as I mentioned, is where
the Temporal founders came from.

Workflows are Amazon States Language: JSON, with a visual Workflow Studio. Notice that the
retry policy is declarative, attached to the state — that part is genuinely nice. The killer
integration story is calling two hundred and twenty AWS services directly without writing a
Lambda, and Distributed Map fanning out to ten thousand concurrent executions, more
parallelism out of the box than anything else here. Auth is IAM, already solved if you're an
AWS shop.

Sixty, held down by two structural things. **Vendor independence: zero.** And **no dynamic
task creation** — the state machine is immutable at runtime; a Map iterates over data but
cannot introduce a new *kind* of step. Also, ASL can't nest, so every sub-workflow is its own
state machine: our four workflows became seven deployed machines.

---

### [SLIDE 35] Google Workflows

*(0.75 min)*

The GCP equivalent. YAML, fully managed, pay per step.

And that one call is the headline: `events.await_callback` holds a genuinely suspended
execution — no worker, no poller, no billed step — for up to a **year.** That's the best
suspend/resume of anything we tested. Only Temporal matches it, and Temporal needs a worker
fleet running to do so.

Seventy, which is up eleven points from what we'd scored on documentation alone — the largest
movement in the comparison, and all of it in Google's favour. That's the single best argument
for actually running these things rather than reading about them.

Two costs that only appear on contact. First, "the engine runs no code" taken to its
conclusion: to run these four workflows we had to build and deploy a **fourteen-route Cloud
Run service** first. Deploying Google Workflows is one command; *building* something for it
is not. Second, the expression language is small with sharp edges — seven distinct defect
classes. Four fail at deploy time, which is the good news. Three fail at runtime pointing
somewhere other than the cause.

---

## Part 3 — Scoring and Results

*Target: 2.25 minutes. The per-tool cards already gave you each score, so this section's job
is the shape of the field — not a re-reading of the numbers.*

### [SLIDE 36] The scoring rubric

*(0.75 min)*

A hundred points across twelve weighted categories. Eight are worth ten points each and
four are worth five, and the weighting is the argument.

The ten-pointers are the things that change what you can *build*: how many languages you can
write in, whether you can create tasks at runtime, whether tasks can have conflicting
dependencies, how durable execution is, whether you can resume from failure, what the audit
trail gives you, how it scales, and whether you're locked to one vendor.

The five-pointers are things you can usually work around: authentication, community size,
local development, and suspend-resume. Suspend at five points is arguable — for our DAG 2
and DAG 4 it was the whole ballgame — but a tool that can't suspend can poll, and several
did exactly that.

---

### [SLIDE 37] The scoreboard

*(1.5 min)*

Here's the whole field. Three things to notice about the *shape* rather than the order.

**First, Temporal at eighty-eight is alone.** Thirteen points clear of second place, and the
gap is almost entirely one category — what survives a crash, where Temporal scores ten and
nothing else scores above eight.

**Second, the middle is a cluster, not a ranking.** Conductor seventy-five, Argo
seventy-four, then three tools tied at seventy, then sixty-eight, sixty-seven, sixty-five,
sixty-three, sixty. That is eight tools inside fifteen points. Any difference of two or three
points here is noise relative to what your existing infrastructure will decide for you — and
the three tied at seventy, Flyte, Hatchet and Google Workflows, are so unalike that the tie
is proof the total is a summary, not a verdict.

**Third, Luigi at thirty-eight is also alone**, twenty-two points below the next tool. That
number is real, and I'll defend it in a moment — but not as "bad software."

The honest reading of this chart: the top and bottom are meaningful, the middle is a field.

---

## Part 4 — What We Learned by Running Them

*Target: 2.25 minutes. This section only exists because we ran the code.*

### [SLIDE 38] Where a tool catches your mistake

*(1.25 min)*

We wrote the same four workflows twelve times, and every one of the twelve had defects in
code that looked correct before it executed. The useful question turned out not to be how
many, but **when you find out**.

**At submission.** Argo and Conductor reject a structurally invalid graph before a single pod
starts. Argo validates the entire call tree — an unresolvable parameter three templates deep
comes back naming the exact path through the templates. Those mistakes cost you seconds.

**At run time, one failed task at a time.** Every Python-native tool. You submit, a task
fails, you fix it, you submit again. And on Kubernetes each of those iterations costs minutes
of pod scheduling — Flyte's task pods carry about seventy seconds of setup each, so a
ten-node workflow is a ten-minute round trip.

**Or never.** Flyte's missing `>>` edges were not errors at all. They were a *race*. The
workflow ran, and whether it worked depended on which task happened to finish first. We found
four more assigned-but-unused task results elsewhere in the same codebase that were latent
races of exactly the same kind.

Two things follow from that, and the second is why this slide is still here.

First, **eager execution is worth a lot.** Temporal's zero defects and Prefect's relatively
shallow ones share one cause — the workflow body runs as ordinary code, so sequential code
*is* sequential and a value *is* a value. Flyte's hardest defects come from the opposite
model: a body that looks imperative but is a graph builder.

Second — and this is the part that has changed since we started this evaluation — **all of
this matters more when the code is generated, not less.** If a model writes your workflow,
you are reviewing something you did not author, and the failure mode you are least equipped
to catch by reading is the one that looks completely correct. Flyte's silently-concurrent
body is exactly the shape of thing a confident model hands you. So a tool that validates the
whole call tree at submission is worth more now than it was two years ago, and a tool that
turns your mistake into a race is worth considerably less.

---

### [SLIDE 39] "What is deployed here?"

*(1 min)*

Here's a question we didn't plan to ask. We deployed the same four workflows everywhere, then
opened each UI and counted what it showed. **The answers ranged from zero to nineteen.**

First, be precise about what this counts, because it is *not* observability. Every one of the
twelve shows you running and past executions perfectly well. The question is narrower:
**before you run anything, what does the tool list as deployable?**

**Temporal lists none, by design.** There is no definition registry — the server learns that a
workflow *type* exists only when an execution of it appears. Its executions are fully visible;
its catalogue simply isn't a concept.

**Prefect and Flyte list none until you register.** Both can answer the question, but only
after an explicit deploy or `pyflyte register` — and Flyte's registry is scoped per project
*and* domain, so you can be looking at the wrong one. Easy to skip, silent when skipped.

**Luigi lists none, ever** — no registry at all, and `luigid` drops a task from state after ten
minutes.

**A high count is not richness either.** Step Functions shows seven because ASL cannot nest;
Dagster shows seven because it has no native suspend. Both are workarounds inflating a count.
Only Airflow's four means what it appears to mean.

**And registration is durable server state that nothing garbage-collects.** Hatchet showed
**nineteen definitions for nine workflows**, because registrations are keyed by a name that
embeds a namespace — one worker run with the wrong namespace leaves a full orphan set behind.
Kestra showed **thirteen for seven**. Neither ships a reaper. Adopting one of those means
owning that cleanup.

This moves no score, because the rubric weights capability rather than introspectability. But
turn it into the onboarding question: **a new engineer opens the UI — can they discover what
this deployment is even capable of running?** For four of the twelve, no. They see what
happens to be running right now and nothing about what else exists. One of those four is the
tool I'm about to recommend, which is worth saying out loud.

---

## Part 5 — Recommendation

*Target: 3 minutes.*

### [SLIDE 40] What we would recommend

*(1.5 min)*

So, a recommendation — and it's conditional, because these families sort by what
infrastructure you already run.

**For long-running service and business-process workflows: Temporal.** Highest score, widest
language support here, and it is the only one that resumes *inside* a function rather than at
the last finished step — which nothing else on this list can do. It also produced zero defects across four workflows, which after doing this
eleven other times I can tell you is remarkable.

**For data pipelines: Dagster.** The asset model means you declare the tables and their
lineage rather than a pile of jobs, its audit trail is the best in the field, and it has the
data-awareness Temporal doesn't — sensors, partitions, lineage. Don't reach for Temporal for
ETL just because it scored highest overall.

**If you already run Kubernetes: Argo Workflows.** Perfect dependency isolation,
submission-time validation that catches structural mistakes for free, and free SSO with
group-based RBAC. Budget for external log aggregation on day one, because pod logs do not
survive pod deletion. And note the qualifier — *at modest volume*. Pod-per-task is the thing
that caps throughput, and it's the same property that earns it a perfect ten on isolation.

**If you're all-in on one cloud and want zero operations: that cloud's tool.** Both are
genuinely good at what they do. Take the lock-in and the per-step billing as deliberate
decisions rather than surprises, and remember the engine runs none of your code, so you're
committing to building and operating every step body too.

---

### [SLIDE 41] Three things to take away

*(1.5 min)*

If you forget the scores, these three are the ones that generalise.

**One. Your task code is the nodes; orchestration is the edges.** You were always going to
write the nodes — nobody is selling you those. What you're buying is somewhere for the edges
to live: declared, persisted, and inspectable, rather than implied by a timestamp and a
four-hour buffer.

**Two. The families sort by what infrastructure you already run — not by quality.** Eight of
these twelve sit inside fifteen points of each other, so if you already run Kubernetes, or
you're already committed to one cloud, that decides more than any row in my matrix does.

I've narrowed that deliberately to *infrastructure*. It used to be fair to say "what you
already have" and mean your team's skills as much as your platform — which brings me to the
third one.

**Three, and this is the one I'd actually leave you with. The learning curve used to decide
this, and it doesn't any more.** Picking Airflow used to mean months of a team absorbing
Airflow's idioms, and switching later was close to unthinkable — so "what do we already know"
and "how big is the ecosystem" outweighed technical fit. AI collapses most of that. Writing
idiomatic ASL, or Argo YAML, or a Temporal workflow is no longer the expensive part.

Which means the decision moves to the constraints that don't care how fast you can learn.
Take a concrete one. Say you need a million workflow runs a day at ten steps each. On Argo
that's ten million *pods* a day — about a hundred and sixteen pods a second, sustained.
Kubernetes pod-per-task is not built for that, and Flyte measured roughly seventy seconds of
setup per task pod. So the isolation that earns those two a perfect ten out of ten is exactly
what disqualifies them at that volume.

Now run the same numbers through the cloud tools. At published rates, Step Functions Standard
is about seven and a half thousand dollars a month; Google Workflows about three thousand —
and that is for *coordination alone*, before the Lambdas or Cloud Run services that do the
actual work. Express Workflows cut it substantially and trade away the audit trail, which was
seven of Step Functions' points. At that volume a worker pool with no per-execution fee —
Temporal, Hatchet, Conductor on Postgres — stops looking like more work and starts looking
like the only economical option.

So: **pod-per-task caps your throughput, per-step billing caps your volume, and worker pools
trade both for operational ownership.** That's arithmetic you can do before you pick.

One honest caveat, because it's the first thing someone will say. AI collapses the cost of
learning the *syntax*. It does not collapse the cost of *operating* the thing — nothing is
going to own your Kubernetes cluster or Temporal's determinism constraints for you — and it
does not collapse the cost of getting the semantics right. Every one of the twelve had defects
in code that looked correct, and Flyte's silently-concurrent workflow body is precisely what a
confident language model will hand you. Which is really the argument for this whole exercise:
what's left once AI removes the learning curve is exactly the stuff we just spent forty
minutes measuring.


---

### [SLIDE 42] Thank you

*(no time budget)*

Thank you — happy to take questions.

---

### [SLIDE 43] Who I am

*Advance to this and leave it. It keeps your name and contact details on screen while
people are deciding whether to follow up, which is the one moment they actually need them.*

---

## Appendix — Facts to spot-check before presenting

Dates and provenance come from my own knowledge rather than from this repo's docs.
Worth verifying, since they'll be said out loud. I've kept most of them vague in
the script above (no years except where load-bearing) so they're cheap to cut.

- Luigi open-sourced by Spotify, ~2012
- Airflow started at Airbnb 2014; Apache top-level project 2019
- Prefect founded 2018 by Jeremiah Lowin, former Airflow PMC member
- Dagster founded by Nick Schrock, GraphQL co-creator
- Temporal forked from Uber Cadence 2019; founders previously built AWS SWF
- Conductor open-sourced by Netflix 2016; Orkes now sole steward
- Argo originally from Applatix, acquired by Intuit; CNCF graduated
- Flyte open-sourced by Lyft 2020; Linux Foundation governance, Union.ai commercial
- Step Functions launched re:Invent 2016
- Google Workflows GA 2021

Everything else — scores, defect counts, measured behaviors, star counts — comes
from `comparison.md` and the per-tool READMEs.

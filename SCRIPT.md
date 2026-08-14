# Orchest-Rated — Presentation Script

**This is the talk as actually delivered on 2026-08-13, cleaned up.** It replaces the
pre-written script: where the live version was better — the ApartmentIQ anecdote, the
assembly analogy, the Airflow rant — the live version won. Where delivery was loose or
rushed, the wording is tightened using the old draft's precision.

The deck is `presentation/slides/slides.md` (mkslides → reveal.js; `just slides-serve` on
:8084). `[SLIDE n]` cues match its order exactly.

**Recorded timings are from `transcript.m4a`**, so they tell you your real pace rather than a
target. The body ran **00:00–45:51**; Q&A was interleaved around 39:25–42:30 and then ran from
46:55 to the end. Q&A is omitted here, except two answers good enough to fold into the body
(flagged where they appear).

| Part | Slides | Recorded |
|---|---|---|
| 1 Concepts | 1–14 | 00:00 – 09:19 |
| 2 The bake-off and the twelve | 15–34 | 09:19 – 35:10 |
| 3 Scoring | 35–37 | 35:10 – 37:15 |
| 4 What running them taught us | 38–39 | 37:15 – 39:25 |
| 5 Recommendation | 40–44 | 42:30 – 45:51 |

**One standing decision baked in:** the live version credited Claude eight separate times.
That is consolidated into a single disclaimer on slide 1, and everything after it is stated in
your own voice. The work is yours; the tooling note only needs saying once.

---

## Part 1 — Concepts

*Recorded 00:00 – 09:19 (~9 minutes)*

### [SLIDE 1] Orchest-Rated

*(recorded 00:01 – 00:50)*

Tonight we're looking at modern workflow orchestration. Quick show of hands — who already
knows what workflow orchestration is? Good, most of the room.

Two disclaimers before I start. First, I made heavy use of agentic tooling — Claude Code —
while building and comparing these, because it accelerates this kind of research enormously.
I'll not keep flagging it; assume it's there throughout. Second, I've been playing with these
tools for weeks and I still wouldn't claim deep expertise in any one of them. So if you have a
very specific question — "how do I do this particular thing in Prefect?" — the answer may well
be "I don't know." Ask anyway.

---

### [SLIDE 2] What is workflow orchestration?

*(recorded 00:50 – 01:30)*

I'd define it as the automated coordination of interdependent tasks.

Most of you have probably, in some role or another, written cron jobs to run something in the
background, or used a tool like Celery for background work. This is the same territory, and
it's particularly useful for data pipelines, ETL, and any repeatable flow where a predictable
set of steps has to happen on some cadence.

These things are generally modelled as a DAG — a directed acyclic graph. More on that shortly.

---

### [SLIDE 3] What does an orchestrator do?

*(recorded 01:30 – 02:04)*

Six things.

It **enforces order** — task A runs before task B, and you don't write the sequencing. It
**remembers where each run got to**. It has **error handling and retry logic** built in. It
handles **parallel branches**, so multiple tasks run concurrently. It **records history**, so
you have an audit trail. And in most of the modern ones, it can **suspend and resume** — call
out asynchronously to an external service and wait for that service to call back.

Not all of them do that last one. Many do, and it's one of the places they differ most.

---

### [SLIDE 4] Isn't that just programming?

*(recorded 02:04 – 02:40)*

So you might reasonably ask: isn't that just programming? We have `if`/`else`. We have
`try`/`except`. Install a library like `tenacity` and you have retries with exponential
backoff in a decorator.

And yes. It is. But there's a lot that an orchestrator does on top of that, and by the end of
tonight I hope to leave you convinced they're worth reaching for as you productionalize code.

---

### [SLIDE 5] Yes, but…

*(recorded 02:40 – 03:11)*

Because as you're writing tasks: what happens if the process dies halfway through? Do you
start again from the beginning? Well — you could log things. How much depth do you want to go
to with that? How do you know which step it's on, if it's a ten-step process?

There are always multiple ways to skin a cat. But here's the framing I'd offer.

**You could write all of your code in assembly. Why not just use Python?** Similarly, you
could write plenty of code that does all of this task orchestration yourself — or you could
use paradigms that other people have already put a great deal of thought into.

---

### [SLIDE 6] So what do people do? — one option: the "do everything" script

*(recorded 03:11 – 04:12)*

There are really two things people build instead. Here's the first: a single lovely file named
something like that, which you just run, and which transforms some data and lands it in S3 or
a database.

The advantage is genuine — it's easy to read. It's easy to understand what it's doing.

But all the durability and auditing aspects are where an orchestrator becomes your best
friend. What happens when you need to run this same script ten thousand times a day? What
happens when you need to pause between two steps for an asynchronous process? How much of that
do you want to roll yourself?

---

### [SLIDE 7] The do-it-yourself pitfall

*(recorded 04:12 – 04:35)*

And the more you commit to doing it yourself, the more the list grows. Okay, I can do retries.
I can add backoff. I can add exponential jitter. I can create a Postgres table and track where
the state is. I can do all of these things.

Congratulations — you've just built yourself an orchestrator.

---

### [SLIDE 8] Another option: scheduling independent tasks

*(recorded 04:35 – 06:00)*

The second thing people build is one I've seen professionally, and I'll use a real example.

I'm actually leaving a job this week. At the job I'm leaving, one of the things we do is scrape
the web for US apartment data and transform that into a usable dataset. So there are processes
that go out and scrape many thousands of sites, and those are scheduled in a background job
runner to run at midnight. Then there's a processing job that has to act on that data, and
that's all scheduled for 4am.

Which is the caption on this slide: **they're connected by time and prayers.** We just trust
that the data will show up at the right time, and then move on to the second thing.

In the happy path that's fine — data loads, and later it processes. But if the load fails, the
processing runs on it anyway and doesn't work. And if the load is merely still *running*, same
outcome. Nobody finds out until somebody looks at the result.

---

### [SLIDE 9] There's a better way

*(recorded 06:00 – 06:14)*

Whereas with workflow orchestration, there's a better way.

Think of the actual code you're writing — the imperative commands that do something — as the
**nodes** in the graph. The workflow orchestrator is what provides the **edges**. It's the glue
between the tasks.

---

### [SLIDE 10] Pull the control flow out

*(recorded 06:14 – 06:50)*

So what you do is pull the control flow out of your code. All the `if`/`else` logic, the `for`
loops, everything else — those get expressed as workflow definitions instead. The syntax varies
by tool, and we'll see a lot of syntax tonight.

Same logic you'd be writing in a script. The difference is that now the workflow engine can
*see* what's happening, and coordinate the dependencies itself.

---

### [SLIDE 11] Fixing the previous example

*(recorded 06:50 – 07:14)*

Which fixes the example I just gave. Instead of running a batch of ten thousand jobs on a cron
schedule, and then another batch later and hoping, you now have a workflow with two tasks — A
and B, load and process — and you run ten thousand of *those*.

You don't need to time them at all. Each pair runs as soon as it's able to.

---

### [SLIDE 12] Directed Acyclic Graphs

*(recorded 07:14 – 08:17)*

A little on directed acyclic graphs. The idea is simple: there's a single direction to the
graph — I've drawn these all left to right — and it's acyclic, meaning the steps don't cycle
back on each other. They can branch out and branch back in.

Bottom left is the interesting one: you unzip an archive containing hundreds of CSV files, fan
out to a hundred workers each processing one, and then coalesce at the end into a SQL command
that transforms the result.

And you can have **choice states** — the blue diamond. That's `if` logic. You're processing an
online order, and if it's over a certain amount you pause for approval; if not you go straight
through.

DAGs are a very expressive way of decomposing what you're already doing in code.

---

### [SLIDE 13] What you can't do

*(recorded 08:17 – 08:35)*

What you can't do is create cycles. Task B can't refer back to task A. And the second panel is
the same thing, just sneakier — spread across four steps so it's harder to spot.

Incidentally, you *can* have the end of one DAG launch another DAG, which breaks the spirit of
it. But the DAG itself has to be acyclic.

---

### [SLIDE 14] Control plane / data plane

*(recorded 08:35 – 09:00)*

And this separates two concepts that are worth naming, because they come up repeatedly tonight.

The **control plane** is the edges of the graph — the thing describing the state and how work
flows through the system. The **data plane** is your code: the thing actually running.

Most of these orchestrators have a centralised controller or scheduler plus a set of worker
nodes. Not all, but most. And the question that follows is who hosts the controller and the
workers — is it managed, are you paying a cloud subscription, or is it self-hosted? That single
question sorts the twelve more than any feature does.
---

## Part 2 — The Bake-Off and the Twelve

*Recorded 09:19 – 35:10 (~26 minutes). This is the bulk of the talk. Live you said you'd
"speed run" it and invited questions throughout, which worked — keep that.*

### [SLIDE 15] The 12-tool bake-off

*(recorded 08:50 – 09:30)*

I looked at twelve different workflow orchestrators for this, and I'll guarantee you up front
this is not comprehensive. Every week I do a search and there's another one. So apologies if I
haven't covered your favourite tonight.

One other disclosure: I said *modern* workflow orchestrators, and that's a little bit of a lie
in the case of Luigi. That one is in maintenance mode now, so it isn't really honest to call it
modern — but I used it in a previous gig, so I've included it anyway.

---

### [SLIDE 16] The four benchmark DAGs

*(recorded 09:30 – 10:50)*

To compare them, I came up with four toy-box DAGs that I could exercise across all twelve, and
then found metrics to rank them on.

**The first is deliberately simple** — a zip file containing CSVs. Unzip it, load the contents
into Postgres tables, run a transform, save the result as Parquet. A very classical data
engineering workflow.

**The second is an API fan-out**, and this is where the suspend-and-resume story gets tested. I
wrote a service you call out to with a token; the workflow then *pauses*; you trigger that
service manually with the same token; and it calls back to the orchestrator to resume where it
left off. Then it makes a batch of follow-up requests and combines them.

**The third is a bite-size payment processing workflow**, where I intentionally made the
services flaky and full of failures, so I could exercise the retry logic properly — including
telling the difference between a failure worth retrying and one that isn't.

**And the fourth is order fulfillment** — another manual approval step, plus conditional
branching, plus rolling back the work already done if the approval never comes.

Everything I'm showing tonight, including these slides and all the scaffolding, is in a public
GitHub repo. Do dig in. It's a little messy.

---

### [SLIDE 17] What it took to stand this up

*(recorded 10:50 – 12:30)*

Testing this took a fair bit of infrastructure, and this is the part nobody budgets for.

There's a **400-line Docker Compose file** that covers eight of the orchestrators — you can run
those locally under Docker or Podman. There are the **mock services** I mentioned, which are all
FastAPI containers.

I needed a **Kubernetes cluster**, because some of these only run on Kubernetes. Interesting
find there: Oracle Cloud, of all places, has a genuinely free tier for Kubernetes. Everywhere
else — AWS, and I think GCP is similar — charges you a minimum of about $25 a month just for the
control plane.

I needed **AWS and GCP accounts**, because two of these are cloud-only.

And I ended up with **three separate Postgres databases**: one inside the Kubernetes cluster,
one in the Compose file, and one hosted — I found a service called Neon that gives you free
hosted Postgres. Each one was needed for a different reason depending on where in the stack the
orchestrator was running. The cloud tools can't reach a database on my laptop, so that one had
to be publicly available.

---

### [SLIDE 18] Twelve tools, five families

*(recorded 12:30 – 13:28)*

I'm going to speed-run the twelve, so save your questions or jump in as we go — either is fine.

They sort into five high-level families.

**The data pipeline lineage** — Luigi, Airflow, Prefect and Dagster. **Durable execution
engines** — Temporal and Hatchet. **Server-side declarative engines** — Conductor and Kestra.
**Kubernetes-native** — Argo Workflows and Flyte. And **managed serverless** — Step Functions on
AWS, Google Workflows on GCP.

---

### [SLIDE 19] Family 1 — The data pipeline lineage

*(recorded 13:28 – 15:00)*

Those first four. All written in Python, and the code you write for them must also be Python.

Before I start on them, a framing note. My intent tonight is not to tell you that some
particular tool is the best and you should use it in every case. I'd rather spark enough
interest that you go and look at these yourself, because different tools genuinely suit
different circumstances. That said — there are one or two where I'll leave you with a clear
personal bias of *don't use this*.

These four all descend from the same problem: you had a lot of batch jobs or background tasks
with dependencies between them, and you needed a repeatable, auditable way to tie them
together.

**The big shared defect is that the tasks are not independent.** In all four, every task in your
DAG shares the same virtual environment. I ran into this professionally a couple of years ago
on Airflow: if you have a data scientist working on one part of a pipeline and an engineer on
another, and they need incompatible versions of pandas, that doesn't work. You have to resolve
it up front or the workflow simply never runs. To my mind that's the sore thumb across this
whole family.

---

### [SLIDE 20] Luigi

*(recorded 15:00 – 16:45)*

I'll start with the one in maintenance mode, which I therefore don't recommend — but which was
genuinely nice to use. Call it the Judas of the twelve.

The distinctive thing about Luigi is the model. You define classes inheriting from
`luigi.Task`, and each task optionally has a `requires` method that just references other
tasks — that's how you link them. And every task has an **output target**, usually a flat file.

So for each step you write out a file, typically to S3. You can write custom targets that look
for a row in a database instead. Sometimes you're just writing an empty success marker at a
deterministic path.

And here's what that buys you: if you ran a pipeline and it failed thirty percent of the way
through, the next run resumes from that point — because the targets for the earlier steps are
already there. In some contexts that's genuinely lovely. Not all contexts.

Because of that model, Luigi has **no backing database at all**. It's just looking at whether
targets exist. So you can run it entirely locally, or host the scheduler if you want to.

---

### [SLIDE 21] Apache Airflow

*(recorded 16:45 – 19:50)*

Airflow. Written in Python — and here's where my bias comes out. I've used Airflow
professionally and I don't think I'll ever use it again.

It really feels like a couple of Java developers got together over a weekend and decided, hey,
let's try this Python thing. Look at the class names: `PythonOperator`. `ExternalTaskMarker`.
`TriggerDagRunOperator`. There's so much excessive object orientation that it's painful to read.

They have got better, I'll concede that. Versions 2 and 3 added the Task API, which lets you do
things much more naturally with decorators. But classic Airflow style is defining all these
verbose operators and then connecting them with **overridden bitwise shift operators**, which I
always found deeply weird. And it works in either direction — you can flip the operator and
reverse the dependency, because it's just an overridden dunder method.

Then Airflow scans all the Python files in its DAGs folder looking for these bare
module-level variables to work out what connects to what. It doesn't feel like Python to me.

*(Live, this got the biggest reaction of the night — someone in the audience volunteered that
they'd wasted months of their life debugging Airflow, particularly Airflow on Kubernetes. Leave
room for that.)*

It does have a nice UI, a real audit interface, and proper auth. And it is the de facto
industry standard, to the point that most cloud providers now offer a managed version of it.
But it is such a beast.

---

### [SLIDE 22] Prefect

*(recorded 19:50 – 21:00)*

Prefect, also Python. It was written by someone who'd originally worked on Airflow, took some
lessons from it, and tried to build something better — so take that for what it's worth.

This has the far more Pythonic approach: you define a flow with a decorator, and the tasks it
calls are decorated too. Look at the body on the slide — that's ordinary Python. `if` is `if`, a
loop is a loop, and a value is a value.

**That's worth more than it sounds like.** Because the flow body genuinely executes, sequential
code really is sequential — which rules out an entire class of bug we'll see later with Flyte,
where code that looks correct runs in parallel.

Prefect also has real suspend and resume, including a mode that tears the infrastructure down
and rebuilds it when the run continues.

There's a self-hosted version and a paid tier, and the paid tier is where the enterprise
niceties live — single sign-on and so on. The one thing that caught me out is the **task
registry**: nothing shows up in the UI until you explicitly register a deployment or run
something. Which brings me to a theme I'll come back to.

---

### [SLIDE 23] Dagster

*(recorded 21:00 – 22:20)*

Dagster, again Python, again decorator style for chaining tasks together.

This one is genuinely different in that it's **asset-centric**. You're not just declaring the
imperative jobs — you're declaring the tables and artefacts you expect them to produce. So the
tool understands your data, not just your execution order.

It borrows heavily from what Airflow did well on auditing, and its audit trail is excellent —
per-step inputs and outputs, asset lineage, and it keeps them.

**What it does not have is the ability to suspend.** So if you want an async callback, you have
to write a task that sits there polling, which is pretty obnoxious. That's a demerit for
Dagster, and it's the reason my four workflows show up as seven in its UI — the ones that wait
had to be split in half and bridged by a sensor.

---

### [SLIDE 24] Family 2 — Durable execution engines

*(recorded 22:20 – 22:50)*

Second family: Temporal and Hatchet.

What "durable" means here is that these are extremely resilient to an error or a crash at any
point. They're more imperative than the declarative style of something like Dagster — you write
ordinary code rather than a graph — and Temporal in particular is battle-tested for high-volume
workflows where getting *through* errors, or at minimum recording them properly, is the whole
job.

To be precise about the difference, since every tool here persists something: the other ten
record which tasks finished, so a crash resumes you at the last completed step and the task
that was in flight starts over. These two go further — the workflow *function* itself survives,
and a fresh process picks up where the old one was.

---

### [SLIDE 25] Temporal

*(recorded 22:50 – 24:00)*

Temporal is written in Go, so I'm stepping outside the Python space here. But it has SDKs for
almost everything, so you can write your workflows in Python or a good half-dozen other
languages.

And it delivers on the durability claim. While testing, I killed processes in the *middle* of a
step and then came back to it, and it resumed from exactly where things left off. It is very,
very good at that.

Two caveats worth knowing.

**It doesn't really have a task registry.** If I open the UI I can schedule things, but a
workflow won't appear until I've actually run it. Its executions are fully visible; its
catalogue of what *could* run isn't a concept.

**And workflow code has to be deterministic**, because recovery works by re-executing your
function and feeding back the recorded results. That sounds like it would rule out dynamic
workflows, and this is worth stating clearly because it's a common misconception: **it doesn't.**
You can write a `while` loop in the body of a Temporal workflow and it works fine, as long as
you advance through the loop deterministically. The shape of a given run has to be reproducible;
it doesn't have to be static.

---

### [SLIDE 26] Hatchet

*(recorded 24:00 – 25:00)*

Hatchet is in the same family and is much newer. They pitch themselves as agentic-first — very
rich documentation, and they actively encourage you to point your coding tools at it. I'm not
sure that's a real differentiator anymore, since these tools are good at parsing anyone's
documentation, but good for them.

Architecturally there's a lot to like: it runs on **PostgreSQL and nothing else** — no Kafka, no
Redis, no Cassandra to operate. If you already run Postgres, self-hosting this is a small ask.

**What I really didn't like is that it's trying to do too much.** This is running entirely
self-hosted in my local Compose file, and the very first thing it does on load is tell me to
create an account. Why? Don't reimplement an auth system inside your workflow orchestrator —
focus on the thing you're supposed to be good at. And as of now there's no way to replace that
auth database with your own identity provider. You just have to live with it.

So that's the one where I thought: I'm not going to take this entirely seriously yet. Maybe
they'll fix it in a future release.

---

### [SLIDE 27] Family 3 — Server-side declarative engines

*(recorded 25:00 – 25:15)*

Conductor and Kestra. Here the **workflow definition is data, not code.**

Both are Java programs, and both have SDKs for other languages so your tasks aren't stuck on
the JVM. And both let you change the graph *without redeploying anything*, which is a genuinely
different operational model from everything else tonight.

---

### [SLIDE 28] Conductor

*(recorded 25:15 – 26:30)*

Conductor came out of Netflix. You express your DAGs as JSON, which you load onto the server —
and you can swap that definition out on the server dynamically. Your workers are written in
whatever language you like and registered separately.

Look at the two halves on the slide, because the seam between them is the whole idea. The top is
the graph: JSON, with a task type and a name, and no code in it at all. The bottom is an
ordinary Python function. The *only* thing connecting them is the string — the task name matches
the name in the JSON. There's no import and nothing compile-time.

**The open source version has no authentication whatsoever, which I actually like** — because it
means you can handle auth orthogonally, with something you already trust. There's a paid version
with the full enterprise single sign-on story if you want it. And the async resume endpoint
needs no authentication either: it's one HTTP POST, no SDK and no token.

Now, if you're running this in production you obviously want it locked down — an
identity-aware proxy, or castle-and-moat networking around it. But I think it's a virtue when a
tool stays focused on the thing it's meant to do rather than worrying about everything else
under the sun.

---

### [SLIDE 29] Kestra

*(recorded 26:30 – 28:00)*

Kestra took a while to get working right. It expresses DAGs as **YAML** rather than JSON, and
the tasks can be in *any* language at all, because each one is just a Docker container that
runs.

So it has most of what I want. Callback-based resume works. And that container-per-task model
means dependency isolation comes free — which, notably, none of the four Python-native tools
manage.

Auth is the same story as Hatchet's: one shared account in the open source version, fancier
options in the paid tier.

**What you cannot do is modify the DAG on the fly**, and let me be precise about what I mean,
because there are two different things here.

You can always **fan out to N tasks where you don't know N in advance**. Unzip an archive with
some unknown number of CSVs in it, and run one task per file — that's fine, every tool here
expresses that.

What you can't do is **programmatically add a different *kind* of step** at runtime — decide
mid-run that this particular execution also needs a validation step it didn't have before. You
can't do that in Kestra, and you can't do it in Airflow either. Some of the others can.

---

### [SLIDE 30] Family 4 — Kubernetes-native

*(recorded 28:00 – 29:10)*

These next two require a Kubernetes cluster. Quick show of hands — who's used Kubernetes? About
half the room.

I'm a huge fan of Kubernetes. I use it constantly, I think it's great. **But if you're not
already familiar with it, I'd steer you clear of these two.** It's a steep learning curve, as
are several of these orchestrators, and you don't want to bite off both at once. If you're
already running Kubernetes, absolutely look at these. If not, look at the other options — it
depends on your comfort level.

What the model buys you is that **dependency isolation is inherent**, because every task runs as
its own pod. That means it's just a container: it can be Python, it can be Fortran, it doesn't
matter. And two tasks can be Python with mutually incompatible dependencies, because they're
fully isolated.

---

### [SLIDE 31] Argo Workflows

*(recorded 29:10 – 30:50)*

A note on naming: Argo CD is the larger platform, aimed at Kubernetes deployment and CI/CD. Argo
*Workflows* is the orchestrator inside that family, and it's pretty robust. If you've heard of
Kubeflow, for ML pipelines — that's actually running Argo Workflows underneath.

You express workflows as YAML. And Argo has **no state database of its own** — the state
database *is* Kubernetes. It registers a set of custom resource definitions and tracks
everything there.

Which leads to the important limitation. Every task is its own container, plus more custom
resources to track state, and Kubernetes has a hard ceiling in the region of 300,000 containers
running at once. **So if you need to run millions of jobs, neither of these two is the right
choice.** You'd be scaling your cluster enormously for something the model isn't built for.

With that said — I've used Argo before and I think it's incredibly expressive and easy to work
with once it's set up. I'm a big fan.

One weakness to budget for: **you lose the logs as soon as the pods disappear.** That's fixable
with a log aggregation pipeline into Logstash or Datadog or similar, but setting that up is on
you.

---

### [SLIDE 32] Flyte

*(recorded 30:50 – 32:10)*

Flyte went a different direction. It **does** have a state database — just Postgres — rather than
tracking everything in custom resources. And each task persists its inputs and outputs as
objects in blob storage, which helps a great deal with auditing, and mitigates that log-loss
problem the Kubernetes model otherwise has.

But you have to write your code carefully, and there are two traps I hit.

**Statement order means nothing.** Flyte derives the graph from data flow only, so two lines
that don't share data will run in parallel regardless of the order you wrote them. In my code a
SQL transform ran alongside the unzip that was supposed to precede it, and died on a table that
didn't exist yet. That `>>` on the slide isn't stylistic — it's required.

**And retries are conditional on your exception type.** If your code throws something that
doesn't subclass `FlyteRecoverableException`, it doesn't qualify for the retries at all. Declare
`retries=5` on a task whose exceptions inherit plain `Exception` and you silently get zero.

One nice bit of history: Luigi — the first tool I mentioned, now in maintenance mode — was built
by Spotify. They stopped maintaining it and **switched to Flyte** instead.

---

### [SLIDE 33] Family 5 — Managed cloud serverless

*(recorded 32:10 – 33:15)*

The last family is the managed services from the big cloud providers. I only looked at two: Step
Functions on AWS and Google Workflows on GCP.

These are fundamentally different from everything else tonight. With any other orchestrator you
have to run the scheduler or controller somehow — you can pay for a managed version, or host it
on an EC2 instance or in your cluster, but either way you're paying for that compute.

The serverless ones have **no compute for you to manage at all.** And so both AWS and Google
came up with a distinctive billing model: instead of billing for uptime or compute, they bill
you **per state transition.** Every time you move from task A to task B, that's a fraction of a
penny.

Both have a free tier, so under some thousands of transitions a month you pay nothing. But I
implemented a Step Functions job at work that we call **literally a million times every night**,
and at that volume you notice the bills.

---

### [SLIDE 34] AWS Step Functions

*(recorded 33:15 – 34:30)*

Step Functions expresses workflows in JSON. You'll almost never write that by hand — partly
because tooling can generate it, but mostly because there's a genuinely nice **visual editor**
where you drag and drop the states. I've used it extensively.

The thing to understand is that **Step Functions doesn't run anything itself.** It is purely the
edges. Every task is "invoke this Lambda", or "spin up this Fargate container", or some other
call into the AWS ecosystem. What it gives you is all the expressiveness around those calls —
retries, timeouts, catch clauses, and so on, declared rather than coded.

Two costs. You're **locking yourself into that vendor** — though maybe that doesn't matter to
you. And at genuinely high scale **the cost can balloon**, if you're doing millions of
executions. Though on the other hand you may not have a choice, because as we just saw, the
Kubernetes-based ones can't keep up at that volume either.

---

### [SLIDE 35] Google Workflows

*(recorded 34:30 – 35:20)*

Google Workflows is the same shape. Workflows are defined in **YAML**, billing is per
transition, and the engine doesn't run anything itself — your tasks are Cloud Run containers or
Cloud Functions.

Where both of these are genuinely excellent is **asynchronous suspend and resume.** Both let you
suspend an execution for **up to a year**, which is faintly nuts, and it costs you nothing while
it waits. There's no worker sitting there and no billed step. If you have a workflow that needs
to wait on a human for a week, this family does that better than anything else here.

---

## Part 3 — Scoring

*Recorded 35:10 – 37:15 (~2 minutes)*

### [SLIDE 36] The scoring rubric

*(recorded 35:10 – 35:45)*

So, how I scored these.

I built a point system across twelve categories that sums to 100 — language flexibility, how
well a tool survives a crash, auditability, the auth story, and so on. The weighting is the
argument: eight categories are worth ten points each because they change what you can *build*,
and four are worth five because you can usually work around them.

---

### [SLIDE 37] The scoreboard

*(recorded 35:45 – 37:00)*

And this was the result, based on actually building and running those four workflows in all
twelve and scoring across every category.

**Temporal came out on top.** I think Temporal is great — maybe perfect for some situations, and
not for others.

**Luigi did the poorest**, which isn't surprising for something in maintenance mode. Read that
as a thin model rather than bad software: every capability this rubric rewards does exist in a
Luigi pipeline, as hand-written code the orchestrator never sees. Cheap to get running,
expensive to own.

**Conductor and Argo are both well up there.** Hatchet is up there too, and I still wouldn't use
it, because the auth situation is that bad.

But look at the shape rather than the order. Apart from the top and the bottom, **the
distribution is fairly uniform — we're basically hovering in the seventies.** So all of these
tools have different strengths and weaknesses, and the total is a summary rather than a verdict.

---

## Part 4 — What Running Them Taught Us

*Recorded 37:15 – 39:25, and the payoff at 42:30 (~2 minutes)*

### [SLIDE 38] Where a tool catches your mistake

*(recorded 37:00 – 37:30)*

Something else worth paying attention to: **where** a tool catches your mistake.

It's a bit like the interpreted-versus-compiled distinction. With some of these you can't even
*submit* a workflow if there's a problem with it. That's true of Argo, because Kubernetes has to
validate the spec, and Conductor does the same thing. Those mistakes cost you seconds.

With others you submit it, run it, and *that's* when the failure happens. On Kubernetes each of
those iterations costs minutes of pod scheduling.

And with Flyte, the missing `>>` I mentioned wasn't an error at all — it was a race. The
workflow ran, and whether it worked depended on which task happened to finish first.

*Worth adding, and it wasn't in the live version: this matters more now, not less. If a model
writes your workflow, you're reviewing code you didn't author — and the failure you're least
able to catch by reading is the one that looks completely correct.*

---

### [SLIDE 39] What can you see before it runs?

*(recorded 37:30 – 38:30, payoff at 42:30)*

The other thing is whether a tool has a proper workflow registry — the ability to see what's
queued up and available in there before you run anything.

Here's Dagster. I said I have four workflows, and it's showing seven, because the ones that
needed to wait got split into sub-workflows. But I can come in and look at past runs.

Step Functions had to split things up as well, and has a genuinely nice interface for it.
Google Workflows groups it better — it has sub-steps, but you only see the top-level workflows
in the list, which is nicer.

**And then there's Temporal.** Earlier I kicked off DAG 1 from the command line while we were
talking. Now that it's run, there it is — I can click in and examine every step that executed.
But **it wasn't there until I ran it.** Its executions are perfectly visible; the catalogue of
what could run simply doesn't exist.

Which is the honest counterweight to the tool I'm about to recommend.

---

## Part 5 — Recommendation

*Recorded 42:30 – 45:51 (~3 minutes)*

### [SLIDE 40] What I would recommend

*(recorded 42:45 – 43:50)*

So, recommendations.

**For long-running service workflows, Temporal is a solid choice.** It's the only one here that
resumes inside a function rather than at the last finished step.

**If you're doing pure data pipelines, Dagster.** Very robust audit trail, and that data
awareness — it understands your tables, not just your jobs.

**If you're already deep in Kubernetes, check out Argo** — and probably not Flyte, unless you
particularly want to.

**And if you don't want to deal with hosting any of this yourself, use Step Functions or Google
Workflows**, depending on which cloud you're in. They're genuinely nice. They're expressive and
they work well. Just know that when you run them a lot, they can start to cost real money.

---

### [SLIDE 41] Three things to take away

*(recorded 43:50 – 45:51)*

**One. The task code you write is the nodes; orchestration is the edges.** I think there's real
value in decomposing the processes you've been writing so that each piece focuses only on the
job it's doing, and stops worrying about how it interconnects.

**Two. Those five families sort by the infrastructure you already run.** So as you consider
options, look at what you're already doing and what you're comfortable with — that will inform
the decision more than any row in my matrix.

**Three, and this is the one that's changed.** In the past there were people whose entire job
was maintaining Apache Airflow, because the learning curve on these things was steep. A lot of
that has been mitigated — you can now say "write me this DAG in Dagster" and get something
workable. **The barrier to entry is much lower.**

But there are things it can't decide for you. If you need to run at a scale of millions of
tasks, you probably don't want one of the Kubernetes-based ones, because there are hard limits
on how many containers you can run. If you're worried about the cost of per-transition billing
at that volume, you may want to self-host something like Conductor instead.

And one escape hatch worth knowing: **not everything has to be a task run by the orchestrator
itself.** If one step fans out to ten thousand workers, you can submit that to an external job
queue — AWS Batch or similar — and have the orchestrator simply watch for it to finish and then
resume. That gets you around several of these limits at once.

---

### [SLIDE 42] Thank you

*(recorded 45:51)*

That's everything I had. These slides and all the scaffolding are on my GitHub — the repo is on
screen, do dig in.

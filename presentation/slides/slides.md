# Workflow Orchestration

---

## What is workflow orchestration?

The automated coordination, management, and sequencing of interdependent tasks, systems, and data — typically modeled as a state machine.

---

## What is a state machine?

A model of computation defined by:

* A finite set of **states**
* A set of **transitions** between those states
* An **initial state**
* One or more **terminal states**

<div class="fragment">
At any point in time, the machine is in exactly one state. An event or condition causes it to transition to the next.
</div>

---

When referring to workflow orchestration, you often hear about DAGs: directed, acyclic graphs.

---

![Simple DAG](images/simple.png)

---

![Parallel DAG](images/parallel.png)

---

![List DAG](images/list.png)

---

![Choice DAG](images/choice.png)

---

![Invalid DAG 1](images/loopback.png)

![Invalid DAG 2](images/sneaky.png)

---

Workflow orchestration runs tasks in a DAG, keeping track of the overall state and triggering transitions.

<div class="fragment">
Isn't that just <em>programming</em>?
</div>

---

The alternative is running a complex script that handles everything: both business logic and orchestration.

---

## The Monolith Lambda

* The Lambda handles the full chain of API calls executed sequentially
* The Lambda handles all conditional branches
* The Lambda is responsible for error handling
* The Lambda handles retry logic
* Oh, and it still has to do the work!

---


## What happens when step 3 fails?

* The entire Lambda fails — re-run from scratch
* Need to dig through logs for errors
* Long-running processes can time out
* Partial failures often become total failures

---

## Modular Approach

Treat each task in the DAG as an independent worker responsible only for its own focus area.

---

## Pull the control flow out

* Each step becomes its own Lambda with a single job
* The workflow definition handles routing, retries, and error handling
* **Choice states** replace `if/else` branches
* **Parallel states** replace hand-rolled concurrency
* **Map states** replace `for` loops over collections

---

## Visibility & Debugging

* See exactly which step failed and why
* Inspect the input and output at each step
* Trace the full execution history visually

---

In other words, by packaging up scripts inside Lambdas _without_ workflow orchestration tools, we end up building an orchestrator ourselves anyway.

It's just more fragile and less visible.

---

## Sound familiar?

AASM defines states and transitions for Ruby classes.

_e.g. "an order can go from pending to confirmed to shipped"_

---

## AASM at ApartmentIQ

We are currently using aasm on the `Apartmentiq::DataPipeline` class, with only three states:

1. collecting
2. completed
3. errored

---

## AASM vs. Workflow Orchestration

**AASM** models state — it tracks *where an object is* in its lifecycle, but it doesn't execute anything.

Your code is still responsible for doing the work and triggering transitions.

**Workflow orchestration** actually *runs* the work. It calls services, handles retries, manages failures, and moves to the next step automatically.

---

AASM is a **state tracker**.

A workflow orchestrator is an **execution engine**.

---

## AWS Step Functions

Step Functions is a serverless workflow orchestrator.

* Fully managed service
* DAGs defined in ASL (Amazon State Language)
* Visual editor in Console or VS Code
* Past execution history
* Many ways to trigger invocations

---

Let's see it...

---

There are many workflow orchestrators:

1. AWS Step Functions
1. Google Workflows
1. Apache Airflow
1. Argo Workflows
1. Dagster
1. Temporal
1. Kestra
1. Prefect
1. Flyte
1. Luigi
1. Hatchet
1. Conductor

---

Most of them define a concept of a scheduler/coordinator, and a worker.

Usually you will have a single scheduler process, and a pool of multiple workers that can scale.

"""Kruxia Flow bake-off — custom Python worker (`py-bakeoff`).

Everything the built-in `std` worker cannot do. Kruxia Flow ships seven built-in
activities (echo, http_request, postgres_query, postgres_transaction, llm_prompt,
embedding, email_send), which covers more of these DAGs than any other
YAML-defined tool here -- all of DAG 3's and DAG 4's database work is
declarative. What is left needs code, and this is where it lives.

Tool idioms demonstrated:

* **External worker registration.** This process polls
  ``POST /api/v1/workers/poll`` for activities whose ``worker:`` is
  ``py-bakeoff``. It is a peer of the engine's internal pool, not a plugin --
  the engine has no idea what code it runs, and the workflow definition names
  the worker by string. That is what makes definitions data (Model 3) while
  still allowing arbitrary code.
* **Worker-side retriability.** ``ActivityResult.error(..., retryable=False)``
  is the ONLY way to express a non-retriable failure in Kruxia Flow. The
  engine's ``RetryPolicy`` has no error predicate, and built-in activities
  report every anyhow error as retryable (worker/src/executor.rs:201). So DAG
  3's "declined card must not be retried, a 503 must be" is necessarily a
  property of this file, not of the workflow YAML.
* **``ActivityContext``** for logging and heartbeats on long activities.

Run it:

    source kruxiaflow_bakeoff/env.sh
    kruxiaflow_bakeoff/.venv/bin/python kruxiaflow_bakeoff/worker.py

Stop it with SIGTERM (Ctrl-C), never SIGKILL.
"""

from __future__ import annotations

import asyncio
import json
import os
import random

from kruxiaflow.worker import (
    ActivityContext,
    ActivityRegistry,
    ActivityResult,
    WorkerConfig,
    WorkerManager,
    activity,
)

# ---- error codes -----------------------------------------------------------
# These are the strings the workflow YAML branches on, so they are contract.
# Kruxia Flow carries `error_code` through to the ActivityFailed event, which is
# how a downstream activity can tell a decline from an outage without parsing
# a human-readable message.

DECLINED = "PaymentDeclined"
GATEWAY_5XX = "PaymentGateway5xx"
GATEWAY_TIMEOUT = "PaymentGatewayTimeout"
INVALID_ADDRESS = "InvalidAddress"


# ---- DAG 3: payment gateway ------------------------------------------------


@activity(name="payment_gateway")
async def payment_gateway(params: dict, ctx: ActivityContext) -> ActivityResult:
    """Simulated external payment gateway. Deliberately flaky.

    Mirrors the same distribution every other implementation uses (see
    ``temporal/activities.py::process_payment`` and
    ``luigi/dag3_payment.py::_call_payment_gateway``): 5% declined, 15% 5xx,
    20% timeout, 60% success.

    THE POINT OF THIS ACTIVITY is the classification on the way out:

    * ``PaymentDeclined``  -> ``retryable=False``. The issuing bank said no;
      retrying is wrong and, on a real gateway, risks double-charging. The
      engine stops immediately regardless of ``max_attempts``.
    * ``PaymentGateway5xx`` / ``PaymentGatewayTimeout`` -> ``retryable=True``.
      Transient; the engine re-queues under the activity's backoff policy.

    That distinction cannot be expressed in the workflow YAML at all -- it is
    not a `retry:` predicate, it is a boolean this function sets. Compare Argo
    (`retryPolicy: Always` cannot classify, so it retried a declined card five
    times) and Kestra (`retry:` has no error-type predicate). Kruxia Flow's
    answer is better than both, but it moves the decision out of the
    declarative layer and into code the engine cannot see.

    Parameters:
        payment_id, amount, currency, from_account, to_account, idempotency_key
        force_outcome: optional test hook -- "decline" | "5xx" | "timeout" |
            "success". Absent, the roll is random. The other implementations
            force an outcome by editing the thresholds; naming it as an input
            makes the edge-case runs reproducible instead.
    """
    payment_id = params["payment_id"]
    forced = params.get("force_outcome") or ""

    if forced:
        roll = {"decline": 0.01, "5xx": 0.10, "timeout": 0.30, "success": 0.99}.get(
            forced
        )
        if roll is None:
            return ActivityResult.error(
                message=f"unknown force_outcome: {forced!r}",
                code="INVALID_PARAMETERS",
                retryable=False,
            )
    else:
        roll = random.random()

    attempt = getattr(ctx, "attempt", "?")
    ctx.logger.info("gateway call for %s (attempt %s, roll %.3f)", payment_id, attempt, roll)

    if roll < 0.05:
        # NON-RETRIABLE. This is the assertion DAG 3 exists to make.
        return ActivityResult.error(
            message=json.dumps(
                {
                    "payment_id": payment_id,
                    "reason": "Card declined by issuing bank",
                    "decline_code": "insufficient_funds",
                }
            ),
            code=DECLINED,
            retryable=False,
        )
    if roll < 0.20:
        return ActivityResult.error(
            message=f"Payment gateway returned 500 for payment {payment_id}",
            code=GATEWAY_5XX,
            retryable=True,
        )
    if roll < 0.40:
        return ActivityResult.error(
            message=f"Payment gateway timed out for payment {payment_id}",
            code=GATEWAY_TIMEOUT,
            retryable=True,
        )

    return ActivityResult.value(
        "result",
        {
            "status": "success",
            "gateway_transaction_id": f"gw-txn-{payment_id}-{random.randint(10000, 99999)}",
            "amount_charged": params["amount"],
            "currency": params.get("currency", "USD"),
        },
    )


# ---- DAG 4: shipping -------------------------------------------------------


@activity(name="shipping_call")
async def shipping_call(params: dict, ctx: ActivityContext) -> ActivityResult:
    """Call the shipping service, classifying its failures.

    This activity exists for the same reason ``payment_gateway`` does, and the
    repetition is the finding: **the built-in ``http_request`` activity cannot
    participate in a retry policy at all.** It returns non-2xx as a *successful*
    output carrying ``{status, success, body}``
    (worker/src/activities/http.rs:307), so a 503 and a 422 both "complete" and
    ``settings.retry`` never fires. Any HTTP call in Kruxia Flow that needs
    retry-with-classification must therefore be reimplemented in a custom
    worker -- which quietly undoes a chunk of the "no code required" story that
    ``postgres_query``/``postgres_transaction`` earn on the database side.

    Classification, per the shipping service's contract:

    * ``422 InvalidAddress`` -> ``retryable=False``. A bad address will still be
      bad on attempt 5; the spec requires immediate failure into compensation.
    * ``503`` / ``504``      -> ``retryable=True``. Carrier blip.
    * ``200``                -> tracking number.
    """
    import httpx

    order_id = params["order_id"]
    body = {
        "order_id": order_id,
        "items": params.get("items") or [],
        "shipping_address": params.get("shipping_address") or {},
        # Idempotency key so a retry after a timeout cannot create a second
        # shipment -- the service replays the cached result for a known key.
        "idempotency_key": params.get("idempotency_key") or f"ship-{order_id}",
    }
    url = (params.get("shipping_url") or "http://localhost:8092").rstrip("/") + "/shipments"

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(url, json=body)
    except httpx.RequestError as exc:
        # Could not reach the carrier at all: transient by assumption.
        return ActivityResult.error(
            message=f"shipping service unreachable: {exc}",
            code="ShippingUnreachable",
            retryable=True,
        )

    if resp.is_success:
        return ActivityResult.value("result", resp.json())

    try:
        detail = resp.json().get("detail") or {}
    except Exception:  # noqa: BLE001
        detail = {}
    error_type = detail.get("error_type") or f"HTTP{resp.status_code}"
    message = detail.get("message") or resp.text[:200]

    if error_type == "InvalidAddress" or resp.status_code == 422:
        return ActivityResult.error(
            message=f"{error_type}: {message}", code=INVALID_ADDRESS, retryable=False
        )

    return ActivityResult.error(
        message=f"{error_type}: {message}", code=error_type, retryable=True
    )


# ---- sub-workflow plumbing -------------------------------------------------


@activity(name="signal_workflow")
async def signal_workflow(params: dict, ctx: ActivityContext) -> ActivityResult:
    """Signal another workflow's waiting activity, and verify it landed.

    Kruxia Flow has no sub-workflow construct (workflow chaining is Deferred
    upstream), so DAG 4's three children are peer definitions that a parent
    starts over HTTP and then waits for. This is the return leg: the child
    signals the parent when it finishes.

    It is a custom activity rather than a plain ``http_request`` for one
    reason, and it is a real defect rather than a style choice:

        POST /api/v1/workflows/{id}/signal returns **HTTP 200 with
        {"signaled": false}** when no activity was waiting.

    Signals are not buffered -- ``signal_activity`` is a bare
    ``UPDATE activity_event_subscriptions ... WHERE`` (see
    core/src/subscription/postgres_subscription.rs:64), so a signal that
    arrives before the parent's wait activity reaches ``waiting`` matches no
    row and is dropped. With ``http_request`` the parent would then hang until
    its timeout and report "child never finished", when in fact the child
    finished early and shouted into a void.

    So this activity treats ``signaled: false`` as a RETRYABLE failure and lets
    the engine's own backoff close the race. That works, and it is worth noting
    what it costs: a missing primitive (buffered signals) is being paid for
    with a retry loop in user code.
    """
    import httpx

    target = params["workflow_id"]
    body = {
        "activity_key": params["activity_key"],
        "event_name": params["event_name"],
        "data": params.get("data") or {},
    }
    base = (params.get("api_url") or "http://localhost:8100").rstrip("/")

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(f"{base}/api/v1/workflows/{target}/signal", json=body)
    except httpx.RequestError as exc:
        return ActivityResult.error(
            message=f"signal transport failed: {exc}", code="SignalTransport", retryable=True
        )

    payload = {}
    try:
        payload = resp.json()
    except Exception:  # noqa: BLE001
        pass

    if resp.is_success and payload.get("signaled"):
        return ActivityResult.value(
            "result", {"signaled": True, "workflow_id": target, "event_name": body["event_name"]}
        )

    # The parent is probably not waiting YET. Retry -- see docstring.
    return ActivityResult.error(
        message=(
            f"signal not delivered to {target}/{body['activity_key']}"
            f" (HTTP {resp.status_code}, signaled={payload.get('signaled')!r})"
        ),
        code="SignalNotDelivered",
        retryable=True,
    )


# ---- notifications (DAG 3 + DAG 4) -----------------------------------------


@activity(name="notify")
async def notify(params: dict, ctx: ActivityContext) -> ActivityResult:
    """Best-effort notification. Never fails the workflow.

    The spec calls for graceful degradation: a notification that does not send
    must not fail an otherwise successful payment or shipment. Every other
    implementation gets this by catching inside the task, and so does this one
    -- returning a value with ``notification_status: failed`` rather than an
    error, because Kruxia Flow has no per-activity "optional / allowFailure"
    setting (Conductor's ``optional: true``, Kestra's ``allowFailure``). The
    engine's only two outcomes are completed and failed, and failed propagates.

    That absence is worth recording: graceful degradation is expressible here
    ONLY by making the activity lie about succeeding.
    """
    subject = params.get("subject") or f"Notification: {params.get('reference', '')}"
    body = params.get("body") or ""
    ctx.logger.info("NOTIFICATION: %s", subject)
    if body:
        ctx.logger.info("BODY: %s", body)

    if params.get("force_failure"):
        ctx.logger.warning("notification delivery failed (forced) -- degrading gracefully")
        return ActivityResult.value(
            "result", {"notification_status": "failed", "subject": subject}
        )

    return ActivityResult.value(
        "result", {"notification_status": "sent", "subject": subject}
    )


# ---- main ------------------------------------------------------------------


async def main() -> None:
    config = WorkerConfig()  # reads KRUXIAFLOW_* from the environment
    registry = ActivityRegistry()
    for impl in (payment_gateway, shipping_call, signal_workflow, notify):
        registry.register(impl, config.worker)

    print(f"worker      {config.worker} (id {config.worker_id})")
    print(f"api         {config.api_url}")
    print(f"activities  {sorted(registry.activity_types())}")
    print(f"namespace   {os.environ.get('BAKEOFF_NS', '(unset)')}")

    manager = WorkerManager(config, registry)
    await manager.run_until_shutdown()


if __name__ == "__main__":
    asyncio.run(main())

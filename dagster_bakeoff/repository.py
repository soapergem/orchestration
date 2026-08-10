"""
Dagster ``Definitions`` for the orchestration bake-off.

Registers all jobs and sensors so ``dagster dev`` can discover them.  Each job
carries its own ``resource_defs`` (see the DAG modules) because DAG 1, 3 and 4
each bind Postgres to a *different* schema; the resources here are only the
defaults for anything launched without a job-level override.

Usage (from the repo root, so ``dagster_bakeoff`` is importable):
    dagster dev -m dagster_bakeoff.repository        # UI on :3000

The package is deliberately *not* named ``dagster``: a local package by that
name shadows the installed library and neither ``-f`` nor ``-m`` can load it.
"""

from dagster import Definitions

from dagster_bakeoff.dag1_csv_etl import csv_etl_job
from dagster_bakeoff.dag2_api_fanout import process_and_fanout_job, submit_fetch_job
from dagster_bakeoff.dag3_payment import payment_processing_job
from dagster_bakeoff.dag4_order_fulfillment import (
    compensation_job,
    order_post_approval_job,
    order_pre_approval_job,
)
from dagster_bakeoff.resources import HttpClientResource, bakeoff_postgres
from dagster_bakeoff.sensors import (
    approval_sensor,
    fetch_completion_sensor,
    shipping_failure_sensor,
)

# ---------------------------------------------------------------------------
# Definitions
# ---------------------------------------------------------------------------

defs = Definitions(
    jobs=[
        # DAG 1
        csv_etl_job,
        # DAG 2
        submit_fetch_job,
        process_and_fanout_job,
        # DAG 3
        payment_processing_job,
        # DAG 4
        order_pre_approval_job,
        order_post_approval_job,
        compensation_job,
    ],
    sensors=[
        fetch_completion_sensor,
        approval_sensor,
        shipping_failure_sensor,
    ],
    resources={
        "postgres": bakeoff_postgres("dag1", create_schema=True),
        "http_client": HttpClientResource(),
    },
)

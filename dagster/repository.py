"""
Dagster repository / Definitions for the orchestration bake-off.

Registers all jobs, sensors, and shared resources so that ``dagster dev`` or
``dagster-webserver`` can discover them.

Usage:
    dagster dev -f dagster/repository.py
    # or
    dagster dev -m dagster.repository
"""

from dagster import Definitions

from .dag1_csv_etl import csv_etl_job
from .dag2_api_fanout import process_and_fanout_job, submit_fetch_job
from .dag3_payment import payment_processing_job
from .dag4_order_fulfillment import (
    compensation_job,
    order_post_approval_job,
    order_pre_approval_job,
)
from .resources import HttpClientResource, PostgresResource
from .sensors import approval_sensor, fetch_completion_sensor

# ---------------------------------------------------------------------------
# Shared resource instances (used as defaults; individual jobs override as needed)
# ---------------------------------------------------------------------------

shared_postgres = PostgresResource(
    host="postgres",
    port=5432,
    database="orchestration",
    user="orchestration",
    password="orchestration",
)

shared_http_client = HttpClientResource(
    callback_fetch_service_url="http://callback-fetch-service:8090",
    approval_service_url="http://approval-service:8091",
    shipping_service_url="http://shipping-service:8092",
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
    ],
    resources={
        "postgres": shared_postgres,
        "http_client": shared_http_client,
    },
)

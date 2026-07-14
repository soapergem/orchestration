"""Shared DB helper for the DAG 1 ETL lambdas.

Same SSM-DSN pattern as the other DAGs, but every connection is pinned to a
dedicated `dag1_etl` schema via search_path. DAG 1 creates tables named after
the CSV files (orders, customers, products) plus combined_report; isolating
them in their own schema keeps them from colliding with the public transactional
tables that DAG 3/4 use in the same Neon database.
"""

import os

import boto3
import psycopg2

_ssm = boto3.client("ssm")
_dsn = None

SCHEMA = "dag1_etl"


def _dsn_from_ssm():
    global _dsn
    if _dsn is None:
        name = os.environ["NEON_DB_PARAM"]
        _dsn = _ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
    return _dsn


def get_db_connection():
    conn = psycopg2.connect(_dsn_from_ssm())
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        cur.execute(f"SET search_path TO {SCHEMA}")
    conn.commit()
    return conn

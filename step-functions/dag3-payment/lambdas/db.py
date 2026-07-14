"""Shared DB helper for the payment lambdas.

Reads the Neon connection string (DSN) from an SSM Parameter Store SecureString
named by the NEON_DB_PARAM env var, caches it across warm invocations, and
returns a psycopg2 connection. Neon requires TLS; the DSN carries
`sslmode=require`, so no extra connect args are needed.
"""

import os

import boto3
import psycopg2

_ssm = boto3.client("ssm")
_dsn = None


def _dsn_from_ssm():
    global _dsn
    if _dsn is None:
        name = os.environ["NEON_DB_PARAM"]
        resp = _ssm.get_parameter(Name=name, WithDecryption=True)
        _dsn = resp["Parameter"]["Value"]
    return _dsn


def get_db_connection():
    return psycopg2.connect(_dsn_from_ssm())

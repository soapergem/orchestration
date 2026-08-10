"""Shared DB helper for the order-fulfillment lambdas.

Reads the Neon connection string (DSN) from an SSM Parameter Store SecureString
named by the NEON_DB_PARAM env var, caches it across warm invocations, and
returns a psycopg2 connection. Neon requires TLS; the DSN carries
`sslmode=require`, so no extra connect args are needed.

Every connection is pinned to this runner's schema via `search_path` --
`<BAKEOFF_NS>_dag4`, the repo-wide convention from shared-services/init-db.sql.
Neon is shared with the Google Workflows implementation, so the namespace is what
keeps the two apart: `stepfunctions_dag4` vs `google_workflows_dag4`.

NOT self-creating, unlike DAG 1: this schema holds the seeded customers/inventory this DAG validates against.
Setting search_path to a missing schema succeeds silently and every later query
then fails with a confusing "relation does not exist", so check up front and say
what to run.
"""

import os

import boto3
import psycopg2

_ssm = boto3.client("ssm")
_dsn = None

BAKEOFF_NS = os.environ.get("BAKEOFF_NS", "stepfunctions")
SCHEMA = f"{BAKEOFF_NS}_dag4"


def _dsn_from_ssm():
    global _dsn
    if _dsn is None:
        name = os.environ["NEON_DB_PARAM"]
        _dsn = _ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
    return _dsn


def get_db_connection():
    conn = psycopg2.connect(_dsn_from_ssm())
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
                (SCHEMA,),
            )
            if cur.fetchone() is None:
                raise RuntimeError(
                    f'schema "{SCHEMA}" does not exist -- seed it with: '
                    f"SELECT bootstrap_bakeoff('{BAKEOFF_NS}');"
                )
            cur.execute(f'SET search_path TO "{SCHEMA}"')
        conn.commit()
    except Exception:
        conn.close()
        raise
    return conn

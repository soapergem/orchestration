"""
Shared Dagster resources for the orchestration bake-off.

Provides:
  - PostgresResource: a Postgres connection scoped to this runner's schema
  - HttpClientResource: a requests-based client with the mock-service base URLs

Tool idioms demonstrated:
  - ``ConfigurableResource`` (Pydantic-typed structured config) rather than the
    older ``@resource`` factory
  - resources injected into ops by parameter name, so a job's ``resource_defs``
    is the only place connection details appear
  - environment-driven defaults, so the same job runs against compose DNS names
    or a host-side ``localhost:54321`` without a code change
"""

import os
from contextlib import contextmanager

import psycopg2
import requests
from dagster import ConfigurableResource

# ---------------------------------------------------------------------------
# Environment defaults
#
# Defaults are the compose DNS names; running `dagster dev` on the *host* needs
# POSTGRES_HOST=localhost POSTGRES_PORT=54321 and localhost:809x service URLs
# (see RUNNING.md).
# ---------------------------------------------------------------------------

BAKEOFF_NS = os.environ.get("BAKEOFF_NS", "dagster")


# ---------------------------------------------------------------------------
# Postgres resource
# ---------------------------------------------------------------------------

class PostgresResource(ConfigurableResource):
    """Postgres connection scoped to one ``<BAKEOFF_NS>_<dag>`` schema.

    Every table name in the DAG modules is unqualified; ``search_path`` is what
    keeps DAG 1's CSV-derived ``orders`` table from colliding with DAG 4's
    transactional one, and keeps this runner's rows away from every other
    runner's.

    ``create_schema`` splits the two cases from init-db.sql: DAG 1 self-creates
    its schema (the tables come from the CSVs), while DAG 3 and DAG 4 need
    seeded fixtures and so must fail fast when the schema is missing.
    """

    host: str = os.environ.get("POSTGRES_HOST", "postgres")
    port: int = int(os.environ.get("POSTGRES_PORT", "5432"))
    database: str = os.environ.get("POSTGRES_DB", "orchestration")
    user: str = os.environ.get("POSTGRES_USER", "orchestration")
    password: str = os.environ.get("POSTGRES_PASSWORD", "orchestration")
    db_schema: str = f"{BAKEOFF_NS}_dag1"
    create_schema: bool = False

    @contextmanager
    def get_connection(self):
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=self.password,
        )
        try:
            with conn.cursor() as cur:
                if self.create_schema:
                    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{self.db_schema}"')
                else:
                    # SET search_path to a missing schema succeeds silently and
                    # then every query fails with a confusing "relation does not
                    # exist", so check up front and say what to actually run.
                    cur.execute(
                        "SELECT 1 FROM information_schema.schemata "
                        "WHERE schema_name = %s",
                        (self.db_schema,),
                    )
                    if cur.fetchone() is None:
                        raise RuntimeError(
                            f'schema "{self.db_schema}" does not exist -- seed it '
                            f"with: just seed {BAKEOFF_NS}"
                        )
                cur.execute(f'SET search_path TO "{self.db_schema}"')
            conn.commit()
            yield conn
        finally:
            conn.close()


def bakeoff_postgres(dag: str, create_schema: bool = False) -> PostgresResource:
    """PostgresResource bound to ``<BAKEOFF_NS>_<dag>`` (e.g. ``dagster_dag3``)."""
    return PostgresResource(
        db_schema=f"{BAKEOFF_NS}_{dag}",
        create_schema=create_schema,
    )


# ---------------------------------------------------------------------------
# HTTP client resource
# ---------------------------------------------------------------------------

class HttpClientResource(ConfigurableResource):
    """``requests.Session`` wrapper with base URLs for the bake-off mocks."""

    callback_fetch_service_url: str = os.environ.get(
        "CALLBACK_FETCH_SERVICE_URL", "http://callback-fetch-service:8090"
    )
    approval_service_url: str = os.environ.get(
        "APPROVAL_SERVICE_URL", "http://approval-service:8091"
    )
    shipping_service_url: str = os.environ.get(
        "SHIPPING_SERVICE_URL", "http://shipping-service:8092"
    )
    default_timeout: float = 30.0

    def get_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update({
            "Content-Type": "application/json",
            "User-Agent": "orchestration-bakeoff/dagster",
        })
        return session

    def post(self, url: str, json_body: dict, timeout: float | None = None) -> requests.Response:
        session = self.get_session()
        return session.post(url, json=json_body, timeout=timeout or self.default_timeout)

    def get(self, url: str, timeout: float | None = None) -> requests.Response:
        session = self.get_session()
        return session.get(url, timeout=timeout or self.default_timeout)

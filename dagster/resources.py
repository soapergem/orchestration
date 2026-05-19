"""
Shared Dagster resources for the orchestration bake-off.

Provides:
  - postgres_resource: a configurable Postgres connection resource
  - http_client_resource: a configurable HTTP client resource (requests-based)
"""

from contextlib import contextmanager

import psycopg2
import requests
from dagster import ConfigurableResource, InitResourceContext, resource


# ---------------------------------------------------------------------------
# Postgres resource
# ---------------------------------------------------------------------------

class PostgresResource(ConfigurableResource):
    """Dagster resource that provides a Postgres connection.

    Yields a psycopg2 connection from ``get_connection()`` and closes it
    automatically when the op/asset finishes.
    """

    host: str = "postgres"
    port: int = 5432
    database: str = "orchestration"
    user: str = "orchestration"
    password: str = "orchestration"

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
            yield conn
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# HTTP client resource
# ---------------------------------------------------------------------------

class HttpClientResource(ConfigurableResource):
    """Dagster resource wrapping ``requests.Session`` with configurable
    base URLs for the bake-off micro-services.
    """

    callback_fetch_service_url: str = "http://callback-fetch-service:8090"
    approval_service_url: str = "http://approval-service:8091"
    shipping_service_url: str = "http://shipping-service:8092"
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

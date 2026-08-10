"""
GCP task service -- the HTTP task layer for the Google Workflows implementation.

Google Workflows executes no code of its own: `http.post` is the only way to run
anything. So every step body that other orchestrators express as a Python task
has to be an HTTP endpoint here. This service plays exactly the role the Lambdas
play in the Step Functions implementation, and the handlers are ports of
`step-functions/dagN-*/lambdas/*.py` so the two serverless paths stay
behaviourally comparable.

Idioms this exists to demonstrate (they belong to the *workflow*, not this file):
- **Retriability is an HTTP status code**, because a Workflows retry predicate
  can see nothing else. `/process` returns 402 for a decline (non-retriable, the
  workflow routes it to failure handling) and 503/504 for gateway faults
  (retriable). Contrast Temporal/Prefect, which classify on exception *type*.
- **DB credentials arrive in the request body**, because DAG 3/4 take `db_config`
  as a workflow input -- the same property that makes Flyte's `DBConfig`
  retargetable without editing the workflow.
- **Schema isolation is decided here, by route group**, so no YAML needs to know
  about `search_path`: `<BAKEOFF_NS>_dag1` / `_dag3` / `_dag4`.

Single-file like its sibling mock services in `shared-services/`, not the layered
FastAPI layout -- these are evaluation fixtures, and consistency across the four
matters more.

Local:  uvicorn app:app --port 8080
Deploy: terraform/gcp (Cloud Run)
"""

import csv
import io
import logging
import os
import random
import uuid
import zipfile
from datetime import datetime, timezone
from typing import Any, Literal

import httpx
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
from fastapi import FastAPI, HTTPException
from google.cloud import storage
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

class Settings(BaseSettings):
    """Service configuration, sourced from environment variables."""

    # Per-(runner, DAG) schema isolation -- see shared-services/init-db.sql.
    bakeoff_ns: str = "google_workflows"

    # Payment-gateway simulation (DAG 3). Mirrors the Step Functions lambda's
    # distribution; `gateway_force` pins an outcome so the branches are testable
    # instead of probabilistic.
    gateway_declined_rate: float = 0.05
    gateway_server_error_rate: float = 0.15
    gateway_timeout_rate: float = 0.20
    gateway_force: Literal["", "success", "declined", "server_error", "timeout"] = ""

    model_config = SettingsConfigDict(case_sensitive=False)


settings = Settings()
app = FastAPI(title="GCP task service", description=__doc__)

# Created on first use, not at import: storage.Client() needs credentials, and
# without this the whole service refuses to start locally even for the DAG 3/4
# routes, which never touch GCS.
_gcs_client: storage.Client | None = None


def _gcs() -> storage.Client:
    global _gcs_client
    if _gcs_client is None:
        _gcs_client = storage.Client()
    return _gcs_client

# Gateway idempotency: a retried /process for the same key must not mint a second
# transaction id. In-memory, like the other mock services -- a restart forgets.
_gateway_results: dict[str, dict] = {}


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class DbParams(BaseModel):
    """The connection fields every DB-touching route receives.

    Flat rather than nested because that is how the workflow YAML sends them
    (`db_host: ${db_config.host}`), one field per line.
    """

    db_host: str
    db_port: int = 5432
    db_name: str
    db_user: str
    db_password: str
    bakeoff_ns: str | None = None


class UnzipRequest(BaseModel):
    zip_url: str
    gcs_bucket: str
    extract_prefix: str = "extracted/"


class LoadCsvRequest(DbParams):
    gcs_bucket: str
    csv_key: str


class ExecuteSqlRequest(DbParams):
    sql_template: Literal["transform"]
    csv_keys: list[str] = []


class ConvertToParquetRequest(DbParams):
    gcs_bucket: str
    transform_result: dict[str, Any] = {}


class ValidatePaymentRequest(DbParams):
    payment_id: str
    amount: float
    currency: str = "USD"
    from_account: str
    to_account: str
    idempotency_key: str | None = None


class ProcessPaymentRequest(BaseModel):
    payment_id: str
    amount: float
    currency: str = "USD"
    from_account: str
    to_account: str
    idempotency_key: str | None = None
    # Per-request outcome pin, overriding the service-wide GATEWAY_FORCE. Without
    # this, exercising the decline branch means redeploying the Cloud Run service,
    # because pydantic-settings reads the environment once at startup. DAG 3
    # forwards it from its workflow input so one execution can choose its branch.
    force_outcome: Literal["", "success", "declined", "server_error", "timeout"] = ""


class UpdatePaymentRequest(DbParams):
    payment_id: str
    status: str
    amount_charged: float
    currency: str = "USD"
    from_account: str
    to_account: str
    gateway_transaction_id: str | None = None
    idempotency_key: str | None = None


class RecordFailureRequest(DbParams):
    payment_id: str
    status: str = "failed"
    error_code: str | None = None
    error_message: str | None = None
    from_account: str | None = None
    to_account: str | None = None
    amount: float | None = None
    currency: str = "USD"
    idempotency_key: str | None = None


class OrderItem(BaseModel):
    sku: str
    quantity: int
    unit_price: float = 0.0


class ValidateOrderRequest(DbParams):
    order_id: str
    customer_id: str
    items: list[OrderItem]
    approval_threshold: float = 500.00


class ReserveInventoryRequest(DbParams):
    order_id: str
    customer_id: str = "unknown"
    items: list[OrderItem]


class ReleaseInventoryRequest(DbParams):
    order_id: str
    reservation_id: str | None = None
    items: list[OrderItem] = []
    failure_reason: str | None = None


class UpdateOrderStatusRequest(DbParams):
    order_id: str
    status: str
    shipment_id: str | None = None
    tracking_number: str | None = None
    failure_reason: str | None = None


class RecordApprovalDecisionRequest(DbParams):
    order_id: str
    decision: str
    approver: str | None = None
    reason: str | None = None
    approval_request_id: str | None = None
    total_amount: float = 0.0


# ---------------------------------------------------------------------------
# Postgres helpers
# ---------------------------------------------------------------------------

def _connect(params: DbParams, dag: str, self_create: bool):
    """Connect with ``search_path`` pinned to ``<BAKEOFF_NS>_<dag>``.

    ``self_create`` follows the repo-wide split: DAG 1 creates its own schema
    (its tables come from whatever CSVs the ZIP holds), while DAG 3/4 must fail
    fast because they need seeded fixtures. Setting ``search_path`` to a missing
    schema succeeds silently and every later query then fails with a confusing
    "relation does not exist".
    """
    ns = params.bakeoff_ns or settings.bakeoff_ns
    schema = f"{ns}_{dag}"

    conn = psycopg2.connect(
        host=params.db_host,
        port=params.db_port,
        dbname=params.db_name,
        user=params.db_user,
        password=params.db_password,
        # Neon terminates non-TLS connections; harmless against local Postgres.
        sslmode=os.environ.get("PGSSLMODE", "prefer"),
    )
    try:
        with conn.cursor() as cur:
            if self_create:
                cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            else:
                cur.execute(
                    "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
                    (schema,),
                )
                if cur.fetchone() is None:
                    raise HTTPException(
                        status_code=500,
                        detail=(
                            f'schema "{schema}" does not exist -- seed it with '
                            f"SELECT bootstrap_bakeoff('{ns}');"
                        ),
                    )
            cur.execute(f'SET search_path TO "{schema}"')
        conn.commit()
    except Exception:
        conn.close()
        raise
    return conn


# ---------------------------------------------------------------------------
# GCS helpers
# ---------------------------------------------------------------------------

def _read_source(url: str, default_bucket: str) -> bytes:
    """Fetch the DAG 1 input, accepting the three forms the input may take.

    ``gs://bucket/obj`` and ``https://storage.googleapis.com/bucket/obj`` read
    through the GCS client (so a private bucket works); anything else is a plain
    HTTP GET, which is what makes a public fixture URL usable as input.
    """
    if url.startswith("gs://"):
        bucket, _, obj = url[len("gs://"):].partition("/")
        return _gcs().bucket(bucket).blob(obj).download_as_bytes()

    prefix = "https://storage.googleapis.com/"
    if url.startswith(prefix):
        bucket, _, obj = url[len(prefix):].partition("/")
        return _gcs().bucket(bucket).blob(obj).download_as_bytes()

    if url.startswith(("http://", "https://")):
        resp = httpx.get(url, timeout=60.0, follow_redirects=True)
        resp.raise_for_status()
        return resp.content

    # Bare object name -- treat it as a key in the workflow's bucket.
    return _gcs().bucket(default_bucket).blob(url).download_as_bytes()


def _write_object(bucket: str, key: str, data: bytes, content_type: str) -> None:
    _gcs().bucket(bucket).blob(key).upload_from_string(data, content_type=content_type)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "bakeoff_ns": settings.bakeoff_ns}


# ---------------------------------------------------------------------------
# DAG 1: CSV ETL  (schema <ns>_dag1, self-creating)
# ---------------------------------------------------------------------------

TRANSFORM_SQL = """
CREATE TABLE combined_report AS
SELECT
    o.order_id,
    o.customer_id,
    c.customer_name,
    c.email,
    o.product_id,
    p.product_name,
    p.category,
    CAST(o.quantity AS INTEGER) AS quantity,
    CAST(p.price AS NUMERIC(10,2)) AS unit_price,
    CAST(o.quantity AS INTEGER) * CAST(p.price AS NUMERIC(10,2)) AS total_amount,
    o.order_date
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON o.product_id = p.product_id
"""


@app.post("/unzip")
async def unzip(req: UnzipRequest) -> dict:
    """Extract the CSVs from the input ZIP into GCS; return their object keys."""
    raw = _read_source(req.zip_url, req.gcs_bucket)

    csv_keys: list[str] = []
    with zipfile.ZipFile(io.BytesIO(raw), "r") as zf:
        for name in zf.namelist():
            if not name.endswith(".csv"):
                continue
            dest = f"{req.extract_prefix}{name.rsplit('/', 1)[-1]}"
            _write_object(req.gcs_bucket, dest, zf.read(name), "text/csv")
            csv_keys.append(dest)

    if not csv_keys:
        raise HTTPException(422, detail=f"No CSV files found in {req.zip_url}")

    logger.info("Extracted %d CSV(s) from %s", len(csv_keys), req.zip_url)
    return {"gcs_bucket": req.gcs_bucket, "csv_keys": csv_keys}


@app.post("/load-csv")
async def load_csv(req: LoadCsvRequest) -> dict:
    """Load one CSV from GCS into a table named after the file."""
    table = req.csv_key.rsplit("/", 1)[-1].removesuffix(".csv").lower()

    blob = _gcs().bucket(req.gcs_bucket).blob(req.csv_key)
    rows = list(csv.DictReader(io.StringIO(blob.download_as_text())))
    if not rows:
        return {"table": table, "rows_loaded": 0}

    columns = list(rows[0].keys())
    quoted = ", ".join(f'"{c}"' for c in columns)
    conn = _connect(req, "dag1", self_create=True)
    try:
        with conn.cursor() as cur:
            col_defs = ", ".join(f'"{c}" TEXT' for c in columns)
            cur.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({col_defs})')
            cur.execute(f'TRUNCATE TABLE "{table}"')

            buf = io.StringIO()
            csv.DictWriter(buf, fieldnames=columns).writerows(rows)
            buf.seek(0)
            cur.copy_expert(
                f'COPY "{table}" ({quoted}) FROM STDIN WITH CSV',
                buf,
            )
        conn.commit()
    finally:
        conn.close()

    logger.info("Loaded %d row(s) into %s", len(rows), table)
    return {"table": table, "rows_loaded": len(rows)}


@app.post("/execute-sql")
async def execute_sql(req: ExecuteSqlRequest) -> dict:
    """Run the DAG 1 join transform.

    Named templates only -- the workflow sends ``sql_template: "transform"``
    rather than SQL, so no caller can push arbitrary statements through here.
    """
    conn = _connect(req, "dag1", self_create=True)
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS combined_report")
            cur.execute(TRANSFORM_SQL)
            cur.execute("SELECT COUNT(*) FROM combined_report")
            row_count = cur.fetchone()[0]
        conn.commit()
    finally:
        conn.close()

    logger.info("Transform produced %d row(s)", row_count)
    return {"table": "combined_report", "row_count": row_count}


@app.post("/convert-to-parquet")
async def convert_to_parquet(req: ConvertToParquetRequest) -> dict:
    """Write the transform output to GCS as Parquet."""
    table = req.transform_result.get("table", "combined_report")
    conn = _connect(req, "dag1", self_create=True)
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT * FROM "{table}"')
            columns = [d[0] for d in cur.description]
            rows = cur.fetchall()
    finally:
        conn.close()

    # Columnar dict -> Arrow table; pyarrow infers types and handles NULLs.
    arrow = pa.table({c: [r[i] for r in rows] for i, c in enumerate(columns)})
    buf = io.BytesIO()
    pq.write_table(arrow, buf)

    output_key = f"output/{table}.parquet"
    _write_object(req.gcs_bucket, output_key, buf.getvalue(), "application/octet-stream")

    logger.info("Wrote %s (%d rows)", output_key, len(rows))
    return {
        "status": "success",
        "output_bucket": req.gcs_bucket,
        "output_key": output_key,
        "row_count": len(rows),
    }


# ---------------------------------------------------------------------------
# DAG 3: Payment  (schema <ns>_dag3, seeded -- fails fast)
# ---------------------------------------------------------------------------

@app.post("/validate-payment")
async def validate_payment(req: ValidatePaymentRequest) -> dict:
    """Check accounts exist, are active, have balance, and aren't a duplicate.

    Returns the verdict nested under ``validation`` because the workflow reads
    ``validation_response.body.validation`` and branches on ``.is_valid``.
    """
    key = req.idempotency_key or req.payment_id
    conn = _connect(req, "dag3", self_create=False)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT balance, status FROM accounts WHERE account_id = %s",
                (req.from_account,),
            )
            row = cur.fetchone()
            if row is None:
                return _invalid(f"Source account {req.from_account} not found")
            balance, status = float(row[0]), row[1]
            if status != "active":
                return _invalid(f"Source account {req.from_account} is {status}")
            if balance < req.amount:
                return _invalid(f"Insufficient balance: {balance} < {req.amount}")

            cur.execute(
                "SELECT status FROM accounts WHERE account_id = %s", (req.to_account,)
            )
            row = cur.fetchone()
            if row is None:
                return _invalid(f"Destination account {req.to_account} not found")
            if row[0] != "active":
                return _invalid(f"Destination account {req.to_account} is {row[0]}")

            cur.execute(
                "SELECT status FROM transactions WHERE idempotency_key = %s", (key,)
            )
            existing = cur.fetchone()
            if existing is not None:
                return _invalid(
                    f"Duplicate payment: existing transaction with status {existing[0]}"
                )
    finally:
        conn.close()

    return {"validation": {"is_valid": True, "reason": None}}


def _invalid(reason: str) -> dict:
    logger.info("Validation failed: %s", reason)
    return {"validation": {"is_valid": False, "reason": reason}}


@app.post("/process")
async def process_payment(req: ProcessPaymentRequest) -> dict:
    """Simulated flaky payment gateway.

    The response *status code* is the whole point: the workflow's retry predicate
    retries 429/500/502/503/504 and its except-branch treats 402 as a decline. A
    declined card must never be retried, and HTTP status is the only signal
    Workflows can classify on.

    Idempotent per key: a retry after a successful charge returns the original
    transaction id rather than minting a second one.
    """
    key = req.idempotency_key or req.payment_id
    if key in _gateway_results:
        logger.info("Returning cached gateway result for %s", key)
        return _gateway_results[key]

    outcome = req.force_outcome or settings.gateway_force
    if not outcome:
        roll = random.random()
        if roll < settings.gateway_declined_rate:
            outcome = "declined"
        elif roll < settings.gateway_declined_rate + settings.gateway_server_error_rate:
            outcome = "server_error"
        elif roll < (
            settings.gateway_declined_rate
            + settings.gateway_server_error_rate
            + settings.gateway_timeout_rate
        ):
            outcome = "timeout"
        else:
            outcome = "success"

    if outcome == "declined":
        # 402 Payment Required -- NOT retriable.
        raise HTTPException(
            status_code=402,
            detail={
                "error_type": "PaymentDeclined",
                "payment_id": req.payment_id,
                "reason": "Card declined by issuing bank",
                "decline_code": "insufficient_funds",
            },
        )
    if outcome == "server_error":
        raise HTTPException(
            status_code=503,
            detail={
                "error_type": "PaymentGateway5xx",
                "message": f"Payment gateway returned 503 for payment {req.payment_id}",
            },
        )
    if outcome == "timeout":
        raise HTTPException(
            status_code=504,
            detail={
                "error_type": "PaymentGatewayTimeout",
                "message": f"TimeoutError: gateway timed out for payment {req.payment_id}",
            },
        )

    result = {
        "status": "success",
        "gateway_transaction_id": f"gw-txn-{req.payment_id}-{random.randint(10000, 99999)}",
        "amount_charged": req.amount,
        "currency": req.currency,
    }
    _gateway_results[key] = result
    logger.info("Charged %s (%s)", req.payment_id, result["gateway_transaction_id"])
    return result


@app.post("/update-payment")
async def update_payment(req: UpdatePaymentRequest) -> dict:
    """Debit, credit, and record the transaction -- idempotent on the key."""
    key = req.idempotency_key or req.payment_id
    conn = _connect(req, "dag3", self_create=False)
    try:
        with conn.cursor() as cur:
            now = datetime.now(timezone.utc)

            cur.execute(
                "SELECT id FROM transactions WHERE idempotency_key = %s", (key,)
            )
            if cur.fetchone() is not None:
                conn.rollback()
                return {
                    "status": "skipped",
                    "reason": "Transaction already recorded (idempotent)",
                }

            cur.execute(
                "UPDATE accounts SET balance = balance - %s, updated_at = %s "
                "WHERE account_id = %s",
                (req.amount_charged, now, req.from_account),
            )
            cur.execute(
                "UPDATE accounts SET balance = balance + %s, updated_at = %s "
                "WHERE account_id = %s",
                (req.amount_charged, now, req.to_account),
            )
            cur.execute(
                """INSERT INTO transactions
                   (payment_id, idempotency_key, from_account, to_account,
                    amount, currency, status, gateway_transaction_id, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    req.payment_id,
                    key,
                    req.from_account,
                    req.to_account,
                    req.amount_charged,
                    req.currency,
                    "completed",
                    req.gateway_transaction_id,
                    now,
                ),
            )
        conn.commit()
    finally:
        conn.close()

    return {"status": "success", "recorded_at": datetime.now(timezone.utc).isoformat()}


@app.post("/record-failure")
async def record_failure(req: RecordFailureRequest) -> dict:
    """Record a failed payment so the failure path leaves a trail."""
    key = req.idempotency_key or req.payment_id
    conn = _connect(req, "dag3", self_create=False)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM transactions WHERE idempotency_key = %s", (key,)
            )
            if cur.fetchone() is not None:
                conn.rollback()
                return {"status": "skipped", "reason": "Already recorded"}

            cur.execute(
                """INSERT INTO transactions
                   (payment_id, idempotency_key, from_account, to_account,
                    amount, currency, status, error_message, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    req.payment_id,
                    key,
                    req.from_account,
                    req.to_account,
                    req.amount,
                    req.currency,
                    "failed",
                    f"{req.error_code}: {req.error_message}",
                    datetime.now(timezone.utc),
                ),
            )
        conn.commit()
    finally:
        conn.close()

    logger.warning("Recorded failure for %s (%s)", req.payment_id, req.error_code)
    return {"status": "recorded", "payment_id": req.payment_id}


# ---------------------------------------------------------------------------
# DAG 4: Order fulfillment  (schema <ns>_dag4, seeded -- fails fast)
# ---------------------------------------------------------------------------

@app.post("/validate-order")
async def validate_order(req: ValidateOrderRequest) -> dict:
    """Check the customer is active and every SKU has stock; compute the total.

    Read-only, so nothing to compensate if it fails. The workflow reads both
    ``.validation`` and ``.total_amount`` off this response.
    """
    conn = _connect(req, "dag4", self_create=False)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM customers WHERE customer_id = %s", (req.customer_id,)
            )
            row = cur.fetchone()
            if not row:
                return _order_invalid(f"Customer {req.customer_id} not found", req)
            if row[0] != "active":
                return _order_invalid(f"Customer {req.customer_id} is {row[0]}", req)

            total = 0.0
            for item in req.items:
                cur.execute(
                    "SELECT available_quantity, unit_price FROM inventory WHERE sku = %s",
                    (item.sku,),
                )
                row = cur.fetchone()
                if not row:
                    return _order_invalid(f"SKU {item.sku} not found", req)
                available, unit_price = row[0], float(row[1])
                if available < item.quantity:
                    return _order_invalid(
                        f"Insufficient stock for {item.sku}: "
                        f"requested {item.quantity}, available {available}",
                        req,
                    )
                total += unit_price * item.quantity
    finally:
        conn.close()

    return {
        "validation": {"is_valid": True, "reason": None},
        "total_amount": round(total, 2),
        "approval_threshold": req.approval_threshold,
    }


def _order_invalid(reason: str, req: ValidateOrderRequest) -> dict:
    logger.info("Order validation failed: %s", reason)
    return {
        "validation": {"is_valid": False, "reason": reason},
        "total_amount": 0.0,
        "approval_threshold": req.approval_threshold,
    }


@app.post("/reserve-inventory")
async def reserve_inventory(req: ReserveInventoryRequest) -> dict:
    """Reserve every item in one transaction -- all-or-nothing, idempotent."""
    reservation_id = f"RES-{uuid.uuid4().hex[:12].upper()}"
    conn = _connect(req, "dag4", self_create=False)
    try:
        with conn.cursor() as cur:
            # Rows are keyed "<parent>-<sku>", so strip the SKU the row itself
            # reports to recover the parent id. Deriving it by splitting on "-"
            # would break on hyphenated SKUs like WIDGET-A.
            cur.execute(
                "SELECT reservation_id, sku FROM inventory_reservations "
                "WHERE order_id = %s AND status = 'reserved' LIMIT 1",
                (req.order_id,),
            )
            existing = cur.fetchone()
            if existing:
                return {
                    "reservation_id": existing[0].removesuffix(f"-{existing[1]}"),
                    "items_reserved": [i.sku for i in req.items],
                    "reserved_at": datetime.now(timezone.utc).isoformat(),
                    "idempotent": True,
                }

            # Order row first: inventory_reservations.order_id is a FK onto
            # orders, so reserving before the order exists is a FK violation.
            total = sum(i.quantity * i.unit_price for i in req.items)
            cur.execute(
                """INSERT INTO orders (order_id, customer_id, total_amount, status)
                   VALUES (%s, %s, %s, 'reserved')
                   ON CONFLICT (order_id) DO UPDATE
                       SET status = 'reserved', updated_at = NOW()""",
                (req.order_id, req.customer_id, total),
            )

            items_reserved = []
            for item in req.items:
                cur.execute(
                    """UPDATE inventory
                       SET available_quantity = available_quantity - %s,
                           reserved_quantity = reserved_quantity + %s
                       WHERE sku = %s AND available_quantity >= %s
                       RETURNING sku""",
                    (item.quantity, item.quantity, item.sku, item.quantity),
                )
                if cur.fetchone() is None:
                    conn.rollback()
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "error_type": "InsufficientStock",
                            "message": f"Cannot reserve {item.quantity} of {item.sku}",
                        },
                    )
                cur.execute(
                    """INSERT INTO inventory_reservations
                           (reservation_id, order_id, sku, quantity, status)
                       VALUES (%s, %s, %s, %s, 'reserved')""",
                    (f"{reservation_id}-{item.sku}", req.order_id, item.sku, item.quantity),
                )
                items_reserved.append(item.sku)
        conn.commit()
    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info("Reserved %s for %s", items_reserved, req.order_id)
    return {
        "reservation_id": reservation_id,
        "items_reserved": items_reserved,
        "reserved_at": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/release-inventory")
async def release_inventory(req: ReleaseInventoryRequest) -> dict:
    """Saga compensation: reverse the reservations. Idempotent."""
    conn = _connect(req, "dag4", self_create=False)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT reservation_id, sku, quantity
                   FROM inventory_reservations
                   WHERE order_id = %s AND status = 'reserved'""",
                (req.order_id,),
            )
            reservations = cur.fetchall()

            if not reservations:
                return {
                    "order_id": req.order_id,
                    "released": 0,
                    "status": "no_reservations_to_release",
                    "failure_reason": req.failure_reason,
                }

            now = datetime.now(timezone.utc)
            for reservation_id, sku, quantity in reservations:
                cur.execute(
                    """UPDATE inventory
                       SET available_quantity = available_quantity + %s,
                           reserved_quantity = reserved_quantity - %s
                       WHERE sku = %s""",
                    (quantity, quantity, sku),
                )
                cur.execute(
                    """UPDATE inventory_reservations
                       SET status = 'released', released_at = %s
                       WHERE reservation_id = %s AND status = 'reserved'""",
                    (now, reservation_id),
                )
        conn.commit()
    finally:
        conn.close()

    logger.warning(
        "Released %d reservation(s) for %s (%s)",
        len(reservations),
        req.order_id,
        req.failure_reason,
    )
    return {
        "order_id": req.order_id,
        "released": len(reservations),
        "status": "inventory_released",
        "failure_reason": req.failure_reason,
    }


@app.post("/update-order-status")
async def update_order_status(req: UpdateOrderStatusRequest) -> dict:
    """Move the order to a new status, optionally stamping shipment details."""
    conn = _connect(req, "dag4", self_create=False)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE orders
                   SET status = %s,
                       shipment_id = COALESCE(%s, shipment_id),
                       tracking_number = COALESCE(%s, tracking_number),
                       failure_reason = COALESCE(%s, failure_reason),
                       updated_at = %s
                   WHERE order_id = %s
                   RETURNING order_id, status""",
                (
                    req.status,
                    req.shipment_id,
                    req.tracking_number,
                    req.failure_reason,
                    datetime.now(timezone.utc),
                    req.order_id,
                ),
            )
            row = cur.fetchone()
        conn.commit()
    finally:
        conn.close()

    if not row:
        raise HTTPException(404, detail=f"Order {req.order_id} not found")

    return {"order_id": row[0], "status": row[1]}


@app.post("/record-approval-decision")
async def record_approval_decision(req: RecordApprovalDecisionRequest) -> dict:
    """Persist the manager's decision and reflect it on the order."""
    conn = _connect(req, "dag4", self_create=False)
    try:
        with conn.cursor() as cur:
            now = datetime.now(timezone.utc)
            if req.approval_request_id:
                # Upsert, not update: this workflow has no equivalent of
                # Airflow's insert-at-request-time, so without the INSERT half
                # the approval leaves no audit row at all -- the decision is
                # recorded only in the broker, which other tools do not rely on.
                cur.execute(
                    """INSERT INTO approval_requests
                           (approval_request_id, order_id, total_amount, status,
                            approver, reason, decided_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (approval_request_id) DO UPDATE
                           SET status = EXCLUDED.status,
                               approver = EXCLUDED.approver,
                               reason = EXCLUDED.reason,
                               decided_at = EXCLUDED.decided_at""",
                    (
                        req.approval_request_id,
                        req.order_id,
                        req.total_amount,
                        req.decision,
                        req.approver,
                        req.reason or "",
                        now,
                    ),
                )
            cur.execute(
                "UPDATE orders SET status = %s, updated_at = %s WHERE order_id = %s",
                ("approved" if req.decision == "approved" else "rejected", now, req.order_id),
            )
        conn.commit()
    finally:
        conn.close()

    logger.info("Order %s %s by %s", req.order_id, req.decision, req.approver)
    return {"order_id": req.order_id, "decision": req.decision}


# ---------------------------------------------------------------------------
# Shared: notification (best-effort, never fails the workflow)
# ---------------------------------------------------------------------------

@app.post("/notify")
async def notify(payload: dict) -> dict:
    """Simulated notification sink for DAG 3 and DAG 4.

    Deliberately schema-free and always 200: both workflows call it on their
    success *and* failure paths with different bodies, and a notification failure
    must never fail the run (graceful degradation, per the DAG 3 spec).
    """
    logger.info("Notification: %s", payload)
    return {
        "notification_sent": True,
        "channel": "simulated",
        "sent_at": datetime.now(timezone.utc).isoformat(),
        "echo": payload,
    }

"""
Shared Temporal activities: DB operations, API calls, notifications.

All activities are stateless functions decorated with @activity.defn.
They communicate with Postgres directly (no Secrets Manager -- credentials
are passed via the shared DB_CONFIG or read from environment variables)
and call the shared services over HTTP.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import uuid
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import httpx
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
from temporalio import activity

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared configuration
# ---------------------------------------------------------------------------

DB_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": int(os.environ.get("POSTGRES_PORT", "5432")),
    "database": os.environ.get("POSTGRES_DB", "orchestration"),
    "user": os.environ.get("POSTGRES_USER", "orchestration"),
    "password": os.environ.get("POSTGRES_PASSWORD", "orchestration"),
}

CALLBACK_FETCH_SERVICE_URL = os.environ.get(
    "CALLBACK_FETCH_SERVICE_URL", "http://callback-fetch-service:8090"
)
APPROVAL_SERVICE_URL = os.environ.get(
    "APPROVAL_SERVICE_URL", "http://approval-service:8091"
)
SHIPPING_SERVICE_URL = os.environ.get(
    "SHIPPING_SERVICE_URL", "http://shipping-service:8092"
)
SIGNAL_SERVER_URL = os.environ.get(
    "SIGNAL_SERVER_URL", "http://localhost:8095"
)


def _get_db_connection() -> psycopg2.extensions.connection:
    """Return a fresh Postgres connection using shared config."""
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        database=DB_CONFIG["database"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )


# ===========================================================================
# DAG 1 -- CSV ETL Pipeline activities
# ===========================================================================


@dataclass
class UnzipInput:
    zip_file_path: str
    extract_dir: str = "/tmp/extracted"


@dataclass
class UnzipOutput:
    csv_paths: list[str]


@activity.defn
async def unzip_file(input: UnzipInput) -> UnzipOutput:
    """Extract CSVs from a ZIP archive on the local filesystem."""
    activity.logger.info("Unzipping %s -> %s", input.zip_file_path, input.extract_dir)
    os.makedirs(input.extract_dir, exist_ok=True)

    csv_paths: list[str] = []
    with zipfile.ZipFile(input.zip_file_path, "r") as zf:
        for filename in zf.namelist():
            if not filename.endswith(".csv"):
                continue
            dest = os.path.join(input.extract_dir, os.path.basename(filename))
            with zf.open(filename) as src, open(dest, "wb") as dst:
                dst.write(src.read())
            csv_paths.append(dest)

    activity.logger.info("Extracted %d CSV files", len(csv_paths))
    return UnzipOutput(csv_paths=csv_paths)


@dataclass
class LoadCSVInput:
    csv_path: str


@dataclass
class LoadCSVOutput:
    table: str
    rows_loaded: int


@activity.defn
async def load_csv_to_postgres(input: LoadCSVInput) -> LoadCSVOutput:
    """Load a single CSV file into a Postgres table (truncate-and-reload)."""
    filename = os.path.basename(input.csv_path)
    table_name = filename.replace(".csv", "").lower()
    activity.logger.info("Loading %s into table '%s'", input.csv_path, table_name)

    with open(input.csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        return LoadCSVOutput(table=table_name, rows_loaded=0)

    columns = list(rows[0].keys())

    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            col_defs = ", ".join(f'"{col}" TEXT' for col in columns)
            cur.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({col_defs})')
            cur.execute(f'TRUNCATE TABLE "{table_name}"')

            buf = io.StringIO()
            writer = csv.DictWriter(buf, fieldnames=columns)
            writer.writerows(rows)
            buf.seek(0)

            cur.copy_expert(
                f"""COPY "{table_name}" ({", ".join(f'"{c}"' for c in columns)})
                    FROM STDIN WITH CSV""",
                buf,
            )
        conn.commit()
    finally:
        conn.close()

    activity.logger.info("Loaded %d rows into '%s'", len(rows), table_name)
    return LoadCSVOutput(table=table_name, rows_loaded=len(rows))


@dataclass
class SQLTransformOutput:
    table: str
    row_count: int


TRANSFORM_SQL = """
CREATE TABLE IF NOT EXISTS combined_report AS
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
JOIN products p ON o.product_id = p.product_id;
"""


@activity.defn
async def run_sql_transform() -> SQLTransformOutput:
    """Run the SQL JOIN transform to produce combined_report."""
    activity.logger.info("Running SQL transform")

    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS combined_report")
            cur.execute(TRANSFORM_SQL)
            cur.execute("SELECT COUNT(*) FROM combined_report")
            row_count = cur.fetchone()[0]
        conn.commit()
    finally:
        conn.close()

    activity.logger.info("Transform complete: %d rows in combined_report", row_count)
    return SQLTransformOutput(table="combined_report", row_count=row_count)


@dataclass
class ConvertToParquetInput:
    table: str
    output_path: str = "/tmp/output"


@dataclass
class ConvertToParquetOutput:
    parquet_path: str
    row_count: int


@activity.defn
async def convert_to_parquet(input: ConvertToParquetInput) -> ConvertToParquetOutput:
    """Read a Postgres table and write it as a Parquet file."""
    os.makedirs(input.output_path, exist_ok=True)
    output_file = os.path.join(input.output_path, f"{input.table}.parquet")
    activity.logger.info("Converting table '%s' to Parquet at %s", input.table, output_file)

    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT * FROM "{input.table}"')
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
    finally:
        conn.close()

    # Build a PyArrow table and write to Parquet
    arrays = []
    for col_idx in range(len(columns)):
        col_data = [row[col_idx] for row in rows]
        arrays.append(pa.array(col_data, type=pa.string()))

    table = pa.table(dict(zip(columns, arrays)))
    pq.write_table(table, output_file)

    activity.logger.info("Wrote %d rows to %s", len(rows), output_file)
    return ConvertToParquetOutput(parquet_path=output_file, row_count=len(rows))


# ===========================================================================
# DAG 2 -- API Fan-Out activities
# ===========================================================================


@dataclass
class SubmitAsyncFetchInput:
    url: str
    callback_url: str
    headers: dict[str, str] = field(default_factory=dict)


@dataclass
class SubmitAsyncFetchOutput:
    correlation_id: str
    status: str


@activity.defn
async def submit_async_fetch(input: SubmitAsyncFetchInput) -> SubmitAsyncFetchOutput:
    """POST to the callback-fetch-service to start an async fetch."""
    correlation_id = str(uuid.uuid4())
    activity.logger.info(
        "Submitting async fetch: url=%s, correlation_id=%s", input.url, correlation_id
    )

    payload = {
        "url": input.url,
        "headers": input.headers,
        "callback_url": input.callback_url,
        "correlation_id": correlation_id,
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(
            f"{CALLBACK_FETCH_SERVICE_URL}/fetch-async",
            json=payload,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "orchestration-bakeoff/1.0",
            },
        )

    if response.status_code != 202:
        raise RuntimeError(
            f"Callback Fetch Service returned {response.status_code}: "
            f"{response.text[:500]}"
        )

    return SubmitAsyncFetchOutput(correlation_id=correlation_id, status="submitted")


@dataclass
class FetchItemDetailInput:
    item_id: str
    name: str
    detail_url: str
    headers: dict[str, str] = field(default_factory=dict)


@dataclass
class FetchItemDetailOutput:
    id: str
    name: str
    detail: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


@activity.defn
async def fetch_item_detail(input: FetchItemDetailInput) -> FetchItemDetailOutput:
    """Fetch detailed information for a single item."""
    activity.logger.info("Fetching detail for item %s from %s", input.item_id, input.detail_url)

    request_headers = {"User-Agent": "orchestration-bakeoff/1.0"}
    request_headers.update(input.headers)

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(input.detail_url, headers=request_headers)

    if response.status_code != 200:
        raise RuntimeError(
            f"Detail API request for {input.item_id} failed with status "
            f"{response.status_code}: {response.text[:500]}"
        )

    detail = response.json()
    return FetchItemDetailOutput(id=input.item_id, name=input.name, detail=detail)


@dataclass
class CombineResultsInput:
    source_url: str
    results: list[dict[str, Any]]


@dataclass
class CombineResultsOutput:
    status: str
    source_url: str
    total_items: int
    successful: int
    failed: int
    results: list[dict[str, Any]]
    errors: list[dict[str, Any]]


@activity.defn
async def combine_results(input: CombineResultsInput) -> CombineResultsOutput:
    """Merge all fan-out API results into a summary."""
    combined = []
    errors = []

    for result in input.results:
        if result.get("error"):
            errors.append({"id": result.get("id"), "error": result["error"]})
        else:
            combined.append({
                "id": result["id"],
                "name": result["name"],
                "detail": result.get("detail", {}),
            })

    return CombineResultsOutput(
        status="success",
        source_url=input.source_url,
        total_items=len(input.results),
        successful=len(combined),
        failed=len(errors),
        results=combined,
        errors=errors,
    )


# ===========================================================================
# DAG 3 -- Payment Processing activities
# ===========================================================================


@dataclass
class ValidatePaymentInput:
    payment_id: str
    amount: float
    currency: str
    from_account: str
    to_account: str
    idempotency_key: str | None = None


@dataclass
class ValidationResult:
    is_valid: bool
    reason: str | None = None


@dataclass
class ValidatePaymentOutput:
    payment_id: str
    amount: float
    currency: str
    from_account: str
    to_account: str
    idempotency_key: str
    validation: ValidationResult


@activity.defn
async def validate_payment(input: ValidatePaymentInput) -> ValidatePaymentOutput:
    """Validate a payment: account exists, sufficient balance, no duplicates."""
    idempotency_key = input.idempotency_key or input.payment_id
    activity.logger.info("Validating payment %s", input.payment_id)

    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            # Check source account
            cur.execute(
                "SELECT balance, status FROM accounts WHERE account_id = %s",
                (input.from_account,),
            )
            row = cur.fetchone()
            if row is None:
                return ValidatePaymentOutput(
                    payment_id=input.payment_id,
                    amount=input.amount,
                    currency=input.currency,
                    from_account=input.from_account,
                    to_account=input.to_account,
                    idempotency_key=idempotency_key,
                    validation=ValidationResult(
                        is_valid=False,
                        reason=f"Source account {input.from_account} not found",
                    ),
                )

            balance, status = row
            if status != "active":
                return ValidatePaymentOutput(
                    payment_id=input.payment_id,
                    amount=input.amount,
                    currency=input.currency,
                    from_account=input.from_account,
                    to_account=input.to_account,
                    idempotency_key=idempotency_key,
                    validation=ValidationResult(
                        is_valid=False,
                        reason=f"Source account {input.from_account} is {status}",
                    ),
                )

            if float(balance) < input.amount:
                return ValidatePaymentOutput(
                    payment_id=input.payment_id,
                    amount=input.amount,
                    currency=input.currency,
                    from_account=input.from_account,
                    to_account=input.to_account,
                    idempotency_key=idempotency_key,
                    validation=ValidationResult(
                        is_valid=False,
                        reason=f"Insufficient balance: {balance} < {input.amount}",
                    ),
                )

            # Check destination account
            cur.execute(
                "SELECT status FROM accounts WHERE account_id = %s",
                (input.to_account,),
            )
            row = cur.fetchone()
            if row is None:
                return ValidatePaymentOutput(
                    payment_id=input.payment_id,
                    amount=input.amount,
                    currency=input.currency,
                    from_account=input.from_account,
                    to_account=input.to_account,
                    idempotency_key=idempotency_key,
                    validation=ValidationResult(
                        is_valid=False,
                        reason=f"Destination account {input.to_account} not found",
                    ),
                )
            if row[0] != "active":
                return ValidatePaymentOutput(
                    payment_id=input.payment_id,
                    amount=input.amount,
                    currency=input.currency,
                    from_account=input.from_account,
                    to_account=input.to_account,
                    idempotency_key=idempotency_key,
                    validation=ValidationResult(
                        is_valid=False,
                        reason=f"Destination account {input.to_account} is {row[0]}",
                    ),
                )

            # Check for duplicate (idempotency)
            cur.execute(
                "SELECT status FROM transactions WHERE idempotency_key = %s",
                (idempotency_key,),
            )
            existing = cur.fetchone()
            if existing is not None:
                return ValidatePaymentOutput(
                    payment_id=input.payment_id,
                    amount=input.amount,
                    currency=input.currency,
                    from_account=input.from_account,
                    to_account=input.to_account,
                    idempotency_key=idempotency_key,
                    validation=ValidationResult(
                        is_valid=False,
                        reason=f"Duplicate payment: existing transaction with status {existing[0]}",
                    ),
                )
    finally:
        conn.close()

    return ValidatePaymentOutput(
        payment_id=input.payment_id,
        amount=input.amount,
        currency=input.currency,
        from_account=input.from_account,
        to_account=input.to_account,
        idempotency_key=idempotency_key,
        validation=ValidationResult(is_valid=True, reason=None),
    )


class PaymentDeclined(Exception):
    """Non-retryable: card declined by issuing bank."""
    pass


@dataclass
class ProcessPaymentInput:
    payment_id: str
    amount: float
    currency: str
    from_account: str
    to_account: str
    idempotency_key: str


@dataclass
class PaymentResult:
    status: str
    gateway_transaction_id: str
    amount_charged: float
    currency: str


@dataclass
class ProcessPaymentOutput:
    payment_id: str
    amount: float
    currency: str
    from_account: str
    to_account: str
    idempotency_key: str
    payment_result: PaymentResult


@activity.defn
async def process_payment(input: ProcessPaymentInput) -> ProcessPaymentOutput:
    """Call simulated payment gateway. Flaky -- raises retriable or non-retriable errors."""
    import random

    activity.logger.info("Processing payment %s for %s %s", input.payment_id, input.amount, input.currency)

    roll = random.random()

    if roll < 0.05:
        raise PaymentDeclined(
            json.dumps({
                "payment_id": input.payment_id,
                "reason": "Card declined by issuing bank",
                "decline_code": "insufficient_funds",
            })
        )
    elif roll < 0.20:
        raise RuntimeError(f"Payment gateway returned 500 for payment {input.payment_id}")
    elif roll < 0.40:
        raise TimeoutError(f"Payment gateway timed out for payment {input.payment_id}")

    gateway_transaction_id = f"gw-txn-{input.payment_id}-{random.randint(10000, 99999)}"

    return ProcessPaymentOutput(
        payment_id=input.payment_id,
        amount=input.amount,
        currency=input.currency,
        from_account=input.from_account,
        to_account=input.to_account,
        idempotency_key=input.idempotency_key,
        payment_result=PaymentResult(
            status="success",
            gateway_transaction_id=gateway_transaction_id,
            amount_charged=input.amount,
            currency=input.currency,
        ),
    )


@dataclass
class UpdateDatabaseInput:
    payment_id: str
    amount: float
    currency: str
    from_account: str
    to_account: str
    idempotency_key: str
    gateway_transaction_id: str


@dataclass
class UpdateDatabaseOutput:
    payment_id: str
    status: str
    recorded_at: str | None = None
    reason: str | None = None


@activity.defn
async def update_payment_database(input: UpdateDatabaseInput) -> UpdateDatabaseOutput:
    """Record the payment in Postgres: debit/credit accounts, write transaction record."""
    activity.logger.info("Updating database for payment %s", input.payment_id)

    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            now = datetime.now(timezone.utc).isoformat()

            # Idempotency check
            cur.execute(
                "SELECT id FROM transactions WHERE idempotency_key = %s",
                (input.idempotency_key,),
            )
            if cur.fetchone() is not None:
                conn.rollback()
                return UpdateDatabaseOutput(
                    payment_id=input.payment_id,
                    status="skipped",
                    reason="Transaction already recorded (idempotent)",
                )

            # Debit source
            cur.execute(
                "UPDATE accounts SET balance = balance - %s, updated_at = %s WHERE account_id = %s",
                (input.amount, now, input.from_account),
            )
            # Credit destination
            cur.execute(
                "UPDATE accounts SET balance = balance + %s, updated_at = %s WHERE account_id = %s",
                (input.amount, now, input.to_account),
            )
            # Record transaction
            cur.execute(
                """INSERT INTO transactions
                   (payment_id, idempotency_key, from_account, to_account,
                    amount, currency, status, gateway_transaction_id, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    input.payment_id,
                    input.idempotency_key,
                    input.from_account,
                    input.to_account,
                    input.amount,
                    input.currency,
                    "completed",
                    input.gateway_transaction_id,
                    now,
                ),
            )
        conn.commit()
    finally:
        conn.close()

    return UpdateDatabaseOutput(
        payment_id=input.payment_id,
        status="success",
        recorded_at=now,
    )


@dataclass
class SendNotificationInput:
    payment_id: str
    status: str
    amount: float | None = None
    currency: str | None = None
    gateway_transaction_id: str | None = None
    message: str | None = None


@dataclass
class SendNotificationOutput:
    payment_id: str
    notification_status: str
    subject: str


@activity.defn
async def send_payment_notification(input: SendNotificationInput) -> SendNotificationOutput:
    """Send a simulated payment notification."""
    if input.status == "failed":
        message = input.message or "Payment processing failed"
        subject = f"Payment Failed: {input.payment_id}"
        body = f"Payment {input.payment_id} for {input.amount} {input.currency} has failed.\nReason: {message}"
    else:
        subject = f"Payment Successful: {input.payment_id}"
        body = (
            f"Payment {input.payment_id} for {input.amount} {input.currency} was processed successfully.\n"
            f"Gateway Transaction ID: {input.gateway_transaction_id or 'N/A'}"
        )

    activity.logger.info("NOTIFICATION: %s", subject)
    activity.logger.info("BODY: %s", body)

    return SendNotificationOutput(
        payment_id=input.payment_id,
        notification_status="sent",
        subject=subject,
    )


@dataclass
class HandlePaymentFailureInput:
    payment_id: str
    amount: float | None = None
    currency: str | None = None
    from_account: str | None = None
    to_account: str | None = None
    idempotency_key: str | None = None
    error_message: str = "Unknown error"


@dataclass
class HandlePaymentFailureOutput:
    payment_id: str
    status: str
    failure_message: str


@activity.defn
async def handle_payment_failure(input: HandlePaymentFailureInput) -> HandlePaymentFailureOutput:
    """Record a payment failure in the database."""
    idempotency_key = input.idempotency_key or input.payment_id
    activity.logger.info("Recording payment failure for %s: %s", input.payment_id, input.error_message)

    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            now = datetime.now(timezone.utc).isoformat()

            cur.execute(
                "SELECT id FROM transactions WHERE idempotency_key = %s",
                (idempotency_key,),
            )
            if cur.fetchone() is None:
                cur.execute(
                    """INSERT INTO transactions
                       (payment_id, idempotency_key, from_account, to_account,
                        amount, currency, status, error_message, created_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (
                        input.payment_id,
                        idempotency_key,
                        input.from_account,
                        input.to_account,
                        input.amount,
                        input.currency,
                        "failed",
                        input.error_message,
                        now,
                    ),
                )
        conn.commit()
    finally:
        conn.close()

    return HandlePaymentFailureOutput(
        payment_id=input.payment_id,
        status="failed",
        failure_message=input.error_message,
    )


# ===========================================================================
# DAG 4 -- Order Fulfillment activities
# ===========================================================================


@dataclass
class OrderItem:
    sku: str
    quantity: int
    unit_price: float = 0.0


@dataclass
class ValidateOrderInput:
    order_id: str
    customer_id: str
    items: list[OrderItem]
    shipping_address: dict[str, str] = field(default_factory=dict)
    approval_threshold: float = 500.00


@dataclass
class OrderValidation:
    is_valid: bool
    reason: str | None = None


@dataclass
class ValidateOrderOutput:
    order_id: str
    customer_id: str
    items: list[OrderItem]
    shipping_address: dict[str, str]
    total_amount: float
    approval_threshold: float
    validation: OrderValidation


@activity.defn
async def validate_order(input: ValidateOrderInput) -> ValidateOrderOutput:
    """Validate that all SKUs exist, customer is active, and compute total amount."""
    activity.logger.info("Validating order %s for customer %s", input.order_id, input.customer_id)

    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM customers WHERE customer_id = %s",
                (input.customer_id,),
            )
            row = cur.fetchone()
            if not row:
                return ValidateOrderOutput(
                    order_id=input.order_id,
                    customer_id=input.customer_id,
                    items=input.items,
                    shipping_address=input.shipping_address,
                    total_amount=0.0,
                    approval_threshold=input.approval_threshold,
                    validation=OrderValidation(
                        is_valid=False,
                        reason=f"Customer {input.customer_id} not found",
                    ),
                )
            if row[0] != "active":
                return ValidateOrderOutput(
                    order_id=input.order_id,
                    customer_id=input.customer_id,
                    items=input.items,
                    shipping_address=input.shipping_address,
                    total_amount=0.0,
                    approval_threshold=input.approval_threshold,
                    validation=OrderValidation(
                        is_valid=False,
                        reason=f"Customer {input.customer_id} is {row[0]}",
                    ),
                )

            total_amount = 0.0
            enriched_items: list[OrderItem] = []
            for item in input.items:
                cur.execute(
                    "SELECT available_quantity, unit_price FROM inventory WHERE sku = %s",
                    (item.sku,),
                )
                row = cur.fetchone()
                if not row:
                    return ValidateOrderOutput(
                        order_id=input.order_id,
                        customer_id=input.customer_id,
                        items=input.items,
                        shipping_address=input.shipping_address,
                        total_amount=0.0,
                        approval_threshold=input.approval_threshold,
                        validation=OrderValidation(
                            is_valid=False,
                            reason=f"SKU {item.sku} not found",
                        ),
                    )

                available, unit_price = row
                if available < item.quantity:
                    return ValidateOrderOutput(
                        order_id=input.order_id,
                        customer_id=input.customer_id,
                        items=input.items,
                        shipping_address=input.shipping_address,
                        total_amount=0.0,
                        approval_threshold=input.approval_threshold,
                        validation=OrderValidation(
                            is_valid=False,
                            reason=f"Insufficient stock for {item.sku}: requested {item.quantity}, available {available}",
                        ),
                    )
                enriched_items.append(OrderItem(
                    sku=item.sku,
                    quantity=item.quantity,
                    unit_price=float(unit_price),
                ))
                total_amount += float(unit_price) * item.quantity
    finally:
        conn.close()

    return ValidateOrderOutput(
        order_id=input.order_id,
        customer_id=input.customer_id,
        items=enriched_items,
        shipping_address=input.shipping_address,
        total_amount=total_amount,
        approval_threshold=input.approval_threshold,
        validation=OrderValidation(is_valid=True, reason=None),
    )


@dataclass
class ReserveInventoryInput:
    order_id: str
    customer_id: str
    items: list[OrderItem]


@dataclass
class ReserveInventoryOutput:
    reservation_id: str
    items_reserved: list[str]
    reserved_at: str
    idempotent: bool = False


@activity.defn
async def reserve_inventory(input: ReserveInventoryInput) -> ReserveInventoryOutput:
    """Atomically reserve inventory for all items in an order."""
    activity.logger.info("Reserving inventory for order %s", input.order_id)

    reservation_id = f"RES-{uuid.uuid4().hex[:12].upper()}"

    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            # Idempotency check
            cur.execute(
                "SELECT reservation_id FROM inventory_reservations WHERE order_id = %s AND status = 'reserved' LIMIT 1",
                (input.order_id,),
            )
            existing = cur.fetchone()
            if existing:
                return ReserveInventoryOutput(
                    reservation_id=existing[0],
                    items_reserved=[i.sku for i in input.items],
                    reserved_at=datetime.now(timezone.utc).isoformat(),
                    idempotent=True,
                )

            items_reserved = []
            for item in input.items:
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
                    raise RuntimeError(f"InsufficientStock: Cannot reserve {item.quantity} of {item.sku}")

                cur.execute(
                    """INSERT INTO inventory_reservations (reservation_id, order_id, sku, quantity, status)
                       VALUES (%s, %s, %s, %s, 'reserved')""",
                    (f"{reservation_id}-{item.sku}", input.order_id, item.sku, item.quantity),
                )
                items_reserved.append(item.sku)

            total = sum(i.quantity * i.unit_price for i in input.items)
            cur.execute(
                """INSERT INTO orders (order_id, customer_id, total_amount, status)
                   VALUES (%s, %s, %s, 'reserved')
                   ON CONFLICT (order_id) DO UPDATE SET status = 'reserved', updated_at = NOW()""",
                (input.order_id, input.customer_id, total),
            )
            conn.commit()

        return ReserveInventoryOutput(
            reservation_id=reservation_id,
            items_reserved=items_reserved,
            reserved_at=datetime.now(timezone.utc).isoformat(),
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@dataclass
class ReleaseInventoryInput:
    order_id: str
    reservation_id: str


@dataclass
class ReleaseInventoryOutput:
    order_id: str
    released: int
    status: str


@activity.defn
async def release_inventory(input: ReleaseInventoryInput) -> ReleaseInventoryOutput:
    """Saga compensation: release inventory reservations for an order."""
    activity.logger.info("Releasing inventory for order %s (reservation %s)", input.order_id, input.reservation_id)

    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT reservation_id, sku, quantity
                   FROM inventory_reservations
                   WHERE order_id = %s AND status = 'reserved'""",
                (input.order_id,),
            )
            reservations = cur.fetchall()

            if not reservations:
                return ReleaseInventoryOutput(
                    order_id=input.order_id,
                    released=0,
                    status="no_reservations_to_release",
                )

            released = 0
            for res_id, sku, quantity in reservations:
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
                    (datetime.now(timezone.utc), res_id),
                )
                released += 1

            conn.commit()
        return ReleaseInventoryOutput(
            order_id=input.order_id,
            released=released,
            status="inventory_released",
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@dataclass
class RequestApprovalInput:
    order_id: str
    customer_id: str
    total_amount: float
    items: list[OrderItem]
    workflow_id: str
    run_id: str


@activity.defn
async def request_approval(input: RequestApprovalInput) -> str:
    """Send approval request to the Approval Service. Returns the approval_request_id."""
    approval_request_id = f"APR-{uuid.uuid4().hex[:12].upper()}"
    activity.logger.info(
        "Requesting manager approval for order %s (amount=%.2f)",
        input.order_id,
        input.total_amount,
    )

    items_summary = ", ".join(f"{item.quantity}x {item.sku}" for item in input.items)

    # Build callback URL that points to our signal_server, which will relay
    # the approval decision as a Temporal signal.
    callback_url = (
        f"{SIGNAL_SERVER_URL}/approval-callback"
        f"?workflow_id={input.workflow_id}"
        f"&run_id={input.run_id}"
    )

    payload = {
        "approval_request_id": approval_request_id,
        "order_id": input.order_id,
        "total_amount": input.total_amount,
        "customer_id": input.customer_id,
        "callback_url": callback_url,
        "items_summary": items_summary,
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(
            f"{APPROVAL_SERVICE_URL}/approval-requests",
            json=payload,
            headers={"Content-Type": "application/json"},
        )

    if response.status_code not in (200, 201):
        raise RuntimeError(
            f"Approval Service returned {response.status_code}: {response.text[:500]}"
        )

    # Record in DB
    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO approval_requests (approval_request_id, order_id, total_amount, status)
                   VALUES (%s, %s, %s, 'pending')
                   ON CONFLICT (approval_request_id) DO NOTHING""",
                (approval_request_id, input.order_id, input.total_amount),
            )
            cur.execute(
                "UPDATE orders SET status = 'pending_approval', updated_at = NOW() WHERE order_id = %s",
                (input.order_id,),
            )
        conn.commit()
    finally:
        conn.close()

    return approval_request_id


@dataclass
class RecordApprovalDecisionInput:
    approval_request_id: str
    order_id: str
    decision: str
    approver: str | None = None
    reason: str = ""


@activity.defn
async def record_approval_decision(input: RecordApprovalDecisionInput) -> None:
    """Persist the approval decision to the database."""
    activity.logger.info(
        "Recording approval decision for order %s: %s", input.order_id, input.decision
    )

    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            now = datetime.now(timezone.utc)

            if input.approval_request_id:
                cur.execute(
                    """UPDATE approval_requests
                       SET status = %s, approver = %s, reason = %s, decided_at = %s
                       WHERE approval_request_id = %s""",
                    (input.decision, input.approver, input.reason, now, input.approval_request_id),
                )

            new_status = "approved" if input.decision == "approved" else "rejected"
            cur.execute(
                "UPDATE orders SET status = %s, updated_at = %s WHERE order_id = %s",
                (new_status, now, input.order_id),
            )
        conn.commit()
    finally:
        conn.close()


@dataclass
class ShippingInput:
    order_id: str
    items: list[OrderItem]
    shipping_address: dict[str, str]


@dataclass
class ShippingOutput:
    shipment_id: str
    tracking_number: str
    carrier: str
    estimated_delivery: str


class InvalidAddress(Exception):
    """Non-retryable: shipping address is invalid."""
    pass


@activity.defn
async def call_shipping_api(input: ShippingInput) -> ShippingOutput:
    """Call the Shipping Service API to create a shipment."""
    activity.logger.info("Calling shipping API for order %s", input.order_id)

    idempotency_key = f"{input.order_id}-ship"

    payload = {
        "order_id": input.order_id,
        "items": [{"sku": i.sku, "quantity": i.quantity, "unit_price": i.unit_price} for i in input.items],
        "shipping_address": input.shipping_address,
        "idempotency_key": idempotency_key,
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            f"{SHIPPING_SERVICE_URL}/shipments",
            json=payload,
            headers={"Content-Type": "application/json"},
        )

    body = response.json()

    if response.status_code == 200:
        return ShippingOutput(
            shipment_id=body["shipment_id"],
            tracking_number=body["tracking_number"],
            carrier=body["carrier"],
            estimated_delivery=body["estimated_delivery"],
        )

    error_type = "Unknown"
    message = str(body)
    if isinstance(body.get("detail"), dict):
        error_type = body["detail"].get("error_type", "Unknown")
        message = body["detail"].get("message", str(body))

    if error_type == "InvalidAddress":
        raise InvalidAddress(message)
    else:
        raise RuntimeError(f"Shipping error ({response.status_code}): {message}")


@dataclass
class UpdateOrderStatusInput:
    order_id: str
    status: str
    shipment_id: str | None = None
    tracking_number: str | None = None
    failure_reason: str | None = None


@dataclass
class UpdateOrderStatusOutput:
    order_id: str
    status: str
    updated_at: str


@activity.defn
async def update_order_status(input: UpdateOrderStatusInput) -> UpdateOrderStatusOutput:
    """Update the order record in the database."""
    activity.logger.info("Updating order %s to status '%s'", input.order_id, input.status)

    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            now = datetime.now(timezone.utc)
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
                    input.status,
                    input.shipment_id,
                    input.tracking_number,
                    input.failure_reason,
                    now,
                    input.order_id,
                ),
            )
            result = cur.fetchone()
            conn.commit()

            if not result:
                raise RuntimeError(f"Order {input.order_id} not found")

        return UpdateOrderStatusOutput(
            order_id=result[0],
            status=result[1],
            updated_at=now.isoformat(),
        )
    finally:
        conn.close()


@dataclass
class SendOrderNotificationInput:
    order_id: str
    status: str
    tracking_number: str | None = None
    carrier: str | None = None
    estimated_delivery: str | None = None
    failure_reason: str | None = None


@dataclass
class SendOrderNotificationOutput:
    notification_sent: bool
    order_id: str
    status: str
    sent_at: str


@activity.defn
async def send_order_notification(input: SendOrderNotificationInput) -> SendOrderNotificationOutput:
    """Send a simulated notification for order status changes."""
    now = datetime.now(timezone.utc).isoformat()

    if input.status == "shipped":
        message = (
            f"Your order {input.order_id} has been shipped! "
            f"Tracking: {input.tracking_number or 'N/A'} "
            f"via {input.carrier or 'N/A'}. "
            f"Estimated delivery: {input.estimated_delivery or 'N/A'}."
        )
    elif input.status == "cancelled":
        message = (
            f"Your order {input.order_id} has been cancelled. "
            f"Reason: {input.failure_reason or 'N/A'}."
        )
    else:
        message = f"Order {input.order_id} status update: {input.status}."

    activity.logger.info("NOTIFICATION: %s", message)

    return SendOrderNotificationOutput(
        notification_sent=True,
        order_id=input.order_id,
        status=input.status,
        sent_at=now,
    )

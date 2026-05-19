"""
Shared dataclasses for typed inputs/outputs across all Flyte DAGs.

These types enforce Flyte's strict typing at workflow boundaries. Every task
input and output is a concrete Python type — no untyped dicts crossing task
boundaries.

All dataclasses use ``@dataclass_json`` so Flytekit can serialize them
automatically when passing values between tasks.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from dataclasses_json import dataclass_json


# ---------------------------------------------------------------------------
# Database configuration (used by all DAGs that talk to Postgres)
# ---------------------------------------------------------------------------

@dataclass_json
@dataclass
class DBConfig:
    """Connection parameters for the shared Postgres instance."""
    host: str = "postgres"
    port: int = 5432
    database: str = "orchestration"
    user: str = "orchestration"
    password: str = "orchestration"


# ---------------------------------------------------------------------------
# DAG 1: CSV ETL Pipeline
# ---------------------------------------------------------------------------

@dataclass_json
@dataclass
class ETLInput:
    """Top-level input for the CSV ETL pipeline."""
    zip_file_path: str          # Local path to the ZIP archive
    extract_dir: str = "/tmp/csv_extract"
    output_dir: str = "/tmp/parquet_output"
    db_config: DBConfig = field(default_factory=DBConfig)


@dataclass_json
@dataclass
class CSVLoadResult:
    """Result of loading a single CSV file into Postgres."""
    table: str
    rows_loaded: int


@dataclass_json
@dataclass
class TransformResult:
    """Result of the SQL JOIN transformation step."""
    table: str
    row_count: int


@dataclass_json
@dataclass
class ETLOutput:
    """Final output of the CSV ETL pipeline."""
    status: str
    parquet_path: str
    row_count: int
    tables_loaded: List[CSVLoadResult]


# ---------------------------------------------------------------------------
# DAG 2: API Fan-Out with Async Callback
# ---------------------------------------------------------------------------

@dataclass_json
@dataclass
class RequestConfig:
    """Configuration for outbound API requests."""
    callback_fetch_service_url: str = "http://callback-fetch-service:8090"
    api_key: str = ""
    user_agent: str = "orchestration-bakeoff/1.0"


@dataclass_json
@dataclass
class FanOutInput:
    """Top-level input for the API fan-out workflow."""
    url: str
    request_config: RequestConfig = field(default_factory=RequestConfig)


@dataclass_json
@dataclass
class FetchResult:
    """Normalized result from the async fetch / callback."""
    status: str
    correlation_id: str
    body: str  # JSON-encoded response body
    url: str = ""


@dataclass_json
@dataclass
class FanOutItem:
    """A single item extracted from the initial fetch, to be fanned out."""
    id: str
    name: str
    detail_url: str


@dataclass_json
@dataclass
class ItemDetail:
    """Detail fetched for a single fan-out item."""
    id: str
    name: str
    detail: str  # JSON-encoded detail payload


@dataclass_json
@dataclass
class ItemError:
    """Records a failed detail fetch."""
    id: str
    error: str


@dataclass_json
@dataclass
class CombinedResult:
    """Merged summary of all fan-out API results."""
    status: str
    source_url: str
    total_items: int
    successful: int
    failed: int
    results: List[ItemDetail]
    errors: List[ItemError]


# ---------------------------------------------------------------------------
# DAG 3: Payment Processing
# ---------------------------------------------------------------------------

@dataclass_json
@dataclass
class PaymentInput:
    """Top-level input for payment processing."""
    payment_id: str
    amount: float
    currency: str
    from_account: str
    to_account: str
    idempotency_key: str = ""
    db_config: DBConfig = field(default_factory=DBConfig)


@dataclass_json
@dataclass
class ValidationResult:
    """Outcome of payment validation."""
    is_valid: bool
    reason: str = ""


@dataclass_json
@dataclass
class PaymentValidated:
    """Full state after validation, carrying forward all fields."""
    payment_id: str
    amount: float
    currency: str
    from_account: str
    to_account: str
    idempotency_key: str
    db_config: DBConfig
    validation: ValidationResult = field(default_factory=lambda: ValidationResult(is_valid=False))


@dataclass_json
@dataclass
class GatewayResult:
    """Response from the payment gateway."""
    status: str
    gateway_transaction_id: str
    amount_charged: float
    currency: str


@dataclass_json
@dataclass
class PaymentProcessed:
    """State after successful payment gateway call."""
    payment_id: str
    amount: float
    currency: str
    from_account: str
    to_account: str
    idempotency_key: str
    db_config: DBConfig
    payment_result: GatewayResult = field(
        default_factory=lambda: GatewayResult(
            status="", gateway_transaction_id="", amount_charged=0.0, currency=""
        )
    )


@dataclass_json
@dataclass
class DBUpdateResult:
    """Outcome of the database update step."""
    status: str
    reason: str = ""
    recorded_at: str = ""


@dataclass_json
@dataclass
class NotificationResult:
    """Outcome of a notification send."""
    payment_id: str
    status: str
    subject: str = ""
    channel: str = "simulated"


@dataclass_json
@dataclass
class PaymentFailureResult:
    """Data produced by the failure handler."""
    payment_id: str
    amount: float
    currency: str
    status: str
    failure_message: str


@dataclass_json
@dataclass
class PaymentOutput:
    """Final output of the payment workflow."""
    payment_id: str
    status: str
    notification: Optional[NotificationResult] = None
    failure: Optional[PaymentFailureResult] = None


# ---------------------------------------------------------------------------
# DAG 4: Order Fulfillment
# ---------------------------------------------------------------------------

@dataclass_json
@dataclass
class OrderItem:
    """A single line item in an order."""
    sku: str
    quantity: int
    unit_price: float


@dataclass_json
@dataclass
class ShippingAddress:
    """Delivery address for the order."""
    street: str
    city: str
    state: str
    zip_code: str
    country: str = "US"


@dataclass_json
@dataclass
class OrderInput:
    """Top-level input for the order fulfillment workflow."""
    order_id: str
    customer_id: str
    items: List[OrderItem]
    shipping_address: ShippingAddress
    approval_threshold: float = 500.00
    db_config: DBConfig = field(default_factory=DBConfig)


@dataclass_json
@dataclass
class OrderValidation:
    """Outcome of order validation."""
    is_valid: bool
    reason: str = ""


@dataclass_json
@dataclass
class OrderValidated:
    """Full state after order validation."""
    order_id: str
    customer_id: str
    items: List[OrderItem]
    shipping_address: ShippingAddress
    approval_threshold: float
    db_config: DBConfig
    total_amount: float = 0.0
    validation: OrderValidation = field(
        default_factory=lambda: OrderValidation(is_valid=False)
    )


@dataclass_json
@dataclass
class ReservationResult:
    """Result of inventory reservation."""
    reservation_id: str
    items_reserved: List[str]
    reserved_at: str
    idempotent: bool = False


@dataclass_json
@dataclass
class ApprovalDecision:
    """Manager approval decision."""
    decision: str  # "approved", "rejected", "expired"
    approver: str = ""
    reason: str = ""
    decided_at: str = ""


@dataclass_json
@dataclass
class ShipmentResult:
    """Result of a shipping API call."""
    shipment_id: str
    tracking_number: str
    carrier: str
    estimated_delivery: str


@dataclass_json
@dataclass
class OrderStatusUpdate:
    """Result of updating the order status in the database."""
    order_id: str
    status: str
    updated_at: str


@dataclass_json
@dataclass
class OrderNotification:
    """Result of sending an order notification."""
    notification_sent: bool
    order_id: str
    status: str
    sent_at: str


@dataclass_json
@dataclass
class CompensationResult:
    """Result of the saga compensation (inventory release)."""
    order_id: str
    released: int
    status: str
    failure_reason: str


@dataclass_json
@dataclass
class OrderOutput:
    """Final output of the order fulfillment workflow."""
    order_id: str
    status: str  # "shipped", "cancelled", "failed"
    shipment: Optional[ShipmentResult] = None
    notification: Optional[OrderNotification] = None
    compensation: Optional[CompensationResult] = None
    failure_reason: Optional[str] = None

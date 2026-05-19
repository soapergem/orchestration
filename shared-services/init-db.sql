-- DAG 1: CSV ETL Pipeline
-- (Tables are created dynamically by the ETL process based on CSV contents)

-- DAG 3: Payment Processing
CREATE TABLE IF NOT EXISTS accounts (
    account_id TEXT PRIMARY KEY,
    account_name TEXT NOT NULL,
    balance NUMERIC(12,2) NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id TEXT PRIMARY KEY,
    idempotency_key TEXT UNIQUE,
    source_account_id TEXT NOT NULL REFERENCES accounts(account_id),
    destination_account_id TEXT NOT NULL REFERENCES accounts(account_id),
    amount NUMERIC(12,2) NOT NULL,
    currency TEXT NOT NULL DEFAULT 'USD',
    status TEXT NOT NULL DEFAULT 'pending',
    gateway_transaction_id TEXT,
    failure_reason TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Seed payment accounts
INSERT INTO accounts (account_id, account_name, balance, status) VALUES
    ('ACC-001', 'Alice Checking', 5000.00, 'active'),
    ('ACC-002', 'Bob Checking', 3000.00, 'active'),
    ('ACC-003', 'Merchant Account', 0.00, 'active'),
    ('ACC-004', 'Suspended Account', 1000.00, 'suspended')
ON CONFLICT (account_id) DO NOTHING;

-- DAG 4: Order Fulfillment
CREATE TABLE IF NOT EXISTS customers (
    customer_id TEXT PRIMARY KEY,
    customer_name TEXT NOT NULL,
    email TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS inventory (
    sku TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    available_quantity INTEGER NOT NULL DEFAULT 0,
    reserved_quantity INTEGER NOT NULL DEFAULT 0,
    unit_price NUMERIC(10,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL REFERENCES customers(customer_id),
    total_amount NUMERIC(10,2),
    status TEXT NOT NULL DEFAULT 'pending',
    shipment_id TEXT,
    tracking_number TEXT,
    failure_reason TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS inventory_reservations (
    reservation_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL REFERENCES orders(order_id),
    sku TEXT NOT NULL REFERENCES inventory(sku),
    quantity INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'reserved',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    released_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS approval_requests (
    approval_request_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL REFERENCES orders(order_id),
    total_amount NUMERIC(10,2),
    status TEXT NOT NULL DEFAULT 'pending',
    approver TEXT,
    reason TEXT,
    requested_at TIMESTAMPTZ DEFAULT NOW(),
    decided_at TIMESTAMPTZ
);

-- Seed customers
INSERT INTO customers (customer_id, customer_name, email, status) VALUES
    ('CUST-42', 'Jane Doe', 'jane@example.com', 'active'),
    ('CUST-43', 'John Smith', 'john@example.com', 'active'),
    ('CUST-99', 'Inactive User', 'inactive@example.com', 'inactive')
ON CONFLICT (customer_id) DO NOTHING;

-- Seed inventory
INSERT INTO inventory (sku, product_name, available_quantity, reserved_quantity, unit_price) VALUES
    ('WIDGET-A', 'Standard Widget', 100, 0, 29.99),
    ('GADGET-B', 'Premium Gadget', 50, 0, 499.99),
    ('THING-C', 'Basic Thing', 200, 0, 9.99),
    ('RARE-D', 'Rare Item', 2, 0, 1500.00)
ON CONFLICT (sku) DO NOTHING;

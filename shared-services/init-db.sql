-- Per-(runner, dag) schema isolation for the bake-off.
--
-- Every orchestrator/provider ("runner") gets its own set of schemas so no two
-- runners and no two DAGs ever share a table:
--   <ns>_dag1  -- DAG 1 CSV ETL: tables (orders/customers/products/combined_report)
--                are created at runtime by the DAG inside this schema
--   <ns>_dag3  -- DAG 3 payment: accounts, transactions
--   <ns>_dag4  -- DAG 4 order fulfillment: customers, inventory, orders,
--                inventory_reservations, approval_requests
--
-- Onboard a runner with a single call, e.g. SELECT bootstrap_bakeoff('temporal');
-- Each orchestrator sets BAKEOFF_NS=<runner> and its DB layer uses
-- search_path = "<runner>_<dag>". DAG 2 uses no database.

CREATE OR REPLACE FUNCTION bootstrap_bakeoff(ns text) RETURNS void AS $fn$
BEGIN
    EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', ns || '_dag1');
    EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', ns || '_dag3');
    EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', ns || '_dag4');

    -- ---- DAG 3: payment ----
    EXECUTE format($ddl$
        CREATE TABLE IF NOT EXISTS %I.accounts (
            account_id TEXT PRIMARY KEY,
            account_name TEXT NOT NULL,
            balance NUMERIC(12,2) NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'active',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )$ddl$, ns || '_dag3');
    EXECUTE format($ddl$
        CREATE TABLE IF NOT EXISTS %I.transactions (
            id BIGSERIAL PRIMARY KEY,
            payment_id TEXT NOT NULL,
            idempotency_key TEXT UNIQUE,
            from_account TEXT,
            to_account TEXT,
            amount NUMERIC(12,2),
            currency TEXT NOT NULL DEFAULT 'USD',
            status TEXT NOT NULL DEFAULT 'pending',
            gateway_transaction_id TEXT,
            error_message TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )$ddl$, ns || '_dag3');
    EXECUTE format($ddl$
        INSERT INTO %I.accounts (account_id, account_name, balance, status) VALUES
            ('ACC-001', 'Alice Checking', 5000.00, 'active'),
            ('ACC-002', 'Bob Checking', 3000.00, 'active'),
            ('ACC-003', 'Merchant Account', 0.00, 'active'),
            ('ACC-004', 'Suspended Account', 1000.00, 'suspended')
        ON CONFLICT (account_id) DO NOTHING$ddl$, ns || '_dag3');

    -- ---- DAG 4: order fulfillment ----
    EXECUTE format($ddl$
        CREATE TABLE IF NOT EXISTS %I.customers (
            customer_id TEXT PRIMARY KEY,
            customer_name TEXT NOT NULL,
            email TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            created_at TIMESTAMPTZ DEFAULT NOW()
        )$ddl$, ns || '_dag4');
    EXECUTE format($ddl$
        CREATE TABLE IF NOT EXISTS %I.inventory (
            sku TEXT PRIMARY KEY,
            product_name TEXT NOT NULL,
            available_quantity INTEGER NOT NULL DEFAULT 0,
            reserved_quantity INTEGER NOT NULL DEFAULT 0,
            unit_price NUMERIC(10,2) NOT NULL
        )$ddl$, ns || '_dag4');
    EXECUTE format($ddl$
        CREATE TABLE IF NOT EXISTS %I.orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL REFERENCES %I.customers(customer_id),
            total_amount NUMERIC(10,2),
            status TEXT NOT NULL DEFAULT 'pending',
            shipment_id TEXT,
            tracking_number TEXT,
            failure_reason TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )$ddl$, ns || '_dag4', ns || '_dag4');
    EXECUTE format($ddl$
        CREATE TABLE IF NOT EXISTS %I.inventory_reservations (
            reservation_id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL REFERENCES %I.orders(order_id),
            sku TEXT NOT NULL REFERENCES %I.inventory(sku),
            quantity INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'reserved',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            released_at TIMESTAMPTZ
        )$ddl$, ns || '_dag4', ns || '_dag4', ns || '_dag4');
    EXECUTE format($ddl$
        CREATE TABLE IF NOT EXISTS %I.approval_requests (
            approval_request_id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL REFERENCES %I.orders(order_id),
            total_amount NUMERIC(10,2),
            status TEXT NOT NULL DEFAULT 'pending',
            approver TEXT,
            reason TEXT,
            requested_at TIMESTAMPTZ DEFAULT NOW(),
            decided_at TIMESTAMPTZ
        )$ddl$, ns || '_dag4', ns || '_dag4');
    EXECUTE format($ddl$
        INSERT INTO %I.customers (customer_id, customer_name, email, status) VALUES
            ('CUST-42', 'Jane Doe', 'jane@example.com', 'active'),
            ('CUST-43', 'John Smith', 'john@example.com', 'active'),
            ('CUST-99', 'Inactive User', 'inactive@example.com', 'inactive')
        ON CONFLICT (customer_id) DO NOTHING$ddl$, ns || '_dag4');
    EXECUTE format($ddl$
        INSERT INTO %I.inventory (sku, product_name, available_quantity, reserved_quantity, unit_price) VALUES
            ('WIDGET-A', 'Standard Widget', 100, 0, 29.99),
            ('GADGET-B', 'Premium Gadget', 50, 0, 499.99),
            ('THING-C', 'Basic Thing', 200, 0, 9.99),
            ('RARE-D', 'Rare Item', 2, 0, 1500.00)
        ON CONFLICT (sku) DO NOTHING$ddl$, ns || '_dag4');
END;
$fn$ LANGUAGE plpgsql;

-- NOTHING IS BOOTSTRAPPED HERE, DELIBERATELY. This file defines the function and
-- stops.
--
-- It used to end with `SELECT bootstrap_bakeoff('temporal'); ... ('prefect');`,
-- which was wrong twice over:
--
--   1. The same file is mounted into BOTH databases -- the local compose pod and
--      the in-cluster bakeoff-postgres (as a ConfigMap, see
--      deploy/templates/postgres.yaml). Two host-run runners were therefore
--      onboarded onto the cluster database, where neither will ever run.
--   2. Worse, and the reason the strays kept coming back: `bakeoff-db.sh seed`
--      re-runs this whole file to refresh the function before bootstrapping the
--      runner you asked for. So `just seed flyte` -- targeting the cluster --
--      re-executed both lines there every single time. Deleting the schemas did
--      not help; the next seed re-created them. (Measured 2026-08-12: one
--      `seed flyte` recreated temporal_dag{1,3,4} and prefect_dag{1,3,4}.)
--
-- Which runners to onboard is a property of the DATABASE, not of this file, so
-- it now lives in `init-runners.sh` driven by $BAKEOFF_RUNNERS -- set per
-- instance by docker-compose.yml (the 8 host-run tools) and by the chart's
-- postgres.runners value (argo, flyte). Keeping this file side-effect-free is
-- what makes it safe for `seed` to reload.
--
-- To onboard a runner by hand on an existing volume: `just seed <runner>`,
-- which routes to the right database. Or SELECT bootstrap_bakeoff('<ns>');

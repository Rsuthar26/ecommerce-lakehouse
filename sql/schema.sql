-- ============================================================
-- schema.sql — Staff DE Journey: Source 01 — RDS PostgreSQL
--
-- Run this once after terraform apply creates the RDS instance.
-- Command: psql $CONNECTION_STRING -f sql/schema.sql
--
-- Design decisions worth understanding:
--
-- 1. JSONB for addresses + metadata
--    Postgres stores and indexes JSONB natively.
--    Shipping addresses change format over time — JSONB absorbs that
--    without schema migrations. CDC captures the whole JSONB blob.
--
-- 2. Products NOT stored here
--    Full product catalog = MongoDB (Source 03).
--    Postgres holds product_sku as a foreign reference only.
--    Avoids duplicating nested attribute data across two systems.
--
-- 3. updated_at trigger on every table
--    Every row records when it last changed.
--    This is the fallback ingestion pattern if CDC is ever down
--    (poll WHERE updated_at > last_run — Source 01 batch path).
--
-- 4. UUIDs vs serial IDs
--    Using BIGSERIAL (auto-increment integer) not UUID.
--    Reason: simpler joins, smaller index size, easier to reason about
--    in a learning context. Production debate: UUID avoids ID collision
--    in distributed inserts but costs more storage + slower joins.
-- ============================================================

-- Ensure we're in the right database
\connect ecommerce

-- ============================================================
-- EXTENSIONS
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "pg_trgm";   -- trigram index for text search on names/email
CREATE EXTENSION IF NOT EXISTS "btree_gin"; -- GIN index support for JSONB queries

-- ============================================================
-- FUNCTION: auto-update updated_at on any row change
-- Applied to every table via trigger below.
-- ============================================================

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- TABLE: customers
--
-- Core entity. Everything references customer_id.
-- ~5K new customers/day in our generator.
-- CDC captures every INSERT (new signup) + UPDATE (address change,
-- deactivation, etc.)
-- ============================================================

CREATE TABLE IF NOT EXISTS customers (
    customer_id     BIGSERIAL PRIMARY KEY,
    email           VARCHAR(255) NOT NULL UNIQUE,
    first_name      VARCHAR(100) NOT NULL,
    last_name       VARCHAR(100) NOT NULL,
    phone           VARCHAR(20),
    
    -- JSONB address: {"street": "...", "city": "...", "postcode": "...", "country": "GB"}
    -- Stored as JSONB so address format changes don't require schema migration
    default_address JSONB,
    
    -- Segmentation fields — used in Silver layer transformations
    customer_tier   VARCHAR(20) DEFAULT 'standard' CHECK (customer_tier IN ('standard', 'silver', 'gold', 'vip')),
    marketing_opt_in BOOLEAN DEFAULT TRUE,
    
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    updated_at      TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE INDEX idx_customers_email    ON customers (email);
CREATE INDEX idx_customers_tier     ON customers (customer_tier);
CREATE INDEX idx_customers_created  ON customers (created_at DESC);

CREATE TRIGGER trg_customers_updated_at
    BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================
-- TABLE: orders
--
-- One row per order. Links customer to their purchase.
-- ~2K new orders/day in our generator.
-- Status flow: pending → confirmed → processing → shipped → delivered
--              pending → cancelled (any point before shipped)
-- CDC captures status transitions — this is the most-updated table.
-- ============================================================

CREATE TABLE IF NOT EXISTS orders (
    order_id        BIGSERIAL PRIMARY KEY,
    customer_id     BIGINT NOT NULL REFERENCES customers(customer_id),
    
    -- Status is the most-changed column — drives downstream pipelines
    order_status    VARCHAR(30) NOT NULL DEFAULT 'pending'
                    CHECK (order_status IN ('pending', 'confirmed', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded')),
    
    -- Monetary fields in pence/cents (integer) — never store money as FLOAT
    -- Reason: float arithmetic is imprecise. 0.1 + 0.2 ≠ 0.3 in binary.
    -- 4999 = £49.99. Divide by 100 in analytics layer.
    subtotal_pence  BIGINT NOT NULL DEFAULT 0,
    discount_pence  BIGINT NOT NULL DEFAULT 0,
    shipping_pence  BIGINT NOT NULL DEFAULT 0,
    tax_pence       BIGINT NOT NULL DEFAULT 0,
    total_pence     BIGINT NOT NULL DEFAULT 0,
    currency        CHAR(3) NOT NULL DEFAULT 'GBP',
    
    -- Shipping destination (may differ from customer's default address)
    shipping_address JSONB,
    
    -- Promo tracking
    promo_code      VARCHAR(50),
    
    -- Channel: web, mobile, api, marketplace
    order_channel   VARCHAR(20) DEFAULT 'web',
    
    -- Timestamps
    placed_at       TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    confirmed_at    TIMESTAMPTZ,
    shipped_at      TIMESTAMPTZ,
    delivered_at    TIMESTAMPTZ,
    cancelled_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    updated_at      TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE INDEX idx_orders_customer_id  ON orders (customer_id);
CREATE INDEX idx_orders_status       ON orders (order_status);
CREATE INDEX idx_orders_placed_at    ON orders (placed_at DESC);
CREATE INDEX idx_orders_updated_at   ON orders (updated_at DESC); -- For batch CDC fallback

CREATE TRIGGER trg_orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================
-- TABLE: order_items
--
-- Each row is one product line within an order.
-- An order always has ≥1 order_item.
-- product_sku links to MongoDB product catalog (Source 03).
-- ~5K new rows/day (average 2.5 items per order × 2K orders).
-- ============================================================

CREATE TABLE IF NOT EXISTS order_items (
    item_id         BIGSERIAL PRIMARY KEY,
    order_id        BIGINT NOT NULL REFERENCES orders(order_id),
    
    -- Links to MongoDB catalog — not a foreign key (cross-system)
    product_sku     VARCHAR(50) NOT NULL,
    product_name    VARCHAR(255) NOT NULL, -- Denormalised snapshot at time of order
    
    quantity        INT NOT NULL DEFAULT 1 CHECK (quantity > 0),
    
    -- Prices in pence — same reason as orders table
    unit_price_pence    BIGINT NOT NULL,
    discount_pence      BIGINT NOT NULL DEFAULT 0,
    total_price_pence   BIGINT NOT NULL, -- (unit_price - discount) * quantity
    
    -- Return tracking (updated when refund processed)
    return_quantity     INT DEFAULT 0,
    return_reason       TEXT,
    
    created_at      TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    updated_at      TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE INDEX idx_order_items_order_id     ON order_items (order_id);
CREATE INDEX idx_order_items_product_sku  ON order_items (product_sku);

CREATE TRIGGER trg_order_items_updated_at
    BEFORE UPDATE ON order_items
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================
-- TABLE: payments
--
-- One row per payment attempt. An order can have multiple
-- (failed attempt then success, or partial refunds).
-- stripe_payment_intent_id links to Stripe API (Source 06).
-- ~2.5K new rows/day + updates for status changes.
-- ============================================================

CREATE TABLE IF NOT EXISTS payments (
    payment_id          BIGSERIAL PRIMARY KEY,
    order_id            BIGINT NOT NULL REFERENCES orders(order_id),
    
    -- Cross-system reference to Stripe (Source 06)
    stripe_payment_intent_id  VARCHAR(100),
    stripe_charge_id          VARCHAR(100),
    
    amount_pence        BIGINT NOT NULL,
    currency            CHAR(3) NOT NULL DEFAULT 'GBP',
    
    payment_status      VARCHAR(30) NOT NULL DEFAULT 'pending'
                        CHECK (payment_status IN ('pending', 'processing', 'succeeded', 'failed', 'refunded', 'partially_refunded', 'disputed')),
    
    payment_method      VARCHAR(30) -- 'card', 'paypal', 'klarna', 'bank_transfer'
                        CHECK (payment_method IN ('card', 'paypal', 'klarna', 'bank_transfer', 'gift_card')),
    
    -- Card details (last4 only — never store full card numbers)
    card_last4          CHAR(4),
    card_brand          VARCHAR(20), -- 'visa', 'mastercard', 'amex'
    
    -- Failure info
    failure_code        VARCHAR(50),
    failure_message     TEXT,
    
    -- Refund tracking
    refunded_amount_pence   BIGINT DEFAULT 0,
    refund_reason           TEXT,
    
    processed_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    updated_at          TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE INDEX idx_payments_order_id         ON payments (order_id);
CREATE INDEX idx_payments_stripe_intent    ON payments (stripe_payment_intent_id);
CREATE INDEX idx_payments_status           ON payments (payment_status);

CREATE TRIGGER trg_payments_updated_at
    BEFORE UPDATE ON payments
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================
-- TABLE: inventory
--
-- Stock levels per product per warehouse.
-- product_sku links to MongoDB catalog (Source 03).
-- warehouse_id links to IoT sensors (Source 15) — same physical location.
-- ~500 updates/day as stock moves.
-- High-frequency update target — good CDC test case.
-- ============================================================

CREATE TABLE IF NOT EXISTS inventory (
    inventory_id        BIGSERIAL PRIMARY KEY,
    product_sku         VARCHAR(50) NOT NULL,
    warehouse_id        VARCHAR(20) NOT NULL, -- 'WH-LONDON-01', 'WH-MANC-01' etc.
    
    quantity_available  INT NOT NULL DEFAULT 0 CHECK (quantity_available >= 0),
    quantity_reserved   INT NOT NULL DEFAULT 0 CHECK (quantity_reserved >= 0),
    quantity_on_order   INT NOT NULL DEFAULT 0 CHECK (quantity_on_order >= 0),
    
    -- Reorder logic (used in Gold layer analytics)
    reorder_threshold   INT NOT NULL DEFAULT 10,
    reorder_quantity    INT NOT NULL DEFAULT 100,
    
    -- Cost at time of last stock receipt (pence)
    unit_cost_pence     BIGINT,
    
    last_counted_at     TIMESTAMPTZ, -- Physical stocktake timestamp
    updated_at          TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

-- Composite unique — one row per product per warehouse
CREATE UNIQUE INDEX idx_inventory_sku_warehouse ON inventory (product_sku, warehouse_id);
CREATE INDEX idx_inventory_low_stock ON inventory (product_sku) WHERE quantity_available < reorder_threshold;

CREATE TRIGGER trg_inventory_updated_at
    BEFORE UPDATE ON inventory
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================
-- REPLICATION USER FOR DEBEZIUM (Source 02)
--
-- Debezium needs its own Postgres user with REPLICATION privilege.
-- Do NOT use the master RDS user for CDC — it's a security anti-pattern.
-- This user has read-only access to data + replication slot permission.
--
-- NOTE: On RDS, use rds_replication role instead of REPLICATION attribute
-- because RDS doesn't grant superuser. This is an RDS-specific detail
-- that catches everyone the first time.
-- ============================================================

-- Create the user (password set via \set or env variable — never hardcoded)
CREATE USER debezium_user WITH PASSWORD 'REPLACE_WITH_STRONG_PASSWORD';

-- Grant RDS replication role (RDS-specific — not standard Postgres)
GRANT rds_replication TO debezium_user;

-- Grant read access to the tables Debezium will monitor
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;

-- Allow Debezium to see future tables too
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO debezium_user;

-- ============================================================
-- VERIFY SETUP
-- Run these manually after schema creation to confirm CDC readiness
-- ============================================================

-- Should return: wal_level | logical
-- SELECT name, setting FROM pg_settings WHERE name = 'wal_level';

-- Should return: max_replication_slots | 5
-- SELECT name, setting FROM pg_settings WHERE name = 'max_replication_slots';

-- Should return your 5 tables
-- SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS customers (
    customer_id BIGSERIAL PRIMARY KEY, email VARCHAR(255) UNIQUE NOT NULL,
    first_name VARCHAR(100), last_name VARCHAR(100), phone VARCHAR(30),
    country CHAR(2) DEFAULT 'GB', city VARCHAR(100), postcode VARCHAR(20),
    marketing_opt_in BOOLEAN DEFAULT FALSE,
    account_status VARCHAR(20) NOT NULL DEFAULT 'active' CHECK (account_status IN ('active','suspended','closed')),
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL, updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id BIGSERIAL PRIMARY KEY, customer_id BIGINT NOT NULL REFERENCES customers(customer_id),
    order_status VARCHAR(30) NOT NULL DEFAULT 'pending' CHECK (order_status IN ('pending','confirmed','processing','shipped','delivered','cancelled','refunded','failed')),
    channel VARCHAR(20) DEFAULT 'web', total_amount_pence BIGINT NOT NULL,
    discount_amount_pence BIGINT DEFAULT 0, shipping_amount_pence BIGINT DEFAULT 0,
    currency CHAR(3) NOT NULL DEFAULT 'GBP', shipping_address TEXT,
    shipping_city VARCHAR(100), shipping_postcode VARCHAR(20), shipping_country CHAR(2) DEFAULT 'GB',
    shopify_order_id VARCHAR(100), notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL, updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE TABLE IF NOT EXISTS order_items (
    item_id BIGSERIAL PRIMARY KEY, order_id BIGINT NOT NULL REFERENCES orders(order_id),
    product_sku VARCHAR(50) NOT NULL, product_name VARCHAR(255),
    quantity INT NOT NULL CHECK (quantity > 0), unit_price_pence BIGINT NOT NULL,
    discount_pence BIGINT DEFAULT 0, created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id BIGSERIAL PRIMARY KEY, order_id BIGINT NOT NULL REFERENCES orders(order_id),
    stripe_payment_intent_id VARCHAR(100), amount_pence BIGINT NOT NULL,
    currency CHAR(3) NOT NULL DEFAULT 'GBP',
    payment_status VARCHAR(30) NOT NULL DEFAULT 'pending' CHECK (payment_status IN ('pending','processing','succeeded','failed','refunded','partially_refunded','disputed')),
    payment_method VARCHAR(30) CHECK (payment_method IN ('card','paypal','klarna','bank_transfer','gift_card')),
    card_last4 CHAR(4), card_brand VARCHAR(20), failure_code VARCHAR(50),
    refunded_amount_pence BIGINT DEFAULT 0, processed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL, updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE TABLE IF NOT EXISTS inventory (
    inventory_id BIGSERIAL PRIMARY KEY, product_sku VARCHAR(50) NOT NULL,
    warehouse_id VARCHAR(20) NOT NULL, quantity_available INT NOT NULL DEFAULT 0 CHECK (quantity_available >= 0),
    quantity_reserved INT NOT NULL DEFAULT 0, quantity_on_order INT NOT NULL DEFAULT 0,
    reorder_threshold INT NOT NULL DEFAULT 10, reorder_quantity INT NOT NULL DEFAULT 100,
    unit_cost_pence BIGINT, last_counted_at TIMESTAMPTZ, updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_inventory_sku_warehouse ON inventory (product_sku, warehouse_id);

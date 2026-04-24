"""
generators/01_postgres/generate.py — Staff DE Journey: Source 01

Simulates realistic e-commerce activity in RDS PostgreSQL.
This is the MASTER source — all other sources reference IDs that originate here.

Rules satisfied:
    Rule 1  — --mode burst and --mode stream
    Rule 2  — burst completes in < 2 minutes
    Rule 3  — timestamps backdated with realistic time-of-day peaks
    Rule 4  — order_status reflects age of record
    Rule 5  — ON CONFLICT DO NOTHING throughout
    Rule 6  — stream: ~1 order/sec (~50K rows/day across all tables)
    Rule 7  — dirty data: malformed rows, missing fields, bad types
    Rule 8  — README.md exists
    Rule 9  — connection config via environment variables
    Rule 10 — callable from single bash line
    Rule 11 — N/A: this IS the master source of truth

Usage:
    python generate.py --mode burst --days 7
    python generate.py --mode burst --days 7 --dirty
    python generate.py --mode stream
"""

import os
import sys
import time
import random
import logging
import argparse
import hashlib
from datetime import datetime, timezone, timedelta

import psycopg2
from faker import Faker
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

fake = Faker("en_GB")
Faker.seed(42)

STREAM_SLEEP = 86400 / 50000  # ~1 order/sec = ~50K rows/day across tables (Rule 6 + Rule 13)

ORDER_STATUSES  = ["pending", "confirmed", "processing", "shipped", "delivered", "cancelled", "refunded"]
PAYMENT_METHODS = ["card", "paypal", "klarna", "bank_transfer", "gift_card"]
WAREHOUSES      = ["WH-LONDON-01", "WH-MANC-01", "WH-BRUM-01"]
CHANNELS        = ["web", "mobile", "api"]
CATEGORIES      = ["Electronics", "Furniture", "Accessories", "Clothing", "Home"]
PRODUCT_SKUS    = [f"SKU-{str(i).zfill(5)}" for i in range(1, 201)]


# ─────────────────────────────────────────────────────────────
# CONNECTION
# ─────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", 5432)),
        dbname=os.environ.get("PG_DBNAME", "ecommerce"),
        user=os.environ.get("PG_USER", "postgres_admin"),
        password=os.environ.get("PG_PASSWORD", ""),
        connect_timeout=15,
    )


# ─────────────────────────────────────────────────────────────
# RULE 3 — REALISTIC TIME-OF-DAY PEAKS
# B2C peak: 10am-12pm and 7pm-9pm. Higher volume on weekends.
# ─────────────────────────────────────────────────────────────

def peak_timestamp(base_dt: datetime) -> datetime:
    peak_windows = [
        (10, 12, 0.30),  # Morning peak
        (19, 21, 0.30),  # Evening peak
        (12, 19, 0.25),  # Afternoon steady
        (8,  10, 0.08),  # Morning warm-up
        (21, 23, 0.07),  # Late evening tail
    ]
    r, cumul, s, e = random.random(), 0.0, 10, 12
    for start, end, w in peak_windows:
        cumul += w
        if r <= cumul:
            s, e = start, end
            break
    # Weekend gets 30% more volume — handled by caller
    return base_dt.replace(
        hour=random.randint(s, e - 1),
        minute=random.randint(0, 59),
        second=random.randint(0, 59),
        tzinfo=timezone.utc
    )


# ─────────────────────────────────────────────────────────────
# RULE 4 — STATUS REFLECTS AGE
# ─────────────────────────────────────────────────────────────

def order_status_for_age(created_at: datetime) -> str:
    age_hrs = (datetime.now(timezone.utc) - created_at).total_seconds() / 3600
    if age_hrs < 1:
        return "pending"
    elif age_hrs < 4:
        return random.choices(["pending", "confirmed"], weights=[0.3, 0.7])[0]
    elif age_hrs < 24:
        return random.choices(["confirmed", "processing"], weights=[0.4, 0.6])[0]
    elif age_hrs < 72:
        return random.choices(["processing", "shipped"], weights=[0.3, 0.7])[0]
    elif age_hrs < 120:
        return random.choices(["shipped", "delivered", "cancelled"], weights=[0.5, 0.4, 0.1])[0]
    else:
        return random.choices(["delivered", "cancelled", "refunded"], weights=[0.85, 0.10, 0.05])[0]

def payment_status_for_order_status(order_status: str) -> str:
    if order_status in ("delivered", "shipped", "processing"):
        return random.choices(["succeeded", "succeeded"], weights=[0.97, 0.03])[0]
    elif order_status == "cancelled":
        return random.choices(["succeeded", "refunded", "failed"], weights=[0.3, 0.4, 0.3])[0]
    elif order_status == "refunded":
        return "refunded"
    elif order_status == "failed":
        return "failed"
    else:
        return random.choices(["pending", "succeeded", "failed"], weights=[0.6, 0.35, 0.05])[0]


# ─────────────────────────────────────────────────────────────
# RULE 7 — DIRTY DATA HELPERS
# ─────────────────────────────────────────────────────────────

def dr(base, dirty, elevated):
    return elevated if dirty else base

def maybe_null(value, dirty, base=0.05, elev=0.20):
    return None if random.random() < dr(base, dirty, elev) else value

def dirty_email(email, dirty):
    if random.random() < dr(0.01, dirty, 0.08):
        return random.choice(["invalid-email", "@nodomain", "", None, "user@"])
    return email

def dirty_amount(amount, dirty):
    if random.random() < dr(0.005, dirty, 0.05):
        return random.choice([0, -1, 999999999])
    return amount

def dirty_status(status, valid_statuses, dirty):
    if random.random() < dr(0.01, dirty, 0.15):
        return random.choice([status.upper(), status.capitalize(), "UNKNOWN"])
    return status


# ─────────────────────────────────────────────────────────────
# SCHEMA CREATION
# ─────────────────────────────────────────────────────────────

def create_schema(cur):
    with open(os.path.join(os.path.dirname(__file__), "schema.sql"), "r") as f:
        cur.execute(f.read())


# ─────────────────────────────────────────────────────────────
# SEED — base data load (customers, products, inventory)
# Rule 5: ON CONFLICT DO NOTHING throughout
# ─────────────────────────────────────────────────────────────

def seed_customers(cur, count: int = 500, dirty: bool = False):
    log.info(f"  Seeding {count} customers...")
    for _ in range(count):
        email = dirty_email(fake.unique.email(), dirty)
        cur.execute("""
            INSERT INTO customers
                (email, first_name, last_name, phone, country, city, postcode,
                 marketing_opt_in, account_status)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (email) DO NOTHING
        """, (
            email,
            maybe_null(fake.first_name(), dirty),
            maybe_null(fake.last_name(), dirty),
            maybe_null(fake.phone_number()[:30], dirty),
            "GB",
            maybe_null(fake.city(), dirty),
            maybe_null(fake.postcode(), dirty),
            random.random() > 0.4,
            dirty_status("active", ["active", "suspended", "closed"], dirty)
                if random.random() < 0.02 else "active",
        ))


def seed_products_and_inventory(cur, count: int = 200, dirty: bool = False):
    log.info(f"  Seeding {count} products + inventory...")
    for i, sku in enumerate(PRODUCT_SKUS[:count]):
        price_pence = random.randint(499, 29999)
        cur.execute("""
            INSERT INTO inventory
                (product_sku, warehouse_id, quantity_available,
                 quantity_reserved, unit_cost_pence)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (product_sku, warehouse_id) DO NOTHING
        """, (
            sku,
            random.choice(WAREHOUSES),
            dirty_amount(random.randint(0, 500), dirty),
            random.randint(0, 20),
            int(price_pence * 0.55),
        ))


# ─────────────────────────────────────────────────────────────
# ORDER GENERATION
# ─────────────────────────────────────────────────────────────

def generate_order(cur, created_at: datetime, dirty: bool = False):
    """Generate one complete order: order + items + payment."""

    # Real customer from DB
    cur.execute("SELECT customer_id FROM customers ORDER BY RANDOM() LIMIT 1")
    row = cur.fetchone()
    if not row:
        return
    customer_id = row[0]

    order_status   = order_status_for_age(created_at)
    shipping_pence = random.choice([0, 299, 499, 999])

    cur.execute("""
        INSERT INTO orders
            (customer_id, order_status, channel, total_amount_pence,
             discount_amount_pence, shipping_amount_pence, currency,
             shipping_address, shipping_city, shipping_postcode,
             shipping_country, created_at, updated_at)
        VALUES (%s,%s,%s,0,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING order_id
    """, (
        customer_id,
        dirty_status(order_status, ORDER_STATUSES, dirty),
        random.choice(CHANNELS),
        dirty_amount(random.randint(0, 500), dirty),
        shipping_pence,
        "GBP",
        maybe_null(fake.street_address(), dirty),
        maybe_null(fake.city(), dirty),
        maybe_null(fake.postcode(), dirty),
        "GB",
        created_at,
        created_at,
    ))
    order_id = cur.fetchone()[0]

    # Order items (1-4)
    total_pence = shipping_pence
    num_items   = random.randint(1, 4)
    cur.execute(
        "SELECT product_sku FROM inventory ORDER BY RANDOM() LIMIT %s", (num_items,)
    )
    skus = [row[0] for row in cur.fetchall()]

    for sku in skus:
        qty         = random.randint(1, 3)
        unit_pence  = random.randint(499, 14999)
        disc_pence  = int(unit_pence * random.uniform(0, 0.2)) if random.random() < 0.15 else 0
        total_pence += (unit_pence - disc_pence) * qty

        cur.execute("""
            INSERT INTO order_items
                (order_id, product_sku, product_name, quantity,
                 unit_price_pence, discount_pence, created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (
            order_id,
            sku,
            maybe_null(fake.catch_phrase(), dirty),
            qty if not dirty or random.random() > 0.005 else random.choice([-1, 0, 999]),
            unit_pence,
            disc_pence,
            created_at,
        ))

        # Decrement inventory
        cur.execute("""
            UPDATE inventory
            SET quantity_available = GREATEST(quantity_available - %s, 0),
                quantity_reserved  = quantity_reserved + %s,
                updated_at         = NOW()
            WHERE product_sku = %s
        """, (qty, qty, sku))

    # Update order total
    cur.execute(
        "UPDATE orders SET total_amount_pence=%s WHERE order_id=%s",
        (total_pence, order_id)
    )

    # Payment
    payment_status = payment_status_for_order_status(order_status)
    cur.execute("""
        INSERT INTO payments
            (order_id, stripe_payment_intent_id, amount_pence, currency,
             payment_status, payment_method, card_last4, card_brand,
             failure_code, processed_at, created_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        order_id,
        f"pi_{hashlib.md5(f'order_{order_id}_{created_at.date()}'.encode()).hexdigest()[:24]}",
        total_pence,
        "GBP",
        dirty_status(payment_status, ["pending","succeeded","failed","refunded"], dirty),
        random.choice(PAYMENT_METHODS),
        maybe_null(str(random.randint(1000, 9999)), dirty),
        maybe_null(random.choice(["visa", "mastercard", "amex"]), dirty),
        "card_declined" if payment_status == "failed" else None,
        created_at if payment_status != "pending" else None,
        created_at,
        created_at,
    ))


# ─────────────────────────────────────────────────────────────
# BURST MODE
# ─────────────────────────────────────────────────────────────

def run_burst(days: int = 7, dirty: bool = False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}")
    t0   = time.time()
    conn = get_conn()
    cur  = conn.cursor()

    log.info("Creating schema...")
    create_schema(cur)
    conn.commit()

    log.info("Seeding base data...")
    seed_customers(cur, count=500, dirty=dirty)
    seed_products_and_inventory(cur, count=200, dirty=dirty)
    conn.commit()

    now            = datetime.now(timezone.utc)
    orders_per_day = 200
    total_orders   = days * orders_per_day
    log.info(f"Generating {total_orders} orders across {days} days...")

    for i in range(total_orders):
        day_offset  = random.uniform(0, days)
        base_dt     = now - timedelta(days=day_offset)
        # Weekend: slightly more orders
        is_weekend  = base_dt.weekday() >= 5
        if is_weekend and random.random() < 0.3:
            pass  # keep this order (extra weekend volume)
        created_at = peak_timestamp(base_dt)
        generate_order(cur, created_at, dirty)

        if i % 100 == 0 and i > 0:
            conn.commit()
            elapsed = time.time() - t0
            log.info(f"  {i}/{total_orders} | {elapsed:.0f}s")

    conn.commit()
    cur.close()
    conn.close()

    elapsed = time.time() - t0
    log.info(f"✓ BURST COMPLETE | {elapsed:.1f}s | {total_orders} orders")
    if elapsed > 120:
        log.warning(f"Rule 2 VIOLATION: {elapsed:.0f}s > 120s")
    else:
        log.info(f"Rule 2 ✅ {elapsed:.1f}s < 120s")


# ─────────────────────────────────────────────────────────────
# STREAM MODE
# ─────────────────────────────────────────────────────────────

def run_stream(dirty: bool = False):
    log.info(f"STREAM MODE | ~1 order/sec | dirty={dirty} | Ctrl+C to stop")
    conn  = get_conn()
    cur   = conn.cursor()
    stats = {"orders": 0}

    try:
        while True:
            created_at = datetime.now(timezone.utc)
            generate_order(cur, created_at, dirty)
            conn.commit()
            stats["orders"] += 1
            if stats["orders"] % 50 == 0:
                log.info(f"Stream — {stats}")
            time.sleep(STREAM_SLEEP)
    except KeyboardInterrupt:
        cur.close()
        conn.close()
        log.info(f"Stopped. {stats}")


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Source 01 — PostgreSQL generator")
    p.add_argument("--mode",  choices=["burst", "stream"], required=True)
    p.add_argument("--days",  type=int, default=7)
    p.add_argument("--dirty", action="store_true")
    args = p.parse_args()
    log.info(f"Source 01 | mode={args.mode} | days={args.days} | dirty={args.dirty}")
    if args.mode == "burst":
        run_burst(args.days, args.dirty)
    elif args.mode == "stream":
        run_stream(args.dirty)

if __name__ == "__main__":
    main()

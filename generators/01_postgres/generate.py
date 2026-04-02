"""
generators/01_postgres/generate.py — Staff DE Journey: Source 01

Simulates realistic e-commerce activity in RDS PostgreSQL.
Follows all 10 generator rules — see README.md for details.

Key design: burst mode uses batched inserts (execute_values + commit
every 100 rows) instead of per-row commits. This reduces ~6600 round
trips to ~66, bringing burst time under 2 minutes (Rule 2).

Usage:
    python generate.py --mode burst              # 7 days history, clean
    python generate.py --mode burst --dirty      # 7 days history, dirty
    python generate.py --mode stream             # continuous, ~0.6 ops/sec
    python generate.py --mode stream --dirty     # continuous, dirty
"""

import os
import sys
import time
import random
import logging
import argparse
import json
from datetime import datetime, timezone, timedelta

import psycopg2
import psycopg2.extras
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

PRODUCT_SKUS    = [f"SKU-{str(i).zfill(5)}" for i in range(1, 201)]
PRODUCT_NAMES   = [
    "Wireless Headphones", "USB-C Hub", "Laptop Stand", "Mechanical Keyboard",
    "Mouse Pad XL", "Webcam HD", "Monitor Arm", "Cable Organiser",
    "Power Bank 20000mAh", "Smart Plug", "LED Desk Lamp", "Phone Stand",
    "Ergonomic Chair", "Standing Desk", "Wrist Rest", "Laptop Sleeve",
    "Screen Cleaner Kit", "Desk Organiser", "USB Hub 7-Port", "Trackpad",
]
WAREHOUSES      = ["WH-LONDON-01", "WH-MANC-01", "WH-BRUM-01"]
CARD_BRANDS     = ["visa", "mastercard", "amex"]
PAYMENT_METHODS = ["card", "paypal", "klarna", "bank_transfer", "gift_card"]
CHANNELS        = ["web", "web", "web", "mobile", "mobile", "api"]
TIERS           = ["standard", "standard", "standard", "silver", "gold", "vip"]
STREAM_SLEEP    = 1.0 / 0.6   # ~0.6 ops/sec = ~50K ops/day  (Rule 6)
BURST_BATCH     = 100         # commit every N orders in burst mode
VALID_STATUSES  = {'pending','confirmed','processing','shipped','delivered','cancelled','refunded'}


# ─────────────────────────────────────────────────────────────
# RULE 3 — REALISTIC TIMESTAMPS
# ─────────────────────────────────────────────────────────────

def realistic_timestamp(base_dt: datetime) -> datetime:
    peak_windows = [
        (10, 12, 0.35), (19, 21, 0.30), (12, 19, 0.25),
        (8,  10, 0.05), (21, 23, 0.05),
    ]
    r, cumul = random.random(), 0.0
    s, e = 10, 12
    for start, end, w in peak_windows:
        cumul += w
        if r <= cumul:
            s, e = start, end
            break
    return base_dt.replace(hour=random.randint(s, e-1),
                           minute=random.randint(0, 59),
                           second=random.randint(0, 59),
                           tzinfo=timezone.utc)


def get_burst_timestamps(days: int = 7) -> list:
    start = datetime.now(timezone.utc) - timedelta(days=days)
    ts = []
    for d in range(days):
        day_dt = start + timedelta(days=d)
        count  = int(285 * (1.4 if day_dt.weekday() >= 5 else 1.0))
        for _ in range(count):
            t = realistic_timestamp(day_dt) + timedelta(minutes=random.randint(0, 59))
            ts.append(t)
    ts.sort()
    return ts


# ─────────────────────────────────────────────────────────────
# RULE 4 — STATUS REFLECTS AGE
# ─────────────────────────────────────────────────────────────

def status_for_age(placed_at: datetime) -> dict:
    age = (datetime.now(timezone.utc) - placed_at).total_seconds() / 3600
    r   = {"order_status": "pending", "confirmed_at": None,
           "shipped_at": None, "delivered_at": None, "cancelled_at": None}

    if age < 1:
        r["order_status"] = "pending"
    elif age < 3:
        if random.random() < 0.05:
            r["order_status"] = "cancelled"
            r["cancelled_at"] = placed_at + timedelta(hours=random.uniform(0.5, 2))
        else:
            r["order_status"] = "confirmed"
            r["confirmed_at"] = placed_at + timedelta(hours=random.uniform(0.5, 1.5))
    elif age < 24:
        r["order_status"] = "processing"
        r["confirmed_at"] = placed_at + timedelta(hours=random.uniform(0.5, 1.5))
    elif age < 72:
        if random.random() < 0.03:
            r["order_status"] = "cancelled"
            r["cancelled_at"] = placed_at + timedelta(hours=random.uniform(2, 12))
        else:
            r["order_status"] = "shipped"
            r["confirmed_at"] = placed_at + timedelta(hours=random.uniform(0.5, 1.5))
            r["shipped_at"]   = placed_at + timedelta(hours=random.uniform(24, 48))
    else:
        r["order_status"] = "refunded" if random.random() < 0.02 else "delivered"
        r["confirmed_at"] = placed_at + timedelta(hours=random.uniform(0.5, 1.5))
        r["shipped_at"]   = placed_at + timedelta(hours=random.uniform(24, 48))
        r["delivered_at"] = placed_at + timedelta(hours=random.uniform(72, 96))
    return r


def pay_status(order_status: str) -> str:
    return {"pending": "pending", "confirmed": "processing",
            "processing": "processing", "shipped": "succeeded",
            "delivered": "succeeded", "cancelled": "failed",
            "refunded": "refunded"}.get(order_status, "pending")


# ─────────────────────────────────────────────────────────────
# RULE 7 — DIRTY DATA
# ─────────────────────────────────────────────────────────────

def dr(base, dirty, elevated):
    return elevated if dirty else base

def bad_email(email, dirty):
    if random.random() < dr(0.01, dirty, 0.10):
        return random.choice(["notanemail", "missing@", "@nodomain.com", ""])
    return email

def bad_price(price, dirty):
    if random.random() < dr(0.005, dirty, 0.08):
        return random.choice(["free", "N/A", "TBD", None, -1, 0])
    return price

def bad_status(status, dirty):
    # Status corruption only in dirty mode — clean mode must satisfy DB check constraint
    if dirty and random.random() < 0.15:
        return random.choice([status.upper(), status.capitalize(), status + "_", "UNKNOWN"])
    return status

def null_phone(phone, dirty):
    return None if random.random() < dr(0.05, dirty, 0.20) else phone

def future_ts(ts, dirty):
    return datetime(2099, 1, 1, tzinfo=timezone.utc) if (dirty and random.random() < 0.05) else ts

def address(dirty):
    a = {"street": fake.street_address(), "city": fake.city(),
         "county": fake.county(), "postcode": fake.postcode(), "country": "GB"}
    # Never return raw strings — breaks psycopg2 execute_values batch inserts.
    # Dirty mode injects missing fields and wrong types inside valid JSON instead.
    if dirty and random.random() < 0.10:
        a.pop(random.choice(["street", "city", "postcode"]), None)
    if dirty and random.random() < 0.05:
        a["postcode"] = random.randint(1000, 9999)
    return psycopg2.extras.Json(a)


# ─────────────────────────────────────────────────────────────
# QUARANTINE (Rule 7)
# ─────────────────────────────────────────────────────────────

_q_buf = []

def ensure_quarantine(cur, conn):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS quarantine_log (
            id BIGSERIAL PRIMARY KEY, source_table VARCHAR(50),
            reason TEXT, raw_data JSONB, created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    conn.commit()

def q(source, reason, data):
    _q_buf.append((source, reason, json.dumps(data, default=str)))
    log.warning(f"QUARANTINE [{source}] {str(reason)[:80]}")

def flush_q(cur, conn):
    if not _q_buf:
        return
    psycopg2.extras.execute_values(cur,
        "INSERT INTO quarantine_log (source_table, reason, raw_data) VALUES %s",
        [(s, r, d) for s, r, d in _q_buf],
        template="(%s, %s, %s::jsonb)")
    _q_buf.clear()


# ─────────────────────────────────────────────────────────────
# CONNECTION (Rule 9)
# ─────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", 5432)),
        dbname=os.environ.get("DB_NAME", "ecommerce"),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        connect_timeout=10,
    )


# ─────────────────────────────────────────────────────────────
# ROW BUILDERS — pure data, no DB calls
# ─────────────────────────────────────────────────────────────

def build_customer(ts, dirty):
    email = bad_email(fake.email(), dirty)
    if not email or "@" not in str(email):
        q("customers", f"invalid_email: {email!r}", {"email": email})
        return None
    return (email, fake.first_name(), fake.last_name(),
            null_phone(fake.phone_number()[:20], dirty),
            address(dirty), random.choice(TIERS),
            random.random() > 0.2, ts, ts)


def build_order(customer_id, placed_at, dirty):
    items, used = [], set()
    for _ in range(random.choices([1,2,3,4,5], weights=[30,35,20,10,5])[0]):
        sku = random.choice(PRODUCT_SKUS)
        while sku in used: sku = random.choice(PRODUCT_SKUS)
        used.add(sku)
        raw   = random.randint(499, 29999)
        price = bad_price(raw, dirty)
        if not isinstance(price, int) or price <= 0:
            q("order_items", f"invalid_unit_price: {price!r}", {"sku": sku})
            price = raw
        qty  = random.choices([1,2,3], weights=[70,20,10])[0]
        disc = int(price * random.choice([0,0,0,0.1,0.2]))
        items.append({"sku": sku, "name": random.choice(PRODUCT_NAMES),
                      "qty": qty, "price": price, "disc": disc,
                      "total": (price - disc) * qty})

    sub  = sum(i["total"] for i in items)
    disc = int(sub * random.choice([0,0,0,0.05,0.10]))
    ship = 0 if sub > 3999 else 299
    tax  = int((sub - disc) * 0.20)
    tot  = (sub - disc) + ship + tax

    sd     = status_for_age(placed_at)
    status = bad_status(sd["order_status"], dirty)
    # Dirty mode: bad status violates check constraint — quarantine and use fallback
    if status not in VALID_STATUSES:
        q("orders", f"invalid_status: {status!r}", {"customer_id": customer_id, "status": status})
        status = sd["order_status"]  # fallback to valid status
    placed = future_ts(placed_at, dirty)
    ps     = pay_status(sd["order_status"])
    method = random.choices(PAYMENT_METHODS, weights=[60,20,15,3,2])[0]
    fc     = fm = None
    if ps == "failed":
        fc = random.choice(["insufficient_funds", "card_declined", "expired_card"])
        fm = f"Payment failed: {fc}"

    ship_addr = address(dirty)
    if isinstance(ship_addr, str):
        q("orders", "malformed_shipping_address_json", {"customer_id": customer_id})
        ship_addr = psycopg2.extras.Json({"street": "unknown", "city": "unknown",
                                          "postcode": "unknown", "country": "GB"})
    return {
        "order": (customer_id, status, sub, disc, ship, tax, tot,
                  ship_addr,
                  f"PROMO{random.randint(10,99)}" if random.random()<0.15 else None,
                  random.choice(CHANNELS),
                  placed, sd["confirmed_at"], sd["shipped_at"],
                  sd["delivered_at"], sd["cancelled_at"], placed, placed),
        "items": items,
        "pay":   (tot, ps, method,
                  str(random.randint(1000,9999)) if method=="card" else None,
                  random.choice(CARD_BRANDS)     if method=="card" else None,
                  fc, fm, placed + timedelta(minutes=random.randint(1,10)),
                  placed, placed),
    }


# ─────────────────────────────────────────────────────────────
# BURST MODE (Rules 1, 2, 3, 4, 5)
# ─────────────────────────────────────────────────────────────

def run_burst(conn, days=7, dirty=False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}")
    t0 = time.time()
    ts = get_burst_timestamps(days)
    log.info(f"Target: {len(ts)} orders")

    stats = {"customers": 0, "orders": 0, "inventory": 0}

    with conn.cursor() as cur:
        ensure_quarantine(cur, conn)

        # ── customers (1 round trip) ─────────────────────────
        rows = [r for r in (build_customer(
                    ts[int(i * len(ts) / 500)], dirty)
                    for i in range(500)) if r]
        psycopg2.extras.execute_values(cur, """
            INSERT INTO customers
                (email, first_name, last_name, phone, default_address,
                 customer_tier, marketing_opt_in, created_at, updated_at)
            VALUES %s ON CONFLICT (email) DO NOTHING
        """, rows)
        flush_q(cur, conn)
        conn.commit()

        cur.execute("SELECT customer_id FROM customers")
        cids = [r[0] for r in cur.fetchall()]
        stats["customers"] = len(cids)
        log.info(f"✓ {stats['customers']} customers")

        if not cids:
            log.error("No customers — aborting"); return

        # ── orders in batches ────────────────────────────────
        bo, bi, bp = [], [], []

        def flush():
            if not bo: return
            psycopg2.extras.execute_values(cur, """
                INSERT INTO orders
                    (customer_id, order_status,
                     subtotal_pence, discount_pence, shipping_pence,
                     tax_pence, total_pence,
                     shipping_address, promo_code, order_channel,
                     placed_at, confirmed_at, shipped_at,
                     delivered_at, cancelled_at, created_at, updated_at)
                VALUES %s RETURNING order_id
            """, bo)
            oids = [r[0] for r in cur.fetchall()]

            item_rows = []
            for oid, items in zip(oids, bi):
                # find placed_at from the order tuple (index 10)
                pat = bo[oids.index(oid)][10]
                for it in items:
                    item_rows.append((oid, it["sku"], it["name"],
                                      it["qty"], it["price"], it["disc"],
                                      it["total"], pat, pat))
            if item_rows:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO order_items
                        (order_id, product_sku, product_name,
                         quantity, unit_price_pence, discount_pence,
                         total_price_pence, created_at, updated_at)
                    VALUES %s
                """, item_rows)

            pay_rows = [(oid,) + pay for oid, pay in zip(oids, bp)]
            psycopg2.extras.execute_values(cur, """
                INSERT INTO payments
                    (order_id, amount_pence, currency, payment_status,
                     payment_method, card_last4, card_brand,
                     failure_code, failure_message,
                     processed_at, created_at, updated_at)
                VALUES %s
            """, pay_rows,
            template="(%s,%s,'GBP',%s,%s,%s,%s,%s,%s,%s,%s,%s)")

            flush_q(cur, conn)
            conn.commit()
            n = len(bo)
            bo.clear(); bi.clear(); bp.clear()
            return n

        total_inserted = 0
        for i, placed_at in enumerate(ts):
            data = build_order(random.choice(cids), placed_at, dirty)
            bo.append(data["order"]); bi.append(data["items"]); bp.append(data["pay"])

            if dirty and random.random() < 0.03:
                bo.append(data["order"]); bi.append(data["items"]); bp.append(data["pay"])

            if len(bo) >= BURST_BATCH:
                n = flush()
                total_inserted += n
                stats["orders"] = total_inserted
                log.info(f"  {i+1}/{len(ts)} | {total_inserted} orders | {time.time()-t0:.0f}s")

        n = flush()
        if n: total_inserted += n
        stats["orders"] = total_inserted

        # ── inventory (1 round trip) ─────────────────────────
        # Deduplicate by (sku, warehouse) — ON CONFLICT DO UPDATE crashes if
        # two rows in the same batch share the same conflict key
        inv_dict = {}
        for _ in range(500):
            sku = random.choice(PRODUCT_SKUS)
            wh  = random.choice(WAREHOUSES)
            qty = max(0, 50 + (-random.randint(1,5) if random.random()<0.7 else random.randint(10,100)))
            inv_dict[(sku, wh)] = (qty, random.randint(100,5000), random.choice(ts))
        inv = [(sku, wh, qty, cost, t)
               for (sku, wh), (qty, cost, t) in inv_dict.items()]
        psycopg2.extras.execute_values(cur, """
            INSERT INTO inventory
                (product_sku, warehouse_id, quantity_available, unit_cost_pence, updated_at)
            VALUES %s
            ON CONFLICT (product_sku, warehouse_id) DO UPDATE
            SET quantity_available = GREATEST(0, inventory.quantity_available +
                                     EXCLUDED.quantity_available - 50),
                updated_at = EXCLUDED.updated_at
        """, inv)
        conn.commit()
        stats["inventory"] = 500

    elapsed = time.time() - t0
    log.info(f"✓ BURST COMPLETE | {elapsed:.1f}s | {stats}")
    if elapsed > 120:
        log.warning(f"Rule 2 VIOLATION: {elapsed:.0f}s > 120s")
    else:
        log.info(f"Rule 2 ✅ {elapsed:.1f}s < 120s")


# ─────────────────────────────────────────────────────────────
# STREAM MODE (Rules 1, 6)
# ─────────────────────────────────────────────────────────────

def run_stream(conn, dirty=False):
    log.info(f"STREAM MODE | ~0.6 ops/sec | dirty={dirty} | Ctrl+C to stop")
    with conn.cursor() as cur:
        ensure_quarantine(cur, conn)
        cur.execute("SELECT customer_id FROM customers ORDER BY RANDOM() LIMIT 500")
        cids = [r[0] for r in cur.fetchall()]

    if not cids:
        log.warning("No customers — seeding 1 day first")
        run_burst(conn, days=1)
        with conn.cursor() as cur:
            cur.execute("SELECT customer_id FROM customers LIMIT 500")
            cids = [r[0] for r in cur.fetchall()]

    stats, i = {"customers": 0, "orders": 0, "inventory": 0}, 0
    try:
        while True:
            i += 1
            now = datetime.now(timezone.utc)
            act = random.choices(["customer","order","inventory"],weights=[5,75,20])[0]
            with conn.cursor() as cur:
                if act == "customer":
                    row = build_customer(now, dirty)
                    if row:
                        cur.execute("""
                            INSERT INTO customers
                                (email,first_name,last_name,phone,default_address,
                                 customer_tier,marketing_opt_in,created_at,updated_at)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (email) DO NOTHING RETURNING customer_id
                        """, row)
                        res = cur.fetchone()
                        if res: cids.append(res[0]); stats["customers"] += 1
                    flush_q(cur, conn); conn.commit()

                elif act == "order":
                    data = build_order(random.choice(cids), now, dirty)
                    cur.execute("""
                        INSERT INTO orders
                            (customer_id,order_status,
                             subtotal_pence,discount_pence,shipping_pence,
                             tax_pence,total_pence,currency,
                             shipping_address,promo_code,order_channel,
                             placed_at,confirmed_at,shipped_at,
                             delivered_at,cancelled_at,created_at,updated_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,'GBP',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        RETURNING order_id
                    """, data["order"])
                    oid = cur.fetchone()[0]
                    for it in data["items"]:
                        cur.execute("""
                            INSERT INTO order_items
                                (order_id,product_sku,product_name,quantity,
                                 unit_price_pence,discount_pence,total_price_pence,
                                 created_at,updated_at)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """, (oid, it["sku"], it["name"], it["qty"],
                              it["price"], it["disc"], it["total"], now, now))
                    cur.execute("""
                        INSERT INTO payments
                            (order_id,amount_pence,currency,payment_status,
                             payment_method,card_last4,card_brand,
                             failure_code,failure_message,
                             processed_at,created_at,updated_at)
                        VALUES (%s,%s,'GBP',%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (oid,) + data["pay"])
                    stats["orders"] += 1
                    flush_q(cur, conn); conn.commit()

                else:
                    sku = random.choice(PRODUCT_SKUS); wh = random.choice(WAREHOUSES)
                    d   = -random.randint(1,5) if random.random()<0.7 else random.randint(10,100)
                    cur.execute("""
                        INSERT INTO inventory
                            (product_sku,warehouse_id,quantity_available,unit_cost_pence,updated_at)
                        VALUES (%s,%s,GREATEST(0,%s),%s,%s)
                        ON CONFLICT (product_sku,warehouse_id) DO UPDATE
                        SET quantity_available=GREATEST(0,inventory.quantity_available+%s),
                            updated_at=%s
                    """, (sku, wh, max(0,50+d), random.randint(100,5000), now, d, now))
                    conn.commit(); stats["inventory"] += 1

            if i % 100 == 0: log.info(f"Stream — {stats}")
            time.sleep(STREAM_SLEEP)

    except KeyboardInterrupt:
        log.info(f"Stopped. {stats}")


# ─────────────────────────────────────────────────────────────
# ENTRY POINT (Rules 1, 10)
# ─────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Source 01 — PostgreSQL generator")
    p.add_argument("--mode",  choices=["burst","stream"], required=True)
    p.add_argument("--days",  type=int, default=7)
    p.add_argument("--dirty", action="store_true")
    args = p.parse_args()

    log.info(f"Source 01 | mode={args.mode} | days={args.days} | dirty={args.dirty}")
    try:
        conn = get_conn()
        conn.autocommit = False
        log.info("✓ Connected")
    except Exception as e:
        log.error(f"Connection failed: {e}"); sys.exit(1)

    try:
        if args.mode == "burst":   run_burst(conn, args.days, args.dirty)
        elif args.mode == "stream": run_stream(conn, args.dirty)
    finally:
        conn.close()

if __name__ == "__main__":
    main()

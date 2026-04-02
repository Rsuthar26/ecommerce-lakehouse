"""
generators/07_shipstation/generate.py — Staff DE Journey: Source 07

Simulates ShipStation API responses for shipments, tracking, carriers.
Reads shipped/delivered orders from Postgres to ensure joinability.
Outputs JSON files to local path (later: S3 Bronze landing zone).

Real ShipStation ingestion pattern:
  1. Airflow triggers hourly
  2. Job calls GET /shipments?modifyDateStart=last_run
  3. Paginate through results
  4. Write each page as JSON to S3: raw/shipstation/YYYY/MM/DD/HH/page_N.json
  5. Databricks Autoloader picks up → Bronze table

Rules satisfied:
    Rule 1  — --mode burst and --mode stream
    Rule 2  — burst < 2 minutes
    Rule 3  — timestamps backdated realistically
    Rule 4  — shipment status reflects order age
    Rule 5  — idempotent (shipment_id is stable per order)
    Rule 6  — stream ~0.058 ops/sec (~5K records/day)
    Rule 7  — dirty data + quarantine
    Rule 8  — README.md exists
    Rule 9  — connection via environment variables
    Rule 10 — callable from single bash line

Usage:
    python generate.py --mode burst --output-dir /tmp/shipstation_raw
    python generate.py --mode burst --dirty --output-dir /tmp/shipstation_raw
    python generate.py --mode stream --output-dir /tmp/shipstation_raw
"""

import os
import sys
import json
import time
import random
import logging
import argparse
import hashlib
from datetime import datetime, timezone, timedelta
from pathlib import Path

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

STREAM_SLEEP = 1.0 / (5000 / 86400)  # ~5K records/day

CARRIERS = [
    {"name": "Royal Mail",   "code": "royal_mail",   "service": "Tracked 48"},
    {"name": "DPD",          "code": "dpd",           "service": "Next Day"},
    {"name": "Hermes",       "code": "hermes",        "service": "Standard"},
    {"name": "DHL",          "code": "dhl_express",   "service": "Express"},
    {"name": "UPS",          "code": "ups",           "service": "Ground"},
    {"name": "FedEx",        "code": "fedex",         "service": "Economy"},
]

WAREHOUSES = ["WH-LONDON-01", "WH-MANC-01", "WH-BRUM-01"]


# ─────────────────────────────────────────────────────────────
# RULE 4 — STATUS REFLECTS AGE
# ─────────────────────────────────────────────────────────────

def shipment_status_for_age(placed_at: datetime, order_status: str) -> dict:
    """
    Shipment status must reflect how old the order is.
    Only shipped/delivered orders have shipments.
    """
    age_hrs = (datetime.now(timezone.utc) - placed_at).total_seconds() / 3600

    if order_status not in ("shipped", "delivered", "refunded"):
        return None  # No shipment for pending/processing/cancelled orders

    if order_status == "delivered" or age_hrs > 72:
        shipped_at   = placed_at + timedelta(hours=random.uniform(24, 48))
        delivered_at = placed_at + timedelta(hours=random.uniform(72, 120))
        return {
            "status":       "delivered",
            "shipped_at":   shipped_at,
            "delivered_at": delivered_at,
            "tracking_events": [
                {"ts": shipped_at,                              "description": "Shipment collected from warehouse"},
                {"ts": shipped_at + timedelta(hours=4),         "description": "In transit - sorting facility"},
                {"ts": shipped_at + timedelta(hours=16),        "description": "Out for delivery"},
                {"ts": delivered_at,                            "description": "Delivered"},
            ]
        }
    elif order_status == "shipped":
        shipped_at = placed_at + timedelta(hours=random.uniform(24, 48))
        return {
            "status":       "in_transit",
            "shipped_at":   shipped_at,
            "delivered_at": None,
            "tracking_events": [
                {"ts": shipped_at,                       "description": "Shipment collected from warehouse"},
                {"ts": shipped_at + timedelta(hours=4),  "description": "In transit - sorting facility"},
            ]
        }
    else:
        shipped_at = placed_at + timedelta(hours=random.uniform(24, 48))
        return {
            "status":       "returned",
            "shipped_at":   shipped_at,
            "delivered_at": shipped_at + timedelta(hours=random.uniform(72, 120)),
            "tracking_events": [
                {"ts": shipped_at, "description": "Shipment collected"},
                {"ts": shipped_at + timedelta(hours=48), "description": "Return initiated"},
            ]
        }


# ─────────────────────────────────────────────────────────────
# RULE 7 — DIRTY DATA
# ─────────────────────────────────────────────────────────────

def dr(base, dirty, elevated):
    return elevated if dirty else base

def missing(value, dirty, base=0.05, elev=0.20):
    return None if random.random() < dr(base, dirty, elev) else value

def bad_tracking(tracking, dirty):
    if random.random() < dr(0.01, dirty, 0.08):
        return random.choice(["", "INVALID", None, "0000000000"])
    return tracking


# ─────────────────────────────────────────────────────────────
# QUARANTINE
# ─────────────────────────────────────────────────────────────

def quarantine(output_dir: Path, reason: str, data: dict):
    q_dir = output_dir / "quarantine"
    q_dir.mkdir(parents=True, exist_ok=True)
    fname = q_dir / f"q_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')}.json"
    with open(fname, "w") as f:
        json.dump({"reason": reason, "data": data,
                   "ts": datetime.now(timezone.utc).isoformat()}, f, default=str)
    log.warning(f"QUARANTINE {reason[:80]}")


# ─────────────────────────────────────────────────────────────
# POSTGRES — fetch shipped/delivered orders
# ─────────────────────────────────────────────────────────────

def get_pg_connection():
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", 5432)),
        dbname=os.environ.get("DB_NAME", "ecommerce"),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        connect_timeout=10,
    )


def fetch_shipped_orders(days: int = 7) -> list[dict]:
    """
    Only orders that have been shipped or delivered need ShipStation records.
    Pending/processing/cancelled orders have no shipment.
    """
    conn = get_pg_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    o.order_id,
                    o.customer_id,
                    o.order_status,
                    o.placed_at,
                    o.shipped_at,
                    o.delivered_at,
                    o.shipping_address,
                    o.total_pence
                FROM orders o
                WHERE o.order_status IN ('shipped', 'delivered', 'refunded')
                  AND o.placed_at >= NOW() - INTERVAL '%s days'
                ORDER BY o.placed_at DESC
            """, (days,))
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────
# SHIPSTATION RECORD BUILDER
# ─────────────────────────────────────────────────────────────

def build_shipment(order: dict, dirty: bool = False) -> dict | None:
    """
    Build a realistic ShipStation shipment record.
    shipment_id is deterministic — same order always gets same shipment ID.
    """
    order_id   = order["order_id"]
    placed_at  = order["placed_at"]
    if placed_at.tzinfo is None:
        placed_at = placed_at.replace(tzinfo=timezone.utc)

    status_data = shipment_status_for_age(placed_at, order["order_status"])
    if not status_data:
        return None  # Order not yet shipped

    # Deterministic shipment ID — Rule 5
    shipment_id = int(hashlib.md5(f"shipment_{order_id}".encode()).hexdigest()[:8], 16)
    carrier     = random.choice(CARRIERS)

    # Tracking number — realistic formats per carrier
    tracking_formats = {
        "royal_mail":  lambda: f"JD{random.randint(100000000, 999999999)}GB",
        "dpd":         lambda: f"1{random.randint(10000000000000, 99999999999999)}",
        "hermes":      lambda: f"H{random.randint(10000000000000, 99999999999999)}",
        "dhl_express": lambda: f"1Z{random.randint(100000000, 999999999)}",
        "ups":         lambda: f"1Z{fake.lexify('??????')}{random.randint(10000000, 99999999)}",
        "fedex":       lambda: str(random.randint(100000000000, 999999999999)),
    }
    tracking_fn  = tracking_formats.get(carrier["code"], lambda: fake.bothify("??##########"))
    tracking_num = bad_tracking(tracking_fn(), dirty)

    # Parse shipping address
    addr = order.get("shipping_address") or {}
    if isinstance(addr, str):
        try:
            import json as _json
            addr = _json.loads(addr)
        except Exception:
            addr = {}

    shipped_at   = status_data["shipped_at"]
    delivered_at = status_data["delivered_at"]

    record = {
        "shipmentId":       shipment_id,
        "orderId":          order_id,                           # Join key to Postgres
        "orderKey":         f"ORD-{order_id:06d}",
        "shipDate":         shipped_at.isoformat() if shipped_at else None,
        "createDate":       placed_at.isoformat(),
        "modifyDate":       (delivered_at or shipped_at).isoformat(),

        "trackingNumber":   missing(tracking_num, dirty, base=0.01, elev=0.05),
        "carrierCode":      carrier["code"],
        "carrierName":      carrier["name"],
        "serviceCode":      carrier["service"],
        "serviceId":        random.randint(1000, 9999),

        "shipTo": {
            "name":         missing(fake.name(), dirty),
            "street1":      missing(addr.get("street", fake.street_address()), dirty),
            "city":         missing(addr.get("city", fake.city()), dirty),
            "postalCode":   missing(addr.get("postcode", fake.postcode()), dirty),
            "country":      missing(addr.get("country", "GB"), dirty),
            "phone":        missing(fake.phone_number()[:20], dirty),
        },

        "shipFrom": {
            "warehouseId":  random.choice(WAREHOUSES),
            "name":         "Ecommerce Fulfilment Centre",
            "street1":      "1 Warehouse Road",
            "city":         "Birmingham",
            "postalCode":   "B1 1AA",
            "country":      "GB",
        },

        "weight": {
            "value": missing(round(random.uniform(0.1, 10.0), 2), dirty),
            "units": "kilograms",
        },

        "dimensions": missing({
            "length": random.randint(5, 60),
            "width":  random.randint(5, 40),
            "height": random.randint(2, 30),
            "units":  "centimetres",
        }, dirty, base=0.10, elev=0.30),

        "shipmentCost":     missing(round(random.uniform(2.99, 14.99), 2), dirty),
        "insuranceCost":    0.0,

        "shipmentStatus":   status_data["status"],
        "deliveryDate":     delivered_at.isoformat() if delivered_at else None,

        "trackingEvents":   [
            {
                "ts":          e["ts"].isoformat(),
                "description": e["description"],
                "location":    missing(fake.city(), dirty),
            }
            for e in status_data["tracking_events"]
        ],

        "isReturnLabel":    order["order_status"] == "refunded",
        "voided":           False,
        "voidDate":         None,

        # Dirty mode: 2% wrong data types
        "orderId": (
            f"ORDER_{order_id}"
            if dirty and random.random() < 0.02
            else order_id
        ),
    }

    return record


# ─────────────────────────────────────────────────────────────
# OUTPUT
# ─────────────────────────────────────────────────────────────

def write_page(records: list, output_dir: Path, page_num: int,
               ts: datetime) -> None:
    date_path = output_dir / ts.strftime("%Y/%m/%d/%H")
    date_path.mkdir(parents=True, exist_ok=True)
    file_path = date_path / f"shipstation_page_{page_num:04d}.json"

    with open(file_path, "w") as f:
        json.dump({
            "total":    len(records),
            "page":     page_num,
            "pages":    1,
            "shipments": records,
            "fetched_at": ts.isoformat(),
        }, f, default=str)


# ─────────────────────────────────────────────────────────────
# BURST MODE
# ─────────────────────────────────────────────────────────────

def run_burst(output_dir: Path, days: int = 7, dirty: bool = False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}")
    t0 = time.time()

    log.info("Fetching shipped orders from Postgres...")
    orders = fetch_shipped_orders(days)
    log.info(f"  Found {len(orders)} shipped/delivered orders")

    if not orders:
        log.warning("No shipped orders found. Run Postgres generator first.")
        return

    stats     = {"written": 0, "skipped": 0, "quarantined": 0, "pages": 0}
    page_size = 100
    page_num  = 0
    buffer    = []

    for order in orders:
        record = build_shipment(order, dirty)

        if record is None:
            stats["skipped"] += 1
            continue

        if not record.get("trackingNumber") and not dirty:
            quarantine(output_dir, "missing_tracking_number",
                       {"order_id": order["order_id"]})
            stats["quarantined"] += 1
            continue

        buffer.append(record)
        stats["written"] += 1

        if len(buffer) >= page_size:
            ts = order["placed_at"]
            if isinstance(ts, datetime) and ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            write_page(buffer, output_dir, page_num, ts)
            stats["pages"] += 1
            page_num += 1
            buffer    = []

    if buffer:
        write_page(buffer, output_dir, page_num, datetime.now(timezone.utc))
        stats["pages"] += 1

    elapsed = time.time() - t0
    log.info(f"✓ BURST COMPLETE | {elapsed:.1f}s | {stats}")
    if elapsed > 120:
        log.warning(f"Rule 2 VIOLATION: {elapsed:.0f}s > 120s")
    else:
        log.info(f"Rule 2 ✅ {elapsed:.1f}s < 120s")


# ─────────────────────────────────────────────────────────────
# STREAM MODE
# ─────────────────────────────────────────────────────────────

def run_stream(output_dir: Path, dirty: bool = False):
    log.info(f"STREAM MODE | ~5K records/day | dirty={dirty} | Ctrl+C to stop")

    orders = fetch_shipped_orders(days=7)
    if not orders:
        log.error("No shipped orders. Run burst first.")
        sys.exit(1)

    stats, i = {"written": 0, "skipped": 0}, 0

    try:
        while True:
            i     += 1
            order  = random.choice(orders)
            record = build_shipment(order, dirty)

            if record:
                write_page([record], output_dir, i, datetime.now(timezone.utc))
                stats["written"] += 1
            else:
                stats["skipped"] += 1

            if i % 100 == 0:
                log.info(f"Stream — {stats}")

            time.sleep(STREAM_SLEEP)

    except KeyboardInterrupt:
        log.info(f"Stopped. {stats}")


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Source 07 — ShipStation generator")
    p.add_argument("--mode",       choices=["burst", "stream"], required=True)
    p.add_argument("--days",       type=int, default=7)
    p.add_argument("--dirty",      action="store_true")
    p.add_argument("--output-dir", type=str,
                   default=os.environ.get("SHIPSTATION_OUTPUT_DIR", "/tmp/shipstation_raw"))
    args = p.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"Source 07 | mode={args.mode} | days={args.days} | "
             f"dirty={args.dirty} | output={output_dir}")

    if args.mode == "burst":
        run_burst(output_dir, args.days, args.dirty)
    elif args.mode == "stream":
        run_stream(output_dir, args.dirty)


if __name__ == "__main__":
    main()

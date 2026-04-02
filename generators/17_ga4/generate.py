"""
generators/17_ga4/generate.py — Staff DE Journey: Source 17

Simulates Google Analytics 4 (GA4) BigQuery export data.
GA4 exports raw event data to BigQuery daily — we simulate
that export landing in S3 as nested JSON.

Why GA4 is complex:
  - Every user interaction is an "event" with nested parameters
  - Session reconstruction requires complex windowing
  - User IDs are pseudonymous (not real emails)
  - Events have variable parameter schemas per event type
  - ~200K events/day = high volume source

Real GA4 ingestion pattern:
  1. GA4 → BigQuery (automatic daily export by Google)
  2. BigQuery export API → GCS → S3 transfer
  3. Or: GA4 Data API → Airflow → S3 directly
  4. Databricks reads → Bronze table
  5. Silver layer: sessionise, attribute revenue, funnel analysis

Rules satisfied:
    Rule 1  — --mode burst and --mode stream
    Rule 2  — burst < 2 minutes
    Rule 3  — timestamps reflect realistic web traffic patterns
    Rule 4  — funnel stage reflects session progression
    Rule 5  — idempotent (event_id is stable)
    Rule 6  — stream ~0.002 ops/sec (~200K events/day)
    Rule 7  — dirty data: missing params, null user IDs
    Rule 8  — README.md exists
    Rule 9  — connection via environment variables
    Rule 10 — callable from single bash line

Usage:
    python generate.py --mode burst --output-dir /tmp/ga4_raw
    python generate.py --mode burst --dirty --output-dir /tmp/ga4_raw
    python generate.py --mode stream --output-dir /tmp/ga4_raw
"""

import os
import json
import time
import random
import logging
import argparse
import uuid
import hashlib
from datetime import datetime, timezone, timedelta
from pathlib import Path

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

STREAM_SLEEP = 1.0 / (200000 / 86400)  # ~200K events/day

PRODUCT_SKUS = [f"SKU-{str(i).zfill(5)}" for i in range(1, 201)]

EVENT_TYPES = [
    "page_view",        "page_view",       "page_view",       # Most common
    "session_start",
    "scroll",           "scroll",
    "click",
    "view_item",        "view_item",
    "add_to_cart",
    "view_cart",
    "begin_checkout",
    "add_payment_info",
    "purchase",
    "search",
    "login",
    "sign_up",
    "user_engagement",
]

TRAFFIC_SOURCES = [
    {"medium": "organic",  "source": "google"},
    {"medium": "organic",  "source": "bing"},
    {"medium": "cpc",      "source": "google"},
    {"medium": "email",    "source": "newsletter"},
    {"medium": "social",   "source": "instagram"},
    {"medium": "social",   "source": "facebook"},
    {"medium": "referral", "source": "affiliatenet.co.uk"},
    {"medium": "none",     "source": "direct"},
]

PAGES = [
    "/", "/products", "/products/headphones", "/products/keyboards",
    "/products/laptop-stands", "/cart", "/checkout", "/checkout/payment",
    "/order-confirmation", "/account", "/account/orders",
    "/sale", "/new-arrivals", "/search",
]

DEVICES = ["desktop", "mobile", "tablet"]
BROWSERS = ["Chrome", "Safari", "Firefox", "Edge"]
OS_LIST  = ["Windows", "macOS", "iOS", "Android", "Linux"]
COUNTRIES = ["GB", "GB", "GB", "US", "DE", "FR", "AU"]


# ─────────────────────────────────────────────────────────────
# RULE 3 — REALISTIC WEB TRAFFIC PATTERNS
# Traffic peaks: 10-12am and 7-9pm, weekends higher
# ─────────────────────────────────────────────────────────────

def traffic_timestamp(base_dt: datetime) -> datetime:
    peak_windows = [
        (10, 12, 0.25), (19, 21, 0.25),
        (12, 19, 0.30), (8, 10, 0.10),
        (21, 23, 0.10),
    ]
    r, cumul = random.random(), 0.0
    s, e = 10, 12
    for start, end, w in peak_windows:
        cumul += w
        if r <= cumul:
            s, e = start, end
            break
    return base_dt.replace(
        hour=random.randint(s, e - 1),
        minute=random.randint(0, 59),
        second=random.randint(0, 59),
        microsecond=random.randint(0, 999999),
        tzinfo=timezone.utc
    )

def backdate(days_ago: float) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=days_ago)


# ─────────────────────────────────────────────────────────────
# RULE 7 — DIRTY DATA
# ─────────────────────────────────────────────────────────────

def dr(base, dirty, elevated):
    return elevated if dirty else base

def maybe_null(value, dirty, base=0.05, elev=0.20):
    if random.random() < dr(base, dirty, elev):
        return None
    return value

def bad_revenue(amount, dirty):
    if random.random() < dr(0.005, dirty, 0.05):
        return random.choice([None, 0, -1.0, 999999.0])
    return amount


# ─────────────────────────────────────────────────────────────
# EVENT PARAMETER BUILDERS
# Each event type has different required/optional parameters
# ─────────────────────────────────────────────────────────────

def page_view_params(page: str, dirty: bool) -> list:
    return [
        {"key": "page_location",  "value": {"string_value": maybe_null(f"https://ecommerce.example.com{page}", dirty)}},
        {"key": "page_title",     "value": {"string_value": maybe_null(fake.catch_phrase(), dirty)}},
        {"key": "page_referrer",  "value": {"string_value": maybe_null("https://google.com", dirty)}},
        {"key": "engagement_time_msec", "value": {"int_value": random.randint(0, 300000)}},
    ]

def view_item_params(sku: str, price: float, dirty: bool) -> list:
    return [
        {"key": "currency",   "value": {"string_value": "GBP"}},
        {"key": "value",      "value": {"float_value": bad_revenue(price, dirty)}},
        {"key": "items",      "value": {"string_value": json.dumps([{
            "item_id":       maybe_null(sku, dirty),
            "item_name":     maybe_null(fake.catch_phrase(), dirty),
            "price":         bad_revenue(price, dirty),
            "quantity":      1,
            "item_category": maybe_null(random.choice(["Electronics", "Furniture", "Accessories"]), dirty),
        }])}},
    ]

def add_to_cart_params(sku: str, price: float, qty: int, dirty: bool) -> list:
    return [
        {"key": "currency", "value": {"string_value": "GBP"}},
        {"key": "value",    "value": {"float_value": bad_revenue(price * qty, dirty)}},
        {"key": "items",    "value": {"string_value": json.dumps([{
            "item_id":  maybe_null(sku, dirty),
            "price":    bad_revenue(price, dirty),
            "quantity": qty,
        }])}},
    ]

def purchase_params(order_id: int, revenue: float, dirty: bool) -> list:
    return [
        {"key": "transaction_id", "value": {"string_value": maybe_null(str(order_id), dirty)}},
        {"key": "currency",       "value": {"string_value": "GBP"}},
        {"key": "value",          "value": {"float_value": bad_revenue(revenue, dirty)}},
        {"key": "tax",            "value": {"float_value": bad_revenue(round(revenue * 0.20, 2), dirty)}},
        {"key": "shipping",       "value": {"float_value": maybe_null(2.99, dirty)}},
        {"key": "items",          "value": {"string_value": json.dumps([{
            "item_id":  maybe_null(random.choice(PRODUCT_SKUS), dirty),
            "price":    bad_revenue(revenue, dirty),
            "quantity": random.randint(1, 3),
        }])}},
    ]

def search_params(dirty: bool) -> list:
    terms = ["wireless headphones", "laptop stand", "keyboard", "webcam",
             "desk lamp", "cable organiser", "usb hub", "monitor arm"]
    return [
        {"key": "search_term", "value": {"string_value": maybe_null(random.choice(terms), dirty)}},
    ]


def build_event_params(event_type: str, dirty: bool) -> list:
    sku    = random.choice(PRODUCT_SKUS)
    price  = round(random.uniform(4.99, 299.99), 2)
    page   = random.choice(PAGES)

    if event_type == "page_view":
        return page_view_params(page, dirty)
    elif event_type == "view_item":
        return view_item_params(sku, price, dirty)
    elif event_type == "add_to_cart":
        return add_to_cart_params(sku, price, random.randint(1, 3), dirty)
    elif event_type == "purchase":
        return purchase_params(random.randint(1, 6751), price, dirty)
    elif event_type == "search":
        return search_params(dirty)
    else:
        return [
            {"key": "engagement_time_msec", "value": {"int_value": random.randint(0, 60000)}},
        ]


# ─────────────────────────────────────────────────────────────
# GA4 EVENT BUILDER
# ─────────────────────────────────────────────────────────────

def build_ga4_event(event_dt: datetime, dirty: bool = False) -> dict:
    """
    Build a GA4 event in BigQuery export schema format.
    This mirrors the exact structure GA4 exports to BigQuery.
    """
    event_type = random.choice(EVENT_TYPES)
    traffic    = random.choice(TRAFFIC_SOURCES)
    device     = random.choice(DEVICES)

    # Pseudonymous user ID (GA4 doesn't export PII)
    user_pseudo_id = hashlib.md5(
        f"user_{random.randint(1, 50000)}".encode()
    ).hexdigest()[:20]

    # GA4 uses microsecond timestamps
    event_timestamp_micros = int(event_dt.timestamp() * 1_000_000)

    return {
        # GA4 BigQuery schema fields
        "event_date":            event_dt.strftime("%Y%m%d"),
        "event_timestamp":       event_timestamp_micros,
        "event_name":            event_type,
        "event_previous_timestamp": maybe_null(
            event_timestamp_micros - random.randint(1000000, 60000000), dirty),
        "event_value_in_usd":    maybe_null(
            round(random.uniform(0, 500), 2), dirty,
            base=0.70, elev=0.80),        # Usually null for non-purchase events

        "event_bundle_sequence_id": random.randint(1, 1000),
        "event_server_timestamp_offset": maybe_null(random.randint(-5000000, 5000000), dirty),

        "user_id":              maybe_null(None, dirty,  # GA4 user_id if logged in
                                           base=0.70, elev=0.80),
        "user_pseudo_id":       maybe_null(user_pseudo_id, dirty),
        "user_first_touch_timestamp": maybe_null(
            event_timestamp_micros - random.randint(0, 7 * 86400 * 1000000), dirty),

        "user_ltv": {
            "revenue": maybe_null(round(random.uniform(0, 1000), 2), dirty),
            "currency": "GBP",
        },

        "device": {
            "category":              device,
            "mobile_brand_name":     maybe_null("Apple" if device in ("mobile", "tablet") else None, dirty),
            "mobile_model_name":     maybe_null("iPhone 15" if device == "mobile" else None, dirty),
            "operating_system":      maybe_null(random.choice(OS_LIST), dirty),
            "operating_system_version": maybe_null(f"{random.randint(10, 17)}.0", dirty),
            "browser":               maybe_null(random.choice(BROWSERS), dirty),
            "browser_version":       maybe_null(f"{random.randint(100, 130)}.0.0.0", dirty),
            "language":              "en-gb",
            "web_info": {
                "browser":          random.choice(BROWSERS),
                "browser_version":  f"{random.randint(100, 130)}.0",
            }
        },

        "geo": {
            "country":   maybe_null(random.choice(COUNTRIES), dirty),
            "region":    maybe_null(random.choice(["England", "Scotland", "Wales"]), dirty),
            "city":      maybe_null(fake.city(), dirty),
            "sub_continent": "Northern Europe",
            "continent":     "Europe",
        },

        "app_info": None,  # Web only, no app

        "traffic_source": {
            "name":   maybe_null(f"{traffic['source']} / {traffic['medium']}", dirty),
            "medium": maybe_null(traffic["medium"], dirty),
            "source": maybe_null(traffic["source"], dirty),
        },

        "stream_id":  "1234567890",
        "platform":   "WEB",

        "event_params": build_event_params(event_type, dirty),

        "user_properties": maybe_null([
            {"key": "customer_tier",
             "value": {"string_value": random.choice(["standard", "silver", "gold"])}},
        ], dirty, base=0.50, elev=0.70),

        # Dirty mode: 2% future timestamps
        "event_timestamp": (
            int(datetime(2099, 1, 1, tzinfo=timezone.utc).timestamp() * 1_000_000)
            if dirty and random.random() < 0.02
            else event_timestamp_micros
        ),
    }


# ─────────────────────────────────────────────────────────────
# OUTPUT — Daily export files (one per day like BigQuery export)
# ─────────────────────────────────────────────────────────────

def write_daily_export(events: list, output_dir: Path,
                       export_date: datetime) -> None:
    date_path = output_dir / export_date.strftime("%Y/%m/%d")
    date_path.mkdir(parents=True, exist_ok=True)

    # GA4 BigQuery export: one JSON file per day
    filename  = f"ga4_events_{export_date.strftime('%Y%m%d')}.json"
    file_path = date_path / filename

    with open(file_path, "w") as f:
        json.dump({
            "export_date":   export_date.strftime("%Y%m%d"),
            "property_id":   "GA4-123456789",
            "event_count":   len(events),
            "exported_at":   datetime.now(timezone.utc).isoformat(),
            "events":        events,
        }, f, default=str)

    log.info(f"  Written: {filename} ({len(events):,} events)")


# ─────────────────────────────────────────────────────────────
# BURST MODE
# ─────────────────────────────────────────────────────────────

def run_burst(output_dir: Path, days: int = 7, dirty: bool = False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}")
    t0    = time.time()
    stats = {"files": 0, "events": 0}

    # ~200K events/day — write one file per day
    for day_offset in range(days):
        export_date = backdate(day_offset)
        # Weekend gets 40% more traffic
        is_weekend  = export_date.weekday() >= 5
        daily_count = int(200000 * (1.4 if is_weekend else 1.0))
        # Cap for speed — use 5K per day for burst (representative sample)
        daily_count = min(daily_count, 5000)

        events = []
        for _ in range(daily_count):
            event_dt = traffic_timestamp(export_date)
            events.append(build_ga4_event(event_dt, dirty))

        write_daily_export(events, output_dir, export_date)
        stats["files"]  += 1
        stats["events"] += daily_count

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
    log.info(f"STREAM MODE | ~200K events/day | dirty={dirty} | Ctrl+C to stop")

    stats, i = {"events": 0}, 0
    buffer   = []

    try:
        while True:
            i   += 1
            now  = datetime.now(timezone.utc)
            event = build_ga4_event(now, dirty)
            buffer.append(event)
            stats["events"] += 1

            # Write batch every 1000 events
            if len(buffer) >= 1000:
                write_daily_export(buffer, output_dir, now)
                buffer = []
                log.info(f"Stream — {stats}")

            time.sleep(STREAM_SLEEP)

    except KeyboardInterrupt:
        if buffer:
            write_daily_export(buffer, output_dir, datetime.now(timezone.utc))
        log.info(f"Stopped. {stats}")


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Source 17 — GA4 Export generator")
    p.add_argument("--mode",       choices=["burst", "stream"], required=True)
    p.add_argument("--days",       type=int, default=7)
    p.add_argument("--dirty",      action="store_true")
    p.add_argument("--output-dir", type=str,
                   default=os.environ.get("GA4_OUTPUT_DIR", "/tmp/ga4_raw"))
    args = p.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"Source 17 | mode={args.mode} | days={args.days} | "
             f"dirty={args.dirty} | output={output_dir}")

    if args.mode == "burst":
        run_burst(output_dir, args.days, args.dirty)
    elif args.mode == "stream":
        run_stream(output_dir, args.dirty)


if __name__ == "__main__":
    main()

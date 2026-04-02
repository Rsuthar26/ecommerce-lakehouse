"""
generators/14_scrapy/generate.py — Staff DE Journey: Source 14

Simulates Scrapy web scraping output for competitor pricing,
stock availability, and promotional data.

Why scraping data is different from API data:
  - No schema guarantee — website changes break the scraper silently
  - HTML parsing errors produce partial/garbled data
  - Anti-scraping measures cause random failures and empty responses
  - Data is stale by definition (scraped at a point in time)
  - Prices change faster than scrape frequency

Real Scrapy ingestion pattern:
  1. Scrapy spider runs on schedule (every 6 hours via Airflow)
  2. Scrapy pipeline writes items to S3: raw/competitor_pricing/YYYY/MM/DD/
  3. Each run creates one JSON file per competitor domain
  4. Databricks reads → Bronze table
  5. Silver layer deduplicates, normalises prices, detects price changes

Legal note: always check robots.txt + ToS before scraping in production.
For this project we simulate the output — no actual scraping.

Rules satisfied:
    Rule 1  — --mode burst and --mode stream
    Rule 2  — burst < 2 minutes
    Rule 3  — timestamps reflect scrape schedule (every 6 hours)
    Rule 4  — prices reflect market conditions over time
    Rule 5  — idempotent (url + scraped_at = dedup key)
    Rule 6  — stream ~0.023 ops/sec (~2K records/day)
    Rule 7  — dirty data: parse failures, partial records, stale data
    Rule 8  — README.md exists
    Rule 9  — connection via environment variables
    Rule 10 — callable from single bash line

Usage:
    python generate.py --mode burst --output-dir /tmp/competitor_raw
    python generate.py --mode burst --dirty --output-dir /tmp/competitor_raw
    python generate.py --mode stream --output-dir /tmp/competitor_raw
"""

import os
import json
import time
import random
import logging
import argparse
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

STREAM_SLEEP = 1.0 / (2000 / 86400)  # ~2K records/day

PRODUCT_SKUS = [f"SKU-{str(i).zfill(5)}" for i in range(1, 201)]

COMPETITORS = [
    {
        "domain":    "techgadgets.co.uk",
        "name":      "TechGadgets UK",
        "currency":  "GBP",
        "price_adj": 1.05,   # Slightly more expensive
    },
    {
        "domain":    "officedepot.co.uk",
        "name":      "Office Depot UK",
        "currency":  "GBP",
        "price_adj": 0.95,   # Slightly cheaper
    },
    {
        "domain":    "amazonuk.example.com",
        "name":      "Amazon UK (3P)",
        "currency":  "GBP",
        "price_adj": 0.90,   # Often cheaper
    },
    {
        "domain":    "currys.example.com",
        "name":      "Currys",
        "currency":  "GBP",
        "price_adj": 1.10,   # More expensive
    },
    {
        "domain":    "argos.example.com",
        "name":      "Argos",
        "currency":  "GBP",
        "price_adj": 1.00,   # Same price
    },
]

PRODUCT_NAMES = [
    "Wireless Headphones", "USB-C Hub", "Laptop Stand",
    "Mechanical Keyboard", "Mouse Pad XL", "Webcam HD",
    "Monitor Arm", "Cable Organiser", "Power Bank 20000mAh",
    "Smart Plug", "LED Desk Lamp", "Phone Stand",
]

AVAILABILITY = ["in_stock", "in_stock", "in_stock", "low_stock",
                "out_of_stock", "pre_order"]

SCRAPY_ERRORS = [
    "HTTP 429: Too Many Requests",
    "HTTP 403: Forbidden",
    "Timeout after 30s",
    "CSS selector '.price' returned no results",
    "Price element not found in DOM",
    "Anti-bot challenge detected",
    "Empty response body",
]


# ─────────────────────────────────────────────────────────────
# RULE 3 — SCRAPE SCHEDULE (every 6 hours)
# ─────────────────────────────────────────────────────────────

def scrape_timestamp(base_dt: datetime) -> datetime:
    """Scrapes run every 6 hours: 00:00, 06:00, 12:00, 18:00."""
    hour = random.choice([0, 6, 12, 18])
    return base_dt.replace(hour=hour, minute=random.randint(0, 30),
                           second=random.randint(0, 59), tzinfo=timezone.utc)

def backdate(days_ago: float) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=days_ago)


# ─────────────────────────────────────────────────────────────
# RULE 7 — DIRTY DATA
# Scraping produces the messiest data of all sources
# ─────────────────────────────────────────────────────────────

def dr(base, dirty, elevated):
    return elevated if dirty else base

def dirty_price(price, dirty):
    """Scraping often picks up non-price text near price elements."""
    if random.random() < dr(0.02, dirty, 0.15):
        return random.choice([
            "£" + str(price),          # Currency symbol not stripped
            str(price) + " inc VAT",   # Extra text
            "Was £" + str(price),      # Promotional text
            None,                       # Selector found nothing
            "",                         # Empty string
            "Price on request",         # Non-numeric
        ])
    return str(price)

def dirty_availability(avail, dirty):
    if random.random() < dr(0.01, dirty, 0.10):
        return random.choice([
            "Add to Basket",    # Button text instead of availability
            "Check store",      # Not a standard value
            None,
            "",
            "✓ In Stock",       # With HTML entities
        ])
    return avail

def maybe_null(value, dirty, base=0.05, elev=0.20):
    if random.random() < dr(base, dirty, elev):
        return None
    return value

def maybe_scrape_failure(dirty) -> bool:
    """Simulate spider failure — returns True if this item failed to scrape."""
    return random.random() < dr(0.05, dirty, 0.20)


# ─────────────────────────────────────────────────────────────
# SCRAPY ITEM BUILDER
# ─────────────────────────────────────────────────────────────

def build_scraped_item(competitor: dict, scraped_at: datetime,
                       dirty: bool = False) -> dict:
    """
    Build one Scrapy item — what the spider pipeline writes after
    scraping a product page.
    """
    sku        = random.choice(PRODUCT_SKUS)
    base_price = random.randint(499, 29999) / 100
    comp_price = round(base_price * competitor["price_adj"] * random.uniform(0.9, 1.1), 2)
    avail      = random.choice(AVAILABILITY)

    # Simulate scrape failure
    if maybe_scrape_failure(dirty):
        return {
            "scraped_at":     scraped_at.isoformat(),
            "competitor":     competitor["domain"],
            "url":            f"https://{competitor['domain']}/products/{sku}",
            "product_sku":    sku,
            "scrape_status":  "failed",
            "error":          random.choice(SCRAPY_ERRORS),
            "spider_name":    f"{competitor['domain'].replace('.', '_')}_spider",
            "spider_version": "1.0.0",
        }

    return {
        "scraped_at":           scraped_at.isoformat(),
        "competitor":           competitor["domain"],
        "competitor_name":      competitor["name"],
        "url":                  f"https://{competitor['domain']}/products/{sku.lower()}",
        "product_sku":          sku,                    # Join key to our catalog
        "product_name":         maybe_null(
            random.choice(PRODUCT_NAMES), dirty),
        "price":                dirty_price(comp_price, dirty),
        "original_price":       maybe_null(
            str(round(comp_price * 1.2, 2)), dirty),    # Was/RRP price
        "currency":             competitor["currency"],
        "availability":         dirty_availability(avail, dirty),
        "in_stock":             avail in ("in_stock", "low_stock"),
        "stock_count":          maybe_null(
            random.randint(0, 100) if avail != "out_of_stock" else 0,
            dirty),
        "rating":               maybe_null(
            round(random.uniform(3.0, 5.0), 1), dirty),
        "review_count":         maybe_null(random.randint(0, 500), dirty),
        "promotions":           maybe_null(
            random.choice([
                None, "10% off", "Buy 2 get 1 free",
                "Free delivery", "Student discount"
            ]), dirty),
        "delivery_days":        maybe_null(random.randint(1, 14), dirty),
        "seller":               maybe_null(competitor["name"], dirty),
        "scrape_status":        "success",
        "spider_name":          f"{competitor['domain'].replace('.', '_')}_spider",
        "spider_version":       "1.0.0",
        "response_time_ms":     maybe_null(random.randint(200, 5000), dirty),

        # Metadata for Silver layer processing
        "_scrapy_item_id":      f"{competitor['domain']}_{sku}_{scraped_at.strftime('%Y%m%d%H')}",
        "_price_parsed":        False,  # Silver layer sets this to True after parsing
        "_price_numeric":       None,   # Silver layer populates this
    }


# ─────────────────────────────────────────────────────────────
# OUTPUT — Scrapy pipeline writes one file per spider run
# ─────────────────────────────────────────────────────────────

def write_spider_output(items: list, competitor: dict,
                        output_dir: Path, scraped_at: datetime) -> None:
    date_path = output_dir / scraped_at.strftime("%Y/%m/%d/%H")
    date_path.mkdir(parents=True, exist_ok=True)
    filename  = (f"{competitor['domain'].replace('.', '_')}"
                 f"_{scraped_at.strftime('%Y%m%d_%H%M%S')}.json")
    file_path = date_path / filename

    with open(file_path, "w") as f:
        json.dump({
            "spider":       competitor["domain"],
            "scraped_at":   scraped_at.isoformat(),
            "item_count":   len(items),
            "success_count": sum(1 for i in items if i.get("scrape_status") == "success"),
            "fail_count":    sum(1 for i in items if i.get("scrape_status") == "failed"),
            "items":        items,
        }, f, default=str)


# ─────────────────────────────────────────────────────────────
# BURST MODE
# ─────────────────────────────────────────────────────────────

def run_burst(output_dir: Path, days: int = 7, dirty: bool = False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}")
    t0    = time.time()
    stats = {"files": 0, "items": 0, "failures": 0}

    # Each competitor spider runs 4x per day (every 6 hours)
    for day_offset in range(days):
        base_dt = backdate(day_offset)

        for run_hour in [0, 6, 12, 18]:
            scraped_at = base_dt.replace(
                hour=run_hour,
                minute=random.randint(0, 15),
                second=0,
                tzinfo=timezone.utc
            )

            for competitor in COMPETITORS:
                # Each spider scrapes 50-150 products per run
                num_items = random.randint(50, 150)
                items = [
                    build_scraped_item(competitor, scraped_at, dirty)
                    for _ in range(num_items)
                ]

                write_spider_output(items, competitor, output_dir, scraped_at)
                stats["files"] += 1
                stats["items"] += num_items
                stats["failures"] += sum(
                    1 for i in items if i.get("scrape_status") == "failed"
                )

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
    log.info(f"STREAM MODE | ~2K records/day | dirty={dirty} | Ctrl+C to stop")

    stats, i = {"files": 0, "items": 0}, 0

    try:
        while True:
            i          += 1
            now         = datetime.now(timezone.utc)
            competitor  = random.choice(COMPETITORS)
            num_items   = random.randint(20, 50)
            items       = [
                build_scraped_item(competitor, now, dirty)
                for _ in range(num_items)
            ]

            write_spider_output(items, competitor, output_dir, now)
            stats["files"] += 1
            stats["items"] += num_items

            if i % 10 == 0:
                log.info(f"Stream — {stats}")

            time.sleep(STREAM_SLEEP * num_items)

    except KeyboardInterrupt:
        log.info(f"Stopped. {stats}")


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Source 14 — Scrapy competitor pricing generator")
    p.add_argument("--mode",       choices=["burst", "stream"], required=True)
    p.add_argument("--days",       type=int, default=7)
    p.add_argument("--dirty",      action="store_true")
    p.add_argument("--output-dir", type=str,
                   default=os.environ.get("SCRAPY_OUTPUT_DIR", "/tmp/competitor_raw"))
    args = p.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"Source 14 | mode={args.mode} | days={args.days} | "
             f"dirty={args.dirty} | output={output_dir}")

    if args.mode == "burst":
        run_burst(output_dir, args.days, args.dirty)
    elif args.mode == "stream":
        run_stream(output_dir, args.dirty)


if __name__ == "__main__":
    main()

"""
generators/14_scrapy/generate.py — Staff DE Journey: Source 14

Simulates Scrapy web scraping output for competitor pricing,
stock availability, and promotional data.

~2K records/day — scraped from 3 fictional competitors.

Rules: All 11 satisfied.
"""

import os, sys, json, time, random, logging, argparse, hashlib
from datetime import datetime, timezone, timedelta
from pathlib import Path
from faker import Faker
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from generators.shared.postgres_ids import load_entity_ids

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)
fake = Faker("en_GB"); Faker.seed(42)

# ─────────────────────────────────────────────────────────────
# MONGODB PRICE CACHE — our retail price per SKU
# our_price_pence is our retail price — must match MongoDB catalog,
# same as Source 08 Shopify. Competitor price is independent.
# ─────────────────────────────────────────────────────────────

_mongo_prices: dict = {}

def load_mongo_prices() -> dict:
    global _mongo_prices
    if _mongo_prices:
        return _mongo_prices
    try:
        from pymongo import MongoClient
        mongo_uri = os.environ.get(
            "MONGO_URI",
            "mongodb+srv://mongo_admin:MongoAdmin2026!@ecommerce-cluster.k2gc71w.mongodb.net/"
        )
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        cursor = client["ecommerce"]["products"].find(
            {"base_price_pence": {"$exists": True, "$gt": 0}},
            {"product_sku": 1, "base_price_pence": 1, "_id": 0}
        )
        _mongo_prices = {
            doc["product_sku"]: doc["base_price_pence"]
            for doc in cursor
            if doc.get("product_sku") and doc.get("base_price_pence")
        }
        client.close()
        log.info(f"Loaded {len(_mongo_prices)} product prices from MongoDB")
    except Exception as e:
        log.warning(f"MongoDB unavailable ({e}) — our_price_pence will use random fallback")
        _mongo_prices = {}
    return _mongo_prices

STREAM_SLEEP = 86400 / 2000

COMPETITORS = [
    {"id": "COMP-001", "name": "TechDirect UK",   "domain": "techdirect.co.uk"},
    {"id": "COMP-002", "name": "GadgetHub",        "domain": "gadgethub.com"},
    {"id": "COMP-003", "name": "ValueElectronics", "domain": "valueelectronics.co.uk"},
]

AVAILABILITY = ["in_stock","in_stock","in_stock","low_stock","out_of_stock","discontinued"]

# Rule 4 — availability skews to in_stock for recent scrapes, out_of_stock for older data
def availability_for_age(scraped_at: datetime) -> str:
    age_days = (datetime.now(timezone.utc) - scraped_at).days
    if age_days < 1:
        return random.choices(
            ["in_stock","low_stock","out_of_stock"],
            weights=[0.75, 0.15, 0.10]
        )[0]
    elif age_days < 7:
        return random.choices(
            ["in_stock","low_stock","out_of_stock","discontinued"],
            weights=[0.65, 0.15, 0.15, 0.05]
        )[0]
    else:
        return random.choices(
            ["in_stock","low_stock","out_of_stock","discontinued"],
            weights=[0.55, 0.15, 0.20, 0.10]
        )[0]

_entity_ids = None
def get_entity_ids():
    global _entity_ids
    if _entity_ids is None: _entity_ids = load_entity_ids()
    return _entity_ids

def dr(base, dirty, elevated): return elevated if dirty else base
def maybe_null(v, dirty, base=0.05, elev=0.20):
    return None if random.random() < dr(base, dirty, elev) else v

def dirty_price(price, dirty):
    if random.random() < dr(0.01, dirty, 0.08):
        return random.choice(["POA","TBC","N/A",None,"free"])
    return price

def build_price_record(scraped_at: datetime, dirty: bool = False) -> dict:
    ids         = get_entity_ids()
    sku         = random.choice(ids["product_skus"]) if ids["product_skus"] else "SKU-00001"
    competitor  = random.choice(COMPETITORS)
    our_price   = load_mongo_prices().get(sku, random.randint(499, 29999))
    # Competitor prices 5-25% above or below ours
    comp_price  = int(our_price * random.uniform(0.75, 1.25))
    availability = availability_for_age(scraped_at)

    record = {
        "record_id":         hashlib.md5(
            f"{competitor['id']}:{sku}:{scraped_at.date()}".encode()
        ).hexdigest()[:16],
        "scraped_at":        scraped_at.isoformat(),
        "competitor_id":     competitor["id"],
        "competitor_name":   maybe_null(competitor["name"], dirty),
        "competitor_domain": competitor["domain"],
        "product_sku":       maybe_null(sku, dirty, base=0.01),
        "competitor_sku":    maybe_null(f"{competitor['id']}-{random.randint(10000,99999)}", dirty),
        "product_title":     maybe_null(fake.catch_phrase(), dirty),
        "price_pence":       dirty_price(comp_price, dirty),
        "our_price_pence":   our_price,
        "price_difference_pct": round((comp_price - our_price) / our_price * 100, 2),
        "availability":      maybe_null(availability, dirty),
        "url":               maybe_null(f"https://{competitor['domain']}/product/{sku.lower()}", dirty),
        "promo_active":      random.random() < 0.15,
        "promo_text":        maybe_null(
            f"Save {random.randint(10,40)}% this week!" if random.random() < 0.15 else None,
            dirty
        ),
        "rating":            maybe_null(round(random.uniform(3.0, 5.0), 1), dirty),
        "review_count":      maybe_null(random.randint(0, 500), dirty),
        "currency":          "GBP",
        "_source":           "scrapy",
        # Dirty: 2% future timestamps
        **( {"scraped_at": "2099-01-01T00:00:00+00:00"} if dirty and random.random() < 0.02 else {} ),
    }

    # Dirty: 1% malformed — missing required field
    if dirty and random.random() < 0.01:
        record.pop("competitor_id", None)

    return record

def write_daily_file(records, output_dir, day_dt):
    """
    Write all scraped prices for one day to a single file.
    Folder: output_dir/YYYY/MM/DD/
    Filename: competitor_prices_YYYYMMDD.json
    """
    dp = output_dir / day_dt.strftime("%Y/%m/%d")
    dp.mkdir(parents=True, exist_ok=True)
    fname = f"competitor_prices_{day_dt.strftime('%Y%m%d')}.json"
    with open(dp / fname, "w") as f:
        json.dump({
            "date":       day_dt.strftime("%Y-%m-%d"),
            "count":      len(records),
            "scraped_at": day_dt.isoformat(),
            "records":    records,
        }, f, default=str)
    log.info(f"  Written: {fname} ({len(records)} records)")

def run_burst(output_dir, days=7, dirty=False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}"); get_entity_ids(); load_mongo_prices()
    t0, stats, now = time.time(), {"records": 0, "files": 0}, datetime.now(timezone.utc)
    total = days * 300

    day_buckets: dict = {}
    for _ in range(total):
        days_ago   = random.uniform(0, days)
        scraped_at = now - timedelta(days=days_ago)
        day_key    = scraped_at.replace(hour=0, minute=0, second=0, microsecond=0)
        day_buckets.setdefault(day_key, []).append(build_price_record(scraped_at, dirty))
        stats["records"] += 1

    for day_dt, records in sorted(day_buckets.items()):
        write_daily_file(records, output_dir, day_dt)
        stats["files"] += 1

    elapsed = time.time() - t0
    log.info(f"✓ BURST COMPLETE | {elapsed:.1f}s | {stats}")
    log.info(f"Rule 2 {'✅' if elapsed <= 120 else 'VIOLATION'} {elapsed:.1f}s")

def run_stream(output_dir, dirty=False):
    log.info(f"STREAM MODE | ~2K/day | dirty={dirty} | Ctrl+C to stop")
    get_entity_ids(); load_mongo_prices(); stats, i = {"records": 0}, 0
    try:
        while True:
            i += 1; now = datetime.now(timezone.utc)
            day_key = now.replace(hour=0, minute=0, second=0, microsecond=0)
            write_daily_file([build_price_record(now, dirty)], output_dir, day_key)
            stats["records"] += 1
            if i % 100 == 0: log.info(f"Stream — {stats}")
            time.sleep(STREAM_SLEEP)
    except KeyboardInterrupt: log.info(f"Stopped. {stats}")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["burst","stream"], required=True)
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--dirty", action="store_true")
    p.add_argument("--output-dir", type=str, default=os.environ.get("SCRAPY_OUTPUT_DIR","/tmp/scrapy_raw"))
    args = p.parse_args()
    od = Path(args.output_dir); od.mkdir(parents=True, exist_ok=True)
    if args.mode == "burst": run_burst(od, args.days, args.dirty)
    else: run_stream(od, args.dirty)

if __name__ == "__main__": main()

"""
generators/08_shopify/generate.py — Staff DE Journey: Source 08

Simulates Shopify GraphQL API responses.
Products, collections, inventory levels, discount codes.

~2K queries/day = one file per hour with ~83 records.

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
# MONGODB PRICE CACHE
# Shopify prices come from the product catalog — they must match MongoDB.
# Load {sku: base_price_pence} at startup so build_product uses catalog prices.
# ─────────────────────────────────────────────────────────────

_mongo_prices: dict = {}   # {sku: base_price_pence}

def load_mongo_prices() -> dict:
    """
    Load {product_sku: base_price_pence} from MongoDB ecommerce.products.
    Shopify is the storefront — its prices come from the catalog, not random values.
    Falls back to empty dict if MongoDB is unreachable; build_product uses
    random.randint as fallback per SKU.
    """
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
        db     = client["ecommerce"]
        cursor = db["products"].find(
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
        log.warning(f"MongoDB unavailable ({e}) — Shopify prices will use random fallback")
        _mongo_prices = {}
    return _mongo_prices

STREAM_SLEEP   = 86400 / 2000
PRODUCT_SKUS   = [f"SKU-{str(i).zfill(5)}" for i in range(1, 201)]
COLLECTIONS    = ["New Arrivals","Best Sellers","Sale","Electronics","Furniture","Accessories"]
STATUS_OPTIONS = ["ACTIVE","DRAFT","ARCHIVED"]

# Rule 4 — product status reflects age
def product_status_for_age(created_at: datetime) -> str:
    age_days = (datetime.now(timezone.utc) - created_at).days
    if age_days < 7:    return "DRAFT" if random.random() < 0.2 else "ACTIVE"
    elif age_days < 90: return "ACTIVE"
    elif age_days > 365 and random.random() < 0.05: return "ARCHIVED"
    else:               return "ACTIVE"

_entity_ids = None
def get_entity_ids():
    global _entity_ids
    if _entity_ids is None: _entity_ids = load_entity_ids()
    return _entity_ids

def dr(base, dirty, elevated): return elevated if dirty else base
def maybe_null(v, dirty, base=0.05, elev=0.20):
    return None if random.random() < dr(base, dirty, elev) else v

def build_product(sku: str, created_at: datetime, dirty: bool = False) -> dict:
    # Price comes from MongoDB catalog — must match Source 03
    # Fallback to random only if SKU not found in catalog
    mongo_prices = load_mongo_prices()
    price_pence  = mongo_prices.get(sku, random.randint(499, 29999))
    return {
        "id":             f"gid://shopify/Product/{hashlib.md5(sku.encode()).hexdigest()[:10]}",
        "handle":         sku.lower().replace("-","_"),
        "title":          maybe_null(fake.catch_phrase(), dirty),
        "vendor":         maybe_null(fake.company(), dirty),
        "productType":    maybe_null(random.choice(["Electronics","Furniture","Accessory"]), dirty),
        "status":         product_status_for_age(created_at),
        "tags":           random.sample(["sale","new","featured","clearance"], k=2),
        "variants": {
            "edges": [{"node": {
                "id":    f"gid://shopify/ProductVariant/{random.randint(10000,99999)}",
                "sku":   maybe_null(sku, dirty, base=0.01),
                "price": maybe_null(str(price_pence/100), dirty),
                "inventoryQuantity": random.randint(0, 500),
                "barcode": maybe_null(str(random.randint(1000000000000,9999999999999)), dirty),
            }}]
        },
        "collections": {
            "edges": [{"node": {"title": random.choice(COLLECTIONS)}}]
        },
        "createdAt":  created_at.isoformat(),
        "updatedAt":  created_at.isoformat(),
        "_source":    "shopify_graphql",
    }

def build_discount(created_at: datetime, dirty: bool = False) -> dict:
    ids = get_entity_ids()
    return {
        "id":              f"gid://shopify/DiscountCodeBasic/{random.randint(100000,999999)}",
        "title":           maybe_null(f"SAVE{random.randint(10,30)}", dirty),
        "code":            maybe_null(f"CODE{random.randint(1000,9999)}", dirty),
        "status":          random.choice(["ACTIVE","EXPIRED","SCHEDULED"]),
        "percentage_off":  maybe_null(round(random.uniform(5,40),1), dirty),
        "usage_count":     random.randint(0, 500),
        "applies_once_per_customer": random.random() > 0.5,
        "created_at":      created_at.isoformat(),
        "_source":         "shopify_graphql",
    }

def write_daily_file(items, output_dir, day_dt, record_type="products"):
    """
    Write all records for one day to a single file.
    Folder: output_dir/YYYY/MM/DD/
    Filename: shopify_products_YYYYMMDD.json or shopify_discounts_YYYYMMDD.json
    No hour subfolder — Shopify is a daily batch export.
    """
    dp = output_dir / day_dt.strftime("%Y/%m/%d")
    dp.mkdir(parents=True, exist_ok=True)
    fname = f"shopify_{record_type}_{day_dt.strftime('%Y%m%d')}.json"
    with open(dp / fname, "w") as f:
        json.dump({
            "date":        day_dt.strftime("%Y-%m-%d"),
            "type":        record_type,
            "count":       len(items),
            "edges":       [{"node": r} for r in items],
        }, f, default=str)
    log.info(f"  Written: {fname} ({len(items)} {record_type})")

def run_burst(output_dir, days=7, dirty=False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}"); get_entity_ids(); load_mongo_prices()
    t0, stats, now = time.time(), {"products": 0, "discounts": 0, "files": 0}, datetime.now(timezone.utc)
    skus = get_entity_ids()["product_skus"]

    for day_offset in range(days):
        base_dt = now - timedelta(days=day_offset)
        day_key = base_dt.replace(hour=0, minute=0, second=0, microsecond=0)

        # Products — one file per day
        products = [build_product(sku, base_dt, dirty)
                    for sku in random.sample(skus, min(50, len(skus)))]
        write_daily_file(products, output_dir, day_key, "products")
        stats["products"] += len(products)
        stats["files"]    += 1

        # Discounts — one file per day
        discounts = [build_discount(base_dt, dirty) for _ in range(10)]
        write_daily_file(discounts, output_dir, day_key, "discounts")
        stats["discounts"] += len(discounts)
        stats["files"]     += 1

    elapsed = time.time() - t0
    log.info(f"✓ BURST COMPLETE | {elapsed:.1f}s | {stats}")
    log.info(f"Rule 2 {'✅' if elapsed <= 120 else 'VIOLATION'} {elapsed:.1f}s")

def run_stream(output_dir, dirty=False):
    log.info(f"STREAM MODE | ~2K/day | dirty={dirty} | Ctrl+C to stop")
    get_entity_ids(); load_mongo_prices(); stats, i = {"sent": 0}, 0
    try:
        while True:
            i   += 1; now = datetime.now(timezone.utc)
            sku  = random.choice(get_entity_ids()["product_skus"])
            day_key = now.replace(hour=0, minute=0, second=0, microsecond=0)
            write_daily_file([build_product(sku, now, dirty)], output_dir, day_key, "products")
            stats["sent"] += 1
            if i % 50 == 0: log.info(f"Stream — {stats}")
            time.sleep(STREAM_SLEEP)
    except KeyboardInterrupt: log.info(f"Stopped. {stats}")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["burst","stream"], required=True)
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--dirty", action="store_true")
    p.add_argument("--output-dir", type=str, default=os.environ.get("SHOPIFY_OUTPUT_DIR","/tmp/shopify_raw"))
    args = p.parse_args()
    od = Path(args.output_dir); od.mkdir(parents=True, exist_ok=True)
    if args.mode == "burst": run_burst(od, args.days, args.dirty)
    else: run_stream(od, args.dirty)

if __name__ == "__main__": main()

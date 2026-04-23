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
    price_pence = random.randint(499, 29999)
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

def write_batch(items, output_dir, batch_num, ts, record_type="products"):
    dp = output_dir / ts.strftime("%Y/%m/%d/%H"); dp.mkdir(parents=True, exist_ok=True)
    with open(dp / f"shopify_{record_type}_{batch_num:04d}.json","w") as f:
        json.dump({"batch": batch_num, "type": record_type, "count": len(items), "edges": [{"node": r} for r in items]}, f, default=str)

def run_burst(output_dir, days=7, dirty=False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}"); get_entity_ids()
    t0, stats, now = time.time(), {"products":0,"discounts":0}, datetime.now(timezone.utc)
    for day_offset in range(days):
        base_dt = now - timedelta(days=day_offset)
        products = [build_product(sku, base_dt, dirty) for sku in random.sample(get_entity_ids()["product_skus"], min(50, len(get_entity_ids()["product_skus"])))]
        write_batch(products, output_dir, day_offset, base_dt)
        stats["products"] += len(products)
        discounts = [build_discount(base_dt, dirty) for _ in range(10)]
        write_batch(discounts, output_dir, day_offset, base_dt, "discounts")
        stats["discounts"] += len(discounts)
    elapsed = time.time() - t0
    log.info(f"✓ BURST COMPLETE | {elapsed:.1f}s | {stats}")
    log.info(f"Rule 2 {'✅' if elapsed <= 120 else 'VIOLATION'} {elapsed:.1f}s")

def run_stream(output_dir, dirty=False):
    log.info(f"STREAM MODE | ~2K/day | dirty={dirty} | Ctrl+C to stop")
    get_entity_ids(); stats, i = {"sent":0}, 0
    try:
        while True:
            i += 1; now = datetime.now(timezone.utc)
            sku = random.choice(get_entity_ids()["product_skus"])
            write_batch([build_product(sku, now, dirty)], output_dir, i, now)
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

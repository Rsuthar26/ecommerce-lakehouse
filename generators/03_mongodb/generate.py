"""
generators/03_mongodb/generate.py — Staff DE Journey: Source 03

Simulates the product catalog stored in MongoDB Atlas.
200 products with nested variants, category-specific attributes, price history.

product_sku matches what order_items in Postgres references.
This is the cross-system foreign key — Postgres holds the order reference,
MongoDB holds the product detail.

Rules satisfied:
    Rule 1  — --mode burst and --mode stream
    Rule 2  — burst < 2 minutes (200 products = fast)
    Rule 3  — product created_at spread across burst period
    Rule 4  — product status reflects age and inventory level
    Rule 5  — upsert by _id (idempotent — safe to run twice)
    Rule 6  — stream: ~1 product update per 9s (~10K/day)
    Rule 7  — dirty: missing fields, wrong types, truncated descriptions
    Rule 8  — README.md exists
    Rule 9  — env vars: MONGO_URI
    Rule 10 — callable from single bash line
    Rule 11 — product_sku matches Postgres order_items (same SKU-XXXXX format)

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
from datetime import datetime, timezone, timedelta

from faker import Faker
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

try:
    from pymongo import MongoClient, UpdateOne
    from pymongo.errors import BulkWriteError
except ImportError:
    log.error("pymongo not installed: pip install pymongo")
    sys.exit(1)

fake = Faker("en_GB")
Faker.seed(42)

STREAM_SLEEP = 86400 / 10000  # Rule 6 + Rule 13: ~10K updates/day
MONGO_URI    = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME      = os.environ.get("MONGO_DBNAME", "ecommerce")
COLLECTION   = "products"
HISTORY_COL  = "product_price_history"

PRODUCT_SKUS = [f"SKU-{str(i).zfill(5)}" for i in range(1, 201)]  # Must match Postgres

CATEGORIES = {
    "Electronics": {
        "subcategories": ["Audio", "Computing", "Cameras", "Wearables", "Smart Home"],
        "brands": ["SoundMax", "TechPro", "AudioEdge", "PixelVision"],
        "attributes": lambda: {
            "connectivity": random.choice(["Bluetooth 5.0", "USB-C", "WiFi 6", "Wired"]),
            "warranty_years": random.randint(1, 3),
            "power_watts": random.randint(5, 150),
        }
    },
    "Furniture": {
        "subcategories": ["Desks", "Chairs", "Storage", "Shelving"],
        "brands": ["WorkSpace Co", "ErgoLine", "ModernHome"],
        "attributes": lambda: {
            "material":  random.choice(["Oak", "Walnut", "MDF", "Steel", "Acrylic"]),
            "assembly_required": random.random() > 0.3,
            "weight_kg": round(random.uniform(2, 50), 1),
            "max_load_kg": random.randint(20, 200),
        }
    },
    "Accessories": {
        "subcategories": ["Cables", "Cases", "Stands", "Organisers"],
        "brands": ["AccessPro", "QuickFit", "SlimLine"],
        "attributes": lambda: {
            "compatibility": random.choice(["Universal", "Apple", "Samsung", "Multi-device"]),
            "colour": random.choice(["Black", "White", "Silver", "Space Grey"]),
        }
    },
}


# ─────────────────────────────────────────────────────────────
# RULE 4 — PRODUCT STATUS REFLECTS AGE AND STOCK
# ─────────────────────────────────────────────────────────────

def product_status(created_at: datetime, stock: int) -> str:
    age_days = (datetime.now(timezone.utc) - created_at).days
    if stock == 0:
        return "out_of_stock"
    elif stock < 10:
        return "low_stock"
    elif age_days < 14:
        return "new"
    elif age_days > 365 and random.random() < 0.05:
        return "discontinued"
    else:
        return "active"


# ─────────────────────────────────────────────────────────────
# RULE 7 — DIRTY DATA
# ─────────────────────────────────────────────────────────────

def dr(base, dirty, elevated): return elevated if dirty else base
def maybe_null(v, dirty, base=0.05, elev=0.20):
    return None if random.random() < dr(base, dirty, elev) else v

def dirty_price(price_pence, dirty):
    if random.random() < dr(0.005, dirty, 0.08):
        return random.choice(["POA", None, -1, 0, "free"])
    return price_pence

def dirty_sku(sku, dirty):
    if random.random() < dr(0.01, dirty, 0.10):
        return random.choice(["", None, sku.lower(), "UNKNOWN"])
    return sku


# ─────────────────────────────────────────────────────────────
# PRODUCT BUILDER
# ─────────────────────────────────────────────────────────────

def build_product(sku: str, created_at: datetime, dirty: bool = False) -> dict:
    category_name = random.choice(list(CATEGORIES.keys()))
    category      = CATEGORIES[category_name]
    subcategory   = random.choice(category["subcategories"])
    brand         = random.choice(category["brands"])
    price_pence   = random.randint(499, 29999)
    cost_pence    = int(price_pence * random.uniform(0.4, 0.65))
    stock         = random.randint(0, 500)

    # Variants — e.g. colour variants of the same product
    variants = []
    for colour in random.sample(["Black", "White", "Silver", "Blue", "Red"], k=random.randint(1, 3)):
        v_price = int(price_pence * random.uniform(0.9, 1.1))
        variants.append({
            "colour":       colour,
            "sku_variant":  f"{sku}-{colour[:3].upper()}",
            "price_pence":  dirty_price(v_price, dirty),
            "stock":        maybe_null(random.randint(0, 200), dirty),
            "weight_grams": maybe_null(random.randint(100, 5000), dirty),
        })

    status = product_status(created_at, stock)

    product = {
        "_id":              dirty_sku(sku, dirty),
        "product_sku":      sku,  # Always clean — needed for joins
        "name":             maybe_null(fake.catch_phrase(), dirty),
        "category":         category_name,
        "subcategory":      maybe_null(subcategory, dirty),
        "brand":            maybe_null(brand, dirty),
        "description":      maybe_null(
            " ".join(fake.sentences(nb=random.randint(2, 4))), dirty),
        "base_price_pence": dirty_price(price_pence, dirty),
        "cost_price_pence": maybe_null(cost_pence, dirty),
        "variants":         variants,
        "attributes":       maybe_null(category["attributes"](), dirty),
        "images":           [
            f"https://cdn.ecommerce.example.com/products/{sku}/main.jpg",
            f"https://cdn.ecommerce.example.com/products/{sku}/alt1.jpg",
        ] if not dirty or random.random() > 0.05 else [],
        "tags":             random.sample(
            ["sale", "new", "bestseller", "clearance", "exclusive", "bundle"], k=2),
        "status":           status,
        "rating":           maybe_null(round(random.uniform(3.0, 5.0), 1), dirty),
        "review_count":     maybe_null(random.randint(0, 500), dirty),
        "created_at":       created_at.isoformat(),
        "updated_at":       created_at.isoformat(),
    }

    # Dirty: 1% truncated description (simulate partial write)
    if dirty and random.random() < 0.01 and product.get("description"):
        desc = product["description"]
        product["description"] = desc[:random.randint(10, 30)]

    # Dirty: 0.5% wrong type for price
    if dirty and random.random() < 0.005:
        product["base_price_pence"] = str(price_pence)  # String instead of int

    return product


def build_price_history(sku: str, created_at: datetime, current_price: int) -> dict:
    return {
        "product_sku": sku,
        "price_pence": current_price,
        "changed_at":  created_at.isoformat(),
        "reason":      random.choice(["promotion", "restock", "market_adjustment"]),
    }


# ─────────────────────────────────────────────────────────────
# BURST MODE
# ─────────────────────────────────────────────────────────────

def run_burst(days: int = 7, dirty: bool = False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}")
    t0     = time.time()
    client = MongoClient(MONGO_URI)
    db     = client[DB_NAME]
    col    = db[COLLECTION]
    hcol   = db[HISTORY_COL]
    stats  = {"upserted": 0, "history": 0}

    now = datetime.now(timezone.utc)
    ops = []
    history_ops = []

    for sku in PRODUCT_SKUS:
        days_ago   = random.uniform(0, days)
        created_at = now - timedelta(days=days_ago)
        product    = build_product(sku, created_at, dirty)

        # Rule 5: upsert by _id — safe to run twice
        ops.append(UpdateOne({"_id": sku}, {"$set": product}, upsert=True))

        # Price history
        if isinstance(product.get("base_price_pence"), int):
            history_ops.append(build_price_history(
                sku, created_at, product["base_price_pence"]
            ))
            stats["history"] += 1

        stats["upserted"] += 1

    # Bulk write
    if ops:
        try:
            result = col.bulk_write(ops, ordered=False)
            log.info(f"  Products upserted: {result.upserted_count + result.modified_count}")
        except BulkWriteError as e:
            log.error(f"Bulk write error: {e.details}")

    if history_ops:
        hcol.insert_many(history_ops)

    client.close()
    elapsed = time.time() - t0
    log.info(f"✓ BURST COMPLETE | {elapsed:.1f}s | {stats}")
    log.info(f"Rule 2 {'✅' if elapsed <= 120 else 'VIOLATION'} {elapsed:.1f}s")


# ─────────────────────────────────────────────────────────────
# STREAM MODE — simulate ongoing catalog changes
# ─────────────────────────────────────────────────────────────

def run_stream(dirty: bool = False):
    log.info(f"STREAM MODE | ~10K/day | dirty={dirty} | Ctrl+C to stop")
    client = MongoClient(MONGO_URI)
    db     = client[DB_NAME]
    col    = db[COLLECTION]
    hcol   = db[HISTORY_COL]
    stats  = {"updates": 0}
    i      = 0

    try:
        while True:
            i   += 1
            sku  = random.choice(PRODUCT_SKUS)
            now  = datetime.now(timezone.utc)

            # Simulate: price change, stock update, or status change
            change_type = random.choice(["price", "stock", "status"])

            if change_type == "price":
                new_price = random.randint(499, 29999)
                col.update_one(
                    {"_id": sku},
                    {"$set": {
                        "base_price_pence": new_price,
                        "updated_at": now.isoformat()
                    }},
                    upsert=True
                )
                hcol.insert_one(build_price_history(sku, now, new_price))

            elif change_type == "stock":
                col.update_one(
                    {"_id": sku},
                    {"$set": {
                        "updated_at": now.isoformat()
                    },
                     "$inc": {"variants.0.stock": random.randint(-5, 50)}},
                    upsert=True
                )
            else:
                col.update_one(
                    {"_id": sku},
                    {"$set": {
                        "status":     random.choice(["active", "low_stock"]),
                        "updated_at": now.isoformat(),
                    }},
                    upsert=True
                )

            stats["updates"] += 1
            if i % 100 == 0:
                log.info(f"Stream — {stats}")
            time.sleep(STREAM_SLEEP)

    except KeyboardInterrupt:
        client.close()
        log.info(f"Stopped. {stats}")


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Source 03 — MongoDB Product Catalog")
    p.add_argument("--mode",  choices=["burst","stream"], required=True)
    p.add_argument("--days",  type=int, default=7)
    p.add_argument("--dirty", action="store_true")
    args = p.parse_args()
    log.info(f"Source 03 | mode={args.mode} | days={args.days} | dirty={args.dirty}")
    if args.mode == "burst":   run_burst(args.days, args.dirty)
    elif args.mode == "stream": run_stream(args.dirty)

if __name__ == "__main__":
    main()

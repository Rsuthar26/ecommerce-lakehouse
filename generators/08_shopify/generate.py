"""
generators/08_shopify/generate.py — Staff DE Journey: Source 08

Simulates Shopify GraphQL API responses for products, collections,
inventory levels, and discounts.

Key difference from REST sources (06, 07):
  - Responses use Relay-style pagination: edges → node
  - Every collection is wrapped: {edges: [{node: {...}, cursor: "..."}]}
  - Silver layer must unwrap this consistently

Real Shopify ingestion pattern:
  1. Airflow triggers every 4 hours
  2. Job sends GraphQL query with cursor-based pagination
  3. Fetches until hasNextPage = false
  4. Writes each page as JSON to S3: raw/shopify/YYYY/MM/DD/HH/page_N.json
  5. Databricks Autoloader picks up → Bronze table

Rules satisfied:
    Rule 1  — --mode burst and --mode stream
    Rule 2  — burst < 2 minutes
    Rule 3  — timestamps backdated realistically
    Rule 4  — product status reflects age
    Rule 5  — idempotent (Shopify GID is stable)
    Rule 6  — stream ~0.023 ops/sec (~2K records/day)
    Rule 7  — dirty data + quarantine
    Rule 8  — README.md exists
    Rule 9  — connection via environment variables
    Rule 10 — callable from single bash line

Usage:
    python generate.py --mode burst --output-dir /tmp/shopify_raw
    python generate.py --mode burst --dirty --output-dir /tmp/shopify_raw
    python generate.py --mode stream --output-dir /tmp/shopify_raw
"""

import os
import sys
import json
import time
import random
import logging
import argparse
import base64
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

# Shopify uses Global IDs (GIDs) — format: gid://shopify/Type/ID
def shopify_gid(resource_type: str, numeric_id: int) -> str:
    raw = f"gid://shopify/{resource_type}/{numeric_id}"
    return base64.b64encode(raw.encode()).decode()

# Product SKUs must match Postgres + MongoDB (cross-source consistency)
PRODUCT_SKUS = [f"SKU-{str(i).zfill(5)}" for i in range(1, 201)]

COLLECTIONS = [
    {"id": 1001, "title": "Electronics",      "handle": "electronics"},
    {"id": 1002, "title": "Home Office",       "handle": "home-office"},
    {"id": 1003, "title": "Accessories",       "handle": "accessories"},
    {"id": 1004, "title": "Best Sellers",      "handle": "best-sellers"},
    {"id": 1005, "title": "New Arrivals",      "handle": "new-arrivals"},
    {"id": 1006, "title": "Sale",              "handle": "sale"},
]

PRODUCT_TYPES = ["Electronics", "Furniture", "Accessories", "Peripherals", "Cables"]
VENDORS       = ["SoundMax", "TechPro", "ErgoDesk", "CableMate", "LightUp"]
COLOURS       = ["Black", "White", "Silver", "Space Grey", "Midnight Blue"]


# ─────────────────────────────────────────────────────────────
# RULE 3 — REALISTIC TIMESTAMPS
# ─────────────────────────────────────────────────────────────

def backdate(days_ago: float) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=days_ago)


# ─────────────────────────────────────────────────────────────
# RULE 4 — STATUS REFLECTS AGE
# ─────────────────────────────────────────────────────────────

def product_status(created_at: datetime) -> str:
    age_days = (datetime.now(timezone.utc) - created_at).days
    if age_days < 1:
        return "DRAFT"
    elif age_days > 180 and random.random() < 0.10:
        return "ARCHIVED"
    return "ACTIVE"


# ─────────────────────────────────────────────────────────────
# RULE 7 — DIRTY DATA
# ─────────────────────────────────────────────────────────────

def dr(base, dirty, elevated):
    return elevated if dirty else base

def missing(value, dirty, base=0.05, elev=0.20):
    return None if random.random() < dr(base, dirty, elev) else value

def bad_price(price_str, dirty):
    if random.random() < dr(0.005, dirty, 0.08):
        return random.choice(["0.00", "-1.00", None, "free", "N/A"])
    return price_str


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
# SHOPIFY DOCUMENT BUILDER
# ─────────────────────────────────────────────────────────────

def build_product_node(sku: str, product_id: int,
                       created_at: datetime, dirty: bool = False) -> dict:
    """
    Build a Shopify product node in Relay-style GraphQL format.
    Mirrors what the Shopify Admin GraphQL API actually returns.
    """
    num_variants = random.randint(1, 4)
    variants     = []

    for i, colour in enumerate(random.sample(COLOURS, num_variants)):
        variant_id    = product_id * 100 + i
        price         = f"{random.randint(499, 29999) / 100:.2f}"
        compare_price = f"{float(price) * 1.2:.2f}" if random.random() < 0.3 else None

        variants.append({
            "node": {
                "id":                shopify_gid("ProductVariant", variant_id),
                "title":             colour,
                "sku":               f"{sku}-{colour[:3].upper()}",
                "price":             bad_price(price, dirty),
                "compareAtPrice":    missing(compare_price, dirty),
                "inventoryQuantity": missing(random.randint(0, 200), dirty),
                "weight":            missing(round(random.uniform(0.1, 10.0), 2), dirty),
                "weightUnit":        "KILOGRAMS",
                "taxable":           True,
                "requiresShipping":  True,
                "availableForSale":  random.random() > 0.1,
                "createdAt":         created_at.isoformat(),
                "updatedAt":         (created_at + timedelta(
                                         days=random.uniform(0, 5))).isoformat(),
            },
            "cursor": base64.b64encode(f"variant_{variant_id}".encode()).decode()
        })

    # Metafields — Shopify's key-value store for custom data
    metafields = []
    if random.random() > 0.3:
        metafields.append({
            "node": {
                "namespace": "custom",
                "key":       "warranty_years",
                "value":     str(random.choice([1, 2, 3])),
                "type":      "number_integer",
            },
            "cursor": base64.b64encode(b"metafield_1").decode()
        })

    status    = product_status(created_at)
    title     = f"{random.choice(VENDORS)} {fake.word().title()} Pro"
    price_str = f"{random.randint(499, 29999) / 100:.2f}"

    node = {
        "id":           shopify_gid("Product", product_id),
        "title":        missing(title, dirty, base=0.01, elev=0.05),
        "handle":       title.lower().replace(" ", "-") if title else None,
        "description":  missing(fake.sentence(nb_words=15), dirty),
        "productType":  missing(random.choice(PRODUCT_TYPES), dirty),
        "vendor":       missing(random.choice(VENDORS), dirty),
        "status":       status,
        "tags":         random.sample(
                            ["bestseller", "new", "sale", "eco", "premium", "bundle"],
                            random.randint(0, 3)),

        # Price range (Shopify returns min/max across all variants)
        "priceRangeV2": {
            "minVariantPrice": {
                "amount":       bad_price(price_str, dirty),
                "currencyCode": "GBP"
            },
            "maxVariantPrice": {
                "amount":       bad_price(f"{float(price_str) * 1.3:.2f}", dirty),
                "currencyCode": "GBP"
            }
        },

        # Cross-system reference — matches Postgres + MongoDB SKU
        "variants": {
            "edges":    variants,
            "pageInfo": {
                "hasNextPage":     False,
                "hasPreviousPage": False,
            }
        },

        "metafields": {
            "edges": metafields
        },

        "images": {
            "edges": [
                {
                    "node": {
                        "id":  shopify_gid("Image", product_id * 10 + i),
                        "url": f"https://cdn.shopify.com/s/files/1/0001/products/{sku}_{i}.jpg",
                        "altText": missing(title, dirty),
                        "width":   800,
                        "height":  800,
                    },
                    "cursor": base64.b64encode(f"image_{i}".encode()).decode()
                }
                for i in range(random.randint(1, 4))
            ]
        },

        "collections": {
            "edges": [
                {
                    "node": {
                        "id":     shopify_gid("Collection", c["id"]),
                        "title":  c["title"],
                        "handle": c["handle"],
                    }
                }
                for c in random.sample(COLLECTIONS, random.randint(1, 3))
            ]
        },

        "createdAt":    created_at.isoformat(),
        "updatedAt":    (created_at + timedelta(days=random.uniform(0, 7))).isoformat(),
        "publishedAt":  created_at.isoformat() if status == "ACTIVE" else None,

        # Custom field — links to our internal SKU system
        # This is the join key to Postgres order_items + MongoDB products
        "_internalSku": sku,
    }

    return node


def wrap_in_graphql_response(nodes: list[dict], query_name: str = "products",
                              has_next_page: bool = False,
                              end_cursor: str = None) -> dict:
    """
    Wrap product nodes in the standard Shopify GraphQL response envelope.

    This is the key difference from REST:
      REST:    {"shipments": [...]}
      GraphQL: {"data": {"products": {"edges": [{"node": {...}, "cursor": "..."}]}}}

    Silver layer must always unwrap: response["data"][query_name]["edges"][N]["node"]
    """
    edges = [
        {
            "node":   node,
            "cursor": base64.b64encode(f"product_{node.get('_internalSku', i)}".encode()).decode()
        }
        for i, node in enumerate(nodes)
    ]

    return {
        "data": {
            query_name: {
                "edges":    edges,
                "pageInfo": {
                    "hasNextPage":     has_next_page,
                    "hasPreviousPage": False,
                    "startCursor":     edges[0]["cursor"] if edges else None,
                    "endCursor":       end_cursor or (edges[-1]["cursor"] if edges else None),
                }
            }
        },
        "extensions": {
            "cost": {
                "requestedQueryCost":  random.randint(10, 50),
                "actualQueryCost":     random.randint(5, 45),
                "throttleStatus": {
                    "maximumAvailable":  1000.0,
                    "currentlyAvailable": random.randint(800, 1000),
                    "restoreRate":       50.0,
                }
            }
        }
    }


# ─────────────────────────────────────────────────────────────
# OUTPUT
# ─────────────────────────────────────────────────────────────

def write_page(response: dict, output_dir: Path, page_num: int,
               ts: datetime) -> None:
    date_path = output_dir / ts.strftime("%Y/%m/%d/%H")
    date_path.mkdir(parents=True, exist_ok=True)
    file_path = date_path / f"shopify_page_{page_num:04d}.json"
    with open(file_path, "w") as f:
        json.dump(response, f, default=str)


# ─────────────────────────────────────────────────────────────
# BURST MODE
# ─────────────────────────────────────────────────────────────

def run_burst(output_dir: Path, days: int = 7, dirty: bool = False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}")
    t0 = time.time()

    stats     = {"written": 0, "quarantined": 0, "pages": 0}
    page_size = 10  # Shopify GraphQL typically returns 10-250 per page
    page_num  = 0
    buffer    = []

    for i, sku in enumerate(PRODUCT_SKUS):
        product_id = 1000000 + i
        days_ago   = random.uniform(0, days)
        created_at = backdate(days_ago)

        node = build_product_node(sku, product_id, created_at, dirty)

        # Validate price
        price = node.get("priceRangeV2", {}).get("minVariantPrice", {}).get("amount")
        if price is None or price in ["free", "N/A", "-1.00"]:
            quarantine(output_dir, f"invalid_price: {price!r}",
                       {"sku": sku, "price": price})
            stats["quarantined"] += 1
            # Still include with flag
            node["_price_dirty"] = True

        buffer.append(node)
        stats["written"] += 1

        if len(buffer) >= page_size:
            ts       = backdate(days_ago)
            response = wrap_in_graphql_response(
                buffer, "products",
                has_next_page=(i < len(PRODUCT_SKUS) - 1)
            )
            write_page(response, output_dir, page_num, ts)
            stats["pages"] += 1
            page_num += 1
            buffer    = []

    # Final partial page
    if buffer:
        response = wrap_in_graphql_response(buffer, "products", has_next_page=False)
        write_page(response, output_dir, page_num, datetime.now(timezone.utc))
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
    log.info(f"STREAM MODE | ~2K records/day | dirty={dirty} | Ctrl+C to stop")

    stats, i = {"written": 0}, 0

    try:
        while True:
            i         += 1
            sku        = random.choice(PRODUCT_SKUS)
            product_id = 1000000 + PRODUCT_SKUS.index(sku)
            created_at = datetime.now(timezone.utc)

            node     = build_product_node(sku, product_id, created_at, dirty)
            response = wrap_in_graphql_response([node], "products")
            write_page(response, output_dir, i, created_at)
            stats["written"] += 1

            if i % 100 == 0:
                log.info(f"Stream — {stats}")

            time.sleep(STREAM_SLEEP)

    except KeyboardInterrupt:
        log.info(f"Stopped. {stats}")


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Source 08 — Shopify GraphQL generator")
    p.add_argument("--mode",       choices=["burst", "stream"], required=True)
    p.add_argument("--days",       type=int, default=7)
    p.add_argument("--dirty",      action="store_true")
    p.add_argument("--output-dir", type=str,
                   default=os.environ.get("SHOPIFY_OUTPUT_DIR", "/tmp/shopify_raw"))
    args = p.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"Source 08 | mode={args.mode} | days={args.days} | "
             f"dirty={args.dirty} | output={output_dir}")

    if args.mode == "burst":
        run_burst(output_dir, args.days, args.dirty)
    elif args.mode == "stream":
        run_stream(output_dir, args.dirty)


if __name__ == "__main__":
    main()

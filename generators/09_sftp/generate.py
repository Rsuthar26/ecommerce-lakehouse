"""
generators/09_sftp/generate.py — Staff DE Journey: Source 09

Simulates supplier catalog and pricing files dropped via SFTP.
Generates CSV and Excel files that look like real supplier data feeds.

Fix applied: Rule 11 — real product SKUs loaded from Postgres at startup.

Rules satisfied: All 11.

Usage:
    python generate.py --mode burst --output-dir /tmp/sftp_raw
    python generate.py --mode burst --dirty --output-dir /tmp/sftp_raw
    python generate.py --mode stream --output-dir /tmp/sftp_raw
"""

import os, sys, csv, json, time, random, logging, argparse, io
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
# MONGODB PRICE CACHE — supplier cost must be below retail price
# Supplier cost = 35-65% of our MongoDB base_price_pence.
# Never random — a supplier cost higher than retail is nonsensical.
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
        log.warning(f"MongoDB unavailable ({e}) — supplier cost will use random fallback")
        _mongo_prices = {}
    return _mongo_prices

STREAM_SLEEP = 86400 / 3   # Rule 13: ~3 files/day

SUPPLIERS = [
    {"id": "SUP001", "name": "TechSupply Ltd",    "format": "csv",   "encoding": "utf-8"},
    {"id": "SUP002", "name": "GlobalParts Co",    "format": "csv",   "encoding": "utf-8"},
    {"id": "SUP003", "name": "PremiumGoods GmbH", "format": "excel", "encoding": "utf-8"},
    {"id": "SUP004", "name": "FastShip Wholesale","format": "csv",   "encoding": "latin-1"},
    {"id": "SUP005", "name": "DirectSource UK",   "format": "excel", "encoding": "utf-8"},
]
WAREHOUSES = ["WH-LONDON-01", "WH-MANC-01", "WH-BRUM-01"]

_entity_ids = None
def get_entity_ids():
    global _entity_ids
    if _entity_ids is None: _entity_ids = load_entity_ids()
    return _entity_ids

def dr(base, dirty, elevated): return elevated if dirty else base
def maybe_null(value, dirty, base=0.05, elev=0.20):
    return "" if random.random() < dr(base, dirty, elev) else value

def dirty_price(price, dirty):
    if random.random() < dr(0.005, dirty, 0.08):
        return random.choice(["POA","TBC","","N/A","-1","free"])
    return str(price)

def dirty_sku(sku, dirty):
    if random.random() < dr(0.01, dirty, 0.10):
        return random.choice(["","UNKNOWN",sku.lower(),sku+" ",""])
    return sku

def dirty_quantity(qty, dirty):
    if random.random() < dr(0.005, dirty, 0.05):
        return random.choice(["","many","-1","N/A"])
    return str(qty)

# Each supplier has different column names — intentional messiness for Silver
SCHEMA_VARIANTS = {
    "SUP001": ["SKU", "Product Name", "Unit Cost GBP", "Stock QTY", "Lead Days", "EAN"],
    "SUP002": ["sku_code", "description", "cost_price", "available_qty", "delivery_days", "barcode"],
    "SUP003": ["Item Code", "Item Description", "Price (GBP)", "Qty Available", "Lead Time", "EAN Code"],
    "SUP004": ["PART_NO", "PART_DESC", "UNIT_PRICE", "QTY_OH", "LEAD_TIME", "BARCODE"],
    "SUP005": ["product_id", "product_name", "wholesale_price", "stock_level", "days_to_ship", "ean"],
}

def generate_supplier_csv(supplier, drop_date, dirty=False):
    ids     = get_entity_ids()
    skus    = random.sample(ids["product_skus"], min(len(ids["product_skus"]), random.randint(30,80)))
    headers = SCHEMA_VARIANTS.get(supplier["id"], SCHEMA_VARIANTS["SUP001"])
    rows    = [headers]

    for sku in skus:
        retail_price = load_mongo_prices().get(sku, random.randint(499, 29999))
        cost_price   = int(retail_price * random.uniform(0.35, 0.65))
        stock_qty  = random.randint(0, 500)
        lead_days  = random.randint(1, 14)
        ean        = str(random.randint(1000000000000, 9999999999999))
        row        = [
            dirty_sku(sku, dirty),
            maybe_null(fake.catch_phrase(), dirty),
            dirty_price(f"{cost_price/100:.2f}", dirty),
            dirty_quantity(stock_qty, dirty),
            maybe_null(str(lead_days), dirty),
            maybe_null(ean, dirty),
        ]
        # Dirty: 2% truncated rows
        if dirty and random.random() < 0.02:
            row = row[:random.randint(1,4)]
        # Dirty: 1% blank rows
        if dirty and random.random() < 0.01:
            row = [""] * len(headers)
        rows.append(row)

    # Dirty: 3% duplicate rows
    if dirty:
        for _ in range(int(len(rows) * 0.03)):
            rows.append(random.choice(rows[1:]))

    output = io.StringIO()
    csv.writer(output).writerows(rows)
    filename = f"{supplier['id']}_{drop_date.strftime('%Y%m%d')}_catalog.csv"
    return filename, output.getvalue().encode(supplier.get("encoding","utf-8"))

def generate_supplier_excel(supplier, drop_date, dirty=False):
    try:
        import openpyxl
        ids = get_entity_ids()
        wb  = openpyxl.Workbook()
        ws  = wb.active; ws.title = "Product Catalog"
        ws.append(["Product Code","Description","RRP GBP","Cost Price GBP","Stock","Warehouse","Updated Date"])
        skus = random.sample(ids["product_skus"], min(len(ids["product_skus"]), random.randint(20,60)))
        for sku in skus:
            rrp   = load_mongo_prices().get(sku, random.randint(499, 29999))
            cost  = int(rrp * random.uniform(0.35, 0.65))
            stock = random.randint(0, 300)
            ws.append([
                dirty_sku(sku, dirty),
                maybe_null(fake.catch_phrase(), dirty),
                dirty_price(f"{rrp/100:.2f}", dirty),
                dirty_price(f"{cost/100:.2f}", dirty),
                dirty_quantity(stock, dirty),
                random.choice(WAREHOUSES),
                drop_date.strftime("%Y-%m-%d"),
            ])
        buf = io.BytesIO(); wb.save(buf); buf.seek(0)
        filename = f"{supplier['id']}_{drop_date.strftime('%Y%m%d')}_catalog.xlsx"
        return filename, buf.read()
    except ImportError:
        log.warning("openpyxl not installed — falling back to CSV")
        return generate_supplier_csv(supplier, drop_date, dirty)

def generate_file(supplier, drop_date, dirty=False):
    return generate_supplier_excel(supplier, drop_date, dirty) if supplier["format"] == "excel" \
           else generate_supplier_csv(supplier, drop_date, dirty)

def quarantine(output_dir, reason, data):
    qd = output_dir / "quarantine"; qd.mkdir(parents=True, exist_ok=True)
    with open(qd / f"q_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')}.json","w") as f:
        json.dump({"reason": reason, "data": data, "ts": datetime.now(timezone.utc).isoformat()}, f, default=str)

def run_burst(output_dir, days=7, dirty=False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}"); get_entity_ids(); load_mongo_prices()
    t0, stats, now = time.time(), {"files":0}, datetime.now(timezone.utc)
    for day_offset in range(days):
        drop_date = now - timedelta(days=day_offset)
        for supplier in SUPPLIERS:
            drop_hour = random.randint(6, 18)
            file_date = drop_date.replace(hour=drop_hour, minute=random.randint(0,59), second=0)
            filename, content = generate_file(supplier, file_date, dirty)
            date_path = output_dir / file_date.strftime("%Y/%m/%d"); date_path.mkdir(parents=True, exist_ok=True)
            filepath = date_path / filename
            if filepath.exists():  # Rule 5: idempotent — skip if already written
                log.info(f"  Skipped (exists): {filename}")
                continue
            with open(filepath, "wb") as f: f.write(content)
            stats["files"] += 1
            log.info(f"  Written: {filename} ({len(content):,} bytes)")
    elapsed = time.time() - t0
    log.info(f"✓ BURST COMPLETE | {elapsed:.1f}s | {stats['files']} files")
    log.info(f"Rule 2 {'✅' if elapsed <= 120 else 'VIOLATION'} {elapsed:.1f}s")

def run_stream(output_dir, dirty=False):
    log.info(f"STREAM MODE | ~3 files/day | dirty={dirty} | Ctrl+C to stop")
    get_entity_ids(); load_mongo_prices(); stats, i = {"files":0}, 0
    try:
        while True:
            i += 1; supplier = random.choice(SUPPLIERS); now = datetime.now(timezone.utc)
            filename, content = generate_file(supplier, now, dirty)
            date_path = output_dir / now.strftime("%Y/%m/%d"); date_path.mkdir(parents=True, exist_ok=True)
            with open(date_path / filename, "wb") as f: f.write(content)
            stats["files"] += 1; log.info(f"Dropped: {filename}")
            time.sleep(STREAM_SLEEP)
    except KeyboardInterrupt: log.info(f"Stopped. {stats}")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["burst","stream"], required=True)
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--dirty", action="store_true")
    p.add_argument("--output-dir", type=str, default=os.environ.get("SFTP_OUTPUT_DIR","/tmp/sftp_raw"))
    args = p.parse_args()
    od = Path(args.output_dir); od.mkdir(parents=True, exist_ok=True)
    log.info(f"Source 09 | mode={args.mode} | days={args.days} | dirty={args.dirty} | output={od}")
    if args.mode == "burst": run_burst(od, args.days, args.dirty)
    else: run_stream(od, args.dirty)

if __name__ == "__main__": main()

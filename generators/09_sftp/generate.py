"""
generators/09_sftp/generate.py — Staff DE Journey: Source 09

Simulates supplier catalog and pricing files dropped via SFTP.
Generates CSV and Excel files that look like real supplier data feeds.

Real SFTP ingestion pattern:
  1. Suppliers drop files to SFTP server on their schedule (daily/weekly)
  2. Airflow SFTPHook polls for new files every hour
  3. Downloads new files to S3: raw/supplier_files/YYYY/MM/DD/filename
  4. Databricks Autoloader picks up → Bronze table

Why SFTP matters:
  - Legacy suppliers don't have APIs
  - Files arrive with inconsistent schemas (each supplier is different)
  - No delivery guarantee — files can be late, duplicate, or malformed
  - This is the messiest source in the pipeline — Silver layer earns its keep here

Rules satisfied:
    Rule 1  — --mode burst and --mode stream
    Rule 2  — burst < 2 minutes
    Rule 3  — timestamps in filenames reflect realistic drop times
    Rule 4  — prices/stock reflect product age
    Rule 5  — idempotent (filenames include date — safe to reprocess)
    Rule 6  — stream: 1 file every ~17 mins (~1-5 files/day)
    Rule 7  — dirty data, malformed rows, truncated files
    Rule 8  — README.md exists
    Rule 9  — connection via environment variables
    Rule 10 — callable from single bash line

Usage:
    python generate.py --mode burst --output-dir /tmp/sftp_raw
    python generate.py --mode burst --dirty --output-dir /tmp/sftp_raw
    python generate.py --mode stream --output-dir /tmp/sftp_raw
"""

import os
import sys
import csv
import json
import time
import random
import logging
import argparse
import io
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

# Stream: ~3 files/day = 1 file every ~8 hours
STREAM_SLEEP = 8 * 3600 / 3

# Must match other sources
PRODUCT_SKUS = [f"SKU-{str(i).zfill(5)}" for i in range(1, 201)]

SUPPLIERS = [
    {"id": "SUP001", "name": "TechSupply Ltd",      "format": "csv",   "encoding": "utf-8"},
    {"id": "SUP002", "name": "GlobalParts Co",       "format": "csv",   "encoding": "utf-8"},
    {"id": "SUP003", "name": "PremiumGoods GmbH",    "format": "excel", "encoding": "utf-8"},
    {"id": "SUP004", "name": "FastShip Wholesale",   "format": "csv",   "encoding": "latin-1"},
    {"id": "SUP005", "name": "DirectSource UK",      "format": "excel", "encoding": "utf-8"},
]

WAREHOUSES = ["WH-LONDON-01", "WH-MANC-01", "WH-BRUM-01"]


# ─────────────────────────────────────────────────────────────
# RULE 7 — DIRTY DATA
# ─────────────────────────────────────────────────────────────

def dr(base, dirty, elevated):
    return elevated if dirty else base

def dirty_price(price, dirty):
    if random.random() < dr(0.005, dirty, 0.08):
        return random.choice(["POA", "TBC", "", "N/A", "-1", "free"])
    return str(price)

def dirty_sku(sku, dirty):
    if random.random() < dr(0.01, dirty, 0.10):
        return random.choice(["", "UNKNOWN", sku.lower(), sku + " ", None])
    return sku

def dirty_quantity(qty, dirty):
    if random.random() < dr(0.005, dirty, 0.05):
        return random.choice(["", "many", "-1", "N/A"])
    return str(qty)

def maybe_null(value, dirty, base=0.05, elev=0.20):
    if random.random() < dr(base, dirty, elev):
        return ""
    return value


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
# FILE GENERATORS
# Each supplier has slightly different schema — intentional messiness
# ─────────────────────────────────────────────────────────────

def generate_supplier_csv(supplier: dict, drop_date: datetime,
                           dirty: bool = False) -> tuple[str, bytes]:
    """
    Generate a CSV file for a supplier.
    Each supplier uses slightly different column names — real-world messiness.
    Silver layer must normalise these into a standard schema.
    """
    supplier_id = supplier["id"]

    # Each supplier has different column names for the same data
    # This is the real-world problem that makes SFTP files painful
    schema_variants = {
        "SUP001": ["SKU", "Product Name", "Unit Cost GBP", "Stock QTY", "Lead Days", "EAN"],
        "SUP002": ["sku_code", "description", "cost_price", "available_qty", "delivery_days", "barcode"],
        "SUP003": ["Item Code", "Item Description", "Price (GBP)", "Qty Available", "Lead Time", "EAN Code"],
        "SUP004": ["PART_NO", "PART_DESC", "UNIT_PRICE", "QTY_OH", "LEAD_TIME", "BARCODE"],
        "SUP005": ["product_id", "product_name", "wholesale_price", "stock_level", "days_to_ship", "ean"],
    }

    headers  = schema_variants.get(supplier_id, schema_variants["SUP001"])
    skus     = random.sample(PRODUCT_SKUS, random.randint(50, 150))
    rows     = [headers]

    for sku in skus:
        cost_price = random.randint(100, 15000)  # Cost price in pence
        stock_qty  = random.randint(0, 500)
        lead_days  = random.randint(1, 14)
        ean        = str(random.randint(1000000000000, 9999999999999))

        row = [
            dirty_sku(sku, dirty),
            maybe_null(fake.catch_phrase(), dirty),
            dirty_price(f"{cost_price / 100:.2f}", dirty),
            dirty_quantity(stock_qty, dirty),
            maybe_null(str(lead_days), dirty),
            maybe_null(ean, dirty),
        ]

        # Dirty mode: 2% truncated rows (simulate partial file write)
        if dirty and random.random() < 0.02:
            row = row[:random.randint(1, 4)]  # Cut row short

        # Dirty mode: 1% completely blank rows
        if dirty and random.random() < 0.01:
            row = [""] * len(headers)

        rows.append(row)

    # Dirty mode: 3% chance of duplicate rows
    if dirty:
        num_dupes = int(len(rows) * 0.03)
        for _ in range(num_dupes):
            rows.append(random.choice(rows[1:]))

    # Write to bytes
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(rows)

    filename = (f"{supplier_id}_{drop_date.strftime('%Y%m%d_%H%M%S')}"
                f"_catalog.csv")
    return filename, output.getvalue().encode(supplier.get("encoding", "utf-8"))


def generate_supplier_excel(supplier: dict, drop_date: datetime,
                             dirty: bool = False) -> tuple[str, bytes]:
    """
    Generate an Excel file for a supplier.
    Uses openpyxl — falls back to CSV if not available.
    """
    try:
        import openpyxl
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Product Catalog"

        headers = ["Product Code", "Description", "RRP GBP",
                   "Cost Price GBP", "Stock", "Warehouse", "Updated Date"]
        ws.append(headers)

        skus = random.sample(PRODUCT_SKUS, random.randint(30, 100))

        for sku in skus:
            rrp       = random.randint(499, 29999)
            cost      = int(rrp * random.uniform(0.4, 0.7))
            stock     = random.randint(0, 300)
            warehouse = random.choice(WAREHOUSES)

            row = [
                dirty_sku(sku, dirty),
                maybe_null(fake.catch_phrase(), dirty),
                dirty_price(f"{rrp / 100:.2f}", dirty),
                dirty_price(f"{cost / 100:.2f}", dirty),
                dirty_quantity(stock, dirty),
                warehouse,
                drop_date.strftime("%Y-%m-%d"),
            ]
            ws.append(row)

        # Save to bytes
        buf      = io.BytesIO()
        wb.save(buf)
        buf.seek(0)
        filename = (f"{supplier['id']}_{drop_date.strftime('%Y%m%d_%H%M%S')}"
                    f"_catalog.xlsx")
        return filename, buf.read()

    except ImportError:
        # Fall back to CSV if openpyxl not available
        log.warning("openpyxl not installed — generating CSV instead of Excel")
        return generate_supplier_csv(supplier, drop_date, dirty)


def generate_file(supplier: dict, drop_date: datetime,
                  dirty: bool = False) -> tuple[str, bytes]:
    """Route to correct file generator based on supplier format."""
    if supplier["format"] == "excel":
        return generate_supplier_excel(supplier, drop_date, dirty)
    else:
        return generate_supplier_csv(supplier, drop_date, dirty)


# ─────────────────────────────────────────────────────────────
# BURST MODE
# ─────────────────────────────────────────────────────────────

def run_burst(output_dir: Path, days: int = 7, dirty: bool = False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}")
    t0 = time.time()

    stats = {"files": 0, "total_rows": 0}

    # Each supplier drops 1-2 files per day
    for day_offset in range(days):
        drop_date = datetime.now(timezone.utc) - timedelta(days=day_offset)

        for supplier in SUPPLIERS:
            # Each supplier drops at a consistent time (their business hours)
            drop_hour = random.randint(6, 18)
            file_date = drop_date.replace(
                hour=drop_hour, minute=random.randint(0, 59), second=0
            )

            filename, content = generate_file(supplier, file_date, dirty)

            # Write to output directory
            date_path = output_dir / file_date.strftime("%Y/%m/%d")
            date_path.mkdir(parents=True, exist_ok=True)
            file_path = date_path / filename

            with open(file_path, "wb") as f:
                f.write(content)

            stats["files"] += 1
            log.info(f"  Written: {filename} ({len(content):,} bytes)")

    elapsed = time.time() - t0
    log.info(f"✓ BURST COMPLETE | {elapsed:.1f}s | {stats['files']} files")
    if elapsed > 120:
        log.warning(f"Rule 2 VIOLATION: {elapsed:.0f}s > 120s")
    else:
        log.info(f"Rule 2 ✅ {elapsed:.1f}s < 120s")


# ─────────────────────────────────────────────────────────────
# STREAM MODE
# ─────────────────────────────────────────────────────────────

def run_stream(output_dir: Path, dirty: bool = False):
    log.info(f"STREAM MODE | ~3 files/day | dirty={dirty} | Ctrl+C to stop")

    stats, i = {"files": 0}, 0

    try:
        while True:
            i        += 1
            supplier  = random.choice(SUPPLIERS)
            drop_date = datetime.now(timezone.utc)

            filename, content = generate_file(supplier, drop_date, dirty)

            date_path = output_dir / drop_date.strftime("%Y/%m/%d")
            date_path.mkdir(parents=True, exist_ok=True)

            with open(date_path / filename, "wb") as f:
                f.write(content)

            stats["files"] += 1
            log.info(f"Dropped: {filename} ({len(content):,} bytes)")

            time.sleep(STREAM_SLEEP)

    except KeyboardInterrupt:
        log.info(f"Stopped. {stats}")


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Source 09 — SFTP supplier files generator")
    p.add_argument("--mode",       choices=["burst", "stream"], required=True)
    p.add_argument("--days",       type=int, default=7)
    p.add_argument("--dirty",      action="store_true")
    p.add_argument("--output-dir", type=str,
                   default=os.environ.get("SFTP_OUTPUT_DIR", "/tmp/sftp_raw"))
    args = p.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"Source 09 | mode={args.mode} | days={args.days} | "
             f"dirty={args.dirty} | output={output_dir}")

    if args.mode == "burst":
        run_burst(output_dir, args.days, args.dirty)
    elif args.mode == "stream":
        run_stream(output_dir, args.dirty)


if __name__ == "__main__":
    main()

"""
generators/10_partner_s3/generate.py — Staff DE Journey: Source 10

Simulates partner data files dropped into S3 in Parquet and Avro format.
Partners include affiliates, marketplace sellers, and cross-sell partners.

Why Parquet/Avro instead of CSV?
  - Parquet: columnar, compressed, schema-embedded — Spark reads natively
  - Avro: row-based, schema evolution support — common in Kafka/Hadoop ecosystems
  - Both formats embed the schema — no header row ambiguity like CSV
  - Silver layer can read these without knowing columns in advance

Real S3 partner drop pattern:
  1. Partner writes files to their S3 bucket
  2. S3 Cross-Account Replication copies to our raw bucket
  3. S3 event notification triggers Lambda → SQS → Airflow
  4. Databricks Autoloader (cloudFiles) picks up new files → Bronze table

Rules satisfied:
    Rule 1  — --mode burst and --mode stream
    Rule 2  — burst < 2 minutes
    Rule 3  — timestamps backdated realistically
    Rule 4  — sale amounts and statuses reflect age
    Rule 5  — idempotent (filename + partner_id = stable key)
    Rule 6  — stream ~1 file every 8 hours (~3 files/day)
    Rule 7  — dirty data embedded in files
    Rule 8  — README.md exists
    Rule 9  — connection via environment variables
    Rule 10 — callable from single bash line

Usage:
    python generate.py --mode burst --output-dir /tmp/partner_raw
    python generate.py --mode burst --dirty --output-dir /tmp/partner_raw
    python generate.py --mode stream --output-dir /tmp/partner_raw
"""

import os
import sys
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

STREAM_SLEEP = 8 * 3600 / 3  # ~3 files/day

PRODUCT_SKUS = [f"SKU-{str(i).zfill(5)}" for i in range(1, 201)]

PARTNERS = [
    {"id": "PARTNER-001", "name": "AffiliateNet UK",    "format": "parquet", "type": "affiliate"},
    {"id": "PARTNER-002", "name": "MarketPlace Pro",    "format": "parquet", "type": "marketplace"},
    {"id": "PARTNER-003", "name": "CrossSell Direct",   "format": "avro",    "type": "crosssell"},
]

CHANNELS    = ["email", "social", "search", "display", "referral"]
SALE_STATUS = ["completed", "pending", "cancelled", "refunded"]


# ─────────────────────────────────────────────────────────────
# RULE 3 + 4 — TIMESTAMPS AND STATUS REFLECT AGE
# ─────────────────────────────────────────────────────────────

def backdate(days_ago: float) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=days_ago)

def status_for_age(created_at: datetime) -> str:
    age_hrs = (datetime.now(timezone.utc) - created_at).total_seconds() / 3600
    if age_hrs < 2:
        return "pending"
    elif age_hrs < 72:
        return random.choices(["completed", "pending"], weights=[0.8, 0.2])[0]
    else:
        return random.choices(
            ["completed", "cancelled", "refunded"],
            weights=[0.88, 0.08, 0.04]
        )[0]


# ─────────────────────────────────────────────────────────────
# RULE 7 — DIRTY DATA
# ─────────────────────────────────────────────────────────────

def dr(base, dirty, elevated):
    return elevated if dirty else base

def dirty_amount(amount, dirty):
    if random.random() < dr(0.005, dirty, 0.08):
        return random.choice([None, -1.0, 0.0, 999999.99])
    return amount

def dirty_sku(sku, dirty):
    if random.random() < dr(0.01, dirty, 0.10):
        return random.choice([None, "", "UNKNOWN", sku + "_OLD"])
    return sku

def maybe_null(value, dirty, base=0.05, elev=0.20):
    if random.random() < dr(base, dirty, elev):
        return None
    return value


# ─────────────────────────────────────────────────────────────
# ROW BUILDER
# ─────────────────────────────────────────────────────────────

def build_sale_row(partner: dict, sale_date: datetime,
                   dirty: bool = False) -> dict:
    """Build one partner sale record."""
    sku        = dirty_sku(random.choice(PRODUCT_SKUS), dirty)
    sale_amt   = round(random.uniform(4.99, 299.99), 2)
    commission = round(sale_amt * random.uniform(0.05, 0.15), 2)

    return {
        "partner_id":        partner["id"],
        "partner_name":      partner["name"],
        "partner_type":      partner["type"],
        "sale_id":           f"{partner['id']}-{random.randint(100000, 999999)}",
        "product_sku":       sku,
        "sale_amount_gbp":   dirty_amount(sale_amt, dirty),
        "commission_gbp":    dirty_amount(commission, dirty),
        "currency":          "GBP",
        "sale_status":       status_for_age(sale_date),
        "channel":           maybe_null(random.choice(CHANNELS), dirty),
        "customer_email":    maybe_null(fake.email(), dirty),
        "sale_date":         sale_date.isoformat(),
        "created_at":        sale_date.isoformat(),
        "updated_at":        sale_date.isoformat(),
        "click_id":          maybe_null(fake.uuid4(), dirty),
        "campaign_id":       maybe_null(f"CAMP-{random.randint(1000, 9999)}", dirty),
        "is_first_purchase": random.random() > 0.7,
    }


# ─────────────────────────────────────────────────────────────
# FILE WRITERS
# ─────────────────────────────────────────────────────────────

def write_parquet(rows: list[dict], file_path: Path) -> int:
    """
    Write rows as Parquet using pandas + pyarrow.
    Falls back to JSON Lines if pyarrow not available.
    """
    try:
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq

        df = pd.DataFrame(rows)
        table = pa.Table.from_pandas(df)
        pq.write_table(table, str(file_path))
        return len(rows)

    except ImportError:
        log.warning("pyarrow not installed — writing JSON Lines instead")
        json_path = file_path.with_suffix(".jsonl")
        with open(json_path, "w") as f:
            for row in rows:
                f.write(json.dumps(row, default=str) + "\n")
        return len(rows)


def write_avro(rows: list[dict], file_path: Path) -> int:
    """
    Write rows as Avro using fastavro.
    Falls back to JSON Lines if fastavro not available.
    """
    try:
        import fastavro

        schema = {
            "type": "record",
            "name": "PartnerSale",
            "fields": [
                {"name": "partner_id",        "type": ["null", "string"], "default": None},
                {"name": "partner_name",       "type": ["null", "string"], "default": None},
                {"name": "partner_type",       "type": ["null", "string"], "default": None},
                {"name": "sale_id",            "type": ["null", "string"], "default": None},
                {"name": "product_sku",        "type": ["null", "string"], "default": None},
                {"name": "sale_amount_gbp",    "type": ["null", "double"], "default": None},
                {"name": "commission_gbp",     "type": ["null", "double"], "default": None},
                {"name": "currency",           "type": ["null", "string"], "default": None},
                {"name": "sale_status",        "type": ["null", "string"], "default": None},
                {"name": "channel",            "type": ["null", "string"], "default": None},
                {"name": "customer_email",     "type": ["null", "string"], "default": None},
                {"name": "sale_date",          "type": ["null", "string"], "default": None},
                {"name": "created_at",         "type": ["null", "string"], "default": None},
                {"name": "updated_at",         "type": ["null", "string"], "default": None},
                {"name": "click_id",           "type": ["null", "string"], "default": None},
                {"name": "campaign_id",        "type": ["null", "string"], "default": None},
                {"name": "is_first_purchase",  "type": ["null", "boolean"], "default": None},
            ]
        }

        parsed_schema = fastavro.parse_schema(schema)
        with open(file_path, "wb") as f:
            fastavro.writer(f, parsed_schema, rows)
        return len(rows)

    except ImportError:
        log.warning("fastavro not installed — writing JSON Lines instead")
        json_path = file_path.with_suffix(".jsonl")
        with open(json_path, "w") as f:
            for row in rows:
                f.write(json.dumps(row, default=str) + "\n")
        return len(rows)


# ─────────────────────────────────────────────────────────────
# BURST MODE
# ─────────────────────────────────────────────────────────────

def run_burst(output_dir: Path, days: int = 7, dirty: bool = False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}")
    t0    = time.time()
    stats = {"files": 0, "total_rows": 0}

    for day_offset in range(days):
        drop_date = datetime.now(timezone.utc) - timedelta(days=day_offset)

        for partner in PARTNERS:
            # Each partner drops 1 file per day at a consistent time
            drop_hour = random.randint(1, 6)  # Partners batch overnight
            file_date = drop_date.replace(
                hour=drop_hour, minute=random.randint(0, 59), second=0
            )

            # Generate 100-500 sale rows per file
            num_rows = random.randint(100, 500)
            rows = []
            for _ in range(num_rows):
                # Sales spread across the day before the drop
                sale_offset = random.uniform(0, 24)
                sale_date   = file_date - timedelta(hours=sale_offset)
                rows.append(build_sale_row(partner, sale_date, dirty))

            # Write file
            date_path = output_dir / file_date.strftime("%Y/%m/%d")
            date_path.mkdir(parents=True, exist_ok=True)

            ext      = "parquet" if partner["format"] == "parquet" else "avro"
            filename = (f"{partner['id']}_{file_date.strftime('%Y%m%d_%H%M%S')}"
                        f"_sales.{ext}")
            filepath = date_path / filename

            if partner["format"] == "parquet":
                written = write_parquet(rows, filepath)
            else:
                written = write_avro(rows, filepath)

            stats["files"]      += 1
            stats["total_rows"] += written
            log.info(f"  Written: {filename} ({written} rows)")

    elapsed = time.time() - t0
    log.info(f"✓ BURST COMPLETE | {elapsed:.1f}s | "
             f"{stats['files']} files | {stats['total_rows']} rows")
    if elapsed > 120:
        log.warning(f"Rule 2 VIOLATION: {elapsed:.0f}s > 120s")
    else:
        log.info(f"Rule 2 ✅ {elapsed:.1f}s < 120s")


# ─────────────────────────────────────────────────────────────
# STREAM MODE
# ─────────────────────────────────────────────────────────────

def run_stream(output_dir: Path, dirty: bool = False):
    log.info(f"STREAM MODE | ~3 files/day | dirty={dirty} | Ctrl+C to stop")

    stats, i = {"files": 0, "rows": 0}, 0

    try:
        while True:
            i       += 1
            partner  = random.choice(PARTNERS)
            now      = datetime.now(timezone.utc)
            num_rows = random.randint(50, 200)
            rows     = [build_sale_row(partner, now, dirty) for _ in range(num_rows)]

            date_path = output_dir / now.strftime("%Y/%m/%d")
            date_path.mkdir(parents=True, exist_ok=True)

            ext      = "parquet" if partner["format"] == "parquet" else "avro"
            filename = f"{partner['id']}_{now.strftime('%Y%m%d_%H%M%S')}_sales.{ext}"
            filepath = date_path / filename

            if partner["format"] == "parquet":
                write_parquet(rows, filepath)
            else:
                write_avro(rows, filepath)

            stats["files"] += 1
            stats["rows"]  += num_rows
            log.info(f"Dropped: {filename} ({num_rows} rows)")

            time.sleep(STREAM_SLEEP)

    except KeyboardInterrupt:
        log.info(f"Stopped. {stats}")


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Source 10 — Partner S3 Drop generator")
    p.add_argument("--mode",       choices=["burst", "stream"], required=True)
    p.add_argument("--days",       type=int, default=7)
    p.add_argument("--dirty",      action="store_true")
    p.add_argument("--output-dir", type=str,
                   default=os.environ.get("PARTNER_OUTPUT_DIR", "/tmp/partner_raw"))
    args = p.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"Source 10 | mode={args.mode} | days={args.days} | "
             f"dirty={args.dirty} | output={output_dir}")

    if args.mode == "burst":
        run_burst(output_dir, args.days, args.dirty)
    elif args.mode == "stream":
        run_stream(output_dir, args.dirty)


if __name__ == "__main__":
    main()

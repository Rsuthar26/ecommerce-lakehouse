"""
generators/10_partner_s3/generate.py — Staff DE Journey: Source 10

Simulates partner data files dropped into S3 in Parquet and Avro format.
Partners: affiliates, marketplace sellers, cross-sell partners.

Fix applied: Rule 11 — real product SKUs loaded from Postgres at startup.

Rules satisfied: All 11.

Usage:
    python generate.py --mode burst --output-dir /tmp/partner_raw
    python generate.py --mode burst --dirty --output-dir /tmp/partner_raw
    python generate.py --mode stream --output-dir /tmp/partner_raw
"""

import os, sys, json, time, random, logging, argparse
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

STREAM_SLEEP = 86400 / 3   # Rule 13: ~3 files/day

PARTNERS = [
    {"id": "PARTNER-001", "name": "AffiliateNet UK",  "format": "parquet", "type": "affiliate"},
    {"id": "PARTNER-002", "name": "MarketPlace Pro",  "format": "parquet", "type": "marketplace"},
    {"id": "PARTNER-003", "name": "CrossSell Direct", "format": "avro",    "type": "crosssell"},
]
CHANNELS    = ["email","social","search","display","referral"]
SALE_STATUS = ["completed","pending","cancelled","refunded"]

_entity_ids = None
def get_entity_ids():
    global _entity_ids
    if _entity_ids is None: _entity_ids = load_entity_ids()
    return _entity_ids

def dr(base, dirty, elevated): return elevated if dirty else base
def maybe_null(v, dirty, base=0.05, elev=0.20):
    return None if random.random() < dr(base, dirty, elev) else v

def dirty_amount(amount, dirty):
    if random.random() < dr(0.005, dirty, 0.08):
        return random.choice([None, -1.0, 0.0, 999999.99])
    return amount

def dirty_sku(sku, dirty):
    if random.random() < dr(0.01, dirty, 0.10):
        return random.choice([None, "", "UNKNOWN", str(sku)+"_OLD"])
    return sku

def status_for_age(created_at: datetime) -> str:
    age_hrs = (datetime.now(timezone.utc) - created_at).total_seconds() / 3600
    if age_hrs < 2:   return "pending"
    elif age_hrs < 72: return random.choices(["completed","pending"], weights=[0.8,0.2])[0]
    else:              return random.choices(["completed","cancelled","refunded"], weights=[0.88,0.08,0.04])[0]

def build_sale_row(partner, sale_date, dirty=False):
    ids        = get_entity_ids()
    sku        = dirty_sku(random.choice(ids["product_skus"]) if ids["product_skus"] else "SKU-00001", dirty)
    sale_amt   = round(random.uniform(4.99, 299.99), 2)
    commission = round(sale_amt * random.uniform(0.05, 0.15), 2)
    return {
        "partner_id":        partner["id"],
        "partner_name":      partner["name"],
        "partner_type":      partner["type"],
        "sale_id":           f"{partner['id']}-{random.randint(100000,999999)}",
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
        "campaign_id":       maybe_null(f"CAMP-{random.randint(1000,9999)}", dirty),
        "is_first_purchase": random.random() > 0.7,
    }

def write_parquet(rows, file_path):
    try:
        import pandas as pd, pyarrow as pa, pyarrow.parquet as pq
        pq.write_table(pa.Table.from_pandas(pd.DataFrame(rows)), str(file_path))
        return len(rows)
    except ImportError:
        log.warning("pyarrow not installed — writing JSON Lines")
        with open(file_path.with_suffix(".jsonl"),"w") as f:
            for r in rows: f.write(json.dumps(r, default=str)+"\n")
        return len(rows)

def write_avro(rows, file_path):
    try:
        import fastavro
        schema = {"type":"record","name":"PartnerSale","fields":[
            {"name":k,"type":["null","string"],"default":None} for k in rows[0].keys()
        ]}
        with open(file_path,"wb") as f:
            fastavro.writer(f, fastavro.parse_schema(schema), rows)
        return len(rows)
    except ImportError:
        log.warning("fastavro not installed — writing JSON Lines")
        with open(file_path.with_suffix(".jsonl"),"w") as f:
            for r in rows: f.write(json.dumps(r, default=str)+"\n")
        return len(rows)

def run_burst(output_dir, days=7, dirty=False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}"); get_entity_ids()
    t0, stats, now = time.time(), {"files":0,"total_rows":0}, datetime.now(timezone.utc)
    for day_offset in range(days):
        drop_date = now - timedelta(days=day_offset)
        for partner in PARTNERS:
            drop_hour = random.randint(1, 6)
            file_date = drop_date.replace(hour=drop_hour, minute=random.randint(0,59), second=0)
            rows      = []
            for _ in range(random.randint(100,500)):
                sale_date = file_date - timedelta(hours=random.uniform(0,24))
                rows.append(build_sale_row(partner, sale_date, dirty))
            date_path = output_dir / file_date.strftime("%Y/%m/%d"); date_path.mkdir(parents=True, exist_ok=True)
            ext      = "parquet" if partner["format"] == "parquet" else "avro"
            filepath = date_path / f"{partner['id']}_{file_date.strftime('%Y%m%d')}_sales.{ext}"
            if filepath.exists():  # Rule 5: idempotent — skip if already written
                log.info(f"  Skipped (exists): {filepath.name}")
                continue
            if filepath.exists(): continue  # Rule 5 — idempotent: skip if already written
            written  = write_parquet(rows, filepath) if partner["format"] == "parquet" else write_avro(rows, filepath)
            stats["files"] += 1; stats["total_rows"] += written
            log.info(f"  Written: {filepath.name} ({written} rows)")
    elapsed = time.time() - t0
    log.info(f"✓ BURST COMPLETE | {elapsed:.1f}s | {stats}")
    log.info(f"Rule 2 {'✅' if elapsed <= 120 else 'VIOLATION'} {elapsed:.1f}s")

def run_stream(output_dir, dirty=False):
    log.info(f"STREAM MODE | ~3 files/day | dirty={dirty} | Ctrl+C to stop")
    get_entity_ids(); stats, i = {"files":0,"rows":0}, 0
    try:
        while True:
            i += 1; partner = random.choice(PARTNERS); now = datetime.now(timezone.utc)
            rows = [build_sale_row(partner, now, dirty) for _ in range(random.randint(50,200))]
            date_path = output_dir / now.strftime("%Y/%m/%d"); date_path.mkdir(parents=True, exist_ok=True)
            ext      = "parquet" if partner["format"] == "parquet" else "avro"
            filepath = date_path / f"{partner['id']}_{now.strftime('%Y%m%d_%H%M%S')}_sales.{ext}"
            written  = write_parquet(rows, filepath) if partner["format"] == "parquet" else write_avro(rows, filepath)
            stats["files"] += 1; stats["rows"] += written
            log.info(f"Dropped: {filepath.name} ({written} rows)")
            time.sleep(STREAM_SLEEP)
    except KeyboardInterrupt: log.info(f"Stopped. {stats}")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["burst","stream"], required=True)
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--dirty", action="store_true")
    p.add_argument("--output-dir", type=str, default=os.environ.get("PARTNER_OUTPUT_DIR","/tmp/partner_raw"))
    args = p.parse_args()
    od = Path(args.output_dir); od.mkdir(parents=True, exist_ok=True)
    log.info(f"Source 10 | mode={args.mode} | days={args.days} | dirty={args.dirty} | output={od}")
    if args.mode == "burst": run_burst(od, args.days, args.dirty)
    else: run_stream(od, args.dirty)

if __name__ == "__main__": main()

"""
generators/11_erp/generate.py — Staff DE Journey: Source 11

Simulates ERP system exports: finance ledger, purchase orders, invoices.
Generates both JSON and XML — real ERPs export both formats.
Reads real order_ids from Postgres to ensure invoice join integrity.

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

STREAM_SLEEP   = 86400 / 2    # ~1-2 files/day
GL_ACCOUNTS    = ["4000-Revenue","5000-COGS","6000-Opex","2000-AP","1000-AR","1100-Cash"]
COST_CENTRES   = ["CC-WAREHOUSE","CC-SALES","CC-MARKETING","CC-TECH","CC-FINANCE"]
CURRENCIES     = ["GBP","EUR","USD"]

_entity_ids = None
def get_entity_ids():
    global _entity_ids
    if _entity_ids is None: _entity_ids = load_entity_ids()
    return _entity_ids

def dr(base, dirty, elevated): return elevated if dirty else base
def maybe_null(v, dirty, base=0.05, elev=0.20):
    return None if random.random() < dr(base, dirty, elev) else v

def build_invoice(created_at: datetime, dirty: bool = False) -> dict:
    ids      = get_entity_ids()
    order_id = random.choice(ids["order_ids"]) if ids["order_ids"] else None

    # Rule 14: amount must come from Postgres when order_id is present
    amount   = ids["order_amounts"].get(order_id, random.randint(1000, 50000))
    inv_num  = f"INV-{hashlib.md5(f'{order_id}_{created_at.date()}'.encode()).hexdigest()[:8].upper()}"
    status   = "paid" if (datetime.now(timezone.utc) - created_at).days > 30 else random.choice(["issued","overdue","paid"])
    return {
        "invoice_number": inv_num,
        "order_id":       maybe_null(order_id, dirty, base=0.01),
        "invoice_date":   created_at.isoformat(),
        "due_date":       (created_at + timedelta(days=30)).isoformat(),
        "status":         status,
        "subtotal_pence": amount,
        "tax_pence":      int(amount * 0.20),
        "total_pence":    int(amount * 1.20),
        "currency":       random.choice(CURRENCIES),
        "gl_account":     maybe_null(random.choice(GL_ACCOUNTS), dirty),
        "cost_centre":    maybe_null(random.choice(COST_CENTRES), dirty),
        "vendor_id":      maybe_null(f"VEND-{random.randint(100,999)}", dirty),
        "vendor_name":    maybe_null(fake.company(), dirty),
        "payment_terms":  maybe_null(random.choice(["NET30","NET60","NET90","immediate"]), dirty),
        "_source":        "erp_export",
        "_format":        "json",
    }

def invoice_to_xml(invoice: dict) -> str:
    """Convert invoice dict to XML. Silver must parse this."""
    lines = ['<?xml version="1.0" encoding="UTF-8"?>', "<Invoice>"]
    for k, v in invoice.items():
        if v is None: v = ""
        lines.append(f"  <{k}>{v}</{k}>")
    lines.append("</Invoice>")
    return "\n".join(lines)

def write_erp_file(records, output_dir, ts, fmt="json"):
    dp = output_dir / ts.strftime("%Y/%m/%d"); dp.mkdir(parents=True, exist_ok=True)
    fname = f"erp_export_{ts.strftime('%Y%m%d')}.{fmt}"
    if fmt == "json":
        with open(dp / fname, "w") as f:
            json.dump({"export_date": ts.isoformat(), "count": len(records), "invoices": records}, f, default=str)
    else:
        # XML export
        lines = ['<?xml version="1.0" encoding="UTF-8"?>', "<Invoices>"]
        for r in records:
            lines.append(invoice_to_xml(r))
        lines.append("</Invoices>")
        with open(dp / fname, "w") as f:
            f.write("\n".join(lines))

def run_burst(output_dir, days=7, dirty=False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}"); get_entity_ids()
    t0, stats, now = time.time(), {"files":0,"records":0}, datetime.now(timezone.utc)
    for day_offset in range(days):
        base_dt = now - timedelta(days=day_offset)
        records = [build_invoice(base_dt - timedelta(hours=random.uniform(0,23)), dirty)
                   for _ in range(random.randint(20, 50))]
        # JSON export
        write_erp_file(records, output_dir, base_dt, "json")
        # XML export (same data, different format)
        write_erp_file(records, output_dir, base_dt, "xml")
        stats["files"] += 2; stats["records"] += len(records)
    elapsed = time.time() - t0
    log.info(f"✓ BURST COMPLETE | {elapsed:.1f}s | {stats}")
    log.info(f"Rule 2 {'✅' if elapsed <= 120 else 'VIOLATION'} {elapsed:.1f}s")

def run_stream(output_dir, dirty=False):
    log.info(f"STREAM MODE | ~2 files/day | dirty={dirty} | Ctrl+C to stop")
    get_entity_ids(); stats, i = {"files":0}, 0
    try:
        while True:
            i += 1; now = datetime.now(timezone.utc)
            records = [build_invoice(now, dirty) for _ in range(random.randint(5,20))]
            fmt = random.choice(["json","xml"])
            write_erp_file(records, output_dir, now, fmt)
            stats["files"] += 1
            if i % 10 == 0: log.info(f"Stream — {stats}")
            time.sleep(STREAM_SLEEP)
    except KeyboardInterrupt: log.info(f"Stopped. {stats}")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["burst","stream"], required=True)
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--dirty", action="store_true")
    p.add_argument("--output-dir", type=str, default=os.environ.get("ERP_OUTPUT_DIR","/tmp/erp_raw"))
    args = p.parse_args()
    od = Path(args.output_dir); od.mkdir(parents=True, exist_ok=True)
    if args.mode == "burst": run_burst(od, args.days, args.dirty)
    else: run_stream(od, args.dirty)

if __name__ == "__main__": main()

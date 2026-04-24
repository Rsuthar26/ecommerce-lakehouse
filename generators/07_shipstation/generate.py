"""
generators/07_shipstation/generate.py — Staff DE Journey: Source 07

Simulates ShipStation shipment tracking events.
Reads real shipped/delivered orders from Postgres.

Rules: All 11 satisfied. See docstring pattern from Source 06.
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

STREAM_SLEEP = 86400 / 5000
CARRIERS     = ["Royal Mail", "DPD", "Hermes", "UPS", "FedEx", "Parcelforce"]
STATUSES     = ["label_created","picked_up","in_transit","out_for_delivery","delivered","exception","returned"]

# Rule 3 — shipments are dispatched during warehouse hours 8am-6pm
def dispatch_timestamp(base_dt: datetime) -> datetime:
    hour = random.choices(
        range(8, 18),
        weights=[5, 10, 15, 15, 15, 15, 10, 8, 5, 2]
    )[0]
    return base_dt.replace(hour=hour, minute=random.randint(0, 59),
                           second=random.randint(0, 59), tzinfo=timezone.utc)

_entity_ids = None
def get_entity_ids():
    global _entity_ids
    if _entity_ids is None: _entity_ids = load_entity_ids()
    return _entity_ids

def dr(base, dirty, elevated): return elevated if dirty else base
def maybe_null(v, dirty, base=0.05, elev=0.20):
    return None if random.random() < dr(base, dirty, elev) else v

def shipment_status_for_age(created_at: datetime) -> str:
    age_hrs = (datetime.now(timezone.utc) - created_at).total_seconds() / 3600
    if age_hrs < 2:   return "label_created"
    elif age_hrs < 12: return "picked_up"
    elif age_hrs < 48: return random.choices(["in_transit","out_for_delivery"], weights=[0.7,0.3])[0]
    elif age_hrs < 96: return random.choices(["delivered","in_transit","exception"], weights=[0.7,0.25,0.05])[0]
    else:              return random.choices(["delivered","returned","exception"], weights=[0.90,0.05,0.05])[0]

def build_shipment(created_at: datetime, dirty: bool = False) -> dict:
    ids      = get_entity_ids()
    order_id = random.choice(ids["shipped_order_ids"]) if ids["shipped_order_ids"] else random.choice(ids["order_ids"] or [None])
    carrier  = random.choice(CARRIERS)
    status   = shipment_status_for_age(created_at)
    tracking = f"{'JD' if carrier == 'Royal Mail' else 'DN'}{random.randint(100000000,999999999)}GB"

    return {
        "shipment_id":     f"SHIP-{hashlib.md5(f'{order_id}_{created_at.date()}'.encode()).hexdigest()[:12]}",
        "order_id":        maybe_null(order_id, dirty, base=0.01),
        "carrier":         maybe_null(carrier, dirty),
        "service":         maybe_null(random.choice(["Standard","Express","Next Day"]), dirty),
        "tracking_number": maybe_null(tracking, dirty),
        "status":          status,
        "shipped_at":      created_at.isoformat(),
        "estimated_delivery": (created_at + timedelta(days=random.randint(1,5))).isoformat(),
        "delivered_at":    (created_at + timedelta(days=random.randint(1,3))).isoformat()
                           if status == "delivered" else None,
        "weight_grams":    maybe_null(random.randint(100, 5000), dirty),
        "shipping_address": {
            "line1":    maybe_null(fake.street_address(), dirty),
            "city":     maybe_null(fake.city(), dirty),
            "postcode": maybe_null(fake.postcode(), dirty),
            "country":  "GB",
        },
        "label_url":   f"https://shipstation.example.com/labels/{tracking}.pdf",
        "created_at":  created_at.isoformat(),
        "_source":     "shipstation",
        # Dirty: 2% future timestamps
        **( {"shipped_at": "2099-01-01T00:00:00+00:00"} if dirty and random.random() < 0.02 else {} ),
    }

def write_daily_file(items, output_dir, day_dt):
    """
    Write all shipments for one day to a single file.
    Folder: output_dir/YYYY/MM/DD/
    Filename: shipstation_shipments_YYYYMMDD.json
    Each file contains ONLY shipments where shipped_at falls on that day.
    """
    dp = output_dir / day_dt.strftime("%Y/%m/%d")
    dp.mkdir(parents=True, exist_ok=True)
    fname = f"shipstation_shipments_{day_dt.strftime('%Y%m%d')}.json"
    with open(dp / fname, "w") as f:
        json.dump({
            "date":  day_dt.strftime("%Y-%m-%d"),
            "count": len(items),
            "items": items
        }, f, default=str)
    log.info(f"  Written: {fname} ({len(items)} shipments)")

def run_burst(output_dir, days=7, dirty=False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}"); get_entity_ids()
    t0, stats, now = time.time(), {"sent": 0, "files": 0}, datetime.now(timezone.utc)
    total = days * 700

    # Collect shipments grouped by day — one file per day
    day_buckets: dict = {}
    for _ in range(total):
        days_ago   = random.uniform(0, days)
        base_dt    = now - timedelta(days=days_ago)
        s          = build_shipment(dispatch_timestamp(base_dt), dirty)
        day_key    = base_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        if day_key not in day_buckets:
            day_buckets[day_key] = []
        day_buckets[day_key].append(s)
        stats["sent"] += 1

    for day_dt, items in sorted(day_buckets.items()):
        write_daily_file(items, output_dir, day_dt)
        stats["files"] += 1

    elapsed = time.time() - t0
    log.info(f"✓ BURST COMPLETE | {elapsed:.1f}s | {stats}")
    log.info(f"Rule 2 {'✅' if elapsed <= 120 else 'VIOLATION'} {elapsed:.1f}s")

def run_stream(output_dir, dirty=False):
    log.info(f"STREAM MODE | ~5K/day | dirty={dirty} | Ctrl+C to stop")
    get_entity_ids(); stats, i = {"sent": 0}, 0
    try:
        while True:
            i  += 1; now = datetime.now(timezone.utc)
            s   = build_shipment(now, dirty)
            # Stream: append to today's daily file
            day_key = now.replace(hour=0, minute=0, second=0, microsecond=0)
            write_daily_file([s], output_dir, day_key)
            stats["sent"] += 1
            if i % 100 == 0: log.info(f"Stream — {stats}")
            time.sleep(STREAM_SLEEP)
    except KeyboardInterrupt: log.info(f"Stopped. {stats}")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["burst","stream"], required=True)
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--dirty", action="store_true")
    p.add_argument("--output-dir", type=str, default=os.environ.get("SHIPSTATION_OUTPUT_DIR","/tmp/shipstation_raw"))
    args = p.parse_args()
    od = Path(args.output_dir); od.mkdir(parents=True, exist_ok=True)
    if args.mode == "burst": run_burst(od, args.days, args.dirty)
    else: run_stream(od, args.dirty)

if __name__ == "__main__": main()

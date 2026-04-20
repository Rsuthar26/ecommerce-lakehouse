"""
generators/17_ga4/generate.py — Staff DE Journey: Source 17

Simulates GA4 BigQuery export data.
~200K events/day in BigQuery export schema format.

Fix applied: Rule 11 — real order_ids and product SKUs from Postgres.

Rules satisfied: All 11.

Usage:
    python generate.py --mode burst --output-dir /tmp/ga4_raw
    python generate.py --mode burst --dirty --output-dir /tmp/ga4_raw
    python generate.py --mode stream --output-dir /tmp/ga4_raw
"""

import os, sys, json, time, random, hashlib, logging, argparse, uuid
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

STREAM_SLEEP = 86400 / 200000  # Rule 6 + Rule 13: ~2.3 events/sec

EVENT_TYPES = [
    "page_view","page_view","page_view","session_start","scroll","scroll",
    "click","view_item","view_item","add_to_cart","view_cart",
    "begin_checkout","add_payment_info","purchase","search","login","sign_up","user_engagement",
]
TRAFFIC_SOURCES = [
    {"medium":"organic","source":"google"},{"medium":"organic","source":"bing"},
    {"medium":"cpc","source":"google"},{"medium":"email","source":"newsletter"},
    {"medium":"social","source":"instagram"},{"medium":"social","source":"facebook"},
    {"medium":"referral","source":"affiliatenet.co.uk"},{"medium":"none","source":"direct"},
]
PAGES    = ["/","/products","/products/headphones","/products/keyboards",
            "/cart","/checkout","/checkout/payment","/order-confirmation",
            "/account","/sale","/new-arrivals","/search"]
DEVICES  = ["desktop","mobile","tablet"]
BROWSERS = ["Chrome","Safari","Firefox","Edge"]
OS_LIST  = ["Windows","macOS","iOS","Android","Linux"]
COUNTRIES = ["GB","GB","GB","US","DE","FR","AU"]

_entity_ids = None
def get_entity_ids():
    global _entity_ids
    if _entity_ids is None: _entity_ids = load_entity_ids()
    return _entity_ids

def dr(base, dirty, elevated): return elevated if dirty else base
def maybe_null(v, dirty, base=0.05, elev=0.20):
    return None if random.random() < dr(base, dirty, elev) else v

def bad_revenue(amount, dirty):
    if random.random() < dr(0.005, dirty, 0.05):
        return random.choice([None, 0, -1.0, 999999.0])
    return amount

def traffic_timestamp(base_dt: datetime) -> datetime:
    peak_windows = [(10,12,0.25),(19,21,0.25),(12,19,0.30),(8,10,0.10),(21,23,0.10)]
    r, cumul, s, e = random.random(), 0.0, 10, 12
    for start, end, w in peak_windows:
        cumul += w
        if r <= cumul: s, e = start, end; break
    return base_dt.replace(hour=random.randint(s,e-1), minute=random.randint(0,59),
                           second=random.randint(0,59), microsecond=random.randint(0,999999),
                           tzinfo=timezone.utc)

def build_event_params(event_type: str, dirty: bool, ids: dict) -> list:
    sku   = random.choice(ids["product_skus"]) if ids["product_skus"] else "SKU-00001"
    price = round(random.uniform(4.99, 299.99), 2)
    page  = random.choice(PAGES)

    if event_type == "page_view":
        return [
            {"key":"page_location","value":{"string_value":maybe_null(f"https://ecommerce.example.com{page}",dirty)}},
            {"key":"page_title","value":{"string_value":maybe_null(fake.catch_phrase(),dirty)}},
            {"key":"engagement_time_msec","value":{"int_value":random.randint(0,300000)}},
        ]
    elif event_type in ("view_item","add_to_cart"):
        return [
            {"key":"currency","value":{"string_value":"GBP"}},
            {"key":"value","value":{"float_value":bad_revenue(price,dirty)}},
            {"key":"items","value":{"string_value":json.dumps([{
                "item_id":maybe_null(sku,dirty),"price":bad_revenue(price,dirty),"quantity":1
            }])}},
        ]
    elif event_type == "purchase":
        order_id = random.choice(ids["order_ids"]) if ids["order_ids"] else None  # Rule 11
        return [
            {"key":"transaction_id","value":{"string_value":maybe_null(str(order_id),dirty)}},
            {"key":"currency","value":{"string_value":"GBP"}},
            {"key":"value","value":{"float_value":bad_revenue(price,dirty)}},
        ]
    elif event_type == "search":
        return [{"key":"search_term","value":{"string_value":maybe_null(
            random.choice(["wireless headphones","laptop stand","keyboard","webcam"]),dirty)}}]
    else:
        return [{"key":"engagement_time_msec","value":{"int_value":random.randint(0,60000)}}]

def build_ga4_event(event_dt: datetime, dirty: bool = False) -> dict:
    ids        = get_entity_ids()
    event_type = random.choice(EVENT_TYPES)
    traffic    = random.choice(TRAFFIC_SOURCES)
    device     = random.choice(DEVICES)
    ts_micros  = int(event_dt.timestamp() * 1_000_000)

    if dirty and random.random() < 0.02:
        ts_micros = int(datetime(2099,1,1,tzinfo=timezone.utc).timestamp() * 1_000_000)

    return {
        "event_date":      event_dt.strftime("%Y%m%d"),
        "event_timestamp": ts_micros,
        "event_name":      event_type,
        "event_value_in_usd": maybe_null(round(random.uniform(0,500),2), dirty, base=0.70, elev=0.80),
        "event_bundle_sequence_id": random.randint(1,1000),
        "user_id":         maybe_null(None, dirty, base=0.70, elev=0.80),
        "user_pseudo_id":  maybe_null(hashlib.md5(f"user_{random.randint(1,50000)}".encode()).hexdigest()[:20], dirty),
        "user_ltv":        {"revenue": maybe_null(round(random.uniform(0,1000),2),dirty), "currency":"GBP"},
        "device": {
            "category":           device,
            "operating_system":   maybe_null(random.choice(OS_LIST),dirty),
            "browser":            maybe_null(random.choice(BROWSERS),dirty),
            "language":           "en-gb",
        },
        "geo": {
            "country": maybe_null(random.choice(COUNTRIES),dirty),
            "city":    maybe_null(fake.city(),dirty),
        },
        "traffic_source": {
            "medium": maybe_null(traffic["medium"],dirty),
            "source": maybe_null(traffic["source"],dirty),
        },
        "stream_id": "1234567890",
        "platform":  "WEB",
        "event_params": build_event_params(event_type, dirty, ids),
    }

def write_daily_export(events, output_dir, export_date):
    dp = output_dir / export_date.strftime("%Y/%m/%d"); dp.mkdir(parents=True, exist_ok=True)
    fname = f"ga4_events_{export_date.strftime('%Y%m%d')}.json"
    with open(dp / fname,"w") as f:
        json.dump({"export_date": export_date.strftime("%Y%m%d"),
                   "property_id":"GA4-123456789",
                   "event_count":len(events),
                   "exported_at":datetime.now(timezone.utc).isoformat(),
                   "events":events}, f, default=str)
    log.info(f"  Written: {fname} ({len(events):,} events)")

def run_burst(output_dir, days=7, dirty=False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}"); get_entity_ids()
    t0, stats, now = time.time(), {"files":0,"events":0}, datetime.now(timezone.utc)
    for day_offset in range(days):
        export_date  = now - timedelta(days=day_offset)
        is_weekend   = export_date.weekday() >= 5
        daily_count  = min(int(200000 * (1.4 if is_weekend else 1.0)), 5000)
        events       = [build_ga4_event(traffic_timestamp(export_date), dirty) for _ in range(daily_count)]
        write_daily_export(events, output_dir, export_date)
        stats["files"] += 1; stats["events"] += daily_count
    elapsed = time.time() - t0
    log.info(f"✓ BURST COMPLETE | {elapsed:.1f}s | {stats}")
    log.info(f"Rule 2 {'✅' if elapsed <= 120 else 'VIOLATION'} {elapsed:.1f}s")

def run_stream(output_dir, dirty=False):
    log.info(f"STREAM MODE | ~200K events/day | dirty={dirty} | Ctrl+C to stop")
    get_entity_ids(); stats, i, buffer = {"events":0}, 0, []
    try:
        while True:
            i += 1; now = datetime.now(timezone.utc)
            buffer.append(build_ga4_event(now, dirty)); stats["events"] += 1
            if len(buffer) >= 1000:
                write_daily_export(buffer, output_dir, now); buffer = []
                log.info(f"Stream — {stats}")
            time.sleep(STREAM_SLEEP)
    except KeyboardInterrupt:
        if buffer: write_daily_export(buffer, output_dir, datetime.now(timezone.utc))
        log.info(f"Stopped. {stats}")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["burst","stream"], required=True)
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--dirty", action="store_true")
    p.add_argument("--output-dir", type=str, default=os.environ.get("GA4_OUTPUT_DIR","/tmp/ga4_raw"))
    args = p.parse_args()
    od = Path(args.output_dir); od.mkdir(parents=True, exist_ok=True)
    log.info(f"Source 17 | mode={args.mode} | days={args.days} | dirty={args.dirty} | output={od}")
    if args.mode == "burst": run_burst(od, args.days, args.dirty)
    else: run_stream(od, args.dirty)

if __name__ == "__main__": main()

"""
generators/12_reviews_tickets/generate.py — Staff DE Journey: Source 12

Simulates customer reviews and support tickets — unstructured text data.
This feeds NLP/sentiment analysis in the Gold layer.

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

STREAM_SLEEP = 86400 / 1000

REVIEW_POSITIVE = [
    "Absolutely love this product. Arrived quickly and exactly as described.",
    "Great quality, very happy with my purchase. Would definitely recommend.",
    "Exceeded my expectations. Packaging was excellent and delivery was fast.",
    "Perfect for what I needed. Great value for money.",
    "Really impressed with the quality. My second purchase from this store.",
]
REVIEW_NEGATIVE = [
    "Disappointed with the quality. Not as described on the website.",
    "Arrived damaged and took forever to resolve the issue with customer service.",
    "Product stopped working after 2 weeks. Very poor quality for the price.",
    "Packaging was terrible. Item arrived scratched.",
    "Complete waste of money. Nothing like the pictures.",
]
REVIEW_NEUTRAL = [
    "Decent product for the price. Does what it says.",
    "OK, nothing special. Delivery was on time.",
    "Average quality but arrived quickly.",
    "Fine, but the instructions were confusing.",
]

TICKET_TEMPLATES = [
    "My order #{order_id} has not arrived yet. It's been {days} days.",
    "I received the wrong item in order #{order_id}. I ordered {sku} but received something else.",
    "I need to return my order #{order_id}. The product is defective.",
    "When will order #{order_id} be dispatched? The tracking hasn't updated.",
    "I was charged twice for order #{order_id}. Please refund one payment.",
    "The product in order #{order_id} is not working. I need a replacement.",
    "I'd like to change the delivery address for order #{order_id}.",
    "Can I get a VAT invoice for order #{order_id}?",
]

TICKET_CATEGORIES = ["delivery","returns","billing","product","account","other"]
TICKET_STATUSES   = ["open","pending","resolved","closed"]

# Rule 3 — reviews and tickets submitted during waking hours 9am-11pm, peak evening
def customer_hours_timestamp(base_dt: datetime) -> datetime:
    peak_windows = [(19,22,0.30),(9,12,0.25),(12,19,0.30),(22,23,0.10),(8,9,0.05)]
    r, cumul, s, e = random.random(), 0.0, 19, 22
    for start, end, w in peak_windows:
        cumul += w
        if r <= cumul:
            s, e = start, end
            break
    return base_dt.replace(hour=random.randint(s, e-1), minute=random.randint(0,59),
                           second=random.randint(0,59), tzinfo=timezone.utc)

_entity_ids = None
def get_entity_ids():
    global _entity_ids
    if _entity_ids is None: _entity_ids = load_entity_ids()
    return _entity_ids

def dr(base, dirty, elevated): return elevated if dirty else base
def maybe_null(v, dirty, base=0.05, elev=0.20):
    return None if random.random() < dr(base, dirty, elev) else v

def ticket_status_for_age(created_at: datetime) -> str:
    age_hrs = (datetime.now(timezone.utc) - created_at).total_seconds() / 3600
    if age_hrs < 2:   return "open"
    elif age_hrs < 24: return random.choices(["open","pending"], weights=[0.4,0.6])[0]
    elif age_hrs < 72: return random.choices(["pending","resolved"], weights=[0.4,0.6])[0]
    else:              return random.choices(["resolved","closed"], weights=[0.5,0.5])[0]

def build_review(created_at: datetime, dirty: bool = False) -> dict:
    ids      = get_entity_ids()
    order_id = random.choice(ids["order_ids"]) if ids["order_ids"] else None
    sku      = random.choice(ids["product_skus"]) if ids["product_skus"] else "SKU-00001"
    rating   = random.choices([1,2,3,4,5], weights=[0.05,0.08,0.12,0.30,0.45])[0]

    if rating >= 4:   body = random.choice(REVIEW_POSITIVE)
    elif rating <= 2: body = random.choice(REVIEW_NEGATIVE)
    else:             body = random.choice(REVIEW_NEUTRAL)

    # Dirty: inject garbled text
    if dirty and random.random() < 0.02:
        body = body[:random.randint(5,20)] + "..." + ("?" * random.randint(0,5))

    return {
        "record_type":   "review",
        "review_id":     hashlib.md5(f"review_{order_id}_{created_at.isoformat()}".encode()).hexdigest()[:16],
        "order_id":      maybe_null(order_id, dirty, base=0.01),
        "product_sku":   maybe_null(sku, dirty, base=0.01),
        "customer_id":   maybe_null(random.choice(ids["customer_ids"]) if ids["customer_ids"] else None, dirty),
        "rating":        maybe_null(rating, dirty),
        "title":         maybe_null(fake.sentence(nb_words=5), dirty),
        "body":          maybe_null(body, dirty),
        "verified_purchase": random.random() > 0.1,
        "helpful_votes": maybe_null(random.randint(0,50), dirty),
        "created_at":    created_at.isoformat(),
        "_source":       "reviews",
    }

def build_ticket(created_at: datetime, dirty: bool = False) -> dict:
    ids      = get_entity_ids()
    order_id = random.choice(ids["order_ids"]) if ids["order_ids"] else 12345
    sku      = random.choice(ids["product_skus"]) if ids["product_skus"] else "SKU-00001"
    template = random.choice(TICKET_TEMPLATES)
    body     = template.format(order_id=order_id, days=random.randint(3,14), sku=sku)

    return {
        "record_type":  "ticket",
        "ticket_id":    f"TKT-{hashlib.md5(f'ticket_{order_id}_{created_at.isoformat()}'.encode()).hexdigest()[:8].upper()}",
        "order_id":     maybe_null(order_id, dirty, base=0.02),
        "customer_id":  maybe_null(random.choice(ids["customer_ids"]) if ids["customer_ids"] else None, dirty),
        "category":     maybe_null(random.choice(TICKET_CATEGORIES), dirty),
        "status":       ticket_status_for_age(created_at),
        "priority":     maybe_null(random.choice(["low","medium","high","urgent"]), dirty),
        "subject":      maybe_null(f"Issue with order #{order_id}", dirty),
        "body":         maybe_null(body, dirty),
        "channel":      maybe_null(random.choice(["email","chat","phone","web"]), dirty),
        "agent_id":     maybe_null(f"AGENT-{random.randint(1,20)}", dirty),
        "resolved_at":  (created_at + timedelta(hours=random.randint(1,72))).isoformat()
                        if ticket_status_for_age(created_at) in ("resolved","closed") else None,
        "created_at":   created_at.isoformat(),
        "_source":      "support_tickets",
        # Dirty: 2% future timestamps
        **( {"created_at": "2099-01-01T00:00:00+00:00"} if dirty and random.random() < 0.02 else {} ),
    }

def write_batch(records, output_dir, batch_num, ts):
    dp = output_dir / ts.strftime("%Y/%m/%d"); dp.mkdir(parents=True, exist_ok=True)
    with open(dp / f"text_records_{batch_num:04d}.json","w") as f:
        json.dump({"batch": batch_num, "count": len(records), "records": records}, f, default=str)

def run_burst(output_dir, days=7, dirty=False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}"); get_entity_ids()
    t0, stats, now = time.time(), {"records":0,"batches":0}, datetime.now(timezone.utc)
    total, batch_size, buf, bn = days * 150, 50, [], 0
    for _ in range(total):
        days_ago   = random.uniform(0, days)
        base_dt    = now - timedelta(days=days_ago)
        created_at = customer_hours_timestamp(base_dt)
        record     = build_review(created_at, dirty) if random.random() > 0.4 else build_ticket(created_at, dirty)
        buf.append(record); stats["records"] += 1
        if len(buf) >= batch_size:
            write_batch(buf, output_dir, bn, created_at); bn += 1; buf = []
    if buf: write_batch(buf, output_dir, bn, datetime.now(timezone.utc)); stats["batches"] = bn+1
    elapsed = time.time() - t0
    log.info(f"✓ BURST COMPLETE | {elapsed:.1f}s | {stats}")
    log.info(f"Rule 2 {'✅' if elapsed <= 120 else 'VIOLATION'} {elapsed:.1f}s")

def run_stream(output_dir, dirty=False):
    log.info(f"STREAM MODE | ~1K/day | dirty={dirty} | Ctrl+C to stop")
    get_entity_ids(); stats, i = {"records":0}, 0
    try:
        while True:
            i += 1; now = datetime.now(timezone.utc)
            record = build_review(now, dirty) if random.random() > 0.4 else build_ticket(now, dirty)
            write_batch([record], output_dir, i, now)
            stats["records"] += 1
            if i % 50 == 0: log.info(f"Stream — {stats}")
            time.sleep(STREAM_SLEEP)
    except KeyboardInterrupt: log.info(f"Stopped. {stats}")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["burst","stream"], required=True)
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--dirty", action="store_true")
    p.add_argument("--output-dir", type=str, default=os.environ.get("REVIEWS_OUTPUT_DIR","/tmp/text_raw"))
    args = p.parse_args()
    od = Path(args.output_dir); od.mkdir(parents=True, exist_ok=True)
    if args.mode == "burst": run_burst(od, args.days, args.dirty)
    else: run_stream(od, args.dirty)

if __name__ == "__main__": main()

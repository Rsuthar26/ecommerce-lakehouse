"""
generators/18_ses_email/generate.py — Staff DE Journey: Source 18

Simulates AWS SES email events.
~5K emails/day: order confirmations, shipping notifications, marketing.

Fix applied: Rule 11 — real order_ids and customer_ids from Postgres.

Rules satisfied: All 11.

Usage:
    python generate.py --mode burst --output-dir /tmp/email_raw
    python generate.py --mode burst --dirty --output-dir /tmp/email_raw
    python generate.py --mode stream --output-dir /tmp/email_raw
"""

import os, sys, json, time, random, logging, argparse, uuid, hashlib
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

STREAM_SLEEP = 86400 / 5000   # Rule 6 + Rule 13

EMAIL_TYPES = [
    "order_confirmation","order_confirmation","shipping_notification","shipping_notification",
    "delivery_confirmation","refund_confirmation","marketing_weekly","marketing_promo",
    "password_reset","review_request",
]
BOUNCE_TYPES    = ["Permanent","Transient"]
COMPLAINT_TYPES = ["abuse","auth-failure","fraud","not-spam","other"]
SES_REGIONS     = ["eu-west-1","eu-north-1"]

_entity_ids = None
def get_entity_ids():
    global _entity_ids
    if _entity_ids is None: _entity_ids = load_entity_ids()
    return _entity_ids

def dr(base, dirty, elevated): return elevated if dirty else base
def maybe_null(v, dirty, base=0.05, elev=0.20):
    return None if random.random() < dr(base, dirty, elev) else v

def bad_email_address(email, dirty):
    if random.random() < dr(0.02, dirty, 0.10):
        return random.choice(["invalid-email","@nodomain.com","user@","",None,"user@domain"])
    return email

def delivery_status_for_age(sent_at: datetime) -> str:
    age_mins = (datetime.now(timezone.utc) - sent_at).total_seconds() / 60
    if age_mins < 1:   return "pending"
    elif age_mins < 5: return random.choices(["pending","delivered"], weights=[0.2,0.8])[0]
    else:              return random.choices(["delivered","bounced","complained"], weights=[0.95,0.04,0.01])[0]

def build_ses_event(sent_at: datetime, dirty: bool = False) -> dict:
    ids         = get_entity_ids()
    email_type  = random.choice(EMAIL_TYPES)
    order_id    = random.choice(ids["order_ids"]) if ids["order_ids"] else None

    # Cross-field integrity fix: customer_id must come from the same order row
    # For transactional emails (order present) — look up customer from that order
    # For marketing/password emails (no order) — pick independently from customer list
    is_order_email = email_type not in ("marketing_weekly", "marketing_promo", "password_reset")
    if is_order_email and order_id is not None:
        customer_id = ids["order_customers"].get(order_id)   # always consistent with order_id
    else:
        customer_id = random.choice(ids["customer_ids"]) if ids["customer_ids"] else None
    recipient   = bad_email_address(fake.email(), dirty)
    message_id  = f"<{uuid.uuid4().hex}@eu-west-1.amazonses.com>"
    status      = delivery_status_for_age(sent_at)

    subjects = {
        "order_confirmation":    f"Order Confirmation #{order_id} - Thank you!",
        "shipping_notification": f"Your order #{order_id} has been shipped",
        "delivery_confirmation": f"Your order #{order_id} has been delivered",
        "refund_confirmation":   f"Refund processed for order #{order_id}",
        "marketing_weekly":      "This week's top picks just for you",
        "marketing_promo":       "Flash sale — up to 30% off this weekend",
        "password_reset":        "Reset your password",
        "review_request":        f"How was your recent order #{order_id}?",
    }

    ts = sent_at.isoformat()
    if dirty and random.random() < 0.02:
        ts = "2099-01-01T00:00:00+00:00"

    event = {
        "message_id":       message_id,
        "event_type":       email_type,
        "ses_message_id":   hashlib.md5(message_id.encode()).hexdigest(),
        "sender":           "noreply@ecommerce.example.com",
        "recipient":        maybe_null(recipient, dirty, base=0.01, elev=0.05),
        "subject":          maybe_null(subjects.get(email_type,""), dirty),
        "sent_at":          ts,
        "ses_region":       random.choice(SES_REGIONS),
        "order_id":         order_id if email_type not in ("marketing_weekly","marketing_promo","password_reset") else None,
        "customer_id":      maybe_null(customer_id, dirty),
        "delivery_status":  status,
        "delivered_at":     (sent_at + timedelta(seconds=random.randint(1,300))).isoformat()
                            if status == "delivered" else None,
        "sending_account_id": "467091806172",
        "configuration_set":  "ecommerce-transactional",
    }

    if status == "bounced":
        bounce_type = random.choice(BOUNCE_TYPES)
        event["bounce"] = {
            "bounce_type":     bounce_type,
            "bounce_subtype":  "General" if bounce_type == "Permanent" else "MailboxFull",
            "bounced_at":      (sent_at + timedelta(seconds=random.randint(1,60))).isoformat(),
            "status":          "5.1.1" if bounce_type == "Permanent" else "4.2.2",
        }

    if status == "complained":
        event["complaint"] = {
            "complaint_type": random.choice(COMPLAINT_TYPES),
            "complained_at":  (sent_at + timedelta(minutes=random.randint(5,60))).isoformat(),
            "feedback_id":    str(uuid.uuid4()),
        }

    if email_type in ("marketing_weekly","marketing_promo") and status == "delivered":
        event["engagement"] = {
            "opened":    maybe_null(random.random() > 0.75, dirty),
            "clicked":   maybe_null(random.random() > 0.90, dirty),
            "unsubscribed": random.random() < 0.002,
        }

    return event

def write_hour_file(events, output_dir, hour_dt):
    """
    Write all SES events for one hour to a single file.
    Folder: output_dir/YYYY/MM/DD/HH/
    Filename: ses_events_YYYYMMDD_HH00_to_YYYYMMDD_HH59.json
    """
    dp = output_dir / hour_dt.strftime("%Y/%m/%d/%H")
    dp.mkdir(parents=True, exist_ok=True)
    fname = (
        f"ses_events_"
        f"{hour_dt.strftime('%Y%m%d_%H')}00_to_"
        f"{hour_dt.strftime('%Y%m%d_%H')}59.json"
    )
    with open(dp / fname, "w") as f:
        json.dump({
            "hour":       hour_dt.strftime("%Y-%m-%d %H:00 UTC"),
            "count":      len(events),
            "source":     "aws:ses",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "events":     events,
        }, f, default=str)
    log.info(f"  Written: {fname} ({len(events)} events)")

def run_burst(output_dir, days=7, dirty=False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}"); get_entity_ids()
    t0, stats = time.time(), {"events": 0, "files": 0}
    total = days * 5000
    now   = datetime.now(timezone.utc)

    hour_buckets: dict = {}
    for _ in range(total):
        days_ago = random.uniform(0, days)
        sent_at  = now - timedelta(days=days_ago)
        hour_key = sent_at.replace(minute=0, second=0, microsecond=0)
        hour_buckets.setdefault(hour_key, []).append(build_ses_event(sent_at, dirty))
        stats["events"] += 1

    for hour_dt, events in sorted(hour_buckets.items()):
        write_hour_file(events, output_dir, hour_dt)
        stats["files"] += 1

    elapsed = time.time() - t0
    log.info(f"✓ BURST COMPLETE | {elapsed:.1f}s | {stats}")
    log.info(f"Rule 2 {'✅' if elapsed <= 120 else 'VIOLATION'} {elapsed:.1f}s")

def run_stream(output_dir, dirty=False):
    log.info(f"STREAM MODE | ~5K/day | dirty={dirty} | Ctrl+C to stop")
    get_entity_ids(); stats, i = {"events": 0}, 0
    try:
        while True:
            i   += 1; now = datetime.now(timezone.utc)
            hour_key = now.replace(minute=0, second=0, microsecond=0)
            write_hour_file([build_ses_event(now, dirty)], output_dir, hour_key)
            stats["events"] += 1
            if i % 100 == 0: log.info(f"Stream — {stats}")
            time.sleep(STREAM_SLEEP)
    except KeyboardInterrupt: log.info(f"Stopped. {stats}")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["burst","stream"], required=True)
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--dirty", action="store_true")
    p.add_argument("--output-dir", type=str, default=os.environ.get("EMAIL_OUTPUT_DIR","/tmp/email_raw"))
    args = p.parse_args()
    od = Path(args.output_dir); od.mkdir(parents=True, exist_ok=True)
    log.info(f"Source 18 | mode={args.mode} | days={args.days} | dirty={args.dirty} | output={od}")
    if args.mode == "burst": run_burst(od, args.days, args.dirty)
    else: run_stream(od, args.dirty)

if __name__ == "__main__": main()

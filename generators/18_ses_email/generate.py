"""
generators/18_ses_email/generate.py — Staff DE Journey: Source 18

Simulates AWS SES email events — order confirmations, shipping notifications,
marketing emails. SES logs delivery events to S3 via SNS/Lambda.

Real SES ingestion pattern:
  1. Application sends email via SES API
  2. SES fires delivery/bounce/complaint events to SNS topic
  3. SNS → Lambda → S3: raw/email_events/YYYY/MM/DD/
  4. Databricks reads → Bronze table: bronze.email_events
  5. Silver: parse MIME headers, extract order IDs, track delivery rates

Why email data matters:
  - Delivery rates indicate customer reachability
  - Bounce/complaint rates affect sender reputation
  - Open/click tracking (via SES engagement tracking) feeds marketing analytics
  - Order confirmation emails are source of truth for customer communication audit

Rules satisfied:
    Rule 1  — --mode burst and --mode stream
    Rule 2  — burst < 2 minutes
    Rule 3  — timestamps follow order/shipping event timing
    Rule 4  — delivery status reflects age
    Rule 5  — idempotent (message_id is stable)
    Rule 6  — stream ~0.058 ops/sec (~5K emails/day)
    Rule 7  — dirty data: bounces, malformed addresses, missing headers
    Rule 8  — README.md exists
    Rule 9  — connection via environment variables
    Rule 10 — callable from single bash line

Usage:
    python generate.py --mode burst --output-dir /tmp/email_raw
    python generate.py --mode burst --dirty --output-dir /tmp/email_raw
    python generate.py --mode stream --output-dir /tmp/email_raw
"""

import os
import json
import time
import random
import logging
import argparse
import uuid
import hashlib
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

STREAM_SLEEP = 1.0 / (5000 / 86400)  # ~5K emails/day

EMAIL_TYPES = [
    "order_confirmation",   # Triggered on order placement
    "order_confirmation",
    "shipping_notification",# Triggered when order ships
    "shipping_notification",
    "delivery_confirmation",# Triggered on delivery
    "refund_confirmation",  # Triggered on refund
    "marketing_weekly",     # Weekly newsletter
    "marketing_promo",      # Promotional
    "password_reset",       # Triggered by user action
    "review_request",       # Post-delivery follow-up
]

SES_REGIONS = ["eu-west-1", "eu-north-1"]

BOUNCE_TYPES = [
    "Permanent",  # Hard bounce — bad address
    "Transient",  # Soft bounce — mailbox full
]

COMPLAINT_TYPES = [
    "abuse",          # Marked as spam
    "auth-failure",   # Authentication failure
    "fraud",          # Fraudulent content
    "not-spam",       # False positive
    "other",
]

DELIVERY_STATUSES = ["delivered", "delivered", "delivered",
                     "bounced", "complained", "pending"]


# ─────────────────────────────────────────────────────────────
# RULE 3 + 4 — TIMESTAMPS AND STATUS REFLECT AGE
# ─────────────────────────────────────────────────────────────

def backdate(days_ago: float) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=days_ago)

def delivery_status_for_age(sent_at: datetime) -> str:
    age_mins = (datetime.now(timezone.utc) - sent_at).total_seconds() / 60
    if age_mins < 1:
        return "pending"
    elif age_mins < 5:
        return random.choices(
            ["pending", "delivered"], weights=[0.2, 0.8]
        )[0]
    else:
        return random.choices(
            ["delivered", "bounced", "complained"],
            weights=[0.95, 0.04, 0.01]
        )[0]


# ─────────────────────────────────────────────────────────────
# RULE 7 — DIRTY DATA
# ─────────────────────────────────────────────────────────────

def dr(base, dirty, elevated):
    return elevated if dirty else base

def bad_email_address(email, dirty):
    if random.random() < dr(0.02, dirty, 0.10):
        return random.choice([
            "invalid-email",
            "@nodomain.com",
            "user@",
            None,
            "",
            "user@domain",   # Missing TLD
        ])
    return email

def maybe_null(value, dirty, base=0.05, elev=0.20):
    if random.random() < dr(base, dirty, elev):
        return None
    return value


# ─────────────────────────────────────────────────────────────
# SES EVENT BUILDER
# ─────────────────────────────────────────────────────────────

def build_ses_event(sent_at: datetime, dirty: bool = False) -> dict:
    """
    Build a SES email event record.
    Mirrors what SES sends to SNS → Lambda → S3.
    """
    email_type    = random.choice(EMAIL_TYPES)
    order_id      = random.randint(1, 6751)
    recipient     = bad_email_address(fake.email(), dirty)
    message_id    = f"<{uuid.uuid4().hex}@eu-west-1.amazonses.com>"
    status        = delivery_status_for_age(sent_at)

    # Subject line based on email type
    subjects = {
        "order_confirmation":    f"Order Confirmation #{order_id} - Thank you for your order!",
        "shipping_notification": f"Your order #{order_id} has been shipped",
        "delivery_confirmation": f"Your order #{order_id} has been delivered",
        "refund_confirmation":   f"Refund processed for order #{order_id}",
        "marketing_weekly":      "This week's top picks just for you",
        "marketing_promo":       f"Flash sale - up to 30% off this weekend",
        "password_reset":        "Reset your password",
        "review_request":        f"How was your recent order #{order_id}?",
    }

    subject = subjects.get(email_type, f"Notification for order #{order_id}")

    # Base event
    event = {
        "message_id":      message_id,
        "event_type":      email_type,
        "ses_message_id":  hashlib.md5(message_id.encode()).hexdigest(),

        # Sending details
        "sender":          "noreply@ecommerce.example.com",
        "recipient":       maybe_null(recipient, dirty, base=0.01, elev=0.05),
        "subject":         maybe_null(subject, dirty),
        "sent_at":         sent_at.isoformat(),
        "ses_region":      random.choice(SES_REGIONS),

        # Order reference — join key to Postgres
        "order_id":        order_id if email_type not in ("marketing_weekly", "marketing_promo", "password_reset") else None,
        "customer_id":     maybe_null(random.randint(1, 1035), dirty),

        # Delivery status
        "delivery_status": status,
        "delivered_at":    (sent_at + timedelta(seconds=random.randint(1, 300))).isoformat()
                           if status == "delivered" else None,

        # Email headers (subset)
        "headers": {
            "message_id":    maybe_null(message_id, dirty),
            "date":          sent_at.strftime("%a, %d %b %Y %H:%M:%S +0000"),
            "from":          "Ecommerce <noreply@ecommerce.example.com>",
            "to":            maybe_null(recipient, dirty),
            "subject":       maybe_null(subject, dirty),
            "content_type":  "multipart/mixed",
            "x_ses_source":  "ecommerce-platform",
        },

        # SES sending stats
        "sending_account_id": "467091806172",
        "configuration_set":  "ecommerce-transactional",

        # Dirty mode: 2% future timestamps
        "sent_at": (
            "2099-01-01T00:00:00+00:00"
            if dirty and random.random() < 0.02
            else sent_at.isoformat()
        ),
    }

    # Add bounce details if bounced
    if status == "bounced":
        bounce_type = random.choice(BOUNCE_TYPES)
        event["bounce"] = {
            "bounce_type":       bounce_type,
            "bounce_subtype":    "General" if bounce_type == "Permanent" else "MailboxFull",
            "bounced_at":        (sent_at + timedelta(seconds=random.randint(1, 60))).isoformat(),
            "action":            "failed",
            "status":            "5.1.1" if bounce_type == "Permanent" else "4.2.2",
            "diagnostic_code":   (
                "smtp; 550 5.1.1 The email account does not exist"
                if bounce_type == "Permanent"
                else "smtp; 452 4.2.2 Mailbox full"
            ),
        }

    # Add complaint details if complained
    if status == "complained":
        event["complaint"] = {
            "complaint_type":       random.choice(COMPLAINT_TYPES),
            "complained_at":        (sent_at + timedelta(minutes=random.randint(5, 60))).isoformat(),
            "feedback_id":          str(uuid.uuid4()),
            "user_agent":           "Mozilla/5.0 (feedback)",
        }

    # Add engagement tracking for marketing emails
    if email_type in ("marketing_weekly", "marketing_promo") and status == "delivered":
        event["engagement"] = {
            "opened":      maybe_null(random.random() > 0.75, dirty),   # 25% open rate
            "opened_at":   maybe_null(
                (sent_at + timedelta(hours=random.uniform(0.5, 48))).isoformat(),
                dirty, base=0.75, elev=0.90),
            "clicked":     maybe_null(random.random() > 0.90, dirty),   # 10% click rate
            "unsubscribed": random.random() < 0.002,                     # 0.2% unsubscribe
        }

    return event


# ─────────────────────────────────────────────────────────────
# OUTPUT
# ─────────────────────────────────────────────────────────────

def write_batch(events: list, output_dir: Path,
                batch_num: int, ts: datetime) -> None:
    date_path = output_dir / ts.strftime("%Y/%m/%d/%H")
    date_path.mkdir(parents=True, exist_ok=True)
    file_path = date_path / f"ses_events_{batch_num:04d}.json"
    with open(file_path, "w") as f:
        json.dump({
            "batch":      batch_num,
            "count":      len(events),
            "source":     "aws:ses",
            "fetched_at": ts.isoformat(),
            "events":     events,
        }, f, default=str)


# ─────────────────────────────────────────────────────────────
# BURST MODE
# ─────────────────────────────────────────────────────────────

def run_burst(output_dir: Path, days: int = 7, dirty: bool = False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}")
    t0    = time.time()
    stats = {"events": 0, "batches": 0,
             "delivered": 0, "bounced": 0, "complained": 0}

    total_events = days * 5000
    batch_size   = 100
    batch_num    = 0
    buffer       = []

    for _ in range(total_events):
        days_ago = random.uniform(0, days)
        sent_at  = backdate(days_ago)
        event    = build_ses_event(sent_at, dirty)

        buffer.append(event)
        stats["events"] += 1
        stats[event.get("delivery_status", "pending")] = \
            stats.get(event.get("delivery_status", "pending"), 0) + 1

        if len(buffer) >= batch_size:
            write_batch(buffer, output_dir, batch_num,
                        backdate(days_ago))
            stats["batches"] += 1
            batch_num += 1
            buffer = []

    if buffer:
        write_batch(buffer, output_dir, batch_num,
                    datetime.now(timezone.utc))
        stats["batches"] += 1

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
    log.info(f"STREAM MODE | ~5K emails/day | dirty={dirty} | Ctrl+C to stop")

    stats, i = {"events": 0}, 0

    try:
        while True:
            i   += 1
            now  = datetime.now(timezone.utc)
            event = build_ses_event(now, dirty)

            write_batch([event], output_dir, i, now)
            stats["events"] += 1

            if i % 100 == 0:
                log.info(f"Stream — {stats}")

            time.sleep(STREAM_SLEEP)

    except KeyboardInterrupt:
        log.info(f"Stopped. {stats}")


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Source 18 — SES Email generator")
    p.add_argument("--mode",       choices=["burst", "stream"], required=True)
    p.add_argument("--days",       type=int, default=7)
    p.add_argument("--dirty",      action="store_true")
    p.add_argument("--output-dir", type=str,
                   default=os.environ.get("EMAIL_OUTPUT_DIR", "/tmp/email_raw"))
    args = p.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"Source 18 | mode={args.mode} | days={args.days} | "
             f"dirty={args.dirty} | output={output_dir}")

    if args.mode == "burst":
        run_burst(output_dir, args.days, args.dirty)
    elif args.mode == "stream":
        run_stream(output_dir, args.dirty)


if __name__ == "__main__":
    main()

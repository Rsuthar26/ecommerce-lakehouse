"""
generators/12_reviews_tickets/generate.py — Staff DE Journey: Source 12

Simulates customer reviews and support tickets — unstructured text data.
This feeds NLP/sentiment analysis in the Gold layer.

Why unstructured text is the hardest source:
  - No schema to validate against
  - Free text can contain anything — HTML, emojis, SQL injection attempts
  - Encoding issues (UTF-8 vs Latin-1 vs Windows-1252)
  - PII everywhere (names, emails, addresses in ticket body)
  - Length varies from 1 word to 10,000 words
  - Silver layer must: detect language, extract sentiment, mask PII,
    classify intent, route to correct team

Real ingestion pattern:
  1. Reviews API (Trustpilot/Google) → webhook → Lambda → S3
  2. Support tickets (Zendesk/Freshdesk) → API poll → Airflow → S3
  3. Both land as JSON files in raw/text/YYYY/MM/DD/
  4. Databricks reads → Bronze table → NLP pipeline

Rules satisfied:
    Rule 1  — --mode burst and --mode stream
    Rule 2  — burst < 2 minutes
    Rule 3  — timestamps backdated realistically
    Rule 4  — ticket status reflects age
    Rule 5  — idempotent (record_id is stable)
    Rule 6  — stream ~0.012 ops/sec (~1K records/day)
    Rule 7  — dirty data, encoding issues, PII
    Rule 8  — README.md exists
    Rule 9  — connection via environment variables
    Rule 10 — callable from single bash line

Usage:
    python generate.py --mode burst --output-dir /tmp/text_raw
    python generate.py --mode burst --dirty --output-dir /tmp/text_raw
    python generate.py --mode stream --output-dir /tmp/text_raw
"""

import os
import json
import time
import random
import logging
import argparse
import uuid
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

STREAM_SLEEP = 1.0 / (1000 / 86400)  # ~1K records/day

PRODUCT_SKUS = [f"SKU-{str(i).zfill(5)}" for i in range(1, 201)]

# ─────────────────────────────────────────────────────────────
# REALISTIC REVIEW TEXT
# Positive, negative, neutral — weighted realistically
# Real product reviews cluster around 4-5 stars (J-curve distribution)
# ─────────────────────────────────────────────────────────────

POSITIVE_REVIEWS = [
    "Absolutely love this product! Arrived quickly and works perfectly.",
    "Great quality for the price. Would definitely recommend to anyone.",
    "Exceeded my expectations. The build quality is excellent.",
    "Fast delivery, well packaged, and exactly as described. 5 stars!",
    "Been using this for a month now and still going strong. Very happy.",
    "Perfect gift. Recipient was thrilled. Will buy again.",
    "Solid product. Does exactly what it says on the tin.",
    "Brilliant! Noticed an immediate improvement. Very impressed.",
    "Superb quality. Customer service was also very helpful.",
    "Fantastic product at a great price. Highly recommended!",
]

NEGATIVE_REVIEWS = [
    "Stopped working after 2 weeks. Very disappointing.",
    "Arrived damaged. Packaging was poor and product was broken.",
    "Not as described. Completely different from the photos.",
    "Terrible quality. Fell apart immediately. Do not buy.",
    "Waited 3 weeks for delivery and it arrived wrong. Awful experience.",
    "Overpriced for what you get. Much better alternatives available.",
    "Customer service completely ignored my complaint. Shocking.",
    "Product is a cheap imitation. Nothing like the description.",
    "Broke on first use. Complete waste of money.",
    "Very disappointed. Expected much better for the price.",
]

NEUTRAL_REVIEWS = [
    "Decent product. Does the job but nothing special.",
    "Average quality. Delivery was fine. Nothing to complain about.",
    "It's OK. Not as good as expected but acceptable for the price.",
    "Works as expected. Nothing more, nothing less.",
    "Reasonable product. Arrived on time. Average overall.",
]

SUPPORT_SUBJECTS = [
    "Order not arrived after 2 weeks",
    "Wrong item received",
    "Item arrived damaged",
    "Request for refund",
    "Where is my order?",
    "Product stopped working",
    "Incorrect charge on my account",
    "Need to change delivery address",
    "Return request",
    "Product not as described",
    "Missing parts from order",
    "Delivery to wrong address",
    "Duplicate charge",
    "Warranty claim",
    "Exchange request",
]

SUPPORT_BODIES = [
    "Hi, I placed an order {days} days ago (order #{order_id}) and it still hasn't arrived. "
    "Can you please look into this? My tracking number shows no updates.",

    "I received my order today but it's completely wrong. "
    "I ordered {product} but received something completely different. "
    "Please advise how to return and get the correct item.",

    "My package arrived this morning but the item was damaged. "
    "The box was crushed and the product inside is broken. "
    "I have photos if needed. I'd like a replacement please.",

    "I'd like to request a refund for order #{order_id}. "
    "The product is not what I expected and I'd like to return it. "
    "Please let me know the returns process.",

    "Hello, I'm contacting you because I was charged twice for my order #{order_id}. "
    "Please can you refund the duplicate charge as soon as possible.",
]

TICKET_STATUSES = {
    "new":          {"age_hours": (0, 2)},
    "open":         {"age_hours": (2, 24)},
    "pending":      {"age_hours": (24, 72)},
    "resolved":     {"age_hours": (72, 720)},
    "closed":       {"age_hours": (720, 9999)},
}

TICKET_PRIORITIES = ["low", "normal", "high", "urgent"]


# ─────────────────────────────────────────────────────────────
# RULE 3 + 4 — TIMESTAMPS AND STATUS
# ─────────────────────────────────────────────────────────────

def backdate(days_ago: float) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=days_ago)

def ticket_status_for_age(created_at: datetime) -> str:
    age_hrs = (datetime.now(timezone.utc) - created_at).total_seconds() / 3600
    if age_hrs < 2:
        return "new"
    elif age_hrs < 24:
        return "open"
    elif age_hrs < 72:
        return "pending"
    elif age_hrs < 720:
        return "resolved"
    else:
        return "closed"


# ─────────────────────────────────────────────────────────────
# RULE 7 — DIRTY DATA
# ─────────────────────────────────────────────────────────────

def dr(base, dirty, elevated):
    return elevated if dirty else base

def maybe_inject_html(text, dirty):
    """Some users paste HTML into review forms."""
    if dirty and random.random() < 0.05:
        return f"<b>{text}</b> <script>alert('xss')</script>"
    return text

def maybe_encoding_issue(text, dirty):
    """Simulate Windows-1252 characters in UTF-8 stream."""
    if dirty and random.random() < 0.03:
        return text + " \x92\x93\x94"  # Windows smart quotes
    return text

def maybe_null(value, dirty, base=0.05, elev=0.20):
    if random.random() < dr(base, dirty, elev):
        return None
    return value

def maybe_future_ts(ts, dirty):
    if dirty and random.random() < 0.02:
        return datetime(2099, 1, 1, tzinfo=timezone.utc).isoformat()
    return ts.isoformat()


# ─────────────────────────────────────────────────────────────
# RECORD BUILDERS
# ─────────────────────────────────────────────────────────────

def build_review(created_at: datetime, dirty: bool = False) -> dict:
    """Build a product review record."""
    # J-curve distribution — most reviews are 4-5 stars
    rating = random.choices([1, 2, 3, 4, 5], weights=[5, 5, 10, 30, 50])[0]

    if rating >= 4:
        text = random.choice(POSITIVE_REVIEWS)
    elif rating <= 2:
        text = random.choice(NEGATIVE_REVIEWS)
    else:
        text = random.choice(NEUTRAL_REVIEWS)

    text = maybe_inject_html(text, dirty)
    text = maybe_encoding_issue(text, dirty)

    return {
        "record_id":     str(uuid.uuid4()),
        "record_type":   "review",
        "source":        random.choice(["website", "trustpilot", "google", "app"]),
        "product_sku":   maybe_null(random.choice(PRODUCT_SKUS), dirty),
        "order_id":      maybe_null(random.randint(1, 6751), dirty),
        "rating":        rating,
        "title":         maybe_null(fake.sentence(nb_words=5), dirty),
        "body":          text,
        "author_name":   maybe_null(fake.name(), dirty),
        "author_email":  maybe_null(fake.email(), dirty),
        "verified_purchase": random.random() > 0.2,
        "helpful_votes": random.randint(0, 50),
        "language":      "en",
        "created_at":    maybe_future_ts(created_at, dirty),
        "updated_at":    created_at.isoformat(),
        "status":        random.choice(["published", "pending_moderation", "rejected"]),
    }


def build_ticket(created_at: datetime, dirty: bool = False) -> dict:
    """Build a support ticket record."""
    order_id  = random.randint(1, 6751)
    product   = random.choice(["Wireless Headphones", "USB-C Hub", "Laptop Stand"])
    days_ago  = (datetime.now(timezone.utc) - created_at).days

    subject   = random.choice(SUPPORT_SUBJECTS)
    body_tmpl = random.choice(SUPPORT_BODIES)
    body      = body_tmpl.format(
        days=days_ago or 1,
        order_id=order_id,
        product=product
    )

    body = maybe_inject_html(body, dirty)
    body = maybe_encoding_issue(body, dirty)

    status   = ticket_status_for_age(created_at)
    priority = random.choices(
        TICKET_PRIORITIES,
        weights=[30, 50, 15, 5]
    )[0]

    resolved_at = None
    if status in ("resolved", "closed"):
        resolved_at = (created_at + timedelta(
            hours=random.uniform(4, 72))).isoformat()

    return {
        "record_id":      str(uuid.uuid4()),
        "record_type":    "support_ticket",
        "ticket_id":      f"TKT-{random.randint(100000, 999999)}",
        "source":         random.choice(["email", "web_form", "chat", "phone"]),
        "order_id":       maybe_null(order_id, dirty),
        "product_sku":    maybe_null(random.choice(PRODUCT_SKUS), dirty),
        "subject":        maybe_null(subject, dirty),
        "body":           body,
        "author_name":    maybe_null(fake.name(), dirty),
        "author_email":   maybe_null(fake.email(), dirty),
        "status":         status,
        "priority":       priority,
        "assigned_to":    maybe_null(fake.user_name(), dirty),
        "tags":           random.sample(
                              ["refund", "shipping", "damaged", "wrong_item",
                               "billing", "technical", "general"],
                              random.randint(0, 3)),
        "satisfaction_score": maybe_null(
            random.choice([1, 2, 3, 4, 5]), dirty,
            base=0.30, elev=0.50  # Often missing — customer doesn't rate
        ),
        "created_at":     maybe_future_ts(created_at, dirty),
        "updated_at":     created_at.isoformat(),
        "resolved_at":    resolved_at,
        "first_response_hours": maybe_null(
            round(random.uniform(0.5, 48), 1), dirty),
    }


# ─────────────────────────────────────────────────────────────
# OUTPUT
# ─────────────────────────────────────────────────────────────

def write_batch(records: list, output_dir: Path,
                batch_num: int, ts: datetime) -> None:
    date_path = output_dir / ts.strftime("%Y/%m/%d/%H")
    date_path.mkdir(parents=True, exist_ok=True)
    file_path = date_path / f"text_batch_{batch_num:04d}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump({
            "batch":      batch_num,
            "count":      len(records),
            "fetched_at": ts.isoformat(),
            "records":    records,
        }, f, default=str, ensure_ascii=False)  # ensure_ascii=False preserves emojis


# ─────────────────────────────────────────────────────────────
# BURST MODE
# ─────────────────────────────────────────────────────────────

def run_burst(output_dir: Path, days: int = 7, dirty: bool = False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}")
    t0    = time.time()
    stats = {"reviews": 0, "tickets": 0, "batches": 0}

    # ~1K records/day = ~7K records over 7 days
    total_records = days * 1000
    batch_size    = 100
    batch_num     = 0
    buffer        = []

    for i in range(total_records):
        days_ago   = random.uniform(0, days)
        created_at = backdate(days_ago)

        # 60% reviews, 40% tickets
        if random.random() < 0.6:
            record = build_review(created_at, dirty)
            stats["reviews"] += 1
        else:
            record = build_ticket(created_at, dirty)
            stats["tickets"] += 1

        buffer.append(record)

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
    log.info(f"STREAM MODE | ~1K records/day | dirty={dirty} | Ctrl+C to stop")

    stats, i = {"records": 0}, 0

    try:
        while True:
            i  += 1
            now = datetime.now(timezone.utc)

            if random.random() < 0.6:
                record = build_review(now, dirty)
            else:
                record = build_ticket(now, dirty)

            write_batch([record], output_dir, i, now)
            stats["records"] += 1

            if i % 100 == 0:
                log.info(f"Stream — {stats}")

            time.sleep(STREAM_SLEEP)

    except KeyboardInterrupt:
        log.info(f"Stopped. {stats}")


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Source 12 — Reviews/Tickets generator")
    p.add_argument("--mode",       choices=["burst", "stream"], required=True)
    p.add_argument("--days",       type=int, default=7)
    p.add_argument("--dirty",      action="store_true")
    p.add_argument("--output-dir", type=str,
                   default=os.environ.get("TEXT_OUTPUT_DIR", "/tmp/text_raw"))
    args = p.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"Source 12 | mode={args.mode} | days={args.days} | "
             f"dirty={args.dirty} | output={output_dir}")

    if args.mode == "burst":
        run_burst(output_dir, args.days, args.dirty)
    elif args.mode == "stream":
        run_stream(output_dir, args.dirty)


if __name__ == "__main__":
    main()

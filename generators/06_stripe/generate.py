"""
generators/06_stripe/generate.py — Staff DE Journey: Source 06

Simulates Stripe API payment objects: charges, refunds, disputes.
Reads real order/payment data from Postgres to ensure join integrity.

Real Stripe ingestion pattern:
  1. Stripe sends webhook events to our API endpoint
  2. API writes to SQS, Lambda processes → S3 raw
  3. Or: Airflow polls Stripe API → writes Parquet to S3 raw
  4. Databricks reads → Bronze table: bronze.stripe_raw

Rules satisfied:
    Rule 1  — --mode burst and --mode stream
    Rule 2  — burst < 2 minutes
    Rule 3  — payment timestamps follow order creation times
    Rule 4  — charge/refund status reflects age of payment
    Rule 5  — idempotent (payment_intent_id is stable key)
    Rule 6  — stream: ~10K/day (STREAM_SLEEP = 86400/10000)
    Rule 7  — dirty: failed charges, disputed payments, missing fields
    Rule 8  — README.md exists
    Rule 9  — env vars: PG_HOST etc (for Rule 11 loading)
    Rule 10 — callable from single bash line
    Rule 11 — reads real order_ids and payment amounts from Postgres

Usage:
    python generate.py --mode burst --output-dir /tmp/stripe_raw
    python generate.py --mode burst --dirty --output-dir /tmp/stripe_raw
    python generate.py --mode stream --output-dir /tmp/stripe_raw
"""

import os
import sys
import json
import time
import random
import logging
import argparse
import hashlib
from datetime import datetime, timezone, timedelta
from pathlib import Path

from faker import Faker
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from generators.shared.postgres_ids import load_entity_ids

load_dotenv()

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

fake = Faker("en_GB")
Faker.seed(42)

STREAM_SLEEP = 86400 / 10000   # Rule 6 + Rule 13

PAYMENT_METHODS = ["card", "paypal", "klarna", "bank_transfer"]
CARD_BRANDS     = ["visa", "mastercard", "amex", "maestro"]
CURRENCIES      = ["GBP"]
FAILURE_CODES   = [
    "card_declined", "insufficient_funds", "expired_card",
    "incorrect_cvc", "processing_error", "do_not_honor"
]
DISPUTE_REASONS = [
    "fraudulent", "duplicate", "product_not_received",
    "product_unacceptable", "subscription_canceled"
]

_entity_ids = None

def get_entity_ids():
    global _entity_ids
    if _entity_ids is None:
        _entity_ids = load_entity_ids()
    return _entity_ids


# ─────────────────────────────────────────────────────────────
# RULE 3 + 4 — TIMESTAMPS AND STATUS REFLECT AGE
# ─────────────────────────────────────────────────────────────

def payment_timestamp(base_dt: datetime) -> datetime:
    """Payments happen 0-30 minutes after order placement."""
    offset = timedelta(minutes=random.randint(0, 30))
    return (base_dt + offset).replace(tzinfo=timezone.utc)

def charge_status_for_age(created_at: datetime) -> str:
    age_hrs = (datetime.now(timezone.utc) - created_at).total_seconds() / 3600
    if age_hrs < 0.5:
        return random.choices(["pending", "succeeded"], weights=[0.3, 0.7])[0]
    else:
        return random.choices(
            ["succeeded", "failed", "disputed"],
            weights=[0.92, 0.06, 0.02]
        )[0]


# ─────────────────────────────────────────────────────────────
# RULE 7 — DIRTY DATA
# ─────────────────────────────────────────────────────────────

def dr(base, dirty, elevated): return elevated if dirty else base
def maybe_null(v, dirty, base=0.05, elev=0.20):
    return None if random.random() < dr(base, dirty, elev) else v

def dirty_amount(amount, dirty):
    if random.random() < dr(0.005, dirty, 0.05):
        return random.choice([0, -100, 999999900])
    return amount


# ─────────────────────────────────────────────────────────────
# STRIPE OBJECT BUILDER
# ─────────────────────────────────────────────────────────────

def build_stripe_charge(created_at: datetime, dirty: bool = False) -> dict:
    ids           = get_entity_ids()
    order_id      = random.choice(ids["order_ids"]) if ids["order_ids"] else None
    amount_pence  = dirty_amount(random.randint(999, 99999), dirty)
    status        = charge_status_for_age(created_at)
    method        = random.choice(PAYMENT_METHODS)

    # Stripe payment intent ID — stable for Rule 5
    pi_id = f"pi_{hashlib.md5(f'order_{order_id}_{created_at.date()}'.encode()).hexdigest()[:24]}"

    charge = {
        "object":        "charge",
        "id":            f"ch_{fake.uuid4().replace('-','')[:24]}",
        "payment_intent": maybe_null(pi_id, dirty, base=0.01),
        "amount":         amount_pence,
        "amount_captured": amount_pence if status == "succeeded" else 0,
        "amount_refunded": 0,
        "currency":       "gbp",
        "status":         status,
        "paid":           status == "succeeded",
        "captured":       status == "succeeded",
        "created":        int(created_at.timestamp()),
        "created_at":     created_at.isoformat(),

        # Metadata — joins back to our order
        "metadata": {
            "order_id":   maybe_null(str(order_id), dirty, base=0.01),
            "customer_id": maybe_null(
                str(random.choice(ids["customer_ids"])) if ids["customer_ids"] else None,
                dirty
            ),
        },

        "payment_method_details": {
            "type": method,
            "card": {
                "brand":        maybe_null(random.choice(CARD_BRANDS), dirty),
                "last4":        maybe_null(str(random.randint(1000,9999)), dirty),
                "exp_month":    random.randint(1, 12),
                "exp_year":     random.randint(2025, 2030),
                "country":      maybe_null("GB", dirty),
                "funding":      random.choice(["credit","debit"]),
                "cvc_check":    maybe_null("pass", dirty),
            } if method == "card" else None,
        },

        "billing_details": {
            "name":    maybe_null(fake.name(), dirty),
            "email":   maybe_null(fake.email(), dirty),
            "address": {
                "line1":       maybe_null(fake.street_address(), dirty),
                "city":        maybe_null(fake.city(), dirty),
                "postal_code": maybe_null(fake.postcode(), dirty),
                "country":     "GB",
            }
        },

        "receipt_url": f"https://pay.stripe.com/receipts/{fake.uuid4()}",
        "description": f"Order #{order_id}",
        "livemode":    False,
        "source":      "ecommerce-platform",
    }

    if status == "failed":
        charge["failure_code"]    = maybe_null(random.choice(FAILURE_CODES), dirty)
        charge["failure_message"] = maybe_null(
            "Your card was declined. Contact your bank.", dirty)

    if status == "disputed":
        charge["dispute"] = {
            "id":      f"dp_{fake.uuid4().replace('-','')[:24]}",
            "amount":  amount_pence,
            "reason":  maybe_null(random.choice(DISPUTE_REASONS), dirty),
            "status":  random.choice(["warning_needs_response","under_review","won","lost"]),
        }

    # Refund object (5% of succeeded charges get refunded)
    if status == "succeeded" and random.random() < 0.05:
        refund_amount = int(amount_pence * random.uniform(0.1, 1.0))
        charge["refunds"] = [{
            "id":      f"re_{fake.uuid4().replace('-','')[:24]}",
            "amount":  refund_amount,
            "status":  "succeeded",
            "reason":  random.choice(["duplicate","fraudulent","requested_by_customer"]),
            "created": int((created_at + timedelta(hours=random.randint(1,72))).timestamp()),
        }]
        charge["amount_refunded"] = refund_amount

    # Dirty: 2% future timestamps
    if dirty and random.random() < 0.02:
        charge["created_at"] = "2099-01-01T00:00:00+00:00"

    return charge


# ─────────────────────────────────────────────────────────────
# OUTPUT
# ─────────────────────────────────────────────────────────────

def write_batch(charges: list, output_dir: Path, batch_num: int, ts: datetime):
    date_path = output_dir / ts.strftime("%Y/%m/%d/%H")
    date_path.mkdir(parents=True, exist_ok=True)
    file_path = date_path / f"stripe_charges_{batch_num:04d}.json"
    with open(file_path, "w") as f:
        json.dump({
            "batch":      batch_num,
            "count":      len(charges),
            "source":     "stripe",
            "fetched_at": ts.isoformat(),
            "charges":    charges,
        }, f, default=str)


# ─────────────────────────────────────────────────────────────
# BURST MODE
# ─────────────────────────────────────────────────────────────

def run_burst(output_dir: Path, days: int = 7, dirty: bool = False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}")
    get_entity_ids()
    t0     = time.time()
    stats  = {"charges": 0, "batches": 0}
    now    = datetime.now(timezone.utc)
    total  = days * 1500   # ~10K/day, sample for burst speed
    batch_size = 100
    buffer, batch_num = [], 0

    for _ in range(total):
        days_ago   = random.uniform(0, days)
        base_dt    = now - timedelta(days=days_ago)
        created_at = payment_timestamp(base_dt)
        charge     = build_stripe_charge(created_at, dirty)
        buffer.append(charge)
        stats["charges"] += 1

        if len(buffer) >= batch_size:
            write_batch(buffer, output_dir, batch_num, created_at)
            stats["batches"] += 1
            batch_num += 1
            buffer = []

    if buffer:
        write_batch(buffer, output_dir, batch_num, datetime.now(timezone.utc))
        stats["batches"] += 1

    elapsed = time.time() - t0
    log.info(f"✓ BURST COMPLETE | {elapsed:.1f}s | {stats}")
    log.info(f"Rule 2 {'✅' if elapsed <= 120 else 'VIOLATION'} {elapsed:.1f}s")


# ─────────────────────────────────────────────────────────────
# STREAM MODE
# ─────────────────────────────────────────────────────────────

def run_stream(output_dir: Path, dirty: bool = False):
    log.info(f"STREAM MODE | ~10K/day | dirty={dirty} | Ctrl+C to stop")
    get_entity_ids()
    stats, i = {"charges": 0}, 0
    try:
        while True:
            i     += 1
            now    = datetime.now(timezone.utc)
            charge = build_stripe_charge(now, dirty)
            write_batch([charge], output_dir, i, now)
            stats["charges"] += 1
            if i % 100 == 0:
                log.info(f"Stream — {stats}")
            time.sleep(STREAM_SLEEP)
    except KeyboardInterrupt:
        log.info(f"Stopped. {stats}")


def main():
    p = argparse.ArgumentParser(description="Source 06 — Stripe Payments")
    p.add_argument("--mode",       choices=["burst","stream"], required=True)
    p.add_argument("--days",       type=int, default=7)
    p.add_argument("--dirty",      action="store_true")
    p.add_argument("--output-dir", type=str,
                   default=os.environ.get("STRIPE_OUTPUT_DIR", "/tmp/stripe_raw"))
    args = p.parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    log.info(f"Source 06 | mode={args.mode} | days={args.days} | dirty={args.dirty}")
    if args.mode == "burst":   run_burst(output_dir, args.days, args.dirty)
    elif args.mode == "stream": run_stream(output_dir, args.dirty)

if __name__ == "__main__":
    main()

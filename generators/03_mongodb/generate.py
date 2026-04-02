"""
generators/06_stripe/generate.py — Staff DE Journey: Source 06

Simulates Stripe API responses for payments, charges, refunds, disputes.
Reads real order/payment data from Postgres to ensure joinability.
Outputs JSON files to S3 (or local path for dev) — Bronze layer.

Real Stripe ingestion pattern:
  1. Airflow triggers this job on a schedule (hourly)
  2. Job calls stripe.PaymentIntent.list(created_after=last_run_ts)
  3. Paginate through results
  4. Write each page as a JSON file to S3: raw/stripe/YYYY/MM/DD/HH/page_N.json
  5. Databricks Autoloader picks up new files → Bronze table

Rules satisfied:
    Rule 1  — --mode burst and --mode stream
    Rule 2  — burst produces 7 days, < 2 minutes
    Rule 3  — timestamps backdated realistically
    Rule 4  — payment status reflects age
    Rule 5  — idempotent (payment_intent_id is stable)
    Rule 6  — stream ~0.12 ops/sec (~10K records/day)
    Rule 7  — dirty data + quarantine file
    Rule 8  — README.md exists
    Rule 9  — connection via environment variables
    Rule 10 — callable from single bash line

Usage:
    python generate.py --mode burst --output-dir /tmp/stripe_output
    python generate.py --mode burst --dirty --output-dir /tmp/stripe_output
    python generate.py --mode stream --output-dir /tmp/stripe_output
"""

import os
import sys
import json
import time
import random
import logging
import argparse
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path

import psycopg2
import psycopg2.extras
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

STREAM_SLEEP = 1.0 / (10000 / 86400)  # ~10K records/day

CARD_BRANDS   = ["visa", "mastercard", "amex", "discover"]
CARD_NETWORKS = ["visa", "mastercard", "amex"]
CURRENCIES    = ["gbp"]
FAILURE_CODES = [
    "insufficient_funds", "card_declined", "expired_card",
    "incorrect_cvc", "processing_error", "do_not_honor"
]

# ─────────────────────────────────────────────────────────────
# RULE 4 — STATUS REFLECTS AGE
# ─────────────────────────────────────────────────────────────

def stripe_status_for_age(created_at: datetime, order_status: str) -> str:
    """Stripe payment status must be consistent with order age and status."""
    age_hrs = (datetime.now(timezone.utc) - created_at).total_seconds() / 3600

    if order_status in ("cancelled", "refunded"):
        return random.choices(
            ["canceled", "requires_payment_method"],
            weights=[0.7, 0.3]
        )[0]
    elif age_hrs < 1:
        return "requires_confirmation"
    elif age_hrs < 3:
        return "processing"
    else:
        return random.choices(
            ["succeeded", "succeeded", "succeeded", "requires_payment_method"],
            weights=[0.92, 0.02, 0.03, 0.03]
        )[0]


# ─────────────────────────────────────────────────────────────
# RULE 7 — DIRTY DATA
# ─────────────────────────────────────────────────────────────

def dr(base, dirty, elevated):
    return elevated if dirty else base

def bad_amount(amount, dirty):
    if random.random() < dr(0.005, dirty, 0.08):
        return random.choice([0, -1, None, "free", 999999999])
    return amount

def bad_currency(dirty):
    if random.random() < dr(0.01, dirty, 0.10):
        return random.choice(["GBP", "gbP", "usd", "EUR", None, ""])
    return "gbp"

def missing_field(value, dirty, base=0.05, elev=0.20):
    return None if random.random() < dr(base, dirty, elev) else value


# ─────────────────────────────────────────────────────────────
# QUARANTINE — written to a separate JSON file
# ─────────────────────────────────────────────────────────────

def quarantine(output_dir: Path, reason: str, data: dict):
    q_dir  = output_dir / "quarantine"
    q_dir.mkdir(parents=True, exist_ok=True)
    q_file = q_dir / f"quarantine_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')}.json"
    with open(q_file, "w") as f:
        json.dump({"reason": reason, "data": data,
                   "ts": datetime.now(timezone.utc).isoformat()}, f)
    log.warning(f"QUARANTINE {reason[:80]}")


# ─────────────────────────────────────────────────────────────
# POSTGRES CONNECTION (Rule 9)
# Read real order/payment data to ensure joinability
# ─────────────────────────────────────────────────────────────

def get_pg_connection():
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", 5432)),
        dbname=os.environ.get("DB_NAME", "ecommerce"),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        connect_timeout=10,
    )


def fetch_payments_from_postgres(days: int = 7) -> list[dict]:
    """
    Fetch real payment records from Postgres.
    These are the source of truth for order IDs and amounts.
    Stripe records must reference the same IDs.
    """
    conn = get_pg_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    p.payment_id,
                    p.order_id,
                    p.amount_pence,
                    p.currency,
                    p.payment_status,
                    p.payment_method,
                    p.card_last4,
                    p.card_brand,
                    p.created_at,
                    o.order_status,
                    o.placed_at
                FROM payments p
                JOIN orders o ON p.order_id = o.order_id
                WHERE p.created_at >= NOW() - INTERVAL '%s days'
                ORDER BY p.created_at DESC
            """, (days,))
            rows = cur.fetchall()
            return [dict(row) for row in rows]
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────
# STRIPE RECORD BUILDER
# ─────────────────────────────────────────────────────────────

def build_stripe_payment_intent(pg_payment: dict, dirty: bool = False) -> dict:
    """
    Build a realistic Stripe PaymentIntent object based on a Postgres payment record.
    The payment_intent_id is derived from payment_id — stable and joinable.

    In production: Stripe generates pi_XXXX IDs. We simulate this by
    using a deterministic format based on our payment_id.
    """
    payment_id   = pg_payment["payment_id"]
    order_id     = pg_payment["order_id"]
    amount_pence = pg_payment["amount_pence"]
    created_at   = pg_payment["created_at"]
    order_status = pg_payment["order_status"]

    # Deterministic payment intent ID — same payment always gets same Stripe ID
    # This is critical for idempotency (Rule 5)
    pi_id     = f"pi_{str(payment_id).zfill(8)}{str(order_id).zfill(8)}"
    charge_id = f"ch_{str(payment_id).zfill(8)}{str(order_id).zfill(6)}"

    stripe_status = stripe_status_for_age(created_at, order_status)
    amount        = bad_amount(amount_pence, dirty)
    currency      = bad_currency(dirty)

    # Validate
    if amount is None or not isinstance(amount, int) or amount <= 0:
        return None  # caller quarantines

    card_brand = pg_payment.get("card_brand") or random.choice(CARD_BRANDS)
    card_last4 = pg_payment.get("card_last4") or str(random.randint(1000, 9999))

    # Stripe charge object (nested inside payment intent)
    charge = {
        "id":                charge_id,
        "object":            "charge",
        "amount":            amount,
        "amount_captured":   amount if stripe_status == "succeeded" else 0,
        "amount_refunded":   0,
        "currency":          currency,
        "captured":          stripe_status == "succeeded",
        "disputed":          random.random() < 0.002,  # 0.2% dispute rate
        "paid":              stripe_status == "succeeded",
        "refunded":          False,
        "status":            "succeeded" if stripe_status == "succeeded" else "failed",
        "failure_code":      random.choice(FAILURE_CODES) if stripe_status != "succeeded" else None,
        "failure_message":   None,
        "payment_method_details": {
            "card": {
                "brand":      card_brand,
                "last4":      card_last4,
                "exp_month":  missing_field(random.randint(1, 12), dirty),
                "exp_year":   missing_field(random.randint(2025, 2030), dirty),
                "country":    missing_field("GB", dirty),
                "funding":    random.choice(["credit", "debit", "prepaid"]),
                "network":    missing_field(card_brand, dirty),
            },
            "type": "card"
        },
        "billing_details": {
            "name":    missing_field(fake.name(), dirty),
            "email":   missing_field(fake.email(), dirty),
            "address": {
                "city":        missing_field(fake.city(), dirty),
                "country":     missing_field("GB", dirty),
                "postal_code": missing_field(fake.postcode(), dirty),
                "line1":       missing_field(fake.street_address(), dirty),
            }
        },
        "created": int(created_at.timestamp()),
    }

    # Add refund if order is refunded
    if order_status == "refunded":
        charge["amount_refunded"] = amount
        charge["refunded"]        = True

    # Add dispute details if disputed
    if charge["disputed"]:
        charge["dispute"] = {
            "id":     f"dp_{uuid.uuid4().hex[:16]}",
            "amount": amount,
            "reason": random.choice(["fraudulent", "not_as_described", "unrecognized"]),
            "status": random.choice(["needs_response", "under_review", "won", "lost"]),
        }

    # Build the full PaymentIntent object
    payment_intent = {
        "id":                      pi_id,
        "object":                  "payment_intent",
        "amount":                  amount,
        "amount_capturable":       0,
        "amount_received":         amount if stripe_status == "succeeded" else 0,
        "currency":                currency,
        "status":                  stripe_status,
        "capture_method":          "automatic",
        "confirmation_method":     "automatic",
        "payment_method_types":    ["card"],
        "payment_method":          f"pm_{str(payment_id).zfill(16)}",

        # Metadata — this is how we join back to our internal order
        "metadata": {
            "order_id":   str(order_id),
            "payment_id": str(payment_id),
            "source":     "ecommerce-platform",
        },

        "charges": {
            "object":   "list",
            "data":     [charge],
            "has_more": False,
            "total_count": 1,
        },

        "created":        int(created_at.timestamp()),
        "created_at_iso": created_at.isoformat(),
        "livemode":       False,  # Always false in our dev environment

        # Dirty mode: 2% future timestamps
        "created_at_iso": (
            "2099-01-01T00:00:00+00:00"
            if dirty and random.random() < 0.02
            else created_at.isoformat()
        ),
    }

    return payment_intent


# ─────────────────────────────────────────────────────────────
# OUTPUT — Write JSON files (Bronze landing zone pattern)
# ─────────────────────────────────────────────────────────────

def write_page(records: list[dict], output_dir: Path, page_num: int,
               ts: datetime) -> Path:
    """
    Write a page of Stripe records as JSON.
    Structure mirrors how real Stripe API pagination works.
    """
    date_path = output_dir / ts.strftime("%Y/%m/%d/%H")
    date_path.mkdir(parents=True, exist_ok=True)

    file_path = date_path / f"stripe_page_{page_num:04d}.json"

    payload = {
        "object":   "list",
        "data":     records,
        "has_more": False,
        "url":      "/v1/payment_intents",
        "fetched_at": ts.isoformat(),
        "page":     page_num,
    }

    with open(file_path, "w") as f:
        json.dump(payload, f, default=str)

    return file_path


# ─────────────────────────────────────────────────────────────
# BURST MODE (Rules 1, 2, 3, 4, 5)
# ─────────────────────────────────────────────────────────────

def run_burst(output_dir: Path, days: int = 7, dirty: bool = False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}")
    t0 = time.time()

    log.info("Fetching payments from Postgres...")
    pg_payments = fetch_payments_from_postgres(days)
    log.info(f"  Found {len(pg_payments)} payments to simulate")

    if not pg_payments:
        log.error("No payments found in Postgres. Run the Postgres generator first.")
        sys.exit(1)

    stats       = {"written": 0, "quarantined": 0, "pages": 0}
    page_size   = 100  # Stripe returns up to 100 per page
    page_num    = 0
    page_buffer = []

    for pg_payment in pg_payments:
        record = build_stripe_payment_intent(pg_payment, dirty)

        if not record:
            quarantine(output_dir, "invalid_amount",
                       {"payment_id": pg_payment["payment_id"],
                        "amount": pg_payment["amount_pence"]})
            stats["quarantined"] += 1
            continue

        page_buffer.append(record)
        stats["written"] += 1

        # Write page when full
        if len(page_buffer) >= page_size:
            ts = pg_payment["created_at"]
            if isinstance(ts, str):
                ts = datetime.fromisoformat(ts)
            write_page(page_buffer, output_dir, page_num,
                       ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts)
            stats["pages"] += 1
            page_num    += 1
            page_buffer  = []

    # Write remaining records
    if page_buffer:
        write_page(page_buffer, output_dir, page_num,
                   datetime.now(timezone.utc))
        stats["pages"] += 1

    elapsed = time.time() - t0
    log.info(f"✓ BURST COMPLETE | {elapsed:.1f}s | {stats}")
    log.info(f"  Output: {output_dir}")

    if elapsed > 120:
        log.warning(f"Rule 2 VIOLATION: {elapsed:.0f}s > 120s")
    else:
        log.info(f"Rule 2 ✅ {elapsed:.1f}s < 120s")


# ─────────────────────────────────────────────────────────────
# STREAM MODE (Rules 1, 6)
# ─────────────────────────────────────────────────────────────

def run_stream(output_dir: Path, dirty: bool = False):
    log.info(f"STREAM MODE | ~10K records/day | dirty={dirty} | Ctrl+C to stop")

    # Load a pool of payments to simulate against
    pg_payments = fetch_payments_from_postgres(days=7)
    if not pg_payments:
        log.error("No payments in Postgres. Run burst first.")
        sys.exit(1)

    stats, i = {"written": 0, "quarantined": 0}, 0

    try:
        while True:
            i += 1
            pg_payment = random.choice(pg_payments)
            record     = build_stripe_payment_intent(pg_payment, dirty)

            if not record:
                quarantine(output_dir, "invalid_amount", pg_payment)
                stats["quarantined"] += 1
            else:
                ts = datetime.now(timezone.utc)
                write_page([record], output_dir, i, ts)
                stats["written"] += 1

            if i % 100 == 0:
                log.info(f"Stream — {stats}")

            time.sleep(STREAM_SLEEP)

    except KeyboardInterrupt:
        log.info(f"Stopped. {stats}")


# ─────────────────────────────────────────────────────────────
# ENTRY POINT (Rules 1, 10)
# ─────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Source 06 — Stripe API generator")
    p.add_argument("--mode",       choices=["burst", "stream"], required=True)
    p.add_argument("--days",       type=int, default=7)
    p.add_argument("--dirty",      action="store_true")
    p.add_argument("--output-dir", type=str,
                   default=os.environ.get("STRIPE_OUTPUT_DIR", "/tmp/stripe_raw"))
    args = p.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"Source 06 | mode={args.mode} | days={args.days} | "
             f"dirty={args.dirty} | output={output_dir}")

    if args.mode == "burst":
        run_burst(output_dir, args.days, args.dirty)
    elif args.mode == "stream":
        run_stream(output_dir, args.dirty)


if __name__ == "__main__":
    main()

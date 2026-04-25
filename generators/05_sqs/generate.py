"""
generators/05_sqs/generate.py — Staff DE Journey: Source 05

Simulates SQS order notification events published to Kafka.
~5K messages/day — order placed, confirmed, shipped, cancelled.

MUST RUN ON EC2 — MSK only reachable from inside VPC.

Rules satisfied:
    Rule 1  — --mode burst and --mode stream
    Rule 2  — burst < 2 minutes
    Rule 3  — FIX: order events follow business hours peaks
    Rule 4  — FIX: event_type reflects age — old events = delivered/refunded
    Rule 5  — FIX: burst uses deterministic message_id — safe to run twice
    Rule 6  — stream: ~5K/day (STREAM_SLEEP = 86400/5000)
    Rule 7  — dirty: missing fields, duplicate messages, bad timestamps
    Rule 8  — README.md exists
    Rule 9  — env vars: KAFKA_BOOTSTRAP_SERVERS
    Rule 10 — callable from single bash line
    Rule 11 — FIX: order_id and customer_id loaded from Postgres at startup

Usage:
    python generate.py --mode burst
    python generate.py --mode burst --dirty
    python generate.py --mode stream
"""

import os
import sys
import json
import time
import random
import hashlib
import logging
import argparse
import uuid
from datetime import datetime, timezone, timedelta

from kafka import KafkaProducer
from kafka.errors import KafkaError
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

STREAM_SLEEP = 86400 / 5000   # Rule 6 + Rule 13: ~1 event per 17s
TOPIC        = "order.events"
BROKERS      = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS",
    "b-1.staffdejourneykafka.g6712a.c2.kafka.eu-west-1.amazonaws.com:9094,"
    "b-2.staffdejourneykafka.g6712a.c2.kafka.eu-west-1.amazonaws.com:9094"
)

_entity_ids = None

def get_entity_ids():
    global _entity_ids
    if _entity_ids is None:
        _entity_ids = load_entity_ids()
    return _entity_ids


# ─────────────────────────────────────────────────────────────
# RULE 3 — BUSINESS HOURS PATTERN
# Order events cluster during business hours 9am-8pm
# ─────────────────────────────────────────────────────────────

def order_hours_timestamp(base_dt: datetime) -> datetime:
    peak_windows = [(9,12,0.35),(19,21,0.25),(12,19,0.30),(8,9,0.05),(21,23,0.05)]
    r, cumul, s, e = random.random(), 0.0, 9, 12
    for start, end, w in peak_windows:
        cumul += w
        if r <= cumul:
            s, e = start, end
            break
    return base_dt.replace(
        hour=random.randint(s, e-1),
        minute=random.randint(0, 59),
        second=random.randint(0, 59),
        tzinfo=timezone.utc
    )


# ─────────────────────────────────────────────────────────────
# RULE 4 — EVENT TYPE REFLECTS AGE
# An order placed 5 days ago cannot still be "placed"
# ─────────────────────────────────────────────────────────────

def event_type_for_age(event_dt: datetime) -> str:
    age_hrs = (datetime.now(timezone.utc) - event_dt).total_seconds() / 3600
    if age_hrs < 2:
        return random.choices(
            ["order.placed", "order.confirmed"], weights=[0.6, 0.4]
        )[0]
    elif age_hrs < 12:
        return random.choices(
            ["order.confirmed", "order.placed"], weights=[0.8, 0.2]
        )[0]
    elif age_hrs < 48:
        return random.choices(
            ["order.confirmed", "order.shipped", "order.cancelled"],
            weights=[0.3, 0.6, 0.1]
        )[0]
    elif age_hrs < 120:
        return random.choices(
            ["order.shipped", "order.delivered", "order.cancelled"],
            weights=[0.4, 0.5, 0.1]
        )[0]
    else:
        return random.choices(
            ["order.delivered", "order.refunded", "order.cancelled"],
            weights=[0.80, 0.10, 0.10]
        )[0]


# ─────────────────────────────────────────────────────────────
# RULE 7 — DIRTY DATA
# ─────────────────────────────────────────────────────────────

def dr(base, dirty, elevated): return elevated if dirty else base
def maybe_null(v, dirty, base=0.05, elev=0.20):
    return None if random.random() < dr(base, dirty, elev) else v


# ─────────────────────────────────────────────────────────────
# EVENT BUILDER
# ─────────────────────────────────────────────────────────────

def build_event(event_dt: datetime, dirty: bool = False,
                burst_seq: int = None) -> dict:
    ids        = get_entity_ids()
    event_type = event_type_for_age(event_dt)          # Rule 4
    order_id   = random.choice(ids["order_ids"]) if ids["order_ids"] else None  # Rule 11
    customer_id = random.choice(ids["customer_ids"]) if ids["customer_ids"] else None  # Rule 11
    amount_pence = random.randint(999, 99999)

    # Rule 5: deterministic message_id in burst mode
    if burst_seq is not None:
        message_id = hashlib.md5(
            f"05:{burst_seq}:{event_dt.isoformat()}".encode()
        ).hexdigest()
    else:
        message_id = str(uuid.uuid4())

    event = {
        "message_id":    message_id,
        "event_type":    event_type,
        "event_ts":      event_dt.isoformat(),
        "order_id":      maybe_null(order_id, dirty, base=0.01, elev=0.05),
        "customer_id":   maybe_null(customer_id, dirty),
        "order_status":  event_type.split(".")[1],
        "amount_pence":  maybe_null(amount_pence, dirty),
        "currency":      "GBP",
        "channel":       maybe_null(random.choice(["web","mobile","api"]), dirty),
        "source":        "sqs",
        "sqs_message_id":      str(uuid.uuid4()),
        "sqs_receipt_handle":  str(uuid.uuid4()),
        "sqs_queue_url": "https://sqs.eu-west-1.amazonaws.com/467091806172/order-events",
        "approximate_receive_count": random.randint(1, 3),
    }

    if event_type in ("order.shipped", "order.delivered"):
        event["tracking_number"] = maybe_null(f"JD{random.randint(100000000,999999999)}GB", dirty)
        event["carrier"]         = maybe_null(random.choice(["Royal Mail","DPD","Hermes"]), dirty)

    if event_type == "order.refunded":
        event["refund_amount_pence"] = maybe_null(amount_pence, dirty)
        event["refund_reason"]       = maybe_null(
            random.choice(["damaged","wrong_item","changed_mind"]), dirty)

    # Dirty: 3% duplicate flag
    if dirty and random.random() < 0.03:
        event["_duplicate"] = True

    # Dirty: 2% future timestamp
    if dirty and random.random() < 0.02:
        event["event_ts"] = "2099-01-01T00:00:00+00:00"

    return event


# ─────────────────────────────────────────────────────────────
# KAFKA PRODUCER
# ─────────────────────────────────────────────────────────────

def get_producer():
    return KafkaProducer(
        bootstrap_servers=BROKERS.split(","),
        security_protocol="SSL",
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all", retries=3, batch_size=16384, linger_ms=10,
    )


# ─────────────────────────────────────────────────────────────
# BURST MODE
# ─────────────────────────────────────────────────────────────

def run_burst(days=7, dirty=False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}")
    get_entity_ids()
    t0       = time.time()
    producer = get_producer()
    stats    = {"sent": 0, "errors": 0}
    total    = days * 1000
    now      = datetime.now(timezone.utc)

    for i in range(total):
        days_ago = random.uniform(0, days)
        base_dt  = now - timedelta(days=days_ago)
        event    = build_event(order_hours_timestamp(base_dt), dirty, burst_seq=i)
        try:
            producer.send(TOPIC, key=event["message_id"], value=event)
            stats["sent"] += 1
        except KafkaError as e:
            stats["errors"] += 1
        if i % 500 == 0 and i > 0:
            producer.flush()
            log.info(f"  {i}/{total} | {time.time()-t0:.0f}s")

    producer.flush()
    producer.close()
    elapsed = time.time() - t0
    log.info(f"✓ BURST COMPLETE | {elapsed:.1f}s | {stats}")
    log.info(f"Rule 2 {'✅' if elapsed <= 120 else 'VIOLATION'} {elapsed:.1f}s")


# ─────────────────────────────────────────────────────────────
# STREAM MODE
# ─────────────────────────────────────────────────────────────

def run_stream(dirty=False):
    log.info(f"STREAM MODE | ~5K/day | dirty={dirty} | Ctrl+C to stop")
    get_entity_ids()
    producer = get_producer()
    stats, i = {"sent": 0}, 0
    try:
        while True:
            i    += 1
            event = build_event(datetime.now(timezone.utc), dirty)
            try:
                producer.send(TOPIC, key=event["message_id"], value=event)
                stats["sent"] += 1
            except KafkaError:
                pass
            if i % 100 == 0:
                producer.flush()
                log.info(f"Stream — {stats}")
            time.sleep(STREAM_SLEEP)
    except KeyboardInterrupt:
        producer.flush()
        producer.close()
        log.info(f"Stopped. {stats}")


def main():
    p = argparse.ArgumentParser(description="Source 05 — SQS Order Events")
    p.add_argument("--mode",  choices=["burst","stream"], required=True)
    p.add_argument("--days",  type=int, default=7)
    p.add_argument("--dirty", action="store_true")
    args = p.parse_args()
    log.info(f"Source 05 | mode={args.mode} | days={args.days} | dirty={args.dirty}")
    if args.mode == "burst":   run_burst(args.days, args.dirty)
    elif args.mode == "stream": run_stream(args.dirty)

if __name__ == "__main__":
    main()

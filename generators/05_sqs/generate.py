"""
generators/05_sqs/generate.py — Staff DE Journey: Source 05

Simulates SQS order notification events published to Kafka.
In real setup: SQS → Lambda → Kafka. We simulate the Kafka output.

~5K messages/day = order placed, confirmed, shipped, cancelled events.

MUST RUN ON EC2 — MSK only reachable from inside VPC.

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
import logging
import argparse
import uuid
from datetime import datetime, timezone, timedelta

from kafka import KafkaProducer
from kafka.errors import KafkaError
from faker import Faker
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

fake = Faker("en_GB")
Faker.seed(42)

STREAM_SLEEP = 1.0 / (5000 / 86400)  # ~5K/day (Rule 6)
TOPIC        = "order.events"
BROKERS      = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS",
    "b-1.staffdejourneykafka.g6712a.c2.kafka.eu-north-1.amazonaws.com:9094,"
    "b-2.staffdejourneykafka.g6712a.c2.kafka.eu-north-1.amazonaws.com:9094"
)

EVENT_TYPES = [
    "order.placed",     "order.placed",
    "order.confirmed",  "order.confirmed",
    "order.shipped",
    "order.delivered",
    "order.cancelled",
    "order.refunded",
]

def dr(base, dirty, elevated): return elevated if dirty else base
def maybe_null(v, dirty, base=0.05, elev=0.20):
    return None if random.random() < dr(base, dirty, elev) else v

def build_event(event_dt: datetime, dirty: bool = False) -> dict:
    event_type = random.choice(EVENT_TYPES)
    order_id   = random.randint(1, 6751)
    amount     = round(random.uniform(9.99, 999.99), 2)

    event = {
        "message_id":   str(uuid.uuid4()),
        "event_type":   event_type,
        "event_ts":     event_dt.isoformat(),
        "order_id":     maybe_null(order_id, dirty, base=0.01, elev=0.05),
        "customer_id":  maybe_null(random.randint(1, 1035), dirty),
        "order_status": event_type.split(".")[1],
        "amount_gbp":   maybe_null(amount, dirty),
        "currency":     "GBP",
        "channel":      maybe_null(random.choice(["web","mobile","api"]), dirty),
        "source":       "sqs",

        # SQS metadata
        "sqs_message_id":       str(uuid.uuid4()),
        "sqs_receipt_handle":   str(uuid.uuid4()),
        "sqs_queue_url":        "https://sqs.eu-north-1.amazonaws.com/467091806172/order-events",
        "approximate_receive_count": random.randint(1, 3),
    }

    if event_type in ("order.shipped", "order.delivered"):
        event["tracking_number"] = maybe_null(f"JD{random.randint(100000000,999999999)}GB", dirty)
        event["carrier"]         = maybe_null(random.choice(["Royal Mail","DPD","Hermes"]), dirty)

    if event_type == "order.refunded":
        event["refund_amount"] = maybe_null(amount, dirty)
        event["refund_reason"] = maybe_null(
            random.choice(["damaged","wrong_item","changed_mind"]), dirty)

    # Dirty: 0.5% duplicate message (SQS at-least-once delivery)
    if dirty and random.random() < 0.03:
        event["_duplicate"] = True

    if dirty and random.random() < 0.02:
        event["event_ts"] = "2099-01-01T00:00:00+00:00"

    return event

def get_producer():
    return KafkaProducer(
        bootstrap_servers=BROKERS.split(","),
        security_protocol="SSL",
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all", retries=3, batch_size=16384, linger_ms=10,
    )

def run_burst(days=7, dirty=False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}")
    t0 = time.time()
    producer = get_producer()
    stats = {"sent": 0, "errors": 0}
    total = days * 1000  # Sample for burst speed
    now   = datetime.now(timezone.utc)

    for i in range(total):
        days_ago = random.uniform(0, days)
        event    = build_event(now - timedelta(days=days_ago), dirty)
        try:
            producer.send(TOPIC, key=str(event.get("order_id", uuid.uuid4())), value=event)
            stats["sent"] += 1
        except KafkaError as e:
            stats["errors"] += 1
        if i % 500 == 0 and i > 0:
            producer.flush()
            log.info(f"  {i}/{total} | {time.time()-t0:.0f}s")

    producer.flush(); producer.close()
    elapsed = time.time() - t0
    log.info(f"✓ BURST COMPLETE | {elapsed:.1f}s | {stats}")
    log.info(f"Rule 2 {'✅' if elapsed <= 120 else 'VIOLATION'} {elapsed:.1f}s")

def run_stream(dirty=False):
    log.info(f"STREAM MODE | ~5K/day | dirty={dirty} | Ctrl+C to stop")
    producer = get_producer()
    stats, i = {"sent": 0}, 0
    try:
        while True:
            i += 1
            event = build_event(datetime.now(timezone.utc), dirty)
            try:
                producer.send(TOPIC, key=str(event.get("order_id")), value=event)
                stats["sent"] += 1
            except KafkaError as e:
                pass
            if i % 100 == 0:
                producer.flush(); log.info(f"Stream — {stats}")
            time.sleep(STREAM_SLEEP)
    except KeyboardInterrupt:
        producer.flush(); producer.close()
        log.info(f"Stopped. {stats}")

def main():
    p = argparse.ArgumentParser(description="Source 05 — SQS Order Events")
    p.add_argument("--mode",  choices=["burst","stream"], required=True)
    p.add_argument("--days",  type=int, default=7)
    p.add_argument("--dirty", action="store_true")
    args = p.parse_args()
    log.info(f"Source 05 | mode={args.mode} | days={args.days} | dirty={args.dirty}")
    if args.mode == "burst": run_burst(args.days, args.dirty)
    elif args.mode == "stream": run_stream(args.dirty)

if __name__ == "__main__": main()

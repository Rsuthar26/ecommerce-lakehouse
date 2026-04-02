"""
generators/04_kafka_clickstream/generate.py — Staff DE Journey: Source 04

Simulates clickstream events published directly to MSK Kafka.
~500K events/day = ~6 events/sec during peak hours.

MUST RUN ON EC2 — MSK is only reachable from inside the VPC.

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

fake = Faker("en_GB")
Faker.seed(42)

STREAM_SLEEP = 1.0 / 6.0  # ~6 events/sec = ~500K/day (Rule 6)
TOPIC        = "clickstream.events"
PRODUCT_SKUS = [f"SKU-{str(i).zfill(5)}" for i in range(1, 201)]

EVENT_TYPES     = ["page_view","page_view","page_view","product_view","product_view",
                   "search","add_to_cart","view_cart","begin_checkout","purchase"]
PAGES           = ["/","/products","/sale","/new-arrivals","/cart","/checkout","/search"]
TRAFFIC_SOURCES = ["organic","cpc","email","social","direct","referral"]
DEVICES         = ["desktop","mobile","tablet"]
BROWSERS        = ["Chrome","Safari","Firefox","Edge"]

BROKERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS",
    "b-1.staffdejourneykafka.g6712a.c2.kafka.eu-north-1.amazonaws.com:9094,"
    "b-2.staffdejourneykafka.g6712a.c2.kafka.eu-north-1.amazonaws.com:9094"
)


def peak_timestamp(base_dt: datetime) -> datetime:
    peak_windows = [(10,12,0.30),(19,21,0.30),(12,19,0.25),(8,10,0.08),(21,23,0.07)]
    r, cumul, s, e = random.random(), 0.0, 10, 12
    for start, end, w in peak_windows:
        cumul += w
        if r <= cumul:
            s, e = start, end
            break
    return base_dt.replace(hour=random.randint(s,e-1), minute=random.randint(0,59),
                           second=random.randint(0,59), tzinfo=timezone.utc)


def dr(base, dirty, elevated):
    return elevated if dirty else base

def maybe_null(value, dirty, base=0.05, elev=0.20):
    return None if random.random() < dr(base, dirty, elev) else value


def build_event(event_dt: datetime, dirty: bool = False) -> dict:
    event_type = random.choice(EVENT_TYPES)
    session_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"session_{random.randint(1,10000)}"))
    sku        = random.choice(PRODUCT_SKUS)
    price      = round(random.uniform(4.99, 299.99), 2)

    event = {
        "event_id":       str(uuid.uuid4()),
        "event_type":     event_type,
        "event_ts":       event_dt.isoformat(),
        "session_id":     maybe_null(session_id, dirty),
        "user_id":        maybe_null(random.randint(1, 1035), dirty, base=0.30, elev=0.50),
        "anonymous_id":   str(uuid.uuid4()),
        "page":           maybe_null(random.choice(PAGES), dirty),
        "referrer":       maybe_null("https://google.com", dirty),
        "device":         random.choice(DEVICES),
        "browser":        maybe_null(random.choice(BROWSERS), dirty),
        "traffic_source": maybe_null(random.choice(TRAFFIC_SOURCES), dirty),
        "ip":             maybe_null(fake.ipv4(), dirty),
        "country":        maybe_null("GB", dirty),
    }

    if event_type in ("product_view", "add_to_cart"):
        event["product_sku"]   = maybe_null(sku, dirty)
        event["product_price"] = maybe_null(price, dirty)

    if event_type == "search":
        event["search_term"] = maybe_null(
            random.choice(["headphones","keyboard","desk lamp","webcam"]), dirty)

    if event_type == "purchase":
        event["order_id"] = maybe_null(random.randint(1, 6751), dirty)
        event["revenue"]  = maybe_null(price, dirty)
        event["currency"] = "GBP"

    if dirty and random.random() < 0.01:
        event["_malformed"] = True
        event.pop("event_type", None)

    if dirty and random.random() < 0.02:
        event["event_ts"] = "2099-01-01T00:00:00+00:00"

    return event


def get_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BROKERS.split(","),
        security_protocol="SSL",
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        batch_size=16384,
        linger_ms=10,
        compression_type="gzip",
    )


def run_burst(days: int = 7, dirty: bool = False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}")
    t0       = time.time()
    producer = get_producer()
    stats    = {"sent": 0, "errors": 0}
    total    = days * 5000  # 5K/day sample for burst speed
    now      = datetime.now(timezone.utc)

    for i in range(total):
        days_ago = random.uniform(0, days)
        base_dt  = now - timedelta(days=days_ago)
        event    = build_event(peak_timestamp(base_dt), dirty)

        try:
            producer.send(TOPIC, key=event.get("session_id", str(uuid.uuid4())), value=event)
            stats["sent"] += 1
        except KafkaError as e:
            log.error(f"Send failed: {e}")
            stats["errors"] += 1

        if i % 1000 == 0 and i > 0:
            producer.flush()
            log.info(f"  {i}/{total} | {time.time()-t0:.0f}s")

    producer.flush()
    producer.close()
    elapsed = time.time() - t0
    log.info(f"✓ BURST COMPLETE | {elapsed:.1f}s | {stats}")
    if elapsed > 120:
        log.warning(f"Rule 2 VIOLATION: {elapsed:.0f}s > 120s")
    else:
        log.info(f"Rule 2 ✅ {elapsed:.1f}s < 120s")


def run_stream(dirty: bool = False):
    log.info(f"STREAM MODE | ~6 events/sec | dirty={dirty} | Ctrl+C to stop")
    producer = get_producer()
    stats, i = {"sent": 0, "errors": 0}, 0
    try:
        while True:
            i    += 1
            event = build_event(datetime.now(timezone.utc), dirty)
            try:
                producer.send(TOPIC, key=event.get("session_id"), value=event)
                stats["sent"] += 1
            except KafkaError as e:
                log.error(f"Send failed: {e}")
                stats["errors"] += 1
            if i % 100 == 0:
                producer.flush()
                log.info(f"Stream — {stats}")
            time.sleep(STREAM_SLEEP)
    except KeyboardInterrupt:
        producer.flush()
        producer.close()
        log.info(f"Stopped. {stats}")


def main():
    p = argparse.ArgumentParser(description="Source 04 — Kafka Clickstream")
    p.add_argument("--mode",  choices=["burst","stream"], required=True)
    p.add_argument("--days",  type=int, default=7)
    p.add_argument("--dirty", action="store_true")
    args = p.parse_args()
    log.info(f"Source 04 | mode={args.mode} | days={args.days} | dirty={args.dirty}")
    if args.mode == "burst":
        run_burst(args.days, args.dirty)
    elif args.mode == "stream":
        run_stream(args.dirty)

if __name__ == "__main__":
    main()

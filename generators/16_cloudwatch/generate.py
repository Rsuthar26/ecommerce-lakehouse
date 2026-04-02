"""
generators/16_cloudwatch/generate.py — Staff DE Journey: Source 16

Simulates CloudWatch application logs published to Kafka via Kinesis.
App errors, access logs, Lambda execution logs.

MUST RUN ON EC2 — MSK only reachable from inside VPC.

Usage:
    python generate.py --mode burst
    python generate.py --mode burst --dirty
    python generate.py --mode stream
"""

import os
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

STREAM_SLEEP = 1.0 / (50000 / 86400)  # ~50K log lines/day (Rule 6)
TOPIC        = "app.logs"
BROKERS      = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS",
    "b-1.staffdejourneykafka.g6712a.c2.kafka.eu-north-1.amazonaws.com:9094,"
    "b-2.staffdejourneykafka.g6712a.c2.kafka.eu-north-1.amazonaws.com:9094"
)

LOG_LEVELS  = ["INFO","INFO","INFO","INFO","WARN","ERROR","DEBUG"]
LOG_SOURCES = ["api-gateway","order-service","payment-service",
               "inventory-service","lambda-image-processor","lambda-sqs-consumer"]

ERROR_MESSAGES = [
    "NullPointerException in OrderProcessor.process()",
    "Connection timeout to RDS after 30000ms",
    "Payment gateway returned 503 Service Unavailable",
    "S3 PutObject failed: Access Denied",
    "DynamoDB ProvisionedThroughputExceededException",
    "Kafka producer failed to send after 3 retries",
    "MongoDB connection pool exhausted",
    "JWT token validation failed: token expired",
]

ACCESS_LOG_PATHS = [
    "GET /api/v1/orders",
    "POST /api/v1/orders",
    "GET /api/v1/products",
    "PUT /api/v1/orders/{id}/status",
    "GET /api/v1/customers/{id}",
    "POST /api/v1/payments",
    "GET /health",
]

STATUS_CODES = [200,200,200,200,201,400,401,403,404,429,500,503]

def dr(base, dirty, elevated): return elevated if dirty else base
def maybe_null(v, dirty, base=0.05, elev=0.20):
    return None if random.random() < dr(base, dirty, elev) else v

def build_log_event(event_dt: datetime, dirty: bool = False) -> dict:
    source    = random.choice(LOG_SOURCES)
    log_level = random.choice(LOG_LEVELS)

    if log_level == "ERROR":
        message = random.choice(ERROR_MESSAGES)
    elif source == "api-gateway":
        path    = random.choice(ACCESS_LOG_PATHS)
        status  = random.choice(STATUS_CODES)
        latency = random.randint(5, 5000)
        message = f"{path} {status} {latency}ms"
    else:
        message = f"Processing {random.choice(['order','payment','inventory'])} event {str(uuid.uuid4())[:8]}"

    event = {
        "log_id":          str(uuid.uuid4()),
        "timestamp":       event_dt.isoformat(),
        "timestamp_ms":    int(event_dt.timestamp() * 1000),
        "log_level":       maybe_null(log_level, dirty),
        "source":          maybe_null(source, dirty),
        "message":         maybe_null(message, dirty),
        "request_id":      str(uuid.uuid4()),
        "trace_id":        maybe_null(str(uuid.uuid4()), dirty),
        "aws_region":      "eu-north-1",
        "log_group":       f"/aws/{source}",
        "log_stream":      f"{source}-{event_dt.strftime('%Y/%m/%d')}",
        "account_id":      "467091806172",
    }

    if log_level == "ERROR":
        event["stack_trace"] = maybe_null(
            f"java.lang.{random.choice(['NullPointerException','RuntimeException'])}\n"
            f"\tat com.ecommerce.{source}.process(Service.java:42)\n"
            f"\tat com.ecommerce.handler.handle(Handler.java:17)",
            dirty)
        event["error_code"] = maybe_null(
            random.choice(["ERR_001","ERR_002","ERR_003","TIMEOUT","CONN_REFUSED"]),
            dirty)

    if source == "api-gateway":
        event["http_method"]  = random.choice(["GET","POST","PUT","DELETE"])
        event["http_status"]  = maybe_null(random.choice(STATUS_CODES), dirty)
        event["latency_ms"]   = maybe_null(random.randint(5, 5000), dirty)
        event["user_agent"]   = maybe_null("Mozilla/5.0", dirty)
        event["source_ip"]    = maybe_null(fake.ipv4(), dirty)

    if dirty and random.random() < 0.02:
        event["timestamp"] = "2099-01-01T00:00:00+00:00"

    # Dirty: 1% completely garbled log line
    if dirty and random.random() < 0.01:
        event["message"] = "�" * random.randint(5, 50)

    return event

def get_producer():
    return KafkaProducer(
        bootstrap_servers=BROKERS.split(","),
        security_protocol="SSL",
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks=1, retries=3, batch_size=32768, linger_ms=50,
        compression_type="gzip",
    )

def run_burst(days=7, dirty=False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}")
    t0 = time.time()
    producer = get_producer()
    stats = {"sent": 0, "errors": 0}
    total = days * 5000  # Sample for burst speed
    now   = datetime.now(timezone.utc)

    for i in range(total):
        days_ago = random.uniform(0, days)
        event    = build_log_event(now - timedelta(days=days_ago), dirty)
        try:
            producer.send(TOPIC, key=event.get("source"), value=event)
            stats["sent"] += 1
        except KafkaError:
            stats["errors"] += 1
        if i % 1000 == 0 and i > 0:
            producer.flush()
            log.info(f"  {i}/{total} | {time.time()-t0:.0f}s")

    producer.flush(); producer.close()
    elapsed = time.time() - t0
    log.info(f"✓ BURST COMPLETE | {elapsed:.1f}s | {stats}")
    log.info(f"Rule 2 {'✅' if elapsed <= 120 else 'VIOLATION'} {elapsed:.1f}s")

def run_stream(dirty=False):
    log.info(f"STREAM MODE | ~50K/day | dirty={dirty} | Ctrl+C to stop")
    producer = get_producer()
    stats, i = {"sent": 0}, 0
    try:
        while True:
            i += 1
            event = build_log_event(datetime.now(timezone.utc), dirty)
            try:
                producer.send(TOPIC, key=event.get("source"), value=event)
                stats["sent"] += 1
            except KafkaError:
                pass
            if i % 500 == 0:
                producer.flush(); log.info(f"Stream — {stats}")
            time.sleep(STREAM_SLEEP)
    except KeyboardInterrupt:
        producer.flush(); producer.close()
        log.info(f"Stopped. {stats}")

def main():
    p = argparse.ArgumentParser(description="Source 16 — CloudWatch Logs")
    p.add_argument("--mode",  choices=["burst","stream"], required=True)
    p.add_argument("--days",  type=int, default=7)
    p.add_argument("--dirty", action="store_true")
    args = p.parse_args()
    log.info(f"Source 16 | mode={args.mode} | days={args.days} | dirty={args.dirty}")
    if args.mode == "burst": run_burst(args.days, args.dirty)
    elif args.mode == "stream": run_stream(args.dirty)

if __name__ == "__main__": main()

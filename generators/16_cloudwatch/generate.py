"""
generators/16_cloudwatch/generate.py — Staff DE Journey: Source 16

Simulates CloudWatch application logs published to Kafka via Kinesis.
~50K log lines/day. App errors, access logs, Lambda execution logs.

MUST RUN ON EC2 — MSK only reachable from inside VPC.

Fix applied: Rule 5 — deterministic log_id in burst mode.
Rule 11: N/A — log lines have no customer/order/product refs.
"""

import os, sys, json, time, random, hashlib, logging, argparse, uuid
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError
from faker import Faker
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)
fake = Faker("en_GB"); Faker.seed(42)

STREAM_SLEEP = 86400 / 50000   # Rule 6 + Rule 13
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
    "Kafka producer failed to send after 3 retries",
    "MongoDB connection pool exhausted",
    "JWT token validation failed: token expired",
]
ACCESS_LOG_PATHS = [
    "GET /api/v1/orders","POST /api/v1/orders","GET /api/v1/products",
    "PUT /api/v1/orders/{id}/status","POST /api/v1/payments","GET /health",
]
STATUS_CODES = [200,200,200,200,201,400,401,403,404,429,500,503]

def dr(base, dirty, elevated): return elevated if dirty else base
def maybe_null(v, dirty, base=0.05, elev=0.20):
    return None if random.random() < dr(base, dirty, elev) else v

def build_log_event(event_dt: datetime, dirty: bool = False,
                    burst_seq: int = None) -> dict:
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

    # Rule 5: deterministic log_id in burst mode
    if burst_seq is not None:
        log_id = hashlib.md5(f"16:{burst_seq}:{event_dt.isoformat()}:{source}".encode()).hexdigest()
    else:
        log_id = str(uuid.uuid4())

    ts = event_dt.isoformat()
    if dirty and random.random() < 0.02:
        ts = "2099-01-01T00:00:00+00:00"

    event = {
        "log_id":        log_id,
        "timestamp":     ts,
        "timestamp_ms":  int(event_dt.timestamp() * 1000),
        "log_level":     maybe_null(log_level, dirty),
        "source":        maybe_null(source, dirty),
        "message":       maybe_null(message, dirty),
        "request_id":    str(uuid.uuid4()),
        "trace_id":      maybe_null(str(uuid.uuid4()), dirty),
        "aws_region":    "eu-north-1",
        "log_group":     f"/aws/{source}",
        "log_stream":    f"{source}-{event_dt.strftime('%Y/%m/%d')}",
        "account_id":    "467091806172",
    }

    if log_level == "ERROR":
        event["stack_trace"] = maybe_null(
            f"java.lang.RuntimeException\n\tat com.ecommerce.{source}.process(Service.java:42)", dirty)
        event["error_code"]  = maybe_null(
            random.choice(["ERR_001","ERR_002","TIMEOUT","CONN_REFUSED"]), dirty)

    if source == "api-gateway":
        event["http_method"] = random.choice(["GET","POST","PUT","DELETE"])
        event["http_status"] = maybe_null(random.choice(STATUS_CODES), dirty)
        event["latency_ms"]  = maybe_null(random.randint(5, 5000), dirty)
        event["source_ip"]   = maybe_null(fake.ipv4(), dirty)

    # Dirty: 1% garbled log line
    if dirty and random.random() < 0.01:
        event["message"] = "?" * random.randint(5, 50)

    return event

def get_producer():
    return KafkaProducer(
        bootstrap_servers=BROKERS.split(","), security_protocol="SSL",
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks=1, retries=3, batch_size=32768, linger_ms=50, compression_type="gzip",
    )

def run_burst(days=7, dirty=False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}")
    t0, producer = time.time(), get_producer()
    stats, total, now = {"sent":0,"errors":0}, days * 5000, datetime.now(timezone.utc)
    for i in range(total):
        days_ago = random.uniform(0, days)
        event    = build_log_event(now - timedelta(days=days_ago), dirty, burst_seq=i)
        try:
            producer.send(TOPIC, key=event["log_id"], value=event); stats["sent"] += 1
        except KafkaError: stats["errors"] += 1
        if i % 1000 == 0 and i > 0:
            producer.flush(); log.info(f"  {i}/{total} | {time.time()-t0:.0f}s")
    producer.flush(); producer.close()
    elapsed = time.time() - t0
    log.info(f"✓ BURST COMPLETE | {elapsed:.1f}s | {stats}")
    log.info(f"Rule 2 {'✅' if elapsed <= 120 else 'VIOLATION'} {elapsed:.1f}s")

def run_stream(dirty=False):
    log.info(f"STREAM MODE | ~50K/day | dirty={dirty} | Ctrl+C to stop")
    producer, stats, i = get_producer(), {"sent":0}, 0
    try:
        while True:
            i += 1
            event = build_log_event(datetime.now(timezone.utc), dirty)
            try:
                producer.send(TOPIC, key=event["log_id"], value=event); stats["sent"] += 1
            except KafkaError: pass
            if i % 500 == 0:
                producer.flush(); log.info(f"Stream — {stats}")
            time.sleep(STREAM_SLEEP)
    except KeyboardInterrupt:
        producer.flush(); producer.close(); log.info(f"Stopped. {stats}")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["burst","stream"], required=True)
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--dirty", action="store_true")
    args = p.parse_args()
    log.info(f"Source 16 | mode={args.mode} | days={args.days} | dirty={args.dirty}")
    if args.mode == "burst": run_burst(args.days, args.dirty)
    else: run_stream(args.dirty)

if __name__ == "__main__": main()

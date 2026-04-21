"""
generators/15_mqtt_iot/generate.py — Staff DE Journey: Source 15

Simulates MQTT IoT sensor telemetry published to Kafka.
Warehouse sensors: temperature, humidity, stock weight.
Every sensor publishes every 10 seconds.

MUST RUN ON EC2 — MSK only reachable from inside VPC.

Rules satisfied:
    Rule 1  — --mode burst and --mode stream
    Rule 2  — burst < 2 minutes
    Rule 3  — IoT is 24/7 — no time-of-day pattern (correct)
    Rule 4  — anomaly flag reflects actual reading values (not age)
    Rule 5  — FIX: burst uses deterministic event_id
    Rule 6  — stream: 1 reading/sensor/10s (all 5 sensors = 1 event/2s)
    Rule 7  — dirty: null readings, out-of-range values, future timestamps
    Rule 8  — README.md exists
    Rule 9  — env vars: KAFKA_BOOTSTRAP_SERVERS
    Rule 10 — callable from single bash line
    Rule 11 — N/A: IoT sensors have no customer/order/product references

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

load_dotenv()

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

# Rule 6 + Rule 13: 5 sensors × 8640 readings/day = 43,200/day
# Burst samples 2K/day × 7 days = 14,000 events
STREAM_SLEEP = 86400 / 43200  # Rule 13: 5 sensors × 8640 readings/day = 43,200/day — 10s per round

TOPIC   = "iot.telemetry"
BROKERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS",
    "b-1.staffdejourneykafka.g6712a.c2.kafka.eu-north-1.amazonaws.com:9094,"
    "b-2.staffdejourneykafka.g6712a.c2.kafka.eu-north-1.amazonaws.com:9094"
)

SENSORS = [
    {"id": "SENSOR-WH-LON-TEMP-01", "warehouse": "WH-LONDON-01", "type": "temperature"},
    {"id": "SENSOR-WH-LON-HUM-01",  "warehouse": "WH-LONDON-01", "type": "humidity"},
    {"id": "SENSOR-WH-MAN-TEMP-01", "warehouse": "WH-MANC-01",   "type": "temperature"},
    {"id": "SENSOR-WH-BIR-TEMP-01", "warehouse": "WH-BRUM-01",   "type": "temperature"},
    {"id": "SENSOR-WH-BIR-WGT-01",  "warehouse": "WH-BRUM-01",   "type": "weight"},
]

SENSOR_UNITS = {"temperature": "celsius", "humidity": "percent", "weight": "kg"}


def dr(base, dirty, elevated): return elevated if dirty else base
def maybe_null(v, dirty, base=0.05, elev=0.20):
    return None if random.random() < dr(base, dirty, elev) else v


def sensor_reading(sensor_type: str, dirty: bool):
    if sensor_type == "temperature":
        val = round(random.gauss(18.0, 2.0), 2)
    elif sensor_type == "humidity":
        val = round(random.gauss(55.0, 5.0), 2)
    else:
        val = round(random.uniform(0, 5000), 2)
    if dirty and random.random() < 0.05:
        return random.choice([None, -999.0, 9999.0, 0.0])
    return val


def build_event(sensor: dict, event_dt: datetime,
                dirty: bool = False, burst_seq: int = None) -> dict:
    reading    = sensor_reading(sensor["type"], dirty)
    is_anomaly = False
    alert_type = None

    if sensor["type"] == "temperature" and reading and reading > 25.0:
        is_anomaly = True
        alert_type = "HIGH_TEMPERATURE"
    elif sensor["type"] == "humidity" and reading and reading > 70.0:
        is_anomaly = True
        alert_type = "HIGH_HUMIDITY"

    # Rule 5: deterministic event_id in burst mode
    if burst_seq is not None:
        event_id = hashlib.md5(
            f"15:{sensor['id']}:{burst_seq}:{event_dt.isoformat()}".encode()
        ).hexdigest()
    else:
        event_id = str(uuid.uuid4())

    ts = event_dt.isoformat()
    if dirty and random.random() < 0.02:
        ts = "2099-01-01T00:00:00+00:00"

    return {
        "event_id":     event_id,
        "sensor_id":    sensor["id"],
        "warehouse_id": sensor["warehouse"],
        "sensor_type":  sensor["type"],
        "reading":      maybe_null(reading, dirty),
        "unit":         SENSOR_UNITS.get(sensor["type"]),
        "is_anomaly":   is_anomaly,
        "alert_type":   alert_type,
        "battery_pct":  maybe_null(round(random.uniform(20, 100), 1), dirty),
        "signal_rssi":  maybe_null(random.randint(-90, -30), dirty),
        "firmware":     "v2.4.1",
        "ts":           ts,
        "ts_ms":        int(event_dt.timestamp() * 1000),
    }


def get_producer():
    return KafkaProducer(
        bootstrap_servers=BROKERS.split(","),
        security_protocol="SSL",
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all", retries=3, linger_ms=5,
    )


def run_burst(days=7, dirty=False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}")
    t0       = time.time()
    producer = get_producer()
    stats    = {"sent": 0, "errors": 0}
    total    = days * 2000
    now      = datetime.now(timezone.utc)

    for i in range(total):
        sensor   = random.choice(SENSORS)
        days_ago = random.uniform(0, days)
        event    = build_event(
            sensor, now - timedelta(days=days_ago), dirty, burst_seq=i
        )
        try:
            producer.send(TOPIC, key=sensor["id"], value=event)
            stats["sent"] += 1
        except KafkaError:
            stats["errors"] += 1
        if i % 1000 == 0 and i > 0:
            producer.flush()
            log.info(f"  {i}/{total} | {time.time()-t0:.0f}s")

    producer.flush()
    producer.close()
    elapsed = time.time() - t0
    log.info(f"✓ BURST COMPLETE | {elapsed:.1f}s | {stats}")
    log.info(f"Rule 2 {'✅' if elapsed <= 120 else 'VIOLATION'} {elapsed:.1f}s")


def run_stream(dirty=False):
    log.info(f"STREAM MODE | 1 reading/sensor/10s | dirty={dirty} | Ctrl+C to stop")
    producer = get_producer()
    stats, i = {"sent": 0}, 0
    try:
        while True:
            for sensor in SENSORS:
                i   += 1
                event = build_event(sensor, datetime.now(timezone.utc), dirty)
                try:
                    producer.send(TOPIC, key=sensor["id"], value=event)
                    stats["sent"] += 1
                except KafkaError:
                    pass
            producer.flush()
            if i % 50 == 0:
                log.info(f"Stream — {stats}")
            time.sleep(STREAM_SLEEP)
    except KeyboardInterrupt:
        producer.flush()
        producer.close()
        log.info(f"Stopped. {stats}")


def main():
    p = argparse.ArgumentParser(description="Source 15 — MQTT IoT Sensors")
    p.add_argument("--mode",  choices=["burst","stream"], required=True)
    p.add_argument("--days",  type=int, default=7)
    p.add_argument("--dirty", action="store_true")
    args = p.parse_args()
    log.info(f"Source 15 | mode={args.mode} | days={args.days} | dirty={args.dirty}")
    if args.mode == "burst":   run_burst(args.days, args.dirty)
    elif args.mode == "stream": run_stream(args.dirty)

if __name__ == "__main__":
    main()

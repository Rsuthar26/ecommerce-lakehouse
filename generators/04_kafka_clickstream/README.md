# Source 04 — MSK Kafka Clickstream

## What It Simulates
User clickstream events: page views, product views, add-to-cart, purchases. ~500K events/day = ~6 events/sec. Must run on EC2 — MSK is VPC-only.

## Event Schema
```json
{
  "event_id": "deterministic-md5-in-burst",
  "event_type": "page_view | product_view | add_to_cart | purchase | search",
  "event_ts": "ISO timestamp",
  "session_id": "uuid5",
  "user_id": "real customer_id from Postgres",
  "anonymous_id": "uuid4",
  "page": "/products",
  "device": "desktop | mobile | tablet",
  "traffic_source": "organic | cpc | email | social"
}
```

## Run Commands
```bash
python generators/04_kafka_clickstream/generate.py --mode burst
python generators/04_kafka_clickstream/generate.py --mode burst --dirty
python generators/04_kafka_clickstream/generate.py --mode stream
```

## Prerequisites
```bash
pip install kafka-python faker python-dotenv
```

## Environment Variables
```
KAFKA_BOOTSTRAP_SERVERS=b-1.staffdejourneykafka...amazonaws.com:9094,...
PG_HOST, PG_PORT, PG_DBNAME, PG_USER, PG_PASSWORD
```

## How to Verify
```bash
kafka-console-consumer.sh --bootstrap-server $BROKERS --topic clickstream.events --max-messages 5
```

## Notes
- Must run on EC2 — MSK brokers are not publicly accessible
- Burst mode uses deterministic event_id (Rule 12) — safe to run twice
- user_id is null ~30% of the time (anonymous sessions) — this is realistic

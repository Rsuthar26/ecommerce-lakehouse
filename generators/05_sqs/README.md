# Source 05 — AWS SQS Order Events

## What It Simulates
Order lifecycle notifications published to Kafka. ~5K messages/day. Simulates the real pattern: SQS → Lambda → Kafka. Event type reflects order age — old events cannot have status "placed". Must run on EC2.

## Event Schema
```json
{
  "message_id": "deterministic-md5-in-burst",
  "event_type": "order.placed | order.confirmed | order.shipped | order.delivered | order.cancelled | order.refunded",
  "event_ts": "ISO timestamp during business hours",
  "order_id": "real order_id from Postgres",
  "customer_id": "real customer_id from Postgres",
  "amount_pence": 4999,
  "currency": "GBP",
  "tracking_number": "JD123456789GB (shipped/delivered only)",
  "sqs_queue_url": "https://sqs.eu-west-1.amazonaws.com/467091806172/order-events",
  "approximate_receive_count": 1
}
```

## Run Commands
```bash
python generators/05_sqs/generate.py --mode burst
python generators/05_sqs/generate.py --mode burst --dirty
python generators/05_sqs/generate.py --mode stream
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
kafka-console-consumer.sh --bootstrap-server $BROKERS --topic order.events --max-messages 10
```

## Notes
- Must run on EC2
- `approximate_receive_count > 1` simulates SQS at-least-once delivery — Silver must dedup on message_id

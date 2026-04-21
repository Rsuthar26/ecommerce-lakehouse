# Source 02 — Debezium CDC

## What It Simulates
Change Data Capture from RDS PostgreSQL. Debezium reads the PostgreSQL WAL (Write-Ahead Log) and converts every INSERT, UPDATE, and DELETE into a real-time Kafka event. There is no `generate.py` — Source 01 generates the database activity and Debezium captures it automatically.

## What a CDC Event Looks Like

```json
{
  "op": "u",
  "before": { "order_id": 5001, "order_status": "pending" },
  "after":  { "order_id": 5001, "order_status": "shipped" },
  "source": { "db": "ecommerce", "table": "orders", "lsn": 12345678, "ts_ms": 1743428580000 }
}
```

| Field | Values | Meaning |
|---|---|---|
| op | c / u / d / r | create / update / delete / snapshot read |
| before | row state | null for inserts |
| after | row state | null for deletes |
| source.lsn | integer | dedup key in Silver |
| source.ts_ms | epoch ms | commit timestamp |

## Kafka Topics Created

```
postgres.public.orders
postgres.public.customers
postgres.public.payments
postgres.public.inventory
postgres.public.order_items
```

## Run Commands

```bash
# Step 1 — SSH to EC2
ssh -i ~/.ssh/staff-de-journey-key.pem ec2-user@56.228.75.74

# Step 2 — Start Kafka Connect
export JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto
/opt/debezium/kafka/bin/connect-distributed.sh \
  /opt/debezium/connect-distributed.properties > /tmp/connect.log 2>&1 &

# Step 3 — Wait 15 seconds, then register connector
curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @/opt/debezium/connector.json
```

## How to Verify

```bash
# Connector status — expect "state": "RUNNING"
curl -s http://localhost:8083/connectors/postgres-cdc-connector/status | python3 -m json.tool

# Watch live events
/opt/debezium/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server <MSK_BOOTSTRAP>:9092 \
  --topic postgres.public.orders --from-beginning
```

## Failure Modes

| Failure | Cause | Fix |
|---|---|---|
| Connector FAILED | Schema change in Postgres | Update config, restart connector |
| WAL filling disk | Slot not consumed | `SELECT pg_drop_replication_slot('debezium_slot')` |
| Duplicate events in Bronze | Kafka at-least-once | Silver deduplicates on `source.lsn` |
| No events after restart | Normal — resumes from last LSN | Nothing to do |

## Notes
- Always delete connector before stopping EC2: `curl -X DELETE http://localhost:8083/connectors/postgres-cdc-connector`
- MSK must be running before starting Debezium
- EC2 instance: i-042dbb1daaa085576, permanent IP 56.228.75.74

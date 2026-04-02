# Source 02 — Debezium CDC

## What it is

Debezium is a Change Data Capture (CDC) connector. It reads PostgreSQL's internal
change log (the WAL — Write-Ahead Log) and converts every INSERT, UPDATE, and DELETE
into a real-time event on Kafka. It does this without touching the application and
without querying the database.

This is not a generator. There is no `generate.py` here.

Source 01 (RDS PostgreSQL) produces the database activity. Debezium captures it
automatically. The two sources work together — Source 01 is the origin, Source 02
is the listener.

---

## In the real world

In a real company, Debezium runs as a Kafka Connect connector on a dedicated server
or container. It holds an open replication slot on PostgreSQL and streams every change
to a Kafka topic the moment it is committed. Data engineers never write to the database
directly — they consume the CDC stream.

Here we run Debezium on an EC2 instance pointed at the RDS PostgreSQL instance from
Source 01. The connector config is in `connector.json`.

---

## What a CDC event looks like

Every database change arrives as a JSON envelope with this structure:

```json
{
  "op": "u",
  "before": {
    "order_id": 5001,
    "status": "pending",
    "updated_at": "2026-03-31T10:00:00Z"
  },
  "after": {
    "order_id": 5001,
    "status": "shipped",
    "updated_at": "2026-03-31T14:23:00Z"
  },
  "source": {
    "db": "ecommerce",
    "table": "orders",
    "lsn": 12345678,
    "ts_ms": 1743428580000
  }
}
```

| Field | Meaning |
|---|---|
| `op` | c = insert, u = update, d = delete, r = snapshot read |
| `before` | Row values before the change. Null for inserts. |
| `after` | Row values after the change. Null for deletes. |
| `source.lsn` | Log Sequence Number — used for deduplication in Silver |
| `source.ts_ms` | Commit timestamp in milliseconds |

---

## Kafka topics created

One topic per table:

```
postgres.public.orders
postgres.public.customers
postgres.public.payments
postgres.public.inventory
postgres.public.order_items
```

---

## How to deploy

### Prerequisites
- Source 01 (RDS PostgreSQL) must be running
- MSK cluster must be running
- EC2 instance with Kafka Connect installed (provisioned by Terraform)

### Step 1 — SSH into EC2

```bash
ssh -i ~/.ssh/staff-de-journey-key.pem ec2-user@<EC2_PUBLIC_IP>
```

### Step 2 — Set environment variables

```bash
export DB_HOST=<RDS_ENDPOINT>
export DB_USER=<DB_USER>
export DB_PASSWORD=<DB_PASSWORD>
export DB_NAME=ecommerce
```

### Step 3 — Start Kafka Connect

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto
/opt/debezium/kafka/bin/connect-distributed.sh \
  /opt/debezium/connect-distributed.properties \
  > /tmp/connect.log 2>&1 &
```

Wait 15 seconds for Connect to start, then:

### Step 4 — Register the connector

```bash
curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @/opt/debezium/connector.json
```

---

## How to verify it is working

### Check connector status
```bash
curl -s http://localhost:8083/connectors/postgres-cdc-connector/status \
  | python3 -m json.tool
```

Expected: `"state": "RUNNING"`

### Check Kafka topics exist
```bash
/opt/debezium/kafka/bin/kafka-topics.sh \
  --bootstrap-server <MSK_BOOTSTRAP>:9092 \
  --list | grep postgres
```

### Watch live CDC events
```bash
/opt/debezium/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server <MSK_BOOTSTRAP>:9092 \
  --topic postgres.public.orders \
  --from-beginning
```

Insert a row in Source 01 and watch it appear here within milliseconds.

---

## How to stop cleanly

```bash
# Delete the connector
curl -s -X DELETE http://localhost:8083/connectors/postgres-cdc-connector

# Stop Kafka Connect
pkill -f connect-distributed

# Stop EC2 instance (saves cost)
aws ec2 stop-instances --instance-ids <INSTANCE_ID> --region eu-north-1
```

Always delete the connector before stopping. If Kafka Connect is killed without
deleting the connector, the replication slot stays open on PostgreSQL and the WAL
accumulates on disk.

---

## Failure modes

| Failure | Cause | Fix |
|---|---|---|
| Connector stuck in FAILED state | Schema change in PostgreSQL | Update connector config, restart |
| WAL filling disk on RDS | Replication slot not consumed | Delete slot: `SELECT pg_drop_replication_slot('debezium_slot')` |
| Duplicate events in Bronze | Kafka at-least-once delivery | Silver deduplicates by `source.lsn` |
| Topics not created | MSK ACL blocking | Check MSK IAM policy allows topic creation |
| No events after restart | Connector restarted from last LSN | Normal — Debezium resumes from checkpoint |

---

## Bronze table

`bronze.cdc_raw`

Raw Debezium envelopes stored exactly as received — one row per database change event.
Silver unpacks the `before` and `after` fields and applies the `op` type to maintain
a deduplicated current-state table.

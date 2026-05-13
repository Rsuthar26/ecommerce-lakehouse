#!/bin/bash
set -e
exec > /var/log/kafka-connect-setup.log 2>&1
echo "=== START setup_kafka_connect.sh $(date) ==="

MSK_CLUSTER_ARN=$1
if [ -z "$MSK_CLUSTER_ARN" ]; then
  echo "ERROR: MSK_CLUSTER_ARN not provided"
  exit 1
fi

# ── 1. System deps ────────────────────────────────────────────
yum update -y
yum install -y java-17-amazon-corretto-headless git python3-pip

# ── 2. Python deps ────────────────────────────────────────────
pip3 install kafka-python boto3 psycopg2-binary pymongo requests faker python-dotenv

# ── 3. Clone repo ─────────────────────────────────────────────
cd /home/ec2-user
git clone https://github.com/Rsuthar26/ecommerce-lakehouse.git
chown -R ec2-user:ec2-user ecommerce-lakehouse
cd ecommerce-lakehouse

# ── 4. Confluent 7.6.0 ───────────────────────────────────────
curl -O https://packages.confluent.io/archive/7.6/confluent-community-7.6.0.tar.gz
tar -xzf confluent-community-7.6.0.tar.gz
rm confluent-community-7.6.0.tar.gz

# ── 5. confluent-hub client ───────────────────────────────────
curl -L -O https://client.hub.confluent.io/confluent-hub-client-latest.tar.gz
mkdir -p /home/ec2-user/confluent-hub-client
tar -xzf confluent-hub-client-latest.tar.gz -C /home/ec2-user/confluent-hub-client
rm confluent-hub-client-latest.tar.gz

# ── 6. S3 Sink + Debezium connectors ─────────────────────────
/home/ec2-user/confluent-hub-client/bin/confluent-hub install confluentinc/kafka-connect-s3:latest \
  --component-dir /home/ec2-user/ecommerce-lakehouse/confluent-7.6.0/share/java \
  --worker-configs /home/ec2-user/ecommerce-lakehouse/confluent-7.6.0/etc/kafka/connect-distributed.properties \
  --no-prompt

/home/ec2-user/confluent-hub-client/bin/confluent-hub install debezium/debezium-connector-postgresql:latest \
  --component-dir /home/ec2-user/ecommerce-lakehouse/confluent-7.6.0/share/java \
  --worker-configs /home/ec2-user/ecommerce-lakehouse/confluent-7.6.0/etc/kafka/connect-distributed.properties \
  --no-prompt

# ── 7. Resolve MSK brokers dynamically ───────────────────────
BROKERS=$(aws kafka get-bootstrap-brokers \
  --cluster-arn "$MSK_CLUSTER_ARN" \
  --region eu-west-1 \
  --query 'BootstrapBrokerStringSaslScram' \
  --output text)
echo "Brokers resolved: $BROKERS"

# ── 8. Write connect-distributed.properties ──────────────────
cat > /tmp/connect-distributed.properties << EOF
bootstrap.servers=${BROKERS}
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="kafka-admin" password="KafkaAdmin2026!";

group.id=kafka-connect-group
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.storage.StringConverter

offset.storage.topic=connect-offsets
offset.storage.replication.factor=1
config.storage.topic=connect-configs
config.storage.replication.factor=1
status.storage.topic=connect-status
status.storage.replication.factor=1

plugin.path=/home/ec2-user/ecommerce-lakehouse/confluent-7.6.0/share/java

producer.security.protocol=SASL_SSL
producer.sasl.mechanism=SCRAM-SHA-512
producer.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="kafka-admin" password="KafkaAdmin2026!";
consumer.security.protocol=SASL_SSL
consumer.sasl.mechanism=SCRAM-SHA-512
consumer.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="kafka-admin" password="KafkaAdmin2026!";
EOF

# ── 9. Kafka Connect as systemd service ───────────────────────
cat > /etc/systemd/system/kafka-connect.service << EOF
[Unit]
Description=Kafka Connect Distributed
After=network.target

[Service]
Type=simple
User=ec2-user
Environment="KAFKA_HEAP_OPTS=-Xmx512m -Xms256m"
ExecStart=/home/ec2-user/ecommerce-lakehouse/confluent-7.6.0/bin/connect-distributed /tmp/connect-distributed.properties
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable kafka-connect
systemctl start kafka-connect

# ── 10. Wait for Kafka Connect REST API ──────────────────────
echo "Waiting for Kafka Connect..."
for i in $(seq 1 30); do
  if curl -s http://localhost:8083/ | grep -q "version"; then
    echo "Kafka Connect ready after $i attempts"
    break
  fi
  echo "Attempt $i - waiting 10s..."
  sleep 10
done

# ── 11. Create S3 Sink connectors ────────────────────────────
BUCKET="ecommerce-lakehouse-467091806172-raw-01"

create_connector() {
  local NAME=$1
  local TOPICS=$2
  local TOPICS_DIR=$3

  curl -s -X POST http://localhost:8083/connectors \
    -H "Content-Type: application/json" \
    -d "{
      \"name\": \"${NAME}\",
      \"config\": {
        \"connector.class\": \"io.confluent.connect.s3.S3SinkConnector\",
        \"tasks.max\": \"1\",
        \"topics\": \"${TOPICS}\",
        \"s3.region\": \"eu-west-1\",
        \"s3.bucket.name\": \"${BUCKET}\",
        \"s3.part.size\": \"5242880\",
        \"topics.dir\": \"${TOPICS_DIR}\",
        \"flush.size\": \"1000\",
        \"rotate.interval.ms\": \"60000\",
        \"storage.class\": \"io.confluent.connect.s3.storage.S3Storage\",
        \"format.class\": \"io.confluent.connect.s3.format.json.JsonFormat\",
        \"locale\": \"en_GB\",
        \"timezone\": \"UTC\",
        \"timestamp.extractor\": \"Wallclock\",
        \"path.format\": \"'year'=YYYY/'month'=MM/'day'=dd\",
        \"partitioner.class\": \"io.confluent.connect.storage.partitioner.TimeBasedPartitioner\",
        \"partition.duration.ms\": \"86400000\",
        \"schema.compatibility\": \"NONE\"
      }
    }"
  echo "Created: ${NAME}"
}

create_connector "s3-sink-04-clickstream" "clickstream.events" "source=04_kafka_clickstream"
create_connector "s3-sink-05-sqs" "order.events" "source=05_sqs"
create_connector "s3-sink-15-iot" "iot.telemetry" "source=15_mqtt_iot"
create_connector "s3-sink-16-cloudwatch" "app.logs" "source=16_cloudwatch"
create_connector "s3-sink-02-debezium" "debezium.ecommerce.public.orders,debezium.ecommerce.public.customers,debezium.ecommerce.public.payments,debezium.ecommerce.public.order_items,debezium.ecommerce.public.inventory" "source=02_debezium_cdc"

# ── 12. Create Debezium Postgres connector ────────────────────
curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "debezium-postgres",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "tasks.max": "1",
      "database.hostname": "ecommerce-lakehouse-postgres.cbyumq8843k2.eu-west-1.rds.amazonaws.com",
      "database.port": "5432",
      "database.user": "postgres_admin",
      "database.password": "DeJourney2026!",
      "database.dbname": "ecommerce",
      "topic.prefix": "debezium.ecommerce",
      "table.include.list": "public.orders,public.customers,public.payments,public.order_items,public.inventory",
      "plugin.name": "pgoutput",
      "slot.name": "debezium_slot",
      "publication.name": "debezium_pub",
      "producer.security.protocol": "SASL_SSL",
      "producer.sasl.mechanism": "SCRAM-SHA-512",
      "producer.sasl.jaas.config": "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafka-admin\" password=\"KafkaAdmin2026!\";"
    }
  }'
echo "Created: debezium-postgres"

chown -R ec2-user:ec2-user /home/ec2-user/ecommerce-lakehouse
echo "=== DONE setup_kafka_connect.sh $(date) ==="

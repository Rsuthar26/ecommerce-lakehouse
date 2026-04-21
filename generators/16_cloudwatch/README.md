# Source 16 — CloudWatch Application Logs

## What It Simulates
Application logs from 6 microservices: API gateway access logs, order/payment/inventory service logs, Lambda execution logs. ~50K log lines/day. Continuous 24/7 stream. Must run on EC2.

## Log Sources
| Source | Log Type |
|---|---|
| api-gateway | HTTP access logs — method, path, status, latency |
| order-service | Order processing events |
| payment-service | Payment processing events |
| inventory-service | Stock update events |
| lambda-image-processor | Image processing results |
| lambda-sqs-consumer | SQS message processing |

## Event Schema
```json
{
  "log_id": "deterministic-hash-in-burst",
  "timestamp": "ISO timestamp",
  "timestamp_ms": 1745234567890,
  "log_level": "INFO | WARN | ERROR | DEBUG",
  "source": "api-gateway",
  "message": "GET /api/v1/orders 200 142ms",
  "request_id": "uuid4",
  "trace_id": "uuid4",
  "log_group": "/aws/api-gateway",
  "account_id": "467091806172",
  "stack_trace": "java.lang.RuntimeException... (ERROR only)",
  "http_status": 200,
  "latency_ms": 142,
  "source_ip": "82.123.45.67"
}
```

## Run Commands
```bash
python generators/16_cloudwatch/generate.py --mode burst
python generators/16_cloudwatch/generate.py --mode burst --dirty
python generators/16_cloudwatch/generate.py --mode stream
```

## Prerequisites
```bash
pip install kafka-python faker python-dotenv
```

## Environment Variables
```
KAFKA_BOOTSTRAP_SERVERS=b-1.staffdejourneykafka...amazonaws.com:9094,...
```

## How to Verify
```bash
kafka-console-consumer.sh --bootstrap-server $BROKERS --topic app.logs --max-messages 50
# Check log level distribution
kafka-console-consumer.sh --bootstrap-server $BROKERS --topic app.logs \
  --from-beginning --max-messages 500 | \
  python3 -c "
import sys,json
from collections import Counter
levels = Counter()
for line in sys.stdin:
    try: levels[json.loads(line).get('log_level')] += 1
    except: pass
print(dict(levels))
# Expect: ~57% INFO, ~14% WARN, ~14% ERROR, ~14% DEBUG
"
```

## Notes
- Must run on EC2
- 1% of records in dirty mode have garbled message bytes — simulates encoding issues in CloudWatch Logs Insights

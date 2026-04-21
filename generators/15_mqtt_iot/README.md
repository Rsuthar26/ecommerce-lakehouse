# Source 15 — MQTT IoT Sensors

## What It Simulates
Warehouse sensor telemetry: temperature, humidity, stock weight. 5 sensors across 3 warehouses, each publishing every 10 seconds = ~43,200 readings/day. Must run on EC2 — MSK is VPC-only.

## Sensors
| Sensor ID | Warehouse | Type | Alert Threshold |
|---|---|---|---|
| SENSOR-WH-LON-TEMP-01 | WH-LONDON-01 | temperature | >25°C |
| SENSOR-WH-LON-HUM-01 | WH-LONDON-01 | humidity | >70% |
| SENSOR-WH-MAN-TEMP-01 | WH-MANC-01 | temperature | >25°C |
| SENSOR-WH-BIR-TEMP-01 | WH-BRUM-01 | temperature | >25°C |
| SENSOR-WH-BIR-WGT-01 | WH-BRUM-01 | weight | N/A |

## Event Schema
```json
{
  "event_id": "deterministic-hash-in-burst",
  "sensor_id": "SENSOR-WH-LON-TEMP-01",
  "warehouse_id": "WH-LONDON-01",
  "sensor_type": "temperature | humidity | weight",
  "reading": 18.4,
  "unit": "celsius | percent | kg",
  "is_anomaly": false,
  "alert_type": "HIGH_TEMPERATURE | HIGH_HUMIDITY | null",
  "battery_pct": 87.3,
  "signal_rssi": -62,
  "firmware": "v2.4.1",
  "ts": "ISO timestamp",
  "ts_ms": 1745234567890
}
```

## Run Commands
```bash
python generators/15_mqtt_iot/generate.py --mode burst
python generators/15_mqtt_iot/generate.py --mode burst --dirty
python generators/15_mqtt_iot/generate.py --mode stream
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
kafka-console-consumer.sh --bootstrap-server $BROKERS --topic iot.telemetry --max-messages 20
# Check anomaly rate (should be ~5-10% with normal data)
kafka-console-consumer.sh --bootstrap-server $BROKERS --topic iot.telemetry \
  --from-beginning --max-messages 1000 | \
  python3 -c "import sys,json; lines=[json.loads(l) for l in sys.stdin]; print(f'Anomaly rate: {sum(l[\"is_anomaly\"] for l in lines)/len(lines)*100:.1f}%')"
```

## Notes
- Must run on EC2
- Burst samples 2K/day × 7 days = 14K events (full volume would be 302K — too slow for demo)
- Stream fires all 5 sensors every 10 seconds — realistic IoT cadence

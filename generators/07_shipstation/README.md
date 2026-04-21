# Source 07 — ShipStation API

## What It Simulates
Shipment tracking events for fulfilled orders. Reads real shipped/delivered order_ids from Postgres. ~5K records/day. Timestamps follow warehouse dispatch hours (8am–6pm) — no shipments at 3am.

## Event Schema
```json
{
  "shipment_id": "SHIP-deterministic-hash",
  "order_id": "real shipped order_id from Postgres",
  "carrier": "Royal Mail | DPD | Hermes | UPS | FedEx | Parcelforce",
  "service": "Standard | Express | Next Day",
  "tracking_number": "JD123456789GB",
  "status": "label_created | picked_up | in_transit | out_for_delivery | delivered | exception | returned",
  "shipped_at": "ISO timestamp during 8am-6pm",
  "estimated_delivery": "ISO timestamp",
  "delivered_at": "ISO timestamp (delivered only)",
  "weight_grams": 450,
  "shipping_address": {"line1": "...", "city": "London", "postcode": "E1 6RF", "country": "GB"}
}
```

## Run Commands
```bash
python generators/07_shipstation/generate.py --mode burst --output-dir /tmp/shipstation_raw
python generators/07_shipstation/generate.py --mode burst --dirty --output-dir /tmp/shipstation_raw
python generators/07_shipstation/generate.py --mode stream --output-dir /tmp/shipstation_raw
```

## Prerequisites
```bash
pip install faker python-dotenv
```

## Environment Variables
```
SHIPSTATION_OUTPUT_DIR=/tmp/shipstation_raw
PG_HOST, PG_PORT, PG_DBNAME, PG_USER, PG_PASSWORD
```

## How to Verify
```bash
find /tmp/shipstation_raw -name "*.json" | wc -l
cat /tmp/shipstation_raw/$(date +%Y/%m/%d)/*/shipstation_0000.json | \
  python3 -c "import json,sys; d=json.load(sys.stdin); [print(i['status'], i['carrier']) for i in d['items'][:5]]"
```

# Source 11 — ERP Export

## What It Simulates
Finance ledger exports: invoices with GL account codes and cost centres. Real ERPs export both JSON and XML — this generator produces both. ~1–2 files/day. Reads real order_ids from Postgres.

## Formats
- JSON: structured invoice array — easier for Spark to read
- XML: same data in XML envelope — Silver must handle both with the same Bronze table

## Invoice Schema
```json
{
  "invoice_number": "INV-A1B2C3D4",
  "order_id": "real order_id from Postgres",
  "invoice_date": "ISO timestamp",
  "due_date": "ISO timestamp (+30 days)",
  "status": "issued | overdue | paid",
  "subtotal_pence": 49900,
  "tax_pence": 9980,
  "total_pence": 59880,
  "currency": "GBP | EUR | USD",
  "gl_account": "4000-Revenue | 5000-COGS | 6000-Opex",
  "cost_centre": "CC-WAREHOUSE | CC-SALES | CC-MARKETING",
  "vendor_name": "Acme Supplies Ltd",
  "payment_terms": "NET30 | NET60 | NET90 | immediate"
}
```

## Run Commands
```bash
python generators/11_erp/generate.py --mode burst --output-dir /tmp/erp_raw
python generators/11_erp/generate.py --mode burst --dirty --output-dir /tmp/erp_raw
python generators/11_erp/generate.py --mode stream --output-dir /tmp/erp_raw
```

## Prerequisites
```bash
pip install faker python-dotenv
```

## Environment Variables
```
ERP_OUTPUT_DIR=/tmp/erp_raw
PG_HOST, PG_PORT, PG_DBNAME, PG_USER, PG_PASSWORD
```

## How to Verify
```bash
find /tmp/erp_raw -name "*.json" -o -name "*.xml" | wc -l
cat /tmp/erp_raw/$(date +%Y/%m/%d)/erp_export_0000.json | python3 -m json.tool | head -30
cat /tmp/erp_raw/$(date +%Y/%m/%d)/erp_export_0001.xml | head -20
```

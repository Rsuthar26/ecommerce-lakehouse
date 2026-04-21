# Source 10 — Partner S3 Drop

## What It Simulates
Partner sales and affiliate data dropped as Parquet and Avro files. 3 partners, ~3 files/day. Databricks Autoloader reads these directly from S3.

## Partners and Formats
| Partner | Type | Format |
|---|---|---|
| PARTNER-001 AffiliateNet UK | affiliate | Parquet |
| PARTNER-002 MarketPlace Pro | marketplace | Parquet |
| PARTNER-003 CrossSell Direct | crosssell | Avro |

## Schema
```json
{
  "partner_id": "PARTNER-001",
  "sale_id": "PARTNER-001-123456",
  "product_sku": "real SKU from Postgres",
  "sale_amount_gbp": 49.99,
  "commission_gbp": 4.99,
  "currency": "GBP",
  "sale_status": "completed | pending | cancelled | refunded",
  "channel": "email | social | search | display | referral",
  "customer_email": "buyer@example.com",
  "click_id": "uuid",
  "campaign_id": "CAMP-1234",
  "is_first_purchase": true
}
```

## Run Commands
```bash
python generators/10_partner_s3/generate.py --mode burst --output-dir /tmp/partner_raw
python generators/10_partner_s3/generate.py --mode burst --dirty --output-dir /tmp/partner_raw
python generators/10_partner_s3/generate.py --mode stream --output-dir /tmp/partner_raw
```

## Prerequisites
```bash
pip install pandas pyarrow fastavro faker python-dotenv
# fastavro only needed for Avro output (PARTNER-003)
```

## Environment Variables
```
PARTNER_OUTPUT_DIR=/tmp/partner_raw
PG_HOST, PG_PORT, PG_DBNAME, PG_USER, PG_PASSWORD
```

## How to Verify
```bash
find /tmp/partner_raw -name "*.parquet" -o -name "*.avro" | wc -l
python3 -c "
import pandas as pd
import glob
files = glob.glob('/tmp/partner_raw/**/*.parquet', recursive=True)
df = pd.read_parquet(files[0])
print(df.head())
print(df.dtypes)
"
```

## Notes
- Burst filenames use date only — Rule 5 idempotent, second run overwrites same file
- Falls back to JSON Lines if pyarrow/fastavro not installed

# Source 09 — SFTP Supplier Files

## What It Simulates
Supplier catalog and pricing files dropped via SFTP. 5 suppliers, each using different column names for the same data — intentional messiness that Silver must normalise. ~3 files/day.

## File Formats
- SUP001, SUP002, SUP004 → CSV
- SUP003, SUP005 → Excel (.xlsx)

## Column Name Variants Per Supplier
| Supplier | SKU column | Price column | Stock column |
|---|---|---|---|
| SUP001 | SKU | Unit Cost GBP | Stock QTY |
| SUP002 | sku_code | cost_price | available_qty |
| SUP003 | Item Code | Price (GBP) | Qty Available |
| SUP004 | PART_NO | UNIT_PRICE | QTY_OH |
| SUP005 | product_id | wholesale_price | stock_level |

## Run Commands
```bash
python generators/09_sftp/generate.py --mode burst --output-dir /tmp/sftp_raw
python generators/09_sftp/generate.py --mode burst --dirty --output-dir /tmp/sftp_raw
python generators/09_sftp/generate.py --mode stream --output-dir /tmp/sftp_raw
```

## Prerequisites
```bash
pip install openpyxl faker python-dotenv
```

## Environment Variables
```
SFTP_OUTPUT_DIR=/tmp/sftp_raw
PG_HOST, PG_PORT, PG_DBNAME, PG_USER, PG_PASSWORD
```

## How to Verify
```bash
find /tmp/sftp_raw -name "*.csv" -o -name "*.xlsx" | wc -l
# Check a CSV file
head -5 /tmp/sftp_raw/$(date +%Y/%m/%d)/SUP001_$(date +%Y%m%d)_catalog.csv
```

## Notes
- Burst filenames use date only (e.g. `SUP001_20260421_catalog.csv`) — Rule 5 idempotent, second run overwrites
- Stream filenames use full datetime — each stream drop is a new file
- Bad data in --dirty: truncated rows, blank rows, price = "POA", duplicate rows

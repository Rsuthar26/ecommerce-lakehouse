# Source 14 — Scrapy Competitor Pricing

## What It Simulates
Web scraping output for competitor pricing, stock availability, and promotions. 3 fictional competitors, ~2K records/day. Availability reflects age of scrape — recent data skews in_stock, older data has more out_of_stock.

## Competitors
| ID | Name | Domain |
|---|---|---|
| COMP-001 | TechDirect UK | techdirect.co.uk |
| COMP-002 | GadgetHub | gadgethub.com |
| COMP-003 | ValueElectronics | valueelectronics.co.uk |

## Schema
```json
{
  "record_id": "deterministic-hash (competitor+sku+date)",
  "scraped_at": "ISO timestamp",
  "competitor_id": "COMP-001",
  "competitor_domain": "techdirect.co.uk",
  "product_sku": "real SKU from Postgres",
  "price_pence": 4999,
  "our_price_pence": 4799,
  "price_difference_pct": 4.17,
  "availability": "in_stock | low_stock | out_of_stock | discontinued",
  "promo_active": false,
  "promo_text": "Save 20% this week!",
  "rating": 4.3,
  "review_count": 127
}
```

## Run Commands
```bash
python generators/14_scrapy/generate.py --mode burst --output-dir /tmp/scrapy_raw
python generators/14_scrapy/generate.py --mode burst --dirty --output-dir /tmp/scrapy_raw
python generators/14_scrapy/generate.py --mode stream --output-dir /tmp/scrapy_raw
```

## Prerequisites
```bash
pip install faker python-dotenv
```

## Environment Variables
```
SCRAPY_OUTPUT_DIR=/tmp/scrapy_raw
PG_HOST, PG_PORT, PG_DBNAME, PG_USER, PG_PASSWORD
```

## How to Verify
```bash
find /tmp/scrapy_raw -name "*.json" | wc -l
python3 -c "
import json, glob
from collections import Counter
files = glob.glob('/tmp/scrapy_raw/**/*.json', recursive=True)
avail = Counter()
for f in files:
    with open(f) as fp:
        d = json.load(fp)
        for r in d.get('records', []):
            avail[r.get('availability')] += 1
print(dict(avail))
"
```

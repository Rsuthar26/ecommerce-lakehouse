# Source 12 — Customer Reviews and Support Tickets

## What It Simulates
Unstructured text data: product reviews with realistic sentiment and support tickets referencing real order_ids. ~1K records/day. Timestamps follow customer waking hours (9am–11pm, evening peak). Feeds NLP/sentiment analysis in Gold layer.

## Record Types

**Review:**
```json
{
  "record_type": "review",
  "review_id": "deterministic-hash",
  "order_id": "real order_id from Postgres",
  "product_sku": "real SKU from Postgres",
  "customer_id": "real customer_id from Postgres",
  "rating": 4,
  "title": "Great product",
  "body": "Absolutely love this product. Arrived quickly and exactly as described.",
  "verified_purchase": true,
  "helpful_votes": 12
}
```

**Ticket:**
```json
{
  "record_type": "ticket",
  "ticket_id": "TKT-A1B2C3D4",
  "order_id": "real order_id from Postgres",
  "category": "delivery | returns | billing | product | account",
  "status": "open | pending | resolved | closed",
  "priority": "low | medium | high | urgent",
  "body": "My order #5001 has not arrived yet. It's been 7 days.",
  "channel": "email | chat | phone | web"
}
```

## Run Commands
```bash
python generators/12_reviews_tickets/generate.py --mode burst --output-dir /tmp/text_raw
python generators/12_reviews_tickets/generate.py --mode burst --dirty --output-dir /tmp/text_raw
python generators/12_reviews_tickets/generate.py --mode stream --output-dir /tmp/text_raw
```

## Prerequisites
```bash
pip install faker python-dotenv
```

## Environment Variables
```
REVIEWS_OUTPUT_DIR=/tmp/text_raw
PG_HOST, PG_PORT, PG_DBNAME, PG_USER, PG_PASSWORD
```

## How to Verify
```bash
python3 -c "
import json, glob
files = glob.glob('/tmp/text_raw/**/*.json', recursive=True)
from collections import Counter
types = Counter()
ratings = Counter()
for f in files:
    with open(f) as fp:
        d = json.load(fp)
        for r in d.get('records', []):
            types[r['record_type']] += 1
            if r['record_type'] == 'review':
                ratings[r.get('rating')] += 1
print('Types:', dict(types))
print('Rating distribution:', dict(sorted(ratings.items())))
"
```

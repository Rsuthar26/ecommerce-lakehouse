# Source 17 — GA4 Export

## What It Simulates
Google Analytics 4 BigQuery export format. ~200K events/day (5K sampled per day in burst mode for speed). Mirrors the exact schema GA4 exports to BigQuery — event_params are key-value pairs, user IDs are pseudonymous hashes, not PII.

## Key Schema Fields
```json
{
  "event_date": "20260421",
  "event_timestamp": 1745234567890123,
  "event_name": "page_view | view_item | add_to_cart | purchase | search | session_start | scroll",
  "user_pseudo_id": "pseudonymous-md5-hash",
  "user_id": null,
  "device": {
    "category": "desktop | mobile | tablet",
    "browser": "Chrome | Safari | Firefox",
    "operating_system": "Windows | macOS | iOS | Android"
  },
  "geo": {"country": "GB", "city": "London"},
  "traffic_source": {"medium": "organic", "source": "google"},
  "event_params": [
    {"key": "page_location", "value": {"string_value": "https://ecommerce.example.com/products"}},
    {"key": "transaction_id", "value": {"string_value": "real order_id from Postgres"}}
  ],
  "user_ltv": {"revenue": 299.50, "currency": "GBP"}
}
```

## Run Commands
```bash
python generators/17_ga4/generate.py --mode burst --output-dir /tmp/ga4_raw
python generators/17_ga4/generate.py --mode burst --dirty --output-dir /tmp/ga4_raw
python generators/17_ga4/generate.py --mode stream --output-dir /tmp/ga4_raw
```

## Prerequisites
```bash
pip install faker python-dotenv
```

## Environment Variables
```
GA4_OUTPUT_DIR=/tmp/ga4_raw
PG_HOST, PG_PORT, PG_DBNAME, PG_USER, PG_PASSWORD
```

## How to Verify
```bash
find /tmp/ga4_raw -name "*.json" | wc -l
python3 -c "
import json
from collections import Counter
with open(sorted(__import__('glob').glob('/tmp/ga4_raw/**/*.json', recursive=True))[0]) as f:
    d = json.load(f)
print(f'{d[\"event_count\"]} events for {d[\"export_date\"]}')
counts = Counter(e['event_name'] for e in d['events'])
print(counts.most_common(5))
"
```

## Notes
- Weekend traffic gets 40% uplift — visible in Gold layer day-of-week analysis
- Burst writes one file per day matching BigQuery daily export pattern
- Silver complexity: sessionise events, attribute revenue, reconstruct funnel

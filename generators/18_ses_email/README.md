# Source 18 — AWS SES Email Events

## What It Simulates
SES email delivery events: order confirmations, shipping notifications, marketing emails, bounce and complaint records. ~5K emails/day. Mirrors real SES SNS notification structure including bounce diagnostics and engagement tracking.

## Email Types
| Type | Trigger | Approx Volume |
|---|---|---|
| order_confirmation | Order placed | ~2K/day |
| shipping_notification | Order shipped | ~1.5K/day |
| delivery_confirmation | Order delivered | ~700/day |
| marketing_weekly | Weekly newsletter | ~300/day |
| marketing_promo | Promotional send | ~200/day |
| refund_confirmation | Refund processed | ~100/day |
| password_reset | User action | ~100/day |
| review_request | Post-delivery | ~100/day |

## Event Schema
```json
{
  "message_id": "<uuid@eu-west-1.amazonses.com>",
  "event_type": "order_confirmation",
  "ses_message_id": "md5-hash",
  "sender": "noreply@ecommerce.example.com",
  "recipient": "customer@example.com",
  "subject": "Order Confirmation #5001 - Thank you!",
  "sent_at": "ISO timestamp",
  "order_id": "real order_id from Postgres",
  "customer_id": "real customer_id from Postgres",
  "delivery_status": "delivered | bounced | complained | pending",
  "bounce": {
    "bounce_type": "Permanent | Transient",
    "status": "5.1.1",
    "diagnostic_code": "smtp; 550 5.1.1 The email account does not exist"
  },
  "engagement": {
    "opened": true,
    "clicked": false,
    "unsubscribed": false
  }
}
```

## Run Commands
```bash
python generators/18_ses_email/generate.py --mode burst --output-dir /tmp/email_raw
python generators/18_ses_email/generate.py --mode burst --dirty --output-dir /tmp/email_raw
python generators/18_ses_email/generate.py --mode stream --output-dir /tmp/email_raw
```

## Prerequisites
```bash
pip install faker python-dotenv
```

## Environment Variables
```
EMAIL_OUTPUT_DIR=/tmp/email_raw
PG_HOST, PG_PORT, PG_DBNAME, PG_USER, PG_PASSWORD
```

## How to Verify
```bash
find /tmp/email_raw -name "*.json" | wc -l
python3 -c "
import json, glob
from collections import Counter
files = glob.glob('/tmp/email_raw/**/*.json', recursive=True)
statuses, types = Counter(), Counter()
for f in files:
    with open(f) as fp:
        d = json.load(fp)
        for e in d.get('events', []):
            statuses[e.get('delivery_status')] += 1
            types[e.get('event_type')] += 1
print('Delivery:', dict(statuses))
# Expect: ~95% delivered, ~4% bounced, ~1% complained
print('Types:', types.most_common(5))
"
```

## Notes
- Bounce records include full SMTP diagnostic codes — Silver can categorise hard vs soft bounces
- Marketing emails include engagement data (open/click rates) — Gold feeds marketing analytics
- Bad email addresses in dirty mode simulate real deliverability issues

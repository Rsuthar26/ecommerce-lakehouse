# Source 06 — Stripe API Payments

## What It Simulates
Stripe charge objects: payments, refunds, disputes. Mirrors real Stripe API response structure exactly. ~10K records/day. Reads real order_ids from Postgres so every charge joins back to a real order.

## Event Schema
```json
{
  "object": "charge",
  "id": "ch_...",
  "payment_intent": "pi_deterministic-hash",
  "amount": 4999,
  "amount_captured": 4999,
  "amount_refunded": 0,
  "currency": "gbp",
  "status": "succeeded | failed | disputed",
  "metadata": {
    "order_id": "real order_id from Postgres",
    "customer_id": "real customer_id from Postgres"
  },
  "payment_method_details": {
    "type": "card",
    "card": {"brand": "visa", "last4": "4242", "exp_month": 12, "exp_year": 2027}
  },
  "refunds": [{"id": "re_...", "amount": 2000, "reason": "requested_by_customer"}]
}
```

## Run Commands
```bash
python generators/06_stripe/generate.py --mode burst --output-dir /tmp/stripe_raw
python generators/06_stripe/generate.py --mode burst --dirty --output-dir /tmp/stripe_raw
python generators/06_stripe/generate.py --mode stream --output-dir /tmp/stripe_raw
```

## Prerequisites
```bash
pip install faker python-dotenv
```

## Environment Variables
```
STRIPE_OUTPUT_DIR=/tmp/stripe_raw
PG_HOST, PG_PORT, PG_DBNAME, PG_USER, PG_PASSWORD
```

## How to Verify
```bash
ls /tmp/stripe_raw/$(date +%Y/%m/%d)/
cat /tmp/stripe_raw/$(date +%Y/%m/%d)/*/stripe_charges_0000.json | python3 -m json.tool | head -40
```

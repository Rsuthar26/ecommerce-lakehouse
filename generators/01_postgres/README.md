# Source 01 — RDS PostgreSQL

## What It Simulates
The core OLTP transactional database. Every order, customer, payment, and inventory change in the system originates here. This is the master source of truth for all entity IDs — every other generator that references a customer_id, order_id, or product_sku must load real IDs from this database.

## Schema

| Table | Key Columns | Purpose |
|---|---|---|
| customers | customer_id, email, account_status | Master customer registry |
| orders | order_id, customer_id, order_status, total_amount_pence | Order lifecycle |
| order_items | item_id, order_id, product_sku, quantity, unit_price_pence | Line items per order |
| payments | payment_id, order_id, payment_status, stripe_payment_intent_id | Payment records |
| inventory | product_sku, warehouse_id, quantity_available | Stock levels |

## Run Commands

```bash
# First time only — seed 7 days of history
python generators/01_postgres/generate.py --mode burst --days 7

# First time dirty — for Silver cleaning practice
python generators/01_postgres/generate.py --mode burst --days 7 --dirty

# Every session after — continuous realistic flow
python generators/01_postgres/generate.py --mode stream
```

## Prerequisites

```bash
pip install psycopg2-binary faker python-dotenv
```

## Environment Variables

```bash
PG_HOST=ecommerce-lakehouse-postgres.c9m24scg8pte.eu-north-1.rds.amazonaws.com
PG_PORT=5432
PG_DBNAME=ecommerce
PG_USER=postgres_admin
PG_PASSWORD=your_password
```

## How to Verify

```sql
-- Row counts
SELECT COUNT(*) FROM customers;
SELECT COUNT(*) FROM orders;
SELECT COUNT(*) FROM order_items;
SELECT COUNT(*) FROM payments;
SELECT COUNT(*) FROM inventory;

-- Status distribution reflects age (Rule 4)
SELECT order_status, COUNT(*) FROM orders GROUP BY order_status ORDER BY COUNT(*) DESC;

-- Payment status follows order status
SELECT o.order_status, p.payment_status, COUNT(*)
FROM orders o JOIN payments p ON o.order_id = p.order_id
GROUP BY 1, 2 ORDER BY 3 DESC;

-- Check dirty data was injected (run with --dirty)
SELECT COUNT(*) FROM customers WHERE email NOT LIKE '%@%';
```

## Notes
- Never run `--mode burst` twice — orders have no conflict guard. Burst is one-time setup only.
- Stream mode is safe to run continuously — commits per order.
- RDS must be running before starting: `aws rds start-db-instance --db-instance-identifier ecommerce-lakehouse-postgres --region eu-north-1`
- Stop RDS at end of every session to avoid unexpected charges.

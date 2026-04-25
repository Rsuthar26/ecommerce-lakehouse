# shared/postgres_ids.py

## What It Is
A shared utility that loads real entity IDs from RDS PostgreSQL at generator startup. Every generator that references `customer_id`, `order_id`, or `product_sku` imports this module instead of hardcoding ID ranges.

## Why It Exists
If a generator uses `random.randint(1, 6751)` for `order_id` but RDS only has 2,223 orders, 67% of generated references point to orders that don't exist. Silver flags them as referential integrity breaks. Gold joins silently lose data. Demo numbers are wrong.

This module is the single source of truth for all entity IDs.

## Usage

```python
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from generators.shared.postgres_ids import load_entity_ids

ids = load_entity_ids()

customer_id  = random.choice(ids["customer_ids"])
order_id     = random.choice(ids["order_ids"])
product_sku  = random.choice(ids["product_skus"])
shipped_order_id = random.choice(ids["shipped_order_ids"])
```

## What It Returns

| Key | Contents |
|---|---|
| `customer_ids` | Active customer IDs from `customers` table |
| `order_ids` | All order IDs from `orders` table |
| `shipped_order_ids` | Order IDs with status `shipped` or `delivered` |
| `payment_ids` | Payment IDs from `payments` table |
| `payment_orders` | List of `(order_id, amount_pence)` tuples |
| `product_skus` | Distinct SKUs that have actually been ordered |

## Environment Variables

```
PG_HOST=ecommerce-lakehouse-postgres.c9m24scg8pte.eu-west-1.rds.amazonaws.com
PG_PORT=5432
PG_DBNAME=ecommerce
PG_USER=postgres_admin
PG_PASSWORD=your_password
```

## Folder Location

```
generators/
└── shared/
    ├── postgres_ids.py   ← this file
    └── README.md         ← this README
```

## Notes
- RDS must be running before any generator that calls `load_entity_ids()` will start
- Results are cached in memory for the lifetime of the generator process — only one DB query per startup
- If Postgres is unreachable, Source 03 MongoDB falls back to a deterministic SKU list — all other generators will fail fast with a clear connection error

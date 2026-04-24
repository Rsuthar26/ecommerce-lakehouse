"""
generators/shared/postgres_ids.py — Staff DE Journey

Shared utility: load real entity IDs from RDS Postgres at generator startup.
Every generator that references customer_id, order_id, or product_sku
must use this module. Never hardcode ID ranges or generate random IDs.

Why this matters:
  If your generator uses random.randint(1, 6751) for order_id but your
  RDS only has 2,223 orders, 67% of your references will be orphaned.
  Silver will flag them as referential integrity breaks.
  Your Gold layer joins will silently lose data.
  Your demo will show wrong numbers.

Usage:
    from generators.shared.postgres_ids import load_entity_ids

    ids = load_entity_ids()  # reads from env vars
    customer_ids  = ids["customer_ids"]   # list of real customer_id ints
    order_ids     = ids["order_ids"]      # list of real order_id ints
    product_skus  = ids["product_skus"]   # list of real sku strings

    # Then sample from them:
    customer_id = random.choice(customer_ids)
    order_id    = random.choice(order_ids)
    sku         = random.choice(product_skus)
"""

import os
import logging
import psycopg2

log = logging.getLogger(__name__)


def get_pg_connection():
    """Connect to RDS PostgreSQL using environment variables."""
    return psycopg2.connect(
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", 5432)),
        dbname=os.environ.get("PG_DBNAME", "ecommerce"),
        user=os.environ.get("PG_USER", "postgres_admin"),
        password=os.environ.get("PG_PASSWORD", ""),
        connect_timeout=10,
    )


def load_entity_ids(limit: int = 1000) -> dict:
    """
    Load real entity IDs from Postgres.
    Returns a dict with customer_ids, order_ids, product_skus.

    Args:
        limit: max IDs to load per entity type (sampling — not all rows)

    Returns:
        {
            "customer_ids": [1, 2, 3, ...],
            "order_ids":    [1, 2, 3, ...],
            "product_skus": ["SKU-00001", ...],
            "shipped_order_ids": [... orders that are shipped/delivered],
        }
    """
    log.info(f"Loading entity IDs from Postgres (limit={limit} per type)...")
    conn = get_pg_connection()
    cur  = conn.cursor()

    result = {}

    # Customer IDs — active customers only
    cur.execute("""
        SELECT customer_id FROM customers
        WHERE account_status = 'active'
        ORDER BY RANDOM()
        LIMIT %s
    """, (limit,))
    result["customer_ids"] = [row[0] for row in cur.fetchall()]

    # All order IDs
    cur.execute("""
        SELECT order_id FROM orders
        ORDER BY RANDOM()
        LIMIT %s
    """, (limit,))
    result["order_ids"] = [row[0] for row in cur.fetchall()]

    # Shipped/delivered order IDs (for ShipStation, fulfillment generators)
    cur.execute("""
        SELECT order_id FROM orders
        WHERE order_status IN ('shipped', 'delivered')
        ORDER BY RANDOM()
        LIMIT %s
    """, (limit,))
    result["shipped_order_ids"] = [row[0] for row in cur.fetchall()]

    # Payment IDs linked to orders
    cur.execute("""
        SELECT p.payment_id, p.order_id, p.amount_pence
        FROM payments p
        ORDER BY RANDOM()
        LIMIT %s
    """, (limit,))
    rows = cur.fetchall()
    result["payment_ids"]   = [r[0] for r in rows]
    result["payment_orders"] = [(r[1], r[2]) for r in rows]  # (order_id, amount_pence)

    # Product SKUs from order_items (only SKUs that have actually been ordered)
    cur.execute("""
        SELECT product_sku FROM order_items
        ORDER BY RANDOM()
        LIMIT %s
    """, (limit,))
    result["product_skus"] = [row[0] for row in cur.fetchall()]

    # Fallback: if no order_items yet, generate deterministic SKU list
    if not result["product_skus"]:
        log.warning("No product_skus found in order_items — using deterministic fallback")
        result["product_skus"] = [f"SKU-{str(i).zfill(5)}" for i in range(1, 201)]

    # Rule 14 — order amounts: {order_id: total_pence}
    # Used by any generator that has both order_id AND a monetary amount field
    cur.execute("""
        SELECT order_id, total_pence
        FROM orders
        WHERE total_pence > 0
        ORDER BY RANDOM()
        LIMIT %s
    """, (limit,))
    result["order_amounts"] = {row[0]: row[1] for row in cur.fetchall()}

    cur.close()
    conn.close()

    log.info(
        f"Loaded: {len(result['customer_ids'])} customers, "
        f"{len(result['order_ids'])} orders, "
        f"{len(result['shipped_order_ids'])} shipped orders, "
        f"{len(result['product_skus'])} SKUs, "
        f"{len(result['order_amounts'])} order amounts"
    )
    return result

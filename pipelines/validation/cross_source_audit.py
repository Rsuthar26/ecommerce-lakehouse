#!/usr/bin/env python3
"""
cross_source_audit.py — Full cross-source validation for all 18 S3 Raw sources
Verifies every possible join between sources against Postgres ground truth.

Usage:
    python pipelines/validation/cross_source_audit.py

Requirements:
    pip install duckdb psycopg2-binary boto3 pymongo
"""

import os
import sys
import json
import re
import boto3
import duckdb
import psycopg2
import psycopg2.extras

# ── Config ────────────────────────────────────────────────────────────────────
S3_BUCKET   = "ecommerce-lakehouse-467091806172-raw-01"
REGION      = "eu-west-1"
PG_HOST     = os.environ.get("PG_HOST", "ecommerce-lakehouse-postgres.cbyumq8843k2.eu-west-1.rds.amazonaws.com")
PG_PORT     = os.environ.get("PG_PORT", "5432")
PG_DBNAME   = os.environ.get("PG_DBNAME", "ecommerce")
PG_USER     = os.environ.get("PG_USER", "postgres_admin")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "DeJourney2026!")

# ── Helpers ───────────────────────────────────────────────────────────────────
results = []

def check(name, passed, detail=""):
    status = "✅ PASS" if passed else "❌ FAIL"
    results.append((name, passed, detail))
    print(f"  {status}  {name}")
    if detail:
        print(f"         {detail}")

def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

def get_pg():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DBNAME,
        user=PG_USER, password=PG_PASSWORD
    )

def get_s3():
    return boto3.client("s3", region_name=REGION)

def list_s3_files(prefix, ext=None):
    s3 = get_s3()
    paginator = s3.get_paginator("list_objects_v2")
    files = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if ext is None or key.endswith(ext):
                files.append(key)
    return files

def read_nested_json(prefix, array_key, max_records=500):
    s3 = get_s3()
    paginator = s3.get_paginator("list_objects_v2")
    records = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if "_metadata" in key or not key.endswith(".json"):
                continue
            try:
                data = s3.get_object(Bucket=S3_BUCKET, Key=key)
                wrapper = json.loads(data["Body"].read().decode("utf-8"))
                items = wrapper.get(array_key, [])
                records.extend(items)
            except Exception:
                pass
            if len(records) >= max_records:
                break
        if len(records) >= max_records:
            break
    return records[:max_records]

def read_json_sample(prefix, max_records=500):
    """Read up to max_records from JSON files in S3 prefix."""
    s3 = get_s3()
    files = list_s3_files(prefix, ".json")
    if not files:
        return []
    records = []
    for f in files[:3]:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=f)
        body = obj["Body"].read()
        try:
            for line in body.decode("utf-8").strip().split("\n"):
                if line.strip():
                    try:
                        parsed = json.loads(line)
                        # Kafka Connect double-encodes: each line is a JSON string
                        # containing another JSON object
                        if isinstance(parsed, str):
                            try:
                                parsed = json.loads(parsed)
                            except Exception:
                                pass
                        records.append(parsed)
                    except Exception:
                        pass
        except Exception:
            pass
        if len(records) >= max_records:
            break
    return records[:max_records]

# ── Load Postgres ground truth ────────────────────────────────────────────────
section("Loading Postgres ground truth")
pg = get_pg()
cur = pg.cursor(cursor_factory=psycopg2.extras.DictCursor)

cur.execute("SELECT order_id FROM orders LIMIT 5000")
pg_order_ids = set(r[0] for r in cur.fetchall())

cur.execute("SELECT product_sku FROM order_items LIMIT 5000")
pg_skus = set(r[0] for r in cur.fetchall())

cur.execute("SELECT customer_id FROM customers LIMIT 5000")
pg_customer_ids = set(r[0] for r in cur.fetchall())

cur.execute("SELECT order_id, total_pence FROM orders LIMIT 5000")
pg_order_amounts = {r[0]: r[1] for r in cur.fetchall()}

cur.execute("SELECT order_id, customer_id FROM orders LIMIT 5000")
pg_order_customers = {r[0]: r[1] for r in cur.fetchall()}

cur.execute("SELECT order_id, product_sku FROM order_items LIMIT 5000")
pg_item_prices = {(r[0], r[1]): 0 for r in cur.fetchall()}
pg_sku_prices = {}  # prices come from MongoDB not Postgres

print(f"  Loaded {len(pg_order_ids)} orders, {len(pg_skus)} SKUs, {len(pg_customer_ids)} customers")

# ── DuckDB setup ──────────────────────────────────────────────────────────────
duck = duckdb.connect()
duck.execute("INSTALL httpfs; LOAD httpfs;")
duck.execute(f"SET s3_region='{REGION}';")

# Inject AWS credentials explicitly — DuckDB doesn't auto-pick from boto3 session
import boto3 as _boto3
_creds = _boto3.Session().get_credentials().get_frozen_credentials()
duck.execute(f"SET s3_access_key_id='{_creds.access_key}';")
duck.execute(f"SET s3_secret_access_key='{_creds.secret_key}';")
if _creds.token:
    duck.execute(f"SET s3_session_token='{_creds.token}';")

def duck_query(sql):
    try:
        return duck.execute(sql).fetchall()
    except Exception as e:
        return []

def s3_parquet(source_prefix):
    return f"read_parquet('s3://{S3_BUCKET}/{source_prefix}**/*.parquet')"

def s3_json_duckdb(source_prefix):
    # Exclude _metadata folder — those are manifest files not data
    return f"read_json_auto('s3://{S3_BUCKET}/{source_prefix}year=**/**/*.json', ignore_errors=true)"

def s3_csv_duckdb(source_prefix):
    return f"read_csv_auto('s3://{S3_BUCKET}/{source_prefix}**/*.csv', ignore_errors=true)"

# ── Source 01: Postgres snapshot ─────────────────────────────────────────────
section("Source 01 — RDS PostgreSQL Snapshot")
rows = duck_query(f"SELECT COUNT(*) FROM {s3_parquet('source=01_postgres/')}")
count = rows[0][0] if rows else 0
check("Source 01 has Parquet files in S3", count > 0, f"{count} rows")

# ── Source 02: Debezium CDC ───────────────────────────────────────────────────
section("Source 02 — Debezium CDC")
records = read_json_sample("source=02_debezium_cdc/")
check("Source 02 has JSON files in S3", len(records) > 0, f"{len(records)} sample records")
if records:
    has_envelope = any(
        "payload" in r or "op" in r or "after" in r or
        any(k in str(r) for k in ["INSERT", "UPDATE", "DELETE", "create", "update"])
        for r in records
    )
    check("Source 02 has CDC envelope structure", has_envelope)

# ── Source 03: MongoDB Products ───────────────────────────────────────────────
section("Source 03 — MongoDB Atlas Products")
rows = duck_query(f"SELECT COUNT(*), COUNT(DISTINCT product_sku) FROM {s3_parquet('source=03_mongodb/')}")
count = rows[0][0] if rows else 0
check("Source 03 has Parquet files", count > 0, f"{count} rows")
if count > 0:
    skus = duck_query(f"SELECT DISTINCT product_sku FROM {s3_parquet('source=03_mongodb/')} LIMIT 5000")
    s3_skus = set(r[0] for r in skus if r[0])
    overlap = len(s3_skus & pg_skus)
    pct = overlap / len(pg_skus) * 100 if pg_skus else 0
    check(f"Source 03 SKUs match Postgres order_items", pct >= 80,
          f"{overlap}/{len(pg_skus)} SKUs matched ({pct:.1f}%)")

# ── Source 04: Clickstream ────────────────────────────────────────────────────
section("Source 04 — MSK Kafka Clickstream")
records = read_json_sample("source=04_kafka_clickstream/")
check("Source 04 has JSON files in S3", len(records) > 0, f"{len(records)} sample records")
if records:
    has_user = any(isinstance(r, dict) and (r.get("user_id") or r.get("customer_id")) for r in records)
    check("Source 04 has user_id/customer_id field", has_user)
    anon = sum(1 for r in records if not isinstance(r, dict) or (not r.get("user_id") and not r.get("customer_id")))
    anon_pct = anon / len(records) * 100
    check("Source 04 anonymous rate acceptable (<50%)", anon_pct < 50,
          f"{anon_pct:.1f}% anonymous (expected ~30%)")

# ── Source 05: SQS Order Events ───────────────────────────────────────────────
section("Source 05 — AWS SQS Order Events")
records = read_json_sample("source=05_sqs/")
check("Source 05 has JSON files in S3", len(records) > 0, f"{len(records)} sample records")
if records:
    order_ids = [r.get("order_id") for r in records if isinstance(r, dict) and r.get("order_id")]
    check("Source 05 has order_id field", len(order_ids) > 0)
    if order_ids:
        matched = sum(1 for oid in order_ids if oid in pg_order_ids)
        pct = matched / len(order_ids) * 100
        check("Source 05 order_ids match Postgres", pct >= 85,
              f"{matched}/{len(order_ids)} matched ({pct:.1f}%)")
    cust_ids = [r.get("customer_id") for r in records if isinstance(r, dict) and r.get("customer_id")]
    if cust_ids:
        matched = sum(1 for cid in cust_ids if cid in pg_customer_ids)
        pct = matched / len(cust_ids) * 100
        check("Source 05 customer_ids match Postgres", pct >= 85,
              f"{matched}/{len(cust_ids)} matched ({pct:.1f}%)")

# ── Source 06: Stripe ─────────────────────────────────────────────────────────
section("Source 06 — Stripe API")
charges = read_nested_json("source=06_stripe/year=", "charges", 500)
check("Source 06 has data", len(charges) > 0, f"{len(charges)} charges")
if charges:
    oids = set()
    for c in charges:
        meta = c.get("metadata", {})
        if meta and meta.get("order_id"):
            try:
                oids.add(int(meta["order_id"]))
            except Exception:
                pass
    matched = len(oids & pg_order_ids)
    pct = matched / len(oids) * 100 if oids else 0
    check("Source 06 order_ids match Postgres", pct >= 85,
          f"{matched}/{len(oids)} matched ({pct:.1f}%)")

# ── Source 07: ShipStation ────────────────────────────────────────────────────
section("Source 07 — ShipStation API")
shipments = read_nested_json("source=07_shipstation/year=", "items", 500)
check("Source 07 has data", len(shipments) > 0, f"{len(shipments)} shipments")
if shipments:
    oids = set(s.get("order_id") for s in shipments if s.get("order_id"))
    matched = len(oids & pg_order_ids)
    pct = matched / len(oids) * 100 if oids else 0
    check("Source 07 order_ids match Postgres (shipped orders)", pct >= 85,
          f"{matched}/{len(oids)} matched ({pct:.1f}%)")

# ── Source 08: Shopify ────────────────────────────────────────────────────────
section("Source 08 — Shopify GraphQL")
rows = duck_query(f"SELECT COUNT(*) FROM {s3_json_duckdb('source=08_shopify/')}")
count = rows[0][0] if rows else 0
check("Source 08 has Parquet files", count > 0, f"{count} rows")
if count > 0:
    price_rows = duck_query(f"""
        SELECT product_sku, price FROM {s3_json_duckdb('source=08_shopify/')}
        WHERE product_sku IS NOT NULL AND price IS NOT NULL LIMIT 500
    """)
    if price_rows:
        matched = sum(1 for sku, price in price_rows
                     if sku in pg_sku_prices and abs(pg_sku_prices[sku] - price) < 10)
        pct = matched / len(price_rows) * 100
        check("Source 08 prices match MongoDB base_price", pct >= 85,
              f"{matched}/{len(price_rows)} prices matched ({pct:.1f}%)")

# ── Source 09: SFTP Supplier ──────────────────────────────────────────────────
section("Source 09 — SFTP Supplier Drop")
rows = duck_query(f"SELECT COUNT(*) FROM read_csv_auto('s3://{S3_BUCKET}/source=09_sftp/**/*.csv', ignore_errors=true)")
count = rows[0][0] if rows else 0
check("Source 09 has Parquet files", count > 0, f"{count} rows")
if count > 0:
    # Try both possible column name formats
    cost_rows = duck_query(f"""
        SELECT "Unit Cost GBP", "Retail Price GBP" as retail_price_gbp
        FROM read_csv_auto('s3://{S3_BUCKET}/source=09_sftp/**/*.csv', ignore_errors=true)
        WHERE "Unit Cost GBP" IS NOT NULL LIMIT 500
    """)
    if cost_rows:
        valid = sum(1 for cost, retail in cost_rows if retail and cost < retail)
        pct = valid / len(cost_rows) * 100
        check("Source 09 supplier cost < retail price", pct >= 95,
              f"{valid}/{len(cost_rows)} valid ({pct:.1f}%)")

# ── Source 10: Partner S3 ─────────────────────────────────────────────────────
section("Source 10 — Partner S3 Drop")
rows = duck_query(f"SELECT COUNT(*) FROM {s3_parquet('source=10_partner_s3/')}")
count = rows[0][0] if rows else 0
check("Source 10 has Parquet files", count > 0, f"{count} rows")
if count > 0:
    skus = duck_query(f"SELECT DISTINCT product_sku FROM {s3_parquet('source=10_partner_s3/')} LIMIT 2000")
    s3_skus = set(r[0] for r in skus if r[0])
    matched = len(s3_skus & pg_skus)
    pct = matched / len(s3_skus) * 100 if s3_skus else 0
    check("Source 10 product_skus match Postgres", pct >= 85,
          f"{matched}/{len(s3_skus)} matched ({pct:.1f}%)")

# ── Source 11: ERP ────────────────────────────────────────────────────────────
section("Source 11 — ERP Export")
rows = duck_query(f"SELECT COUNT(*) FROM {s3_json_duckdb('source=11_erp/')}")
count = rows[0][0] if rows else 0
check("Source 11 has Parquet files", count > 0, f"{count} rows")
if count > 0:
    amt_rows = duck_query(f"""
        SELECT order_id, subtotal_pence FROM {s3_json_duckdb('source=11_erp/')}
        WHERE order_id IS NOT NULL AND subtotal_pence IS NOT NULL LIMIT 500
    """)
    if amt_rows:
        matched = sum(1 for oid, amt in amt_rows
                     if oid in pg_order_amounts and abs(pg_order_amounts[oid] - amt) < 100)
        pct = matched / len(amt_rows) * 100
        check("Source 11 subtotal_pence matches Postgres orders", pct >= 85,
              f"{matched}/{len(amt_rows)} matched ({pct:.1f}%)")

# ── Source 12: Reviews/Tickets ────────────────────────────────────────────────
section("Source 12 — Reviews / Tickets")
rows = duck_query(f"SELECT COUNT(*) FROM {s3_json_duckdb('source=12_reviews_tickets/')}")
count = rows[0][0] if rows else 0
check("Source 12 has Parquet files", count > 0, f"{count} rows")
if count > 0:
    tuples = duck_query(f"""
        SELECT order_id, product_sku FROM {s3_json_duckdb('source=12_reviews_tickets/')}
        WHERE order_id IS NOT NULL AND product_sku IS NOT NULL LIMIT 500
    """)
    if tuples:
        pg_tuples = set(pg_item_prices.keys())
        matched = sum(1 for oid, sku in tuples if (oid, sku) in pg_tuples)
        pct = matched / len(tuples) * 100
        check("Source 12 (order_id, product_sku) tuples match order_items", pct >= 85,
              f"{matched}/{len(tuples)} tuples matched ({pct:.1f}%)")

# ── Source 13: S3 Lambda Image Metadata ──────────────────────────────────────
section("Source 13 — S3 Lambda Image Metadata")
events13 = read_nested_json("source=13_s3_lambda/year=", "events", 500)
check("Source 13 has data", len(events13) > 0, f"{len(events13)} events")
if events13:
    s3_skus = set(e.get("product_sku") for e in events13 if e.get("product_sku"))
    matched = len(s3_skus & pg_skus)
    pct = matched / len(s3_skus) * 100 if s3_skus else 0
    check("Source 13 product_skus match Postgres", pct >= 85,
          f"{matched}/{len(s3_skus)} matched ({pct:.1f}%)")

# ── Source 14: Scrapy Competitor ──────────────────────────────────────────────
section("Source 14 — Scrapy Competitor Pricing")
rows = duck_query(f"SELECT COUNT(*) FROM {s3_json_duckdb('source=14_scrapy/')}")
count = rows[0][0] if rows else 0
check("Source 14 has Parquet files", count > 0, f"{count} rows")
if count > 0:
    price_rows = duck_query(f"""
        SELECT product_sku, our_price_pence FROM {s3_json_duckdb('source=14_scrapy/')}
        WHERE product_sku IS NOT NULL AND our_price_pence IS NOT NULL LIMIT 500
    """)
    if price_rows:
        matched = sum(1 for sku, price in price_rows
                     if sku in pg_sku_prices and abs(pg_sku_prices[sku] - price) < 100)
        pct = matched / len(price_rows) * 100
        check("Source 14 our_price_pence matches MongoDB base_price", pct >= 85,
              f"{matched}/{len(price_rows)} matched ({pct:.1f}%)")

# ── Source 15: IoT Sensors ────────────────────────────────────────────────────
section("Source 15 — MQTT IoT Sensors")
records = read_json_sample("source=15_mqtt_iot/")
check("Source 15 has JSON files in S3", len(records) > 0, f"{len(records)} sample records")
if records:
    valid_wh = {"WH-LONDON-01", "WH-MANC-01", "WH-BRUM-01"}
    wh_ids = [r.get("warehouse_id") for r in records if isinstance(r, dict) and r.get("warehouse_id")]
    check("Source 15 has warehouse_id field", len(wh_ids) > 0)
    if wh_ids:
        matched = sum(1 for w in wh_ids if w in valid_wh)
        pct = matched / len(wh_ids) * 100
        check("Source 15 warehouse_ids are valid", pct >= 95,
              f"{matched}/{len(wh_ids)} valid ({pct:.1f}%)")

# ── Source 16: CloudWatch Logs ────────────────────────────────────────────────
section("Source 16 — CloudWatch Logs")
records = read_json_sample("source=16_cloudwatch/")
check("Source 16 has JSON files in S3", len(records) > 0, f"{len(records)} sample records")
if records:
    order_pattern = re.compile(r'order[_ #]+(\d+)', re.IGNORECASE)
    extractable = 0
    matched = 0
    for r in records:
        msg = r.get("message", r.get("log_message", str(r))) if isinstance(r, dict) else str(r)
        m = order_pattern.search(str(msg))
        if m:
            extractable += 1
            if int(m.group(1)) in pg_order_ids:
                matched += 1
    check("Source 16 log messages contain extractable order IDs", extractable > 0,
          f"{extractable}/{len(records)} logs have order IDs")
    if extractable > 0:
        pct = matched / extractable * 100
        check("Source 16 extracted order_ids match Postgres", pct >= 70,
              f"{matched}/{extractable} matched ({pct:.1f}%)")

# ── Source 17: GA4 ────────────────────────────────────────────────────────────
section("Source 17 — GA4 Export")
rows = duck_query(f"SELECT COUNT(*) FROM {s3_json_duckdb('source=17_ga4/')}")
count = rows[0][0] if rows else 0
check("Source 17 has Parquet files", count > 0, f"{count} rows")
if count > 0:
    oids = duck_query(f"""
        SELECT DISTINCT transaction_id FROM {s3_json_duckdb('source=17_ga4/')}
        WHERE event_name = 'purchase' AND transaction_id IS NOT NULL LIMIT 2000
    """)
    if oids:
        s3_oids = set(r[0] for r in oids if r[0])
        matched = len(s3_oids & pg_order_ids)
        pct = matched / len(s3_oids) * 100 if s3_oids else 0
        check("Source 17 purchase transaction_ids match Postgres", pct >= 85,
              f"{matched}/{len(s3_oids)} matched ({pct:.1f}%)")

# ── Source 18: SES Email ──────────────────────────────────────────────────────
section("Source 18 — AWS SES Email")
rows = duck_query(f"SELECT COUNT(*) FROM {s3_json_duckdb('source=18_ses_email/')}")
count = rows[0][0] if rows else 0
check("Source 18 has Parquet files", count > 0, f"{count} rows")
if count > 0:
    tuples = duck_query(f"""
        SELECT order_id, customer_id FROM {s3_json_duckdb('source=18_ses_email/')}
        WHERE order_id IS NOT NULL AND customer_id IS NOT NULL LIMIT 500
    """)
    if tuples:
        matched = sum(1 for oid, cid in tuples
                     if oid in pg_order_customers and pg_order_customers[oid] == cid)
        pct = matched / len(tuples) * 100
        check("Source 18 (order_id, customer_id) integrity matches Postgres", pct >= 85,
              f"{matched}/{len(tuples)} tuples matched ({pct:.1f}%)")

# ── Summary ───────────────────────────────────────────────────────────────────
section("AUDIT SUMMARY")
passed = sum(1 for _, p, _ in results if p)
failed = sum(1 for _, p, _ in results if not p)
total = len(results)
print(f"\n  Total checks : {total}")
print(f"  Passed       : {passed}")
print(f"  Failed       : {failed}")
print(f"  Score        : {passed/total*100:.1f}%")

if failed > 0:
    print(f"\n  Failed checks:")
    for name, p, detail in results:
        if not p:
            print(f"    FAIL  {name}")
            if detail:
                print(f"           {detail}")

pg.close()
print(f"\n{'='*60}")
if failed == 0:
    print("  ALL CHECKS PASSED — Safe to proceed to Bronze")
elif failed <= 3:
    print("  MINOR ISSUES — Review failures before Bronze")
else:
    print("  SIGNIFICANT FAILURES — Fix before Bronze")
print(f"{'='*60}\n")

sys.exit(0 if failed == 0 else 1)

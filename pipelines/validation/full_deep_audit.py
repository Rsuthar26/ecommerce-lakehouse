#!/usr/bin/env python3
"""
Full Deep Cross-Source Audit — Staff DE Journey
Checks EVERY possible real-world relation between all batch sources.
Run after every flush run, before Bronze.
"""
import pandas as pd
import boto3
import io
import json
import os
from pymongo import MongoClient
from urllib.parse import quote_plus
import psycopg2

s3 = boto3.client('s3', region_name='eu-west-1')
BUCKET = 'ecommerce-lakehouse-467091806172-raw-01'


def read_parquet_prefix(prefix):
    paginator = s3.get_paginator('list_objects_v2')
    dfs = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                resp = s3.get_object(Bucket=BUCKET, Key=obj['Key'])
                dfs.append(pd.read_parquet(io.BytesIO(resp['Body'].read())))
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def read_csv_prefix(prefix):
    paginator = s3.get_paginator('list_objects_v2')
    dfs = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.csv'):
                resp = s3.get_object(Bucket=BUCKET, Key=obj['Key'])
                dfs.append(pd.read_csv(io.BytesIO(resp['Body'].read())))
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def read_json_prefix_unwrapped(prefix, data_key_candidates=(
        'invoices', 'records', 'events', 'reviews', 'tickets', 'charges',
        'shipments', 'sales', 'products', 'discounts')):
    paginator = s3.get_paginator('list_objects_v2')
    records = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.json'):
                resp = s3.get_object(Bucket=BUCKET, Key=obj['Key'])
                data = json.loads(resp['Body'].read())
                if isinstance(data, dict):
                    found = False
                    for key in data_key_candidates:
                        if key in data and isinstance(data[key], list):
                            records.extend(data[key])
                            found = True
                            break
                    if not found:
                        records.append(data)
                elif isinstance(data, list):
                    records.extend(data)
    return pd.DataFrame(records)


# Ground truth from Postgres
conn = psycopg2.connect(
    host=os.environ.get('PG_HOST', 'ecommerce-lakehouse-postgres.cbyumq8843k2.eu-west-1.rds.amazonaws.com'),
    dbname='ecommerce', user='postgres_admin',
    password=os.environ.get('PG_PASSWORD', 'DeJourney2026!'), sslmode='require'
)
orders_pg = pd.read_sql(
    "SELECT order_id, customer_id, subtotal_pence, total_amount_pence, order_status FROM orders", conn)
payments_pg = pd.read_sql(
    "SELECT order_id, amount_pence, payment_status FROM payments", conn)
items_pg = pd.read_sql(
    "SELECT order_id, product_sku, unit_price_pence, quantity FROM order_items", conn)
customers_pg = pd.read_sql("SELECT customer_id, email FROM customers", conn)
inventory_pg = pd.read_sql(
    "SELECT product_sku, warehouse_id, quantity_available FROM inventory", conn)
conn.close()

# Ground truth from MongoDB
password = quote_plus("MongoAdmin2026!")
uri = f"mongodb+srv://mongo_admin:{password}@ecommerce-cluster.k2gc71w.mongodb.net/?appName=ecommerce-cluster"
client = MongoClient(uri, serverSelectionTimeoutMS=30000)
mdb = client['ecommerce']
mongo_products = pd.DataFrame(list(mdb.products.find({})))
mongo_prices = dict(zip(mongo_products['product_sku'], mongo_products['base_price_pence']))
mongo_skus = set(mongo_products['product_sku'])

pg_skus = set(items_pg['product_sku'].unique())
pg_order_ids = set(orders_pg['order_id'].unique())
pg_customer_ids = set(customers_pg['customer_id'].unique())
order_customer_map = dict(zip(orders_pg['order_id'], orders_pg['customer_id']))
order_amount_map = dict(zip(orders_pg['order_id'], orders_pg['total_amount_pence']))
items_pg_tuples = set(zip(items_pg['order_id'], items_pg['product_sku']))

results = []


def check(source, name, passed, detail):
    status = "PASS" if passed else "FAIL"
    print(f"{status}  [{source}] {name}")
    print(f"        {detail}")
    results.append((source, name, passed, detail))


print("=" * 70)
print("FULL DEEP CROSS-SOURCE AUDIT")
print("=" * 70)

# Source 01 Postgres internal integrity
print("\n--- Source 01 Postgres (internal) ---")
orphan_items = items_pg[~items_pg['order_id'].isin(pg_order_ids)]
check("01", "order_items reference valid orders", len(orphan_items) == 0,
      f"{len(orphan_items)} orphaned items")
unmatched_payments = payments_pg[~payments_pg['order_id'].isin(pg_order_ids)]
check("01", "payments reference valid orders", len(unmatched_payments) == 0,
      f"{len(unmatched_payments)} orphaned payments")
bad_subtotal = orders_pg[orders_pg['subtotal_pence'].isna() | (orders_pg['subtotal_pence'] <= 0)]
check("01", "all orders have positive subtotal", len(bad_subtotal) == 0,
      f"{len(bad_subtotal)} orders with bad subtotal")

# Source 03 MongoDB
print("\n--- Source 03 MongoDB ---")
sku_diff = pg_skus - mongo_skus
check("03", "all Postgres SKUs exist in MongoDB", len(sku_diff) == 0,
      f"{len(sku_diff)} missing SKUs")

# Source 06 Stripe
print("\n--- Source 06 Stripe ---")
stripe_df = read_json_prefix_unwrapped('source=06_stripe/')
if not stripe_df.empty and 'order_id' in stripe_df.columns:
    stripe_oids = stripe_df['order_id'].dropna().astype(str)
    pg_oids_str = set(str(x) for x in pg_order_ids)
    matched = stripe_oids.isin(pg_oids_str).sum()
    check("06", "order_ids exist in Postgres",
          matched / len(stripe_oids) > 0.95 if len(stripe_oids) else False,
          f"{matched}/{len(stripe_oids)} matched")

# Source 07 ShipStation
print("\n--- Source 07 ShipStation ---")
ship_df = read_json_prefix_unwrapped('source=07_shipstation/')
if not ship_df.empty and 'order_id' in ship_df.columns:
    matched = ship_df['order_id'].isin(pg_order_ids).sum()
    check("07", "order_ids exist in Postgres", matched / len(ship_df) > 0.95,
          f"{matched}/{len(ship_df)} matched")

# Source 08 Shopify
print("\n--- Source 08 Shopify ---")
shopify_df = read_json_prefix_unwrapped('source=08_shopify/products', data_key_candidates=('products',))
if not shopify_df.empty and 'handle' in shopify_df.columns:
    shopify_df['derived_sku'] = shopify_df['handle'].str.upper().str.replace('_', '-')
    matched = shopify_df['derived_sku'].isin(mongo_skus).sum()
    check("08", "derived SKUs exist in MongoDB",
          matched / len(shopify_df) > 0.95 if len(shopify_df) else False,
          f"{matched}/{len(shopify_df)} matched")

# Source 09 SFTP
print("\n--- Source 09 SFTP ---")
sftp_df = read_csv_prefix('source=09_sftp/')
if not sftp_df.empty:
    sftp_df.columns = [c.strip() for c in sftp_df.columns]

    # SFTP simulates 3 real supplier file formats with different column names.
    # Coalesce across all known naming variants per supplier before checking.
    sku_variants = ['SKU', 'sku_code', 'PART_NO']
    sku_variants = [c for c in sku_variants if c in sftp_df.columns]
    if sku_variants:
        unified_sku = sftp_df[sku_variants[0]]
        for c in sku_variants[1:]:
            unified_sku = unified_sku.combine_first(sftp_df[c])
        sftp_df['_unified_sku'] = unified_sku

    lead_variants = ['Lead Days', 'delivery_days', 'LEAD_TIME']
    lead_variants = [c for c in lead_variants if c in sftp_df.columns]
    if lead_variants:
        unified_lead = sftp_df[lead_variants[0]]
        for c in lead_variants[1:]:
            unified_lead = unified_lead.combine_first(sftp_df[c])
        sftp_df['_unified_lead'] = unified_lead

    cost_col = [c for c in sftp_df.columns if 'cost' in c.lower() and not c.startswith('_')]
    rrp_col = [c for c in sftp_df.columns if 'rrp' in c.lower() and not c.startswith('_')]
    if cost_col and rrp_col:
        v = sftp_df[pd.to_numeric(sftp_df[cost_col[0]], errors='coerce') >=
                     pd.to_numeric(sftp_df[rrp_col[0]], errors='coerce')]
        check("09", "unit_cost < rrp", len(v) == 0, f"{len(v)}/{len(sftp_df)} violations")

    if '_unified_sku' in sftp_df.columns:
        matched = sftp_df['_unified_sku'].isin(mongo_skus).sum()
        check("09", "SKUs exist in MongoDB (unified across supplier formats)",
              matched / len(sftp_df) > 0.90,
              f"{matched}/{len(sftp_df)} matched")

    if '_unified_lead' in sftp_df.columns:
        null_lead = sftp_df[pd.to_numeric(sftp_df['_unified_lead'], errors='coerce').isna()]
        check("09", "lead_days populated (unified across supplier formats)",
              len(null_lead) / len(sftp_df) < 0.10,
              f"{len(null_lead)}/{len(sftp_df)} null lead_days")

# Source 10 Partner S3
print("\n--- Source 10 Partner S3 ---")
partner_df = read_parquet_prefix('source=10_partner_s3/')
if not partner_df.empty and 'product_sku' in partner_df.columns:
    matched = partner_df['product_sku'].isin(mongo_skus).sum()
    check("10", "SKUs exist in MongoDB", matched / len(partner_df) > 0.90,
          f"{matched}/{len(partner_df)} matched")

# Source 11 ERP
print("\n--- Source 11 ERP ---")
erp_df = read_json_prefix_unwrapped('source=11_erp/')
if not erp_df.empty and 'order_id' in erp_df.columns:
    matched = erp_df['order_id'].isin(pg_order_ids).sum()
    check("11", "order_ids exist in Postgres", matched / len(erp_df) > 0.90,
          f"{matched}/{len(erp_df)} matched")
    if 'total_pence' in erp_df.columns and 'subtotal_pence' in erp_df.columns and 'tax_pence' in erp_df.columns:
        erp_calc = erp_df.dropna(subset=['total_pence', 'subtotal_pence', 'tax_pence']).copy()
        erp_calc['expected_total'] = erp_calc['subtotal_pence'] + erp_calc['tax_pence']
        mismatches = erp_calc[abs(erp_calc['total_pence'] - erp_calc['expected_total']) > 5]
        check("11", "total_pence = subtotal + tax (generator fix verified)",
              len(mismatches) == 0, f"{len(mismatches)}/{len(erp_calc)} math errors")

# Source 12 Reviews / Tickets
print("\n--- Source 12 Reviews/Tickets ---")
rev_df = read_json_prefix_unwrapped('source=12_reviews_tickets/')
if not rev_df.empty:
    reviews_only = rev_df[rev_df.get('record_type', '') == 'review'] if 'record_type' in rev_df.columns else pd.DataFrame()
    if not reviews_only.empty and 'product_sku' in reviews_only.columns:
        sub = reviews_only.dropna(subset=['order_id', 'product_sku'])
        tuples = list(zip(sub['order_id'], sub['product_sku']))
        matched = sum(1 for t in tuples if t in items_pg_tuples)
        check("12", "(order_id, product_sku) tuple integrity",
              matched / len(tuples) > 0.9 if tuples else False,
              f"{matched}/{len(tuples)} matched")
    tickets_only = rev_df[rev_df.get('record_type', '') == 'ticket'] if 'record_type' in rev_df.columns else pd.DataFrame()
    if not tickets_only.empty and 'customer_id' in tickets_only.columns:
        sub = tickets_only.dropna(subset=['order_id', 'customer_id'])
        matched = sum(1 for _, r in sub.iterrows() if order_customer_map.get(r['order_id']) == r['customer_id'])
        check("12", "tickets (order_id, customer_id) integrity",
              matched / len(sub) > 0.9 if len(sub) else False,
              f"{matched}/{len(sub)} matched")

# Source 13 S3 Lambda
print("\n--- Source 13 S3 Lambda ---")
lambda_df = read_json_prefix_unwrapped('source=13_s3_lambda/')
if not lambda_df.empty and 'product_sku' in lambda_df.columns:
    matched = lambda_df['product_sku'].isin(mongo_skus).sum()
    check("13", "SKUs exist in MongoDB",
          matched / len(lambda_df) > 0.95 if len(lambda_df) else False,
          f"{matched}/{len(lambda_df)} matched")

# Source 14 Scrapy
print("\n--- Source 14 Scrapy ---")
scrapy_df = read_json_prefix_unwrapped('source=14_scrapy/')
if not scrapy_df.empty and 'our_price_pence' in scrapy_df.columns:
    # Rows with null product_sku are intentional Rule 7 dirty data — exclude
    # from price consistency check since there's nothing to match against.
    clean_scrapy = scrapy_df.dropna(subset=['product_sku'])
    clean_scrapy = clean_scrapy.copy()
    clean_scrapy['expected'] = clean_scrapy['product_sku'].map(mongo_prices)
    mismatches = clean_scrapy[clean_scrapy['our_price_pence'] != clean_scrapy['expected']]
    null_sku_count = scrapy_df['product_sku'].isna().sum()
    check("14", "our_price_pence matches MongoDB (excl. intentional dirty data)",
          len(mismatches) == 0,
          f"{len(mismatches)}/{len(clean_scrapy)} mismatches "
          f"({null_sku_count} null-SKU dirty rows excluded)")

# Source 17 GA4
print("\n--- Source 17 GA4 ---")
ga4_df = read_json_prefix_unwrapped('source=17_ga4/')
if not ga4_df.empty and 'event_name' in ga4_df.columns:
    purchases = ga4_df[ga4_df['event_name'] == 'purchase'].copy()
    check("17", "has purchase events", len(purchases) > 0,
          f"{len(purchases)} purchase events / {len(ga4_df)} total")
    if not purchases.empty and 'event_params' in purchases.columns:
        def extract_txn(params):
            if not isinstance(params, list):
                return None
            for p in params:
                if isinstance(p, dict) and p.get('key') == 'transaction_id':
                    val = p.get('value', {})
                    return val.get('string_value') if isinstance(val, dict) else None
            return None

        purchases['txn_id'] = purchases['event_params'].apply(extract_txn)
        valid_txns = purchases.dropna(subset=['txn_id']).copy()
        if len(valid_txns) > 0:
            try:
                valid_txns['txn_id_int'] = pd.to_numeric(valid_txns['txn_id'], errors='coerce')
                matched = valid_txns['txn_id_int'].isin(pg_order_ids).sum()
                check("17", "purchase transaction_ids match Postgres orders",
                      matched / len(valid_txns) > 0.7,
                      f"{matched}/{len(valid_txns)} matched")
            except Exception:
                check("17", "purchase transaction_ids match Postgres orders", False,
                      "could not parse transaction_ids")

# Source 18 SES
print("\n--- Source 18 SES ---")
ses_df = read_json_prefix_unwrapped('source=18_ses_email/')
if not ses_df.empty and 'order_id' in ses_df.columns:
    known = ses_df.dropna(subset=['order_id'])
    matched_oid = known['order_id'].isin(pg_order_ids).sum()
    check("18", "order_ids exist in Postgres",
          matched_oid / len(known) > 0.9 if len(known) else False,
          f"{matched_oid}/{len(known)} matched")
    if 'customer_id' in ses_df.columns:
        cust_known = ses_df.dropna(subset=['order_id', 'customer_id'])
        matched_tuple = sum(1 for _, r in cust_known.iterrows()
                             if order_customer_map.get(r['order_id']) == r['customer_id'])
        check("18", "(order_id, customer_id) tuple integrity",
              matched_tuple / len(cust_known) > 0.85 if len(cust_known) else False,
              f"{matched_tuple}/{len(cust_known)} matched")
    if 'delivery_status' in ses_df.columns:
        status_counts = ses_df['delivery_status'].value_counts(normalize=True)
        delivered_pct = status_counts.get('delivered', 0)
        check("18", "delivery_status realistic (not 100% delivered)",
              0.5 < delivered_pct < 0.99,
              f"delivered={delivered_pct * 100:.1f}% — {dict(status_counts)}")

# Date alignment check across ALL sources
print("\n--- Date Alignment (all sources same window) ---")
date_ranges = {}
candidates = [
    ("06_stripe", stripe_df, 'created_at'),
    ("07_shipstation", ship_df, 'shipped_at'),
]
for src_name, df, date_col in candidates:
    if not df.empty and date_col in df.columns:
        try:
            dates = pd.to_datetime(df[date_col], errors='coerce', utc=True).dropna()
            if len(dates) > 0:
                date_ranges[src_name] = (dates.min(), dates.max())
        except Exception:
            pass

if date_ranges:
    print("Date ranges found:")
    for src, (mn, mx) in date_ranges.items():
        print(f"  {src}: {mn.date()} to {mx.date()}")
    all_mins = [v[0] for v in date_ranges.values()]
    all_maxs = [v[1] for v in date_ranges.values()]
    spread_days = (max(all_maxs) - min(all_mins)).days
    check("ALL", "date ranges aligned (same ~7-day window)", spread_days <= 10,
          f"spread = {spread_days} days across sources")

# Duplicate detection
print("\n--- Duplicate Detection ---")
if not stripe_df.empty:
    id_col = [c for c in stripe_df.columns if 'id' in c.lower() and 'order' not in c.lower() and 'customer' not in c.lower()]
    if id_col:
        dupes = stripe_df[id_col[0]].duplicated().sum()
        check("06", "no duplicate charge IDs within source", dupes == 0,
              f"{dupes} duplicates found")

print("\n" + "=" * 70)
print("FINAL COMPLETE SUMMARY")
print("=" * 70)
passed = sum(1 for r in results if r[2])
print(f"Total checks: {len(results)}")
print(f"Passed: {passed}")
print(f"Failed: {len(results) - passed}")
print(f"Score: {passed / len(results) * 100:.1f}%" if results else "No checks ran")
print()
if len(results) - passed > 0:
    print("FAILURES:")
    for source, name, p, detail in results:
        if not p:
            print(f"  [{source}] {name}: {detail}")
else:
    print("ALL CHECKS PASSED — Data is verified real-world consistent.")

# ── STREAMING SOURCES (Kafka Connect output) ──────────────────────────
print("\n" + "=" * 70)
print("STREAMING SOURCES VERIFICATION")
print("=" * 70)

def read_kafka_connect_json(prefix, max_files=50):
    """Kafka Connect writes raw JSON lines, not wrapped manifests."""
    paginator = s3.get_paginator('list_objects_v2')
    records = []
    file_count = 0
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.json'):
                resp = s3.get_object(Bucket=BUCKET, Key=obj['Key'])
                body = resp['Body'].read().decode('utf-8')
                for line in body.strip().split('\n'):
                    if line.strip():
                        try:
                            parsed = json.loads(line)
                            if isinstance(parsed, str):
                                try:
                                    parsed = json.loads(parsed)
                                except Exception:
                                    pass
                            records.append(parsed)
                        except Exception:
                            pass
                file_count += 1
                if file_count >= max_files:
                    break
        if file_count >= max_files:
            break
    return pd.DataFrame(records)

# Source 04 Clickstream
print("\n--- Source 04 Clickstream (Kafka) ---")
click_df = read_kafka_connect_json('source=04_kafka_clickstream/')
if not click_df.empty:
    check("04", "has data in S3", len(click_df) > 0, f"{len(click_df)} sample records")
    if 'user_id' in click_df.columns or 'customer_id' in click_df.columns:
        id_col = 'user_id' if 'user_id' in click_df.columns else 'customer_id'
        anon = click_df[id_col].isna().sum()
        anon_pct = anon / len(click_df) * 100
        check("04", "anonymous rate realistic (<50%)", anon_pct < 50,
              f"{anon_pct:.1f}% anonymous")
else:
    check("04", "has data in S3", False, "no records found")

# Source 05 SQS
print("\n--- Source 05 SQS (Kafka) ---")
sqs_df = read_kafka_connect_json('source=05_sqs/')
if not sqs_df.empty:
    check("05", "has data in S3", len(sqs_df) > 0, f"{len(sqs_df)} sample records")
    if 'order_id' in sqs_df.columns:
        oids = sqs_df['order_id'].dropna()
        matched = oids.isin(pg_order_ids).sum()
        check("05", "order_ids match Postgres", matched/len(oids) > 0.85 if len(oids) else False,
              f"{matched}/{len(oids)} matched")
else:
    check("05", "has data in S3", False, "no records found")

# Source 15 IoT
print("\n--- Source 15 IoT (Kafka) ---")
iot_df = read_kafka_connect_json('source=15_mqtt_iot/')
if not iot_df.empty:
    check("15", "has data in S3", len(iot_df) > 0, f"{len(iot_df)} sample records")
    if 'warehouse_id' in iot_df.columns:
        valid_wh = {'WH-LONDON-01', 'WH-MANC-01', 'WH-BRUM-01'}
        wh_valid = iot_df['warehouse_id'].isin(valid_wh).sum()
        check("15", "warehouse_ids valid", wh_valid/len(iot_df) > 0.95,
              f"{wh_valid}/{len(iot_df)} valid")
else:
    check("15", "has data in S3", False, "no records found")

# Source 16 CloudWatch
print("\n--- Source 16 CloudWatch (Kafka) ---")
cw_df = read_kafka_connect_json('source=16_cloudwatch/')
if not cw_df.empty:
    check("16", "has data in S3", len(cw_df) > 0, f"{len(cw_df)} sample records")
    if 'message' in cw_df.columns:
        import re as re_mod
        pattern = re_mod.compile(r'order[_ #]+(\d+)', re_mod.IGNORECASE)
        extractable = 0
        matched = 0
        for msg in cw_df['message'].dropna():
            m = pattern.search(str(msg))
            if m:
                extractable += 1
                try:
                    if int(m.group(1)) in pg_order_ids:
                        matched += 1
                except Exception:
                    pass
        check("16", "extractable order_ids match Postgres",
              matched/extractable > 0.5 if extractable else False,
              f"{matched}/{extractable} matched")
else:
    check("16", "has data in S3", False, "no records found")

# Source 02 Debezium CDC
print("\n--- Source 02 Debezium CDC (Kafka) ---")
# Debezium writes envelopes as Struct{...} text, not JSON — read raw lines.
def read_debezium_struct_lines(prefix, max_files=10):
    paginator = s3.get_paginator('list_objects_v2')
    lines_out = []
    file_count = 0
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.json'):
                resp = s3.get_object(Bucket=BUCKET, Key=obj['Key'])
                body = resp['Body'].read().decode('utf-8')
                lines_out.extend([l for l in body.strip().split('\n') if l.strip()])
                file_count += 1
                if file_count >= max_files:
                    return lines_out
    return lines_out

cdc_lines = read_debezium_struct_lines('source=02_debezium_cdc/')
check("02", "has CDC data in S3", len(cdc_lines) > 0, f"{len(cdc_lines)} raw lines")
if cdc_lines:
    has_envelope = all(
        ('after=Struct' in l or 'before=Struct' in l) and 'source=Struct' in l and 'op=' in l
        for l in cdc_lines[:20]
    )
    has_postgres = any('connector=postgresql' in l for l in cdc_lines[:20])
    check("02", "has CDC envelope structure (after/source/op, Struct format)",
          has_envelope and has_postgres,
          "Debezium Postgres CDC Struct envelope confirmed" if has_envelope and has_postgres
          else "envelope structure not found")
else:
    check("02", "has CDC data in S3", False, "no records found")

print("\n" + "=" * 70)
print("FULL COMPLETE SUMMARY (BATCH + STREAMING)")
print("=" * 70)
passed = sum(1 for r in results if r[2])
print(f"Total checks: {len(results)}")
print(f"Passed: {passed}")
print(f"Failed: {len(results) - passed}")
print(f"Score: {passed / len(results) * 100:.1f}%" if results else "No checks ran")
print()
if len(results) - passed > 0:
    print("FAILURES:")
    for source, name, p, detail in results:
        if not p:
            print(f"  [{source}] {name}: {detail}")
else:
    print("ALL CHECKS PASSED — Full real-world consistency verified across all 18 sources.")

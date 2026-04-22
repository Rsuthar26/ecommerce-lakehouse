"""
pipelines/ingestion/03_mongodb_to_s3_raw.py — Staff DE Journey

Production-grade incremental ingestion from MongoDB Atlas → S3 Raw.

Collections ingested:
  - products             (~200 docs, full catalog)
  - product_price_history (~150+ docs, price changes)

Incremental pattern (same as Source 01):
  - First run: full extract
  - Every run after: find({updated_at: {$gt: last_watermark}})
  - Watermark stored in S3 as JSON state file

Key differences from Source 01 (Postgres):
  - PyMongo not psycopg2
  - Query filter uses $gt operator not SQL WHERE
  - Timestamps from MongoDB are Python datetime objects with UTC timezone
  - Documents are nested (variants array, attributes object) — 
    we flatten to Parquet by serialising nested fields as JSON strings
  - _id field is a string (SKU) not an integer — no special handling needed

Why flatten nested documents?
  Parquet is columnar — it needs flat schemas.
  Nested arrays/objects must be serialised to JSON strings.
  The Bronze → Silver step in Databricks will parse and explode them.

Partition path:
  s3://bucket/source=03_mongodb/year=YYYY/month=MM/day=DD/
              collection=<name>/<collection>_<start>_to_<end>.parquet

Usage:
  python 03_mongodb_to_s3_raw.py
  python 03_mongodb_to_s3_raw.py --full-load
  python 03_mongodb_to_s3_raw.py --collection products
  python 03_mongodb_to_s3_raw.py --dry-run
"""

import os
import io
import sys
import json
import logging
import argparse
from datetime import datetime, timezone

import boto3
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────

RAW_BUCKET    = os.environ.get("S3_RAW_BUCKET", "ecommerce-lakehouse-467091806172-raw-01")
SOURCE_PREFIX = "source=03_mongodb"
WATERMARK_KEY = f"{SOURCE_PREFIX}/_metadata/watermark.json"
AWS_REGION    = os.environ.get("AWS_DEFAULT_REGION", "eu-north-1")
DB_NAME       = "ecommerce"

COLLECTIONS = {
    "products": {
        "incremental_col": "updated_at",
        "nested_fields":   ["variants", "attributes", "images", "tags"],
    },
    "product_price_history": {
        "incremental_col": "changed_at",
        "nested_fields":   [],
    },
}


# ─────────────────────────────────────────────────────────────
# WATERMARK
# ─────────────────────────────────────────────────────────────

def load_watermark(s3_client) -> dict:
    try:
        response  = s3_client.get_object(Bucket=RAW_BUCKET, Key=WATERMARK_KEY)
        watermark = json.loads(response["Body"].read())
        log.info(f"Loaded watermark from s3://{RAW_BUCKET}/{WATERMARK_KEY}")
        for col, ts in watermark.items():
            log.info(f"  {col}: last run = {ts}")
        return watermark
    except Exception:
        log.info("No watermark found — first run (full extract)")
        return {}


def save_watermark(s3_client, watermark: dict):
    s3_client.put_object(
        Bucket=RAW_BUCKET,
        Key=WATERMARK_KEY,
        Body=json.dumps(watermark, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    log.info(f"Watermark saved to s3://{RAW_BUCKET}/{WATERMARK_KEY}")


# ─────────────────────────────────────────────────────────────
# CONNECTIONS
# ─────────────────────────────────────────────────────────────

def get_mongo_client() -> MongoClient:
    uri = os.environ["MONGO_URI"]
    return MongoClient(uri, serverSelectionTimeoutMS=10000)


def get_s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


# ─────────────────────────────────────────────────────────────
# EXTRACTION
# ─────────────────────────────────────────────────────────────

def flatten_document(doc: dict, nested_fields: list) -> dict:
    """
    Flatten a MongoDB document for Parquet serialisation.

    Nested arrays/objects (variants, attributes, images, tags) are
    serialised to JSON strings. Databricks Silver layer will parse these.

    Why not use pandas json_normalize?
      json_normalize creates one column per nested key — 'attributes.brand',
      'attributes.warranty_years' etc. This breaks when different documents
      have different attribute keys (which our products do).
      JSON string is safer and more flexible.
    """
    flat = {}
    for key, value in doc.items():
        if key in nested_fields:
            # Serialise nested field to JSON string
            flat[key] = json.dumps(value, default=str)
        elif isinstance(value, datetime):
            flat[key] = value
        elif isinstance(value, dict):
            # Unexpected nested dict — serialise it
            flat[key] = json.dumps(value, default=str)
        elif isinstance(value, list):
            # Unexpected list — serialise it
            flat[key] = json.dumps(value, default=str)
        else:
            flat[key] = value
    return flat


def extract_collection(db, collection_name: str, incremental_col: str,
                       nested_fields: list,
                       last_watermark: str | None) -> tuple:
    """
    Extract documents from a MongoDB collection incrementally.

    MongoDB query filter:
      Full load:   {} (no filter — all documents)
      Incremental: {incremental_col: {$gt: datetime(last_watermark)}}

    Returns (DataFrame, high_watermark, data_start, data_end)
    """
    collection = db[collection_name]

    if last_watermark:
        # Parse watermark string back to datetime for MongoDB query
        # MongoDB $gt operator requires actual datetime, not string
        wm_dt = datetime.fromisoformat(last_watermark)
        if wm_dt.tzinfo is None:
            wm_dt = wm_dt.replace(tzinfo=timezone.utc)

        query_filter = {incremental_col: {"$gt": wm_dt}}
        log.info(f"  Incremental: {collection_name} WHERE {incremental_col} > {last_watermark}")
    else:
        query_filter = {}
        log.info(f"  Full extract: {collection_name}")

    docs  = list(collection.find(query_filter).sort(incremental_col, 1))

    if not docs:
        log.info(f"  No new documents in {collection_name} since last run")
        return pd.DataFrame(), last_watermark, None, None

    # Flatten documents for Parquet
    flat_docs = [flatten_document(doc, nested_fields) for doc in docs]
    df        = pd.DataFrame(flat_docs)

    # Convert _id (ObjectId or string) to string
    if "_id" in df.columns:
        df["_id"] = df["_id"].astype(str)

    # Calculate watermark and data date range
    high_ts    = df[incremental_col].max()
    low_ts     = df[incremental_col].min()

    high_wm    = high_ts.isoformat() if hasattr(high_ts, "isoformat") else str(high_ts)
    data_start = low_ts.strftime("%Y%m%d")  if hasattr(low_ts,  "strftime") else str(low_ts)[:10].replace("-","")
    data_end   = high_ts.strftime("%Y%m%d") if hasattr(high_ts, "strftime") else str(high_ts)[:10].replace("-","")

    log.info(f"  ✓ {len(df):,} docs | data: {data_start} → {data_end} | watermark: {high_wm}")
    return df, high_wm, data_start, data_end


# ─────────────────────────────────────────────────────────────
# S3 WRITE
# ─────────────────────────────────────────────────────────────

def build_s3_key(collection_name: str, run_ts: datetime,
                 data_start: str, data_end: str) -> str:
    return (
        f"{SOURCE_PREFIX}/"
        f"year={run_ts.strftime('%Y')}/"
        f"month={run_ts.strftime('%m')}/"
        f"day={run_ts.strftime('%d')}/"
        f"collection={collection_name}/"
        f"{collection_name}_{data_start}_to_{data_end}.parquet"
    )


def df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, compression="snappy", engine="pyarrow")
    buffer.seek(0)
    return buffer.read()


def write_to_s3(s3_client, bucket: str, key: str, data: bytes,
                run_ts: datetime) -> str:
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
        ContentType="application/octet-stream",
        Metadata={
            "source":      "03_mongodb",
            "ingested_at": run_ts.isoformat(),
        }
    )
    return f"s3://{bucket}/{key}"


def verify_s3_write(s3_client, bucket: str, key: str, expected_bytes: int) -> bool:
    response = s3_client.head_object(Bucket=bucket, Key=key)
    actual   = response["ContentLength"]
    if actual == expected_bytes:
        log.info(f"  ✓ Verified: {actual:,} bytes in S3")
        return True
    log.error(f"  ✗ Size mismatch: expected {expected_bytes}, got {actual}")
    return False


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def run(collections_to_run: dict, full_load: bool = False, dry_run: bool = False):
    run_ts = datetime.now(timezone.utc)
    log.info(f"Source 03 → S3 Raw | run_ts={run_ts.isoformat()}")
    log.info(f"Mode: {'FULL LOAD' if full_load else 'INCREMENTAL'} | dry_run={dry_run}")
    log.info(f"Target: s3://{RAW_BUCKET}/{SOURCE_PREFIX}/")

    mongo_client = get_mongo_client()
    db           = mongo_client[DB_NAME]
    s3_client    = None if dry_run else get_s3_client()

    watermarks     = {} if full_load else (load_watermark(s3_client) if not dry_run else {})
    new_watermarks = dict(watermarks)

    stats = {"collections": 0, "docs": 0, "bytes": 0, "skipped": 0, "files": []}

    try:
        for coll_name, config in collections_to_run.items():
            log.info(f"\n── {coll_name} ──")

            last_wm = None if full_load else watermarks.get(coll_name)

            df, high_wm, data_start, data_end = extract_collection(
                db, coll_name,
                config["incremental_col"],
                config["nested_fields"],
                last_wm
            )

            if df.empty:
                stats["skipped"] += 1
                continue

            parquet_bytes = df_to_parquet_bytes(df)
            log.info(f"  Parquet: {len(parquet_bytes)/1024:.1f} KB")

            if dry_run:
                key = build_s3_key(coll_name, run_ts, data_start, data_end)
                log.info(f"  [DRY RUN] Would write: {key}")
                log.info(f"  [DRY RUN] Would update watermark to: {high_wm}")
                continue

            key = build_s3_key(coll_name, run_ts, data_start, data_end)
            uri = write_to_s3(s3_client, RAW_BUCKET, key, parquet_bytes, run_ts)
            log.info(f"  Written: {uri}")

            if not verify_s3_write(s3_client, RAW_BUCKET, key, len(parquet_bytes)):
                raise RuntimeError(f"S3 write verification failed for {coll_name}")

            new_watermarks[coll_name] = high_wm

            stats["collections"] += 1
            stats["docs"]        += len(df)
            stats["bytes"]       += len(parquet_bytes)
            stats["files"].append(uri)

    except Exception as e:
        log.error(f"Ingestion failed: {e}")
        log.error("Watermark NOT updated")
        raise
    finally:
        mongo_client.close()

    if not dry_run and stats["collections"] > 0:
        save_watermark(s3_client, new_watermarks)

    log.info(f"\n✓ INGESTION COMPLETE")
    log.info(f"  Collections processed: {stats['collections']}")
    log.info(f"  Collections skipped:   {stats['skipped']}")
    log.info(f"  Docs:  {stats['docs']:,}")
    log.info(f"  Size:  {stats['bytes'] / 1024:.1f} KB")
    if stats["files"]:
        log.info(f"  Files written:")
        for f in stats["files"]:
            log.info(f"    {f}")

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Source 03 MongoDB Atlas → S3 Raw (incremental)"
    )
    parser.add_argument("--collection", help="Run single collection only")
    parser.add_argument("--full-load",  action="store_true")
    parser.add_argument("--dry-run",    action="store_true")
    args = parser.parse_args()

    if args.collection and args.collection not in COLLECTIONS:
        log.error(f"Unknown collection: {args.collection}. Valid: {list(COLLECTIONS.keys())}")
        sys.exit(1)

    collections = {args.collection: COLLECTIONS[args.collection]} \
        if args.collection else COLLECTIONS

    run(collections, full_load=args.full_load, dry_run=args.dry_run)


if __name__ == "__main__":
    main()

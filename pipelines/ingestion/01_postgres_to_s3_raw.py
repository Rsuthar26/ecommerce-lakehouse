"""
pipelines/ingestion/01_postgres_to_s3_raw.py

Production-grade incremental ingestion from RDS PostgreSQL → S3 Raw.

Incremental pattern:
  - First run: full extract (no watermark exists yet)
  - Every run after: WHERE updated_at > last_watermark
  - Watermark stored in S3 as a JSON state file
  - On success: watermark updated to current run time
  - On failure: watermark NOT updated — next run reprocesses from last good state

Why S3 for watermark storage (not a database)?
  - No additional infrastructure needed
  - Survives Lambda/container restarts
  - Easy to inspect and manually reset if needed
  - Consistent with how Airflow stores task state in production

Partition path (project rules):
  s3://bucket/source=01_postgres/year=YYYY/month=MM/day=DD/table=<n>/<ts>.parquet

Incremental files are APPENDED — never overwritten.
Databricks Autoloader detects new files and processes only those.

Usage:
  python 01_postgres_to_s3_raw.py                    # incremental run
  python 01_postgres_to_s3_raw.py --full-load        # force full extract
  python 01_postgres_to_s3_raw.py --table orders     # single table
  python 01_postgres_to_s3_raw.py --dry-run          # print stats only
"""

import os
import io
import sys
import json
import logging
import argparse
from datetime import datetime, timezone, timedelta

import boto3
import pandas as pd
import psycopg2
import psycopg2.extras
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
SOURCE_PREFIX = "source=01_postgres"
WATERMARK_KEY = f"{SOURCE_PREFIX}/_metadata/watermark.json"
AWS_REGION    = os.environ.get("AWS_DEFAULT_REGION", "eu-north-1")

# Tables and their incremental column
# All tables use updated_at — set by triggers in schema.sql
TABLES = {
    "customers":   {"incremental_col": "updated_at"},
    "orders":      {"incremental_col": "updated_at"},
    "order_items": {"incremental_col": "updated_at"},
    "payments":    {"incremental_col": "updated_at"},
    "inventory":   {"incremental_col": "updated_at"},
}


# ─────────────────────────────────────────────────────────────
# WATERMARK MANAGEMENT
# Tracks the last successful run per table in S3
# ─────────────────────────────────────────────────────────────

def load_watermark(s3_client) -> dict:
    """
    Load the watermark state from S3.
    Returns empty dict if no watermark exists (first run).

    Watermark format:
    {
      "customers":   "2026-04-21T22:59:10+00:00",
      "orders":      "2026-04-21T22:59:10+00:00",
      ...
    }
    """
    try:
        response = s3_client.get_object(Bucket=RAW_BUCKET, Key=WATERMARK_KEY)
        watermark = json.loads(response["Body"].read())
        log.info(f"Loaded watermark from s3://{RAW_BUCKET}/{WATERMARK_KEY}")
        for table, ts in watermark.items():
            log.info(f"  {table}: last run = {ts}")
        return watermark
    except s3_client.exceptions.NoSuchKey:
        log.info("No watermark found — this is a first run (full extract)")
        return {}
    except Exception as e:
        log.warning(f"Could not load watermark: {e} — defaulting to full extract")
        return {}


def save_watermark(s3_client, watermark: dict):
    """
    Save updated watermark to S3 after successful ingestion.
    Only called on success — failure leaves watermark unchanged.
    """
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

def get_pg_conn():
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", 5432)),
        dbname=os.environ.get("DB_NAME", "ecommerce"),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        connect_timeout=10,
    )


def get_s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


# ─────────────────────────────────────────────────────────────
# EXTRACTION
# ─────────────────────────────────────────────────────────────

def extract_table(conn, table_name: str, incremental_col: str,
                  last_watermark: str | None) -> tuple[pd.DataFrame, str]:
    """
    Extract rows from a table incrementally.

    If last_watermark is None: full extract (first run)
    If last_watermark exists: WHERE updated_at > last_watermark

    Returns (DataFrame, high_watermark) where high_watermark is the
    maximum updated_at seen in this batch — used to update the state file.

    Why MAX(updated_at) and not NOW()?
      If we store NOW() as the watermark but the query took 5 minutes,
      any rows that changed during those 5 minutes would be missed.
      MAX(updated_at) from the actual data is safer.
    """
    if last_watermark:
        query = f"""
            SELECT *
            FROM {table_name}
            WHERE {incremental_col} > '{last_watermark}'
            ORDER BY {incremental_col} ASC
        """
        log.info(f"  Incremental extract: {table_name} WHERE {incremental_col} > {last_watermark}")
    else:
        query = f"SELECT * FROM {table_name} ORDER BY {incremental_col} ASC"
        log.info(f"  Full extract: {table_name}")

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query)
        rows = cur.fetchall()

    if not rows:
        log.info(f"  No new rows in {table_name} since last run")
        return pd.DataFrame(), last_watermark

    df = pd.DataFrame([dict(r) for r in rows])

    # Calculate the new high watermark from actual data
    high_watermark = df[incremental_col].max()
    if hasattr(high_watermark, "isoformat"):
        high_watermark = high_watermark.isoformat()
    else:
        high_watermark = str(high_watermark)

    # Data date range for filename — project naming convention
    low_ts     = df[incremental_col].min()
    high_ts    = df[incremental_col].max()
    data_start = low_ts.strftime("%Y%m%d")  if hasattr(low_ts,  "strftime") else str(low_ts)[:10].replace("-","")
    data_end   = high_ts.strftime("%Y%m%d") if hasattr(high_ts, "strftime") else str(high_ts)[:10].replace("-","")

    log.info(f"  ✓ {len(df):,} rows | data: {data_start} → {data_end} | watermark: {high_watermark}")
    return df, high_watermark, data_start, data_end


# ─────────────────────────────────────────────────────────────
# S3 WRITE
# ─────────────────────────────────────────────────────────────

def build_s3_key(table_name: str, run_ts: datetime,
                 data_start: str, data_end: str) -> str:
    """
    Hive-style partition key.
    Folder: partitioned by ingestion date (when data arrived).
    Filename: {table}_{data_start}_to_{data_end}.parquet (project naming rule).

    This separates ingestion date (folder) from data date (filename),
    so you can tell at a glance what time range each file covers.
    """
    return (
        f"{SOURCE_PREFIX}/"
        f"year={run_ts.strftime('%Y')}/"
        f"month={run_ts.strftime('%m')}/"
        f"day={run_ts.strftime('%d')}/"
        f"table={table_name}/"
        f"{table_name}_{data_start}_to_{data_end}.parquet"
    )


def df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    """Serialize DataFrame to Parquet bytes (snappy compressed)."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, compression="snappy", engine="pyarrow")
    buffer.seek(0)
    return buffer.read()


def write_to_s3(s3_client, bucket: str, key: str, data: bytes,
                run_ts: datetime, watermark: str) -> str:
    """Write Parquet bytes to S3 with metadata tags."""
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
        ContentType="application/octet-stream",
        Metadata={
            "source":        "01_postgres",
            "ingested_at":   run_ts.isoformat(),
            "watermark":     watermark,
        }
    )
    return f"s3://{bucket}/{key}"


def verify_s3_write(s3_client, bucket: str, key: str, expected_bytes: int) -> bool:
    """Confirm file landed in S3 with correct size."""
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

def run(tables_to_run: dict, full_load: bool = False, dry_run: bool = False):
    run_ts = datetime.now(timezone.utc)
    log.info(f"Source 01 → S3 Raw | run_ts={run_ts.isoformat()}")
    log.info(f"Mode: {'FULL LOAD' if full_load else 'INCREMENTAL'} | dry_run={dry_run}")
    log.info(f"Target: s3://{RAW_BUCKET}/{SOURCE_PREFIX}/")

    pg_conn   = get_pg_conn()
    s3_client = None if dry_run else get_s3_client()

    # Load existing watermarks (unless forcing full load)
    watermarks     = {} if full_load else (load_watermark(s3_client) if not dry_run else {})
    new_watermarks = dict(watermarks)  # Will be updated per table on success

    stats = {"tables": 0, "rows": 0, "bytes": 0, "skipped": 0, "files": []}

    try:
        for table_name, config in tables_to_run.items():
            log.info(f"\n── {table_name} ──")

            last_wm = None if full_load else watermarks.get(table_name)

            result = extract_table(
                pg_conn, table_name,
                config["incremental_col"],
                last_wm
            )
            if len(result) == 2:
                df, high_wm = result
                data_start = data_end = "unknown"
            else:
                df, high_wm, data_start, data_end = result

            if df.empty:
                stats["skipped"] += 1
                continue

            parquet_bytes = df_to_parquet_bytes(df)
            size_kb       = len(parquet_bytes) / 1024
            log.info(f"  Parquet: {size_kb:.1f} KB")

            if dry_run:
                key = build_s3_key(table_name, run_ts, data_start, data_end)
                log.info(f"  [DRY RUN] Would write: {key}")
                log.info(f"  [DRY RUN] Would update watermark to: {high_wm}")
                continue

            # Write to S3
            key = build_s3_key(table_name, run_ts, data_start, data_end)
            uri = write_to_s3(s3_client, RAW_BUCKET, key, parquet_bytes, run_ts, high_wm)
            log.info(f"  Written: {uri}")

            if not verify_s3_write(s3_client, RAW_BUCKET, key, len(parquet_bytes)):
                raise RuntimeError(f"S3 write verification failed for {table_name}")

            # Only update watermark after verified write
            new_watermarks[table_name] = high_wm

            stats["tables"] += 1
            stats["rows"]   += len(df)
            stats["bytes"]  += len(parquet_bytes)
            stats["files"].append(uri)

    except Exception as e:
        log.error(f"Ingestion failed: {e}")
        log.error("Watermark NOT updated — next run will reprocess from last good state")
        pg_conn.close()
        raise

    finally:
        pg_conn.close()

    # Only save watermark if at least one table succeeded
    if not dry_run and stats["tables"] > 0:
        save_watermark(s3_client, new_watermarks)

    log.info(f"\n✓ INGESTION COMPLETE")
    log.info(f"  Tables processed: {stats['tables']}")
    log.info(f"  Tables skipped (no new data): {stats['skipped']}")
    log.info(f"  Rows:  {stats['rows']:,}")
    log.info(f"  Size:  {stats['bytes'] / 1024:.1f} KB")
    if stats["files"]:
        log.info(f"  Files written:")
        for f in stats["files"]:
            log.info(f"    {f}")

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Source 01 PostgreSQL → S3 Raw (incremental)"
    )
    parser.add_argument("--table",     help="Run single table only (e.g. orders)")
    parser.add_argument("--full-load", action="store_true",
                        help="Ignore watermark, extract everything")
    parser.add_argument("--dry-run",   action="store_true",
                        help="Print stats without writing to S3")
    args = parser.parse_args()

    if args.table and args.table not in TABLES:
        log.error(f"Unknown table: {args.table}. Valid: {list(TABLES.keys())}")
        sys.exit(1)

    tables = {args.table: TABLES[args.table]} if args.table else TABLES
    run(tables, full_load=args.full_load, dry_run=args.dry_run)


if __name__ == "__main__":
    main()

"""
pipelines/ingestion/06_stripe_to_s3_raw.py — Staff DE Journey

Ingests Stripe JSON files from local output directory → S3 Raw.

Pattern:
  /tmp/stripe_raw/YYYY/MM/DD/HH/stripe_page_NNNN.json
      → s3://bucket/source=06_stripe/year=YYYY/month=MM/day=DD/
              stripe_{data_start}_to_{data_end}.json

Why JSON not Parquet for Stripe?
  Stripe data is already structured JSON. Converting to Parquet at
  ingestion adds complexity with no benefit — the Bronze layer reads
  JSON natively via Databricks Autoloader. Parquet conversion happens
  at Bronze → Silver where we understand the full schema.

Watermark pattern for file-based sources:
  No updated_at column to query. Instead we track which files have
  already been uploaded by storing a manifest in S3.
  Manifest: s3://bucket/source=06_stripe/_metadata/uploaded_files.json
  Each run: list local files → diff against manifest → upload new only.

This is naturally idempotent — running twice never duplicates files
because S3 keys are deterministic from the local file path.

Usage:
  python 06_stripe_to_s3_raw.py
  python 06_stripe_to_s3_raw.py --dry-run
  python 06_stripe_to_s3_raw.py --input-dir /tmp/stripe_raw
"""

import os
import re
import sys
import json
import logging
import argparse
from datetime import datetime, timezone
from pathlib import Path

import boto3
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

RAW_BUCKET    = os.environ.get("S3_RAW_BUCKET", "ecommerce-lakehouse-467091806172-raw-01")
SOURCE_PREFIX = "source=06_stripe"
MANIFEST_KEY  = f"{SOURCE_PREFIX}/_metadata/uploaded_files.json"
AWS_REGION    = os.environ.get("AWS_DEFAULT_REGION", "eu-west-1")


# ─────────────────────────────────────────────────────────────
# MANIFEST — tracks uploaded files
# ─────────────────────────────────────────────────────────────

def load_manifest(s3_client) -> set:
    """Load set of already-uploaded local file paths."""
    try:
        response = s3_client.get_object(Bucket=RAW_BUCKET, Key=MANIFEST_KEY)
        data     = json.loads(response["Body"].read())
        log.info(f"Manifest loaded: {len(data['uploaded'])} files already in S3")
        return set(data["uploaded"])
    except Exception:
        log.info("No manifest found — first run, all files are new")
        return set()


def save_manifest(s3_client, uploaded: set):
    """Save updated manifest to S3."""
    s3_client.put_object(
        Bucket=RAW_BUCKET,
        Key=MANIFEST_KEY,
        Body=json.dumps({
            "uploaded":    sorted(uploaded),
            "updated_at":  datetime.now(timezone.utc).isoformat(),
            "total_files": len(uploaded),
        }, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    log.info(f"Manifest saved: {len(uploaded)} total files tracked")


# ─────────────────────────────────────────────────────────────
# S3 KEY BUILDER
# ─────────────────────────────────────────────────────────────

def build_s3_key(local_path: Path, input_dir: Path,
                 data_start: str, data_end: str) -> str:
    """
    Build S3 key from local file path.

    Local:  /tmp/stripe_raw/2026/04/23/10/stripe_page_0001.json
    S3:     source=06_stripe/year=2026/month=04/day=23/
            stripe_20260423_to_20260423.json

    We consolidate all files from the same day into one S3 object
    with a descriptive filename following project naming convention.
    Each page file gets its own S3 key to preserve granularity.
    """
    # Extract date parts from local path
    # Path structure: input_dir/YYYY/MM/DD/HH/filename.json
    try:
        rel   = local_path.relative_to(input_dir)
        parts = rel.parts  # ('2026', '04', '23', '10', 'stripe_page_0001.json')
        year, month, day = parts[0], parts[1], parts[2]
        filename = parts[-1]  # stripe_page_0001.json

        # Rename to project convention
        stem    = Path(filename).stem  # stripe_page_0001
        new_name = f"stripe_{data_start}_to_{data_end}_{stem}.json"

        hour = parts[3] if len(parts) > 4 else "00"
        # Filename already contains correct time range — use as-is
        return (
            f"{SOURCE_PREFIX}/"
            f"year={year}/month={month}/day={day}/hour={hour}/"
            f"{local_path.name}"
        )
    except Exception:
        # Fallback
        return f"{SOURCE_PREFIX}/{local_path.name}"


def get_data_date_range(local_path: Path) -> tuple[str, str]:
    """
    Extract data date range from JSON file content.
    Returns (data_start, data_end) as YYYYMMDD strings.
    """
    try:
        with open(local_path) as f:
            data = json.load(f)

        charges    = data.get("charges", [])
        timestamps = []
        for charge in charges:
            ts = charge.get("created_at") or charge.get("created")
            if ts:
                if isinstance(ts, int):
                    timestamps.append(datetime.fromtimestamp(ts, tz=timezone.utc))
                else:
                    try:
                        timestamps.append(datetime.fromisoformat(str(ts)))
                    except Exception:
                        pass

        if timestamps:
            data_start = min(timestamps).strftime("%Y%m%d")
            data_end   = max(timestamps).strftime("%Y%m%d")
            return data_start, data_end
    except Exception:
        pass

    # Fallback: use date from file path
    parts = local_path.parts
    try:
        idx   = next(i for i, p in enumerate(parts) if re.match(r'^\d{4}$', p))
        year, month, day = parts[idx], parts[idx+1], parts[idx+2]
        date_str = f"{year}{month}{day}"
        return date_str, date_str
    except Exception:
        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        return today, today


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def run(input_dir: Path, dry_run: bool = False):
    run_ts = datetime.now(timezone.utc)
    log.info(f"Source 06 → S3 Raw | run_ts={run_ts.isoformat()}")
    log.info(f"Input:  {input_dir}")
    log.info(f"Target: s3://{RAW_BUCKET}/{SOURCE_PREFIX}/")
    log.info(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")

    s3_client = None if dry_run else boto3.client("s3", region_name=AWS_REGION)

    # Load manifest of already-uploaded files
    already_uploaded = set() if dry_run else load_manifest(s3_client)

    # Find all JSON files (excluding quarantine)
    all_files = [
        p for p in input_dir.rglob("*.json")
        if "quarantine" not in str(p)
        and "_metadata" not in str(p)
    ]

    log.info(f"Found {len(all_files)} local JSON files")

    # Filter to new files only
    new_files = [f for f in all_files if str(f) not in already_uploaded]
    log.info(f"New files to upload: {len(new_files)}")

    if not new_files:
        log.info("✓ Nothing to upload — S3 is up to date")
        return

    stats          = {"uploaded": 0, "skipped": 0, "errors": 0}
    newly_uploaded = set()

    for local_path in sorted(new_files):
        try:
            data_start, data_end = get_data_date_range(local_path)
            s3_key = build_s3_key(local_path, input_dir, data_start, data_end)

            if dry_run:
                log.info(f"  [DRY RUN] {local_path.name} → {s3_key}")
                stats["uploaded"] += 1
                continue

            # Upload
            with open(local_path, "rb") as f:
                s3_client.put_object(
                    Bucket=RAW_BUCKET,
                    Key=s3_key,
                    Body=f.read(),
                    ContentType="application/json",
                    Metadata={
                        "source":      "06_stripe",
                        "ingested_at": run_ts.isoformat(),
                        "data_start":  data_start,
                        "data_end":    data_end,
                    }
                )

            newly_uploaded.add(str(local_path))
            stats["uploaded"] += 1
            log.info(f"  ✓ {local_path.name} → {s3_key}")

        except Exception as e:
            log.error(f"  ✗ Failed {local_path.name}: {e}")
            stats["errors"] += 1

    # Save updated manifest
    if not dry_run and newly_uploaded:
        save_manifest(s3_client, already_uploaded | newly_uploaded)

    log.info(f"\n✓ INGESTION COMPLETE")
    log.info(f"  Uploaded: {stats['uploaded']}")
    log.info(f"  Errors:   {stats['errors']}")


def main():
    parser = argparse.ArgumentParser(
        description="Source 06 Stripe → S3 Raw ingestion"
    )
    parser.add_argument("--input-dir", type=str,
                        default=os.environ.get("STRIPE_OUTPUT_DIR", "/tmp/stripe_raw"))
    parser.add_argument("--dry-run",   action="store_true")
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        log.error(f"Input directory does not exist: {input_dir}")
        sys.exit(1)

    run(input_dir, dry_run=args.dry_run)


if __name__ == "__main__":
    main()

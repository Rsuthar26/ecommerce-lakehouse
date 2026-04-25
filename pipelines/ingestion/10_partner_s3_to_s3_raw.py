"""
pipelines/ingestion/10_partner_s3_to_s3_raw.py — Staff DE Journey

Production-grade ingestion from Partner S3 local files → S3 Raw.

Real-world pattern:
  Partners (affiliates, resellers) drop sales files into a shared S3 bucket
  or SFTP daily. Airflow syncs new files to our S3 Raw bucket.
  Mixed formats: Parquet (PARTNER-001, PARTNER-002) and Avro (PARTNER-003).
  Bronze layer handles format detection and parsing.

Gold layer note:
  Partner data has no order_id — affiliate sales reference product_sku + date only.
  Gold layer infers order attribution by matching product_sku + sale_date window
  to orders.placed_at. This is probabilistic attribution, not exact join.

File structure (local):
  /tmp/partner_raw/YYYY/MM/DD/PARTNER-001_YYYYMMDD_sales.parquet
  /tmp/partner_raw/YYYY/MM/DD/PARTNER-003_YYYYMMDD_sales.avro

S3 partition structure:
  source=10_partner_s3/year=YYYY/month=MM/day=DD/
      PARTNER-001_YYYYMMDD_sales.parquet
      PARTNER-003_YYYYMMDD_sales.avro

Watermark: manifest-based dedup
  s3://bucket/source=10_partner_s3/_metadata/uploaded_files.json

Usage:
  python 10_partner_s3_to_s3_raw.py
  python 10_partner_s3_to_s3_raw.py --dry-run
  python 10_partner_s3_to_s3_raw.py --input-dir /tmp/partner_raw
"""

import os
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
SOURCE_PREFIX = "source=10_partner_s3"
MANIFEST_KEY  = f"{SOURCE_PREFIX}/_metadata/uploaded_files.json"
AWS_REGION    = os.environ.get("AWS_DEFAULT_REGION", "eu-west-1")

CONTENT_TYPES = {
    ".parquet": "application/octet-stream",
    ".avro":    "application/octet-stream",
}


# ─────────────────────────────────────────────────────────────
# MANIFEST
# ─────────────────────────────────────────────────────────────

def load_manifest(s3_client) -> set:
    try:
        response = s3_client.get_object(Bucket=RAW_BUCKET, Key=MANIFEST_KEY)
        data     = json.loads(response["Body"].read())
        log.info(f"Manifest loaded: {len(data['uploaded'])} files already in S3")
        return set(data["uploaded"])
    except Exception:
        log.info("No manifest found — first run, all files are new")
        return set()


def save_manifest(s3_client, uploaded: set):
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

def build_s3_key(local_path: Path, input_dir: Path) -> str:
    """
    Build S3 key from local file path.

    Local:  /tmp/partner_raw/2026/04/18/PARTNER-001_20260418_sales.parquet
    S3:     source=10_partner_s3/year=2026/month=04/day=18/
                PARTNER-001_20260418_sales.parquet

    No hour partition — Partner S3 is a daily batch source.
    File format preserved as-is (Parquet or Avro).
    """
    rel   = local_path.relative_to(input_dir)
    parts = rel.parts  # ('2026', '04', '18', 'PARTNER-001_20260418_sales.parquet')

    year  = parts[0]
    month = parts[1]
    day   = parts[2]

    return (
        f"{SOURCE_PREFIX}/"
        f"year={year}/month={month}/day={day}/"
        f"{local_path.name}"
    )


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def run(input_dir: Path, dry_run: bool = False):
    run_ts = datetime.now(timezone.utc)
    log.info(f"Source 10 → S3 Raw | run_ts={run_ts.isoformat()}")
    log.info(f"Input:  {input_dir}")
    log.info(f"Target: s3://{RAW_BUCKET}/{SOURCE_PREFIX}/")
    log.info(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")

    s3_client = None if dry_run else boto3.client("s3", region_name=AWS_REGION)

    already_uploaded = set() if dry_run else load_manifest(s3_client)

    all_files = [
        p for p in input_dir.rglob("*")
        if p.suffix in (".parquet", ".avro")
        and "quarantine" not in str(p)
        and "_metadata"  not in str(p)
    ]

    log.info(f"Found {len(all_files)} local files (Parquet + Avro)")

    new_files = [f for f in all_files if str(f) not in already_uploaded]
    log.info(f"New files to upload: {len(new_files)}")

    if not new_files:
        log.info("✓ Nothing to upload — S3 is up to date")
        return

    stats          = {"uploaded": 0, "errors": 0}
    newly_uploaded = set()

    for local_path in sorted(new_files):
        try:
            s3_key       = build_s3_key(local_path, input_dir)
            content_type = CONTENT_TYPES.get(local_path.suffix, "application/octet-stream")

            if dry_run:
                log.info(f"  [DRY RUN] {local_path.name} ({local_path.suffix}) → {s3_key}")
                stats["uploaded"] += 1
                continue

            with open(local_path, "rb") as f:
                s3_client.put_object(
                    Bucket=RAW_BUCKET,
                    Key=s3_key,
                    Body=f.read(),
                    ContentType=content_type,
                    Metadata={
                        "source":      "10_partner_s3",
                        "ingested_at": run_ts.isoformat(),
                        "format":      local_path.suffix.lstrip("."),
                    }
                )

            newly_uploaded.add(str(local_path))
            stats["uploaded"] += 1
            log.info(f"  ✓ {local_path.name} → {s3_key}")

        except Exception as e:
            log.error(f"  ✗ Failed {local_path.name}: {e}")
            stats["errors"] += 1

    if not dry_run and newly_uploaded:
        save_manifest(s3_client, already_uploaded | newly_uploaded)

    log.info(f"\n✓ INGESTION COMPLETE")
    log.info(f"  Uploaded: {stats['uploaded']}")
    log.info(f"  Errors:   {stats['errors']}")


def main():
    parser = argparse.ArgumentParser(
        description="Source 10 Partner S3 → S3 Raw ingestion"
    )
    parser.add_argument("--input-dir", type=str,
                        default=os.environ.get("PARTNER_OUTPUT_DIR", "/tmp/partner_raw"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        import sys
        log.error(f"Input directory does not exist: {input_dir}")
        sys.exit(1)

    run(input_dir, dry_run=args.dry_run)


if __name__ == "__main__":
    main()

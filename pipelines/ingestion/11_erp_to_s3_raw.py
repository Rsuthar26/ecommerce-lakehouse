"""
pipelines/ingestion/11_erp_to_s3_raw.py — Staff DE Journey

Production-grade ingestion from ERP export files → S3 Raw.

Real-world pattern:
  ERP systems (SAP, Oracle) export financial data daily.
  Two files per day: JSON and XML format (same data, different format
  for different downstream consumers).
  Airflow picks up files daily at 03:00 AM.

File structure (local):
  /tmp/erp_raw/YYYY/MM/DD/erp_export_YYYYMMDD.json
  /tmp/erp_raw/YYYY/MM/DD/erp_export_YYYYMMDD.xml

S3 partition structure:
  source=11_erp/year=YYYY/month=MM/day=DD/
      erp_export_YYYYMMDD.json
      erp_export_YYYYMMDD.xml

Silver join note:
  ERP order_id joins to orders.order_id directly.
  ERP subtotal_pence matches orders.total_pence (Rule 14 compliant).
  ERP total_pence includes VAT (subtotal * 1.20) — Silver must handle.
  ERP currency may be GBP/EUR/USD — Silver must normalise to GBP.

Watermark: manifest-based dedup
  s3://bucket/source=11_erp/_metadata/uploaded_files.json

Usage:
  python 11_erp_to_s3_raw.py
  python 11_erp_to_s3_raw.py --dry-run
  python 11_erp_to_s3_raw.py --input-dir /tmp/erp_raw
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
SOURCE_PREFIX = "source=11_erp"
MANIFEST_KEY  = f"{SOURCE_PREFIX}/_metadata/uploaded_files.json"
AWS_REGION    = os.environ.get("AWS_DEFAULT_REGION", "eu-north-1")

CONTENT_TYPES = {
    ".json": "application/json",
    ".xml":  "application/xml",
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

    Local:  /tmp/erp_raw/2026/04/18/erp_export_20260418.json
    S3:     source=11_erp/year=2026/month=04/day=18/
                erp_export_20260418.json

    No hour partition — ERP is a daily batch source.
    Both JSON and XML files preserved as-is.
    """
    rel   = local_path.relative_to(input_dir)
    parts = rel.parts  # ('2026', '04', '18', 'erp_export_20260418.json')

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
    log.info(f"Source 11 → S3 Raw | run_ts={run_ts.isoformat()}")
    log.info(f"Input:  {input_dir}")
    log.info(f"Target: s3://{RAW_BUCKET}/{SOURCE_PREFIX}/")
    log.info(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")

    s3_client = None if dry_run else boto3.client("s3", region_name=AWS_REGION)

    already_uploaded = set() if dry_run else load_manifest(s3_client)

    all_files = [
        p for p in input_dir.rglob("*")
        if p.suffix in (".json", ".xml")
        and "quarantine" not in str(p)
        and "_metadata"  not in str(p)
    ]

    log.info(f"Found {len(all_files)} local files (JSON + XML)")

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
                        "source":      "11_erp",
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
        description="Source 11 ERP → S3 Raw ingestion"
    )
    parser.add_argument("--input-dir", type=str,
                        default=os.environ.get("ERP_OUTPUT_DIR", "/tmp/erp_raw"))
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

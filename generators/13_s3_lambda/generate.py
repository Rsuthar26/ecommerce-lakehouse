"""
generators/13_s3_lambda/generate.py — Staff DE Journey: Source 13

Simulates product image uploads triggering Lambda events.
When an image is uploaded to S3, Lambda fires and writes metadata to SQS.
We simulate the Lambda output — the JSON metadata payload.

Real pattern:
  S3 PutObject event
      → Lambda (extract EXIF, validate format, resize)
      → SQS queue
      → Airflow SQS consumer
      → S3 raw/image_metadata/YYYY/MM/DD/
      → Bronze table: bronze.image_metadata

Why this matters:
  - Image metadata enriches product catalog (dimensions, format, file size)
  - Broken images (wrong format, too small, corrupted) must be caught early
  - Volume: ~500 uploads/day = ~1 every 2-3 minutes during business hours

Rules satisfied:
    Rule 1  — --mode burst and --mode stream
    Rule 2  — burst < 2 minutes
    Rule 3  — timestamps follow business hours pattern
    Rule 4  — image status reflects upload age
    Rule 5  — idempotent (s3_key is stable per image)
    Rule 6  — stream ~0.006 ops/sec (~500 events/day)
    Rule 7  — dirty data: corrupt images, wrong formats, missing metadata
    Rule 8  — README.md exists
    Rule 9  — connection via environment variables
    Rule 10 — callable from single bash line

Usage:
    python generate.py --mode burst --output-dir /tmp/image_metadata_raw
    python generate.py --mode burst --dirty --output-dir /tmp/image_metadata_raw
    python generate.py --mode stream --output-dir /tmp/image_metadata_raw
"""

import os
import json
import time
import random
import logging
import argparse
import uuid
import hashlib
from datetime import datetime, timezone, timedelta
from pathlib import Path

from faker import Faker
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

fake = Faker("en_GB")
Faker.seed(42)

STREAM_SLEEP = 1.0 / (500 / 86400)   # ~500 events/day

PRODUCT_SKUS   = [f"SKU-{str(i).zfill(5)}" for i in range(1, 201)]
IMAGE_FORMATS  = ["jpg", "jpeg", "png", "webp"]
IMAGE_POSITIONS = ["main", "alt_1", "alt_2", "alt_3", "lifestyle", "detail", "packaging"]
UPLOAD_SOURCES = ["cms", "bulk_upload", "api", "mobile_app"]
S3_BUCKET      = "ecommerce-product-images-dev"


# ─────────────────────────────────────────────────────────────
# RULE 3 — BUSINESS HOURS PATTERN
# Image uploads happen during business hours (9am-6pm weekdays)
# ─────────────────────────────────────────────────────────────

def business_hours_timestamp(base_dt: datetime) -> datetime:
    """Uploads cluster during business hours 9am-6pm."""
    hour   = random.choices(
        range(9, 18),
        weights=[5, 10, 15, 15, 10, 10, 15, 15, 5]
    )[0]
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return base_dt.replace(hour=hour, minute=minute, second=second,
                           tzinfo=timezone.utc)

def backdate(days_ago: float) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=days_ago)


# ─────────────────────────────────────────────────────────────
# RULE 4 — STATUS REFLECTS AGE
# ─────────────────────────────────────────────────────────────

def processing_status(uploaded_at: datetime) -> str:
    age_mins = (datetime.now(timezone.utc) - uploaded_at).total_seconds() / 60
    if age_mins < 2:
        return "processing"
    elif age_mins < 10:
        return random.choices(
            ["processing", "completed"], weights=[0.3, 0.7]
        )[0]
    else:
        return random.choices(
            ["completed", "failed", "quarantined"],
            weights=[0.92, 0.05, 0.03]
        )[0]


# ─────────────────────────────────────────────────────────────
# RULE 7 — DIRTY DATA
# ─────────────────────────────────────────────────────────────

def dr(base, dirty, elevated):
    return elevated if dirty else base

def bad_dimensions(w, h, dirty):
    if random.random() < dr(0.005, dirty, 0.08):
        return random.choice([0, -1, None]), random.choice([0, -1, None])
    return w, h

def bad_format(fmt, dirty):
    if random.random() < dr(0.01, dirty, 0.10):
        return random.choice(["exe", "pdf", "txt", "bmp", None, ""])
    return fmt

def bad_file_size(size, dirty):
    if random.random() < dr(0.005, dirty, 0.05):
        return random.choice([0, -1, None, 999999999999])
    return size

def maybe_null(value, dirty, base=0.05, elev=0.20):
    if random.random() < dr(base, dirty, elev):
        return None
    return value


# ─────────────────────────────────────────────────────────────
# LAMBDA EVENT BUILDER
# ─────────────────────────────────────────────────────────────

def build_image_metadata_event(uploaded_at: datetime,
                                dirty: bool = False) -> dict:
    """
    Build a Lambda output event for a product image upload.
    Mirrors what a real Lambda would extract from the S3 object.
    """
    sku      = random.choice(PRODUCT_SKUS)
    position = random.choice(IMAGE_POSITIONS)
    fmt      = bad_format(random.choice(IMAGE_FORMATS), dirty)
    ext      = fmt if fmt and fmt not in ["exe", "pdf", "txt", "bmp"] else "jpg"

    # S3 key — deterministic path structure
    s3_key = f"products/{sku}/{position}.{ext}"

    # Image dimensions — product images should be min 800x800
    width  = random.choice([800, 1000, 1200, 1500, 2000, 3000])
    height = random.choice([800, 1000, 1200, 1500, 2000, 3000])
    width, height = bad_dimensions(width, height, dirty)

    # File size in bytes
    file_size = random.randint(50_000, 5_000_000)
    file_size = bad_file_size(file_size, dirty)

    # Generate a stable ETag (MD5 of key — deterministic for same image)
    etag = hashlib.md5(s3_key.encode()).hexdigest()

    status = processing_status(uploaded_at)

    # Validation flags — set by Lambda based on image analysis
    is_valid_format     = fmt in IMAGE_FORMATS if fmt else False
    meets_min_size      = (width or 0) >= 800 and (height or 0) >= 800
    has_white_bg        = maybe_null(random.random() > 0.3, dirty)
    compression_ratio   = maybe_null(round(random.uniform(0.05, 0.95), 3), dirty)

    event = {
        # S3 event metadata
        "event_id":         str(uuid.uuid4()),
        "event_type":       "s3:ObjectCreated:Put",
        "event_time":       uploaded_at.isoformat(),
        "event_source":     "aws:s3",

        # S3 object details
        "s3_bucket":        S3_BUCKET,
        "s3_key":           s3_key,
        "s3_etag":          etag,
        "s3_size_bytes":    file_size,
        "s3_content_type":  f"image/{fmt}" if fmt else None,

        # Product reference — join key to MongoDB + Postgres
        "product_sku":      maybe_null(sku, dirty, base=0.01, elev=0.05),
        "image_position":   position,
        "upload_source":    random.choice(UPLOAD_SOURCES),
        "uploaded_by":      maybe_null(fake.user_name(), dirty),

        # Lambda-extracted image metadata
        "image_format":     fmt,
        "width_px":         width,
        "height_px":        height,
        "colour_space":     maybe_null(
            random.choice(["RGB", "CMYK", "sRGB"]), dirty),
        "dpi":              maybe_null(
            random.choice([72, 96, 150, 300]), dirty),
        "has_alpha":        random.random() > 0.7,
        "is_animated":      False,

        # Validation results
        "is_valid_format":  is_valid_format,
        "meets_min_size":   meets_min_size,
        "has_white_background": has_white_bg,
        "compression_ratio": compression_ratio,

        # Derived paths (Lambda writes resized versions)
        "thumbnail_key":    f"thumbnails/{sku}/{position}_thumb.webp"
                            if status == "completed" else None,
        "resized_keys": {
            "small":  f"resized/{sku}/{position}_400x400.webp",
            "medium": f"resized/{sku}/{position}_800x800.webp",
            "large":  f"resized/{sku}/{position}_1200x1200.webp",
        } if status == "completed" else None,

        # Processing status
        "processing_status": status,
        "processing_error":  (
            random.choice([
                "Invalid image format",
                "Image too small (minimum 800x800)",
                "Corrupted file",
                "Timeout during processing",
            ])
            if status in ("failed", "quarantined") else None
        ),

        # Dirty mode: 2% future timestamps
        "event_time": (
            "2099-01-01T00:00:00+00:00"
            if dirty and random.random() < 0.02
            else uploaded_at.isoformat()
        ),
    }

    return event


# ─────────────────────────────────────────────────────────────
# OUTPUT
# ─────────────────────────────────────────────────────────────

def write_batch(events: list, output_dir: Path,
                batch_num: int, ts: datetime) -> None:
    date_path = output_dir / ts.strftime("%Y/%m/%d/%H")
    date_path.mkdir(parents=True, exist_ok=True)
    file_path = date_path / f"image_events_{batch_num:04d}.json"
    with open(file_path, "w") as f:
        json.dump({
            "batch":      batch_num,
            "count":      len(events),
            "source":     "lambda:s3-image-processor",
            "fetched_at": ts.isoformat(),
            "events":     events,
        }, f, default=str)


# ─────────────────────────────────────────────────────────────
# BURST MODE
# ─────────────────────────────────────────────────────────────

def run_burst(output_dir: Path, days: int = 7, dirty: bool = False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}")
    t0    = time.time()
    stats = {"events": 0, "batches": 0}

    total_events = days * 500
    batch_size   = 50
    batch_num    = 0
    buffer       = []

    for _ in range(total_events):
        days_ago    = random.uniform(0, days)
        base_dt     = backdate(days_ago)
        uploaded_at = business_hours_timestamp(base_dt)

        event = build_image_metadata_event(uploaded_at, dirty)
        buffer.append(event)
        stats["events"] += 1

        if len(buffer) >= batch_size:
            write_batch(buffer, output_dir, batch_num, uploaded_at)
            stats["batches"] += 1
            batch_num += 1
            buffer = []

    if buffer:
        write_batch(buffer, output_dir, batch_num,
                    datetime.now(timezone.utc))
        stats["batches"] += 1

    elapsed = time.time() - t0
    log.info(f"✓ BURST COMPLETE | {elapsed:.1f}s | {stats}")
    if elapsed > 120:
        log.warning(f"Rule 2 VIOLATION: {elapsed:.0f}s > 120s")
    else:
        log.info(f"Rule 2 ✅ {elapsed:.1f}s < 120s")


# ─────────────────────────────────────────────────────────────
# STREAM MODE
# ─────────────────────────────────────────────────────────────

def run_stream(output_dir: Path, dirty: bool = False):
    log.info(f"STREAM MODE | ~500 events/day | dirty={dirty} | Ctrl+C to stop")

    stats, i = {"events": 0}, 0

    try:
        while True:
            i   += 1
            now  = datetime.now(timezone.utc)
            event = build_image_metadata_event(now, dirty)

            write_batch([event], output_dir, i, now)
            stats["events"] += 1

            if i % 100 == 0:
                log.info(f"Stream — {stats}")

            time.sleep(STREAM_SLEEP)

    except KeyboardInterrupt:
        log.info(f"Stopped. {stats}")


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Source 13 — S3 Lambda image metadata generator")
    p.add_argument("--mode",       choices=["burst", "stream"], required=True)
    p.add_argument("--days",       type=int, default=7)
    p.add_argument("--dirty",      action="store_true")
    p.add_argument("--output-dir", type=str,
                   default=os.environ.get("IMAGE_OUTPUT_DIR", "/tmp/image_metadata_raw"))
    args = p.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"Source 13 | mode={args.mode} | days={args.days} | "
             f"dirty={args.dirty} | output={output_dir}")

    if args.mode == "burst":
        run_burst(output_dir, args.days, args.dirty)
    elif args.mode == "stream":
        run_stream(output_dir, args.dirty)


if __name__ == "__main__":
    main()

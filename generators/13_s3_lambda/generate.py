"""
generators/13_s3_lambda/generate.py — Staff DE Journey: Source 13

Simulates product image uploads triggering Lambda events.
~500 uploads/day = ~1 every 2-3 minutes during business hours.

Fix applied: Rule 11 — real product SKUs loaded from Postgres at startup.

Rules satisfied: All 11.

Usage:
    python generate.py --mode burst --output-dir /tmp/image_metadata_raw
    python generate.py --mode burst --dirty --output-dir /tmp/image_metadata_raw
    python generate.py --mode stream --output-dir /tmp/image_metadata_raw
"""

import os, sys, json, time, random, logging, argparse, uuid, hashlib
from datetime import datetime, timezone, timedelta
from pathlib import Path
from faker import Faker
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from generators.shared.postgres_ids import load_entity_ids

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)
fake = Faker("en_GB"); Faker.seed(42)

STREAM_SLEEP   = 86400 / 500   # Rule 6 + Rule 13
IMAGE_FORMATS  = ["jpg","jpeg","png","webp"]
IMAGE_POSITIONS = ["main","alt_1","alt_2","alt_3","lifestyle","detail","packaging"]
UPLOAD_SOURCES = ["cms","bulk_upload","api","mobile_app"]
S3_BUCKET      = os.environ.get("IMAGE_S3_BUCKET","ecommerce-product-images-dev")

_entity_ids = None
def get_entity_ids():
    global _entity_ids
    if _entity_ids is None: _entity_ids = load_entity_ids()
    return _entity_ids

def dr(base, dirty, elevated): return elevated if dirty else base
def maybe_null(v, dirty, base=0.05, elev=0.20):
    return None if random.random() < dr(base, dirty, elev) else v

def business_hours_timestamp(base_dt: datetime) -> datetime:
    hour = random.choices(range(9,18), weights=[5,10,15,15,10,10,15,15,5])[0]
    return base_dt.replace(hour=hour, minute=random.randint(0,59),
                           second=random.randint(0,59), tzinfo=timezone.utc)

def processing_status(uploaded_at: datetime) -> str:
    age_mins = (datetime.now(timezone.utc) - uploaded_at).total_seconds() / 60
    if age_mins < 2:   return "processing"
    elif age_mins < 10: return random.choices(["processing","completed"], weights=[0.3,0.7])[0]
    else:               return random.choices(["completed","failed","quarantined"], weights=[0.92,0.05,0.03])[0]

def bad_dimensions(w, h, dirty):
    if random.random() < dr(0.005, dirty, 0.08):
        return random.choice([0,-1,None]), random.choice([0,-1,None])
    return w, h

def bad_format(fmt, dirty):
    if random.random() < dr(0.01, dirty, 0.10):
        return random.choice(["exe","pdf","txt","bmp",None,""])
    return fmt

def bad_file_size(size, dirty):
    if random.random() < dr(0.005, dirty, 0.05):
        return random.choice([0,-1,None,999999999999])
    return size

def build_image_metadata_event(uploaded_at: datetime, dirty: bool = False) -> dict:
    ids      = get_entity_ids()
    sku      = random.choice(ids["product_skus"]) if ids["product_skus"] else "SKU-00001"  # Rule 11
    position = random.choice(IMAGE_POSITIONS)
    fmt      = bad_format(random.choice(IMAGE_FORMATS), dirty)
    ext      = fmt if fmt and fmt not in ["exe","pdf","txt","bmp"] else "jpg"
    s3_key   = f"products/{sku}/{position}.{ext}"
    width    = random.choice([800,1000,1200,1500,2000,3000])
    height   = random.choice([800,1000,1200,1500,2000,3000])
    width, height = bad_dimensions(width, height, dirty)
    file_size     = bad_file_size(random.randint(50_000,5_000_000), dirty)
    etag          = hashlib.md5(s3_key.encode()).hexdigest()
    status        = processing_status(uploaded_at)
    is_valid_fmt  = fmt in IMAGE_FORMATS if fmt else False
    meets_min     = (width or 0) >= 800 and (height or 0) >= 800

    ts = uploaded_at.isoformat()
    if dirty and random.random() < 0.02:
        ts = "2099-01-01T00:00:00+00:00"

    return {
        "event_id":           str(uuid.uuid4()),
        "event_type":         "s3:ObjectCreated:Put",
        "event_time":         ts,
        "event_source":       "aws:s3",
        "s3_bucket":          S3_BUCKET,
        "s3_key":             s3_key,
        "s3_etag":            etag,
        "s3_size_bytes":      file_size,
        "s3_content_type":    f"image/{fmt}" if fmt else None,
        "product_sku":        maybe_null(sku, dirty, base=0.01, elev=0.05),
        "image_position":     position,
        "upload_source":      random.choice(UPLOAD_SOURCES),
        "uploaded_by":        maybe_null(fake.user_name(), dirty),
        "image_format":       fmt,
        "width_px":           width,
        "height_px":          height,
        "colour_space":       maybe_null(random.choice(["RGB","CMYK","sRGB"]), dirty),
        "dpi":                maybe_null(random.choice([72,96,150,300]), dirty),
        "has_alpha":          random.random() > 0.7,
        "is_valid_format":    is_valid_fmt,
        "meets_min_size":     meets_min,
        "has_white_background": maybe_null(random.random() > 0.3, dirty),
        "compression_ratio":  maybe_null(round(random.uniform(0.05,0.95),3), dirty),
        "thumbnail_key":      f"thumbnails/{sku}/{position}_thumb.webp" if status == "completed" else None,
        "processing_status":  status,
        "processing_error":   random.choice(["Invalid format","Image too small","Corrupted file"])
                              if status in ("failed","quarantined") else None,
    }

def write_batch(events, output_dir, batch_num, ts):
    dp = output_dir / ts.strftime("%Y/%m/%d/%H"); dp.mkdir(parents=True, exist_ok=True)
    with open(dp / f"image_events_{batch_num:04d}.json","w") as f:
        json.dump({"batch": batch_num, "count": len(events),
                   "source": "lambda:s3-image-processor",
                   "fetched_at": ts.isoformat(), "events": events}, f, default=str)

def run_burst(output_dir, days=7, dirty=False):
    log.info(f"BURST MODE | days={days} | dirty={dirty}"); get_entity_ids()
    t0, stats = time.time(), {"events":0,"batches":0}
    total, batch_size, buf, bn = days * 500, 50, [], 0
    now = datetime.now(timezone.utc)
    for _ in range(total):
        days_ago    = random.uniform(0, days)
        base_dt     = now - timedelta(days=days_ago)
        uploaded_at = business_hours_timestamp(base_dt)
        buf.append(build_image_metadata_event(uploaded_at, dirty)); stats["events"] += 1
        if len(buf) >= batch_size:
            write_batch(buf, output_dir, bn, uploaded_at); stats["batches"] += 1; bn += 1; buf = []
    if buf: write_batch(buf, output_dir, bn, datetime.now(timezone.utc)); stats["batches"] += 1
    elapsed = time.time() - t0
    log.info(f"✓ BURST COMPLETE | {elapsed:.1f}s | {stats}")
    log.info(f"Rule 2 {'✅' if elapsed <= 120 else 'VIOLATION'} {elapsed:.1f}s")

def run_stream(output_dir, dirty=False):
    log.info(f"STREAM MODE | ~500 events/day | dirty={dirty} | Ctrl+C to stop")
    get_entity_ids(); stats, i = {"events":0}, 0
    try:
        while True:
            i += 1; now = datetime.now(timezone.utc)
            write_batch([build_image_metadata_event(now, dirty)], output_dir, i, now)
            stats["events"] += 1
            if i % 100 == 0: log.info(f"Stream — {stats}")
            time.sleep(STREAM_SLEEP)
    except KeyboardInterrupt: log.info(f"Stopped. {stats}")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["burst","stream"], required=True)
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--dirty", action="store_true")
    p.add_argument("--output-dir", type=str, default=os.environ.get("IMAGE_OUTPUT_DIR","/tmp/image_metadata_raw"))
    args = p.parse_args()
    od = Path(args.output_dir); od.mkdir(parents=True, exist_ok=True)
    log.info(f"Source 13 | mode={args.mode} | days={args.days} | dirty={args.dirty} | output={od}")
    if args.mode == "burst": run_burst(od, args.days, args.dirty)
    else: run_stream(od, args.dirty)

if __name__ == "__main__": main()

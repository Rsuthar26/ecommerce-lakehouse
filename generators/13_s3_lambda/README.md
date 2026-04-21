# Source 13 — S3 + Lambda Image Metadata

## What It Simulates
Product image upload events processed by Lambda. Real pattern: S3 PutObject → Lambda extracts metadata → writes JSON event. ~500 uploads/day during business hours (9am–6pm).

## Event Schema
```json
{
  "event_id": "uuid4",
  "event_type": "s3:ObjectCreated:Put",
  "event_time": "ISO timestamp during 9am-6pm",
  "s3_bucket": "ecommerce-product-images-dev",
  "s3_key": "products/SKU-00001/main.jpg",
  "s3_etag": "deterministic-md5",
  "s3_size_bytes": 245000,
  "product_sku": "real SKU from Postgres",
  "image_position": "main | alt_1 | alt_2 | lifestyle | detail | packaging",
  "image_format": "jpg | png | webp",
  "width_px": 1200,
  "height_px": 1200,
  "is_valid_format": true,
  "meets_min_size": true,
  "processing_status": "processing | completed | failed | quarantined",
  "processing_error": "null or reason string",
  "thumbnail_key": "thumbnails/SKU-00001/main_thumb.webp"
}
```

## Run Commands
```bash
python generators/13_s3_lambda/generate.py --mode burst --output-dir /tmp/image_metadata_raw
python generators/13_s3_lambda/generate.py --mode burst --dirty --output-dir /tmp/image_metadata_raw
python generators/13_s3_lambda/generate.py --mode stream --output-dir /tmp/image_metadata_raw
```

## Prerequisites
```bash
pip install faker python-dotenv
```

## Environment Variables
```
IMAGE_OUTPUT_DIR=/tmp/image_metadata_raw
IMAGE_S3_BUCKET=ecommerce-product-images-dev
PG_HOST, PG_PORT, PG_DBNAME, PG_USER, PG_PASSWORD
```

## How to Verify
```bash
find /tmp/image_metadata_raw -name "*.json" | wc -l
python3 -c "
import json, glob
from collections import Counter
files = glob.glob('/tmp/image_metadata_raw/**/*.json', recursive=True)
statuses = Counter()
for f in files:
    with open(f) as fp:
        d = json.load(fp)
        for e in d.get('events', []):
            statuses[e['processing_status']] += 1
print(dict(statuses))
# Expect ~92% completed, ~5% failed, ~3% quarantined
"
```

# Source 08 — Shopify GraphQL API

## What It Simulates
Shopify product catalog and discount codes in GraphQL response format. ~2K queries/day. Product status reflects age — new products start DRAFT, become ACTIVE, old products may be ARCHIVED.

## Event Schema
```json
{
  "id": "gid://shopify/Product/abc123",
  "handle": "sku-00001",
  "status": "ACTIVE | DRAFT | ARCHIVED",
  "vendor": "TechPro Ltd",
  "productType": "Electronics",
  "tags": ["sale", "new"],
  "variants": {
    "edges": [{"node": {
      "sku": "SKU-00001",
      "price": "49.99",
      "inventoryQuantity": 150,
      "barcode": "1234567890123"
    }}]
  },
  "collections": {"edges": [{"node": {"title": "Best Sellers"}}]},
  "createdAt": "ISO timestamp",
  "updatedAt": "ISO timestamp"
}
```

## Run Commands
```bash
python generators/08_shopify/generate.py --mode burst --output-dir /tmp/shopify_raw
python generators/08_shopify/generate.py --mode burst --dirty --output-dir /tmp/shopify_raw
python generators/08_shopify/generate.py --mode stream --output-dir /tmp/shopify_raw
```

## Prerequisites
```bash
pip install faker python-dotenv
```

## Environment Variables
```
SHOPIFY_OUTPUT_DIR=/tmp/shopify_raw
PG_HOST, PG_PORT, PG_DBNAME, PG_USER, PG_PASSWORD
```

## How to Verify
```bash
find /tmp/shopify_raw -name "*.json" | wc -l
cat /tmp/shopify_raw/$(date +%Y/%m/%d)/*/shopify_products_0000.json | \
  python3 -c "import json,sys; d=json.load(sys.stdin); [print(n['node']['handle'], n['node']['status']) for n in d['edges'][:5]]"
```

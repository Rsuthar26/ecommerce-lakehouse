# Source 03 — MongoDB Atlas Product Catalog

## What It Simulates
The product catalog stored in MongoDB Atlas. 200 products with nested variants, category-specific attributes, and price history. `product_sku` matches `order_items.product_sku` in Postgres — this is the cross-system foreign key.

## Schema

```
products collection:
  _id              — same as product_sku (SKU-00001 to SKU-00200)
  product_sku      — join key to Postgres order_items
  name, category, subcategory, brand, description
  base_price_pence — integer
  variants         — array of {colour, sku_variant, price_pence, stock, weight_grams}
  attributes       — nested object, keys vary by category
  images           — array of CDN URLs
  status           — new | active | low_stock | out_of_stock | discontinued
  rating, review_count, created_at, updated_at

product_price_history collection:
  _id          — deterministic md5 hash (Rule 12 — idempotent)
  product_sku, price_pence, changed_at, reason
```

## Run Commands

```bash
python generators/03_mongodb/generate.py --mode burst --days 7
python generators/03_mongodb/generate.py --mode burst --days 7 --dirty
python generators/03_mongodb/generate.py --mode stream
```

## Prerequisites
```bash
pip install pymongo faker python-dotenv
```

## Environment Variables
```
MONGO_URI=mongodb+srv://mongo_admin:password@cluster.mongodb.net/
MONGO_DBNAME=ecommerce
```

## How to Verify
```javascript
db.products.countDocuments()
db.products.findOne({_id: "SKU-00001"})
db.products.aggregate([{$group: {_id: "$status", count: {$sum: 1}}}])
db.product_price_history.countDocuments()
```

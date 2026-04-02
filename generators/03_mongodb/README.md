# Source 03 — MongoDB Atlas Generator

## What It Simulates

The product catalog stored in MongoDB Atlas.
200 products with nested variants, category-specific attributes, and price history.

The `product_sku` field matches what `order_items` in Postgres references.
This is the cross-system foreign key — Postgres holds the reference, MongoDB holds the data.

## Schema

```
products collection:
  _id            — same as product_sku (SKU-00001 to SKU-00200)
  product_sku    — redundant but useful for queries
  name           — product name
  category       — electronics | furniture | accessories
  subcategory    — audio | computing | desks | etc.
  brand          — SoundMax | TechPro | etc.
  description    — free text
  base_price_pence — integer (pence)
  variants       — array of {colour, sku_variant, price_pence, stock, weight_grams}
  attributes     — nested object, keys vary by category
  images         — array of CDN URLs
  tags           — array of strings
  status         — new | active | low_stock | out_of_stock | discontinued
  rating         — float 3.0–5.0
  review_count   — integer
  created_at     — timestamp
  updated_at     — timestamp

product_price_history collection:
  product_sku    — reference to products
  price_pence    — historical price
  changed_at     — when price changed
  reason         — promotion | restock | market_adjustment
```

## Prerequisites

```bash
pip install pymongo faker python-dotenv
```

## Environment Variables

```
MONGO_URI=mongodb+srv://mongo_admin:password@ecommerce-cluster.k2gc71w.mongodb.net/
```

## Run Commands

### Burst — clean
```bash
python generators/03_mongodb/generate.py --mode burst --days 7
```

### Burst — dirty
```bash
python generators/03_mongodb/generate.py --mode burst --days 7 --dirty
```

### Stream
```bash
python generators/03_mongodb/generate.py --mode stream
```

## How to Verify

In MongoDB Atlas → Data Explorer → ecommerce → products:
```javascript
// Count products
db.products.countDocuments()

// Check a product with variants
db.products.findOne({_id: "SKU-00001"})

// Products by category
db.products.aggregate([
  {$group: {_id: "$category", count: {$sum: 1}}}
])

// Verify status reflects age (Rule 4)
db.products.aggregate([
  {$group: {_id: "$status", count: {$sum: 1}}}
])
```

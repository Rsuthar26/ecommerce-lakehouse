import json

# Fix Source 01 — path wildcard
nb = json.load(open('pipelines/bronze/bronze_01_postgres.ipynb'))
for cell in nb['cells']:
    if cell['cell_type'] == 'code':
        src = ''.join(cell['source'])
        if 'month=04' in src and 'table=' in src:
            src = src.replace(
                'path = f"{RAW_BUCKET}/source={SOURCE}/year=2026/month=04/day=*/table={table}/"',
                'path = f"{RAW_BUCKET}/source={SOURCE}/**/table={table}/"'
            ).replace(
                '.load(f"{RAW_BUCKET}/source={SOURCE}/year=2026/month=04/day=*/table=orders/")',
                '.load(f"{RAW_BUCKET}/source={SOURCE}/**/table=orders/")'
            )
            cell['source'] = [src]
json.dump(nb, open('pipelines/bronze/bronze_01_postgres.ipynb', 'w'), indent=1)
print("Fixed 01")

# Fix Source 03 — path wildcard
nb = json.load(open('pipelines/bronze/bronze_03_mongodb.ipynb'))
for cell in nb['cells']:
    if cell['cell_type'] == 'code':
        src = ''.join(cell['source'])
        if 'month=04/day=22' in src:
            src = src.replace(
                'path = f"{RAW_BUCKET}/source={SOURCE}/year=2026/month=04/day=22/"',
                'path = f"{RAW_BUCKET}/source={SOURCE}/**/"'
            )
            cell['source'] = [src]
json.dump(nb, open('pipelines/bronze/bronze_03_mongodb.ipynb', 'w'), indent=1)
print("Fixed 03")

# Fix Source 06 — path wildcard
nb = json.load(open('pipelines/bronze/bronze_06_stripe.ipynb'))
for cell in nb['cells']:
    if cell['cell_type'] == 'code':
        src = ''.join(cell['source'])
        if 'month=04' in src and 'stripe_charges' in src:
            src = src.replace(
                'path = f"{RAW_BUCKET}/source={SOURCE}/year=2026/month=04/day=*/hour=*/stripe_charges_*.json"',
                'path = f"{RAW_BUCKET}/source={SOURCE}/**/stripe_charges_*.json"'
            )
            cell['source'] = [src]
json.dump(nb, open('pipelines/bronze/bronze_06_stripe.ipynb', 'w'), indent=1)
print("Fixed 06")

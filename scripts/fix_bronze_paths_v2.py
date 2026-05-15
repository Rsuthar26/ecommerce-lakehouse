import json

# Fix Source 01 — correct glob pattern for Spark S3
nb = json.load(open('pipelines/bronze/bronze_01_postgres.ipynb'))
for cell in nb['cells']:
    if cell['cell_type'] == 'code':
        src = ''.join(cell['source'])
        if '**/table=' in src:
            src = src.replace(
                'path = f"{RAW_BUCKET}/source={SOURCE}/**/table={table}/"',
                'path = f"{RAW_BUCKET}/source={SOURCE}/year=*/month=*/day=*/table={table}/"'
            ).replace(
                '.load(f"{RAW_BUCKET}/source={SOURCE}/**/table=orders/")',
                '.load(f"{RAW_BUCKET}/source={SOURCE}/year=*/month=*/day=*/table=orders/")'
            )
            cell['source'] = [src]
json.dump(nb, open('pipelines/bronze/bronze_01_postgres.ipynb', 'w'), indent=1)
print("Fixed 01")

# Fix Source 03 — correct glob pattern
nb = json.load(open('pipelines/bronze/bronze_03_mongodb.ipynb'))
for cell in nb['cells']:
    if cell['cell_type'] == 'code':
        src = ''.join(cell['source'])
        if '/**/' in src:
            src = src.replace(
                'path = f"{RAW_BUCKET}/source={SOURCE}/**/"',
                'path = f"{RAW_BUCKET}/source={SOURCE}/year=*/month=*/day=*/"'
            ).replace(
                '.load(f"{RAW_BUCKET}/source={SOURCE}/**/")',
                '.load(f"{RAW_BUCKET}/source={SOURCE}/year=*/month=*/day=*/")'
            )
            cell['source'] = [src]
json.dump(nb, open('pipelines/bronze/bronze_03_mongodb.ipynb', 'w'), indent=1)
print("Fixed 03")

# Fix Source 06 — correct glob pattern
nb = json.load(open('pipelines/bronze/bronze_06_stripe.ipynb'))
for cell in nb['cells']:
    if cell['cell_type'] == 'code':
        src = ''.join(cell['source'])
        if '**/' in src and 'stripe_charges' in src:
            src = src.replace(
                'path = f"{RAW_BUCKET}/source={SOURCE}/**/stripe_charges_*.json"',
                'path = f"{RAW_BUCKET}/source={SOURCE}/year=*/month=*/day=*/hour=*/stripe_charges_*.json"'
            )
            cell['source'] = [src]
json.dump(nb, open('pipelines/bronze/bronze_06_stripe.ipynb', 'w'), indent=1)
print("Fixed 06")

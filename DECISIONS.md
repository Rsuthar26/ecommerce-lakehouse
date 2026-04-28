# Architecture Decisions & Challenges

Real decisions made during the build — what happened, why, exactly what
was done, and what was learned. Written so anyone reading this repo
understands the real engineering behind it.

---

## Decision 001 — Region Migration: eu-north-1 → eu-west-1

**Date:** April 2026

---

### What happened

Project was initially provisioned entirely in eu-north-1 (Stockholm):
- RDS PostgreSQL — via Terraform
- S3 buckets (raw, bronze, silver, gold) — via Terraform
- VPC, subnets, security groups, internet gateway

When Databricks workspace setup began, Databricks does not support
eu-north-1. A workspace could not be created there at all. Full
infrastructure migration to eu-west-1 (Ireland) was required.

---

### Why eu-west-1

| Tool | eu-north-1 | eu-west-1 |
|---|---|---|
| Databricks | ❌ Not supported | ✅ Supported |
| AWS MSK (Kafka) | ✅ | ✅ |
| AWS MWAA (Airflow) | ❌ Not supported | ✅ Supported |
| RDS PostgreSQL | ✅ | ✅ |
| AWS Kinesis | ✅ | ✅ |

eu-west-1 was the closest region supporting the full stack.

---

### Exact steps taken

**Step 1 — Snapshot RDS in eu-north-1**
```bash
aws rds create-db-snapshot \
  --db-instance-identifier ecommerce-lakehouse-postgres \
  --db-snapshot-identifier ecommerce-lakehouse-postgres-migration \
  --region eu-north-1
```
Waited for status `available` (~3 minutes).

**Step 2 — Copy snapshot cross-region to eu-west-1**

Encrypted snapshots require the destination KMS key specified explicitly.
First retrieved the default RDS KMS key ARN in eu-west-1:
```bash
aws kms describe-key \
  --key-id alias/aws/rds \
  --region eu-west-1 \
  --query 'KeyMetadata.Arn' \
  --output text
```

Then copied:
```bash
aws rds copy-db-snapshot \
  --source-db-snapshot-identifier \
    arn:aws:rds:eu-north-1:[YOUR-ACCOUNT-ID]:snapshot:ecommerce-lakehouse-postgres-migration \
  --target-db-snapshot-identifier ecommerce-lakehouse-postgres-migration \
  --kms-key-id arn:aws:kms:eu-west-1:[YOUR-ACCOUNT-ID]:key/[KEY-ID] \
  --source-region eu-north-1 \
  --region eu-west-1
```

**Step 3 — Restore RDS from snapshot in eu-west-1**

Updated Terraform region to eu-west-1, restored RDS from copied snapshot.
All data preserved — 749 customers, 5,871 orders, payments, inventory.

**Step 4 — Recreate S3 buckets in eu-west-1**

Updated `terraform/variables.tf` region to eu-west-1. Ran targeted
Terraform apply to create new buckets in the correct region.
Copied existing Raw data using `aws s3 sync` with `--source-region` flag.

**Step 5 — Update Terraform and env vars**

Updated `aws_region` in `variables.tf`. Ran `terraform apply` to
recreate VPC, subnets, security groups in eu-west-1. Updated `.env`
with new RDS endpoint and region (`.env` is gitignored — see `.env.example`).

**Step 6 — Clean up eu-north-1 resources**

After verifying everything worked in eu-west-1:
```bash
# Delete old S3 buckets
aws s3 rb s3://ecommerce-lakehouse-[ACCOUNT-ID]-raw-01 \
  --force --region eu-north-1

# Delete old RDS snapshot
aws rds delete-db-snapshot \
  --db-snapshot-identifier ecommerce-lakehouse-postgres-migration \
  --region eu-north-1

# Delete old RDS instance
aws rds delete-db-instance \
  --db-instance-identifier ecommerce-lakehouse-postgres \
  --skip-final-snapshot \
  --region eu-north-1
```

---

### What broke along the way

**KMS error on cross-region snapshot copy:**
Encrypted snapshots cannot be copied cross-region without explicitly
specifying the KMS key in the destination region. AWS does not
default to the managed key automatically.
Fix: retrieve KMS key ARN in destination region first, include in copy command.

**Terraform state mismatch:**
After manually deleting old S3 buckets, Terraform state still referenced
them. Had to run `terraform state rm` for each resource before recreating.
Lesson: when manually deleting Terraform-managed resources, always clean
the state file first.

---

### The lesson

Tool regional availability must be checked before provisioning anything.

**Correct order for any new project:**

Decide all tools in the stack
Check each tool's supported regions
Find the region all tools support
Then provision infrastructure


**References:**
- Databricks regions: https://docs.databricks.com/en/resources/supported-regions.html
- MWAA regions: https://docs.aws.amazon.com/mwaa/latest/userguide/what-is-mwaa.html

---

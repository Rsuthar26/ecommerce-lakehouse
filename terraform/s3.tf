# ============================================================
# s3.tf — Staff DE Journey: Medallion Data Lake
#
# What this provisions:
#   1. S3 Raw Landing Zone     — files land here as-is, never modified
#   2. S3 Bronze bucket        — Delta tables, raw ingested data
#   3. S3 Silver bucket        — Delta tables, cleaned data
#   4. S3 Gold bucket          — Delta tables, business-ready data
#
# Why 4 separate buckets instead of one?
#   - Separate IAM policies per layer (Silver can't write to Gold)
#   - Independent lifecycle policies per layer
#   - Clear cost attribution per layer in AWS Cost Explorer
#   - Easier to grant Databricks access per layer
#
# Naming: includes account ID because S3 names are globally unique
# across all AWS accounts. Two people can't have the same bucket name.
#
# Cost: S3 Standard ~$0.023/GB/month
# At learning scale (<10GB total) = pennies per month
# ============================================================


locals {
  account_id  = data.aws_caller_identity.current.account_id
  bucket_prefix = "${var.project_name}-${local.account_id}"
}

# ============================================================
# RAW LANDING ZONE
# Files land here exactly as received from source systems.
# Nothing is ever modified or deleted from this bucket.
# This is your safety net — if a pipeline corrupts data,
# the original file is always here to reprocess from.
# ============================================================

resource "aws_s3_bucket" "raw" {
  bucket = "${local.bucket_prefix}-raw-01"
  tags   = { Name = "${var.project_name}-raw", layer = "raw" }
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Lifecycle: move to Glacier after 90 days, delete after 365
# Raw files are rarely accessed after Bronze ingestion
resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    id     = "archive-old-raw-files"
    status = "Enabled"

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    expiration {
      days = 365
    }
  }
}

# ============================================================
# BRONZE BUCKET
# Delta Lake tables — raw ingested data, schema applied.
# One partition per source per date.
# Nothing is cleaned here — that is Silver's job.
# ============================================================

resource "aws_s3_bucket" "bronze" {
  bucket = "${local.bucket_prefix}-bronze-01"
  tags   = { Name = "${var.project_name}-bronze", layer = "bronze" }
}

resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket                  = aws_s3_bucket.bronze.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ============================================================
# SILVER BUCKET
# Delta Lake tables — cleaned, deduplicated, validated data.
# Bad records go to quarantine tables here too.
# ============================================================

resource "aws_s3_bucket" "silver" {
  bucket = "${local.bucket_prefix}-silver-01"
  tags   = { Name = "${var.project_name}-silver", layer = "silver" }
}

resource "aws_s3_bucket_versioning" "silver" {
  bucket = aws_s3_bucket.silver.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "silver" {
  bucket = aws_s3_bucket.silver.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "silver" {
  bucket                  = aws_s3_bucket.silver.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ============================================================
# GOLD BUCKET
# Delta Lake tables — aggregated, joined, business-ready.
# These are the tables that feed Databricks SQL Warehouse,
# dbt, Tableau, and Power BI.
# ============================================================

resource "aws_s3_bucket" "gold" {
  bucket = "${local.bucket_prefix}-gold-01"
  tags   = { Name = "${var.project_name}-gold", layer = "gold" }
}

resource "aws_s3_bucket_versioning" "gold" {
  bucket = aws_s3_bucket.gold.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "gold" {
  bucket = aws_s3_bucket.gold.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "gold" {
  bucket                  = aws_s3_bucket.gold.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ============================================================
# IAM POLICY — Databricks access to all 4 buckets
#
# Databricks needs read/write on bronze, silver, gold.
# Read-only on raw (it should never write to raw).
# This policy is attached to the Databricks IAM role.
# ============================================================

resource "aws_iam_policy" "databricks_s3" {
  name        = "${var.project_name}-databricks-s3-policy"
  description = "Allows Databricks to read/write Bronze, Silver, Gold and read Raw"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadRaw"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.raw.arn,
          "${aws_s3_bucket.raw.arn}/*"
        ]
      },
      {
        Sid    = "ReadWriteBronzeSilverGold"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.bronze.arn,
          "${aws_s3_bucket.bronze.arn}/*",
          aws_s3_bucket.silver.arn,
          "${aws_s3_bucket.silver.arn}/*",
          aws_s3_bucket.gold.arn,
          "${aws_s3_bucket.gold.arn}/*"
        ]
      }
    ]
  })
}

# ============================================================
# OUTPUTS — printed after terraform apply
# Copy these into your .env file
# ============================================================

output "s3_raw_bucket" {
  value       = aws_s3_bucket.raw.bucket
  description = "S3 Raw Landing Zone bucket name"
}

output "s3_bronze_bucket" {
  value       = aws_s3_bucket.bronze.bucket
  description = "S3 Bronze bucket name"
}

output "s3_silver_bucket" {
  value       = aws_s3_bucket.silver.bucket
  description = "S3 Silver bucket name"
}

output "s3_gold_bucket" {
  value       = aws_s3_bucket.gold.bucket
  description = "S3 Gold bucket name"
}

output "databricks_s3_policy_arn" {
  value       = aws_iam_policy.databricks_s3.arn
  description = "IAM policy ARN to attach to Databricks instance profile"
}

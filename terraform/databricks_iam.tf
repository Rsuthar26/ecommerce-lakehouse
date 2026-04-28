# ============================================================
# databricks_iam.tf — IAM resources for Databricks
#
# What this provisions:
#   1. Cross-account role — allows Databricks to launch EC2
#   2. Instance profile role — gives clusters S3 access
#   3. Instance profile — wraps the role for EC2
#   4. S3 root bucket — Databricks workspace storage
#   5. PassRole policy — allows Databricks compute role to use instance profile
#
# Note: Databricks workspace itself is created via the Databricks
# account console (accounts.cloud.databricks.com) — not Terraform.
# The Unity Catalog external location is created via CloudFormation
# (databricks-s3-external-location stack).
# Both are one-time setup steps documented in DECISIONS.md.
# ============================================================

# ── Cross-account role (allows Databricks to manage EC2) ──
resource "aws_iam_role" "databricks_cross_account" {
  name = "databricks-cross-account-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::414351767826:root"
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = "0000"
          }
        }
      }
    ]
  })

  tags = { Name = "${var.project_name}-databricks-cross-account" }
}

resource "aws_iam_role_policy" "databricks_cross_account_policy" {
  name = "databricks-cross-account-policy"
  role = aws_iam_role.databricks_cross_account.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ec2:AssignPrivateIpAddresses",
          "ec2:CancelSpotInstanceRequests",
          "ec2:CreateTags",
          "ec2:CreateVolume",
          "ec2:DeleteTags",
          "ec2:DeleteVolume",
          "ec2:DescribeAvailabilityZones",
          "ec2:DescribeIamInstanceProfileAssociations",
          "ec2:DescribeInstanceStatus",
          "ec2:DescribeInstances",
          "ec2:DescribeInternetGateways",
          "ec2:DescribeNatGateways",
          "ec2:DescribeNetworkAcls",
          "ec2:DescribeNetworkInterfaces",
          "ec2:DescribePrefixLists",
          "ec2:DescribeReservedInstancesOfferings",
          "ec2:DescribeRouteTables",
          "ec2:DescribeSecurityGroups",
          "ec2:DescribeSpotInstanceRequests",
          "ec2:DescribeSpotPriceHistory",
          "ec2:DescribeSubnets",
          "ec2:DescribeVolumes",
          "ec2:DescribeVpcAttribute",
          "ec2:DescribeVpcs",
          "ec2:DetachVolume",
          "ec2:RequestSpotInstances",
          "ec2:RunInstances",
          "ec2:TerminateInstances",
          "ec2:AttachVolume",
          "ec2:AuthorizeSecurityGroupEgress",
          "ec2:AuthorizeSecurityGroupIngress",
          "ec2:CreatePlacementGroup",
          "ec2:CreateSecurityGroup",
          "ec2:DeletePlacementGroup",
          "ec2:DeleteSecurityGroup",
          "ec2:DescribePlacementGroups",
          "ec2:RevokeSecurityGroupEgress",
          "ec2:RevokeSecurityGroupIngress",
          "ec2:ModifyInstanceMetadataOptions",
          "iam:CreateServiceLinkedRole",
          "iam:GetRolePolicy",
          "iam:ListRolePolicies",
          "iam:ListAttachedRolePolicies",
          "iam:PassRole"
        ]
        Resource = "*"
      }
    ]
  })
}

# ── Instance profile role (gives clusters S3 access) ──
resource "aws_iam_role" "databricks_instance_profile" {
  name = "databricks-instance-profile"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = { Name = "${var.project_name}-databricks-instance-profile" }
}

resource "aws_iam_role_policy_attachment" "databricks_s3_access" {
  role       = aws_iam_role.databricks_instance_profile.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_instance_profile" "databricks" {
  name = "databricks-instance-profile"
  role = aws_iam_role.databricks_instance_profile.name
}

# ── S3 root bucket for Databricks workspace storage ──
resource "aws_s3_bucket" "databricks_root" {
  bucket = "${var.project_name}-${data.aws_caller_identity.current.account_id}-dbx-root"

  tags = { Name = "${var.project_name}-dbx-root" }
}

resource "aws_s3_bucket_versioning" "databricks_root" {
  bucket = aws_s3_bucket.databricks_root.id
  versioning_configuration {
    status = "Disabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "databricks_root" {
  bucket = aws_s3_bucket.databricks_root.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# ── Outputs ──
output "databricks_cross_account_role_arn" {
  value       = aws_iam_role.databricks_cross_account.arn
  description = "ARN of the Databricks cross-account IAM role"
}

output "databricks_instance_profile_arn" {
  value       = aws_iam_instance_profile.databricks.arn
  description = "ARN of the Databricks instance profile"
}

output "databricks_root_bucket" {
  value       = aws_s3_bucket.databricks_root.bucket
  description = "S3 bucket used as Databricks workspace root storage"
}

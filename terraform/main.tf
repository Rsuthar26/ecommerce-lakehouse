# ============================================================
# main.tf — Staff DE Journey: Core Infrastructure
#
# What this provisions:
#   1. Provider config — AWS eu-west-1
#   2. Security group for RDS — port 5432 access control
#   3. DB parameter group — WAL = logical for CDC
#   4. DB subnet group — uses default VPC subnets
#   5. RDS PostgreSQL instance — db.t3.micro
#
# What this does NOT provision (managed separately):
#   - VPC: uses AWS default VPC (data source only)
#   - Databricks workspace: provisioned via Databricks console
#   - Unity Catalog external location: provisioned via CloudFormation
#   - MSK Kafka: provisioned on-demand via msk.tf when needed
#   - EC2 Debezium: provisioned on-demand via ec2.tf when needed
#
# Region: eu-west-1 (Ireland)
# Reason: Databricks does not support eu-north-1 (Stockholm).
# All infrastructure must be in eu-west-1 to co-locate with
# the Databricks workspace. See DECISIONS.md for full context.
#
# Cost: RDS db.t3.micro ~$0 on free tier
# STOP RDS at end of every session to avoid charges.
# ============================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      project = var.project_name
      env     = var.env
    }
  }
}

# ============================================================
# DATA SOURCES — reference existing AWS-managed resources
# ============================================================

data "aws_caller_identity" "current" {}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# ============================================================
# SECURITY GROUP — RDS PostgreSQL access
# ============================================================

resource "aws_security_group" "rds" {
  name        = "${var.project_name}-rds-sg"
  description = "Controls access to RDS PostgreSQL port 5432"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "PostgreSQL from allowed CIDRs"
    from_port   = var.db_port
    to_port     = var.db_port
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  egress {
    description = "Allow all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project_name}-rds-sg" }
}

# ============================================================
# DB PARAMETER GROUP — CDC configuration
# ============================================================

resource "aws_db_parameter_group" "postgres_cdc" {
  name        = "${var.project_name}-postgres15-cdc"
  family      = "postgres15"
  description = "PostgreSQL 15 with logical replication enabled for Debezium CDC"

  parameter {
    name         = "rds.logical_replication"
    value        = "1"
    apply_method = "pending-reboot"
  }

  parameter {
    name         = "max_replication_slots"
    value        = "5"
    apply_method = "pending-reboot"
  }

  parameter {
    name         = "max_wal_senders"
    value        = "5"
    apply_method = "pending-reboot"
  }

  tags = { Name = "${var.project_name}-pg-params" }
}

# ============================================================
# DB SUBNET GROUP
# ============================================================

resource "aws_db_subnet_group" "main" {
  name        = "${var.project_name}-subnet-group"
  description = "Subnet group for ecommerce-lakehouse RDS"
  subnet_ids  = data.aws_subnets.default.ids

  tags = { Name = "${var.project_name}-db-subnet-group" }
}

# ============================================================
# RDS INSTANCE
# ============================================================

resource "aws_db_instance" "postgres" {
  identifier = "${var.project_name}-postgres"

  engine         = "postgres"
  engine_version = "15.17"
  instance_class = var.db_instance_class

  allocated_storage     = var.db_allocated_storage
  max_allocated_storage = 20
  storage_type          = "gp2"
  storage_encrypted     = true

  db_name  = var.db_name
  username = var.db_username
  password = var.db_password

  vpc_security_group_ids = [aws_security_group.rds.id]
  publicly_accessible    = true
  port                   = var.db_port

  parameter_group_name = aws_db_parameter_group.postgres_cdc.name

  backup_retention_period    = 0
  backup_window              = "03:00-04:00"
  maintenance_window         = "Mon:04:00-Mon:05:00"
  auto_minor_version_upgrade = true

  deletion_protection = false
  skip_final_snapshot = true
  apply_immediately   = true

  tags = { Name = "${var.project_name}-postgres" }
}

# ============================================================
# OUTPUTS
# ============================================================

output "rds_hostname" {
  value       = aws_db_instance.postgres.address
  description = "RDS hostname — use as PG_HOST in .env"
}

output "rds_endpoint" {
  value       = aws_db_instance.postgres.endpoint
  description = "RDS endpoint with port"
}

output "rds_security_group_id" {
  value       = aws_security_group.rds.id
  description = "Security group ID — use to whitelist IPs"
}

output "vpc_id" {
  value       = data.aws_vpc.default.id
  description = "Default VPC ID"
}

output "private_subnet_ids" {
  value       = data.aws_subnets.default.ids
  description = "Default VPC subnet IDs"
}

output "connection_string_template" {
  value       = "postgresql://${var.db_username}:****@${aws_db_instance.postgres.address}:${var.db_port}/${var.db_name}"
  description = "Connection string template"
  sensitive   = true
}

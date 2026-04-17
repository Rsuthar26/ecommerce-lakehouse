# ============================================================
# main.tf — Staff DE Journey: Source 01 — RDS PostgreSQL
#
# What this provisions:
#   1. VPC + 2 subnets (RDS requires subnet group across 2 AZs)
#   2. Security group (controls who can reach port 5432)
#   3. DB parameter group (THIS is where WAL = logical lives)
#   4. DB subnet group (tells RDS which subnets to use)
#   5. RDS PostgreSQL instance (db.t3.micro, free tier)
#
# Cost: ~$0/month on free tier (first 12 months, 750hrs limit)
# After free tier: ~$15-20/month for db.t3.micro
# STOP the instance when not developing to save cost.
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
      # Every resource gets these tags — mandatory for cost tracking.
      # In AWS Cost Explorer, filter by project=staff-de-journey
      # to see exactly what this project costs.
    }
  }
}

# ============================================================
# NETWORKING — VPC + Subnets
# RDS requires a subnet group spanning at least 2 AZs.
# Even though we only use one AZ in dev, AWS enforces this.
# ============================================================

resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true # Required — RDS endpoint is a DNS hostname
  enable_dns_support   = true

  tags = { Name = "${var.project_name}-vpc" }
}

# Two subnets in different AZs — required for RDS subnet group
resource "aws_subnet" "private_a" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.1.0/24"
  availability_zone = "${var.aws_region}a"

  tags = { Name = "${var.project_name}-private-a" }
}

resource "aws_subnet" "private_b" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.2.0/24"
  availability_zone = "${var.aws_region}b"

  tags = { Name = "${var.project_name}-private-b" }
}

# Internet Gateway — needed so your local machine / Databricks can reach RDS
# In production: use VPC peering or PrivateLink instead
resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "${var.project_name}-igw" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = { Name = "${var.project_name}-rt-public" }
}

resource "aws_route_table_association" "a" {
  subnet_id      = aws_subnet.private_a.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "b" {
  subnet_id      = aws_subnet.private_b.id
  route_table_id = aws_route_table.public.id
}

# ============================================================
# SECURITY GROUP
# Restricts who can connect to port 5432.
# Never open 0.0.0.0/0 — add your actual IP in allowed_cidr_blocks.
# In the project we'll also add Debezium's IP here when we
# build Source 02.
# ============================================================

resource "aws_security_group" "rds" {
  name        = "${var.project_name}-rds-sg"
  description = "Controls access to RDS PostgreSQL port 5432"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "PostgreSQL from allowed CIDRs (your IP + Debezium)"
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
# DB PARAMETER GROUP — THE CRITICAL CDC CONFIGURATION
#
# This is the most important block in this file.
# Default RDS PostgreSQL parameters DO NOT support CDC.
# We must override 3 settings before the DB starts.
#
# wal_level = logical
#   Tells Postgres to write full row contents to WAL,
#   not just enough for crash recovery.
#   Without this: Debezium cannot read changes. Full stop.
#
# max_replication_slots = 5
#   A replication slot is Debezium's bookmark in the WAL.
#   It tells Postgres "don't delete WAL entries past this point
#   because my consumer hasn't read them yet."
#   Without this: Debezium can't create its slot.
#
# max_wal_senders = 5
#   Max concurrent WAL streaming connections.
#   Debezium uses one. We set 5 to leave room for replicas + monitoring.
#
# IMPORTANT: Changing wal_level on an existing RDS requires
# a reboot. In production that means downtime. Do it right, once.
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
# Tells RDS which subnets it can use. Must span 2 AZs.
# ============================================================

resource "aws_db_subnet_group" "main" {
  name        = "${var.project_name}-subnet-group"
  description = "Subnet group for staff-de-journey RDS"
  subnet_ids  = [aws_subnet.private_a.id, aws_subnet.private_b.id]

  tags = { Name = "${var.project_name}-db-subnet-group" }
}

# ============================================================
# RDS INSTANCE
# db.t3.micro = 2 vCPU, 1GB RAM, free tier eligible.
# This is enough for our 50K rows/day generator.
# If you hit performance issues: db.t3.small (~$30/month).
#
# publicly_accessible = true here for dev only.
# In production: false, accessed via bastion or VPC peering.
# ============================================================

resource "aws_db_instance" "postgres" {
  identifier = "${var.project_name}-postgres"

  # Engine
  engine               = "postgres"
  engine_version       = "15.17"
  instance_class       = var.db_instance_class
  
  # Storage
  allocated_storage     = var.db_allocated_storage
  max_allocated_storage = 20 # Autoscaling ceiling — prevents runaway storage cost
  storage_type          = "gp2"
  storage_encrypted     = true # Always encrypt — costs nothing, good habit

  # Credentials
  db_name  = var.db_name
  username = var.db_username
  password = var.db_password

  # Networking
  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  publicly_accessible    = true  # Dev only — change to false in production
  port                   = var.db_port

  # The CDC parameter group we defined above
  parameter_group_name = aws_db_parameter_group.postgres_cdc.name

  # Backup — 7 days, not 0 (0 disables point-in-time recovery)
  backup_retention_period = 0
  backup_window           = "03:00-04:00" # UTC — pick a quiet window

  # Maintenance
  maintenance_window         = "Mon:04:00-Mon:05:00"
  auto_minor_version_upgrade = true

  # Cost control
  deletion_protection = false   # Set true in production
  skip_final_snapshot = true    # Set false in production (would save a snapshot on delete)
  apply_immediately   = true    # Parameter group changes apply immediately in dev

  tags = { Name = "${var.project_name}-postgres" }
}

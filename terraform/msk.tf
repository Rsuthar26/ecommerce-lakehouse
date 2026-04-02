# ============================================================
# msk.tf — Staff DE Journey: MSK Kafka Cluster
#
# What this provisions:
#   1. MSK cluster (1 broker, kafka.m5.large — minimum viable)
#   2. Security group for MSK (allows Debezium + Spark access)
#   3. CloudWatch log group for MSK broker logs
#
# Cost: ~$0.21/hr per broker = ~$5/day
# DELETE when not actively developing — recreate takes 15 mins
# Command to destroy only MSK: terraform destroy -target=aws_msk_cluster.main
#
# Broker count: 1 (minimum — not fault tolerant but fine for dev)
# Production minimum: 3 brokers across 3 AZs
# ============================================================

# ── CloudWatch log group for MSK broker logs ────────────────

resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/${var.project_name}"
  retention_in_days = 7 # Keep 7 days — enough for dev debugging

  tags = { Name = "${var.project_name}-msk-logs" }
}

# ── Security group for MSK ───────────────────────────────────
# MSK needs to accept connections from:
#   - Your Mac (Debezium connector running locally)
#   - Databricks (Spark Structured Streaming)
#   - Any future services in the same VPC

resource "aws_security_group" "msk" {
  name        = "${var.project_name}-msk-sg"
  description = "Controls access to MSK Kafka brokers"
  vpc_id      = aws_vpc.main.id

  # Kafka plaintext port
  ingress {
    description = "Kafka plaintext from allowed CIDRs"
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  # Kafka TLS port
  ingress {
    description = "Kafka TLS from allowed CIDRs"
    from_port   = 9094
    to_port     = 9094
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  # Zookeeper port (needed for older Kafka clients)
  ingress {
    description = "Zookeeper from allowed CIDRs"
    from_port   = 2181
    to_port     = 2181
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  # Allow all traffic within the VPC (Spark, Lambda, etc.)
  ingress {
    description = "All traffic within VPC"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = [aws_vpc.main.cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "Kafka public TLS port"
    from_port   = 9196
    to_port     = 9196
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  tags = { Name = "${var.project_name}-msk-sg" }
}

# ── MSK cluster ──────────────────────────────────────────────
# kafka.m5.large is the minimum broker type MSK supports.
# There is no t3.micro equivalent for MSK — this is a real cost.
#
# Why 1 broker?
#   Dev only — no fault tolerance needed.
#   Replication factor = 1 means all partitions on one broker.
#   Production: 3 brokers, replication factor = 3.
#
# Kafka version 3.5.1:
#   Supports KRaft mode (no ZooKeeper dependency).
#   Most compatible with Debezium 2.x and Spark 3.x.

resource "aws_msk_cluster" "main" {
  cluster_name           = "${var.project_name}-kafka"
  kafka_version          = "3.6.0"
  number_of_broker_nodes = 2

  broker_node_group_info {
    instance_type = "kafka.m5.large"

    # Use only one subnet for single broker
    # Must match number_of_broker_nodes
    client_subnets  = [aws_subnet.private_a.id, aws_subnet.private_b.id]
    security_groups = [aws_security_group.msk.id]

    connectivity_info {
      public_access {
        type = "DISABLED"
      }
    }

    storage_info {
      ebs_storage_info {
        volume_size = 20 # GB — enough for dev, expand if topics fill up
      }
    }
  }

  # Encryption
  encryption_info {
    encryption_in_transit {
      client_broker = "TLS" # Allow plaintext for dev simplicity
      in_cluster    = true
    }
  }

  # Client authentication — none for dev
  # Production: use SASL/SCRAM or mutual TLS
  client_authentication {
    unauthenticated = true
  }

  # Kafka configuration
  # auto.create.topics.enable = true — Debezium creates topics automatically
  # log.retention.hours = 168 — keep messages for 7 days
  configuration_info {
    arn      = aws_msk_configuration.main.arn
    revision = aws_msk_configuration.main.latest_revision
  }

  # Logging to CloudWatch
  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
    }
  }

  # Monitoring
  enhanced_monitoring = "DEFAULT"

  tags = { Name = "${var.project_name}-kafka" }
}

# ── MSK configuration ────────────────────────────────────────

resource "aws_msk_configuration" "main" {
  name              = "${var.project_name}-kafka-config"
  kafka_versions    = ["3.6.0"]
  description       = "Kafka config for staff-de-journey dev cluster"

  server_properties = <<-EOT
    auto.create.topics.enable=true
    default.replication.factor=1
    min.insync.replicas=1
    num.partitions=3
    log.retention.hours=168
    log.retention.bytes=1073741824
  EOT
}

# ── Outputs ──────────────────────────────────────────────────

output "msk_bootstrap_brokers" {
  description = "MSK bootstrap broker string — use this in Debezium + Spark config"
  value       = aws_msk_cluster.main.bootstrap_brokers
}

output "msk_zookeeper_connect" {
  description = "ZooKeeper connection string"
  value       = aws_msk_cluster.main.zookeeper_connect_string
}

output "msk_cluster_arn" {
  description = "MSK cluster ARN"
  value       = aws_msk_cluster.main.arn
}

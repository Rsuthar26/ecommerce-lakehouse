# ============================================================
# ec2.tf — Debezium + Kafka Connect host
#
# user_data fully automates on first boot:
#   1. Java 17, Python deps, git
#   2. Confluent 7.6.0 + S3 Sink connector
#   3. Kafka Connect as systemd service (survives reboots)
#   4. All 5 S3 Sink connector configs via REST API
#   5. Debezium Postgres connector
#
# Brokers resolved dynamically via aws kafka get-bootstrap-brokers
# NEVER hardcoded — MSK subdomain changes every recreation
#
# RULE: terraform apply once. No recreation. Plan first, execute once.
# Cost: t3.small ~$0.023/hr — stop when not using
# ============================================================

data "aws_ami" "amazon_linux" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_security_group" "debezium" {
  name        = "${var.project_name}-debezium-sg"
  description = "Debezium + Kafka Connect host"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "SSH from allowed IPs"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  ingress {
    description = "Kafka Connect REST API"
    from_port   = 8083
    to_port     = 8083
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  egress {
    description = "All outbound - needs to reach RDS, MSK, S3"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project_name}-debezium-sg" }
}

resource "aws_instance" "debezium" {
  ami                         = data.aws_ami.amazon_linux.id
  instance_type               = "t3.small"
  key_name                    = "ecommerce-lakehouse-key"
  subnet_id                   = tolist(data.aws_subnets.default.ids)[0]
  vpc_security_group_ids      = [aws_security_group.debezium.id]
  associate_public_ip_address = true
  iam_instance_profile        = "databricks-instance-profile"

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
  }

  user_data = <<-EOF
#!/bin/bash
set -e
exec > /var/log/user-data.log 2>&1

# Download and run setup script from repo
curl -fsSL https://raw.githubusercontent.com/Rsuthar26/ecommerce-lakehouse/main/scripts/setup_kafka_connect.sh \
  -o /tmp/setup_kafka_connect.sh
chmod +x /tmp/setup_kafka_connect.sh
bash /tmp/setup_kafka_connect.sh ${aws_msk_cluster.main.arn}

  EOF

  tags = { Name = "${var.project_name}-debezium" }
}

output "debezium_public_ip" {
  description = "Public IP — SSH and Kafka Connect REST API"
  value       = aws_instance.debezium.public_ip
}

output "debezium_instance_id" {
  description = "Instance ID — use to stop/start"
  value       = aws_instance.debezium.id
}

output "ssh_command" {
  description = "Ready-to-use SSH command"
  value       = "ssh -i ~/.ssh/ecommerce-lakehouse-key.pem ec2-user@${aws_instance.debezium.public_ip}"
}

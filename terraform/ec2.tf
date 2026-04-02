# ============================================================
# ec2.tf — Staff DE Journey: Debezium Host
#
# A t3.nano EC2 instance inside the VPC that runs:
#   - Kafka Connect (with Debezium Postgres connector)
#   - All traffic stays inside the VPC — no public MSK needed
#
# Cost: t3.nano = ~$0.006/hr = ~$4.50/month
# Stop when not developing: aws ec2 stop-instances --instance-ids <id>
# ============================================================

# ── Get latest Amazon Linux 2023 AMI ────────────────────────

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

# ── Security group for EC2 ───────────────────────────────────

resource "aws_security_group" "debezium" {
  name        = "${var.project_name}-debezium-sg"
  description = "Debezium host - SSH access only"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "SSH from your IP"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  egress {
    description = "All outbound - needs to reach RDS and MSK in VPC"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project_name}-debezium-sg" }
}

# ── EC2 instance ─────────────────────────────────────────────

resource "aws_instance" "debezium" {
  ami                         = data.aws_ami.amazon_linux.id
  instance_type               = "t3.small"

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
  } # Kafka Connect needs ~1GB RAM minimum
  key_name                    = var.ec2_key_name
  subnet_id                   = aws_subnet.private_a.id
  vpc_security_group_ids      = [aws_security_group.debezium.id]
  associate_public_ip_address = true # Need public IP to SSH from your Mac

  # User data — runs on first boot
  # Installs Java, downloads Kafka Connect + Debezium connector
  user_data = <<-EOF
    #!/bin/bash
    set -e

    # Update system
    yum update -y

    # Install Java 17 (required for Kafka Connect)
    yum install -y java-17-amazon-corretto

    # Install Docker (alternative way to run Debezium)
    yum install -y docker
    systemctl start docker
    systemctl enable docker
    usermod -aG docker ec2-user

    # Create working directory
    mkdir -p /opt/debezium
    chown ec2-user:ec2-user /opt/debezium

    echo "Bootstrap complete" > /opt/debezium/ready.txt
  EOF

  tags = { Name = "${var.project_name}-debezium" }
}

# ── Outputs ──────────────────────────────────────────────────

output "debezium_public_ip" {
  description = "SSH to this IP to manage Debezium"
  value       = aws_instance.debezium.public_ip
}

output "debezium_instance_id" {
  description = "Instance ID — use to stop/start the instance"
  value       = aws_instance.debezium.id
}

output "ssh_command" {
  description = "Ready-to-use SSH command"
  value       = "ssh -i ~/.ssh/staff-de-journey-key.pem ec2-user@${aws_instance.debezium.public_ip}"
}

resource "aws_eip_association" "debezium" {
  instance_id   = aws_instance.debezium.id
  allocation_id = "eipalloc-0ca6709d9686a9e78"
}

output "debezium_elastic_ip" {
  value = "56.228.75.74"
  description = "Permanent static IP for EC2 Debezium host"
}

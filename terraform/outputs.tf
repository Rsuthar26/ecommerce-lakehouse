# ============================================================
# outputs.tf — Values you'll need after terraform apply
#
# These are referenced by:
#   - Schema setup script (needs the endpoint)
#   - Data generator (needs endpoint + credentials)
#   - Debezium config (Source 02 — needs endpoint + port)
#   - Airflow connections (needs all of the above)
# ============================================================

output "rds_endpoint" {
  description = "RDS connection endpoint — use this everywhere instead of the IP"
  value       = aws_db_instance.postgres.endpoint
  # Format: <identifier>.<random>.<region>.rds.amazonaws.com:5432
}

output "rds_hostname" {
  description = "Hostname only (without port) — needed for some connection strings"
  value       = aws_db_instance.postgres.address
}

output "rds_port" {
  description = "PostgreSQL port"
  value       = aws_db_instance.postgres.port
}

output "rds_db_name" {
  description = "Database name"
  value       = aws_db_instance.postgres.db_name
}

output "rds_username" {
  description = "Master username"
  value       = aws_db_instance.postgres.username
  sensitive   = true # Won't print in console — use: terraform output rds_username
}

output "vpc_id" {
  description = "VPC ID — needed when adding MSK, Debezium to the same network"
  value       = aws_vpc.main.id
}

output "private_subnet_ids" {
  description = "Subnet IDs — needed for MSK broker placement (Source 02)"
  value       = [aws_subnet.private_a.id, aws_subnet.private_b.id]
}

output "rds_security_group_id" {
  description = "RDS security group — add Debezium's SG as an ingress rule here"
  value       = aws_security_group.rds.id
}

output "connection_string_template" {
  description = "Template for psql connection — replace PASSWORD with actual value"
  value       = "postgresql://${aws_db_instance.postgres.username}:PASSWORD@${aws_db_instance.postgres.endpoint}/${aws_db_instance.postgres.db_name}"
  sensitive   = true
}

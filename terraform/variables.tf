# ============================================================
# variables.tf — Staff DE Journey
# All configurable values in one place.
# Never hardcode credentials — use environment variables or
# AWS Secrets Manager. These are structure-only defaults.
# ============================================================

variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "eu-north-1" # London — change to your nearest region
}

variable "project_name" {
  description = "Project tag applied to every resource — used for cost tracking"
  type        = string
  default     = "ecommerce-lakehouse"
}

variable "env" {
  description = "Environment tag"
  type        = string
  default     = "dev"
}

# ---- RDS -------------------------------------------------------

variable "db_name" {
  description = "Name of the PostgreSQL database"
  type        = string
  default     = "ecommerce"
}

variable "db_username" {
  description = "Master username — override via TF_VAR_db_username env var"
  type        = string
  default     = "postgres_admin"
  sensitive   = true
}

variable "db_password" {
  description = "Master password — ALWAYS override via TF_VAR_db_password env var, never commit"
  type        = string
  sensitive   = true
}

variable "db_instance_class" {
  description = "RDS instance size. t3.micro is free-tier eligible (750hrs/month)"
  type        = string
  default     = "db.t3.micro"
}

variable "db_allocated_storage" {
  description = "Storage in GB. 20 is the free-tier minimum"
  type        = number
  default     = 20
}

variable "db_port" {
  description = "PostgreSQL port"
  type        = number
  default     = 5432
}

# ---- Networking ------------------------------------------------

variable "vpc_cidr" {
  description = "CIDR block for the project VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "allowed_cidr_blocks" {
  description = "CIDRs allowed to reach the RDS instance. Add your local IP here."
  type        = list(string)
  default     = [] # Set via tfvars — do not leave open (0.0.0.0/0) in real environments
}

variable "ec2_key_name" {
  description = "EC2 key pair name for bastion/Debezium host"
  type        = string
  default     = "staff-de-journey-key"
}

# ──────────────────────────────────────────────
# AWS
# ──────────────────────────────────────────────

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "ap-southeast-2"
}

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "nz-electricity"
}

variable "s3_bucket_name" {
  description = "S3 data lake bucket name"
  type        = string
}

variable "budget_alert_email" {
  description = "Email address for AWS budget alerts"
  type        = string
}

# ──────────────────────────────────────────────
# Snowflake
# ──────────────────────────────────────────────

variable "snowflake_organization_name" {
  description = "Snowflake organization name"
  type        = string
}

variable "snowflake_account_name" {
  description = "Snowflake account name"
  type        = string
}

variable "snowflake_user" {
  description = "Snowflake admin user for Terraform provider"
  type        = string
}

variable "snowflake_password" {
  description = "Snowflake admin password for Terraform provider"
  type        = string
  sensitive   = true
}

variable "snowflake_database" {
  description = "Snowflake database name"
  type        = string
  default     = "NZ_ELECTRICITY"
}

variable "snowflake_transformer_password" {
  description = "Password for the transformer service account"
  type        = string
  sensitive   = true
}

variable "snowflake_reader_password" {
  description = "Password for the reader service account"
  type        = string
  sensitive   = true
}

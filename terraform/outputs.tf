# ──────────────────────────────────────────────
# AWS
# ──────────────────────────────────────────────

output "s3_bucket_name" {
  description = "S3 data lake bucket name"
  value       = aws_s3_bucket.data_lake.id
}

output "s3_bucket_arn" {
  description = "S3 data lake bucket ARN"
  value       = aws_s3_bucket.data_lake.arn
}

output "snowflake_s3_access_key_id" {
  description = "IAM access key ID for Snowflake S3 access"
  value       = aws_iam_access_key.snowflake_s3.id
  sensitive   = true
}

output "snowflake_s3_secret_access_key" {
  description = "IAM secret access key for Snowflake S3 access"
  value       = aws_iam_access_key.snowflake_s3.secret
  sensitive   = true
}

# ──────────────────────────────────────────────
# Snowflake
# ──────────────────────────────────────────────

output "snowflake_database" {
  description = "Snowflake database name"
  value       = snowflake_database.this.name
}

output "snowflake_raw_schema" {
  description = "Snowflake raw schema name"
  value       = snowflake_schema.raw.name
}

output "snowflake_analytics_schema" {
  description = "Snowflake analytics schema name"
  value       = snowflake_schema.analytics.name
}

output "snowflake_transform_warehouse" {
  description = "Snowflake transform warehouse name"
  value       = snowflake_warehouse.transform.name
}

output "snowflake_dashboard_warehouse" {
  description = "Snowflake dashboard warehouse name"
  value       = snowflake_warehouse.dashboard.name
}

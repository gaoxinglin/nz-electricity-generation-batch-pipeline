# ──────────────────────────────────────────────
# S3 Data Lake Bucket
# ──────────────────────────────────────────────

resource "aws_s3_bucket" "data_lake" {
  bucket = var.s3_bucket_name
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "transition-to-ia"
    status = "Enabled"

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ──────────────────────────────────────────────
# IAM User for Snowflake S3 Access
# ──────────────────────────────────────────────

resource "aws_iam_user" "snowflake_s3" {
  name = "${var.project_name}-snowflake-s3"
}

resource "aws_iam_access_key" "snowflake_s3" {
  user = aws_iam_user.snowflake_s3.name
}

resource "aws_iam_user_policy" "snowflake_s3" {
  name = "${var.project_name}-snowflake-s3-policy"
  user = aws_iam_user.snowflake_s3.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*",
        ]
      }
    ]
  })
}

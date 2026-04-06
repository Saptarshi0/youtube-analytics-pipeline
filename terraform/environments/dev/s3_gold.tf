# ============================================================
# Gold S3 bucket — analytics-ready Delta Lake tables
# Mirrors the Silver bucket config; force_destroy=false
# because Gold holds the source of truth for dbt + Streamlit.
# ============================================================
resource "aws_s3_bucket" "gold" {
  bucket        = "youtube-analytics-gold-${var.environment}"
  force_destroy = false
  tags          = local.common_tags
}

resource "aws_s3_bucket_versioning" "gold" {
  bucket = aws_s3_bucket.gold.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "gold" {
  bucket = aws_s3_bucket.gold.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "gold" {
  bucket                  = aws_s3_bucket.gold.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "gold" {
  bucket = aws_s3_bucket.gold.id

  # Gold data is read frequently by Streamlit + dbt — keep hot for 60 days
  # then move to Intelligent-Tiering (older aggregations queried rarely)
  rule {
    id     = "gold-intelligent-tiering"
    status = "Enabled"
    filter { prefix = "gold/" }

    transition {
      days          = 60
      storage_class = "INTELLIGENT_TIERING"
    }
  }

  # Delta transaction logs — 90 days matches Delta's default log retention
  rule {
    id     = "gold-delta-log-expiry"
    status = "Enabled"
    filter { prefix = "gold/" }

    expiration { days = 90 }
  }
}

# ============================================================
# Output
# ============================================================
output "gold_bucket_name" {
  value       = aws_s3_bucket.gold.bucket
  description = "Gold S3 bucket — Delta Lake tables for dbt and Streamlit"
}

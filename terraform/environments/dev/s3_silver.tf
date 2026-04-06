# ============================================================
# Silver S3 bucket
# ============================================================
resource "aws_s3_bucket" "silver" {
  bucket = "youtube-analytics-silver-${var.environment}"

  # Do NOT force_destroy Silver — it holds the source-of-truth Delta tables
  force_destroy = false

  tags = local.common_tags
}

resource "aws_s3_bucket_versioning" "silver" {
  bucket = aws_s3_bucket.silver.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "silver" {
  bucket = aws_s3_bucket.silver.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "silver" {
  bucket                  = aws_s3_bucket.silver.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "silver" {
  bucket = aws_s3_bucket.silver.id

  # Move Silver data to Intelligent-Tiering after 30 days —
  # trending data is hot on ingest day, cold within a week
  rule {
    id     = "silver-intelligent-tiering"
    status = "Enabled"
    filter { prefix = "silver/" }

    transition {
      days          = 30
      storage_class = "INTELLIGENT_TIERING"
    }
  }

  # Delta transaction logs grow indefinitely with every write.
  # VACUUM keeps data files clean; this lifecycle rule handles the log files.
  # 90 days matches the default Delta Lake log retention window.
  rule {
    id     = "silver-delta-log-expiry"
    status = "Enabled"
    filter { prefix = "silver/videos/_delta_log/" }

    expiration { days = 90 }
  }
}

# ============================================================
# Output
# ============================================================
output "silver_bucket_name" {
  value = aws_s3_bucket.silver.bucket
}
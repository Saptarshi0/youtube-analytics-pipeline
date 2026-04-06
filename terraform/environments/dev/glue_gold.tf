# ============================================================
# glue_gold.tf — Gold layer additions
# Adds to the existing Glue role (defined in glue.tf):
#   • S3 read/write policy for the Gold bucket
#   • Glue Catalog policy for the 4 Gold tables
#   • Script upload to Glue assets bucket
#   • Gold Glue job definition
# ============================================================

# ── IAM: Gold S3 access ───────────────────────────────────
resource "aws_iam_policy" "glue_gold_s3_access" {
  name        = "youtube-glue-gold-s3-access-${var.environment}"
  description = "Allows Glue job role to read Silver and write Gold S3 buckets"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "SilverRead"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::${aws_s3_bucket.silver.bucket}",
          "arn:aws:s3:::${aws_s3_bucket.silver.bucket}/*",
        ]
      },
      {
        Sid    = "GoldReadWrite"
        Effect = "Allow"
        Action = [
          "s3:GetObject", "s3:PutObject",
          "s3:DeleteObject", "s3:ListBucket",
        ]
        Resource = [
          "arn:aws:s3:::${aws_s3_bucket.gold.bucket}",
          "arn:aws:s3:::${aws_s3_bucket.gold.bucket}/*",
        ]
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_gold_s3_attach" {
  role       = aws_iam_role.glue_silver_role.name
  policy_arn = aws_iam_policy.glue_gold_s3_access.arn
}

# ── IAM: Gold Glue Catalog access ────────────────────────
resource "aws_iam_policy" "glue_gold_catalog_access" {
  name        = "youtube-glue-gold-catalog-access-${var.environment}"
  description = "Allows Glue job to register and update the 4 Gold tables in Glue Catalog"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GoldCatalogAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:CreateTable",
          "glue:UpdateTable",
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/youtube_analytics_${var.environment}",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/youtube_analytics_${var.environment}/gold_daily_trending_rank",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/youtube_analytics_${var.environment}/gold_category_stats",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/youtube_analytics_${var.environment}/gold_channel_stats",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/youtube_analytics_${var.environment}/gold_view_velocity",
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_gold_catalog_attach" {
  role       = aws_iam_role.glue_silver_role.name
  policy_arn = aws_iam_policy.glue_gold_catalog_access.arn
}

# ── Script upload ─────────────────────────────────────────
resource "aws_s3_object" "gold_job_script" {
  bucket = aws_s3_bucket.glue_assets.id
  key    = "scripts/glue_gold_job.py"
  source = "${path.module}/../../../glue_jobs/silver_to_gold/glue_gold_job.py"
  etag   = filemd5("${path.module}/../../../glue_jobs/silver_to_gold/glue_gold_job.py")
}

# ── Glue Job: Gold Transform ──────────────────────────────
resource "aws_glue_job" "gold_transform" {
  name         = "youtube-gold-transform-${var.environment}"
  role_arn     = aws_iam_role.glue_silver_role.arn
  glue_version = "4.0"

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.glue_assets.bucket}/scripts/glue_gold_job.py"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--enable-glue-datacatalog"          = "true"
    "--TempDir"                          = "s3://${aws_s3_bucket.glue_assets.bucket}/temp/"
    "--datalake-formats"                 = "delta"
    "--silver_bucket"                    = aws_s3_bucket.silver.bucket
    "--silver_prefix"                    = "silver/videos/"
    "--gold_bucket"                      = aws_s3_bucket.gold.bucket
    "--gold_prefix"                      = "gold/"
    "--glue_database"                    = aws_glue_catalog_database.youtube_analytics.name
  }

  execution_property { max_concurrent_runs = 1 }
  number_of_workers = 4
  worker_type       = "G.1X"
  timeout           = 60

  tags = local.common_tags

  depends_on = [
    aws_s3_object.gold_job_script,
    aws_iam_role_policy_attachment.glue_gold_s3_attach,
    aws_iam_role_policy_attachment.glue_gold_catalog_attach,
  ]
}

# ── Outputs ───────────────────────────────────────────────
output "gold_transform_job_name" {
  value       = aws_glue_job.gold_transform.name
  description = "Trigger with: aws glue start-job-run --job-name youtube-gold-transform-dev"
}

output "gold_catalog_tables" {
  value = [
    "gold_daily_trending_rank",
    "gold_category_stats",
    "gold_channel_stats",
    "gold_view_velocity",
  ]
  description = "4 Gold Delta Lake tables registered in the Glue Catalog"
}

# ============================================================
# IAM Role for Glue jobs
# ============================================================
resource "aws_iam_role" "glue_silver_role" {
  name = "youtube-glue-silver-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "glue_service_managed" {
  role       = aws_iam_role.glue_silver_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_policy" "glue_s3_access" {
  name = "youtube-glue-s3-access-${var.environment}"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "BronzeRead"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::${aws_s3_bucket.bronze.bucket}",
          "arn:aws:s3:::${aws_s3_bucket.bronze.bucket}/*",
        ]
      },
      {
        Sid    = "SilverReadWrite"
        Effect = "Allow"
        Action = [
          "s3:GetObject", "s3:PutObject",
          "s3:DeleteObject", "s3:ListBucket",
        ]
        Resource = [
          "arn:aws:s3:::${aws_s3_bucket.silver.bucket}",
          "arn:aws:s3:::${aws_s3_bucket.silver.bucket}/*",
        ]
      },
      {
        Sid    = "GlueAssetsReadWrite"
        Effect = "Allow"
        Action = [
          "s3:GetObject", "s3:PutObject",
          "s3:DeleteObject", "s3:ListBucket",
        ]
        Resource = [
          "arn:aws:s3:::${aws_s3_bucket.glue_assets.bucket}",
          "arn:aws:s3:::${aws_s3_bucket.glue_assets.bucket}/*",
        ]
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_s3_attach" {
  role       = aws_iam_role.glue_silver_role.name
  policy_arn = aws_iam_policy.glue_s3_access.arn
}

# ============================================================
# NEW: Glue Catalog IAM Policy
# Required by the boto3-based catalog registration in
# glue_silver_job.py (Step 6). The spark.sql("CREATE DATABASE")
# approach set an empty LocationUri which caused:
#   IllegalArgumentException: Can not create a Path from an empty string
# boto3 glue_client calls bypass Spark's path resolution entirely
# but need explicit Glue Catalog permissions on the role.
# ============================================================
resource "aws_iam_policy" "glue_catalog_access" {
  name        = "youtube-glue-catalog-access-${var.environment}"
  description = "Allows Glue job to register and update tables in the Glue Data Catalog via boto3"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GlueCatalogAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:CreateTable",
          "glue:UpdateTable",
        ]
        Resource = [
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:database/youtube_analytics_${var.environment}",
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/youtube_analytics_${var.environment}/silver_youtube_videos",
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_catalog_attach" {
  role       = aws_iam_role.glue_silver_role.name
  policy_arn = aws_iam_policy.glue_catalog_access.arn
}

# ============================================================
# Glue Assets bucket (scripts + GE validation reports)
# ============================================================
resource "aws_s3_bucket" "glue_assets" {
  bucket        = "youtube-glue-assets-${var.environment}-${data.aws_caller_identity.current.account_id}"
  force_destroy = true
  tags          = local.common_tags
}

resource "aws_s3_bucket_versioning" "glue_assets" {
  bucket = aws_s3_bucket.glue_assets.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_public_access_block" "glue_assets" {
  bucket                  = aws_s3_bucket.glue_assets.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Upload scripts — re-uploaded automatically when file content changes
resource "aws_s3_object" "silver_job_script" {
  bucket = aws_s3_bucket.glue_assets.id
  key    = "scripts/glue_silver_job.py"
  source = "${path.module}/../../../glue_jobs/bronze_to_silver/glue_silver_job.py"
  etag   = filemd5("${path.module}/../../../glue_jobs/bronze_to_silver/glue_silver_job.py")
}

resource "aws_s3_object" "ge_validation_script" {
  bucket = aws_s3_bucket.glue_assets.id
  key    = "scripts/great_expectations_validation.py"
  source = "${path.module}/../../../glue_jobs/bronze_to_silver/great_expectations_validation.py"
  etag   = filemd5("${path.module}/../../../glue_jobs/bronze_to_silver/great_expectations_validation.py")
}

# ============================================================
# Glue Catalog Database
# ============================================================
resource "aws_glue_catalog_database" "youtube_analytics" {
  name        = "youtube_analytics_${var.environment}"
  description = "YouTube Analytics pipeline — Silver and Gold layers"
}

# ============================================================
# Job 1: GE Bronze Validation
# ============================================================
resource "aws_glue_job" "ge_bronze_validation" {
  name         = "youtube-ge-bronze-validation-${var.environment}"
  role_arn     = aws_iam_role.glue_silver_role.arn
  glue_version = "4.0"

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.glue_assets.bucket}/scripts/great_expectations_validation.py"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--TempDir"                          = "s3://${aws_s3_bucket.glue_assets.bucket}/temp/"
    "--bronze_bucket"                    = aws_s3_bucket.bronze.bucket
    "--bronze_prefix"                    = "2026/"
    "--results_bucket"                   = aws_s3_bucket.glue_assets.bucket
    "--results_prefix"                   = "ge_results/bronze/"
    "--fail_on_error"                    = "true"
    "--additional-python-modules"        = "great_expectations==0.18.19"
  }

  execution_property { max_concurrent_runs = 1 }
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 30

  tags = local.common_tags

  depends_on = [aws_s3_object.ge_validation_script]
}

# ============================================================
# Job 2: Silver Transformation
# ============================================================
resource "aws_glue_job" "silver_transform" {
  name         = "youtube-silver-transform-${var.environment}"
  role_arn     = aws_iam_role.glue_silver_role.arn
  glue_version = "4.0"

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.glue_assets.bucket}/scripts/glue_silver_job.py"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--enable-glue-datacatalog"          = "true"
    "--TempDir"                          = "s3://${aws_s3_bucket.glue_assets.bucket}/temp/"
    "--datalake-formats"                 = "delta"
    "--bronze_bucket"                    = aws_s3_bucket.bronze.bucket
    "--bronze_prefix"                    = "2026/"
    "--silver_bucket"                    = aws_s3_bucket.silver.bucket
    "--silver_prefix"                    = "silver/videos/"
    "--glue_database"                    = aws_glue_catalog_database.youtube_analytics.name
  }

  execution_property { max_concurrent_runs = 1 }
  number_of_workers = 4
  worker_type       = "G.1X"
  timeout           = 60

  tags = local.common_tags

  depends_on = [
    aws_s3_object.silver_job_script,
    aws_iam_role_policy_attachment.glue_catalog_attach,  # ensure catalog perms are in place before job runs
  ]
}

# ============================================================
# Outputs
# ============================================================
output "ge_validation_job_name" {
  value = aws_glue_job.ge_bronze_validation.name
}

output "silver_transform_job_name" {
  value = aws_glue_job.silver_transform.name
}

output "glue_catalog_database" {
  value = aws_glue_catalog_database.youtube_analytics.name
}

output "glue_catalog_policy_arn" {
  value       = aws_iam_policy.glue_catalog_access.arn
  description = "ARN of the Glue Catalog access policy attached to the Glue job role"
}

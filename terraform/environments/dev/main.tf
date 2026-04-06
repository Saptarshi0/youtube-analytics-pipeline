terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ── S3 Bronze Bucket ──────────────────────────────────
resource "aws_s3_bucket" "bronze" {
  bucket = "${var.project_name}-bronze-${var.environment}"
  tags   = local.common_tags
}

resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration { status = "Enabled" }
}

# ── Kinesis Firehose ──────────────────────────────────
resource "aws_kinesis_firehose_delivery_stream" "youtube" {
  name        = "${var.project_name}-firehose"
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose.arn
    bucket_arn = aws_s3_bucket.bronze.arn
  }
}
# ------ Locals block ----------------------------------------
locals {
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

# --------------- youtube_api_key ------------------------
resource "aws_secretsmanager_secret" "youtube_api" {
  name = "${var.project_name}/youtube-api-key"
}

resource "aws_secretsmanager_secret_version" "youtube_api" {
  secret_id     = aws_secretsmanager_secret.youtube_api.id
  secret_string = jsonencode({ api_key = var.youtube_api_key })
}

# ── Lambda Function ───────────────────────────────────
resource "aws_lambda_function" "youtube_ingest" {
  function_name = "${var.project_name}-ingest"
  runtime       = "python3.11"
  handler       = "lambda_function.lambda_handler"
  role          = aws_iam_role.lambda.arn
  filename      = "${path.module}/lambda_function.zip"
  timeout       = 300
  memory_size   = 256

  environment {
    variables = {
      FIREHOSE_STREAM_NAME = aws_kinesis_firehose_delivery_stream.youtube.name
      SECRET_NAME          = aws_secretsmanager_secret.youtube_api.name
      REGIONS              = "US,GB,IN,CA"
    }
  }
  tags = local.common_tags
}

# ── EventBridge Scheduler ─────────────────────────────
resource "aws_cloudwatch_event_rule" "youtube_trigger" {
  name                = "${var.project_name}-trigger"
  description         = "Trigger YouTube ingestion"
  schedule_expression = "cron(0 2 * * ? *)"
}

resource "aws_cloudwatch_event_target" "lambda" {
  rule = aws_cloudwatch_event_rule.youtube_trigger.name
  arn  = aws_lambda_function.youtube_ingest.arn
}

resource "aws_lambda_permission" "eventbridge" {
  statement_id  = "AllowEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.youtube_ingest.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.youtube_trigger.arn
}


# fetch my AWS Account ID at runtime
data "aws_caller_identity" "current" {}
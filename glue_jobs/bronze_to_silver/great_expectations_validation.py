import sys
import json
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, ArrayType
)

import great_expectations as ge
from great_expectations.dataset import SparkDFDataset

# --------------------------------------------------------------------------- #
# Job parameters — required
# --------------------------------------------------------------------------- #
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "bronze_bucket",
    "results_bucket",
    "results_prefix",
])

# Optional arguments with safe defaults
try:
    args["bronze_prefix"] = getResolvedOptions(sys.argv, ["bronze_prefix"])["bronze_prefix"]
except Exception:
    args["bronze_prefix"] = "2026/"

try:
    args["fail_on_error"] = getResolvedOptions(sys.argv, ["fail_on_error"])["fail_on_error"]
except Exception:
    args["fail_on_error"] = "true"

sc        = SparkContext()
glueContext = GlueContext(sc)
spark     = glueContext.spark_session
job       = Job(glueContext)
job.init(args["JOB_NAME"], args)

BRONZE_PREFIX = args['bronze_prefix'].rstrip('/')
BRONZE_PATH = f"s3://{args['bronze_bucket']}/{BRONZE_PREFIX}/*/*/*/*"
RESULTS_PATH = f"s3://{args['results_bucket']}/{args['results_prefix']}"
FAIL_ON_ERROR = args["fail_on_error"].lower() == "true"

# --------------------------------------------------------------------------- #
# Bronze schema — matches Lambda output exactly
# --------------------------------------------------------------------------- #
BRONZE_SCHEMA = StructType([
    StructField("video_id",         StringType(),            False),
    StructField("channel_id",       StringType(),            True),
    StructField("region",           StringType(),            True),
    StructField("title",            StringType(),            True),
    StructField("description",      StringType(),            True),
    StructField("channel_title",    StringType(),            True),
    StructField("category_id",      StringType(),            True),
    StructField("tags",             ArrayType(StringType()), True),
    StructField("duration",         StringType(),            True),
    StructField("default_language", StringType(),            True),
    StructField("published_at",     StringType(),            True),
    StructField("ingested_at",      StringType(),            True),
    StructField("view_count",       LongType(),              True),
    StructField("like_count",       LongType(),              True),
    StructField("comment_count",    LongType(),              True),
    StructField("source",           StringType(),            True),
    StructField("pipeline_version", StringType(),            True),
])

# --------------------------------------------------------------------------- #
# Read Bronze
# --------------------------------------------------------------------------- #
print(f"[GE] Reading Bronze from: {BRONZE_PATH}")
bronze_df = (
    spark.read
    .schema(BRONZE_SCHEMA)
    .option("multiline", "false")
    .json(BRONZE_PATH)
)

# Composite key for uniqueness check
bronze_df = bronze_df.withColumn(
    "_video_region_key",
    F.concat_ws("::", F.col("video_id"), F.col("region"))
)

ge_df = SparkDFDataset(bronze_df)

# --------------------------------------------------------------------------- #
# Expectation runner
# --------------------------------------------------------------------------- #
results = []

def check(name: str, result: dict, critical: bool = True) -> bool:
    passed = result["success"]
    tag    = "PASS" if passed else ("FAIL [CRITICAL]" if critical else "FAIL [WARN]")
    print(f"[GE] {tag}: {name}")
    results.append({
        "expectation": name,
        "critical":    critical,
        "success":     passed,
        "result":      result.get("result", {}),
    })
    return passed

# --------------------------------------------------------------------------- #
# Completeness — critical
# --------------------------------------------------------------------------- #
check("video_id_not_null",
    ge_df.expect_column_values_to_not_be_null("video_id"))

check("region_not_null",
    ge_df.expect_column_values_to_not_be_null("region"))

check("ingested_at_not_null",
    ge_df.expect_column_values_to_not_be_null("ingested_at"))

check("view_count_not_null",
    ge_df.expect_column_values_to_not_be_null("view_count"))

check("channel_id_not_null",
    ge_df.expect_column_values_to_not_be_null("channel_id"))

# --------------------------------------------------------------------------- #
# Validity — critical
# --------------------------------------------------------------------------- #
check("region_in_expected_set",
    ge_df.expect_column_values_to_be_in_set("region", ["US", "GB", "IN", "CA"]))

check("view_count_non_negative",
    ge_df.expect_column_values_to_be_between("view_count", min_value=0))

check("like_count_non_negative",
    ge_df.expect_column_values_to_be_between("like_count", min_value=0))

check("comment_count_non_negative",
    ge_df.expect_column_values_to_be_between("comment_count", min_value=0))

check("source_is_youtube_trending",
    ge_df.expect_column_values_to_be_in_set("source", ["youtube_trending"]))

# --------------------------------------------------------------------------- #
# Format — 95% threshold to allow edge-case API responses
# --------------------------------------------------------------------------- #
check("published_at_iso8601_format",
    ge_df.expect_column_values_to_match_regex(
        "published_at",
        r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$",
        mostly=0.95,
    )
)

check("ingested_at_iso8601_format",
    ge_df.expect_column_values_to_match_regex(
        "ingested_at",
        r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}",
        mostly=0.95,
    )
)

check("duration_iso8601_format",
    ge_df.expect_column_values_to_match_regex(
        "duration",
        r"^PT(\d+H)?(\d+M)?(\d+S)?$",
        mostly=0.95,
    )
)

# --------------------------------------------------------------------------- #
# Uniqueness — warn only (Firehose retries naturally cause duplicates)
# --------------------------------------------------------------------------- #
check("video_region_key_mostly_unique",
    ge_df.expect_column_values_to_be_unique(
        "_video_region_key",
        mostly=0.95,
    ),
    critical=False,
)

# --------------------------------------------------------------------------- #
# Volume — critical
# --------------------------------------------------------------------------- #
check("dataset_not_empty",
    ge_df.expect_table_row_count_to_be_between(min_value=1))

# --------------------------------------------------------------------------- #
# Write validation report to S3
# Using spark.write instead of saveAsTextFile (Glue 4.0 compatible)
# --------------------------------------------------------------------------- #
run_ts            = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
critical_failures = [r for r in results if not r["success"] and r["critical"]]
warn_failures     = [r for r in results if not r["success"] and not r["critical"]]

summary = {
    "run_timestamp":      run_ts,
    "bronze_path":        BRONZE_PATH,
    "total_expectations": len(results),
    "passed":             sum(1 for r in results if r["success"]),
    "critical_failures":  len(critical_failures),
    "warning_failures":   len(warn_failures),
    "overall_success":    len(critical_failures) == 0,
    "results":            results,
}

report_json = json.dumps(summary, indent=2, default=str)
report_path = f"{RESULTS_PATH}{run_ts}/"

report_df = spark.createDataFrame([(report_json,)], ["content"])
report_df.coalesce(1).write.mode("overwrite").text(report_path)

print(f"[GE] Validation report written to: {report_path}")
print(f"[GE] Passed: {summary['passed']} / {summary['total_expectations']}")

if warn_failures:
    print(f"[GE] Warnings (non-blocking): {[r['expectation'] for r in warn_failures]}")

# --------------------------------------------------------------------------- #
# Halt pipeline on critical failures
# --------------------------------------------------------------------------- #
if FAIL_ON_ERROR and critical_failures:
    raise Exception(
        f"[GE] Bronze validation FAILED — {len(critical_failures)} critical "
        f"expectation(s) not met: {[r['expectation'] for r in critical_failures]}. "
        f"Silver job will not run."
    )

print("[GE] Bronze validation passed. Silver job may proceed.")
job.commit()
import sys
import re
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, DoubleType, ArrayType,
)
from delta.tables import DeltaTable

# --------------------------------------------------------------------------- #
# Job parameters — all required, all set in glue.tf default_arguments
# --------------------------------------------------------------------------- #
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "bronze_bucket",
    "bronze_prefix",
    "silver_bucket",
    "silver_prefix",
    "glue_database",
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Debug — print every resolved arg so we can see exactly what Glue passed in
print(f"[SILVER] bronze_bucket  : '{args['bronze_bucket']}'")
print(f"[SILVER] bronze_prefix  : '{args['bronze_prefix']}'")
print(f"[SILVER] silver_bucket  : '{args['silver_bucket']}'")
print(f"[SILVER] silver_prefix  : '{args['silver_prefix']}'")
print(f"[SILVER] glue_database  : '{args['glue_database']}'")

BRONZE_PREFIX = args['bronze_prefix'].rstrip('/')
SILVER_PREFIX = args['silver_prefix'].rstrip('/')
BRONZE_PATH = f"s3://{args['bronze_bucket']}/{BRONZE_PREFIX}/*/*/*/*"
SILVER_PATH   = f"s3://{args['silver_bucket']}/{SILVER_PREFIX}"
GLUE_DATABASE = args["glue_database"]
GLUE_TABLE    = "silver_youtube_videos"

print(f"[SILVER] BRONZE_PATH    : '{BRONZE_PATH}'")
print(f"[SILVER] SILVER_PATH    : '{SILVER_PATH}'")

# --------------------------------------------------------------------------- #
# Bronze schema
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
# UDF: ISO 8601 duration → seconds
# --------------------------------------------------------------------------- #
_DURATION_PATTERN = re.compile(r"P(?:(\d+)D)?T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?")

def iso_duration_to_seconds(duration: str):
    if not duration:
        return None
    m = _DURATION_PATTERN.match(duration)
    if not m:
        return None
    days    = int(m.group(1) or 0)
    hours   = int(m.group(2) or 0)
    minutes = int(m.group(3) or 0)
    seconds = int(m.group(4) or 0)
    return days * 86400 + hours * 3600 + minutes * 60 + seconds

duration_udf = F.udf(iso_duration_to_seconds, IntegerType())

# --------------------------------------------------------------------------- #
# 1. Read Bronze
# --------------------------------------------------------------------------- #
print(f"[SILVER] Reading Bronze from: {BRONZE_PATH}")

bronze_df = (
    spark.read
    .schema(BRONZE_SCHEMA)
    .option("multiline", "false")
    .json(BRONZE_PATH)
)

raw_count = bronze_df.count()
print(f"[SILVER] Raw Bronze record count: {raw_count}")

if raw_count == 0:
    raise Exception("[SILVER] Bronze path returned 0 records. Aborting.")

# --------------------------------------------------------------------------- #
# 2. Drop rows missing primary keys
# --------------------------------------------------------------------------- #
bronze_clean = bronze_df.dropna(subset=["video_id", "region"])
dropped      = raw_count - bronze_clean.count()
print(f"[SILVER] Records dropped (null video_id or region): {dropped}")

# --------------------------------------------------------------------------- #
# 3. Transformations
# --------------------------------------------------------------------------- #
silver_df = (
    bronze_clean
    .withColumn("published_at",  F.to_timestamp("published_at"))
    .withColumn("ingested_at",   F.to_timestamp("ingested_at"))
    .withColumn("category_id",   F.col("category_id").cast(IntegerType()))
    .withColumn("duration_seconds", duration_udf(F.col("duration")))
    .withColumn("tags_count",
        F.when(F.col("tags").isNull(), F.lit(0))
         .otherwise(F.size(F.col("tags")))
    )
    .withColumn("engagement_rate",
        F.when(F.col("view_count") > 0,
            (F.col("like_count") + F.col("comment_count")).cast(DoubleType())
            / F.col("view_count").cast(DoubleType())
        ).otherwise(F.lit(0.0))
    )
    .withColumn("like_to_view_ratio",
        F.when(F.col("view_count") > 0,
            F.col("like_count").cast(DoubleType())
            / F.col("view_count").cast(DoubleType())
        ).otherwise(F.lit(0.0))
    )
    .withColumn("published_year",  F.year("published_at"))
    .withColumn("published_month", F.month("published_at"))
    .withColumn("published_date",  F.to_date("published_at"))
    .withColumn("_silver_processed_at", F.current_timestamp())
    .withColumn("_pipeline_version",    F.col("pipeline_version"))
    .drop("duration", "pipeline_version")
)

# --------------------------------------------------------------------------- #
# 4. Deduplication — keep latest ingested_at per (video_id, region)
# --------------------------------------------------------------------------- #
dedup_window = Window.partitionBy("video_id", "region").orderBy(F.col("ingested_at").desc())

silver_deduped = (
    silver_df
    .withColumn("_rn", F.row_number().over(dedup_window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# Drop records with null partition columns — Delta cannot write null partition paths
silver_deduped = silver_deduped.filter(
    F.col("region").isNotNull() &
    F.col("published_year").isNotNull() &
    F.col("published_month").isNotNull()
)

partition_safe_count = silver_deduped.count()
print(f"[SILVER] Records safe for partitioned write: {partition_safe_count}")

# --------------------------------------------------------------------------- #
# 5. Write Silver Delta Lake — overwrite mode (safe for first run)
# --------------------------------------------------------------------------- #

# Filter out any empty string partition values that cause path errors
silver_deduped = silver_deduped.filter(
    F.col("region").isNotNull() & (F.col("region") != "") &
    F.col("published_year").isNotNull() &
    F.col("published_month").isNotNull()
)

write_count = silver_deduped.count()
print(f"[SILVER] Records to write: {write_count}")

# Show sample of partition values for debugging
silver_deduped.select("region", "published_year", "published_month") \
    .distinct() \
    .show(truncate=False)

print(f"[SILVER] Writing to: {SILVER_PATH}")

silver_deduped.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("region", "published_year", "published_month") \
    .option("overwriteSchema", "true") \
    .save(SILVER_PATH)

print("[SILVER] Delta write complete.")


# --------------------------------------------------------------------------- #
# 6. Register in Glue Catalog via boto3 — avoids Glue 4.0 + Delta DDL bug
#    where spark.sql("CREATE DATABASE") sets empty LocationUri, causing
#    "Can not create a Path from an empty string" on the CREATE TABLE call.
# --------------------------------------------------------------------------- #
import boto3

glue_client = boto3.client("glue", region_name="us-east-1")

# Check if table already exists — if so, update the location; else create it
try:
    glue_client.get_table(DatabaseName=GLUE_DATABASE, Name=GLUE_TABLE)
    table_exists = True
    print(f"[SILVER] Glue table {GLUE_DATABASE}.{GLUE_TABLE} already exists — updating location.")
except glue_client.exceptions.EntityNotFoundException:
    table_exists = False
    print(f"[SILVER] Glue table {GLUE_DATABASE}.{GLUE_TABLE} not found — creating.")

# Build column list from the written DataFrame schema
def spark_type_to_glue(spark_type_str: str) -> str:
    mapping = {
        "StringType":    "string",
        "IntegerType":   "int",
        "LongType":      "bigint",
        "DoubleType":    "double",
        "TimestampType": "timestamp",
        "DateType":      "date",
        "BooleanType":   "boolean",
    }
    for k, v in mapping.items():
        if k in spark_type_str:
            return v
    return "string"  # safe default

# Partition columns — must be excluded from StorageDescriptor columns
PARTITION_KEYS = {"region", "published_year", "published_month"}

sd_columns = [
    {
        "Name":    field.name,
        "Type":    spark_type_to_glue(str(field.dataType)),
        "Comment": "",
    }
    for field in silver_deduped.schema.fields
    if field.name not in PARTITION_KEYS
]

partition_keys = [
    {"Name": "region",          "Type": "string"},
    {"Name": "published_year",  "Type": "int"},
    {"Name": "published_month", "Type": "int"},
]

table_input = {
    "Name": GLUE_TABLE,
    "StorageDescriptor": {
        "Columns":            sd_columns,
        "Location":           SILVER_PATH,
        "InputFormat":        "org.apache.hadoop.mapred.SequenceFileInputFormat",
        "OutputFormat":       "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat",
        "SerdeInfo": {
            "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
        },
        "Compressed": False,
    },
    "PartitionKeys": partition_keys,
    "TableType":     "EXTERNAL_TABLE",
    "Parameters": {
        "classification":        "delta",
        "spark.sql.sources.provider": "delta",
        "EXTERNAL":              "TRUE",
        "path":                  SILVER_PATH,
    },
}

if table_exists:
    glue_client.update_table(DatabaseName=GLUE_DATABASE, TableInput=table_input)
    print(f"[SILVER] Glue Catalog table updated: {GLUE_DATABASE}.{GLUE_TABLE}")
else:
    glue_client.create_table(DatabaseName=GLUE_DATABASE, TableInput=table_input)
    print(f"[SILVER] Glue Catalog table created: {GLUE_DATABASE}.{GLUE_TABLE}")
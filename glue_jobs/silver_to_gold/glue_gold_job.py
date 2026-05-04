import sys
import boto3
from datetime import date

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType

# --------------------------------------------------------------------------- #
# Job parameters
# --------------------------------------------------------------------------- #
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "silver_bucket",
    "silver_prefix",
    "gold_bucket",
    "gold_prefix",
    "glue_database",
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

SILVER_PATH   = f"s3://{args['silver_bucket']}/{args['silver_prefix'].rstrip('/')}"
GOLD_BASE     = f"s3://{args['gold_bucket']}/{args['gold_prefix'].rstrip('/')}"
GLUE_DATABASE = args["glue_database"]

# Gold table paths
PATHS = {
    "daily_trending_rank": f"{GOLD_BASE}/daily_trending_rank",
    "category_stats":      f"{GOLD_BASE}/category_stats",
    "channel_stats":       f"{GOLD_BASE}/channel_stats",
    "view_velocity":       f"{GOLD_BASE}/view_velocity",
}

print(f"[GOLD] Reading Silver from : {SILVER_PATH}")
print(f"[GOLD] Writing Gold to     : {GOLD_BASE}")
print(f"[GOLD] Glue database       : {GLUE_DATABASE}")

# --------------------------------------------------------------------------- #
# 1. Read Silver Delta table
# --------------------------------------------------------------------------- #
silver_df = spark.read.format("delta").load(SILVER_PATH)

record_count = silver_df.count()
print(f"[GOLD] Silver record count : {record_count}")

if record_count == 0:
    raise Exception("[GOLD] Silver table is empty — aborting Gold job.")

# Add snapshot_date (date portion of ingested_at — the daily partition key for Gold)
silver_df = silver_df.withColumn(
    "snapshot_date", F.to_date("ingested_at")
)

# --------------------------------------------------------------------------- #
# 2. Table 1 — Daily Trending Rank
#    Ranks every video by view_count within each (region, snapshot_date).
#    Rank 1 = most viewed trending video that day in that region.
# --------------------------------------------------------------------------- #
print("[GOLD] Computing daily_trending_rank ...")

rank_window = Window.partitionBy("region", "snapshot_date") \
                    .orderBy(F.col("view_count").desc())

daily_trending_rank = (
    silver_df
    .withColumn("trending_rank", F.row_number().over(rank_window))
    .select(
        "video_id",
        "title",
        "channel_id",
        "channel_title",
        "category_id",
        "region",
        "snapshot_date",
        "view_count",
        "like_count",
        "comment_count",
        "engagement_rate",
        "like_to_view_ratio",
        "duration_seconds",
        "tags_count",
        "published_date",
        "trending_rank",
        F.current_timestamp().alias("_gold_processed_at"),
    )
)

print(f"[GOLD] daily_trending_rank rows: {daily_trending_rank.count()}")

daily_trending_rank.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("region", "snapshot_date") \
    .save(PATHS["daily_trending_rank"])

print(f"[GOLD] daily_trending_rank written to {PATHS['daily_trending_rank']}")

# --------------------------------------------------------------------------- #
# 3. Table 2 — Category Stats
#    Aggregates per (category_id, region, snapshot_date):
#    video count, avg/total views, likes, comments, engagement, duration.
# --------------------------------------------------------------------------- #
print("[GOLD] Computing category_stats ...")

category_stats = (
    silver_df
    .groupBy("category_id", "region", "snapshot_date")
    .agg(
        F.count("video_id")             .alias("video_count"),
        F.sum("view_count")             .alias("total_views"),
        F.sum("like_count")             .alias("total_likes"),
        F.sum("comment_count")          .alias("total_comments"),
        F.avg("view_count")             .alias("avg_view_count"),
        F.avg("like_count")             .alias("avg_like_count"),
        F.avg("comment_count")          .alias("avg_comment_count"),
        F.avg("engagement_rate")        .alias("avg_engagement_rate"),
        F.avg("duration_seconds")       .alias("avg_duration_seconds"),
        F.max("view_count")             .alias("max_view_count"),
        F.min("view_count")             .alias("min_view_count"),
        # Top video in this category by views
        F.first("title", ignorenulls=True).alias("top_video_title"),
        F.first("video_id", ignorenulls=True).alias("top_video_id"),
    )
    .withColumn("_gold_processed_at", F.current_timestamp())
)

print(f"[GOLD] category_stats rows: {category_stats.count()}")

category_stats.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("region", "snapshot_date") \
    .save(PATHS["category_stats"])

print(f"[GOLD] category_stats written to {PATHS['category_stats']}")

# --------------------------------------------------------------------------- #
# 4. Table 3 — Channel Stats
#    Aggregates per (channel_id, region, snapshot_date):
#    how many videos a channel has trending, total reach, avg engagement.
# --------------------------------------------------------------------------- #
print("[GOLD] Computing channel_stats ...")

channel_stats = (
    silver_df
    .groupBy("channel_id", "channel_title", "region", "snapshot_date")
    .agg(
        F.count("video_id")         .alias("trending_video_count"),
        F.sum("view_count")         .alias("total_views"),
        F.sum("like_count")         .alias("total_likes"),
        F.sum("comment_count")      .alias("total_comments"),
        F.avg("view_count")         .alias("avg_view_count"),
        F.avg("engagement_rate")    .alias("avg_engagement_rate"),
        F.max("view_count")         .alias("max_video_views"),
        # Channel with multiple trending videos — list their titles
        F.collect_list("title")     .alias("trending_video_titles"),
    )
    .withColumn("_gold_processed_at", F.current_timestamp())
)

print(f"[GOLD] channel_stats rows: {channel_stats.count()}")

channel_stats.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("region", "snapshot_date") \
    .save(PATHS["channel_stats"])

print(f"[GOLD] channel_stats written to {PATHS['channel_stats']}")

# --------------------------------------------------------------------------- #
# 5. Table 4 — View Velocity
#    Day-over-day view count growth per (video_id, region).
#    Shows which videos are gaining momentum vs plateauing.
#    NOTE: velocity_pct and view_count_delta will be null on the first run
#    (no previous day to compare against) — this is expected behaviour.
# --------------------------------------------------------------------------- #
print("[GOLD] Computing view_velocity ...")

velocity_window = Window.partitionBy("video_id", "region") \
                        .orderBy("snapshot_date")

view_velocity = (
    silver_df
    .select(
        "video_id",
        "title",
        "channel_id",
        "channel_title",
        "category_id",
        "region",
        "snapshot_date",
        "view_count",
        "like_count",
        "comment_count",
        "engagement_rate",
    )
    .withColumn("prev_view_count",
        F.lag("view_count").over(velocity_window)
    )
    .withColumn("prev_snapshot_date",
        F.lag("snapshot_date").over(velocity_window)
    )
    .withColumn("view_count_delta",
        F.when(F.col("prev_view_count").isNotNull(),
            F.col("view_count") - F.col("prev_view_count")
        ).otherwise(F.lit(None).cast("long"))
    )
    .withColumn("velocity_pct",
        F.when(
            F.col("prev_view_count").isNotNull() & (F.col("prev_view_count") > 0),
            (F.col("view_count") - F.col("prev_view_count")).cast(DoubleType())
            / F.col("prev_view_count").cast(DoubleType()) * 100
        ).otherwise(F.lit(None).cast(DoubleType()))
    )
    .withColumn("days_since_prev_snapshot",
        F.when(F.col("prev_snapshot_date").isNotNull(),
            F.datediff(F.col("snapshot_date"), F.col("prev_snapshot_date"))
        ).otherwise(F.lit(None).cast(IntegerType()))
    )
    .withColumn("_gold_processed_at", F.current_timestamp())
    .drop("prev_view_count", "prev_snapshot_date")
)

print(f"[GOLD] view_velocity rows: {view_velocity.count()}")

view_velocity.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("region", "snapshot_date") \
    .save(PATHS["view_velocity"])

print(f"[GOLD] view_velocity written to {PATHS['view_velocity']}")

# --------------------------------------------------------------------------- #
# 6. Register all 4 Gold tables in Glue Catalog via boto3
#    (same approach as Silver — avoids the empty LocationUri DDL bug)
# --------------------------------------------------------------------------- #
print("[GOLD] Registering tables in Glue Catalog ...")

glue_client = boto3.client("glue", region_name="us-east-1")

# Partition keys are the same for all 4 Gold tables
GOLD_PARTITION_KEYS = [
    {"Name": "region",         "Type": "string"},
    {"Name": "snapshot_date",  "Type": "date"},
]

GOLD_TABLES = {
    "gold_daily_trending_rank": (daily_trending_rank, PATHS["daily_trending_rank"]),
    "gold_category_stats":      (category_stats,      PATHS["category_stats"]),
    "gold_channel_stats":       (channel_stats,       PATHS["channel_stats"]),
    "gold_view_velocity":       (view_velocity,       PATHS["view_velocity"]),
}

PARTITION_KEY_NAMES = {"region", "snapshot_date"}

def spark_type_to_glue(spark_type_str: str) -> str:
    mapping = {
        "StringType":    "string",
        "IntegerType":   "int",
        "LongType":      "bigint",
        "DoubleType":    "double",
        "TimestampType": "timestamp",
        "DateType":      "date",
        "BooleanType":   "boolean",
        "ArrayType":     "array<string>",
    }
    for k, v in mapping.items():
        if k in spark_type_str:
            return v
    return "string"

for table_name, (df, path) in GOLD_TABLES.items():
    sd_columns = [
        {"Name": f.name, "Type": spark_type_to_glue(str(f.dataType)), "Comment": ""}
        for f in df.schema.fields
        if f.name not in PARTITION_KEY_NAMES
    ]

    table_input = {
        "Name": table_name,
        "StorageDescriptor": {
            "Columns":     sd_columns,
            "Location":    path,
            "InputFormat":  "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            },
            "Compressed": False,
        },
        "PartitionKeys": GOLD_PARTITION_KEYS,
        "TableType":     "EXTERNAL_TABLE",
        "Parameters": {
            "classification":             "delta",
            "spark.sql.sources.provider": "delta",
            "EXTERNAL":                   "TRUE",
            "parquet.compression":        "SNAPPY",
            "path":                       path,
            "table_type":                 "DELTA",
        },
    }

    try:
        existing = glue_client.get_table(DatabaseName=GLUE_DATABASE, Name=table_name)
        # Preserve existing columns if they exist — only update parameters and location
        existing_cols = existing["Table"]["StorageDescriptor"]["Columns"]
        if existing_cols:
            table_input["StorageDescriptor"]["Columns"] = existing_cols
        glue_client.update_table(DatabaseName=GLUE_DATABASE, TableInput=table_input)
        print(f"[GOLD] Updated  : {GLUE_DATABASE}.{table_name}")
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_table(DatabaseName=GLUE_DATABASE, TableInput=table_input)
        print(f"[GOLD] Created  : {GLUE_DATABASE}.{table_name}")

# --------------------------------------------------------------------------- #
# 7. Final summary
# --------------------------------------------------------------------------- #
print("\n[GOLD] ── Final row counts ──────────────────────────")
for table_name, (_, path) in GOLD_TABLES.items():
    count = spark.read.format("delta").load(path).count()
    print(f"[GOLD]   {table_name:<30} : {count} rows")

print("[GOLD] Job completed successfully.")
job.commit()
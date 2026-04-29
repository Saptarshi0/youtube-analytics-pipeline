from __future__ import annotations
 
import os
from datetime import datetime, timedelta
 
import boto3
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
 
# ── Config ────────────────────────────────────────────────────────────────────
BRONZE_BUCKET = os.environ["YOUTUBE_BRONZE_BUCKET"]          # youtube-raw-dev-900932787422
GLUE_DB       = os.environ.get("YOUTUBE_GLUE_DB", "youtube_analytics_dev")
DBT_PROJECT   = os.environ.get("YOUTUBE_DBT_PROJECT_DIR", "/opt/dbt/youtube_analytics")
DBT_PROFILES  = os.environ.get("YOUTUBE_DBT_PROFILES_DIR", "/home/airflow/.dbt")
AWS_REGION    = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
AWS_ACCOUNT   = "900932787422"
 
# Glue job names — matched to actual AWS Glue jobs
GLUE_BRONZE_VALIDATION_JOB = "youtube-ge-bronze-validation-dev"
GLUE_SILVER_JOB            = "youtube-silver-transform-dev"
GLUE_GOLD_JOB              = "youtube-gold-transform-dev"
 
# dbt mart tables to row-count validate after run
DBT_MART_TABLES = [
    "dbt_gold.fct_trending_leaderboard",
    "dbt_gold.fct_top_channels",
    "dbt_gold.fct_category_performance",
    "dbt_gold.fct_momentum_videos",
]
 
# ── Default args ──────────────────────────────────────────────────────────────
default_args = {
    "owner": "boby",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
 
# ── Validation helper ─────────────────────────────────────────────────────────
def validate_mart_tables(**context) -> None:
    """
    Row-count sanity check on all dbt mart tables via Athena.
    Raises if any table returns 0 rows — catches empty-run failures
    that dbt tests might miss.
    """
    import time
 
    athena = boto3.client("athena", region_name=AWS_REGION)
    s3_staging = f"s3://youtube-glue-assets-dev-{AWS_ACCOUNT}/dbt-results/"
 
    failed = []
 
    for table in DBT_MART_TABLES:
        query = f"SELECT COUNT(*) AS cnt FROM {table}"
        resp = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": "dbt_gold"},
            ResultConfiguration={"OutputLocation": s3_staging},
        )
        qid = resp["QueryExecutionId"]
 
        # Poll until complete (max 2 min per table)
        for _ in range(24):
            status = athena.get_query_execution(QueryExecutionId=qid)
            state = status["QueryExecution"]["Status"]["State"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(5)
 
        if state != "SUCCEEDED":
            failed.append(f"{table}: query state={state}")
            continue
 
        result = athena.get_query_results(QueryExecutionId=qid)
        count = int(result["ResultSet"]["Rows"][1]["Data"][0]["VarCharValue"])
 
        if count == 0:
            failed.append(f"{table}: 0 rows — pipeline may have produced no output")
        else:
            print(f"✓ {table}: {count:,} rows")
 
    if failed:
        raise ValueError(
            f"Validation failed for {len(failed)} table(s):\n"
            + "\n".join(f"  • {f}" for f in failed)
        )
 
    print("✓ All mart tables passed row-count validation")
 
 
# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="youtube_analytics_pipeline",
    description="YouTube Analytics Medallion pipeline: Bronze → Silver → Gold → dbt mart",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="15 2 * * *",   # 02:15 UTC daily
    catchup=False,
    max_active_runs=1,
    tags=["youtube", "medallion", "portfolio"],
) as dag:
 
    # ── 1. S3 Sensor — wait for today's Bronze data ───────────────────────────
    # Firehose writes to: youtube-raw-dev-.../YYYY/MM/DD/HH/
    # We watch for any file under today's date prefix.
    # poke_interval=120s, timeout=3600s (1 hour) — gives Lambda time to land data.
    wait_for_bronze = S3KeySensor(
        task_id="wait_for_bronze_data",
        bucket_name=BRONZE_BUCKET,
        # Wildcard pattern — matches any file under today's partition
        bucket_key="trending/{{ execution_date.strftime('%Y/%m/%d') }}/*/*/*",
        wildcard_match=True,
        aws_conn_id="aws_default",
        poke_interval=120,      # check every 2 minutes
        timeout=3600,           # give up after 1 hour
        soft_fail=False,        # hard fail if no data arrives
        mode="reschedule",      # release the worker slot while waiting
    )
 
    # ── 2. Glue Bronze validation (Great Expectations) ───────────────────────
    run_bronze_validation = GlueJobOperator(
        task_id="run_bronze_validation",
        job_name=GLUE_BRONZE_VALIDATION_JOB,
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
        wait_for_completion=True,
        script_args={
            "--execution_date": "{{ ds }}",
            "--source_bucket": BRONZE_BUCKET,
        },
    )
 
    # ── 3. Glue Silver job ────────────────────────────────────────────────────
    run_silver = GlueJobOperator(
        task_id="run_silver_transform",
        job_name=GLUE_SILVER_JOB,
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
        wait_for_completion=True,
        script_args={
            "--execution_date": "{{ ds }}",
            "--source_bucket": BRONZE_BUCKET,
        },
    )
 
    # ── 4. Glue Gold job — builds all 4 Gold tables ───────────────────────────
    run_gold = GlueJobOperator(
        task_id="run_gold_transform",
        job_name=GLUE_GOLD_JOB,
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
        wait_for_completion=True,
        script_args={"--execution_date": "{{ ds }}"},
    )
 
    # ── 5. dbt run ────────────────────────────────────────────────────────────
    run_dbt = BashOperator(
        task_id="run_dbt_models",
        bash_command=(
            f"cd {DBT_PROJECT} && "
            f"dbt run --profiles-dir {DBT_PROFILES} --target dev 2>&1"
        ),
    )
 
    # ── 6. dbt test ───────────────────────────────────────────────────────────
    test_dbt = BashOperator(
        task_id="test_dbt_models",
        bash_command=(
            f"cd {DBT_PROJECT} && "
            f"dbt test --profiles-dir {DBT_PROFILES} --target dev 2>&1"
        ),
    )
 
    # ── 7. Validation ─────────────────────────────────────────────────────────
    validate = PythonOperator(
        task_id="validate_mart_tables",
        python_callable=validate_mart_tables,
    )
 
    # ── Dependencies ──────────────────────────────────────────────────────────
    #
    #  wait_for_bronze
    #       │
    #  run_bronze_validation   (GE checks on raw data)
    #       │
    #  run_silver_transform    (Bronze → Silver Delta Lake)
    #       │
    #  run_gold_transform      (Silver → Gold Delta Lake, all 4 tables)
    #       │
    #  run_dbt                 (Gold → Iceberg mart tables)
    #       │
    #  test_dbt
    #       │
    #  validate                (Athena row-count sanity check)
    #
    (
        wait_for_bronze
        >> run_bronze_validation
        >> run_silver
        >> run_gold
        >> run_dbt
        >> test_dbt
        >> validate
    )
 
import os
import pandas as pd
import streamlit as st
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor
from dotenv import load_dotenv

load_dotenv()

AWS_REGION       = os.getenv("AWS_REGION", "us-east-1")
S3_STAGING_DIR   = os.getenv("ATHENA_S3_STAGING_DIR")
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "primary")


def _get_connection():
    return connect(
        s3_staging_dir=S3_STAGING_DIR,
        region_name=AWS_REGION,
        work_group=ATHENA_WORKGROUP,
        cursor_class=PandasCursor,
    )


@st.cache_data(ttl=300)
def run_query(sql: str) -> pd.DataFrame:
    conn   = _get_connection()
    cursor = conn.cursor()
    return cursor.execute(sql).as_pandas()


@st.cache_data(ttl=3600)
def get_available_dates() -> list:
    sql = """
        SELECT DISTINCT cast(snapshot_date AS varchar) AS snapshot_date
        FROM   dbt_gold.fct_trending_leaderboard
        ORDER  BY snapshot_date DESC
    """
    df = run_query(sql)
    return df["snapshot_date"].tolist() if not df.empty else []


@st.cache_data(ttl=3600)
def get_available_regions() -> list:
    sql = """
        SELECT DISTINCT region
        FROM   dbt_gold.fct_trending_leaderboard
        ORDER  BY region
    """
    df = run_query(sql)
    return df["region"].tolist() if not df.empty else ["US", "GB", "IN", "CA"]

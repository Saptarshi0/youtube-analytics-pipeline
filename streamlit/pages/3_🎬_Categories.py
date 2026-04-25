import streamlit as st
from utils.theme import apply_theme, page_header, section
from utils.athena import run_query
from components.kpi_cards import kpi_row, fmt, pct
from components.charts import bar_category_views, bar_category_engagement, treemap_category

st.set_page_config(page_title="Categories · YouTube Analytics", layout="wide")
apply_theme()

snapshot_date    = st.session_state.get("selected_date", "")
selected_regions = st.session_state.get("selected_regions", ["US","GB","IN","CA"])

page_header("🎬", "Category Performance", "Which content types dominate trending?")

if not snapshot_date:
    st.warning("Go to the Home page to select a snapshot date.")
    st.stop()

region_list = ", ".join(f"'{r}'" for r in selected_regions)

sql = f"""
    SELECT category_id, category_name, region, category_rank_in_region,
           video_count, total_views, total_likes, total_comments,
           avg_view_count, avg_engagement_rate_pct, avg_duration_minutes,
           max_view_count, top_video_title, region_view_share_pct
    FROM dbt_gold.fct_category_performance
    WHERE snapshot_date = date('{snapshot_date}')
      AND region IN ({region_list})
    ORDER BY region, category_rank_in_region
"""
with st.spinner(""):
    df = run_query(sql)

if df.empty:
    st.info("No category data for the selected filters.")
    st.stop()

top_views = df.groupby("category_name")["total_views"].sum().idxmax()
top_eng   = df.groupby("category_name")["avg_engagement_rate_pct"].mean().idxmax()

kpi_row([
    {"label": "Active Categories",    "value": str(df["category_name"].nunique())},
    {"label": "#1 by Views",          "value": top_views},
    {"label": "#1 by Engagement",     "value": top_eng},
    {"label": "Total Trending Views", "value": fmt(df["total_views"].sum())},
])

st.markdown("<br>", unsafe_allow_html=True)
section("VIEW DISTRIBUTION")

c1, c2 = st.columns(2)
with c1:
    st.plotly_chart(bar_category_views(df), use_container_width=True)
with c2:
    st.plotly_chart(bar_category_engagement(df), use_container_width=True)

st.plotly_chart(treemap_category(df), use_container_width=True)

section("CATEGORY DETAIL")
region_filter = st.selectbox("Filter region", ["All"] + selected_regions, label_visibility="collapsed")
detail = df if region_filter == "All" else df[df["region"] == region_filter]

show = detail[["category_name","region","category_rank_in_region","video_count",
               "total_views","avg_engagement_rate_pct","avg_duration_minutes","top_video_title"]].copy()
show.columns = ["Category","Region","Rank","Videos","Total Views","Avg Engagement %","Avg Duration (min)","Top Video"]
show["Total Views"]      = show["Total Views"].apply(fmt)
show["Avg Engagement %"] = show["Avg Engagement %"].apply(lambda v: f"{v:.3f}%")
st.dataframe(show, use_container_width=True, hide_index=True)

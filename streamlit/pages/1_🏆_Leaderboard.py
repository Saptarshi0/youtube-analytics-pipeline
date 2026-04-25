import streamlit as st
from utils.theme import apply_theme, page_header, section
from utils.athena import run_query
from components.kpi_cards import kpi_row, fmt, pct
from components.charts import bar_top_videos, scatter_engagement

st.set_page_config(page_title="Leaderboard · YouTube Analytics", layout="wide")
apply_theme()

snapshot_date    = st.session_state.get("selected_date", "")
selected_regions = st.session_state.get("selected_regions", ["US","GB","IN","CA"])

page_header("🏆", "Leaderboard", "Top 10 trending videos per region")

if not snapshot_date:
    st.warning("Go to the Home page to select a snapshot date.")
    st.stop()

tabs = st.tabs([f"  {r}  " for r in selected_regions])

for tab, region in zip(tabs, selected_regions):
    with tab:
        sql = f"""
            SELECT video_id, title, channel_title, category_name,
                   trending_rank, view_count, like_count, comment_count,
                   engagement_rate, like_to_view_ratio,
                   duration_seconds, tags_count, published_date, days_to_trend,
                   engagement_quintile
            FROM dbt_gold.fct_trending_leaderboard
            WHERE snapshot_date = date('{snapshot_date}')
              AND region = '{region}'
            ORDER BY trending_rank
        """
        with st.spinner(""):
            df = run_query(sql)

        if df.empty:
            st.info(f"No data for {region} on {snapshot_date}.")
            continue

        kpi_row([
            {"label": "Total Views",        "value": fmt(df["view_count"].sum()),
             "help": "Combined views of top-10 videos"},
            {"label": "Avg Engagement",     "value": pct(df["engagement_rate"].mean() * 100, 3),
             "help": "(likes + comments) / views"},
            {"label": "Fastest to Trend",   "value": f"{int(df['days_to_trend'].min())}d",
             "help": "Min days from publish to trending"},
            {"label": "Categories",         "value": str(df["category_name"].nunique()),
             "help": "Distinct content categories in top 10"},
        ])

        st.markdown("<br>", unsafe_allow_html=True)
        section("PERFORMANCE BREAKDOWN")

        c1, c2 = st.columns(2)
        with c1:
            st.plotly_chart(bar_top_videos(df, region), use_container_width=True)
        with c2:
            st.plotly_chart(scatter_engagement(df), use_container_width=True)

        section("FULL RANKINGS")
        display = df[[
            "trending_rank","title","channel_title","category_name",
            "view_count","like_count","comment_count","engagement_rate","days_to_trend",
        ]].copy()
        display.columns = ["#","Title","Channel","Category","Views","Likes","Comments","Engagement","Days to Trend"]
        display["Views"]      = display["Views"].apply(fmt)
        display["Likes"]      = display["Likes"].apply(fmt)
        display["Comments"]   = display["Comments"].apply(fmt)
        display["Engagement"] = display["Engagement"].apply(lambda v: pct(v*100, 3))
        st.dataframe(display, use_container_width=True, hide_index=True)

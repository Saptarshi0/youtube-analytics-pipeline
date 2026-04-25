import streamlit as st
from utils.theme import apply_theme, page_header, section
from utils.athena import run_query
from components.kpi_cards import kpi_row, fmt, pct
from components.charts import bar_top_channels, scatter_channel_dominance

st.set_page_config(page_title="Channels · YouTube Analytics", layout="wide")
apply_theme()

snapshot_date    = st.session_state.get("selected_date", "")
selected_regions = st.session_state.get("selected_regions", ["US","GB","IN","CA"])

page_header("📡", "Channel Dominance", "Which channels own the trending page?")

if not snapshot_date:
    st.warning("Go to the Home page to select a snapshot date.")
    st.stop()

region_list = ", ".join(f"'{r}'" for r in selected_regions)

sql = f"""
    SELECT channel_id, channel_title, region, channel_rank,
           trending_video_count, total_views, total_likes, total_comments,
           avg_view_count, avg_engagement_rate_pct, max_video_views, view_share_pct
    FROM dbt_gold.fct_top_channels
    WHERE snapshot_date = date('{snapshot_date}')
      AND region IN ({region_list})
    ORDER BY region, channel_rank
"""
with st.spinner(""):
    df = run_query(sql)

if df.empty:
    st.info("No channel data for the selected filters.")
    st.stop()

top = df.sort_values("total_views", ascending=False).iloc[0]

kpi_row([
    {"label": "Channels on Trending",  "value": str(df["channel_id"].nunique())},
    {"label": "Top Channel",           "value": top["channel_title"],
     "help": fmt(top["total_views"]) + " total views"},
    {"label": "Max View Share",        "value": pct(df["view_share_pct"].max())},
    {"label": "Avg Videos / Channel",  "value": f"{df['trending_video_count'].mean():.1f}"},
])

st.markdown("<br>", unsafe_allow_html=True)
section("REACH & DOMINANCE")

c1, c2 = st.columns(2)
with c1:
    st.plotly_chart(bar_top_channels(df), use_container_width=True)
with c2:
    st.plotly_chart(scatter_channel_dominance(df), use_container_width=True)

section("REGIONAL RANKINGS")
tabs = st.tabs([f"  {r}  " for r in selected_regions])
for tab, region in zip(tabs, selected_regions):
    with tab:
        rdf = df[df["region"] == region].copy()
        if rdf.empty:
            st.info(f"No data for {region}.")
            continue
        show = rdf[["channel_rank","channel_title","trending_video_count",
                    "total_views","avg_engagement_rate_pct","view_share_pct"]].copy()
        show.columns = ["Rank","Channel","Trending Videos","Total Views","Avg Engagement %","View Share %"]
        show["Total Views"]       = show["Total Views"].apply(fmt)
        show["Avg Engagement %"]  = show["Avg Engagement %"].apply(lambda v: f"{v:.3f}%")
        show["View Share %"]      = show["View Share %"].apply(lambda v: f"{v:.2f}%")
        st.dataframe(show.head(20), use_container_width=True, hide_index=True)

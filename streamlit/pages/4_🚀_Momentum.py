import streamlit as st
from utils.theme import apply_theme, page_header, section
from utils.athena import run_query
from components.kpi_cards import kpi_row, fmt, pct
from components.charts import bar_momentum, donut_momentum

st.set_page_config(page_title="Momentum · YouTube Analytics", layout="wide")
apply_theme()

snapshot_date    = st.session_state.get("selected_date", "")
selected_regions = st.session_state.get("selected_regions", ["US","GB","IN","CA"])

page_header("🚀", "Video Momentum", "Day-over-day view velocity — what's rising fast?")

if not snapshot_date:
    st.warning("Go to the Home page to select a snapshot date.")
    st.stop()

region_list = ", ".join(f"'{r}'" for r in selected_regions)

sql = f"""
    SELECT video_id, title, channel_title, category_id, region, snapshot_date,
           velocity_rank, view_count, view_count_delta, velocity_pct,
           days_since_prev_snapshot, momentum_label, like_count, comment_count,
           engagement_rate_pct
    FROM dbt_gold.fct_momentum_videos
    WHERE snapshot_date = date('{snapshot_date}')
      AND region IN ({region_list})
    ORDER BY region, velocity_rank
"""
with st.spinner(""):
    df = run_query(sql)

if df.empty:
    st.info("No momentum data yet — requires at least 2 daily snapshots.", icon="ℹ️")
    st.stop()

accel = (df["momentum_label"] == "accelerating").sum()

kpi_row([
    {"label": "Accelerating",    "value": str(accel),
     "help": "Videos with >20% day-over-day growth"},
    {"label": "Avg Velocity",    "value": pct(df["velocity_pct"].mean(), 1)},
    {"label": "Peak Velocity",   "value": pct(df["velocity_pct"].max(), 1)},
    {"label": "Total New Views", "value": fmt(df["view_count_delta"].sum())},
])

st.markdown("<br>", unsafe_allow_html=True)
section("VELOCITY CHARTS")

c1, c2 = st.columns([2, 1])
with c1:
    st.plotly_chart(bar_momentum(df), use_container_width=True)
with c2:
    st.plotly_chart(donut_momentum(df), use_container_width=True)

section("REGIONAL BREAKDOWN")
BADGES = {"accelerating":"🟢","growing":"🔵","flat":"⚪","declining":"🔴"}

tabs = st.tabs([f"  {r}  " for r in selected_regions])
for tab, region in zip(tabs, selected_regions):
    with tab:
        rdf = df[df["region"] == region].copy()
        if rdf.empty:
            st.info(f"No momentum data for {region}.")
            continue
        rdf["momentum_label"] = rdf["momentum_label"].apply(
            lambda l: f"{BADGES.get(l,'⚪')} {l}"
        )
        show = rdf[["velocity_rank","title","channel_title","velocity_pct",
                    "view_count_delta","view_count","momentum_label","engagement_rate_pct"]].copy()
        show.columns = ["Rank","Title","Channel","Velocity %","New Views","Total Views","Momentum","Engagement %"]
        show["Velocity %"]  = show["Velocity %"].apply(lambda v: f"+{v:.1f}%")
        show["New Views"]   = show["New Views"].apply(fmt)
        show["Total Views"] = show["Total Views"].apply(fmt)
        st.dataframe(show, use_container_width=True, hide_index=True)

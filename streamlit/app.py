import streamlit as st
from utils.theme import apply_theme, page_header, REGION_COLOURS
from utils.athena import get_available_dates, get_available_regions

st.set_page_config(
    page_title="YouTube Analytics",
    page_icon="▶️",
    layout="wide",
    initial_sidebar_state="expanded",
)

apply_theme()

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="padding: 0.5rem 0 1.5rem; border-bottom: 1px solid #1f2d45; margin-bottom: 1.5rem;">
        <div style="font-size:1.3rem; font-weight:600; letter-spacing:-0.02em;">▶️ YouTube</div>
        <div style="font-size:0.7rem; text-transform:uppercase; letter-spacing:0.1em; color:#64748b; margin-top:2px;">Analytics Pipeline</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div style="font-size:0.7rem;text-transform:uppercase;letter-spacing:0.1em;color:#64748b;margin-bottom:0.5rem">Snapshot Date</div>', unsafe_allow_html=True)
    dates = get_available_dates()
    selected_date = st.selectbox("", options=dates, index=0, label_visibility="collapsed")

    st.markdown('<div style="font-size:0.7rem;text-transform:uppercase;letter-spacing:0.1em;color:#64748b;margin:1rem 0 0.5rem">Regions</div>', unsafe_allow_html=True)
    regions = get_available_regions()
    selected_regions = st.multiselect("", options=regions, default=regions, label_visibility="collapsed")

    st.session_state["selected_date"]    = selected_date
    st.session_state["selected_regions"] = selected_regions

    st.markdown("""
    <div style="position:fixed;bottom:1.5rem;left:0;width:18rem;padding:0 1.5rem">
        <div style="font-size:0.68rem;color:#334155;font-family:'DM Mono',monospace;line-height:1.6">
            Lambda → Firehose → S3<br>
            Glue Silver → Glue Gold<br>
            dbt Iceberg → Athena
        </div>
    </div>
    """, unsafe_allow_html=True)

# ── Home page ─────────────────────────────────────────────────────────────────
page_header("▶️", "YouTube Analytics", f"Trending data · {selected_date} · {', '.join(selected_regions) if selected_regions else 'No regions'}")

# Pipeline status cards
st.markdown('<div style="font-size:0.7rem;text-transform:uppercase;letter-spacing:0.1em;color:#64748b;margin-bottom:0.75rem">Pipeline Layers</div>', unsafe_allow_html=True)

cols = st.columns(5)
layers = [
    ("Bronze",    "S3 + Firehose",      "#f59e0b"),
    ("Silver",    "Glue + Delta Lake",   "#3b82f6"),
    ("Gold",      "Glue Aggregations",   "#f59e0b"),
    ("dbt",       "Iceberg Mart Tables", "#22c55e"),
    ("Dashboard", "Streamlit + Athena",  "#ff4b4b"),
]
for col, (name, desc, colour) in zip(cols, layers):
    with col:
        st.markdown(f"""
        <div style="background:#111827;border:1px solid #1f2d45;border-top:3px solid {colour};
                    border-radius:10px;padding:1rem;text-align:center">
            <div style="font-size:0.95rem;font-weight:600;color:#e8edf5">{name}</div>
            <div style="font-size:0.72rem;color:#64748b;margin-top:4px">{desc}</div>
        </div>
        """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# Navigation cards
st.markdown('<div style="font-size:0.7rem;text-transform:uppercase;letter-spacing:0.1em;color:#64748b;margin-bottom:0.75rem">Dashboard Pages</div>', unsafe_allow_html=True)

nav_cols = st.columns(4)
pages = [
    ("🏆", "Leaderboard",  "Top 10 trending videos per region with engagement metrics"),
    ("📡", "Channels",     "Channel dominance, view share, and reach analysis"),
    ("🎬", "Categories",   "Content category performance across all regions"),
    ("🚀", "Momentum",     "Fastest rising videos by day-over-day view velocity"),
]
for col, (icon, name, desc) in zip(nav_cols, pages):
    with col:
        st.markdown(f"""
        <div style="background:#111827;border:1px solid #1f2d45;border-radius:10px;
                    padding:1.25rem;height:110px;cursor:pointer;transition:border-color 0.2s"
             onmouseover="this.style.borderColor='#ff4b4b'"
             onmouseout="this.style.borderColor='#1f2d45'">
            <div style="font-size:1.4rem">{icon}</div>
            <div style="font-size:0.92rem;font-weight:600;color:#e8edf5;margin-top:6px">{name}</div>
            <div style="font-size:0.75rem;color:#64748b;margin-top:4px;line-height:1.4">{desc}</div>
        </div>
        """, unsafe_allow_html=True)

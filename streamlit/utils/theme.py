"""
utils/theme.py
──────────────
Injects a premium dark theme into every Streamlit page.
Call apply_theme() at the top of every page file.
"""

import streamlit as st

THEME_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600&family=DM+Mono:wght@400;500&display=swap');

/* ── Root variables ─────────────────────────────── */
:root {
    --bg:        #0a0e1a;
    --surface:   #111827;
    --surface2:  #1a2236;
    --border:    #1f2d45;
    --accent:    #ff4b4b;
    --accent2:   #ff7e7e;
    --text:      #e8edf5;
    --muted:     #64748b;
    --green:     #22c55e;
    --blue:      #3b82f6;
    --gold:      #f59e0b;
    --radius:    12px;
    --font:      'DM Sans', sans-serif;
    --mono:      'DM Mono', monospace;
}

/* ── Global reset ───────────────────────────────── */
html, body, [class*="css"] {
    font-family: var(--font) !important;
    background-color: var(--bg) !important;
    color: var(--text) !important;
}

/* ── Hide Streamlit chrome ──────────────────────── */
#MainMenu, footer, header { visibility: hidden; }
.stDeployButton { display: none; }

/* ── Main content area ──────────────────────────── */
.main .block-container {
    padding: 2rem 2.5rem 4rem !important;
    max-width: 1400px !important;
}

/* ── Sidebar ────────────────────────────────────── */
[data-testid="stSidebar"] {
    background-color: var(--surface) !important;
    border-right: 1px solid var(--border) !important;
}
[data-testid="stSidebar"] * { color: var(--text) !important; }
[data-testid="stSidebar"] .stSelectbox > div > div,
[data-testid="stSidebar"] .stMultiSelect > div > div {
    background-color: var(--surface2) !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
    color: var(--text) !important;
}

/* ── Page title ─────────────────────────────────── */
h1 {
    font-size: 1.8rem !important;
    font-weight: 600 !important;
    letter-spacing: -0.02em !important;
    color: var(--text) !important;
    margin-bottom: 0.25rem !important;
}
h2 {
    font-size: 1.1rem !important;
    font-weight: 500 !important;
    color: var(--muted) !important;
    margin-top: 2rem !important;
    margin-bottom: 1rem !important;
}
h3 {
    font-size: 0.95rem !important;
    font-weight: 500 !important;
    color: var(--muted) !important;
    text-transform: uppercase !important;
    letter-spacing: 0.06em !important;
}

/* ── KPI cards ──────────────────────────────────── */
[data-testid="metric-container"] {
    background: var(--surface) !important;
    border: 1px solid var(--border) !important;
    border-radius: var(--radius) !important;
    padding: 1.2rem 1.4rem !important;
    transition: border-color 0.2s ease !important;
}
[data-testid="metric-container"]:hover {
    border-color: var(--accent) !important;
}
[data-testid="stMetricLabel"] {
    font-size: 0.72rem !important;
    font-weight: 500 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.07em !important;
    color: var(--muted) !important;
}
[data-testid="stMetricValue"] {
    font-size: 1.7rem !important;
    font-weight: 600 !important;
    color: var(--text) !important;
    letter-spacing: -0.02em !important;
}
[data-testid="stMetricDelta"] {
    font-size: 0.8rem !important;
    font-family: var(--mono) !important;
}

/* ── Tabs ───────────────────────────────────────── */
[data-testid="stTabs"] {
    border-bottom: 1px solid var(--border) !important;
    margin-bottom: 1.5rem !important;
}
button[data-baseweb="tab"] {
    background: transparent !important;
    color: var(--muted) !important;
    font-size: 0.82rem !important;
    font-weight: 500 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.06em !important;
    padding: 0.6rem 1rem !important;
    border-bottom: 2px solid transparent !important;
    transition: all 0.2s !important;
}
button[data-baseweb="tab"]:hover { color: var(--text) !important; }
button[data-baseweb="tab"][aria-selected="true"] {
    color: var(--accent) !important;
    border-bottom: 2px solid var(--accent) !important;
}
[data-testid="stTabPanel"] { padding-top: 1rem !important; }

/* ── Dataframe ──────────────────────────────────── */
[data-testid="stDataFrame"] {
    border: 1px solid var(--border) !important;
    border-radius: var(--radius) !important;
    overflow: hidden !important;
}
.dvn-scroller { background: var(--surface) !important; }

/* ── Expander ───────────────────────────────────── */
[data-testid="stExpander"] {
    background: var(--surface) !important;
    border: 1px solid var(--border) !important;
    border-radius: var(--radius) !important;
}

/* ── Selectbox / Multiselect ────────────────────── */
[data-testid="stSelectbox"] > div > div,
[data-testid="stMultiSelect"] > div > div {
    background: var(--surface2) !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
}

/* ── Info / Warning / Error boxes ──────────────── */
[data-testid="stAlert"] {
    background: var(--surface2) !important;
    border: 1px solid var(--border) !important;
    border-radius: var(--radius) !important;
}

/* ── Spinner ────────────────────────────────────── */
[data-testid="stSpinner"] { color: var(--accent) !important; }

/* ── Divider ────────────────────────────────────── */
hr {
    border-color: var(--border) !important;
    margin: 1.5rem 0 !important;
}

/* ── Caption / small text ───────────────────────── */
[data-testid="stCaption"], .stCaption {
    color: var(--muted) !important;
    font-size: 0.75rem !important;
    font-family: var(--mono) !important;
}

/* ── Navigation sidebar links ───────────────────── */
[data-testid="stSidebarNav"] a {
    border-radius: 8px !important;
    padding: 0.4rem 0.75rem !important;
    font-size: 0.85rem !important;
    font-weight: 500 !important;
    color: var(--muted) !important;
    transition: all 0.15s !important;
}
[data-testid="stSidebarNav"] a:hover,
[data-testid="stSidebarNav"] a[aria-current="page"] {
    background: var(--surface2) !important;
    color: var(--text) !important;
}

/* ── Page header badge ──────────────────────────── */
.page-badge {
    display: inline-block;
    background: var(--surface2);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 0.2rem 0.65rem;
    font-size: 0.72rem;
    font-family: var(--mono);
    color: var(--muted);
    margin-bottom: 1.2rem;
    letter-spacing: 0.05em;
}

/* ── Section label ──────────────────────────────── */
.section-label {
    font-size: 0.7rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: var(--muted);
    margin-bottom: 0.75rem;
    padding-bottom: 0.5rem;
    border-bottom: 1px solid var(--border);
}

/* ── Accent line under page title ───────────────── */
.title-accent {
    width: 2.5rem;
    height: 3px;
    background: var(--accent);
    border-radius: 2px;
    margin-bottom: 1.5rem;
}
</style>
"""

# Plotly dark theme config — use this in every fig.update_layout()
PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="#111827",
    font=dict(family="DM Sans, sans-serif", color="#e8edf5", size=12),
    xaxis=dict(
        gridcolor="#1f2d45", linecolor="#1f2d45",
        tickfont=dict(color="#64748b", size=11),
        title_font=dict(color="#64748b"),
    ),
    yaxis=dict(
        gridcolor="#1f2d45", linecolor="#1f2d45",
        tickfont=dict(color="#64748b", size=11),
        title_font=dict(color="#64748b"),
    ),
    legend=dict(
        bgcolor="rgba(0,0,0,0)",
        font=dict(color="#94a3b8", size=11),
    ),
    margin=dict(l=16, r=16, t=40, b=16),
    title_font=dict(size=13, color="#e8edf5"),
    hoverlabel=dict(
        bgcolor="#1a2236",
        bordercolor="#1f2d45",
        font=dict(family="DM Sans", color="#e8edf5", size=12),
    ),
)

REGION_COLOURS = {
    "US": "#ff4b4b",
    "GB": "#3b82f6",
    "IN": "#f59e0b",
    "CA": "#22c55e",
}

MOMENTUM_COLOURS = {
    "accelerating": "#22c55e",
    "growing":      "#3b82f6",
    "flat":         "#475569",
    "declining":    "#ff4b4b",
}


def apply_theme():
    """Call at the top of every page to inject the CSS theme."""
    st.markdown(THEME_CSS, unsafe_allow_html=True)


def page_header(icon: str, title: str, subtitle: str = ""):
    """Render a styled page header."""
    st.markdown(f"# {icon} {title}")
    st.markdown('<div class="title-accent"></div>', unsafe_allow_html=True)
    if subtitle:
        st.markdown(f'<p style="color:#64748b;font-size:0.88rem;margin-top:-0.5rem;margin-bottom:1.5rem">{subtitle}</p>', unsafe_allow_html=True)


def section(label: str):
    """Render a section label."""
    st.markdown(f'<div class="section-label">{label}</div>', unsafe_allow_html=True)

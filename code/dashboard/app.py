"""
OpenLens Dashboard — Leaderboard (home page)

Run:
    streamlit run code/dashboard/app.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import time

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from api_client import api_health, get_leaderboard

# ── page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="OpenLens",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── loading splash (first visit only) ───────────────────────────────────────
if "app_loaded" not in st.session_state:
    _loader = st.empty()
    with _loader.container():
        st.markdown("<br>" * 2, unsafe_allow_html=True)
        st.title("OpenLens")
        st.caption("Python open-source package intelligence")
        st.divider()
        _bar = st.progress(0, text="Initializing...")
        time.sleep(1)
        _bar.progress(25, text="Compiling pipeline data...")
        time.sleep(1)
        _bar.progress(50, text="Loading health scores...")
        time.sleep(1)
        _bar.progress(75, text="Running analysis...")
        time.sleep(1)
        _bar.progress(100, text="Dashboard ready.")
        time.sleep(0.3)
    _loader.empty()
    st.session_state["app_loaded"] = True

# ── sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.page_link("app.py",                           label="Leaderboard",     icon=None)
    st.page_link("pages/1_Package_Detail.py",        label="Package Detail",  icon=None)
    st.page_link("pages/2_Compare.py",               label="Compare",         icon=None)
    st.divider()
    try:
        h = api_health()
        st.success(f"API online · {h['packages_cached']} packages cached")
    except Exception:
        st.error("API offline — Will be online in a few seconds, please refresh")

# ── load data ─────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_leaderboard():
    return pd.DataFrame(get_leaderboard())

with st.spinner("Fetching package data, please wait..."):
    try:
        df = load_leaderboard()
    except Exception as e:
        st.error(f"Could not reach API: {e}")
        st.stop()

# ── tier helpers ──────────────────────────────────────────────────────────────
TIER_COLOR = {"A": "#2ecc71", "B": "#3498db", "C": "#f39c12", "D": "#e74c3c"}
TIER_LABEL = {"A": "A", "B": "B", "C": "C", "D": "D"}

# ── header ────────────────────────────────────────────────────────────────────
st.title("OpenLens")
st.subheader("Package Health Leaderboard")
st.caption(f"Scored at: {df['scored_at'].iloc[0][:19] if not df.empty else '—'}")

# ── metric strip ─────────────────────────────────────────────────────────────
cols = st.columns(4)
tier_counts = df["health_tier"].value_counts()
for i, tier in enumerate(["A", "B", "C", "D"]):
    cols[i].metric(f"Tier {tier}", tier_counts.get(tier, 0), label_visibility="visible")

st.divider()

# ── grouped bar chart ─────────────────────────────────────────────────────────
st.subheader("Sub-score breakdown")

score_cols = ["github_score", "pypi_score", "community_score", "sentiment_score"]
score_labels = ["GitHub", "PyPI", "Community", "Sentiment"]
bar_colors   = ["#3498db", "#e67e22", "#9b59b6", "#1abc9c"]

fig = go.Figure()
for col, label, color in zip(score_cols, score_labels, bar_colors):
    fig.add_trace(go.Bar(
        name=label,
        x=df["package_name"],
        y=df[col],
        marker_color=color,
    ))

fig.update_layout(
    barmode="group",
    height=380,
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    margin=dict(l=0, r=0, t=10, b=0),
    yaxis=dict(range=[0, 100], gridcolor="#333"),
    xaxis=dict(gridcolor="#333"),
    font=dict(color="#eee"),
)
st.plotly_chart(fig, width='stretch')

st.divider()

# ── ranked table ──────────────────────────────────────────────────────────────
st.subheader("Rankings")

display = df[["package_name", "overall_health_score", "health_tier",
              "github_score", "pypi_score", "community_score", "sentiment_score"]].copy()
display.insert(0, "rank", range(1, len(display) + 1))
display["health_tier"] = display["health_tier"].map(TIER_LABEL)
display.columns = ["#", "Package", "Overall", "Tier", "GitHub", "PyPI", "Community", "Sentiment"]

st.dataframe(
    display,
    width='stretch',
    hide_index=True,
    column_config={
        "Overall":    st.column_config.ProgressColumn("Overall", min_value=0, max_value=100, format="%.1f"),
        "GitHub":     st.column_config.ProgressColumn("GitHub",  min_value=0, max_value=100, format="%.1f"),
        "PyPI":       st.column_config.ProgressColumn("PyPI",    min_value=0, max_value=100, format="%.1f"),
        "Community":  st.column_config.ProgressColumn("Community", min_value=0, max_value=100, format="%.1f"),
        "Sentiment":  st.column_config.ProgressColumn("Sentiment", min_value=0, max_value=100, format="%.1f"),
    },
)

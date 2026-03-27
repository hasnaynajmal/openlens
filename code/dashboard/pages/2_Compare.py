"""
OpenLens Dashboard — Compare page

Side-by-side sub-score comparison for any two packages.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import plotly.graph_objects as go
import streamlit as st

from api_client import get_leaderboard, get_package

# ── page config ──────────────────────────────────────────────────────────────
st.set_page_config(page_title="Compare · OpenLens", page_icon=None, layout="wide")

# ── sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.page_link("app.py",                    label="Leaderboard")
    st.page_link("pages/1_Package_Detail.py", label="Package Detail")
    st.page_link("pages/2_Compare.py",        label="Compare")

# ── package selectors ────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def package_names():
    return [p["package_name"] for p in get_leaderboard()]

with st.spinner("Fetching package list, please wait..."):
    try:
        names = package_names()
    except Exception as e:
        st.error(f"Could not reach API: {e}")
        st.stop()

col_a, col_b = st.columns(2)
with col_a:
    pkg_a = st.selectbox("Package A", names, index=0)
with col_b:
    pkg_b = st.selectbox("Package B", names, index=min(1, len(names) - 1))

if pkg_a == pkg_b:
    st.warning("Select two different packages to compare.")
    st.stop()

# ── load data ─────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load(name: str):
    return get_package(name)

with st.spinner(f"Loading comparison data for {pkg_a} and {pkg_b}..."):
    data_a = load(pkg_a)
    data_b = load(pkg_b)
s_a = data_a["scores"]
s_b = data_b["scores"]

TIER_COLOR = {"A": "#2ecc71", "B": "#3498db", "C": "#f39c12", "D": "#e74c3c"}

# ── header cards ──────────────────────────────────────────────────────────────
st.title("Package Comparison")
st.divider()

ca, cb = st.columns(2)
for col, s, name in [(ca, s_a, pkg_a), (cb, s_b, pkg_b)]:
    tier = s["health_tier"]
    color = TIER_COLOR.get(tier, "#888")
    with col:
        st.markdown(
            f"### {name}\n"
            f'<span style="background:{color};color:#fff;padding:3px 10px;'
            f'border-radius:5px;font-weight:bold;">Tier {tier}</span>'
            f'&nbsp; **{s["overall_health_score"]:.1f} / 100**',
            unsafe_allow_html=True,
        )

st.divider()

# ── grouped bar — all sub-scores ──────────────────────────────────────────────
st.subheader("Sub-score comparison")

score_keys   = ["github_score", "pypi_score", "community_score", "sentiment_score", "overall_health_score"]
score_labels = ["GitHub", "PyPI", "Community", "Sentiment", "Overall"]

fig_bar = go.Figure()
fig_bar.add_trace(go.Bar(
    name=pkg_a,
    x=score_labels,
    y=[s_a[k] for k in score_keys],
    marker_color="#3498db",
    text=[f"{s_a[k]:.1f}" for k in score_keys],
    textposition="outside",
))
fig_bar.add_trace(go.Bar(
    name=pkg_b,
    x=score_labels,
    y=[s_b[k] for k in score_keys],
    marker_color="#e67e22",
    text=[f"{s_b[k]:.1f}" for k in score_keys],
    textposition="outside",
))
fig_bar.update_layout(
    barmode="group",
    height=380,
    yaxis=dict(range=[0, 110], gridcolor="#333"),
    xaxis=dict(gridcolor="#333"),
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#eee"),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    margin=dict(l=0, r=0, t=10, b=0),
)
st.plotly_chart(fig_bar, width='stretch')

st.divider()

# ── overlapping radar ─────────────────────────────────────────────────────────
st.subheader("Radar overlay")

categories = ["GitHub", "PyPI", "Community", "Sentiment"]
radar_keys = ["github_score", "pypi_score", "community_score", "sentiment_score"]

def _radar_trace(s, name, color):
    vals = [s[k] for k in radar_keys]
    vals_closed = vals + [vals[0]]
    cats_closed = categories + [categories[0]]
    return go.Scatterpolar(
        r=vals_closed,
        theta=cats_closed,
        fill="toself",
        fillcolor=color.replace(")", ",0.2)").replace("rgb", "rgba"),
        line=dict(color=color, width=2),
        name=name,
    )

fig_radar = go.Figure()
fig_radar.add_trace(_radar_trace(s_a, pkg_a, "rgb(52,152,219)"))
fig_radar.add_trace(_radar_trace(s_b, pkg_b, "rgb(230,126,34)"))
fig_radar.update_layout(
    polar=dict(
        radialaxis=dict(visible=True, range=[0, 100], gridcolor="#444"),
        angularaxis=dict(gridcolor="#444"),
        bgcolor="rgba(0,0,0,0)",
    ),
    paper_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#eee"),
    height=420,
    legend=dict(orientation="h", yanchor="bottom", y=-0.15, xanchor="center", x=0.5),
    margin=dict(l=40, r=40, t=20, b=40),
)
st.plotly_chart(fig_radar, width='stretch')

st.divider()

# ── delta table ───────────────────────────────────────────────────────────────
st.subheader("Score deltas (A − B)")

import pandas as pd

delta_rows = []
for key, label in zip(score_keys, score_labels):
    va, vb = s_a[key], s_b[key]
    delta_rows.append({
        "Metric":    label,
        pkg_a:       round(va, 2),
        pkg_b:       round(vb, 2),
        "Delta":     round(va - vb, 2),
        "Winner":    pkg_a if va > vb else (pkg_b if vb > va else "Tie"),
    })

delta_df = pd.DataFrame(delta_rows)
st.dataframe(delta_df, width='stretch', hide_index=True)

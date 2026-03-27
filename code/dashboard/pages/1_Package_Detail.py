"""
OpenLens Dashboard Package Detail page

Shows the full health score breakdown and sentiment analysis for one package.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import plotly.graph_objects as go
import streamlit as st

from api_client import get_leaderboard, get_package

# ── page config ──────────────────────────────────────────────────────────────
st.set_page_config(page_title="Package Detail · OpenLens", page_icon=None, layout="wide")

# ── sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.page_link("app.py",                    label="Leaderboard")
    st.page_link("pages/1_Package_Detail.py", label="Package Detail")
    st.page_link("pages/2_Compare.py",        label="Compare")

# ── package selector ─────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def package_names():
    return [p["package_name"] for p in get_leaderboard()]

with st.spinner("Fetching package list, please wait..."):
    try:
        names = package_names()
    except Exception as e:
        st.error(f"Could not reach API: {e}")
        st.stop()

selected = st.selectbox("Select a package", names)

# ── load detail ───────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_detail(name: str):
    return get_package(name)

with st.spinner(f"Loading data for {selected}..."):
    data = load_detail(selected)
scores = data["scores"]
sent   = data["sentiment"]

# ── header ────────────────────────────────────────────────────────────────────
TIER_COLOR = {"A": "#2ecc71", "B": "#3498db", "C": "#f39c12", "D": "#e74c3c"}
tier = scores["health_tier"]
color = TIER_COLOR.get(tier, "#888")

st.title(selected)
st.markdown(
    f'<span style="background:{color};color:#fff;padding:4px 12px;'
    f'border-radius:6px;font-weight:bold;font-size:1rem;">Tier {tier}</span>'
    f'&nbsp;&nbsp;<span style="font-size:1.6rem;font-weight:bold">'
    f'{scores["overall_health_score"]:.1f} / 100</span>',
    unsafe_allow_html=True,
)
st.caption(f"Scored at: {scores['scored_at'][:19]}")
st.divider()

# ── score cards ───────────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
c1.metric("GitHub",    f"{scores['github_score']:.1f}")
c2.metric("PyPI",      f"{scores['pypi_score']:.1f}")
c3.metric("Community", f"{scores['community_score']:.1f}")
c4.metric("Sentiment", f"{scores['sentiment_score']:.1f}")

st.divider()

# ── radar chart ───────────────────────────────────────────────────────────────
st.subheader("Score radar")

categories = ["GitHub", "PyPI", "Community", "Sentiment"]
values     = [
    scores["github_score"],
    scores["pypi_score"],
    scores["community_score"],
    scores["sentiment_score"],
]
values_closed = values + [values[0]]
categories_closed = categories + [categories[0]]

fig_radar = go.Figure(go.Scatterpolar(
    r=values_closed,
    theta=categories_closed,
    fill="toself",
    fillcolor=f"rgba(52,152,219,0.25)",
    line=dict(color="#3498db", width=2),
    name=selected,
))
fig_radar.update_layout(
    polar=dict(
        radialaxis=dict(visible=True, range=[0, 100], gridcolor="#444"),
        angularaxis=dict(gridcolor="#444"),
        bgcolor="rgba(0,0,0,0)",
    ),
    paper_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#eee"),
    height=380,
    margin=dict(l=40, r=40, t=20, b=20),
)
st.plotly_chart(fig_radar, width='stretch')

st.divider()

# ── sentiment breakdown ───────────────────────────────────────────────────────
st.subheader("Sentiment breakdown")

sent_sources = {
    "SO Questions":    sent.get("so_question_sentiment_avg"),
    "SO Answers":      sent.get("so_answer_sentiment_avg"),
    "README":          sent.get("readme_sentiment_compound"),
    "PyPI Description":sent.get("pypi_desc_sentiment_compound"),
    "Overall":         sent.get("overall_sentiment"),
}

# Filter out nulls
sent_sources = {k: v for k, v in sent_sources.items() if v is not None}

if sent_sources:
    bar_colors = ["#2ecc71" if v >= 0 else "#e74c3c" for v in sent_sources.values()]
    fig_sent = go.Figure(go.Bar(
        x=list(sent_sources.keys()),
        y=list(sent_sources.values()),
        marker_color=bar_colors,
        text=[f"{v:.3f}" for v in sent_sources.values()],
        textposition="outside",
    ))
    fig_sent.update_layout(
        height=320,
        yaxis=dict(range=[-1, 1], gridcolor="#333", zeroline=True, zerolinecolor="#888"),
        xaxis=dict(gridcolor="#333"),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#eee"),
        margin=dict(l=0, r=0, t=10, b=0),
    )
    st.plotly_chart(fig_sent, width='stretch')
    st.caption("Compound sentiment in [-1, +1]. Green = positive, red = negative.")
else:
    st.info("No sentiment data available for this package.")

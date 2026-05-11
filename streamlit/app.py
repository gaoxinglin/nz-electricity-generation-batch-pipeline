"""
NZ Electricity Generation Dashboard
Entry point — sets page config and navigation only.
Run: streamlit run streamlit/app.py
"""

import streamlit as st

st.set_page_config(
    page_title="NZ Electricity Generation",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    /* Tighten metric cards */
    div[data-testid="metric-container"] {
        background: var(--secondary-background-color);
        border-radius: 10px;
        padding: 16px 20px;
        border: 1px solid rgba(128,128,128,0.12);
    }
    /* Remove top padding from main block */
    .block-container { padding-top: 1.5rem; }
</style>
""", unsafe_allow_html=True)

pg = st.navigation({
    "Generation (V1)": [
        st.Page("pages/overview.py",        title="Overview",          icon="📊", default=True),
        st.Page("pages/fuel_trends.py",     title="Fuel Trends",       icon="📈"),
        st.Page("pages/plant_ranking.py",   title="Plant Ranking",     icon="🏆"),
        st.Page("pages/renewable_share.py", title="Renewable Share",   icon="🌱"),
        st.Page("pages/seasonal.py",        title="Seasonal Patterns", icon="🌤️"),
    ],
    "Wholesale Price (V2)": [
        st.Page("pages/price_overview.py",  title="Price Overview",   icon="⚡"),
        st.Page("pages/price_spikes.py",    title="Price Spikes",     icon="🔺"),
        st.Page("pages/renewable_price.py", title="Renewable vs Price", icon="🌿"),
        st.Page("pages/island_spread.py",   title="Island Spread",    icon="🏝️"),
    ],
    "Operations": [
        st.Page("pages/pipeline_health.py", title="Pipeline Health",  icon="🩺"),
    ],
})

import os

with st.sidebar:
    mode = os.environ.get("NZEG_MODE", "local").lower()
    badge = "🟢 Local (DuckDB)" if mode == "local" else "☁️ Cloud (Snowflake)"
    st.caption(f"Mode: **{badge}**")
    st.caption("Source: Electricity Authority (EMI). Data refreshes hourly.")
    st.divider()

pg.run()

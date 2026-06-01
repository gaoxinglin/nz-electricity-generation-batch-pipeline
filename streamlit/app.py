"""NZ electricity market dashboard entry point.

Run: streamlit run streamlit/app.py
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import streamlit as st
from ui import inject_global_styles

st.set_page_config(
    page_title="NZ Electricity Market Monitor",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

inject_global_styles()

pg = st.navigation({
    "Start here": [
        st.Page("pages/overview.py", title="Executive overview", default=True),
    ],
    "Generation supply": [
        st.Page("pages/fuel_trends.py", title="Fuel mix trend"),
        st.Page("pages/plant_ranking.py", title="Largest stations"),
        st.Page("pages/renewable_share.py", title="Renewable share"),
        st.Page("pages/seasonal.py", title="Seasonal pattern"),
    ],
    "Wholesale prices": [
        st.Page("pages/price_overview.py", title="Price overview"),
        st.Page("pages/price_spikes.py", title="Price spikes"),
        st.Page("pages/market_context.py", title="Spike drivers"),
        st.Page("pages/renewable_price.py", title="Renewables and price"),
        st.Page("pages/island_spread.py", title="Island price spread"),
        st.Page("pages/hydro_price_driver.py", title="Hydro storage driver"),
    ],
    "Data product health": [
        st.Page("pages/pipeline_health.py", title="Pipeline health"),
    ],
})

with st.sidebar:
    mode = os.environ.get("NZEG_MODE", "local").lower()
    badge = "Local DuckDB" if mode == "local" else "Cloud Snowflake"
    st.markdown("### NZ Electricity Market Monitor")
    st.caption(f"Mode: **{badge}**")
    st.caption("Source: Electricity Authority EMI public datasets.")
    st.caption("The dashboard reads curated analytics tables, so every number should be traceable.")
    st.divider()
    st.caption("Use the first page for the project story, then drill into generation, price risk, and pipeline health.")

pg.run()

"""North Island vs South Island wholesale price comparison."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import plotly.express as px
import plotly.graph_objects as go
from charts import apply_layout
from loader import load_price_daily
from ui import date_range_filter, fmt_price, insight_box, page_header

import streamlit as st

page_header(
    "Island Price Spread",
    "Is one island consistently paying more than the other?",
    "Positive spread means the North Island daily average is above the South Island daily average.",
)

try:
    df = load_price_daily().dropna(subset=["island"])
except Exception:
    st.error("Could not load the island price summary for this environment.")
    st.stop()

if df.empty:
    st.info("No price data with island attribution is available. Load Network Supply Points first.")
    st.stop()

d0, d1 = date_range_filter(df, "trading_date", key="island_spread_dates")
view = df[df["trading_date"].between(d0, d1)].copy()

island_daily = (
    view.groupby(["trading_date", "island"], as_index=False)["avg_price_all"]
    .mean()
)
pivot = island_daily.pivot(index="trading_date", columns="island", values="avg_price_all")

if "NI" not in pivot.columns or "SI" not in pivot.columns:
    st.warning("The selected date range needs both NI and SI prices for spread analysis.")
    st.stop()

pivot["spread"] = pivot["NI"] - pivot["SI"]
mean_ni = pivot["NI"].mean()
mean_si = pivot["SI"].mean()
mean_spread = pivot["spread"].mean()
max_abs_spread = pivot["spread"].abs().max()
dominant_side = "North Island" if mean_spread > 0 else "South Island" if mean_spread < 0 else "Neither island"

k1, k2, k3, k4 = st.columns(4)
k1.metric("Average NI price", fmt_price(mean_ni, 1))
k2.metric("Average SI price", fmt_price(mean_si, 1))
k3.metric("Mean spread NI - SI", fmt_price(mean_spread, 1))
k4.metric("Largest absolute spread", fmt_price(max_abs_spread, 1))

insight_box(
    "Market read",
    f"{dominant_side} was more expensive on average in this selection. Persistent spreads can indicate inter-island transfer constraints or regional scarcity.",
)

st.subheader("Daily average price by island")
fig = px.line(
    island_daily,
    x="trading_date",
    y="avg_price_all",
    color="island",
    labels={"avg_price_all": "$/MWh", "trading_date": "Date", "island": "Island"},
    color_discrete_map={"NI": "#1565C0", "SI": "#EF6C00"},
)
apply_layout(fig, height=380)
st.plotly_chart(fig, width="stretch")

st.subheader("Daily spread: NI minus SI")
spread_df = pivot.reset_index()[["trading_date", "spread"]]
fig2 = go.Figure(
    go.Bar(
        x=spread_df["trading_date"],
        y=spread_df["spread"],
        marker_color=["#1565C0" if value >= 0 else "#EF6C00" for value in spread_df["spread"]],
    )
)
fig2.update_layout(yaxis_title="$/MWh (NI - SI)", xaxis_title="Date")
apply_layout(fig2, height=320)
st.plotly_chart(fig2, width="stretch")

st.subheader("Spread distribution")
fig3 = px.histogram(
    spread_df,
    x="spread",
    nbins=30,
    labels={"spread": "NI - SI ($/MWh)"},
)
fig3.update_traces(marker_color="#5C6BC0")
apply_layout(fig3, height=280)
st.plotly_chart(fig3, width="stretch")

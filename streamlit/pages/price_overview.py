"""Daily wholesale price summary across NZ points of connection."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import plotly.express as px
from charts import apply_layout
from loader import load_price_daily
from ui import date_range_filter, fmt_int, fmt_pct, fmt_price, insight_box, multiselect_all, page_header, safe_divide

import streamlit as st

page_header(
    "Price Overview",
    "What did wholesale electricity prices look like across the loaded nodes and days?",
    "This page shows daily averages, peak prices, negative prices, and spike frequency.",
)

try:
    df = load_price_daily()
except Exception:
    st.error("Could not load the wholesale price summary for this environment.")
    st.stop()

if df.empty:
    st.info("No wholesale price data is available. Load price data and run the transformation job.")
    st.stop()

control_a, control_b = st.columns([2, 3])
with control_a:
    pick_islands = multiselect_all("Island", df["island"].dropna().unique().tolist(), key="price_overview_islands")
with control_b:
    d0, d1 = date_range_filter(df, "trading_date", key="price_overview_dates")

mask = df["trading_date"].between(d0, d1)
if pick_islands:
    mask &= df["island"].isin(pick_islands)
view = df[mask].copy()

if view.empty:
    st.warning("No price rows match the selected filters.")
    st.stop()

total_poc_days = len(view)
mean_price = view["avg_price_all"].mean()
max_price = view["max_price"].max()
neg_tp_total = int(view["negative_tp_count"].sum())
spike_tp_total = int(view["spike_tp_count"].sum())
total_tps = int(view["tp_count"].sum())
spike_rate = safe_divide(spike_tp_total, total_tps)
negative_rate = safe_divide(neg_tp_total, total_tps)

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Node-days covered", fmt_int(total_poc_days))
k2.metric("Average price", fmt_price(mean_price, 1))
k3.metric("Peak price", fmt_price(max_price))
k4.metric("Spike periods", fmt_int(spike_tp_total), fmt_pct(spike_rate * 100) if spike_rate is not None else None)
k5.metric("Negative periods", fmt_int(neg_tp_total), fmt_pct(negative_rate * 100) if negative_rate is not None else None)

insight_box(
    "What to watch",
    "Average price shows the broad market level, while spike periods reveal short intervals where local scarcity or network conditions mattered more.",
)

st.subheader("Daily average price by island")
daily = (
    view.groupby(["trading_date", "island"], dropna=False)["avg_price_all"]
    .mean()
    .reset_index()
)
fig = px.line(
    daily,
    x="trading_date",
    y="avg_price_all",
    color="island",
    labels={"avg_price_all": "$/MWh", "trading_date": "Date", "island": "Island"},
)
apply_layout(fig, height=380)
st.plotly_chart(fig, width="stretch")

st.subheader("Distribution of daily node prices")
fig2 = px.histogram(
    view,
    x="avg_price_all",
    color="island",
    nbins=40,
    opacity=0.72,
    labels={"avg_price_all": "$/MWh", "count": "Node-days", "island": "Island"},
    barmode="overlay",
)
apply_layout(fig2, height=320)
st.plotly_chart(fig2, width="stretch")

left, right = st.columns(2)
with left:
    st.subheader("Spike periods by day")
    sp = view.groupby("trading_date", as_index=False)["spike_tp_count"].sum()
    fig3 = px.bar(
        sp,
        x="trading_date",
        y="spike_tp_count",
        labels={"spike_tp_count": "Periods above threshold", "trading_date": "Date"},
    )
    fig3.update_traces(marker_color="#C62828")
    apply_layout(fig3, height=300)
    st.plotly_chart(fig3, width="stretch")

with right:
    st.subheader("Negative-price periods by day")
    ng = view.groupby("trading_date", as_index=False)["negative_tp_count"].sum()
    if ng["negative_tp_count"].sum() == 0:
        st.info("No negative-price trading periods appear in the selected window.")
    fig4 = px.bar(
        ng,
        x="trading_date",
        y="negative_tp_count",
        labels={"negative_tp_count": "Periods below $0/MWh", "trading_date": "Date"},
    )
    fig4.update_traces(marker_color="#1565C0")
    apply_layout(fig4, height=300)
    st.plotly_chart(fig4, width="stretch")

st.caption("Spike and negative-price thresholds come from the transformation configuration, so the dashboard stays consistent with the data model.")

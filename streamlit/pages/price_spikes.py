"""Trading-period price spike events with co-located generation context."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import plotly.express as px
from charts import apply_layout
from loader import load_price_spikes
from ui import date_range_filter, fmt_int, fmt_pct, fmt_price, insight_box, multiselect_all, page_header

import streamlit as st

page_header(
    "Price Spikes",
    "When and where did short-interval wholesale price spikes occur?",
    "A spike event is a node and trading period above the configured price threshold.",
)

try:
    df = load_price_spikes()
except Exception:
    st.error("Could not load the price spike summary for this environment.")
    st.stop()

if df.empty:
    st.info("No price spike events are available for the loaded data.")
    st.stop()

control_a, control_b = st.columns([2, 3])
with control_a:
    pick_islands = multiselect_all("Island", df["island"].dropna().unique().tolist(), key="price_spike_islands")
with control_b:
    d0, d1 = date_range_filter(df, "trading_date", key="price_spike_dates")

mask = df["trading_date"].between(d0, d1)
if pick_islands:
    mask &= df["island"].isin(pick_islands)
view = df[mask].copy()

if view.empty:
    st.warning("No spike events match the selected filters.")
    st.stop()

n_events = len(view)
unmatched = int(view["unmatched_generation"].fillna(False).sum())
unmatched_pct = 100.0 * unmatched / n_events if n_events else 0
top_poc_count = view["poc_code"].value_counts().head(1)
top_poc = top_poc_count.index[0] if len(top_poc_count) else "N/A"

k1, k2, k3, k4 = st.columns(4)
k1.metric("Spike events", fmt_int(n_events))
k2.metric("Nodes affected", fmt_int(view["poc_code"].nunique()))
k3.metric("Peak spike price", fmt_price(view["price_nzd_mwh"].max()))
k4.metric("Most frequent node", top_poc, fmt_int(top_poc_count.iloc[0]) if len(top_poc_count) else None)

if unmatched_pct > 50:
    st.warning(
        f"{unmatched_pct:.0f}% of selected spikes are at load-only nodes with no co-located generation. "
        "Fuel context below only represents matched generation rows."
    )
else:
    insight_box(
        "Coverage note",
        f"{unmatched_pct:.0f}% of selected spikes have no co-located generation. Use the spike-driver page for volume and offer context.",
    )

st.subheader("Spike density by date and trading period")
heat = (
    view.assign(date_label=view["trading_date"].dt.strftime("%Y-%m-%d"))
    .groupby(["date_label", "tp_number"])
    .size()
    .reset_index(name="spike_count")
    .pivot(index="date_label", columns="tp_number", values="spike_count")
    .fillna(0)
)
if heat.empty:
    st.info("No spike density data is available.")
else:
    fig = px.imshow(
        heat,
        aspect="auto",
        color_continuous_scale="Reds",
        labels=dict(x="Trading period", y="Date", color="Spike count"),
    )
    apply_layout(fig, height=420)
    st.plotly_chart(fig, width="stretch")

left, right = st.columns(2)
with left:
    st.subheader("Nodes hit most often")
    top = (
        view.groupby(["poc_code", "island"], dropna=False)
        .size()
        .reset_index(name="spike_count")
        .sort_values("spike_count", ascending=False)
        .head(20)
    )
    fig_t = px.bar(
        top.sort_values("spike_count"),
        x="spike_count",
        y="poc_code",
        color="island",
        orientation="h",
        labels={"spike_count": "Spike periods", "poc_code": "Node", "island": "Island"},
    )
    apply_layout(fig_t, height=480)
    st.plotly_chart(fig_t, width="stretch")

with right:
    st.subheader("Generation mix during matched spikes")
    matched = view[~view["unmatched_generation"].fillna(False)].copy()
    if matched.empty:
        st.info("No matched generation is available in this selection.")
    else:
        matched["renewable_kwh"] = matched["renewable_generation_kwh"].fillna(0)
        matched["thermal_kwh"] = matched["thermal_generation_kwh"].fillna(0)
        fuel_long = matched.melt(
            id_vars=["trading_date"],
            value_vars=["renewable_kwh", "thermal_kwh"],
            var_name="source",
            value_name="kwh",
        )
        daily_fuel = fuel_long.groupby(["trading_date", "source"], as_index=False)["kwh"].sum()
        fig_f = px.area(
            daily_fuel,
            x="trading_date",
            y="kwh",
            color="source",
            labels={"kwh": "kWh", "trading_date": "Date", "source": "Source"},
            color_discrete_map={"renewable_kwh": "#2E7D32", "thermal_kwh": "#616161"},
        )
        apply_layout(fig_f, height=480)
        st.plotly_chart(fig_f, width="stretch")

st.caption(f"Matched-generation coverage: {fmt_pct(100 - unmatched_pct, 0)} of selected spike events.")

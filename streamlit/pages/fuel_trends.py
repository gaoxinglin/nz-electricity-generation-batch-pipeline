from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from charts import apply_layout, color_map
from loader import load_monthly, load_renewable
from ui import fmt_gwh, fmt_month, fmt_pct, insight_box, month_range_filter, page_header

import streamlit as st

page_header(
    "Fuel Mix Trend",
    "How is New Zealand's generation mix changing over the loaded period?",
    "The chart can show physical output in GWh or each fuel's share of the monthly total.",
)

with st.spinner("Loading generation summaries..."):
    monthly = load_monthly()
    renewable = load_renewable()

if monthly.empty:
    st.error("No generation data is available.")
    st.stop()

start_ym, end_ym = month_range_filter(monthly, key="fuel_trend_months")
filtered = monthly[(monthly["year_month"] >= start_ym) & (monthly["year_month"] <= end_ym)].copy()

if filtered.empty:
    st.warning("No generation rows match the selected months.")
    st.stop()

measure = st.radio(
    "Measure",
    ["Generation output (GWh)", "Share of monthly generation (%)"],
    horizontal=True,
)

if measure == "Share of monthly generation (%)":
    totals = filtered.groupby("year_month")["total_generation_gwh"].transform("sum").replace(0, pd.NA)
    filtered["y_val"] = filtered["total_generation_gwh"] / totals * 100
    y_label = "Share of monthly generation (%)"
else:
    filtered["y_val"] = filtered["total_generation_gwh"]
    y_label = "Generation (GWh)"

filtered["month"] = pd.to_datetime(filtered["year_month"], format="%Y%m")
colors = color_map(filtered["fuel_type"].unique().tolist())
period_total = filtered["total_generation_gwh"].sum()
latest_ym = filtered["year_month"].max()
latest_mix = (
    filtered[filtered["year_month"] == latest_ym]
    .groupby("fuel_type")["total_generation_gwh"]
    .sum()
    .sort_values(ascending=False)
)
top_fuel = latest_mix.index[0] if not latest_mix.empty else "N/A"
top_share = latest_mix.iloc[0] / latest_mix.sum() * 100 if latest_mix.sum() else pd.NA

c1, c2, c3 = st.columns(3)
c1.metric("Selected generation", fmt_gwh(period_total))
c2.metric("Latest month", fmt_month(latest_ym))
c3.metric("Largest latest fuel", top_fuel, fmt_pct(top_share))

insight_box(
    "How to read it",
    "A rising share means that fuel is taking a larger role in the supply mix. A rising GWh line means actual output increased, even if the percentage share stayed flat.",
)

st.subheader("Monthly generation by fuel")
fig = px.area(
    filtered.sort_values(["month", "fuel_type"]),
    x="month",
    y="y_val",
    color="fuel_type",
    color_discrete_map=colors,
    labels={"month": "Month", "y_val": y_label, "fuel_type": "Fuel"},
)
fig = apply_layout(fig, height=480)
st.plotly_chart(fig, width="stretch")

st.subheader("Renewable share trend")
ren = renewable[
    (renewable["year_month"] >= start_ym) & (renewable["year_month"] <= end_ym)
].copy()
if ren.empty:
    st.info("Renewable ratio mart has no rows for the selected period.")
else:
    ren = ren.sort_values("year_month")
    ren["month"] = pd.to_datetime(ren["year_month"], format="%Y%m")
    ren["rolling"] = ren["renewable_pct"].rolling(12, min_periods=min(3, len(ren))).mean()
    avg = ren["renewable_pct"].mean()

    fig2 = go.Figure()
    fig2.add_trace(
        go.Scatter(
            x=ren["month"],
            y=ren["renewable_pct"],
            mode="lines+markers",
            name="Monthly share",
            line=dict(color="#2E7D32", width=1.8),
        )
    )
    if len(ren) >= 3:
        fig2.add_trace(
            go.Scatter(
                x=ren["month"],
                y=ren["rolling"],
                mode="lines",
                name="Rolling average",
                line=dict(color="#1565C0", width=3),
            )
        )
    fig2.add_hline(
        y=avg,
        line_dash="dot",
        line_color="gray",
        annotation_text=f"Period average {avg:.1f}%",
        annotation_position="top left",
    )
    fig2 = apply_layout(fig2, height=400)
    st.plotly_chart(fig2, width="stretch")

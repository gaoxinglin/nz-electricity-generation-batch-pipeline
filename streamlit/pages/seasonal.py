from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from charts import apply_layout, color_map
from loader import load_monthly_raw, load_seasonal
from ui import fmt_gwh, insight_box, page_header

import streamlit as st

SEASON_ORDER = ["Summer", "Autumn", "Winter", "Spring"]
MONTH_LABELS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

page_header(
    "Seasonal Pattern",
    "How does the generation mix change by season and calendar month?",
    "Seasonality helps explain winter demand, hydro inflows, and thermal backup requirements.",
)

with st.spinner("Loading seasonal summaries..."):
    seasonal = load_seasonal()
    monthly = load_monthly_raw()

if seasonal.empty:
    st.error("No seasonal data is available.")
    st.stop()

all_years = sorted(seasonal["season_year"].dropna().unique())
if len(all_years) == 1:
    start_year = end_year = all_years[0]
    st.caption(f"Available season year: {start_year}")
else:
    col_a, col_b = st.columns(2)
    start_year = col_a.selectbox("From year", all_years, index=0)
    end_year = col_b.selectbox("To year", all_years, index=len(all_years) - 1)
    if start_year > end_year:
        st.info("The selected start year is after the end year, so the dashboard has swapped them.")
        start_year, end_year = end_year, start_year

filtered = seasonal[
    (seasonal["season_year"] >= start_year) & (seasonal["season_year"] <= end_year)
].copy()

period_total = filtered["total_generation_gwh"].sum()
season_totals = (
    filtered.groupby(["season", "fuel_type"], as_index=False)["total_generation_gwh"]
    .sum()
)
season_totals["season"] = pd.Categorical(season_totals["season"], categories=SEASON_ORDER, ordered=True)
season_totals = season_totals.sort_values("season")

if season_totals.empty:
    st.warning("No seasonal rows match the selected year range.")
    st.stop()

peak_season = (
    season_totals.groupby("season", observed=False)["total_generation_gwh"].sum().sort_values(ascending=False)
)

c1, c2, c3 = st.columns(3)
c1.metric("Selected generation", fmt_gwh(period_total))
c2.metric("Highest generation season", str(peak_season.index[0]), fmt_gwh(peak_season.iloc[0]))
c3.metric("Fuel categories", f"{season_totals['fuel_type'].nunique():,}")

insight_box(
    "How to use this view",
    "Look for the fuels that expand in high-demand seasons. Those fuels often represent the system's flexible or backup capacity.",
)

st.subheader("Generation by season and fuel")
colors = color_map(season_totals["fuel_type"].unique().tolist())
fig = px.bar(
    season_totals,
    x="season",
    y="total_generation_gwh",
    color="fuel_type",
    barmode="group",
    color_discrete_map=colors,
    labels={"season": "Season", "total_generation_gwh": "Total GWh", "fuel_type": "Fuel"},
)
fig = apply_layout(fig, height=460)
st.plotly_chart(fig, width="stretch")

st.subheader("Average monthly generation by fuel")
if monthly.empty:
    st.info("Monthly generation detail is not available for the heatmap.")
else:
    heatmap_data = (
        monthly.groupby(["month_num", "fuel_type"], as_index=False)["total_generation_gwh"]
        .mean()
    )
    pivot = heatmap_data.pivot(index="fuel_type", columns="month_num", values="total_generation_gwh").fillna(0)
    pivot = pivot.reindex(columns=range(1, 13), fill_value=0)
    pivot.columns = [MONTH_LABELS[int(c) - 1] for c in pivot.columns]

    fig2 = go.Figure(
        data=go.Heatmap(
            z=pivot.values,
            x=pivot.columns.tolist(),
            y=pivot.index.tolist(),
            colorscale=[[0, "#F5F5F5"], [0.5, "#90CAF9"], [1, "#1565C0"]],
            colorbar=dict(title="Avg GWh"),
            hoverongaps=False,
        )
    )
    fig2.update_layout(
        height=380,
        margin=dict(l=16, r=16, t=40, b=16),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(size=13),
    )
    st.plotly_chart(fig2, width="stretch")

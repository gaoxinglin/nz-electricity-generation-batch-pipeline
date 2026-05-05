import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from charts import apply_layout, color_map
from loader import load_monthly_raw, load_seasonal

import streamlit as st

SEASON_ORDER = ["Summer", "Autumn", "Winter", "Spring"]

st.title("Seasonal Patterns")
st.caption("How generation mix varies by season and calendar month.")

with st.spinner("Fetching data…"):
    seasonal = load_seasonal()
    monthly  = load_monthly_raw()

if seasonal.empty:
    st.error("No data available.")
    st.stop()

# ── Year range filter ──────────────────────────────────────────────────────────
all_years  = sorted(seasonal["season_year"].unique())
col_a, col_b = st.columns(2)
start_year = col_a.selectbox("From Year", all_years, index=0)
end_year   = col_b.selectbox("To Year",   all_years, index=len(all_years) - 1)

filtered = seasonal[
    (seasonal["season_year"] >= start_year) & (seasonal["season_year"] <= end_year)
]

# ── Season grouped bar ─────────────────────────────────────────────────────────
st.subheader("Generation by Season and Fuel Type")
season_totals = (
    filtered.groupby(["season", "fuel_type"])["total_generation_gwh"]
    .sum()
    .reset_index()
)
season_totals["season"] = pd.Categorical(
    season_totals["season"], categories=SEASON_ORDER, ordered=True
)
season_totals = season_totals.sort_values("season")

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
st.plotly_chart(fig, use_container_width=True)

# ── Monthly heatmap ────────────────────────────────────────────────────────────
st.subheader("Average Monthly Generation by Fuel (GWh)")
heatmap_data = (
    monthly.groupby(["month_num", "fuel_type"])["total_generation_gwh"]
    .mean()
    .reset_index()
)
pivot = heatmap_data.pivot(
    index="fuel_type", columns="month_num", values="total_generation_gwh"
).fillna(0)

MONTH_LABELS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
pivot.columns = [MONTH_LABELS[c - 1] for c in pivot.columns]

fig2 = go.Figure(data=go.Heatmap(
    z=pivot.values,
    x=pivot.columns.tolist(),
    y=pivot.index.tolist(),
    colorscale=[[0, "#E3F2FD"], [0.5, "#42A5F5"], [1, "#1565C0"]],
    colorbar=dict(title="Avg GWh"),
    hoverongaps=False,
))
fig2.update_layout(
    height=380,
    margin=dict(l=16, r=16, t=40, b=16),
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(size=13),
)
st.plotly_chart(fig2, use_container_width=True)

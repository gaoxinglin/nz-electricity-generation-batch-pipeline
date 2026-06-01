from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import plotly.express as px
from charts import apply_layout, color_map
from loader import load_ranking
from ui import fmt_gwh, fmt_month, fmt_pct, insight_box, page_header

import streamlit as st

page_header(
    "Largest Stations",
    "Which power stations contributed the most generation in a selected month?",
    "Use this page to move from national fuel mix into the individual assets behind it.",
)

with st.spinner("Loading plant ranking mart..."):
    ranking = load_ranking()

if ranking.empty:
    st.error("No plant ranking data is available.")
    st.stop()

all_months = sorted(ranking["year_month"].unique(), reverse=True)
month_labels = [fmt_month(month) for month in all_months]
ym_by_label = dict(zip(month_labels, all_months, strict=True))

control_a, control_b = st.columns([3, 1])
selected_label = control_a.selectbox("Month", month_labels)
top_n = control_b.slider("Stations shown", min_value=5, max_value=25, value=10, step=5)

selected_ym = ym_by_label[selected_label]
month_data = ranking[ranking["year_month"] == selected_ym].sort_values("monthly_rank")
top_data = month_data.head(top_n)

if top_data.empty:
    st.warning("No stations are available for the selected month.")
    st.stop()

total_month_gwh = month_data["total_generation_gwh"].sum()
top_share = top_data["total_generation_gwh"].sum() / total_month_gwh * 100 if total_month_gwh else 0
largest = top_data.iloc[0]

k1, k2, k3 = st.columns(3)
k1.metric("Stations in month", f"{len(month_data):,}")
k2.metric("Largest station", largest["site_code"], fmt_gwh(largest["total_generation_gwh"]))
k3.metric(f"Top {top_n} share", fmt_pct(top_share))

insight_box(
    "Why this matters",
    "A concentrated ranking means a small number of stations can have a large effect on system supply, outages, and fuel exposure.",
)

colors = color_map(top_data["primary_fuel_type"].unique().tolist())

st.subheader(f"Top {top_n} stations in {selected_label}")
fig = px.bar(
    top_data,
    x="total_generation_gwh",
    y="site_code",
    color="primary_fuel_type",
    orientation="h",
    color_discrete_map=colors,
    text="total_generation_gwh",
    labels={
        "total_generation_gwh": "Generation (GWh)",
        "site_code": "Station",
        "primary_fuel_type": "Fuel",
    },
)
fig.update_traces(texttemplate="%{x:.1f}", textposition="outside")
fig.update_layout(yaxis={"categoryorder": "total ascending"})
fig = apply_layout(fig, height=max(360, top_n * 38))
st.plotly_chart(fig, width="stretch")

st.subheader("Full monthly ranking")
display = (
    month_data[["monthly_rank", "site_code", "primary_fuel_type", "total_generation_gwh"]]
    .rename(
        columns={
            "monthly_rank": "Rank",
            "site_code": "Station",
            "primary_fuel_type": "Fuel",
            "total_generation_gwh": "Generation GWh",
        }
    )
    .reset_index(drop=True)
)
st.dataframe(display, width="stretch", hide_index=True)

st.download_button(
    label="Download ranking CSV",
    data=display.to_csv(index=False),
    file_name=f"plant_ranking_{selected_ym}.csv",
    mime="text/csv",
)

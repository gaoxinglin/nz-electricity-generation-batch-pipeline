import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import plotly.express as px
from charts import apply_layout, color_map, fmt_month
from loader import load_ranking

import streamlit as st

st.title("Plant Ranking")
st.caption("Top generators each month, ranked by site output.")

with st.spinner("Fetching data…"):
    ranking = load_ranking()

if ranking.empty:
    st.error("No data available.")
    st.stop()

# ── Month + Top-N selectors ────────────────────────────────────────────────────
all_months   = sorted(ranking["year_month"].unique(), reverse=True)
month_labels = [fmt_month(m) for m in all_months]
ym_by_label  = dict(zip(month_labels, all_months, strict=True))

col_a, col_b = st.columns([3, 1])
selected_label = col_a.selectbox("Month", month_labels)
top_n          = col_b.slider("Top N", min_value=5, max_value=25, value=10, step=5)

selected_ym = ym_by_label[selected_label]
month_data  = ranking[ranking["year_month"] == selected_ym].sort_values("monthly_rank")
top_data    = month_data.head(top_n)

colors = color_map(top_data["primary_fuel_type"].unique().tolist())

# ── Horizontal bar ─────────────────────────────────────────────────────────────
st.subheader(f"Top {top_n} Power Stations — {selected_label}")
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
        "primary_fuel_type": "Fuel Type",
    },
)
fig.update_traces(texttemplate="%{x:.1f}", textposition="outside")
fig.update_layout(yaxis={"categoryorder": "total ascending"})
fig = apply_layout(fig, height=max(360, top_n * 38))
st.plotly_chart(fig, use_container_width=True)

# ── Full ranking table + download ─────────────────────────────────────────────
st.subheader(f"Full Ranking — {selected_label}")

display = (
    month_data[["monthly_rank", "site_code", "primary_fuel_type", "total_generation_gwh"]]
    .rename(columns={
        "monthly_rank":         "Rank",
        "site_code":            "Station",
        "primary_fuel_type":    "Fuel Type",
        "total_generation_gwh": "Generation (GWh)",
    })
    .reset_index(drop=True)
)
st.dataframe(display, use_container_width=True, hide_index=True)

st.download_button(
    label="⬇ Download CSV",
    data=display.to_csv(index=False),
    file_name=f"plant_ranking_{selected_ym}.csv",
    mime="text/csv",
)

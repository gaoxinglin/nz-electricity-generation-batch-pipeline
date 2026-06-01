"""Hydro storage as a driver of wholesale price pressure."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from charts import apply_layout
from loader import load_hydro_price_driver, load_hydro_storage_detail
from plotly.subplots import make_subplots
from ui import fmt_pct, fmt_price, insight_box, multiselect_all, page_header

import streamlit as st

page_header(
    "Hydro Storage Driver",
    "Are hydro storage levels adding upward pressure to wholesale prices?",
    "Hydro is a major part of New Zealand supply, so low storage can increase reliance on expensive thermal generation.",
)

try:
    df = load_hydro_price_driver()
except Exception:
    st.info(
        "Hydro-price data is not loaded in this environment. Run the hydro ingestion and transformation job, then return to this page."
    )
    st.stop()

if df.empty:
    st.info("No hydro storage rows are available for the loaded period.")
    st.stop()

df["year_month_date"] = pd.to_datetime(df["year_month"], format="%Y%m")
sel_islands = multiselect_all("Island", df["island"].dropna().unique().tolist(), key="hydro_islands")
if sel_islands:
    df = df[df["island"].isin(sel_islands)]

if df.empty:
    st.warning("No hydro rows match the selected filters.")
    st.stop()

has_price = df["avg_price_nzd_mwh"].notna().any()
latest = df.sort_values("year_month").groupby("island", as_index=False).last()

kpi_cols = st.columns(max(1, len(latest) * 2))
for idx, row in latest.iterrows():
    k = idx * 2
    storage_val = f"{row['avg_total_storage_mm3']:,.0f} Mm3" if pd.notna(row["avg_total_storage_mm3"]) else "N/A"
    pct_val = fmt_pct(row["storage_pct_of_max"], 0) + " of max" if pd.notna(row["storage_pct_of_max"]) else None
    price_val = fmt_price(row["avg_price_nzd_mwh"]) if pd.notna(row.get("avg_price_nzd_mwh")) else "N/A"
    kpi_cols[k].metric(f"{row['island']} storage", storage_val, pct_val)
    if k + 1 < len(kpi_cols):
        kpi_cols[k + 1].metric(f"{row['island']} average price", price_val)

if not has_price:
    st.info("Hydro storage is loaded, but overlapping price data is not available for correlation in this environment.")
else:
    insight_box(
        "How to read it",
        "Lower storage with higher price supports the expected scarcity story. A weak relationship means price was probably driven by demand, constraints, offers, or fuel costs.",
    )

st.subheader("Monthly storage and wholesale price")
for island in sel_islands or sorted(df["island"].dropna().unique()):
    idf = df[df["island"] == island].sort_values("year_month_date")
    if idf.empty:
        continue

    if has_price and idf["avg_price_nzd_mwh"].notna().any():
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(
            go.Bar(
                x=idf["year_month_date"],
                y=idf["avg_total_storage_mm3"],
                name="Storage (Mm3)",
                marker_color="#1565C0" if island == "NI" else "#2E7D32",
                opacity=0.75,
            ),
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(
                x=idf["year_month_date"],
                y=idf["avg_price_nzd_mwh"],
                name="Average price ($/MWh)",
                line={"color": "#C62828", "width": 2},
                mode="lines+markers",
                marker={"size": 4},
            ),
            secondary_y=True,
        )
        fig.update_yaxes(title_text="Total storage (Mm3)", secondary_y=False)
        fig.update_yaxes(title_text="Average price ($/MWh)", secondary_y=True)
    else:
        fig = go.Figure(
            go.Bar(
                x=idf["year_month_date"],
                y=idf["avg_total_storage_mm3"],
                name="Storage (Mm3)",
                marker_color="#1565C0" if island == "NI" else "#2E7D32",
                opacity=0.75,
            )
        )
        fig.update_yaxes(title_text="Total storage (Mm3)")

    fig.update_layout(title=f"{island} monthly hydro storage", legend={"orientation": "h", "y": 1.08})
    apply_layout(fig, height=390)
    st.plotly_chart(fig, width="stretch")

st.subheader("Storage vs price")
scatter_df = df.dropna(subset=["avg_total_storage_mm3", "avg_price_nzd_mwh"])

if scatter_df.empty:
    st.info("No months have both hydro storage and price data in the selected period.")
else:
    fig_scatter = px.scatter(
        scatter_df,
        x="avg_total_storage_mm3",
        y="avg_price_nzd_mwh",
        color="island",
        hover_data=["year_month"],
        labels={
            "avg_total_storage_mm3": "Total island storage (Mm3)",
            "avg_price_nzd_mwh": "Average wholesale price ($/MWh)",
            "island": "Island",
        },
        color_discrete_map={"NI": "#1565C0", "SI": "#EF6C00"},
    )
    apply_layout(fig_scatter, height=400)
    st.plotly_chart(fig_scatter, width="stretch")

    corr_cols = st.columns(max(1, len(sel_islands)))
    for i, island in enumerate(sel_islands):
        idf = scatter_df[scatter_df["island"] == island]
        if len(idf) >= 3:
            r = idf["avg_total_storage_mm3"].corr(idf["avg_price_nzd_mwh"])
            direction = "expected inverse" if r < -0.1 else "positive" if r > 0.1 else "weak"
            corr_cols[i].metric(f"{island} correlation", f"{r:.3f}", direction)
        else:
            corr_cols[i].metric(f"{island} correlation", "N/A", "< 3 points")

with st.expander("Individual lake storage curves"):
    try:
        lake_df = load_hydro_storage_detail()
        if lake_df.empty:
            st.info("No per-lake storage detail is available.")
        else:
            if sel_islands:
                lake_df = lake_df[lake_df["island"].isin(sel_islands)]
            fig_lake = px.line(
                lake_df,
                x="trading_date",
                y="active_storage_mm3",
                color="catchment_name",
                facet_col="island",
                labels={
                    "active_storage_mm3": "Active storage (Mm3)",
                    "trading_date": "Date",
                    "catchment_name": "Lake",
                },
                height=420,
            )
            fig_lake.update_layout(legend={"orientation": "h", "y": -0.2})
            st.plotly_chart(fig_lake, width="stretch")
    except Exception:
        st.info("Per-lake storage detail is not loaded in this environment.")

st.caption("Storage is measured in million cubic metres. A negative correlation means low storage tends to align with high prices.")

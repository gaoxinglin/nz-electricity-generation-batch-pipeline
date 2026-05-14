"""Q10: Hydro-Price Driver — storage levels as a leading indicator for wholesale prices."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import streamlit as st
from charts import apply_layout
from loader import load_hydro_price_driver, load_hydro_storage_detail

st.title("💧 Hydro-Price Driver")
st.caption(
    "NZ generates ~55–60% of electricity from hydro. When lake storage is low, "
    "thermal generation fills the gap and wholesale prices tend to rise. "
    "**Grain:** monthly island total storage (Mm³) vs. average wholesale price ($/MWh)."
)

# ─── load data ────────────────────────────────────────────────────────────────
try:
    df = load_hydro_price_driver()
except Exception as exc:
    st.error(f"Could not load hydro data: {exc}")
    st.stop()

if df.empty:
    st.warning(
        "No hydro data loaded yet. Run:\n"
        "```\npython scripts/download_hydro.py --output data/raw/\n"
        "python scripts/load_local.py --db data/nzeg.duckdb --source data/raw/\n"
        "```\nthen re-run `dbt run`."
    )
    st.stop()

df["year_month_date"] = pd.to_datetime(df["year_month"], format="%Y%m")

# ─── filters ──────────────────────────────────────────────────────────────────
islands = sorted(df["island"].dropna().unique())
sel_islands = st.multiselect("Island", islands, default=islands, key="island_sel")
df = df[df["island"].isin(sel_islands)]

if df.empty:
    st.info("No data for the selected filters.")
    st.stop()

has_price = df["avg_price_nzd_mwh"].notna().any()

# ─── KPI strip ────────────────────────────────────────────────────────────────
latest = df.sort_values("year_month").groupby("island").last().reset_index()
kpi_cols = st.columns(len(latest) * 2)
for idx, row in latest.iterrows():
    k = idx * 2
    storage_val = f"{row['avg_total_storage_mm3']:,.0f} Mm³" if pd.notna(row["avg_total_storage_mm3"]) else "N/A"
    pct_val     = f"{row['storage_pct_of_max']:.0f}% of max" if pd.notna(row["storage_pct_of_max"]) else None
    price_val   = f"${row['avg_price_nzd_mwh']:.0f}/MWh" if pd.notna(row.get("avg_price_nzd_mwh")) else "N/A"
    kpi_cols[k].metric(f"{row['island']} Storage", storage_val, delta=pct_val)
    kpi_cols[k + 1].metric(f"{row['island']} Avg Price", price_val)

if not has_price:
    st.info(
        "Price data not yet available for the hydro date range (hydro ends 2024-12-31; "
        "only demo price month loaded). Run `make local-full` for full correlation."
    )

st.divider()

# ─── time series per island: storage bar + price line overlay ─────────────────
st.subheader("Monthly storage and wholesale price")

for island in sel_islands:
    idf = df[df["island"] == island].sort_values("year_month_date")
    if idf.empty:
        continue

    if has_price and idf["avg_price_nzd_mwh"].notna().any():
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(
            go.Bar(
                x=idf["year_month_date"],
                y=idf["avg_total_storage_mm3"],
                name="Storage (Mm³)",
                marker_color="#2196F3" if island == "NI" else "#4CAF50",
                opacity=0.75,
            ),
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(
                x=idf["year_month_date"],
                y=idf["avg_price_nzd_mwh"],
                name="Avg price ($/MWh)",
                line={"color": "#FF5722", "width": 2},
                mode="lines+markers",
                marker={"size": 4},
            ),
            secondary_y=True,
        )
        fig.update_yaxes(title_text="Total Storage (Mm³)", secondary_y=False)
        fig.update_yaxes(title_text="Avg Price ($/MWh)", secondary_y=True)
    else:
        fig = go.Figure(
            go.Bar(
                x=idf["year_month_date"],
                y=idf["avg_total_storage_mm3"],
                name="Storage (Mm³)",
                marker_color="#2196F3" if island == "NI" else "#4CAF50",
                opacity=0.75,
            )
        )
        fig.update_yaxes(title_text="Total Storage (Mm³)")

    fig.update_layout(
        title=f"{island} — Monthly Hydro Storage{'  +  Wholesale Price' if has_price else ''}",
        legend={"orientation": "h", "y": 1.08},
    )
    apply_layout(fig, height=390)
    st.plotly_chart(fig, use_container_width=True)

# ─── scatter: storage vs price ────────────────────────────────────────────────
st.divider()
st.subheader("Storage vs price (correlation)")

scatter_df = df.dropna(subset=["avg_total_storage_mm3", "avg_price_nzd_mwh"])

if scatter_df.empty:
    st.info(
        "No months with both hydro and price data. "
        "Hydro storage covers 2016–2024; price data loaded so far does not overlap. "
        "Run `make local-full` to download full-history price data."
    )
else:
    fig_scatter = px.scatter(
        scatter_df,
        x="avg_total_storage_mm3",
        y="avg_price_nzd_mwh",
        color="island",
        hover_data=["year_month"],
        labels={
            "avg_total_storage_mm3": "Total island storage (Mm³)",
            "avg_price_nzd_mwh":     "Avg wholesale price ($/MWh)",
        },
        color_discrete_map={"NI": "#1565C0", "SI": "#EF6C00"},
    )
    apply_layout(fig_scatter, height=400)
    st.plotly_chart(fig_scatter, use_container_width=True)

    # Per-island Pearson correlation
    corr_cols = st.columns(len(sel_islands))
    for i, island in enumerate(sel_islands):
        idf = scatter_df[scatter_df["island"] == island]
        if len(idf) >= 3:
            r = idf["avg_total_storage_mm3"].corr(idf["avg_price_nzd_mwh"])
            direction = "↓ as expected" if r < -0.1 else ("↑ unexpected" if r > 0.1 else "≈ no clear trend")
            corr_cols[i].metric(f"{island} Pearson r", f"{r:.3f}", delta=direction)
        else:
            corr_cols[i].metric(f"{island} Pearson r", "N/A", delta="< 3 data points")

# ─── per-lake detail ──────────────────────────────────────────────────────────
st.divider()
st.subheader("Per-lake storage detail")

with st.expander("Show individual lake storage curves"):
    try:
        lake_df = load_hydro_storage_detail()
        if lake_df.empty:
            st.info("No per-lake detail available.")
        else:
            lake_df = lake_df[lake_df["island"].isin(sel_islands)]
            fig_lake = px.line(
                lake_df,
                x="trading_date",
                y="active_storage_mm3",
                color="catchment_name",
                facet_col="island",
                labels={
                    "active_storage_mm3": "Active storage (Mm³)",
                    "trading_date": "Date",
                    "catchment_name": "Lake",
                },
                height=420,
            )
            fig_lake.update_layout(legend={"orientation": "h", "y": -0.2})
            st.plotly_chart(fig_lake, use_container_width=True)
    except Exception as exc:
        st.warning(f"Could not load per-lake detail: {exc}")

st.caption(
    "Storage data: EMI Hydrological Modelling Dataset (latest release 2024-12-31). "
    "Price data: EMI Final Energy Prices. "
    "Negative Pearson r = low storage → high price (the expected relationship)."
)

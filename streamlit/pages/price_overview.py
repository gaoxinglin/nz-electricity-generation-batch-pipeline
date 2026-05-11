"""Price Overview — daily wholesale price summary across NZ POCs."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

import streamlit as st
from charts import apply_layout
from loader import load_price_daily

st.title("⚡ Price Overview")
st.caption(
    "Daily $/MWh statistics per Point Of Connection, derived from "
    "`mart_price_daily`. Source: EMI Final Energy Prices."
)

df = load_price_daily()
if df.empty:
    st.info("No price data available — load `raw_price` first.")
    st.stop()


# ─── filters ────────────────────────────────────────────────────
c1, c2 = st.columns([2, 3])
with c1:
    islands_available = sorted(df["island"].dropna().unique().tolist())
    pick_islands = st.multiselect(
        "Island", islands_available, default=islands_available
    )
with c2:
    date_min, date_max = df["trading_date"].min(), df["trading_date"].max()
    date_range = st.date_input(
        "Date range", value=(date_min, date_max),
        min_value=date_min, max_value=date_max,
    )
    if isinstance(date_range, tuple) and len(date_range) == 2:
        d0, d1 = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
    else:
        d0, d1 = date_min, date_max

mask = df["trading_date"].between(d0, d1)
if pick_islands:
    mask &= df["island"].isin(pick_islands)
view = df[mask]
if view.empty:
    st.warning("No rows match those filters.")
    st.stop()


# ─── KPI strip ─────────────────────────────────────────────────
total_poc_days = len(view)
mean_price = view["avg_price_all"].mean()
max_price = view["max_price"].max()
neg_tp_total = int(view["negative_tp_count"].sum())
spike_tp_total = int(view["spike_tp_count"].sum())

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("POC-days", f"{total_poc_days:,}")
k2.metric("Avg price", f"${mean_price:,.1f}/MWh")
k3.metric("Peak price", f"${max_price:,.1f}/MWh")
k4.metric("Spike TPs", f"{spike_tp_total:,}")
k5.metric("Negative TPs", f"{neg_tp_total:,}")


# ─── daily mean trend (line per island) ────────────────────────
st.subheader("Daily mean price by island")
daily = (
    view.groupby(["trading_date", "island"], dropna=False)["avg_price_all"]
    .mean()
    .reset_index()
)
fig = px.line(
    daily,
    x="trading_date", y="avg_price_all", color="island",
    labels={"avg_price_all": "$/MWh", "trading_date": "Date"},
)
apply_layout(fig, height=380)
st.plotly_chart(fig, use_container_width=True)


# ─── price distribution histogram ──────────────────────────────
st.subheader("Daily-mean price distribution")
fig2 = px.histogram(
    view, x="avg_price_all", color="island", nbins=40, opacity=0.7,
    labels={"avg_price_all": "$/MWh", "count": "POC-days"},
    barmode="overlay",
)
apply_layout(fig2, height=320)
st.plotly_chart(fig2, use_container_width=True)


# ─── spike / negative TP counts ────────────────────────────────
col_a, col_b = st.columns(2)
with col_a:
    st.subheader("Spike-TP density (count per day)")
    sp = view.groupby("trading_date")["spike_tp_count"].sum().reset_index()
    fig3 = px.bar(sp, x="trading_date", y="spike_tp_count",
                  labels={"spike_tp_count": "TPs > $300/MWh"})
    fig3.update_traces(marker_color="#E53935")
    apply_layout(fig3, height=300)
    st.plotly_chart(fig3, use_container_width=True)

with col_b:
    st.subheader("Negative-price TPs (count per day)")
    ng = view.groupby("trading_date")["negative_tp_count"].sum().reset_index()
    if ng["negative_tp_count"].sum() == 0:
        st.info("No negative-price TPs in selected window.")
    fig4 = px.bar(ng, x="trading_date", y="negative_tp_count",
                  labels={"negative_tp_count": "TPs < $0/MWh"})
    fig4.update_traces(marker_color="#1E88E5")
    apply_layout(fig4, height=300)
    st.plotly_chart(fig4, use_container_width=True)


st.caption(
    "Spike threshold $300/MWh and negative threshold $0/MWh are configured "
    "via dbt vars in `dbt_project.yml`."
)

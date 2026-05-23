"""Renewable vs Price — does a greener mix actually mean cheaper power?"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
from charts import apply_layout
from loader import load_renewable_price_impact

import streamlit as st

st.title("🌱 Renewable share vs wholesale price")
st.caption(
    "Per (POC × date × TP): renewable share of co-located generation paired "
    "with the wholesale price. From `mart_renewable_price_impact`."
)

df = load_renewable_price_impact()
if df.empty:
    st.info("No data — load price + generation marts first.")
    st.stop()


# ─── filters ────────────────────────────────────────────────────
c1, c2 = st.columns([2, 3])
with c1:
    islands_available = sorted(df["island"].dropna().unique().tolist())
    pick_islands = st.multiselect(
        "Island", islands_available, default=islands_available
    )
with c2:
    d0, d1 = df["trading_date"].min(), df["trading_date"].max()
    date_range = st.date_input(
        "Date range", value=(d0, d1), min_value=d0, max_value=d1
    )
    if isinstance(date_range, tuple) and len(date_range) == 2:
        d0, d1 = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])

mask = df["trading_date"].between(d0, d1)
if pick_islands:
    mask &= df["island"].isin(pick_islands)
view = df[mask].dropna(subset=["renewable_pct"])
if view.empty:
    st.warning("No rows after filtering.")
    st.stop()


# ─── KPI strip ─────────────────────────────────────────────────
mean_renew = view["renewable_pct"].mean()
mean_price = view["price_nzd_mwh"].mean()
# Pearson correlation (note: bounded variable, interpret cautiously)
corr = view[["renewable_pct", "price_nzd_mwh"]].corr().iloc[0, 1]

k1, k2, k3, k4 = st.columns(4)
k1.metric("TPs covered", f"{len(view):,}")
k2.metric("Avg renewable %", f"{mean_renew:,.1f}%")
k3.metric("Avg price", f"${mean_price:,.1f}/MWh")
k4.metric("Pearson r",
          f"{corr:+.2f}",
          help="Correlation between renewable share and price across "
               "all TP-level observations.")


# ─── renewable-band bar chart ─────────────────────────────────
st.subheader("Average price by renewable-share band")


def _band(p):
    if pd.isna(p):
        return "no gen"
    if p >= 100:
        return "100% renew"
    if p >= 75:
        return "75–99%"
    if p >= 50:
        return "50–74%"
    if p >= 25:
        return "25–49%"
    if p > 0:
        return "1–24%"
    return "0%"


bands = ["0%", "1–24%", "25–49%", "50–74%", "75–99%", "100% renew"]
view = view.assign(band=view["renewable_pct"].map(_band))
band_stats = (
    view.groupby("band")
        .agg(n_tps=("price_nzd_mwh", "size"),
             avg_price=("price_nzd_mwh", "mean"))
        .reindex(bands).dropna(how="all").reset_index()
)
fig = px.bar(
    band_stats, x="band", y="avg_price",
    text=band_stats["avg_price"].round(0).map("${:.0f}".format),
    labels={"band": "renewable share", "avg_price": "avg $/MWh"},
)
fig.update_traces(marker_color="#4CAF50", textposition="outside")
apply_layout(fig, height=380)
st.plotly_chart(fig, use_container_width=True)
st.caption(
    f"Across {len(view):,} TP observations, the relationship is non-monotonic — "
    "the mid-renewable band (50-74%) often sits *higher* than fully renewable "
    "POCs, suggesting price is driven more by location and load than by green-share."
)


# ─── scatter ──────────────────────────────────────────────────
st.subheader("Renewable% × price (sampled)")
sample = view.sample(min(8000, len(view)), random_state=42)
fig2 = px.scatter(
    sample, x="renewable_pct", y="price_nzd_mwh", color="island",
    opacity=0.35,
    labels={"renewable_pct": "renewable share %", "price_nzd_mwh": "$/MWh"},
)
fig2.update_traces(marker=dict(size=4))
apply_layout(fig2, height=460)
st.plotly_chart(fig2, use_container_width=True)

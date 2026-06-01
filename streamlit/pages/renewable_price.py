"""Relationship between renewable generation share and wholesale price."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import plotly.express as px
from charts import apply_layout
from loader import load_renewable_price_impact
from ui import date_range_filter, fmt_int, fmt_pct, fmt_price, insight_box, multiselect_all, page_header

import streamlit as st

page_header(
    "Renewables and Price",
    "Do higher local renewable shares line up with lower wholesale prices?",
    "This is an exploratory relationship. Prices also depend on demand, constraints, offers, and node location.",
)

try:
    df = load_renewable_price_impact()
except Exception:
    st.error("Could not load the renewable-price summary for this environment.")
    st.stop()

if df.empty:
    st.info("No renewable-price data is available. Load both generation and price data, then run the transformation job.")
    st.stop()

control_a, control_b = st.columns([2, 3])
with control_a:
    pick_islands = multiselect_all("Island", df["island"].dropna().unique().tolist(), key="renew_price_islands")
with control_b:
    d0, d1 = date_range_filter(df, "trading_date", key="renew_price_dates")

mask = df["trading_date"].between(d0, d1)
if pick_islands:
    mask &= df["island"].isin(pick_islands)
view = df[mask].dropna(subset=["renewable_pct", "price_nzd_mwh"]).copy()

if view.empty:
    st.warning("No renewable-price rows match the selected filters.")
    st.stop()

mean_renew = view["renewable_pct"].mean()
mean_price = view["price_nzd_mwh"].mean()
corr = view[["renewable_pct", "price_nzd_mwh"]].corr().iloc[0, 1] if len(view) >= 2 else pd.NA

k1, k2, k3, k4 = st.columns(4)
k1.metric("Trading periods covered", fmt_int(len(view)))
k2.metric("Average renewable share", fmt_pct(mean_renew))
k3.metric("Average price", fmt_price(mean_price, 1))
k4.metric("Correlation", f"{corr:+.2f}" if pd.notna(corr) else "N/A", help="Pearson correlation for the selected rows.")

insight_box(
    "Interpretation guardrail",
    "A negative correlation supports the idea that more renewable output can coincide with lower prices, but this view does not prove causation.",
)


def _band(value: float) -> str:
    if pd.isna(value):
        return "No generation"
    if value >= 100:
        return "100%"
    if value >= 75:
        return "75-99%"
    if value >= 50:
        return "50-74%"
    if value >= 25:
        return "25-49%"
    if value > 0:
        return "1-24%"
    return "0%"


st.subheader("Average price by renewable-share band")
bands = ["0%", "1-24%", "25-49%", "50-74%", "75-99%", "100%"]
view = view.assign(band=view["renewable_pct"].map(_band))
band_stats = (
    view.groupby("band")
    .agg(n_tps=("price_nzd_mwh", "size"), avg_price=("price_nzd_mwh", "mean"))
    .reindex(bands)
    .dropna(how="all")
    .reset_index()
)
fig = px.bar(
    band_stats,
    x="band",
    y="avg_price",
    text=band_stats["avg_price"].round(0).map("${:.0f}".format),
    labels={"band": "Renewable share band", "avg_price": "Average $/MWh"},
)
fig.update_traces(marker_color="#2E7D32", textposition="outside")
apply_layout(fig, height=380)
st.plotly_chart(fig, width="stretch")

st.subheader("Renewable share vs price sample")
sample = view.sample(min(8000, len(view)), random_state=42)
fig2 = px.scatter(
    sample,
    x="renewable_pct",
    y="price_nzd_mwh",
    color="island",
    opacity=0.35,
    labels={"renewable_pct": "Renewable share %", "price_nzd_mwh": "$/MWh", "island": "Island"},
)
fig2.update_traces(marker=dict(size=4))
apply_layout(fig2, height=460)
st.plotly_chart(fig2, width="stretch")

with st.expander("Band details"):
    st.dataframe(
        band_stats.rename(
            columns={
                "band": "Renewable share band",
                "n_tps": "Trading periods",
                "avg_price": "Average price $/MWh",
            }
        ),
        width="stretch",
        hide_index=True,
    )

"""Island Spread — North vs South Island wholesale price comparison."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

import streamlit as st
from charts import apply_layout
from loader import load_price_daily

st.title("🏝️ Island Spread")
st.caption(
    "Daily mean NI vs SI price (and the spread). Built on `mart_price_daily` "
    "with a GROUP BY island — this is the V4.2 reduction of the original "
    "`mart_island_price_spread` mart (PRD §5.2)."
)

df = load_price_daily().dropna(subset=["island"])
if df.empty:
    st.info("No price data with island attribution — load NSP first.")
    st.stop()


# ─── filter to date range ──────────────────────────────────────
d0, d1 = df["trading_date"].min(), df["trading_date"].max()
date_range = st.date_input("Date range", value=(d0, d1), min_value=d0, max_value=d1)
if isinstance(date_range, tuple) and len(date_range) == 2:
    d0, d1 = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
view = df[df["trading_date"].between(d0, d1)]

# Aggregate to island × date
island_daily = (
    view.groupby(["trading_date", "island"])["avg_price_all"]
    .mean()
    .reset_index()
)
pivot = island_daily.pivot(index="trading_date", columns="island", values="avg_price_all")

if "NI" not in pivot.columns or "SI" not in pivot.columns:
    st.warning("Need both NI and SI in the date range for spread analysis.")
    st.stop()

pivot["spread"] = pivot["NI"] - pivot["SI"]


# ─── KPI strip ────────────────────────────────────────────────
mean_ni = pivot["NI"].mean()
mean_si = pivot["SI"].mean()
mean_spread = pivot["spread"].mean()
max_abs_spread = pivot["spread"].abs().max()

k1, k2, k3, k4 = st.columns(4)
k1.metric("Avg NI price", f"${mean_ni:,.1f}/MWh")
k2.metric("Avg SI price", f"${mean_si:,.1f}/MWh")
k3.metric("Mean spread (NI−SI)",
          f"${mean_spread:+,.1f}/MWh",
          delta=f"{100 * mean_spread / mean_si:+.1f}% of SI" if mean_si else None)
k4.metric("Max |spread|", f"${max_abs_spread:,.1f}/MWh")


# ─── side-by-side daily means ─────────────────────────────────
st.subheader("Daily mean by island")
fig = px.line(
    island_daily, x="trading_date", y="avg_price_all", color="island",
    labels={"avg_price_all": "$/MWh", "trading_date": "Date"},
    color_discrete_map={"NI": "#1565C0", "SI": "#EF6C00"},
)
apply_layout(fig, height=380)
st.plotly_chart(fig, use_container_width=True)


# ─── spread bar chart ─────────────────────────────────────────
st.subheader("Daily spread (NI − SI)")
spread_df = pivot.reset_index()[["trading_date", "spread"]]
fig2 = go.Figure(go.Bar(
    x=spread_df["trading_date"], y=spread_df["spread"],
    marker_color=["#1565C0" if v >= 0 else "#EF6C00" for v in spread_df["spread"]],
))
fig2.update_layout(
    yaxis_title="$/MWh (NI − SI)", xaxis_title="Date",
)
apply_layout(fig2, height=320)
st.plotly_chart(fig2, use_container_width=True)


# ─── spread distribution ──────────────────────────────────────
st.subheader("Spread distribution")
fig3 = px.histogram(
    spread_df, x="spread", nbins=30,
    labels={"spread": "NI − SI ($/MWh)"},
)
fig3.update_traces(marker_color="#5C6BC0")
apply_layout(fig3, height=280)
st.plotly_chart(fig3, use_container_width=True)

st.caption(
    "Positive spread = NI more expensive than SI. NZ's HVDC inter-island link "
    "(Cook Strait) keeps spreads usually narrow; persistent positive spreads "
    "typically indicate HVDC export constraints from SI."
)

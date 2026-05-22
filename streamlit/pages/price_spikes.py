"""Price Spikes — TP-level spike events with co-located fuel mix."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
from charts import apply_layout
from loader import load_price_spikes

import streamlit as st

st.title("🔺 Price Spikes")
st.caption(
    "Trading-period events where `price_nzd_mwh > $300/MWh`, joined to "
    "co-located generation if any. From `mart_price_spike_events`."
)

df = load_price_spikes()
if df.empty:
    st.info("No spike events available.")
    st.stop()


# ─── filters ───────────────────────────────────────────────────
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
view = df[mask]
if view.empty:
    st.warning("No rows match those filters.")
    st.stop()


# ─── KPI strip ─────────────────────────────────────────────────
n_events = len(view)
unmatched = int(view["unmatched_generation"].sum())
unmatched_pct = 100.0 * unmatched / n_events if n_events else 0
top_poc_count = view["poc_code"].value_counts().head(1)
top_poc = top_poc_count.index[0] if len(top_poc_count) else "—"

k1, k2, k3, k4 = st.columns(4)
k1.metric("Spike events", f"{n_events:,}")
k2.metric("Distinct POCs hit", f"{view['poc_code'].nunique():,}")
k3.metric("Peak price", f"${view['price_nzd_mwh'].max():,.0f}/MWh")
k4.metric("Unmatched-gen %",
          f"{unmatched_pct:.0f}%",
          help="Load-only POCs see prices but have no co-located generation.")


# ─── coverage advisory ────────────────────────────────────────
if unmatched_pct > 50:
    st.warning(
        f"**{unmatched_pct:.0f}%** of spikes are at load-only POCs (no "
        "co-located generator). Fuel-mix charts below reflect the subset "
        "where generation data exists — typically grid-edge / industrial POCs."
    )


# ─── daily spike count heatmap (day × TP) ──────────────────────
st.subheader("Spike density: day × trading period")
heat = (
    view.assign(day=view["trading_date"].dt.day)
        .groupby(["day", "tp_number"]).size().reset_index(name="n")
        .pivot(index="day", columns="tp_number", values="n")
        .fillna(0)
)
if not heat.empty:
    fig = px.imshow(
        heat, aspect="auto", color_continuous_scale="Reds",
        labels=dict(x="Trading Period", y="Day of month", color="spike count"),
    )
    apply_layout(fig, height=420)
    st.plotly_chart(fig, use_container_width=True)


# ─── top POCs hit by spikes ────────────────────────────────────
col_a, col_b = st.columns(2)
with col_a:
    st.subheader("Top 20 POCs by spike-TP count")
    top = (view.groupby(["poc_code", "island"]).size().reset_index(name="spike_count")
                .sort_values("spike_count", ascending=False).head(20))
    fig_t = px.bar(
        top.sort_values("spike_count"),
        x="spike_count", y="poc_code", color="island",
        orientation="h",
        labels={"spike_count": "spike-TPs", "poc_code": "POC"},
    )
    apply_layout(fig_t, height=480)
    st.plotly_chart(fig_t, use_container_width=True)

with col_b:
    st.subheader("Fuel mix when spike hits (matched POCs only)")
    matched = view[~view["unmatched_generation"]].copy()
    if matched.empty:
        st.info("No matched-generation spike events in this window.")
    else:
        matched["renewable_kwh"] = matched["renewable_generation_kwh"].fillna(0)
        matched["thermal_kwh"] = matched["thermal_generation_kwh"].fillna(0)
        fuel_long = matched.melt(
            id_vars=["trading_date"],
            value_vars=["renewable_kwh", "thermal_kwh"],
            var_name="source", value_name="kwh",
        )
        daily_fuel = fuel_long.groupby(["trading_date", "source"])["kwh"].sum().reset_index()
        fig_f = px.area(
            daily_fuel, x="trading_date", y="kwh", color="source",
            labels={"kwh": "kWh"},
            color_discrete_map={"renewable_kwh": "#4CAF50", "thermal_kwh": "#9E9E9E"},
        )
        apply_layout(fig_f, height=480)
        st.plotly_chart(fig_f, use_container_width=True)

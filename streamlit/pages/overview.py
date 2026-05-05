import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import plotly.express as px
from charts import apply_layout, color_map, fmt_month, sparkline
from loader import load_monthly, load_renewable

import streamlit as st

st.title("NZ Electricity — Overview")

with st.spinner("Fetching data…"):
    monthly = load_monthly()
    renewable = load_renewable()

if monthly.empty:
    st.error("No generation data found.")
    st.stop()

# ── Derive latest + prior-year context ───────────────────────────────────────
latest_ym = monthly["year_month"].max()
latest_date = pd.to_datetime(latest_ym, format="%Y%m")
prior_ym = (latest_date - pd.DateOffset(years=1)).strftime("%Y%m")

latest_total = monthly.loc[monthly["year_month"] == latest_ym, "total_generation_gwh"].sum()
prior_total  = monthly.loc[monthly["year_month"] == prior_ym,  "total_generation_gwh"].sum()
yoy_pct = ((latest_total - prior_total) / prior_total * 100) if prior_total else 0.0

ren_latest = renewable.loc[renewable["year_month"] == latest_ym, "renewable_pct"]
ren_prior  = renewable.loc[renewable["year_month"] == prior_ym,  "renewable_pct"]
ren_pct    = float(ren_latest.iloc[0]) if not ren_latest.empty else 0.0
ren_delta  = ren_pct - (float(ren_prior.iloc[0]) if not ren_prior.empty else 0.0)

latest_gen_count = int(monthly.loc[monthly["year_month"] == latest_ym, "generator_count"].sum())

# ── KPI cards ─────────────────────────────────────────────────────────────────
st.subheader(f"Latest data: {fmt_month(latest_ym)}", divider="gray")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Generation",   f"{latest_total:,.1f} GWh",  f"{yoy_pct:+.1f}% YoY")
c2.metric("Renewable Share",    f"{ren_pct:.1f}%",           f"{ren_delta:+.1f} pp YoY")
c3.metric("Active Generators",  f"{latest_gen_count:,}")
c4.metric("Data Through",       fmt_month(latest_ym))

# ── Sparklines (last 24 months) ───────────────────────────────────────────────
recent_total = (
    monthly.groupby("year_month")["total_generation_gwh"]
    .sum()
    .sort_index()
    .tail(24)
)
recent_ren = (
    renewable.sort_values("year_month")["renewable_pct"]
    .tail(24)
    .reset_index(drop=True)
)

sp1, sp2, _, _ = st.columns(4)
with sp1:
    st.caption("Total GWh — 24 months")
    st.plotly_chart(sparkline(recent_total, "#2196F3"),
                    use_container_width=True, config={"displayModeBar": False})
with sp2:
    st.caption("Renewable % — 24 months")
    st.plotly_chart(sparkline(recent_ren, "#4CAF50"),
                    use_container_width=True, config={"displayModeBar": False})

# ── Fuel mix donut ─────────────────────────────────────────────────────────────
col_left, col_right = st.columns([1, 1])

with col_left:
    st.subheader(f"Generation Mix — {fmt_month(latest_ym)}")
    mix = monthly[monthly["year_month"] == latest_ym][
        ["fuel_type", "total_generation_gwh"]
    ].sort_values("total_generation_gwh", ascending=False)

    fig_donut = px.pie(
        mix,
        values="total_generation_gwh",
        names="fuel_type",
        hole=0.45,
        color="fuel_type",
        color_discrete_map=color_map(mix["fuel_type"].tolist()),
        labels={"total_generation_gwh": "GWh", "fuel_type": "Fuel"},
    )
    fig_donut = apply_layout(fig_donut, height=400)
    st.plotly_chart(fig_donut, use_container_width=True)

# ── Last 12 months stacked bar ────────────────────────────────────────────────
with col_right:
    st.subheader("Last 12 Months by Fuel")
    last12_yms = sorted(monthly["year_month"].unique())[-12:]
    last12 = monthly[monthly["year_month"].isin(last12_yms)].copy()
    last12["label"] = last12["year_month"].apply(fmt_month)

    all_fuels = last12["fuel_type"].unique().tolist()
    fig_bar = px.bar(
        last12.sort_values(["year_month", "fuel_type"]),
        x="label",
        y="total_generation_gwh",
        color="fuel_type",
        barmode="stack",
        color_discrete_map=color_map(all_fuels),
        labels={"label": "Month", "total_generation_gwh": "GWh", "fuel_type": "Fuel"},
    )
    fig_bar = apply_layout(fig_bar, height=400)
    st.plotly_chart(fig_bar, use_container_width=True)

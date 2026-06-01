from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import plotly.express as px
from charts import apply_layout, color_map, sparkline
from loader import load_dbt_runs, load_monthly, load_price_daily, load_price_spikes, load_ranking, load_renewable
from ui import (
    fmt_delta,
    fmt_gwh,
    fmt_int,
    fmt_month,
    fmt_pct,
    fmt_price,
    insight_box,
    page_header,
    scope_bar,
)

import streamlit as st

page_header(
    "Executive Overview",
    "What does this electricity data product prove, and what is the latest market read?",
    "Use this page as the non-technical briefing. The drill-down pages explain each point in more detail.",
)


def _try_load(loader):
    try:
        return loader()
    except Exception:
        return pd.DataFrame()


with st.spinner("Loading mart-layer data..."):
    monthly = _try_load(load_monthly)
    renewable = _try_load(load_renewable)
    ranking = _try_load(load_ranking)
    price_daily = _try_load(load_price_daily)
    price_spikes = _try_load(load_price_spikes)
    dbt_runs = _try_load(load_dbt_runs)

if monthly.empty:
    st.error("No generation summary data is available. Run the local demo or transformation job before opening the dashboard.")
    st.stop()

latest_ym = str(monthly["year_month"].max())
latest_month = monthly[monthly["year_month"] == latest_ym].copy()
latest_total = latest_month["total_generation_gwh"].sum()
latest_fuel = (
    latest_month.groupby("fuel_type")["total_generation_gwh"].sum().sort_values(ascending=False)
)
top_fuel = latest_fuel.index[0] if not latest_fuel.empty else "N/A"
top_fuel_share = latest_fuel.iloc[0] / latest_total * 100 if latest_total else pd.NA

prior_ym = (pd.to_datetime(latest_ym, format="%Y%m") - pd.DateOffset(years=1)).strftime("%Y%m")
prior_total = monthly.loc[monthly["year_month"] == prior_ym, "total_generation_gwh"].sum()
generation_yoy = ((latest_total - prior_total) / prior_total * 100) if prior_total else pd.NA

ren_latest_row = renewable[renewable["year_month"] == latest_ym] if not renewable.empty else pd.DataFrame()
renewable_pct = ren_latest_row["renewable_pct"].iloc[0] if not ren_latest_row.empty else pd.NA

price_mean = price_daily["avg_price_all"].mean() if not price_daily.empty else pd.NA
price_peak = price_daily["max_price"].max() if not price_daily.empty else pd.NA
spike_count = len(price_spikes) if not price_spikes.empty else 0

scope = {
    "Generation data": f"{fmt_month(monthly['year_month'].min())} to {fmt_month(monthly['year_month'].max())}",
}
if not price_daily.empty:
    scope["Price data"] = f"{price_daily['trading_date'].min():%d %b %Y} to {price_daily['trading_date'].max():%d %b %Y}"
if not dbt_runs.empty:
    scope["Latest pipeline run"] = f"{dbt_runs['generated_at'].max():%d %b %Y %H:%M}"
scope_bar(scope)

st.subheader(f"Latest generation month: {fmt_month(latest_ym)}")
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total generation", fmt_gwh(latest_total), fmt_delta(generation_yoy, suffix="% YoY"))
k2.metric("Renewable share", fmt_pct(renewable_pct), help="Renewable generation divided by total generation.")
k3.metric("Largest fuel source", top_fuel, fmt_pct(top_fuel_share))
k4.metric("Average wholesale price", fmt_price(price_mean, 1), help="Mean of daily POC average prices in the loaded price window.")
k5.metric("Spike events", fmt_int(spike_count), help="Trading periods where price is above the configured spike threshold.")

story_parts = [
    f"In {fmt_month(latest_ym)}, the loaded generation dataset produced {fmt_gwh(latest_total)}.",
]
if pd.notna(renewable_pct):
    story_parts.append(f"Renewables supplied {fmt_pct(renewable_pct)} of that generation.")
if top_fuel != "N/A":
    story_parts.append(f"{top_fuel} was the largest fuel category at {fmt_pct(top_fuel_share)} of output.")
if not price_daily.empty:
    story_parts.append(
        f"The loaded wholesale price window averaged {fmt_price(price_mean, 1)}, with a peak daily POC price of {fmt_price(price_peak)}."
    )
insight_box("Market read", " ".join(story_parts))

left, right = st.columns([1.05, 1])

with left:
    st.subheader("Generation mix")
    mix = latest_month[["fuel_type", "total_generation_gwh"]].sort_values("total_generation_gwh", ascending=False)
    fig_mix = px.pie(
        mix,
        values="total_generation_gwh",
        names="fuel_type",
        hole=0.48,
        color="fuel_type",
        color_discrete_map=color_map(mix["fuel_type"].tolist()),
        labels={"total_generation_gwh": "GWh", "fuel_type": "Fuel"},
    )
    apply_layout(fig_mix, height=380)
    st.plotly_chart(fig_mix, width="stretch")

with right:
    st.subheader("Wholesale price trend")
    if price_daily.empty:
        st.info("Price summaries are not loaded in this environment.")
    else:
        daily = (
            price_daily.groupby(["trading_date", "island"], dropna=False)["avg_price_all"]
            .mean()
            .reset_index()
        )
        fig_price = px.line(
            daily,
            x="trading_date",
            y="avg_price_all",
            color="island",
            labels={"trading_date": "Date", "avg_price_all": "$/MWh", "island": "Island"},
        )
        apply_layout(fig_price, height=380)
        st.plotly_chart(fig_price, width="stretch")

spark_cols = st.columns(4)
recent_total = monthly.groupby("year_month")["total_generation_gwh"].sum().sort_index().tail(24)
spark_cols[0].caption("Generation over loaded months")
spark_cols[0].plotly_chart(sparkline(recent_total, "#1976D2"), width="stretch", config={"displayModeBar": False})
if not renewable.empty:
    recent_renewable = renewable.sort_values("year_month")["renewable_pct"].tail(24).reset_index(drop=True)
    spark_cols[1].caption("Renewable share over loaded months")
    spark_cols[1].plotly_chart(
        sparkline(recent_renewable, "#2E7D32"),
        width="stretch",
        config={"displayModeBar": False},
    )
if not price_daily.empty:
    daily_avg = price_daily.groupby("trading_date")["avg_price_all"].mean().sort_index()
    spark_cols[2].caption("Daily wholesale price")
    spark_cols[2].plotly_chart(sparkline(daily_avg, "#C62828"), width="stretch", config={"displayModeBar": False})
if not dbt_runs.empty:
    latest_invocation = dbt_runs[dbt_runs["invocation_rank_desc"] == 1]
    pass_rate = latest_invocation["is_success"].mean() * 100 if not latest_invocation.empty else pd.NA
    spark_cols[3].metric("Latest pipeline pass rate", fmt_pct(pass_rate))

st.subheader("What each section answers")
q1, q2, q3, q4 = st.columns(4)
q1.markdown("**Generation supply**  \nWhich fuels and stations are producing electricity, and how stable is the mix?")
q2.markdown("**Wholesale prices**  \nWhere are price spikes occurring, and what market signals explain them?")
q3.markdown("**Hydro storage**  \nAre lake storage conditions likely to put upward pressure on prices?")
q4.markdown("**Pipeline health**  \nCan reviewers trust the numbers, refresh cadence, and transformation tests?")

if not ranking.empty:
    latest_ranking = ranking[ranking["year_month"] == latest_ym].sort_values("monthly_rank").head(5)
    with st.expander("Top stations in the latest generation month"):
        st.dataframe(
            latest_ranking.rename(
                columns={
                    "monthly_rank": "Rank",
                    "site_code": "Station",
                    "primary_fuel_type": "Fuel",
                    "total_generation_gwh": "Generation GWh",
                }
            )[["Rank", "Station", "Fuel", "Generation GWh"]],
            hide_index=True,
            width="stretch",
        )

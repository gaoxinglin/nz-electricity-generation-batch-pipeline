"""Explain price spikes using volume and offer-curve context."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from charts import apply_layout
from loader import load_market_context_window, load_market_spike_context
from plotly.subplots import make_subplots
from ui import date_range_filter, fmt_int, fmt_mw, fmt_pct, fmt_price, insight_box, multiselect_all, page_header

import streamlit as st

SPIKE_THRESHOLD = 300

page_header(
    "Spike Drivers",
    "When prices spike, was the local market short on cheap supply, volume, or both?",
    "This page uses optional offer and reconciled-volume summaries. It stays quiet when those summaries are not loaded.",
)

try:
    df = load_market_spike_context()
except Exception:
    st.info(
        "Spike-driver context is not loaded in this environment. Run the volume and offer transformations, then return to this page."
    )
    st.stop()

if df.empty:
    st.info("No spike-driver rows are available for the loaded period.")
    st.stop()

df["has_offer_curve"] = df["total_offered_mw"].notna()
df["has_volume"] = df["offtake_mw"].notna() | df["injection_mw"].notna()

control_a, control_b, control_c = st.columns([1.4, 2.1, 2.5])
with control_a:
    pick_islands = multiselect_all("Island", df["island"].dropna().unique().tolist(), key="context_islands")
with control_b:
    d0, d1 = date_range_filter(df, "trading_date", key="context_dates")
with control_c:
    poc_options = sorted(df["poc_code"].dropna().unique().tolist())
    pick_pocs = st.multiselect("Node", poc_options, default=[], key="context_pocs")

mask = df["trading_date"].between(d0, d1)
if pick_islands:
    mask &= df["island"].isin(pick_islands)
if pick_pocs:
    mask &= df["poc_code"].isin(pick_pocs)
view = df[mask].copy()

if view.empty:
    st.warning("No spike-driver rows match the selected filters.")
    st.stop()

offer_coverage = 100.0 * view["has_offer_curve"].mean()
volume_coverage = 100.0 * view["has_volume"].mean()
avg_cheap_share = view["cheap_offer_share_below_300"].mean()

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Spike events", fmt_int(len(view)))
k2.metric("Peak price", fmt_price(view["price_nzd_mwh"].max()))
k3.metric("Median offtake", fmt_mw(view["offtake_mw"].median()))
k4.metric("Offer coverage", fmt_pct(offer_coverage, 0))
k5.metric("Cheap-offer share", fmt_pct(avg_cheap_share * 100, 0) if pd.notna(avg_cheap_share) else "N/A")

if offer_coverage < 50 or volume_coverage < 50:
    st.info(
        "Some context is sparse in this selection. That usually means the price mart is loaded but volume or offers "
        "were loaded for only part of the period."
    )
else:
    insight_box(
        "Practical read",
        "Low cheap-offer share plus high offtake points to a thin local supply stack. High price with normal offers may point to constraints or node-specific effects.",
    )

left, right = st.columns(2)
with left:
    st.subheader("Spike events by day")
    daily = view.groupby("trading_date", as_index=False).size().rename(columns={"size": "spike_events"})
    fig_daily = px.bar(
        daily,
        x="trading_date",
        y="spike_events",
        labels={"trading_date": "Date", "spike_events": "Spike events"},
    )
    fig_daily.update_traces(marker_color="#C62828")
    apply_layout(fig_daily, height=330)
    st.plotly_chart(fig_daily, width="stretch")

with right:
    st.subheader("Nodes with repeated spikes")
    top_poc = (
        view.groupby(["poc_code", "island"], dropna=False)
        .agg(spike_events=("price_nzd_mwh", "size"), peak_price=("price_nzd_mwh", "max"))
        .reset_index()
        .sort_values(["spike_events", "peak_price"], ascending=False)
        .head(15)
    )
    fig_top = px.bar(
        top_poc.sort_values("spike_events"),
        x="spike_events",
        y="poc_code",
        color="island",
        orientation="h",
        labels={"spike_events": "Spike events", "poc_code": "Node", "island": "Island"},
    )
    apply_layout(fig_top, height=330)
    st.plotly_chart(fig_top, width="stretch")

left2, right2 = st.columns(2)
with left2:
    st.subheader("Price vs local offtake")
    scatter = view.dropna(subset=["offtake_mw"])
    if scatter.empty:
        st.info("No offtake values are available for selected spikes.")
    else:
        fig_load = px.scatter(
            scatter,
            x="offtake_mw",
            y="price_nzd_mwh",
            color="island",
            hover_data=["trading_date", "tp_number", "poc_code", "total_offered_mw"],
            labels={"offtake_mw": "Offtake MW", "price_nzd_mwh": "$/MWh", "island": "Island"},
        )
        apply_layout(fig_load, height=360)
        st.plotly_chart(fig_load, width="stretch")

with right2:
    st.subheader("Price vs cheap-offer share")
    offer_scatter = view.dropna(subset=["cheap_offer_share_below_300"])
    if offer_scatter.empty:
        st.info("No offer-curve rows are available for selected spikes.")
    else:
        fig_offer = px.scatter(
            offer_scatter,
            x="cheap_offer_share_below_300",
            y="price_nzd_mwh",
            color="island",
            hover_data=["trading_date", "tp_number", "poc_code", "total_offered_mw"],
            labels={
                "cheap_offer_share_below_300": "Share of offered MW <= $300/MWh",
                "price_nzd_mwh": "$/MWh",
                "island": "Island",
            },
        )
        fig_offer.update_xaxes(tickformat=".0%")
        apply_layout(fig_offer, height=360)
        st.plotly_chart(fig_offer, width="stretch")

st.divider()
st.subheader("Event drill-down")

events = view.sort_values(
    ["trading_date", "tp_number", "price_nzd_mwh"],
    ascending=[False, True, False],
).copy()
events["event_label"] = (
    events["trading_date"].dt.strftime("%Y-%m-%d")
    + " TP"
    + events["tp_number"].astype(str).str.zfill(2)
    + " | "
    + events["poc_code"]
    + " | "
    + events["price_nzd_mwh"].map(lambda value: fmt_price(value))
)

selected_label = st.selectbox("Spike event", events["event_label"].tolist())
selected = events.loc[events["event_label"] == selected_label].iloc[0]

detail_cols = st.columns(5)
detail_cols[0].metric("Price", fmt_price(selected["price_nzd_mwh"]))
detail_cols[1].metric("Offtake", fmt_mw(selected["offtake_mw"]))
detail_cols[2].metric("Injection", fmt_mw(selected["injection_mw"]))
detail_cols[3].metric("Total offered", fmt_mw(selected["total_offered_mw"]))
detail_cols[4].metric(
    "Offer/load ratio",
    f"{selected['offered_to_offtake_ratio']:.2f}x" if pd.notna(selected["offered_to_offtake_ratio"]) else "N/A",
)

event_date = selected["trading_date"].strftime("%Y-%m-%d")
event_tp = int(selected["tp_number"])
window = load_market_context_window(selected["poc_code"], event_date, event_tp - 6, event_tp + 6)

if window.empty:
    st.info("No same-day trading-period context is available for this event.")
else:
    fig_window = make_subplots(specs=[[{"secondary_y": True}]])
    fig_window.add_trace(
        go.Bar(
            x=window["tp_number"],
            y=window["offtake_mw"],
            name="Offtake MW",
            marker_color="#90A4AE",
            opacity=0.55,
        ),
        secondary_y=False,
    )
    fig_window.add_trace(
        go.Scatter(
            x=window["tp_number"],
            y=window["total_offered_mw"],
            name="Total offered MW",
            mode="lines+markers",
            line={"color": "#2E7D32", "width": 2},
        ),
        secondary_y=False,
    )
    fig_window.add_trace(
        go.Scatter(
            x=window["tp_number"],
            y=window["price_nzd_mwh"],
            name="Price $/MWh",
            mode="lines+markers",
            line={"color": "#C62828", "width": 3},
        ),
        secondary_y=True,
    )
    fig_window.add_hline(y=SPIKE_THRESHOLD, line_dash="dot", line_color="#C62828", secondary_y=True)
    fig_window.update_yaxes(title_text="MW", secondary_y=False)
    fig_window.update_yaxes(title_text="$/MWh", secondary_y=True)
    fig_window.update_xaxes(dtick=1, title_text="Trading period")
    apply_layout(fig_window, height=420)
    st.plotly_chart(fig_window, width="stretch")

    window["cheap_offer_share_pct"] = window["cheap_offer_share_below_300"] * 100
    display_cols = [
        "tp_number",
        "price_nzd_mwh",
        "offtake_mw",
        "injection_mw",
        "total_offered_mw",
        "offered_mw_at_or_below_300",
        "weighted_avg_offer_price_nzd_mwh",
        "cheap_offer_share_pct",
        "offered_to_offtake_ratio",
    ]
    st.dataframe(
        window[display_cols],
        width="stretch",
        hide_index=True,
        column_config={
            "tp_number": st.column_config.NumberColumn("TP", format="%d"),
            "price_nzd_mwh": st.column_config.NumberColumn("Price $/MWh", format="$%.0f"),
            "offtake_mw": st.column_config.NumberColumn("Offtake MW", format="%.0f"),
            "injection_mw": st.column_config.NumberColumn("Injection MW", format="%.0f"),
            "total_offered_mw": st.column_config.NumberColumn("Total offered MW", format="%.0f"),
            "offered_mw_at_or_below_300": st.column_config.NumberColumn("Offered MW <= $300", format="%.0f"),
            "weighted_avg_offer_price_nzd_mwh": st.column_config.NumberColumn("Weighted offer $/MWh", format="$%.0f"),
            "cheap_offer_share_pct": st.column_config.ProgressColumn(
                "Cheap-offer share",
                format="%.0f%%",
                min_value=0,
                max_value=100,
            ),
            "offered_to_offtake_ratio": st.column_config.NumberColumn("Offer/load ratio", format="%.2f"),
        },
    )

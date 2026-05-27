"""Market Context — price spike explanation using volume and offer-curve marts."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from charts import apply_layout
from loader import load_market_context_window, load_market_spike_context
from plotly.subplots import make_subplots

import streamlit as st

SPIKE_THRESHOLD = 300

st.title("📉 Market Context")
st.caption(
    "Spike-level context from `mart_price_offer_context`: price, reconciled "
    "injection/offtake, and compact offer-curve features."
)

try:
    df = load_market_spike_context()
except Exception as exc:
    st.error(f"Could not load market context marts: {exc}")
    st.stop()

if df.empty:
    st.info(
        "No price spikes are available yet. Load price data and run dbt models "
        "through `mart_price_offer_context`."
    )
    st.stop()

df["has_offer_curve"] = df["total_offered_mw"].notna()
df["has_volume"] = df["offtake_mw"].notna() | df["injection_mw"].notna()


# ─── filters ────────────────────────────────────────────────────────────────
c1, c2, c3 = st.columns([1.3, 2.2, 2.5])
with c1:
    islands_available = sorted(df["island"].dropna().unique().tolist())
    pick_islands = st.multiselect("Island", islands_available, default=islands_available)
with c2:
    d0, d1 = df["trading_date"].min(), df["trading_date"].max()
    date_range = st.date_input("Date range", value=(d0, d1), min_value=d0, max_value=d1)
    if isinstance(date_range, tuple) and len(date_range) == 2:
        d0, d1 = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
with c3:
    poc_options = sorted(df["poc_code"].dropna().unique().tolist())
    pick_pocs = st.multiselect("POC", poc_options, default=[])

mask = df["trading_date"].between(d0, d1)
if pick_islands:
    mask &= df["island"].isin(pick_islands)
if pick_pocs:
    mask &= df["poc_code"].isin(pick_pocs)
view = df[mask].copy()

if view.empty:
    st.warning("No spike events match those filters.")
    st.stop()


# ─── KPI strip ──────────────────────────────────────────────────────────────
offer_coverage = 100.0 * view["has_offer_curve"].mean()
volume_coverage = 100.0 * view["has_volume"].mean()
avg_cheap_share = view["cheap_offer_share_below_300"].mean()
avg_cheap_share_txt = "N/A" if pd.isna(avg_cheap_share) else f"{avg_cheap_share:.0%}"

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Spike events", f"{len(view):,}")
k2.metric("Peak price", f"${view['price_nzd_mwh'].max():,.0f}/MWh")
k3.metric("Median offtake", f"{view['offtake_mw'].median():,.0f} MW")
k4.metric("Offer coverage", f"{offer_coverage:.0f}%")
k5.metric("Cheap-offer share", avg_cheap_share_txt, help="Share of offered MW priced <= $300/MWh.")

if offer_coverage < 50:
    st.info(
        "Offer context is sparse in this selection. That is expected when only "
        "price/volume data has been loaded, or when Offers were loaded for a "
        "sample day rather than the whole month."
    )
if volume_coverage < 50:
    st.info("Reconciled volume context is sparse in this selection.")


# ─── charts ────────────────────────────────────────────────────────────────
left, right = st.columns(2)

with left:
    st.subheader("Spike count by day")
    daily = view.groupby("trading_date").size().reset_index(name="spike_events")
    fig_daily = px.bar(
        daily,
        x="trading_date",
        y="spike_events",
        labels={"trading_date": "Date", "spike_events": "Spike events"},
    )
    fig_daily.update_traces(marker_color="#D32F2F")
    apply_layout(fig_daily, height=330)
    st.plotly_chart(fig_daily, use_container_width=True)

with right:
    st.subheader("Top POCs by spike count")
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
        labels={"spike_events": "Spike events", "poc_code": "POC"},
    )
    apply_layout(fig_top, height=330)
    st.plotly_chart(fig_top, use_container_width=True)

left2, right2 = st.columns(2)

with left2:
    st.subheader("Price vs offtake")
    scatter = view.dropna(subset=["offtake_mw"])
    if scatter.empty:
        st.info("No offtake values available for selected spikes.")
    else:
        fig_load = px.scatter(
            scatter,
            x="offtake_mw",
            y="price_nzd_mwh",
            color="island",
            hover_data=["trading_date", "tp_number", "poc_code", "total_offered_mw"],
            labels={"offtake_mw": "Offtake MW", "price_nzd_mwh": "$/MWh"},
        )
        apply_layout(fig_load, height=360)
        st.plotly_chart(fig_load, use_container_width=True)

with right2:
    st.subheader("Price vs cheap offer share")
    offer_scatter = view.dropna(subset=["cheap_offer_share_below_300"])
    if offer_scatter.empty:
        st.info("No offer-curve rows available for selected spikes.")
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
            },
        )
        fig_offer.update_xaxes(tickformat=".0%")
        apply_layout(fig_offer, height=360)
        st.plotly_chart(fig_offer, use_container_width=True)


# ─── event selector and local context ───────────────────────────────────────
st.divider()
st.subheader("Event drill-down")

events = view.sort_values(["trading_date", "tp_number", "price_nzd_mwh"], ascending=[False, True, False]).copy()
events["event_label"] = (
    events["trading_date"].dt.strftime("%Y-%m-%d")
    + " TP"
    + events["tp_number"].astype(str).str.zfill(2)
    + " · "
    + events["poc_code"]
    + " · $"
    + events["price_nzd_mwh"].round(0).astype(int).astype(str)
    + "/MWh"
)

selected_label = st.selectbox("Spike event", events["event_label"].tolist())
selected = events.loc[events["event_label"] == selected_label].iloc[0]

detail_cols = st.columns(5)
detail_cols[0].metric("Price", f"${selected['price_nzd_mwh']:,.0f}/MWh")
detail_cols[1].metric("Offtake", f"{selected['offtake_mw']:,.0f} MW" if pd.notna(selected["offtake_mw"]) else "N/A")
detail_cols[2].metric("Injection", f"{selected['injection_mw']:,.0f} MW" if pd.notna(selected["injection_mw"]) else "N/A")
detail_cols[3].metric("Total offered", f"{selected['total_offered_mw']:,.0f} MW" if pd.notna(selected["total_offered_mw"]) else "N/A")
detail_cols[4].metric(
    "Offer/load ratio",
    f"{selected['offered_to_offtake_ratio']:.2f}x" if pd.notna(selected["offered_to_offtake_ratio"]) else "N/A",
)

event_date = selected["trading_date"].strftime("%Y-%m-%d")
event_tp = int(selected["tp_number"])
window = load_market_context_window(
    selected["poc_code"],
    event_date,
    event_tp - 6,
    event_tp + 6,
)

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
            line={"color": "#00897B", "width": 2},
        ),
        secondary_y=False,
    )
    fig_window.add_trace(
        go.Scatter(
            x=window["tp_number"],
            y=window["price_nzd_mwh"],
            name="Price $/MWh",
            mode="lines+markers",
            line={"color": "#D32F2F", "width": 3},
        ),
        secondary_y=True,
    )
    fig_window.add_hline(
        y=SPIKE_THRESHOLD,
        line_dash="dot",
        line_color="#D32F2F",
        secondary_y=True,
    )
    fig_window.update_yaxes(title_text="MW", secondary_y=False)
    fig_window.update_yaxes(title_text="$/MWh", secondary_y=True)
    fig_window.update_xaxes(dtick=1, title_text="Trading period")
    apply_layout(fig_window, height=420)
    st.plotly_chart(fig_window, use_container_width=True)

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
        use_container_width=True,
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

st.caption(
    "A low cheap-offer share or low offer/load ratio around a spike usually "
    "means the local supply stack was thin or expensive. Missing offer values "
    "mean Offers have not been loaded for that trading day."
)

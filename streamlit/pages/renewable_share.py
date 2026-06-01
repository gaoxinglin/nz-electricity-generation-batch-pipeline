from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import plotly.express as px
import plotly.graph_objects as go
from charts import apply_layout
from loader import load_renewable
from ui import fmt_gwh, fmt_month, fmt_pct, insight_box, page_header

import streamlit as st

page_header(
    "Renewable Share",
    "How much of the loaded generation was renewable, and how consistent is that share?",
    "This view separates the decarbonisation signal from the broader fuel-mix chart.",
)

with st.spinner("Loading renewable ratio mart..."):
    ren = load_renewable()

if ren.empty:
    st.error("No renewable ratio data is available.")
    st.stop()

ren = ren.sort_values("year_month").copy()
ren["month"] = ren["date"]
ren["rolling"] = ren["renewable_pct"].rolling(12, min_periods=min(3, len(ren))).mean()
ren["year"] = ren["date"].dt.year

avg = ren["renewable_pct"].mean()
latest = ren.iloc[-1]
max_row = ren.loc[ren["renewable_pct"].idxmax()]
min_row = ren.loc[ren["renewable_pct"].idxmin()]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Period average", fmt_pct(avg))
c2.metric("Latest month", fmt_pct(latest["renewable_pct"]), fmt_month(latest["year_month"]))
c3.metric("Highest month", fmt_pct(max_row["renewable_pct"]), fmt_month(max_row["year_month"]))
c4.metric("Lowest month", fmt_pct(min_row["renewable_pct"]), fmt_month(min_row["year_month"]))

insight_box(
    "Business interpretation",
    "Renewable share is a mix indicator, not a reliability guarantee. Prices can still spike when local demand, network constraints, or offer stacks are tight.",
)

st.subheader("Monthly renewable share")
fig = go.Figure()
fig.add_trace(
    go.Scatter(
        x=ren["month"],
        y=ren["renewable_pct"],
        mode="lines+markers",
        name="Monthly share",
        line=dict(color="#2E7D32", width=2),
        fill="tozeroy",
        fillcolor="rgba(46,125,50,0.08)",
    )
)
if len(ren) >= 3:
    fig.add_trace(
        go.Scatter(
            x=ren["month"],
            y=ren["rolling"],
            mode="lines",
            name="Rolling average",
            line=dict(color="#1565C0", width=3),
        )
    )
fig.add_hline(
    y=avg,
    line_dash="dot",
    line_color="rgba(128,128,128,0.65)",
    annotation_text=f"Period average {avg:.1f}%",
    annotation_position="top left",
)
if ren["renewable_pct"].max() >= 90:
    fig.add_hline(
        y=90,
        line_dash="dash",
        line_color="#C62828",
        annotation_text="90% reference",
        annotation_position="bottom right",
    )
fig = apply_layout(fig, height=460)
st.plotly_chart(fig, width="stretch")

st.subheader("Annual average renewable share")
annual = ren.groupby("year", as_index=False).agg(
    renewable_pct=("renewable_pct", "mean"),
    renewable_gwh=("renewable_gwh", "sum"),
    total_gwh=("total_gwh", "sum"),
)
fig2 = px.bar(
    annual,
    x="year",
    y="renewable_pct",
    color="renewable_pct",
    color_continuous_scale=["#E3F2FD", "#2E7D32"],
    text="renewable_pct",
    labels={"year": "Year", "renewable_pct": "Average renewable share"},
)
fig2.update_traces(texttemplate="%{y:.1f}%", textposition="outside")
fig2.update_coloraxes(showscale=False)
fig2 = apply_layout(fig2, height=360)
st.plotly_chart(fig2, width="stretch")

with st.expander("Export renewable-share data"):
    export = ren[["year_month", "renewable_pct", "renewable_gwh", "total_gwh"]].rename(
        columns={
            "year_month": "Month",
            "renewable_pct": "Renewable %",
            "renewable_gwh": "Renewable GWh",
            "total_gwh": "Total GWh",
        }
    )
    st.dataframe(export, width="stretch", hide_index=True)
    st.download_button(
        label="Download renewable share CSV",
        data=export.to_csv(index=False),
        file_name="renewable_share.csv",
        mime="text/csv",
    )

st.caption(
    f"Latest renewable generation: {fmt_gwh(latest['renewable_gwh'])} out of {fmt_gwh(latest['total_gwh'])} total generation."
)

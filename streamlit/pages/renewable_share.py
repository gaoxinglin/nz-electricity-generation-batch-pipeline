import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import plotly.express as px
import plotly.graph_objects as go
from charts import apply_layout, fmt_month
from loader import load_renewable

import streamlit as st

st.title("Renewable Share")
st.caption("NZ's renewable generation as a percentage of total output.")

with st.spinner("Fetching data…"):
    ren = load_renewable()

if ren.empty:
    st.error("No data available.")
    st.stop()

ren = ren.sort_values("year_month").copy()
ren["label"]     = ren["year_month"].apply(fmt_month)
ren["rolling"]   = ren["renewable_pct"].rolling(12, min_periods=3).mean()
ren["year"]      = ren["date"].dt.year

avg     = ren["renewable_pct"].mean()
latest  = ren.iloc[-1]
max_row = ren.loc[ren["renewable_pct"].idxmax()]
min_row = ren.loc[ren["renewable_pct"].idxmin()]

# ── KPI strip ─────────────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
c1.metric("All-Time Average",  f"{avg:.1f}%")
c2.metric("Latest Month",      f"{latest['renewable_pct']:.1f}%",
          fmt_month(latest["year_month"]))
c3.metric("All-Time High",     f"{max_row['renewable_pct']:.1f}%",
          fmt_month(max_row["year_month"]))
c4.metric("All-Time Low",      f"{min_row['renewable_pct']:.1f}%",
          fmt_month(min_row["year_month"]))

# ── Timeline with rolling average ─────────────────────────────────────────────
st.subheader("Monthly Renewable Share")

fig = go.Figure()
fig.add_trace(go.Scatter(
    x=ren["label"], y=ren["renewable_pct"],
    mode="lines", name="Monthly %",
    line=dict(color="#4CAF50", width=1.5),
    fill="tozeroy", fillcolor="rgba(76,175,80,0.08)",
))
fig.add_trace(go.Scatter(
    x=ren["label"], y=ren["rolling"],
    mode="lines", name="12-Month Avg",
    line=dict(color="#00897B", width=3),
))
fig.add_hline(
    y=avg, line_dash="dot", line_color="rgba(128,128,128,0.6)",
    annotation_text=f"Avg {avg:.1f}%",
    annotation_position="top left",
)
if ren["renewable_pct"].max() >= 90:
    fig.add_hline(
        y=90, line_dash="dash", line_color="#FF5722",
        annotation_text="90% milestone",
        annotation_position="bottom right",
    )
fig = apply_layout(fig, height=460)
st.plotly_chart(fig, use_container_width=True)

# ── Annual bar chart ───────────────────────────────────────────────────────────
st.subheader("Annual Average Renewable Share")
annual = ren.groupby("year")["renewable_pct"].mean().reset_index()
fig2 = px.bar(
    annual,
    x="year", y="renewable_pct",
    color="renewable_pct",
    color_continuous_scale=["#E3F2FD", "#00897B"],
    text="renewable_pct",
    labels={"year": "Year", "renewable_pct": "Avg Renewable %"},
)
fig2.update_traces(texttemplate="%{y:.1f}%", textposition="outside")
fig2.update_coloraxes(showscale=False)
fig2 = apply_layout(fig2, height=360)
st.plotly_chart(fig2, use_container_width=True)

# ── Download ───────────────────────────────────────────────────────────────────
export = ren[["label", "renewable_pct", "renewable_gwh", "total_gwh"]].rename(columns={
    "label":         "Month",
    "renewable_pct": "Renewable %",
    "renewable_gwh": "Renewable GWh",
    "total_gwh":     "Total GWh",
})
st.download_button(
    label="⬇ Download CSV",
    data=export.to_csv(index=False),
    file_name="renewable_share.csv",
    mime="text/csv",
)

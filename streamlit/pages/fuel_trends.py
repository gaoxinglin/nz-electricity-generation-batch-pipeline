import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import plotly.graph_objects as go
import plotly.express as px
import streamlit as st

from charts import apply_layout, color_map, fmt_month
from loader import load_monthly, load_renewable

st.title("Fuel Trends")
st.caption("How has NZ's generation mix evolved over time?")

with st.spinner("Fetching data…"):
    monthly   = load_monthly()
    renewable = load_renewable()

if monthly.empty:
    st.error("No data available.")
    st.stop()

# ── Date range selectors ───────────────────────────────────────────────────────
all_months  = sorted(monthly["year_month"].unique())          # ["202001", ...]
month_labels = [fmt_month(m) for m in all_months]             # ["Jan 2020", ...]
ym_by_label  = dict(zip(month_labels, all_months))

col_a, col_b = st.columns(2)
start_label = col_a.selectbox("From", month_labels, index=0)
end_label   = col_b.selectbox("To",   month_labels, index=len(month_labels) - 1)

start_ym = ym_by_label[start_label]
end_ym   = ym_by_label[end_label]

filtered = monthly[
    (monthly["year_month"] >= start_ym) & (monthly["year_month"] <= end_ym)
].copy()
filtered["label"] = filtered["year_month"].apply(fmt_month)

if filtered.empty:
    st.warning("No data in selected range.")
    st.stop()

all_fuels = filtered["fuel_type"].unique().tolist()
colors    = color_map(all_fuels)

# ── Toggle: absolute vs % share ────────────────────────────────────────────────
mode = st.radio("View as", ["Absolute (GWh)", "% Share"], horizontal=True)

if mode == "% Share":
    totals = filtered.groupby("year_month")["total_generation_gwh"].transform("sum")
    filtered["y_val"] = filtered["total_generation_gwh"] / totals * 100
    y_label = "Share (%)"
else:
    filtered["y_val"] = filtered["total_generation_gwh"]
    y_label = "GWh"

# ── Stacked area ────────────────────────────────────────────────────────────────
st.subheader("Monthly Generation by Fuel Type")
fig = px.area(
    filtered.sort_values(["year_month", "fuel_type"]),
    x="label",
    y="y_val",
    color="fuel_type",
    color_discrete_map=colors,
    labels={"label": "Month", "y_val": y_label, "fuel_type": "Fuel"},
)
fig = apply_layout(fig, height=480)
st.plotly_chart(fig, use_container_width=True)

# ── Renewable % with 12-month rolling average ──────────────────────────────────
st.subheader("Renewable Share with 12-Month Rolling Average")

ren = renewable[
    (renewable["year_month"] >= start_ym) & (renewable["year_month"] <= end_ym)
].copy().sort_values("year_month")
ren["label"]   = ren["year_month"].apply(fmt_month)
ren["rolling"] = ren["renewable_pct"].rolling(12, min_periods=3).mean()
avg = ren["renewable_pct"].mean()

fig2 = go.Figure()
fig2.add_trace(go.Scatter(
    x=ren["label"], y=ren["renewable_pct"],
    mode="lines", name="Monthly %",
    line=dict(color="#4CAF50", width=1.5),
    opacity=0.55,
))
fig2.add_trace(go.Scatter(
    x=ren["label"], y=ren["rolling"],
    mode="lines", name="12-Month Avg",
    line=dict(color="#00897B", width=3),
))
fig2.add_hline(
    y=avg, line_dash="dot", line_color="gray",
    annotation_text=f"Period avg {avg:.1f}%",
    annotation_position="top left",
)
fig2 = apply_layout(fig2, height=400)
st.plotly_chart(fig2, use_container_width=True)

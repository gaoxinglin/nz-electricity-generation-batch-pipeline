"""Observability for dbt runs and warehouse cost."""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import plotly.express as px
from charts import apply_layout
from loader import load_dbt_runs, load_warehouse_cost
from ui import fmt_int, fmt_pct, insight_box, page_header

import streamlit as st

SLO_DATA_FRESHNESS_DAYS = 7
SLO_TEST_PASS_RATE = 0.99
SLO_DAG_SUCCESS_RATE = 0.95

page_header(
    "Pipeline Health",
    "Can reviewers trust the dashboard numbers and the batch pipeline behind them?",
    "This page uses dbt artifact history and, in Snowflake mode, warehouse-cost summaries.",
)

mode = os.environ.get("NZEG_MODE", "local").lower()
df = load_dbt_runs()

if df.empty:
    st.info("No dbt artifact history is loaded yet. Run dbt and ingest `target/run_results.json` to populate this page.")
    st.stop()

now = pd.Timestamp.now(tz="UTC").tz_convert(None)
cutoff_30d = now - pd.Timedelta(days=30)
last_30 = df[df["generated_at"] >= cutoff_30d].copy()

last_model = df[(df["node_type"] == "model") & df["is_success"]]["generated_at"].max()
freshness_days = (now - last_model).days if pd.notna(last_model) else pd.NA
models_30 = last_30[last_30["node_type"] == "model"]
tests_30 = last_30[last_30["node_type"] == "test"]
dag_success_rate = models_30["is_success"].mean() if not models_30.empty else pd.NA
test_pass_rate = (tests_30["status"] == "pass").mean() if not tests_30.empty else pd.NA


def _status_text(ok: bool | None) -> str:
    if ok is None:
        return "No recent data"
    return "On target" if ok else "Needs attention"


ok_fresh = bool(freshness_days <= SLO_DATA_FRESHNESS_DAYS) if pd.notna(freshness_days) else None
ok_models = bool(dag_success_rate >= SLO_DAG_SUCCESS_RATE) if pd.notna(dag_success_rate) else None
ok_tests = bool(test_pass_rate >= SLO_TEST_PASS_RATE) if pd.notna(test_pass_rate) else None

k1, k2, k3, k4 = st.columns(4)
k1.metric(
    "Data freshness",
    f"{int(freshness_days)} days" if pd.notna(freshness_days) else "N/A",
    _status_text(ok_fresh),
)
k2.metric(
    "Model success, 30 days",
    fmt_pct(dag_success_rate * 100) if pd.notna(dag_success_rate) else "N/A",
    _status_text(ok_models),
)
k3.metric(
    "Test pass, 30 days",
    fmt_pct(test_pass_rate * 100) if pd.notna(test_pass_rate) else "N/A",
    _status_text(ok_tests),
)
k4.metric("Invocations tracked", fmt_int(df["invocation_id"].nunique()))

insight_box(
    "Reliability standard",
    f"Targets: freshness within {SLO_DATA_FRESHNESS_DAYS} days, model success at least {SLO_DAG_SUCCESS_RATE:.0%}, and test pass rate at least {SLO_TEST_PASS_RATE:.0%}.",
)

st.subheader("Daily success rate by dbt node type")
if last_30.empty:
    st.info("No dbt run artifacts were generated in the last 30 days.")
else:
    daily = (
        last_30.assign(date=last_30["generated_date"])
        .groupby(["date", "node_type"], as_index=False)
        .agg(n=("is_success", "size"), success=("is_success", lambda values: int(values.sum())))
    )
    daily["success_rate"] = daily["success"] / daily["n"]
    fig = px.line(
        daily,
        x="date",
        y="success_rate",
        color="node_type",
        markers=True,
        labels={"success_rate": "Success rate", "date": "Date", "node_type": "Node type"},
    )
    fig.update_yaxes(tickformat=".0%", range=[0, 1.05])
    apply_layout(fig, height=320)
    st.plotly_chart(fig, width="stretch")

st.subheader("Slowest dbt models")
model_df = df[df["node_type"] == "model"].copy()
if model_df.empty:
    st.info("No model execution rows are available.")
else:
    recent_invs = (
        model_df.sort_values("generated_at", ascending=False)["invocation_id"]
        .drop_duplicates()
        .head(10)
        .tolist()
    )
    recent = model_df[model_df["invocation_id"].isin(recent_invs)]
    median_rt = (
        recent.groupby("node_name", as_index=False)["execution_time_seconds"]
        .median()
        .sort_values("execution_time_seconds", ascending=False)
        .head(10)
    )
    fig2 = px.bar(
        median_rt.sort_values("execution_time_seconds"),
        x="execution_time_seconds",
        y="node_name",
        orientation="h",
        labels={"execution_time_seconds": "Median seconds", "node_name": "Model"},
    )
    fig2.update_traces(marker_color="#5C6BC0")
    apply_layout(fig2, height=400)
    st.plotly_chart(fig2, width="stretch")

st.subheader("Most recent non-pass results")
non_pass = df[
    (df["status"].isin(["fail", "error", "warn", "skipped"]))
    & (df["invocation_rank_desc"] == 1)
][["generated_at", "node_type", "node_name", "status", "failures", "execution_time_seconds"]]
if non_pass.empty:
    st.success("All latest-per-node results are passing or successful.")
else:
    st.dataframe(non_pass.reset_index(drop=True), width="stretch", hide_index=True)

st.subheader("Snowflake warehouse cost")
cost = load_warehouse_cost()
if cost is None or cost.empty:
    if mode == "local":
        st.info("Cost metrics are Snowflake-only and are not available in local DuckDB mode.")
    else:
        st.info("No warehouse-cost mart rows are available yet.")
else:
    last_30d_cost = cost[cost["usage_date"] >= cutoff_30d]
    total_usd = last_30d_cost["usd_estimated"].sum()
    total_credits = last_30d_cost["credits_used"].sum()
    total_queries = int(last_30d_cost["total_queries"].sum())
    failed_queries = int(last_30d_cost["failed_queries"].sum())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("30-day credits", f"{total_credits:,.1f}")
    c2.metric("30-day estimated USD", f"${total_usd:,.2f}")
    c3.metric("30-day queries", fmt_int(total_queries))
    c4.metric("30-day failed queries", fmt_int(failed_queries))

    daily_cost = (
        last_30d_cost.groupby(["usage_date", "warehouse_name"], as_index=False)["credits_used"]
        .sum()
    )
    fig3 = px.bar(
        daily_cost,
        x="usage_date",
        y="credits_used",
        color="warehouse_name",
        labels={"credits_used": "Credits", "usage_date": "Date", "warehouse_name": "Warehouse"},
    )
    apply_layout(fig3, height=340)
    st.plotly_chart(fig3, width="stretch")

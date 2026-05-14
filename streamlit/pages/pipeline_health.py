"""Pipeline Health — observability for the dbt pipeline (Phase 5 / Tier 1)."""

from __future__ import annotations

import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

import streamlit as st
from charts import apply_layout
from loader import load_dbt_runs, load_warehouse_cost

st.title("🩺 Pipeline Health")
st.caption(
    "dbt run-result history (from `fct_dbt_run`) + Snowflake warehouse "
    "cost (SF only). Built on top of artifacts ingested by "
    "`scripts/ingest_dbt_artifacts.py` after every `dbt run` / `dbt test`."
)


# ─── SLO config (kept in sync with README §SLO) ────────────────────
SLO_DATA_FRESHNESS_DAYS = 7      # EMI publish → ingested within 7 cal days
SLO_TEST_PASS_RATE = 0.99        # 30-day dbt test pass rate ≥ 99%
SLO_DAG_SUCCESS_RATE = 0.95      # 30-day model-execution success rate ≥ 95%

mode = os.environ.get("NZEG_MODE", "local").lower()
df = load_dbt_runs()

if df.empty:
    st.warning(
        "No `fct_dbt_run` rows yet — run "
        "`python scripts/ingest_dbt_artifacts.py --artifact dbt/target/run_results.json` "
        "after a `dbt run` / `dbt test`."
    )
    st.stop()


# ─── SLO badges ────────────────────────────────────────────────────
now = pd.Timestamp.utcnow().tz_convert(None) if hasattr(pd.Timestamp.utcnow(), "tz_convert") else pd.Timestamp.utcnow()
cutoff_30d = now - pd.Timedelta(days=30)
last_30 = df[df["generated_at"] >= cutoff_30d]

# data freshness: last successful model run timestamp
last_model = df[(df["node_type"] == "model") & df["is_success"]]["generated_at"].max()
freshness_days = (now - last_model).days if pd.notna(last_model) else None

models_30 = last_30[last_30["node_type"] == "model"]
dag_success_rate = models_30["is_success"].mean() if not models_30.empty else None

tests_30 = last_30[last_30["node_type"] == "test"]
test_pass_rate = (tests_30["status"] == "pass").mean() if not tests_30.empty else None


def _slo_badge(ok: bool | None) -> str:
    """Return the SLO status badge alone. Goes into the metric *label* so the
    value column stays uncluttered and won't truncate at narrow widths."""
    if ok is None:
        return "❓"
    return "✅" if ok else "❌"


k1, k2, k3, k4 = st.columns(4)

ok_fresh = freshness_days is not None and freshness_days <= SLO_DATA_FRESHNESS_DAYS
k1.metric(
    f"{_slo_badge(ok_fresh)} Freshness (≤{SLO_DATA_FRESHNESS_DAYS}d)",
    f"{freshness_days}d ago" if freshness_days is not None else "—",
)

ok_models = dag_success_rate is not None and dag_success_rate >= SLO_DAG_SUCCESS_RATE
k2.metric(
    f"{_slo_badge(ok_models if dag_success_rate is not None else None)} "
    f"Model success 30d (≥{SLO_DAG_SUCCESS_RATE:.0%})",
    f"{dag_success_rate:.1%}" if dag_success_rate is not None else "—",
)

ok_tests = test_pass_rate is not None and test_pass_rate >= SLO_TEST_PASS_RATE
k3.metric(
    f"{_slo_badge(ok_tests if test_pass_rate is not None else None)} "
    f"Test pass 30d (≥{SLO_TEST_PASS_RATE:.0%})",
    f"{test_pass_rate:.1%}" if test_pass_rate is not None else "—",
)

k4.metric("Invocations tracked", f"{df['invocation_id'].nunique():,}")


# ─── Per-day success/pass trend ────────────────────────────────────
st.subheader("Daily success rate by node type (30 day window)")

daily = (
    last_30.assign(date=last_30["generated_date"])
    .groupby(["date", "node_type"])
    .agg(n=("is_success", "size"),
         success=("is_success", lambda s: int(s.sum())))
    .reset_index()
)
daily["success_rate"] = daily["success"] / daily["n"]

fig = px.line(
    daily, x="date", y="success_rate", color="node_type",
    markers=True,
    labels={"success_rate": "rate", "date": ""},
)
fig.update_yaxes(tickformat=".0%", range=[0, 1.05])
apply_layout(fig, height=320)
st.plotly_chart(fig, use_container_width=True)


# ─── Model runtime trend (median over recent invocations) ──────────
st.subheader("Median model runtime — top 10 longest")

model_df = df[df["node_type"] == "model"].copy()
recent_invs = (
    model_df.sort_values("generated_at", ascending=False)["invocation_id"]
    .drop_duplicates().head(10).tolist()
)
recent = model_df[model_df["invocation_id"].isin(recent_invs)]
median_rt = (
    recent.groupby("node_name")["execution_time_seconds"]
    .median()
    .sort_values(ascending=False)
    .head(10)
    .reset_index()
)
fig2 = px.bar(
    median_rt.sort_values("execution_time_seconds"),
    x="execution_time_seconds", y="node_name", orientation="h",
    labels={"execution_time_seconds": "median seconds", "node_name": ""},
)
fig2.update_traces(marker_color="#5C6BC0")
apply_layout(fig2, height=400)
st.plotly_chart(fig2, use_container_width=True)


# ─── Latest failures / warnings table ──────────────────────────────
st.subheader("Most recent non-pass results")
non_pass = df[
    (df["status"].isin(["fail", "error", "warn", "skipped"]))
    & (df["invocation_rank_desc"] == 1)
][["generated_at", "node_type", "node_name", "status", "failures", "execution_time_seconds"]]
if non_pass.empty:
    st.success("All latest-per-node results are passing or successful. 🎉")
else:
    st.dataframe(non_pass.reset_index(drop=True), use_container_width=True)


# ─── Cost panel (SF only) ──────────────────────────────────────────
st.subheader("Snowflake warehouse cost")
cost = load_warehouse_cost()
if cost is None or cost.empty:
    if mode == "local":
        st.info("Cost panel is Snowflake-only — not available in local mode.")
    else:
        st.info(
            "No cost data yet — `mart_warehouse_cost` requires "
            "`SNOWFLAKE.ACCOUNT_USAGE` access (granted to ACCOUNTADMIN by default)."
        )
else:
    last_30d_cost = cost[cost["usage_date"] >= cutoff_30d]
    total_usd = last_30d_cost["usd_estimated"].sum()
    total_credits = last_30d_cost["credits_used"].sum()
    total_queries = int(last_30d_cost["total_queries"].sum())
    failed_queries = int(last_30d_cost["failed_queries"].sum())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("30d credits", f"{total_credits:,.1f}")
    c2.metric("30d est. USD", f"${total_usd:,.2f}")
    c3.metric("30d queries", f"{total_queries:,}")
    c4.metric("30d failures", f"{failed_queries:,}")

    daily_cost = (
        last_30d_cost.groupby(["usage_date", "warehouse_name"])["credits_used"]
        .sum()
        .reset_index()
    )
    fig3 = px.bar(
        daily_cost, x="usage_date", y="credits_used", color="warehouse_name",
        labels={"credits_used": "credits", "usage_date": ""},
    )
    apply_layout(fig3, height=340)
    st.plotly_chart(fig3, use_container_width=True)

st.caption(
    f"SLO targets (also documented in README): freshness ≤{SLO_DATA_FRESHNESS_DAYS} "
    f"days, model success ≥{SLO_DAG_SUCCESS_RATE:.0%}, test pass ≥{SLO_TEST_PASS_RATE:.0%}."
)

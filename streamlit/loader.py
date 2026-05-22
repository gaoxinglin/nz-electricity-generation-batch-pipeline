"""
Dual-mode warehouse loader.

NZEG_MODE=local   → DuckDB at NZEG_DUCKDB_PATH (read_only; avoids dbt write lock)
NZEG_MODE=cloud   → Snowflake using st.secrets["snowflake"] or SNOWFLAKE_*
(unset)           → defaults to local

All load_*() functions return pandas DataFrames with lowercase columns. SQL is
written to be portable across DuckDB and Snowflake (no SF-only functions like
TO_CHAR). Schema names differ by mode, so each load function references the
right schema via _analytics()/_staging().
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path

import pandas as pd
from streamlit.errors import StreamlitSecretNotFoundError

import streamlit as st

_SNOWFLAKE_ENV = {
    "account": "SNOWFLAKE_ACCOUNT",
    "user": "SNOWFLAKE_USER",
    "private_key_path": "SNOWFLAKE_PRIVATE_KEY_PATH",
    "database": "SNOWFLAKE_DATABASE",
    "warehouse": "SNOWFLAKE_WAREHOUSE",
    "role": "SNOWFLAKE_ROLE",
}


def _mode() -> str:
    return os.environ.get("NZEG_MODE", "local").lower()


def _analytics() -> str:
    """Fully-qualified analytics schema for the current mode."""
    if _mode() == "local":
        return "main_analytics"
    return "RAW_ANALYTICS"  # Snowflake: target.schema='RAW' + +schema='analytics'


def _staging() -> str:
    if _mode() == "local":
        return "main_raw"
    return "RAW_RAW"


# ─── connection plumbing ──────────────────────────────────────────────


@contextmanager
def _duckdb_conn():
    import duckdb
    path = os.environ.get("NZEG_DUCKDB_PATH", "data/nzeg.duckdb")
    # Resolve relative to repo root if launched from streamlit/ subdir
    if not Path(path).exists():
        repo_root = Path(__file__).resolve().parent.parent
        candidate = repo_root / path
        if candidate.exists():
            path = str(candidate)
    conn = duckdb.connect(path, read_only=True)
    try:
        yield conn
    finally:
        conn.close()


def _snowflake_settings() -> dict[str, str]:
    """Prefer deployed Streamlit secrets, then fall back to local env vars."""
    try:
        settings = dict(st.secrets["snowflake"])
    except (KeyError, StreamlitSecretNotFoundError):
        settings = {}

    if not settings:
        settings = {
            key: value
            for key, env_name in _SNOWFLAKE_ENV.items()
            if (value := os.environ.get(env_name))
        }

    missing = [key for key in _SNOWFLAKE_ENV if not settings.get(key)]
    if missing:
        env_names = ", ".join(_SNOWFLAKE_ENV[key] for key in missing)
        secret_names = ", ".join(f"snowflake.{key}" for key in missing)
        raise RuntimeError(
            "Cloud dashboard Snowflake settings are incomplete. "
            f"Set {env_names} or Streamlit secrets {secret_names}."
        )
    return settings


def _snowflake_private_key(settings: dict[str, str]) -> bytes:
    from cryptography.hazmat.primitives.serialization import (
        Encoding,
        NoEncryption,
        PrivateFormat,
        load_pem_private_key,
    )
    path = os.path.expanduser(settings["private_key_path"])
    with open(path, "rb") as fh:
        return load_pem_private_key(fh.read(), password=None).private_bytes(
            Encoding.DER, PrivateFormat.PKCS8, NoEncryption()
        )


@contextmanager
def _snowflake_conn():
    import snowflake.connector
    s = _snowflake_settings()
    conn = snowflake.connector.connect(
        account=s["account"],
        user=s["user"],
        private_key=_snowflake_private_key(s),
        database=s["database"],
        warehouse=s["warehouse"],
        role=s["role"],
        schema="RAW_ANALYTICS",
    )
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def _conn():
    if _mode() == "local":
        with _duckdb_conn() as c:
            yield c
    else:
        with _snowflake_conn() as c:
            yield c


# ─── query helper ─────────────────────────────────────────────────────


@st.cache_data(ttl=3600, show_spinner=False)
def _q(sql: str) -> pd.DataFrame:
    with _conn() as conn:
        if _mode() == "local":
            df = conn.execute(sql).fetchdf()
        else:
            df = pd.read_sql(sql, conn)
    df.columns = df.columns.str.lower()
    if "year_month" in df.columns:
        df["year_month"] = df["year_month"].astype(str).str.zfill(6)
    return df


# ─── V1 loaders (Phase 0/Phase 1 preserved) ───────────────────────────


def load_monthly() -> pd.DataFrame:
    df = _q(f"""
        SELECT year_month, fuel_type,
               total_generation_gwh, generator_count, active_days
        FROM {_analytics()}.mart_generation_monthly
        ORDER BY year_month, fuel_type
    """)
    df["date"] = pd.to_datetime(df["year_month"], format="%Y%m")
    return df


def load_renewable() -> pd.DataFrame:
    df = _q(f"""
        SELECT year_month, total_gwh, renewable_gwh, renewable_pct
        FROM {_analytics()}.mart_renewable_ratio
        ORDER BY year_month
    """)
    df["date"] = pd.to_datetime(df["year_month"], format="%Y%m")
    return df


def load_ranking() -> pd.DataFrame:
    df = _q(f"""
        SELECT year_month, site_code,
               total_generation_gwh, monthly_rank, primary_fuel_type
        FROM {_analytics()}.mart_plant_ranking
        ORDER BY year_month DESC, monthly_rank
    """)
    df["date"] = pd.to_datetime(df["year_month"], format="%Y%m")
    return df


def load_seasonal() -> pd.DataFrame:
    return _q(f"""
        SELECT season_year, season, fuel_type,
               total_generation_gwh, avg_generation_gwh
        FROM {_analytics()}.mart_seasonal_pattern
        ORDER BY season_year, season, fuel_type
    """)


def load_monthly_raw() -> pd.DataFrame:
    """Monthly with month_num column, used for heatmap."""
    df = load_monthly()
    df["month_num"] = df["date"].dt.month
    return df


# ─── V2 Phase 3 loaders (price marts) ─────────────────────────────────


def load_price_daily() -> pd.DataFrame:
    """mart_price_daily: POC × date with island/region and stats."""
    df = _q(f"""
        SELECT trading_date, poc_code, island, region, zone,
               tp_count, avg_price_all, min_price, max_price, stddev_price,
               spike_tp_count, negative_tp_count, pricing_regime
        FROM {_analytics()}.mart_price_daily
        ORDER BY trading_date, poc_code
    """)
    df["trading_date"] = pd.to_datetime(df["trading_date"])
    return df


def load_price_spikes() -> pd.DataFrame:
    df = _q(f"""
        SELECT trading_date, poc_code, tp_number, price_nzd_mwh,
               island, region,
               total_generation_kwh, renewable_generation_kwh,
               thermal_generation_kwh, unmatched_generation
        FROM {_analytics()}.mart_price_spike_events
        ORDER BY trading_date, tp_number, poc_code
    """)
    df["trading_date"] = pd.to_datetime(df["trading_date"])
    return df


def load_renewable_price_impact() -> pd.DataFrame:
    df = _q(f"""
        SELECT trading_date, poc_code, tp_number, price_nzd_mwh,
               total_generation_kwh, renewable_generation_kwh,
               renewable_pct, island, region
        FROM {_analytics()}.mart_renewable_price_impact
        ORDER BY trading_date, tp_number, poc_code
    """)
    df["trading_date"] = pd.to_datetime(df["trading_date"])
    return df


# ─── Observability (Phase 5) ──────────────────────────────────────────


def load_dbt_runs() -> pd.DataFrame:
    """fct_dbt_run: one row per (invocation, node) — pipeline observability."""
    try:
        df = _q(f"""
            SELECT
                invocation_id, generated_at, generated_date,
                dbt_version, node_unique_id, node_type, node_name,
                status, execution_time_seconds, failures, is_success,
                invocation_rank_desc
            FROM {_analytics()}.fct_dbt_run
            ORDER BY generated_at DESC
        """)
    except Exception:
        # First run before any artifact ingest: return empty frame with right schema
        return pd.DataFrame(columns=[
            "invocation_id", "generated_at", "generated_date", "dbt_version",
            "node_unique_id", "node_type", "node_name", "status",
            "execution_time_seconds", "failures", "is_success",
            "invocation_rank_desc",
        ])
    df["generated_at"] = pd.to_datetime(df["generated_at"])
    df["generated_date"] = pd.to_datetime(df["generated_date"])
    return df


def load_hydro_price_driver() -> pd.DataFrame:
    """mart_hydro_price_driver: monthly island storage vs price."""
    df = _q(f"""
        SELECT
            island, year_month,
            avg_total_storage_mm3, min_storage_mm3, max_storage_mm3,
            storage_pct_of_max, observation_days, avg_sites_reporting,
            avg_price_nzd_mwh, avg_price_non_proxy, price_days, poc_count
        FROM {_analytics()}.mart_hydro_price_driver
        ORDER BY island, year_month
    """)
    return df


def load_hydro_storage_detail() -> pd.DataFrame:
    """fct_hydro: daily per-lake storage for sparklines and detail views."""
    df = _q(f"""
        SELECT
            site_code, catchment_name, island, scheme,
            trading_date, active_storage_mm3, level_m, quality_code
        FROM {_analytics()}.fct_hydro
        ORDER BY island, site_code, trading_date
    """)
    df["trading_date"] = pd.to_datetime(df["trading_date"])
    return df


def load_warehouse_cost() -> pd.DataFrame | None:
    """mart_warehouse_cost (SF only). Returns None on DuckDB target."""
    if _mode() == "local":
        return None
    try:
        df = _q(f"""
            SELECT usage_date, warehouse_name, credits_used,
                   credits_used_compute, credits_used_cloud_services,
                   usd_estimated, total_queries, failed_queries,
                   avg_query_seconds
            FROM {_analytics()}.mart_warehouse_cost
            ORDER BY usage_date DESC, warehouse_name
        """)
    except Exception:
        return None
    df["usage_date"] = pd.to_datetime(df["usage_date"])
    return df

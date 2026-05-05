import os
from contextlib import contextmanager

import pandas as pd
import snowflake.connector
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    load_pem_private_key,
)

import streamlit as st


def _private_key_bytes() -> bytes:
    path = os.path.expanduser(st.secrets["snowflake"]["private_key_path"])
    with open(path, "rb") as fh:
        return load_pem_private_key(fh.read(), password=None).private_bytes(
            Encoding.DER, PrivateFormat.PKCS8, NoEncryption()
        )


@contextmanager
def _conn():
    s = st.secrets["snowflake"]
    conn = snowflake.connector.connect(
        account=s["account"],
        user=s["user"],
        private_key=_private_key_bytes(),
        database=s["database"],
        warehouse=s["warehouse"],
        role=s["role"],
        schema="RAW_ANALYTICS",
    )
    try:
        yield conn
    finally:
        conn.close()


@st.cache_data(ttl=3600, show_spinner=False)
def _q(sql: str) -> pd.DataFrame:
    with _conn() as conn:
        df = pd.read_sql(sql, conn)
    df.columns = df.columns.str.lower()
    # Normalize year_month to zero-padded string ("202301") for consistent comparisons
    if "year_month" in df.columns:
        df["year_month"] = df["year_month"].astype(str).str.zfill(6)
    return df


def load_monthly() -> pd.DataFrame:
    df = _q("""
        SELECT year_month, fuel_type,
               total_generation_gwh, generator_count, active_days
        FROM RAW_ANALYTICS.mart_generation_monthly
        ORDER BY year_month, fuel_type
    """)
    df["date"] = pd.to_datetime(df["year_month"], format="%Y%m")
    return df


def load_renewable() -> pd.DataFrame:
    df = _q("""
        SELECT year_month, total_gwh, renewable_gwh, renewable_pct
        FROM RAW_ANALYTICS.mart_renewable_ratio
        ORDER BY year_month
    """)
    df["date"] = pd.to_datetime(df["year_month"], format="%Y%m")
    return df


def load_ranking() -> pd.DataFrame:
    df = _q("""
        SELECT year_month, site_code,
               total_generation_gwh, monthly_rank, primary_fuel_type
        FROM RAW_ANALYTICS.mart_plant_ranking
        ORDER BY year_month DESC, monthly_rank
    """)
    df["date"] = pd.to_datetime(df["year_month"], format="%Y%m")
    return df


def load_seasonal() -> pd.DataFrame:
    return _q("""
        SELECT season_year, season, fuel_type,
               total_generation_gwh, avg_generation_gwh
        FROM RAW_ANALYTICS.mart_seasonal_pattern
        ORDER BY season_year, season, fuel_type
    """)


def load_monthly_raw() -> pd.DataFrame:
    """Monthly with month_num column, used for heatmap."""
    df = load_monthly()
    df["month_num"] = df["date"].dt.month
    return df

"""
Phase 0.0 Mini POC: prove the unpivot_trading_periods macro produces
byte-identical output on Snowflake and DuckDB for a 2-day generation slice.

Fixture (per PRD §16.2):
  - Month: 202401
  - Days kept: 2024-01-15 and 2024-01-16
  - TP range: macro called with TP1, TP2, TP3 only (the cross-dialect test
    surface; full TP1-TP50 is exercised by stg_generation in 0.3+)

Test surface: this script runs the macro DIRECTLY (not through stg_generation),
because stg_generation will only switch to the macro in Phase 0.3. The macro
is called with the same 3 TP columns on both targets and the resulting
(site_code, gen_code, trading_date, tp_number, value) rows are compared.

Pass: row count equal AND every row's value column identical after sorting
on the natural key. Integer kWh values mean exact equality is safe.

Pre-reqs:
  - DuckDB raw_generation must be loaded with 202401 data (run load_local.py).
  - Snowflake RAW.raw_generation must contain 202401 data (V1 backfill).
  - SNOWFLAKE_* env vars set; SNOWFLAKE_PRIVATE_KEY_PATH must point to a key
    readable from the host (use ~/.ssh/snowflake_rsa_key.p8 if .env still has
    the Docker container path).

Usage:
    python scripts/mini_poc_fixture.py
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import duckdb
import pandas as pd

logger = logging.getLogger("mini_poc")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

DUCKDB_PATH = Path(os.environ.get("NZEG_DUCKDB_PATH", "data/nzeg.duckdb"))
FIXTURE_DATES = ("2024-01-15", "2024-01-16")
TP_COLS = ("tp1", "tp2", "tp3")  # subset under test


def snowflake_query() -> pd.DataFrame:
    """Run the Snowflake side of the macro logic. Returns a DataFrame."""
    import snowflake.connector
    from cryptography.hazmat.primitives.serialization import (
        Encoding, NoEncryption, PrivateFormat, load_pem_private_key,
    )

    key_path = os.path.expanduser(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"])
    with open(key_path, "rb") as f:
        pkey = load_pem_private_key(f.read(), password=None).private_bytes(
            Encoding.DER, PrivateFormat.PKCS8, NoEncryption()
        )

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=pkey,
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema="RAW",
    )
    try:
        # Mirror the Snowflake branch of unpivot_trading_periods exactly.
        sql = f"""
        SELECT
          site_code,
          gen_code,
          trading_date::date AS trading_date,
          f.value:tp::INT AS tp_number,
          f.value:val::STRING AS generation_kwh
        FROM raw_generation,
        LATERAL FLATTEN(
          input => ARRAY_CONSTRUCT(
            OBJECT_CONSTRUCT('tp', 1, 'val', tp1),
            OBJECT_CONSTRUCT('tp', 2, 'val', tp2),
            OBJECT_CONSTRUCT('tp', 3, 'val', tp3)
          )
        ) AS f
        WHERE f.value:val IS NOT NULL
          AND trading_date IN ('{FIXTURE_DATES[0]}', '{FIXTURE_DATES[1]}')
        """
        df = conn.cursor().execute(sql).fetch_pandas_all()
    finally:
        conn.close()

    df.columns = [c.lower() for c in df.columns]
    return df


def duckdb_query() -> pd.DataFrame:
    """Mirror the DuckDB branch of unpivot_trading_periods."""
    conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    try:
        sql = f"""
        SELECT
          site_code,
          gen_code,
          CAST(trading_date AS DATE) AS trading_date,
          unnested.tp AS tp_number,
          unnested.val AS generation_kwh
        FROM raw.raw_generation,
        UNNEST([
          {{'tp': 1, 'val': tp1}},
          {{'tp': 2, 'val': tp2}},
          {{'tp': 3, 'val': tp3}}
        ]) AS _u(unnested)
        WHERE unnested.val IS NOT NULL
          AND trading_date IN ('{FIXTURE_DATES[0]}', '{FIXTURE_DATES[1]}')
        """
        return conn.execute(sql).fetchdf()
    finally:
        conn.close()


def normalise(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["trading_date"] = pd.to_datetime(out["trading_date"]).dt.date.astype(str)
    out["tp_number"] = out["tp_number"].astype(int)
    out["generation_kwh"] = out["generation_kwh"].astype(str)
    out = out.sort_values(
        by=["site_code", "gen_code", "trading_date", "tp_number"],
        kind="stable",
    ).reset_index(drop=True)
    return out


def main() -> int:
    logger.info("running Snowflake side...")
    sf = normalise(snowflake_query())
    logger.info("Snowflake rows: %d", len(sf))

    logger.info("running DuckDB side...")
    dd = normalise(duckdb_query())
    logger.info("DuckDB    rows: %d", len(dd))

    if len(sf) != len(dd):
        logger.error("ROW COUNT MISMATCH: SF=%d DuckDB=%d", len(sf), len(dd))
        return 1

    if not sf.equals(dd):
        diff = sf.compare(dd)
        logger.error("ROW CONTENT MISMATCH — first diffs:\n%s", diff.head(20))
        return 1

    logger.info("PASS — %d rows identical across Snowflake and DuckDB", len(sf))
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""
Load EMI Final Energy Prices CSVs from S3 into Snowflake — module + CLI.

Designed to be called from the Phase 4 v2 Airflow DAG; CLI entry kept for
manual replays. Idempotent per (trading_month): DELETE then COPY INTO inside
one transaction with explicit ROLLBACK on failure.

Schema parity with V1's load_to_snowflake (in nz_electricity_monthly.py):
  - raw_price table has the 4 source cols (renamed lowercase) + trading_month
    + _source_file_modified_at, all VARCHAR except the timestamp.

Pre-reqs:
  - Snowflake stage `raw_stage` already exists (created by Phase 4.3 terraform
    update; same pattern as generation).
  - raw_price table exists in Snowflake RAW schema.
  - Env: SNOWFLAKE_ACCOUNT / USER / PRIVATE_KEY_PATH / ROLE / WAREHOUSE / DATABASE.

CLI usage:
    python scripts/load_snowflake_price.py --year-month 202401 \\
        --s3-key raw/final_energy_prices/202401_FinalEnergyPrices.csv
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

logger = logging.getLogger("load_sf_price")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _snowflake_connect():
    import snowflake.connector
    from cryptography.hazmat.primitives.serialization import (
        Encoding, NoEncryption, PrivateFormat, load_pem_private_key,
    )

    key_path = os.path.expanduser(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"])
    with open(key_path, "rb") as f:
        pkey = load_pem_private_key(f.read(), password=None).private_bytes(
            Encoding.DER, PrivateFormat.PKCS8, NoEncryption()
        )
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=pkey,
        database=os.environ["SNOWFLAKE_DATABASE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.environ["SNOWFLAKE_ROLE"],
        schema="RAW",
    )


def ensure_table(cur) -> None:
    """Idempotent create of raw_price. All cols VARCHAR for stg layer cast."""
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_price (
            trading_date         VARCHAR,
            trading_period       VARCHAR,
            point_of_connection  VARCHAR,
            dollars_per_mwh      VARCHAR,
            trading_month        VARCHAR,
            _source_file_modified_at TIMESTAMP_NTZ
        )
        """
    )


def load_price_month(year_month: str, s3_key: str) -> int:
    """Idempotently load one month of price CSV into raw_price.

    Returns the row count loaded for this month.
    """
    conn = _snowflake_connect()
    try:
        cur = conn.cursor()
        ensure_table(cur)
        cur.execute("BEGIN")

        cur.execute(
            "DELETE FROM raw_price WHERE trading_month = %s",
            (year_month,),
        )
        logger.info("DELETE done for trading_month=%s", year_month)

        copy_sql = f"""
            COPY INTO raw_price
            FROM (
                SELECT
                    $1, $2, $3, $4,
                    '{year_month}'                       AS trading_month,
                    METADATA$FILE_LAST_MODIFIED          AS _source_file_modified_at
                FROM @raw_stage/{s3_key}
            )
            FILE_FORMAT = (FORMAT_NAME = 'csv_format')
            ON_ERROR = 'ABORT_STATEMENT'
        """
        cur.execute(copy_sql)
        loaded = cur.fetchone()[0]
        cur.execute("COMMIT")
        logger.info("loaded %s rows for trading_month=%s", loaded, year_month)
        return loaded
    except Exception:
        conn.cursor().execute("ROLLBACK")
        logger.error("ROLLBACK on trading_month=%s", year_month)
        raise
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Load price CSV from S3 into Snowflake raw_price")
    p.add_argument("--year-month", required=True, help="YYYYMM")
    p.add_argument("--s3-key", required=True, help="S3 key under raw_stage")
    args = p.parse_args(argv)
    load_price_month(args.year_month, args.s3_key)
    return 0


if __name__ == "__main__":
    sys.exit(main())

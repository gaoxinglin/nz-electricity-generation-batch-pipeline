"""
Load EMI reconciled injection/offtake volume CSVs from S3 into Snowflake.

Idempotent per trading_month: DELETE then COPY INTO inside one transaction
with explicit ROLLBACK on failure.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

logger = logging.getLogger("load_sf_volume")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _snowflake_connect():
    import snowflake.connector
    from cryptography.hazmat.primitives.serialization import (
        Encoding,
        NoEncryption,
        PrivateFormat,
        load_pem_private_key,
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
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_market_volume (
            point_of_connection       VARCHAR,
            network                   VARCHAR,
            island                    VARCHAR,
            participant               VARCHAR,
            trading_date              VARCHAR,
            trading_period            VARCHAR,
            trading_period_start_time VARCHAR,
            flow_direction            VARCHAR,
            kilowatt_hours            VARCHAR,
            trading_month             VARCHAR,
            _source_file_modified_at  TIMESTAMP_NTZ
        )
        """
    )


def load_volume_month(year_month: str, s3_key: str) -> int:
    conn = _snowflake_connect()
    try:
        cur = conn.cursor()
        ensure_table(cur)
        cur.execute("BEGIN")

        cur.execute(
            "DELETE FROM raw_market_volume WHERE trading_month = %s",
            (year_month,),
        )
        logger.info("DELETE done for trading_month=%s", year_month)

        copy_sql = f"""
            COPY INTO raw_market_volume
            FROM (
                SELECT
                    $1, $2, $3, $4, $5, $6, $7, $8, $9,
                    '{year_month}'              AS trading_month,
                    METADATA$FILE_LAST_MODIFIED AS _source_file_modified_at
                FROM @raw_stage/{s3_key}
            )
            FILE_FORMAT = (FORMAT_NAME = 'csv_format')
            ON_ERROR = 'ABORT_STATEMENT'
            FORCE = TRUE
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
    parser = argparse.ArgumentParser(description="Load volume CSV from S3 into Snowflake raw_market_volume")
    parser.add_argument("--year-month", required=True, help="YYYYMM")
    parser.add_argument("--s3-key", required=True, help="S3 key under raw_stage")
    args = parser.parse_args(argv)
    load_volume_month(args.year_month, args.s3_key)
    return 0


if __name__ == "__main__":
    sys.exit(main())

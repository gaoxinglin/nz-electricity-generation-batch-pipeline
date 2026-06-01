"""
Load EMI daily offer CSVs from S3 into Snowflake.

Idempotent per trading_date: DELETE then COPY INTO inside one transaction
with explicit ROLLBACK on failure.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date

logger = logging.getLogger("load_sf_offers")
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


def _parse_day(value: str) -> date:
    if len(value) == 8 and value.isdigit():
        return date(int(value[:4]), int(value[4:6]), int(value[6:]))
    if len(value) == 10 and value[4] == "-" and value[7] == "-":
        return date.fromisoformat(value)
    raise argparse.ArgumentTypeError("trading date must be YYYYMMDD or YYYY-MM-DD")


def ensure_table(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_offers (
            trading_date                                  VARCHAR,
            trading_period                                VARCHAR,
            participant_code                              VARCHAR,
            point_of_connection                           VARCHAR,
            unit                                          VARCHAR,
            product_type                                  VARCHAR,
            product_class                                 VARCHAR,
            reserve_type                                  VARCHAR,
            product_description                           VARCHAR,
            utc_submission_date                           VARCHAR,
            utc_submission_time                           VARCHAR,
            submission_order                              VARCHAR,
            is_latest_yes_no                              VARCHAR,
            tranche                                       VARCHAR,
            maximum_ramp_up_mw_per_hour                   VARCHAR,
            maximum_ramp_down_mw_per_hour                 VARCHAR,
            partially_loaded_spinning_reserve_percent     VARCHAR,
            maximum_output_mw                             VARCHAR,
            forecast_generation_potential_mw              VARCHAR,
            megawatts                                     VARCHAR,
            dollars_per_mwh                               VARCHAR,
            trading_month                                 VARCHAR,
            _source_file_modified_at                      TIMESTAMP_NTZ
        )
        """
    )


def load_offer_day(trading_day: date, s3_key: str) -> int:
    trading_date = trading_day.isoformat()
    trading_month = f"{trading_day:%Y%m}"
    conn = _snowflake_connect()
    try:
        cur = conn.cursor()
        ensure_table(cur)
        cur.execute("BEGIN")

        cur.execute("DELETE FROM raw_offers WHERE trading_date = %s", (trading_date,))
        logger.info("DELETE done for trading_date=%s", trading_date)

        copy_sql = f"""
            COPY INTO raw_offers
            FROM (
                SELECT
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11, $12, $13, $14, $15, $16, $17, $18, $19,
                    $20, $21,
                    '{trading_month}'            AS trading_month,
                    METADATA$FILE_LAST_MODIFIED  AS _source_file_modified_at
                FROM @raw_stage/{s3_key}
            )
            FILE_FORMAT = (FORMAT_NAME = 'csv_format')
            ON_ERROR = 'ABORT_STATEMENT'
            FORCE = TRUE
        """
        cur.execute(copy_sql)
        copy_result = cur.fetchone()
        copy_columns = [column[0].lower() for column in cur.description or []]
        if "rows_loaded" in copy_columns:
            loaded = int(copy_result[copy_columns.index("rows_loaded")])
        elif len(copy_result) > 3:
            loaded = int(copy_result[3])
        else:
            loaded = int(copy_result[0])
        cur.execute("COMMIT")
        logger.info("loaded %s rows for trading_date=%s", loaded, trading_date)
        return loaded
    except Exception:
        conn.cursor().execute("ROLLBACK")
        logger.error("ROLLBACK on trading_date=%s", trading_date)
        raise
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Load offer CSV from S3 into Snowflake raw_offers")
    parser.add_argument("--trading-date", required=True, type=_parse_day, help="YYYYMMDD or YYYY-MM-DD")
    parser.add_argument("--s3-key", required=True, help="S3 key under raw_stage")
    args = parser.parse_args(argv)
    load_offer_day(args.trading_date, args.s3_key)
    return 0


if __name__ == "__main__":
    sys.exit(main())

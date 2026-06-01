"""
Idempotent CSV → DuckDB loader for local development.

Scans `data/raw/` for EMI dataset files and loads each into a DuckDB database.
Each file is loaded in its own transaction (BEGIN / COMMIT / ROLLBACK), so a
partial run never leaves the warehouse half-loaded.

Currently handles:
  - {YYYYMM}_Generation_MD.csv      → raw.raw_generation
  - {YYYYMM}_FinalEnergyPrices.csv  → raw.raw_price
  - {YYYYMM}_ReconciledInjectionAndOfftake.csv[.gz] → raw.raw_market_volume
  - {YYYYMMDD}_Offers.csv           → raw.raw_offers
  - NetworkSupplyPointsTable.csv    → raw.raw_nsp (full reload — small file)

Schema parity with Snowflake (V1):
  raw_generation has 59 columns, all VARCHAR, named lowercase:
    site_code, poc_code, nwk_code, gen_code, fuel_code, tech_code,
    trading_date, tp1..tp50, trading_month, _source_file_modified_at
  This matches the column references in dbt/models/staging/stg_generation.sql.

Usage:
    python scripts/load_local.py --db data/nzeg.duckdb --source data/raw/
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

import duckdb

logger = logging.getLogger("load_local")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

GENERATION_FILENAME_RE = re.compile(r"^(\d{6})_Generation_MD\.csv$")
PRICE_FILENAME_RE = re.compile(r"^(\d{6})_FinalEnergyPrices\.csv$")
MARKET_VOLUME_FILENAME_RE = re.compile(r"^(\d{6})_ReconciledInjectionAndOfftake\.csv(?:\.gz)?$")
OFFERS_FILENAME_RE = re.compile(r"^(\d{8})_Offers\.csv$")
NSP_FILENAME = "NetworkSupplyPointsTable.csv"
HYDRO_STORAGE_SUBDIR = "hydro"
# Filename pattern: {IslandCode}_{SiteCode}_Storage_{LakeName}.csv
HYDRO_STORAGE_FILENAME_RE = re.compile(r"^([A-Z]{2}_[A-Z]{2,4})_Storage_.*\.csv$")

# Maps raw CSV column names (including unicode) to normalised DuckDB column names.
# Source header: Date, Time, Lake level (m), Active storage (Mm³),
#                Active contingent storage (Mm³), QualityCode
HYDRO_COLUMN_MAP = {
    "Date": "date_str",
    "Time": "time_str",
    "Lake level (m)": "level_m",
    "Active storage (Mm³)": "active_storage_mm3",       # Mm³
    "Active contingent storage (Mm³)": "contingent_storage_mm3",
    "QualityCode": "quality_code",
}

# EMI Final Energy Prices: 4 source cols (observed 2026-05; PRD §2.3 originally
# claimed 7 — Island/IsProxyPriceFlag/PublishDateTime do not exist in the file).
PRICE_RAW_COLUMNS = [
    "trading_date", "trading_period", "point_of_connection", "dollars_per_mwh",
]

MARKET_VOLUME_SOURCE_COLUMNS = {
    "PointOfConnection": "point_of_connection",
    "Network": "network",
    "Island": "island",
    "Participant": "participant",
    "TradingDate": "trading_date",
    "TradingPeriod": "trading_period",
    "TradingPeriodStartTime": "trading_period_start_time",
    "FlowDirection": "flow_direction",
    "KilowattHours": "kilowatt_hours",
}
MARKET_VOLUME_RAW_COLUMNS = list(MARKET_VOLUME_SOURCE_COLUMNS.values())

OFFERS_SOURCE_COLUMNS = {
    "TradingDate": "trading_date",
    "TradingPeriod": "trading_period",
    "ParticipantCode": "participant_code",
    "PointOfConnection": "point_of_connection",
    "Unit": "unit",
    "ProductType": "product_type",
    "ProductClass": "product_class",
    "ReserveType": "reserve_type",
    "ProductDescription": "product_description",
    "UTCSubmissionDate": "utc_submission_date",
    "UTCSubmissionTime": "utc_submission_time",
    "SubmissionOrder": "submission_order",
    "IsLatestYesNo": "is_latest_yes_no",
    "Tranche": "tranche",
    "MaximumRampUpMegawattsPerHour": "maximum_ramp_up_mw_per_hour",
    "MaximumRampDownMegawattsPerHour": "maximum_ramp_down_mw_per_hour",
    "PartiallyLoadedSpinningReservePercent": "partially_loaded_spinning_reserve_percent",
    "MaximumOutputMegawatts": "maximum_output_mw",
    "ForecastOfGenerationPotentialMegawatts": "forecast_generation_potential_mw",
    "Megawatts": "megawatts",
    "DollarsPerMegawattHour": "dollars_per_mwh",
}

# EMI Generation_MD column order (verified from the file header — index 4 is
# Fuel_Code, index 6 is Trading_date, indices 7-56 are TP1-TP50).
GENERATION_ID_COLUMNS = [
    "site_code", "poc_code", "nwk_code", "gen_code",
    "fuel_code", "tech_code", "trading_date",
]
GENERATION_TP_COLUMNS = [f"tp{i}" for i in range(1, 51)]
GENERATION_ALL_RAW_COLUMNS = GENERATION_ID_COLUMNS + GENERATION_TP_COLUMNS  # 57


def ensure_raw_generation(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the raw_generation table (idempotent). All columns VARCHAR."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    cols_sql = ",\n    ".join(f'"{c}" VARCHAR' for c in GENERATION_ALL_RAW_COLUMNS)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS raw.raw_generation (
            {cols_sql},
            trading_month VARCHAR,
            _source_file_modified_at TIMESTAMP
        )
        """
    )


def load_generation_file(
    conn: duckdb.DuckDBPyConnection,
    csv_path: Path,
    trading_month: str,
) -> int:
    """Load one Generation_MD CSV into raw_generation transactionally.

    Idempotency: DELETE WHERE trading_month=? then INSERT, all in one txn.
    """
    file_mtime = datetime.fromtimestamp(csv_path.stat().st_mtime, tz=UTC)
    col_list = ", ".join(f'"{c}"' for c in GENERATION_ALL_RAW_COLUMNS)

    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute("DELETE FROM raw.raw_generation WHERE trading_month = ?", [trading_month])
        conn.execute(
            f"""
            INSERT INTO raw.raw_generation
            SELECT
                {col_list},
                ? AS trading_month,
                ? AS _source_file_modified_at
            FROM read_csv(
                ?,
                header=true,
                all_varchar=true,
                columns={{
                    {", ".join(f"'{c}': 'VARCHAR'" for c in GENERATION_ALL_RAW_COLUMNS)}
                }}
            )
            """,
            [trading_month, file_mtime, str(csv_path)],
        )
        row_count = conn.execute(
            "SELECT count(*) FROM raw.raw_generation WHERE trading_month = ?",
            [trading_month],
        ).fetchone()[0]
        conn.execute("COMMIT")
        return row_count
    except Exception:
        conn.execute("ROLLBACK")
        raise


def ensure_raw_price(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.raw_price (
            trading_date VARCHAR,
            trading_period VARCHAR,
            point_of_connection VARCHAR,
            dollars_per_mwh VARCHAR,
            trading_month VARCHAR,
            _source_file_modified_at TIMESTAMP
        )
        """
    )


def load_price_file(
    conn: duckdb.DuckDBPyConnection,
    csv_path: Path,
    trading_month: str,
) -> int:
    file_mtime = datetime.fromtimestamp(csv_path.stat().st_mtime, tz=UTC)
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute("DELETE FROM raw.raw_price WHERE trading_month = ?", [trading_month])
        conn.execute(
            f"""
            INSERT INTO raw.raw_price
            SELECT
                {", ".join(PRICE_RAW_COLUMNS)},
                ? AS trading_month,
                ? AS _source_file_modified_at
            FROM read_csv(
                ?,
                header=true,
                all_varchar=true,
                columns={{
                    {", ".join(f"'{c}': 'VARCHAR'" for c in PRICE_RAW_COLUMNS)}
                }}
            )
            """,
            [trading_month, file_mtime, str(csv_path)],
        )
        row_count = conn.execute(
            "SELECT count(*) FROM raw.raw_price WHERE trading_month = ?",
            [trading_month],
        ).fetchone()[0]
        conn.execute("COMMIT")
        return row_count
    except Exception:
        conn.execute("ROLLBACK")
        raise


def load_all_generation(db_path: Path, source_dir: Path) -> None:
    files = sorted(
        f for f in source_dir.iterdir()
        if GENERATION_FILENAME_RE.match(f.name)
    )
    if not files:
        logger.warning("no Generation_MD files found under %s", source_dir)
        return

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    try:
        ensure_raw_generation(conn)
        total = 0
        for f in files:
            match = GENERATION_FILENAME_RE.match(f.name)
            assert match is not None
            ym = match.group(1)
            rows = load_generation_file(conn, f, ym)
            logger.info("loaded %s — %d rows", f.name, rows)
            total += rows
        logger.info("done — %d files, %d total rows in raw.raw_generation", len(files), total)
    finally:
        conn.close()


def load_nsp(db_path: Path, source_dir: Path) -> None:
    """Full-reload (DELETE all + INSERT) the NSP table.

    NSP is a small daily snapshot (~2.5k rows × 27 cols ≈ 400KB). We treat it
    as a current snapshot — full reload every refresh keeps things trivial.
    All columns kept as VARCHAR for staging-layer cast.
    """
    csv_path = source_dir / NSP_FILENAME
    if not csv_path.exists():
        logger.warning("no NSP file at %s — skipping", csv_path)
        return
    file_mtime = datetime.fromtimestamp(csv_path.stat().st_mtime, tz=UTC)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute("DROP TABLE IF EXISTS raw.raw_nsp")
            conn.execute(
                """
                CREATE TABLE raw.raw_nsp AS
                SELECT *, ? AS _source_file_modified_at
                FROM read_csv(?, header=true, all_varchar=true)
                """,
                [file_mtime, str(csv_path)],
            )
            row_count = conn.execute("SELECT count(*) FROM raw.raw_nsp").fetchone()[0]
            conn.execute("COMMIT")
            logger.info("loaded NSP — %d rows", row_count)
        except Exception:
            conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()


def load_all_price(db_path: Path, source_dir: Path) -> None:
    files = sorted(
        f for f in source_dir.iterdir()
        if PRICE_FILENAME_RE.match(f.name)
    )
    if not files:
        logger.warning("no FinalEnergyPrices files found under %s", source_dir)
        return

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    try:
        ensure_raw_price(conn)
        total = 0
        for f in files:
            match = PRICE_FILENAME_RE.match(f.name)
            assert match is not None
            ym = match.group(1)
            rows = load_price_file(conn, f, ym)
            logger.info("loaded %s — %d rows", f.name, rows)
            total += rows
        logger.info("done — %d files, %d total rows in raw.raw_price", len(files), total)
    finally:
        conn.close()


def ensure_raw_market_volume(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.raw_market_volume (
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
            _source_file_modified_at  TIMESTAMP
        )
        """
    )


def load_market_volume_file(
    conn: duckdb.DuckDBPyConnection,
    csv_path: Path,
    trading_month: str,
) -> int:
    file_mtime = datetime.fromtimestamp(csv_path.stat().st_mtime, tz=UTC)
    select_cols = ",\n                ".join(
        f'"{source}" AS {target}'
        for source, target in MARKET_VOLUME_SOURCE_COLUMNS.items()
    )
    source_columns = ", ".join(
        f"'{source}': 'VARCHAR'" for source in MARKET_VOLUME_SOURCE_COLUMNS
    )

    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute(
            "DELETE FROM raw.raw_market_volume WHERE trading_month = ?",
            [trading_month],
        )
        conn.execute(
            f"""
            INSERT INTO raw.raw_market_volume
            SELECT
                {select_cols},
                ? AS trading_month,
                ? AS _source_file_modified_at
            FROM read_csv(
                ?,
                header=true,
                all_varchar=true,
                columns={{
                    {source_columns}
                }}
            )
            """,
            [trading_month, file_mtime, str(csv_path)],
        )
        row_count = conn.execute(
            "SELECT count(*) FROM raw.raw_market_volume WHERE trading_month = ?",
            [trading_month],
        ).fetchone()[0]
        conn.execute("COMMIT")
        return row_count
    except Exception:
        conn.execute("ROLLBACK")
        raise


def load_all_market_volume(db_path: Path, source_dir: Path) -> None:
    files = sorted(
        f for f in source_dir.iterdir()
        if MARKET_VOLUME_FILENAME_RE.match(f.name)
    )

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    try:
        ensure_raw_market_volume(conn)
        if not files:
            logger.warning("no ReconciledInjectionAndOfftake files found under %s — raw table left empty", source_dir)
            return

        total = 0
        for f in files:
            match = MARKET_VOLUME_FILENAME_RE.match(f.name)
            assert match is not None
            ym = match.group(1)
            rows = load_market_volume_file(conn, f, ym)
            logger.info("loaded %s — %d rows", f.name, rows)
            total += rows
        logger.info("done — %d files, %d total rows in raw.raw_market_volume", len(files), total)
    finally:
        conn.close()


def ensure_raw_offers(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.raw_offers (
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
            _source_file_modified_at                      TIMESTAMP
        )
        """
    )


def load_offers_file(
    conn: duckdb.DuckDBPyConnection,
    csv_path: Path,
    trading_day: str,
) -> int:
    file_mtime = datetime.fromtimestamp(csv_path.stat().st_mtime, tz=UTC)
    trading_date = f"{trading_day[:4]}-{trading_day[4:6]}-{trading_day[6:]}"
    trading_month = trading_day[:6]
    select_cols = ",\n                ".join(
        f'"{source}" AS {target}'
        for source, target in OFFERS_SOURCE_COLUMNS.items()
    )
    source_columns = ", ".join(
        f"'{source}': 'VARCHAR'" for source in OFFERS_SOURCE_COLUMNS
    )

    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute(
            "DELETE FROM raw.raw_offers WHERE trading_date = ?",
            [trading_date],
        )
        conn.execute(
            f"""
            INSERT INTO raw.raw_offers
            SELECT
                {select_cols},
                ? AS trading_month,
                ? AS _source_file_modified_at
            FROM read_csv(
                ?,
                header=true,
                all_varchar=true,
                columns={{
                    {source_columns}
                }}
            )
            """,
            [trading_month, file_mtime, str(csv_path)],
        )
        row_count = conn.execute(
            "SELECT count(*) FROM raw.raw_offers WHERE trading_date = ?",
            [trading_date],
        ).fetchone()[0]
        conn.execute("COMMIT")
        return row_count
    except Exception:
        conn.execute("ROLLBACK")
        raise


def load_all_offers(db_path: Path, source_dir: Path) -> None:
    files = sorted(
        f for f in source_dir.iterdir()
        if OFFERS_FILENAME_RE.match(f.name)
    )

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    try:
        ensure_raw_offers(conn)
        if not files:
            logger.warning("no Offers files found under %s — raw table left empty", source_dir)
            return

        total = 0
        for f in files:
            match = OFFERS_FILENAME_RE.match(f.name)
            assert match is not None
            trading_day = match.group(1)
            rows = load_offers_file(conn, f, trading_day)
            logger.info("loaded %s — %d rows", f.name, rows)
            total += rows
        logger.info("done — %d files, %d total rows in raw.raw_offers", len(files), total)
    finally:
        conn.close()


def ensure_raw_hydro_storage(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.raw_hydro_storage (
            site_code                 VARCHAR,
            date_str                  VARCHAR,
            time_str                  VARCHAR,
            level_m                   VARCHAR,
            active_storage_mm3        VARCHAR,
            contingent_storage_mm3    VARCHAR,
            quality_code              VARCHAR,
            _source_file_modified_at  TIMESTAMP
        )
        """
    )


def load_hydro_file(
    conn: duckdb.DuckDBPyConnection,
    csv_path: Path,
    site_code: str,
) -> int:
    """Load one lake storage CSV into raw_hydro_storage transactionally.

    Idempotency: DELETE WHERE site_code=? then INSERT, all in one txn.
    Reads via pandas so unicode column names are normalised before insertion.
    """
    import pandas as pd

    file_mtime = datetime.fromtimestamp(csv_path.stat().st_mtime, tz=UTC)
    df = pd.read_csv(csv_path, dtype=str)
    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns=HYDRO_COLUMN_MAP)
    df["site_code"] = site_code
    df["_source_file_modified_at"] = str(file_mtime)

    keep = [
        "site_code", "date_str", "time_str", "level_m",
        "active_storage_mm3", "contingent_storage_mm3",
        "quality_code", "_source_file_modified_at",
    ]
    df = df[[c for c in keep if c in df.columns]]

    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute(
            "DELETE FROM raw.raw_hydro_storage WHERE site_code = ?", [site_code]
        )
        conn.register("_tmp_hydro", df)
        conn.execute("INSERT INTO raw.raw_hydro_storage SELECT * FROM _tmp_hydro")
        conn.unregister("_tmp_hydro")
        row_count = conn.execute(
            "SELECT count(*) FROM raw.raw_hydro_storage WHERE site_code = ?",
            [site_code],
        ).fetchone()[0]
        conn.execute("COMMIT")
        return row_count
    except Exception:
        conn.execute("ROLLBACK")
        raise


def load_hydro(db_path: Path, source_dir: Path) -> None:
    """Load all lake storage CSVs from source_dir/hydro/ into raw_hydro_storage."""
    hydro_dir = source_dir / HYDRO_STORAGE_SUBDIR
    if not hydro_dir.exists():
        logger.warning("no hydro directory at %s — skipping", hydro_dir)
        return

    files = sorted(
        f for f in hydro_dir.iterdir()
        if HYDRO_STORAGE_FILENAME_RE.match(f.name)
    )
    if not files:
        logger.warning("no hydro storage CSVs found in %s — skipping", hydro_dir)
        return

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    try:
        ensure_raw_hydro_storage(conn)
        total = 0
        for f in files:
            match = HYDRO_STORAGE_FILENAME_RE.match(f.name)
            assert match is not None
            site_code = match.group(1)
            rows = load_hydro_file(conn, f, site_code)
            logger.info("loaded %s — %d rows (site=%s)", f.name, rows, site_code)
            total += rows
        logger.info(
            "done — %d files, %d total rows in raw.raw_hydro_storage", len(files), total
        )
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Load EMI CSVs into local DuckDB")
    p.add_argument("--db", required=True, help="Path to .duckdb file (created if missing)")
    p.add_argument("--source", required=True, help="Directory containing raw CSVs")
    args = p.parse_args(argv)

    load_all_generation(Path(args.db), Path(args.source))
    load_all_price(Path(args.db), Path(args.source))
    load_all_market_volume(Path(args.db), Path(args.source))
    load_all_offers(Path(args.db), Path(args.source))
    load_nsp(Path(args.db), Path(args.source))
    load_hydro(Path(args.db), Path(args.source))
    return 0


if __name__ == "__main__":
    sys.exit(main())

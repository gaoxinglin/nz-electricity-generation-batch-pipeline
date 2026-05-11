"""
Idempotent CSV → DuckDB loader for local development.

Scans `data/raw/` for EMI dataset files and loads each into a DuckDB database.
Each file is loaded in its own transaction (BEGIN / COMMIT / ROLLBACK), so a
partial run never leaves the warehouse half-loaded.

Currently handles:
  - {YYYYMM}_Generation_MD.csv  → raw_generation

Phase 1 will add: {YYYYMM}_FinalEnergyPrices.csv → raw_price
Phase 2 will add: NetworkSupplyPointsTable.csv  → raw_nsp

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
from datetime import datetime, timezone
from pathlib import Path

import duckdb

logger = logging.getLogger("load_local")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

GENERATION_FILENAME_RE = re.compile(r"^(\d{6})_Generation_MD\.csv$")

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
    file_mtime = datetime.fromtimestamp(csv_path.stat().st_mtime, tz=timezone.utc)
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


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Load EMI CSVs into local DuckDB")
    p.add_argument("--db", required=True, help="Path to .duckdb file (created if missing)")
    p.add_argument("--source", required=True, help="Directory containing raw CSVs")
    args = p.parse_args(argv)

    load_all_generation(Path(args.db), Path(args.source))
    return 0


if __name__ == "__main__":
    sys.exit(main())

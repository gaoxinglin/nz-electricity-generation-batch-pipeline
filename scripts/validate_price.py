"""
Strict schema validator for EMI Final Energy Prices CSVs.

Observed schema (4 columns):
    TradingDate, TradingPeriod, PointOfConnection, DollarsPerMegawattHour

Fails loudly on:
  - column name or count mismatch
  - rows where trading_date's YYYYMM does not match the filename
  - non-numeric DollarsPerMegawattHour (NULL/empty OK)
  - TradingPeriod outside 1..50 (NZ DST range)

Usage:
    python scripts/validate_price.py data/raw/202401_FinalEnergyPrices.csv
    python scripts/validate_price.py --check-completeness data/raw/ \\
        --start 2024-01 --end 2024-12
"""

from __future__ import annotations

import argparse
import csv
import logging
import re
import sys
from pathlib import Path

logger = logging.getLogger("validate_price")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

EXPECTED_PRICE_COLUMNS = [
    "TradingDate", "TradingPeriod", "PointOfConnection", "DollarsPerMegawattHour",
]
PRICE_FILENAME_RE = re.compile(r"^(\d{6})_FinalEnergyPrices\.csv$")


class SchemaValidationError(Exception):
    pass


class DataCompletenessError(Exception):
    pass


def validate_file(csv_path: Path) -> int:
    """Return row count if valid, else raise SchemaValidationError."""
    m = PRICE_FILENAME_RE.match(csv_path.name)
    if not m:
        raise SchemaValidationError(f"unexpected filename: {csv_path.name}")
    expected_ym = m.group(1)

    with csv_path.open(encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        if header != EXPECTED_PRICE_COLUMNS:
            raise SchemaValidationError(
                f"{csv_path.name}: schema mismatch.\n"
                f"  expected: {EXPECTED_PRICE_COLUMNS}\n"
                f"  got:      {header}"
            )

        row_count = 0
        for row in reader:
            row_count += 1
            if len(row) != 4:
                raise SchemaValidationError(
                    f"{csv_path.name} row {row_count}: expected 4 columns, got {len(row)}"
                )
            td, tp, _poc, price = row
            if len(td) != 10 or td[4] != "-":
                raise SchemaValidationError(
                    f"{csv_path.name} row {row_count}: bad TradingDate {td!r}"
                )
            row_ym = td[:7].replace("-", "")
            if row_ym != expected_ym:
                raise SchemaValidationError(
                    f"{csv_path.name} row {row_count}: TradingDate month {row_ym} "
                    f"does not match filename {expected_ym}"
                )
            try:
                tp_int = int(tp)
            except ValueError as e:
                raise SchemaValidationError(
                    f"{csv_path.name} row {row_count}: bad TradingPeriod {tp!r}"
                ) from e
            if not (1 <= tp_int <= 50):
                raise SchemaValidationError(
                    f"{csv_path.name} row {row_count}: TradingPeriod {tp_int} out of 1..50"
                )
            if price.strip() and price.upper() != "NULL":
                try:
                    float(price)
                except ValueError as e:
                    raise SchemaValidationError(
                        f"{csv_path.name} row {row_count}: bad price {price!r}"
                    ) from e

    if row_count < 100:
        raise SchemaValidationError(
            f"{csv_path.name}: only {row_count} rows — suspiciously small"
        )
    return row_count


def _yymm_list(start_ym: str, end_ym: str) -> list[str]:
    sy, sm = int(start_ym[:4]), int(start_ym[5:])
    ey, em = int(end_ym[:4]), int(end_ym[5:])
    out: list[str] = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        out.append(f"{y:04d}{m:02d}")
        m += 1
        if m == 13:
            m = 1
            y += 1
    return out


def check_month_completeness(raw_dir: Path, start_ym: str, end_ym: str) -> None:
    expected = set(_yymm_list(start_ym, end_ym))
    actual = {
        m.group(1)
        for p in raw_dir.iterdir()
        if (m := PRICE_FILENAME_RE.match(p.name))
    }
    missing = sorted(expected - actual)
    if missing:
        raise DataCompletenessError(
            f"Missing price months: {missing}. "
            f"Re-run: python scripts/download_price.py --year-month <YYYYMM> per missing month."
        )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Validate EMI Final Energy Prices CSVs")
    p.add_argument("paths", nargs="*", help="CSV files to validate")
    p.add_argument("--check-completeness", metavar="DIR",
                   help="Directory to scan for month-completeness check")
    p.add_argument("--start", help="YYYY-MM, with --check-completeness")
    p.add_argument("--end", help="YYYY-MM, with --check-completeness")
    args = p.parse_args(argv)

    if args.check_completeness:
        if not (args.start and args.end):
            raise SystemExit("--check-completeness requires --start and --end (YYYY-MM)")
        check_month_completeness(Path(args.check_completeness), args.start, args.end)
        logger.info("month completeness OK for %s..%s", args.start, args.end)

    for raw in args.paths:
        path = Path(raw)
        if not path.exists():
            raise SystemExit(f"not found: {path}")
        rows = validate_file(path)
        logger.info("OK %s — %d rows", path.name, rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())

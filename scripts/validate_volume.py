"""
Validate EMI reconciled injection/offtake volume CSVs.

Expected source header:
  PointOfConnection, Network, Island, Participant, TradingDate,
  TradingPeriod, TradingPeriodStartTime, FlowDirection, KilowattHours
"""

from __future__ import annotations

import argparse
import csv
import gzip
import logging
import re
import sys
from pathlib import Path

logger = logging.getLogger("validate_volume")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

EXPECTED_HEADER = [
    "PointOfConnection",
    "Network",
    "Island",
    "Participant",
    "TradingDate",
    "TradingPeriod",
    "TradingPeriodStartTime",
    "FlowDirection",
    "KilowattHours",
]
FILENAME_RE = re.compile(r"^(\d{6})_ReconciledInjectionAndOfftake\.csv(?:\.gz)?$")


class SchemaValidationError(ValueError):
    pass


def _open_text(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", newline="")
    return path.open("r", encoding="utf-8", newline="")


def validate_file(csv_path: Path) -> int:
    match = FILENAME_RE.match(csv_path.name)
    if not match:
        raise SchemaValidationError(
            f"{csv_path.name}: expected YYYYMM_ReconciledInjectionAndOfftake.csv[.gz]"
        )
    expected_ym = match.group(1)

    with _open_text(csv_path) as fh:
        reader = csv.reader(fh)
        try:
            header = next(reader)
        except StopIteration as exc:
            raise SchemaValidationError(f"{csv_path.name}: empty file") from exc

        if header != EXPECTED_HEADER:
            raise SchemaValidationError(
                f"{csv_path.name}: unexpected header {header!r}; expected {EXPECTED_HEADER!r}"
            )

        row_count = 0
        for row in reader:
            row_count += 1
            if len(row) != len(EXPECTED_HEADER):
                raise SchemaValidationError(
                    f"{csv_path.name} row {row_count}: expected {len(EXPECTED_HEADER)} columns, got {len(row)}"
                )
            poc, _network, island, _participant, trading_date, tp, _tp_start, flow, kwh = row
            if not poc:
                raise SchemaValidationError(f"{csv_path.name} row {row_count}: empty PointOfConnection")
            if island not in {"NI", "SI"}:
                raise SchemaValidationError(f"{csv_path.name} row {row_count}: bad Island {island!r}")
            if len(trading_date) != 10 or trading_date[4] != "-" or trading_date[7] != "-":
                raise SchemaValidationError(f"{csv_path.name} row {row_count}: bad TradingDate {trading_date!r}")
            if trading_date[:7].replace("-", "") != expected_ym:
                raise SchemaValidationError(
                    f"{csv_path.name} row {row_count}: TradingDate month mismatch {trading_date!r}"
                )
            try:
                tp_int = int(tp)
            except ValueError as exc:
                raise SchemaValidationError(f"{csv_path.name} row {row_count}: bad TradingPeriod {tp!r}") from exc
            if not 1 <= tp_int <= 50:
                raise SchemaValidationError(
                    f"{csv_path.name} row {row_count}: TradingPeriod {tp_int} out of 1..50"
                )
            if flow not in {"Injection", "Offtake"}:
                raise SchemaValidationError(f"{csv_path.name} row {row_count}: bad FlowDirection {flow!r}")
            try:
                float(kwh)
            except ValueError as exc:
                raise SchemaValidationError(f"{csv_path.name} row {row_count}: bad KilowattHours {kwh!r}") from exc

    if row_count < 100:
        raise SchemaValidationError(f"{csv_path.name}: only {row_count} rows - suspiciously small")
    return row_count


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate EMI volume CSV")
    parser.add_argument("files", nargs="+")
    args = parser.parse_args(argv)

    for file_name in args.files:
        rows = validate_file(Path(file_name))
        logger.info("%s OK - %d rows", file_name, rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())

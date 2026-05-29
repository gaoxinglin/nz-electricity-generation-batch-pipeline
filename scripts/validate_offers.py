"""
Validate EMI daily offer CSVs.

Expected source header:
  TradingDate, TradingPeriod, ParticipantCode, PointOfConnection, Unit,
  ProductType, ProductClass, ReserveType, ProductDescription,
  UTCSubmissionDate, UTCSubmissionTime, SubmissionOrder, IsLatestYesNo,
  Tranche, MaximumRampUpMegawattsPerHour,
  MaximumRampDownMegawattsPerHour,
  PartiallyLoadedSpinningReservePercent, MaximumOutputMegawatts,
  ForecastOfGenerationPotentialMegawatts, Megawatts,
  DollarsPerMegawattHour
"""

from __future__ import annotations

import argparse
import csv
import logging
import re
import sys
from pathlib import Path

logger = logging.getLogger("validate_offers")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

EXPECTED_HEADER = [
    "TradingDate",
    "TradingPeriod",
    "ParticipantCode",
    "PointOfConnection",
    "Unit",
    "ProductType",
    "ProductClass",
    "ReserveType",
    "ProductDescription",
    "UTCSubmissionDate",
    "UTCSubmissionTime",
    "SubmissionOrder",
    "IsLatestYesNo",
    "Tranche",
    "MaximumRampUpMegawattsPerHour",
    "MaximumRampDownMegawattsPerHour",
    "PartiallyLoadedSpinningReservePercent",
    "MaximumOutputMegawatts",
    "ForecastOfGenerationPotentialMegawatts",
    "Megawatts",
    "DollarsPerMegawattHour",
]
FILENAME_RE = re.compile(r"^(\d{8})_Offers\.csv$")


class SchemaValidationError(ValueError):
    pass


def _is_iso_date(value: str) -> bool:
    return len(value) == 10 and value[4] == "-" and value[7] == "-"


def _require_float(csv_path: Path, row_count: int, value: str, column: str) -> None:
    try:
        float(value)
    except ValueError as exc:
        raise SchemaValidationError(
            f"{csv_path.name} row {row_count}: bad {column} {value!r}"
        ) from exc


def validate_file(csv_path: Path) -> int:
    match = FILENAME_RE.match(csv_path.name)
    if not match:
        raise SchemaValidationError(f"{csv_path.name}: expected YYYYMMDD_Offers.csv")
    expected_date = f"{match.group(1)[:4]}-{match.group(1)[4:6]}-{match.group(1)[6:]}"

    with csv_path.open("r", encoding="utf-8", newline="") as fh:
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

            trading_date = row[0]
            trading_period = row[1]
            participant = row[2]
            poc = row[3]
            unit = row[4]
            product_type = row[5]
            product_class = row[6]
            utc_submission_date = row[9]
            submission_order = row[11]
            is_latest = row[12]
            tranche = row[13]
            megawatts = row[19]
            dollars_per_mwh = row[20]

            if trading_date != expected_date:
                raise SchemaValidationError(
                    f"{csv_path.name} row {row_count}: TradingDate mismatch {trading_date!r}"
                )
            if not participant:
                raise SchemaValidationError(f"{csv_path.name} row {row_count}: empty ParticipantCode")
            if not poc:
                raise SchemaValidationError(f"{csv_path.name} row {row_count}: empty PointOfConnection")
            if not product_type:
                raise SchemaValidationError(f"{csv_path.name} row {row_count}: empty ProductType")
            if not product_class:
                raise SchemaValidationError(f"{csv_path.name} row {row_count}: empty ProductClass")
            if product_type == "Energy" and product_class == "Injection" and not unit:
                raise SchemaValidationError(
                    f"{csv_path.name} row {row_count}: empty Unit for Energy/Injection offer"
                )
            if not _is_iso_date(utc_submission_date):
                raise SchemaValidationError(
                    f"{csv_path.name} row {row_count}: bad UTCSubmissionDate {utc_submission_date!r}"
                )
            try:
                tp_int = int(trading_period)
            except ValueError as exc:
                raise SchemaValidationError(
                    f"{csv_path.name} row {row_count}: bad TradingPeriod {trading_period!r}"
                ) from exc
            if not 1 <= tp_int <= 50:
                raise SchemaValidationError(
                    f"{csv_path.name} row {row_count}: TradingPeriod {tp_int} out of 1..50"
                )
            try:
                int(submission_order)
            except ValueError as exc:
                raise SchemaValidationError(
                    f"{csv_path.name} row {row_count}: bad SubmissionOrder {submission_order!r}"
                ) from exc
            if is_latest not in {"Y", "N"}:
                raise SchemaValidationError(
                    f"{csv_path.name} row {row_count}: bad IsLatestYesNo {is_latest!r}"
                )
            try:
                int(tranche)
            except ValueError as exc:
                raise SchemaValidationError(
                    f"{csv_path.name} row {row_count}: bad Tranche {tranche!r}"
                ) from exc
            _require_float(csv_path, row_count, megawatts, "Megawatts")
            _require_float(csv_path, row_count, dollars_per_mwh, "DollarsPerMegawattHour")

    if row_count < 20:
        raise SchemaValidationError(f"{csv_path.name}: only {row_count} rows - suspiciously small")
    return row_count


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate EMI daily offer CSV")
    parser.add_argument("files", nargs="+")
    args = parser.parse_args(argv)

    for file_name in args.files:
        rows = validate_file(Path(file_name))
        logger.info("%s OK - %d rows", file_name, rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())

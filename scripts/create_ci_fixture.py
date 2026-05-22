"""
Create a deterministic raw-data fixture for CI end-to-end runs.

The fixture intentionally mirrors the public EMI CSV shapes, but stays tiny
enough for GitHub Actions:
  - one month of wide Generation_MD rows
  - one month of FinalEnergyPrices rows
  - current NSP rows for the fixture POCs
  - one hydro storage lake file

It is consumed by scripts/load_local.py, so CI exercises the same raw CSV to
DuckDB path used by local demos.
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import date
from pathlib import Path

YEAR = 2024
MONTH = 1
TRADING_MONTH = f"{YEAR}{MONTH:02d}"
TRADING_DATES = [date(YEAR, MONTH, day) for day in range(1, 32)]
TRADING_PERIODS = range(1, 51)

GENERATION_HEADER = [
    "Site_Code",
    "POC_Code",
    "Nwk_Code",
    "Gen_Code",
    "Fuel_Code",
    "Tech_Code",
    "Trading_Date",
    *[f"TP{i}" for i in TRADING_PERIODS],
]

GENERATION_PLANTS = [
    {
        "site_code": "CI_HYD",
        "poc_code": "POC_NI",
        "nwk_code": "NWK_NI",
        "gen_code": "GEN_HYD_1",
        "fuel_code": "Hydro",
        "tech_code": "TURB",
        "base_kwh": 1200,
    },
    {
        "site_code": "CI_GAS",
        "poc_code": "POC_NI",
        "nwk_code": "NWK_NI",
        "gen_code": "GEN_GAS_1",
        "fuel_code": "Gas",
        "tech_code": "CCGT",
        "base_kwh": 650,
    },
    {
        "site_code": "CI_WIN",
        "poc_code": "POC_SI",
        "nwk_code": "NWK_SI",
        "gen_code": "GEN_WIN_1",
        "fuel_code": "Wind",
        "tech_code": "TURB",
        "base_kwh": 420,
    },
    {
        "site_code": "CI_SOL",
        "poc_code": "POC_SI",
        "nwk_code": "NWK_SI",
        "gen_code": "GEN_SOL_1",
        "fuel_code": "Solar",
        "tech_code": "PV",
        "base_kwh": 180,
    },
]

PRICE_HEADER = [
    "TradingDate",
    "TradingPeriod",
    "PointOfConnection",
    "DollarsPerMegawattHour",
]

NSP_HEADER = [
    "Current flag",
    "NSP",
    "NSP replaced by",
    "POC code",
    "Network participant",
    "Embedded under POC code",
    "Embedded under network participant",
    "Reconciliation type",
    "X flow",
    "I flow",
    "Description",
    "NZTM easting",
    "NZTM northing",
    "Network reporting region ID",
    "Network reporting region",
    "Zone",
    "Island",
    "Start date",
    "Start TP",
    "End date",
    "End TP",
    "SB ICP",
    "Balancing code",
    "MEP",
    "Responsible participant",
    "Certification expiry",
    "Metering information exemption expiry date",
]

HYDRO_HEADER = [
    "Date",
    "Time",
    "Lake level (m)",
    "Active storage (Mm\u00b3)",
    "Active contingent storage (Mm\u00b3)",
    "QualityCode",
]


def write_csv(path: Path, header: list[str], rows: list[list[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)


def generation_rows() -> list[list[object]]:
    rows: list[list[object]] = []
    for trading_date in TRADING_DATES:
        for plant in GENERATION_PLANTS:
            values = [
                plant["base_kwh"] + trading_date.day * 3 + tp
                for tp in TRADING_PERIODS
            ]
            rows.append([
                plant["site_code"],
                plant["poc_code"],
                plant["nwk_code"],
                plant["gen_code"],
                plant["fuel_code"],
                plant["tech_code"],
                trading_date.isoformat(),
                *values,
            ])
    return rows


def price_rows() -> list[list[object]]:
    rows: list[list[object]] = []
    for trading_date in TRADING_DATES:
        for poc_code, base_price in [("POC_NI", 82.0), ("POC_SI", 68.0)]:
            for tp in TRADING_PERIODS:
                price = base_price + (trading_date.day % 7) * 1.5 + tp * 0.12
                if poc_code == "POC_NI" and trading_date.day == 15 and tp == 18:
                    price = 450.0
                if poc_code == "POC_SI" and trading_date.day == 20 and tp == 22:
                    price = -12.5
                rows.append([
                    trading_date.isoformat(),
                    tp,
                    poc_code,
                    f"{price:.2f}",
                ])
    return rows


def nsp_rows() -> list[list[object]]:
    return [
        [
            "1",
            "NSP_NI",
            "",
            "POC_NI",
            "CI Network North",
            "",
            "",
            "GEN",
            "Y",
            "N",
            "CI North fixture POC",
            "1765000",
            "5910000",
            "NI",
            "North Island",
            "Upper North",
            "NI",
            "2020-01-01",
            "1",
            "",
            "",
            "",
            "",
            "",
            "CI Participant",
            "",
            "",
        ],
        [
            "1",
            "NSP_SI",
            "",
            "POC_SI",
            "CI Network South",
            "",
            "",
            "GEN",
            "Y",
            "N",
            "CI South fixture POC",
            "1375000",
            "5170000",
            "SI",
            "South Island",
            "Lower South",
            "SI",
            "2020-01-01",
            "1",
            "",
            "",
            "",
            "",
            "",
            "CI Participant",
            "",
            "",
        ],
    ]


def hydro_rows() -> list[list[object]]:
    rows: list[list[object]] = []
    for trading_date in TRADING_DATES:
        rows.append([
            trading_date.isoformat(),
            "23:59:59",
            f"{356.0 + trading_date.day * 0.05:.3f}",
            f"{570.0 + trading_date.day * 1.75:.3f}",
            f"{25.0 + trading_date.day * 0.2:.3f}",
            "320",
        ])
    return rows


def create_fixture(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(
        output_dir / f"{TRADING_MONTH}_Generation_MD.csv",
        GENERATION_HEADER,
        generation_rows(),
    )
    write_csv(
        output_dir / f"{TRADING_MONTH}_FinalEnergyPrices.csv",
        PRICE_HEADER,
        price_rows(),
    )
    write_csv(output_dir / "NetworkSupplyPointsTable.csv", NSP_HEADER, nsp_rows())
    write_csv(
        output_dir / "hydro" / "NI_TPO_Storage_LakeTaupo.csv",
        HYDRO_HEADER,
        hydro_rows(),
    )
    (output_dir / "hydro" / "_release.txt").write_text("ci-fixture\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Create a deterministic CI raw-data fixture")
    parser.add_argument("--output", required=True, help="Directory to write raw fixture CSVs into")
    args = parser.parse_args(argv)

    create_fixture(Path(args.output))
    return 0


if __name__ == "__main__":
    sys.exit(main())

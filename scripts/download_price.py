"""
Download EMI Final Energy Prices CSVs.

URL strategy (per PRD §2.3 + observed EMI behaviour 2026-05):
  - Try ByMonth first: …/ByMonth/{YYYYMM}_FinalEnergyPrices.csv
  - On 404 (or month > BYMONTH_CUTOFF), fall back to daily files and stitch:
    …/{YYYYMMDD}_FinalEnergyPrices.csv

Observed schema (4 columns, NOT 7 as PRD §2.3 originally claimed):
    TradingDate, TradingPeriod, PointOfConnection, DollarsPerMegawattHour

Island / IsProxyPriceFlag / PublishDateTime columns do not appear in either
ByMonth or daily files — Island enrichment is deferred to dim_node (Phase 2)
and is_proxy is carried as a FALSE placeholder by stg_price.

Usage:
    python scripts/download_price.py --output data/raw/ --months 1
    python scripts/download_price.py --output data/raw/ --year-month 202401
"""

from __future__ import annotations

import argparse
import calendar
import logging
import sys
import time
from datetime import date
from pathlib import Path

import requests

logger = logging.getLogger("download_price")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

BYMONTH_BASE = (
    "https://emidatasets.blob.core.windows.net/publicdata/Datasets/"
    "Wholesale/DispatchAndPricing/FinalEnergyPrices/ByMonth"
)
DAILY_BASE = (
    "https://emidatasets.blob.core.windows.net/publicdata/Datasets/"
    "Wholesale/DispatchAndPricing/FinalEnergyPrices"
)
HISTORICAL_START = (2016, 1)
REQUEST_TIMEOUT = 60
# Default cutoff after which we prefer daily-stitch over ByMonth. PRD §10.1
# said ByMonth stops at 2024-12, but observed EMI behaviour 2026-05 shows
# ByMonth still working through 202604. Keep the knob; default permissive.
DEFAULT_BYMONTH_CUTOFF = "9999-99"


def last_complete_month(today: date) -> tuple[int, int]:
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def shift_months(end: tuple[int, int], n_months: int) -> tuple[int, int]:
    y, m = end
    total = y * 12 + (m - 1) - (n_months - 1)
    return total // 12, total % 12 + 1


def month_range(start: tuple[int, int], end: tuple[int, int]) -> list[tuple[int, int]]:
    sy, sm = start
    ey, em = end
    out: list[tuple[int, int]] = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        out.append((y, m))
        m += 1
        if m == 13:
            m = 1
            y += 1
    return out


def resolve_months(months: int | None, years: int | None, year_month: str | None) -> list[tuple[int, int]]:
    today = date.today()
    end = last_complete_month(today)
    if year_month:
        if len(year_month) != 6 or not year_month.isdigit():
            raise SystemExit(f"--year-month must be YYYYMM (got {year_month!r})")
        return [(int(year_month[:4]), int(year_month[4:]))]
    if months is not None:
        if months < 1:
            raise SystemExit("--months must be >= 1")
        return month_range(shift_months(end, months), end)
    if years is not None:
        if years < 1:
            raise SystemExit("--years must be >= 1")
        return month_range(shift_months(end, years * 12), end)
    return month_range(HISTORICAL_START, end)


def _retry_get(url: str) -> requests.Response | None:
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_exc = exc
            wait = 30 * (2 ** attempt)
            logger.warning("attempt %d failed (%s); retrying in %ds", attempt + 1, exc, wait)
            time.sleep(wait)
    raise RuntimeError(f"download failed: {url}") from last_exc


def download_bymonth(year: int, month: int, output_dir: Path) -> Path | None:
    filename = f"{year}{month:02d}_FinalEnergyPrices.csv"
    url = f"{BYMONTH_BASE}/{filename}"
    dest = output_dir / filename
    if dest.exists() and dest.stat().st_size > 0:
        logger.info("skip (present): %s", filename)
        return dest
    resp = _retry_get(url)
    if resp is None:
        return None
    dest.write_bytes(resp.content)
    logger.info("downloaded ByMonth %s (%d bytes)", filename, len(resp.content))
    return dest


def download_daily_stitch(year: int, month: int, output_dir: Path) -> Path | None:
    """Fetch each day's file for the month, concat into one month CSV.

    Header is taken from the first successful daily file. Missing days are
    logged but tolerated (rare gaps near month boundary).
    """
    days_in_month = calendar.monthrange(year, month)[1]
    rows: list[bytes] = []
    header: bytes | None = None
    fetched = 0
    for d in range(1, days_in_month + 1):
        day = date(year, month, d)
        daily_name = f"{day:%Y%m%d}_FinalEnergyPrices.csv"
        resp = _retry_get(f"{DAILY_BASE}/{daily_name}")
        if resp is None:
            logger.warning("daily 404: %s", daily_name)
            continue
        lines = resp.content.splitlines(keepends=True)
        if not lines:
            continue
        if header is None:
            header = lines[0]
            rows.append(header)
        rows.extend(lines[1:])
        fetched += 1
    if fetched == 0:
        return None

    out_name = f"{year}{month:02d}_FinalEnergyPrices.csv"
    dest = output_dir / out_name
    dest.write_bytes(b"".join(rows))
    logger.info("stitched daily → %s (%d days, %d bytes)", out_name, fetched, dest.stat().st_size)
    return dest


def fetch_month(year: int, month: int, output_dir: Path, bymonth_cutoff_ym: str) -> Path | None:
    ym = f"{year:04d}-{month:02d}"
    if ym <= bymonth_cutoff_ym:
        path = download_bymonth(year, month, output_dir)
        if path is not None:
            return path
        logger.info("ByMonth 404 for %s — falling back to daily stitch", ym)
    return download_daily_stitch(year, month, output_dir)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Download EMI Final Energy Prices")
    p.add_argument("--output", required=True)
    p.add_argument("--bymonth-cutoff", default=DEFAULT_BYMONTH_CUTOFF,
                   help="YYYY-MM. Months > this cutoff skip ByMonth and stitch from daily.")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--months", type=int)
    g.add_argument("--years", type=int)
    g.add_argument("--year-month")
    args = p.parse_args(argv)

    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    targets = resolve_months(args.months, args.years, args.year_month)
    logger.info("resolved %d month(s): %s", len(targets), [f"{y}{m:02d}" for y, m in targets])
    fetched = 0
    for y, m in targets:
        if fetch_month(y, m, output, args.bymonth_cutoff) is not None:
            fetched += 1
    logger.info("done — %d/%d months fetched", fetched, len(targets))
    return 0


if __name__ == "__main__":
    sys.exit(main())

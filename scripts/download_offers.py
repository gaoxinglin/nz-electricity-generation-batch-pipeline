"""
Download EMI energy offer CSVs.

The public blob container publishes one daily file under:
  Datasets/Wholesale/BidsAndOffers/Offers/{YYYY}/{YYYYMMDD}_Offers.csv

Offer files are large (hundreds of MB per day in recent months), so this
script defaults to recent-day samples instead of full history. Use --date for
one day, --year-month when deliberately backfilling a whole month, or --days
for the latest N complete days.

Usage:
    python scripts/download_offers.py --output data/raw/ --date 20260401
    python scripts/download_offers.py --output data/raw/ --days 1
"""

from __future__ import annotations

import argparse
import calendar
import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import requests

logger = logging.getLogger("download_offers")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

OFFERS_BASE = (
    "https://emidatasets.blob.core.windows.net/publicdata/Datasets/"
    "Wholesale/BidsAndOffers/Offers"
)
HISTORICAL_START = date(2016, 1, 1)
REQUEST_TIMEOUT = 120


def _parse_day(value: str) -> date:
    if len(value) != 8 or not value.isdigit():
        raise SystemExit(f"date must be YYYYMMDD (got {value!r})")
    try:
        return date(int(value[:4]), int(value[4:6]), int(value[6:]))
    except ValueError as exc:
        raise SystemExit(f"bad date {value!r}") from exc


def _parse_year_month(value: str) -> tuple[int, int]:
    if len(value) != 6 or not value.isdigit():
        raise SystemExit(f"--year-month must be YYYYMM (got {value!r})")
    year = int(value[:4])
    month = int(value[4:])
    if not 1 <= month <= 12:
        raise SystemExit(f"bad month in --year-month {value!r}")
    return year, month


def last_complete_day(today: date) -> date:
    return today - timedelta(days=1)


def day_range(start: date, end: date) -> list[date]:
    if end < start:
        return []
    out: list[date] = []
    current = start
    while current <= end:
        out.append(current)
        current += timedelta(days=1)
    return out


def month_days(year: int, month: int, latest_day: date) -> list[date]:
    start = date(year, month, 1)
    end = date(year, month, calendar.monthrange(year, month)[1])
    return day_range(start, min(end, latest_day))


def resolve_days(day: str | None, year_month: str | None, days: int | None) -> list[date]:
    latest = last_complete_day(date.today())
    if day:
        target = _parse_day(day)
        if target > latest:
            raise SystemExit(f"{target:%Y%m%d} is not a complete trading day yet")
        return [target]
    if year_month:
        year, month = _parse_year_month(year_month)
        targets = month_days(year, month, latest)
        if not targets:
            raise SystemExit(f"no complete offer days available for {year_month}")
        return targets
    if days is not None:
        if days < 1:
            raise SystemExit("--days must be >= 1")
        start = latest - timedelta(days=days - 1)
        if start < HISTORICAL_START:
            start = HISTORICAL_START
        return day_range(start, latest)
    return [latest]


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


def fetch_day(trading_day: date, output_dir: Path, force: bool = False) -> Path | None:
    filename = f"{trading_day:%Y%m%d}_Offers.csv"
    dest = output_dir / filename
    if dest.exists() and dest.stat().st_size > 0 and not force:
        logger.info("skip (present): %s", filename)
        return dest

    url = f"{OFFERS_BASE}/{trading_day:%Y}/{filename}"
    resp = _retry_get(url)
    if resp is None:
        logger.warning("offer file not found: %s", filename)
        return None

    dest.write_bytes(resp.content)
    logger.info("downloaded %s (%d bytes)", filename, len(resp.content))
    return dest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Download EMI daily offer CSVs")
    parser.add_argument("--output", required=True)
    parser.add_argument("--force", action="store_true", help="Re-download even when the file is present")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--date", help="Single trading day, YYYYMMDD")
    group.add_argument("--year-month", help="Whole month, YYYYMM")
    group.add_argument("--days", type=int, help="Latest N complete trading days")
    args = parser.parse_args(argv)

    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    targets = resolve_days(args.date, args.year_month, args.days)
    logger.info("resolved %d day(s): %s", len(targets), [f"{d:%Y%m%d}" for d in targets])

    fetched = 0
    for trading_day in targets:
        if fetch_day(trading_day, output, force=args.force) is not None:
            fetched += 1
    logger.info("done - %d/%d days fetched", fetched, len(targets))
    return 0


if __name__ == "__main__":
    sys.exit(main())

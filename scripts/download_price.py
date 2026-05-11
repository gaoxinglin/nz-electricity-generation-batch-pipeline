"""
Download EMI Final Energy Prices — skeleton (full ByMonth + daily logic ships
in Phase 1.1).

Phase 0 contract: accept --months/--years/--year-month so demo/local-subset
Makefile targets do not break. For Phase 0 we only attempt ByMonth files;
daily fallback + BYMONTH_CUTOFF arrive in Phase 1.

Usage:
    python scripts/download_price.py --output data/raw/ --months 1
"""

from __future__ import annotations

import argparse
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
HISTORICAL_START = (2016, 1)
REQUEST_TIMEOUT = 60


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
        return month_range(shift_months(end, months), end)
    if years is not None:
        return month_range(shift_months(end, years * 12), end)
    return month_range(HISTORICAL_START, end)


def download_bymonth(year: int, month: int, output_dir: Path) -> Path | None:
    filename = f"{year}{month:02d}_FinalEnergyPrices.csv"
    url = f"{BYMONTH_BASE}/{filename}"
    dest = output_dir / filename
    if dest.exists() and dest.stat().st_size > 0:
        logger.info("skip (present): %s", filename)
        return dest
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 404:
                logger.warning("ByMonth 404: %s (daily fallback ships in Phase 1.1)", filename)
                return None
            resp.raise_for_status()
            dest.write_bytes(resp.content)
            logger.info("downloaded %s (%d bytes)", filename, len(resp.content))
            return dest
        except Exception as exc:
            last_exc = exc
            wait = 30 * (2 ** attempt)
            logger.warning("attempt %d failed (%s); retrying in %ds", attempt + 1, exc, wait)
            time.sleep(wait)
    raise RuntimeError(f"download failed: {url}") from last_exc


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Download EMI Final Energy Prices (Phase 0 skeleton)")
    p.add_argument("--output", required=True)
    g = p.add_mutually_exclusive_group()
    g.add_argument("--months", type=int)
    g.add_argument("--years", type=int)
    g.add_argument("--year-month")
    args = p.parse_args(argv)

    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    targets = resolve_months(args.months, args.years, args.year_month)
    logger.info("resolved %d month(s)", len(targets))
    fetched = 0
    for y, m in targets:
        if download_bymonth(y, m, output) is not None:
            fetched += 1
    logger.info("done — %d/%d files fetched", fetched, len(targets))
    return 0


if __name__ == "__main__":
    sys.exit(main())

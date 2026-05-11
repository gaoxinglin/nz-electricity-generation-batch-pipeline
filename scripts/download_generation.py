"""
Download EMI Generation_MD monthly CSVs.

CLI entry — parallel to airflow/dags/nz_electricity_monthly.py (which still
embeds its own copy of the download logic). Both paths will coexist through
Phase 4; the DAG will be migrated in Phase 4.1 to call this module.

Usage:
    python scripts/download_generation.py --output data/raw/                # all from 2016-01
    python scripts/download_generation.py --output data/raw/ --months 1     # last 1 complete month
    python scripts/download_generation.py --output data/raw/ --years 1      # last 12 complete months
    python scripts/download_generation.py --output data/raw/ --year-month 202401

"--months N" / "--years N" mean "the N most recent complete months ending
last month" (current month is excluded — file may not be published yet).
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date
from pathlib import Path

import requests

logger = logging.getLogger("download_generation")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

EMI_BASE_URL = "https://www.emi.ea.govt.nz/Wholesale/Datasets/Generation/Generation_MD"
HISTORICAL_START = (2016, 1)
REQUEST_TIMEOUT = 60


def last_complete_month(today: date) -> tuple[int, int]:
    """Return (year, month) of the month immediately before `today`'s month."""
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def month_range(start: tuple[int, int], end: tuple[int, int]) -> list[tuple[int, int]]:
    """Inclusive list of (year, month) from start to end (both inclusive, chronological)."""
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


def shift_months(end: tuple[int, int], n_months: int) -> tuple[int, int]:
    """Return the (year, month) `n_months` before `end` (inclusive window of n)."""
    y, m = end
    # Want N months total ending at `end`, so start = end - (N-1) months
    total = y * 12 + (m - 1) - (n_months - 1)
    return total // 12, total % 12 + 1


def resolve_months(months: int | None, years: int | None, year_month: str | None) -> list[tuple[int, int]]:
    today = date.today()
    end = last_complete_month(today)

    if year_month:
        ym = year_month
        if len(ym) != 6 or not ym.isdigit():
            raise SystemExit(f"--year-month must be YYYYMM (got {ym!r})")
        return [(int(ym[:4]), int(ym[4:]))]

    if months is not None:
        return month_range(shift_months(end, months), end)

    if years is not None:
        return month_range(shift_months(end, years * 12), end)

    # Full history: from HISTORICAL_START to last complete month
    return month_range(HISTORICAL_START, end)


def download_one(year: int, month: int, output_dir: Path) -> Path | None:
    """Download one month with exponential backoff. Return path or None if 404."""
    filename = f"{year}{month:02d}_Generation_MD.csv"
    url = f"{EMI_BASE_URL}/{filename}"
    dest = output_dir / filename

    if dest.exists() and dest.stat().st_size > 0:
        logger.info("skip (already present): %s", filename)
        return dest

    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 404:
                logger.warning("404 (not yet published): %s", filename)
                return None
            resp.raise_for_status()
            # Strip stray NUL bytes (occasional in EMI exports)
            content = resp.content.replace(b"\x00", b"")
            dest.write_bytes(content)
            logger.info("downloaded %s (%d bytes)", filename, len(content))
            return dest
        except Exception as exc:
            last_exc = exc
            wait = 30 * (2 ** attempt)
            logger.warning("attempt %d failed (%s); retrying in %ds", attempt + 1, exc, wait)
            time.sleep(wait)
    raise RuntimeError(f"download failed after retries: {url}") from last_exc


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Download EMI Generation_MD monthly CSVs")
    p.add_argument("--output", required=True, help="Output directory")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--months", type=int, help="N most recent complete months")
    g.add_argument("--years", type=int, help="N years (=12N months) ending last complete month")
    g.add_argument("--year-month", help="Single YYYYMM")
    args = p.parse_args(argv)

    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)

    targets = resolve_months(args.months, args.years, args.year_month)
    logger.info("resolved %d month(s): %s", len(targets), [f"{y}{m:02d}" for y, m in targets])

    fetched = 0
    for y, m in targets:
        if download_one(y, m, output) is not None:
            fetched += 1

    logger.info("done — %d/%d files fetched", fetched, len(targets))
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""
Download the latest EMI Network Supply Points Table CSV.

The file is published daily under
  https://www.emi.ea.govt.nz/Wholesale/Datasets/MappingsAndGeospatial/
    NetworkSupplyPointsTable/{YYYYMMDD}_NetworkSupplyPointsTable.csv
The HTTP endpoint 302-redirects to an Azure Blob URL; -L follows it.

Strategy:
  - Probe today, then walk back day-by-day until a 200 hits (most likely
    yesterday's date on weekdays; rare gaps near month-end). Cap at 14 days
    back to avoid runaway loops.
  - Save as raw/NetworkSupplyPointsTable.csv (the trailing date is dropped
    so dbt source / loader can reference a stable filename).

Usage:
    python scripts/download_nsp.py --output data/raw/
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import requests

logger = logging.getLogger("download_nsp")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

BASE = (
    "https://www.emi.ea.govt.nz/Wholesale/Datasets/MappingsAndGeospatial/"
    "NetworkSupplyPointsTable"
)
STABLE_FILENAME = "NetworkSupplyPointsTable.csv"
LOOKBACK_DAYS = 14
REQUEST_TIMEOUT = 60


def find_latest(output_dir: Path) -> Path | None:
    today = date.today()
    for offset in range(LOOKBACK_DAYS):
        d = today - timedelta(days=offset)
        url = f"{BASE}/{d:%Y%m%d}_NetworkSupplyPointsTable.csv"
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        except Exception as exc:
            logger.warning("attempt %s failed: %s", d, exc)
            continue
        if resp.status_code == 200 and resp.content.startswith(b"Current flag,"):
            dest = output_dir / STABLE_FILENAME
            dest.write_bytes(resp.content)
            logger.info("downloaded NSP dated %s (%d bytes)", d, len(resp.content))
            return dest
        if resp.status_code != 404:
            logger.warning("%s → HTTP %d", d, resp.status_code)
    logger.error("no NSP file found in the last %d days", LOOKBACK_DAYS)
    return None


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Download latest EMI NSP Table")
    p.add_argument("--output", required=True)
    args = p.parse_args(argv)
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    return 0 if find_latest(output) else 1


if __name__ == "__main__":
    sys.exit(main())

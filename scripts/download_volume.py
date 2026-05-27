"""
Download EMI reconciled injection and offtake volume CSVs.

The public blob container publishes monthly gzip files under:
  Datasets/Wholesale/Volumes/Reconciliation/{YYYY}/
    ReconciledInjectionAndOfftake_{YYYYMM}_{published_at}.csv.gz

The publication timestamp changes when EMI republishes a month, so this
script discovers the latest matching blob instead of hard-coding filenames.

Usage:
    python scripts/download_volume.py --output data/raw/ --months 1
    python scripts/download_volume.py --output data/raw/ --year-month 202604
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
import xml.etree.ElementTree as ET
from datetime import date
from pathlib import Path

import requests

logger = logging.getLogger("download_volume")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

CONTAINER_URL = "https://emidatasets.blob.core.windows.net/publicdata"
VOLUME_PREFIX = "Datasets/Wholesale/Volumes/Reconciliation"
HISTORICAL_START = (2016, 1)
REQUEST_TIMEOUT = 120


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


def _list_blobs(prefix: str) -> list[str]:
    url = f"{CONTAINER_URL}?restype=container&comp=list&prefix={prefix}"
    resp = _retry_get(url)
    if resp is None:
        return []
    root = ET.fromstring(resp.text)
    names: list[str] = []
    for blob in root.findall(".//Blob"):
        name = blob.findtext("Name")
        if name:
            names.append(name)
    return names


def latest_blob_for_month(year: int, month: int) -> str | None:
    year_month = f"{year}{month:02d}"
    prefix = (
        f"{VOLUME_PREFIX}/{year}/"
        f"ReconciledInjectionAndOfftake_{year_month}"
    )
    names = [
        n for n in _list_blobs(prefix)
        if n.endswith(".csv.gz") and f"_{year_month}_" in n
    ]
    if not names:
        return None
    return sorted(names)[-1]


def fetch_month(year: int, month: int, output_dir: Path, force: bool = False) -> Path | None:
    year_month = f"{year}{month:02d}"
    dest = output_dir / f"{year_month}_ReconciledInjectionAndOfftake.csv.gz"
    source_marker = output_dir / f"{dest.name}.source"

    blob_name = latest_blob_for_month(year, month)
    if blob_name is None:
        logger.warning("no reconciled volume file found for %s", year_month)
        return None

    if dest.exists() and dest.stat().st_size > 0 and source_marker.exists() and not force:
        if source_marker.read_text(encoding="utf-8").strip() == blob_name:
            logger.info("skip (present): %s", dest.name)
            return dest

    url = f"{CONTAINER_URL}/{blob_name}"
    resp = _retry_get(url)
    if resp is None:
        logger.warning("404 while fetching discovered volume blob: %s", blob_name)
        return None

    dest.write_bytes(resp.content)
    source_marker.write_text(f"{blob_name}\n", encoding="utf-8")
    logger.info("downloaded %s -> %s (%d bytes)", blob_name, dest.name, len(resp.content))
    return dest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Download EMI reconciled injection/offtake volumes")
    parser.add_argument("--output", required=True)
    parser.add_argument("--force", action="store_true", help="Re-download even when the latest discovered blob is present")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--months", type=int)
    group.add_argument("--years", type=int)
    group.add_argument("--year-month")
    args = parser.parse_args(argv)

    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    targets = resolve_months(args.months, args.years, args.year_month)
    logger.info("resolved %d month(s): %s", len(targets), [f"{y}{m:02d}" for y, m in targets])

    fetched = 0
    for y, m in targets:
        if fetch_month(y, m, output, force=args.force) is not None:
            fetched += 1
    logger.info("done - %d/%d months fetched", fetched, len(targets))
    return 0


if __name__ == "__main__":
    sys.exit(main())

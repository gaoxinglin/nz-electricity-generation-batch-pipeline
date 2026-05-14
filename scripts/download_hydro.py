"""
Download the EMI Hydrological Modelling Dataset (HMD) — Storage section.

The HMD is a periodic release (≈ annually). This script:
  - Discovers the latest available release by probing known release dates
  - Downloads FileIndex_Storage.csv to enumerate lake files
  - Downloads each lake storage CSV into {output}/hydro/
  - Files are saved with stable names (the original filenames) so
    load_local.py and dbt sources can reference them without knowing the
    release date.

Usage:
    python scripts/download_hydro.py --output data/raw/
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import requests

logger = logging.getLogger("download_hydro")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

HMD_BASE = (
    "https://emidatasets.blob.core.windows.net/publicdata/Datasets/"
    "Environment/HydrologicalModellingDataset"
)
# Known HMD release dates, newest first. Add new entries as EMI publishes them.
KNOWN_RELEASES = ["20241231", "20231231", "20221231", "20211231", "20201231"]
REQUEST_TIMEOUT = 120


def _storage_base(release: str) -> str:
    return f"{HMD_BASE}/3_StorageAndSpill_{release}/3_1_Storage"


def find_latest_release() -> tuple[str, str] | tuple[None, None]:
    """Return (release_date, storage_base_url) for the newest accessible release."""
    for release in KNOWN_RELEASES:
        url = f"{_storage_base(release)}/FileIndex_Storage.csv"
        try:
            resp = requests.head(url, timeout=30, allow_redirects=True)
        except Exception as exc:
            logger.warning("HEAD %s failed: %s", url, exc)
            continue
        if resp.status_code == 200:
            return release, _storage_base(release)
        if resp.status_code not in (404, 403):
            logger.warning("%s → HTTP %d", url, resp.status_code)
    return None, None


def download_hydro_storage(output_dir: Path) -> bool:
    """Download all lake storage CSVs into output_dir/hydro/.

    Returns True on success, False if no release found or all downloads failed.
    """
    hydro_dir = output_dir / "hydro"
    hydro_dir.mkdir(parents=True, exist_ok=True)

    release, storage_base = find_latest_release()
    if release is None:
        logger.error("no HMD release found among %s", KNOWN_RELEASES)
        return False
    logger.info("using HMD release %s", release)

    # Fetch file index
    index_url = f"{storage_base}/FileIndex_Storage.csv"
    index_resp = requests.get(index_url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
    index_resp.raise_for_status()

    # Parse filenames from index (header row + data rows)
    filenames: list[str] = []
    for i, line in enumerate(index_resp.text.splitlines()):
        if i == 0:
            continue  # skip header
        parts = line.split(",")
        if parts and parts[0].strip():
            filenames.append(parts[0].strip())

    if not filenames:
        logger.error("FileIndex_Storage.csv is empty or unreadable")
        return False

    # Download individual lake files
    downloaded = 0
    for filename in filenames:
        url = f"{storage_base}/{filename}"
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            resp.raise_for_status()
        except Exception as exc:
            logger.warning("failed to download %s: %s", filename, exc)
            continue
        dest = hydro_dir / filename
        dest.write_bytes(resp.content)
        logger.info("downloaded %s (%d bytes)", filename, len(resp.content))
        downloaded += 1

    # Write a release marker so load_local.py can log which version is present
    (hydro_dir / "_release.txt").write_text(release)
    logger.info(
        "done — %d/%d files fetched (HMD release %s)", downloaded, len(filenames), release
    )
    return downloaded > 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Download EMI Hydrological Modelling Dataset (storage)")
    p.add_argument("--output", required=True, help="Root raw data directory (hydro/ subdir created here)")
    args = p.parse_args(argv)
    return 0 if download_hydro_storage(Path(args.output)) else 1


if __name__ == "__main__":
    sys.exit(main())

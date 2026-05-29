"""
Backfill EMI reconciled injection/offtake volume files to S3 only.

Files are published as monthly .csv.gz objects. Keep that compressed format in
S3; Snowflake COPY can read gzip files and the compressed landing zone avoids
unnecessary storage and transfer cost.
"""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
import time
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)

CONTAINER_URL = "https://emidatasets.blob.core.windows.net/publicdata"
VOLUME_PREFIX = "Datasets/Wholesale/Volumes/Reconciliation"
HISTORICAL_START = (2016, 1)
S3_BUCKET = os.environ.get("S3_BUCKET_NAME", "nz-electricity-generation")
S3_PREFIX = os.environ.get("NZEG_VOLUME_S3_PREFIX", "raw/reconciled_volumes").strip("/")
CHUNK_SIZE = 8 * 1024 * 1024


def _last_complete_month(today: date) -> tuple[int, int]:
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def _month_range(start: tuple[int, int], end: tuple[int, int]) -> list[tuple[int, int]]:
    sy, sm = start
    ey, em = end
    months: list[tuple[int, int]] = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        months.append((y, m))
        m += 1
        if m == 13:
            m = 1
            y += 1
    return months


def _parse_year_month(value: str) -> tuple[int, int]:
    if len(value) != 6 or not value.isdigit():
        raise ValueError(f"year_month must be YYYYMM, got {value!r}")
    month = int(value[4:])
    if not 1 <= month <= 12:
        raise ValueError(f"year_month has invalid month: {value!r}")
    return int(value[:4]), month


def _resolve_months(conf: dict[str, object]) -> list[tuple[int, int]]:
    if conf.get("year_month"):
        return [_parse_year_month(str(conf["year_month"]))]

    start = _parse_year_month(str(conf.get("start_month", "201601")))
    end = _parse_year_month(str(conf.get("end_month", f"{_last_complete_month(date.today())[0]}{_last_complete_month(date.today())[1]:02d}")))
    start = max(start, HISTORICAL_START)
    if end < start:
        return []
    return _month_range(start, end)


def _retry_get(url: str) -> requests.Response | None:
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            response = requests.get(url, timeout=120)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            return response
        except Exception as exc:
            last_exc = exc
            wait = 30 * (2**attempt)
            logger.warning("GET attempt %d failed for %s: %s; retrying in %ds", attempt + 1, url, exc, wait)
            time.sleep(wait)
    raise RuntimeError(f"GET failed for {url}") from last_exc


def _retry_head(url: str) -> int | None:
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            response = requests.head(url, timeout=30)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            value = response.headers.get("content-length")
            return int(value) if value else None
        except Exception as exc:
            last_exc = exc
            wait = 30 * (2**attempt)
            logger.warning("HEAD attempt %d failed for %s: %s; retrying in %ds", attempt + 1, url, exc, wait)
            time.sleep(wait)
    raise RuntimeError(f"HEAD failed for {url}") from last_exc


def _list_blobs(prefix: str) -> list[str]:
    response = _retry_get(f"{CONTAINER_URL}?restype=container&comp=list&prefix={prefix}")
    if response is None:
        return []
    root = ET.fromstring(response.text)
    names: list[str] = []
    for blob in root.findall(".//Blob"):
        name = blob.findtext("Name")
        if name:
            names.append(name)
    return names


def _latest_blob_for_month(year: int, month: int) -> str | None:
    year_month = f"{year}{month:02d}"
    prefix = f"{VOLUME_PREFIX}/{year}/ReconciledInjectionAndOfftake_{year_month}"
    names = [
        name for name in _list_blobs(prefix)
        if name.endswith(".csv.gz") and f"_{year_month}_" in name
    ]
    if not names:
        return None
    return sorted(names)[-1]


def _download(blob_name: str, destination: Path) -> None:
    url = f"{CONTAINER_URL}/{blob_name}"
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            with requests.get(url, stream=True, timeout=(10, 300)) as response:
                if response.status_code == 404:
                    raise AirflowSkipException(f"source volume file not found: {blob_name}")
                response.raise_for_status()
                with destination.open("wb") as handle:
                    for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            handle.write(chunk)
            return
        except AirflowSkipException:
            raise
        except Exception as exc:
            last_exc = exc
            wait = 30 * (2**attempt)
            logger.warning("download attempt %d failed for %s: %s; retrying in %ds", attempt + 1, url, exc, wait)
            time.sleep(wait)
    raise RuntimeError(f"download failed for {url}") from last_exc


def _s3_content_length(hook: S3Hook, key: str) -> int | None:
    from botocore.exceptions import ClientError

    try:
        response = hook.get_conn().head_object(Bucket=S3_BUCKET, Key=key)
        return int(response["ContentLength"])
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise


def backfill_volume_to_s3(**kwargs) -> dict[str, int]:
    conf = kwargs["dag_run"].conf or {}
    force = bool(conf.get("force", False))
    months = _resolve_months(conf)
    if not months:
        raise AirflowSkipException("no reconciled volume months resolved for this run")

    hook = S3Hook(aws_conn_id="aws_default")
    local_dir = Path(tempfile.mkdtemp(prefix="volume_s3_"))
    uploaded = 0
    skipped_existing = 0
    missing_source = 0
    uploaded_bytes = 0

    try:
        for year, month in months:
            year_month = f"{year}{month:02d}"
            blob_name = _latest_blob_for_month(year, month)
            if blob_name is None:
                logger.warning("source missing, skip: %s", year_month)
                missing_source += 1
                continue

            source_url = f"{CONTAINER_URL}/{blob_name}"
            source_len = _retry_head(source_url)
            if source_len is None:
                logger.warning("source HEAD missing, skip: %s", blob_name)
                missing_source += 1
                continue

            filename = f"{year_month}_ReconciledInjectionAndOfftake.csv.gz"
            s3_key = f"{S3_PREFIX}/{filename}"
            existing_len = _s3_content_length(hook, s3_key)
            if not force and existing_len == source_len:
                logger.info("skip existing: s3://%s/%s (%d bytes)", S3_BUCKET, s3_key, existing_len)
                skipped_existing += 1
                continue

            local_path = local_dir / filename
            _download(blob_name, local_path)
            actual_len = local_path.stat().st_size
            if actual_len != source_len:
                raise ValueError(f"{filename}: downloaded {actual_len} bytes, expected {source_len}")

            hook.load_file(filename=str(local_path), key=s3_key, bucket_name=S3_BUCKET, replace=True)
            local_path.unlink(missing_ok=True)
            uploaded += 1
            uploaded_bytes += actual_len
            logger.info("uploaded s3://%s/%s (%d bytes) from %s", S3_BUCKET, s3_key, actual_len, blob_name)
    finally:
        shutil.rmtree(local_dir, ignore_errors=True)

    result = {
        "months": len(months),
        "uploaded": uploaded,
        "skipped_existing": skipped_existing,
        "missing_source": missing_source,
        "uploaded_bytes": uploaded_bytes,
    }
    logger.info("volume S3 backfill summary: %s", result)
    return result


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nz_volume_s3_backfill",
    default_args=default_args,
    description="S3-only historical backfill for EMI reconciled volume gzip files",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=2,
    tags=["nz-electricity", "volume", "s3", "backfill"],
) as dag:
    PythonOperator(
        task_id="volume_to_s3",
        python_callable=backfill_volume_to_s3,
        pool="emi_download_pool",
        execution_timeout=timedelta(hours=1),
    )

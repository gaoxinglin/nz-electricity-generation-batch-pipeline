"""
Backfill EMI daily Offers files to S3 only.

This DAG intentionally avoids Snowflake and dbt. The main monthly V2 DAG can
load one Offers day as part of an end-to-end smoke test, but full history is
large enough that S3 landing should be handled separately and idempotently.
"""

from __future__ import annotations

import calendar
import logging
import os
import shutil
import tempfile
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)

OFFERS_BASE = (
    "https://emidatasets.blob.core.windows.net/publicdata/Datasets/"
    "Wholesale/BidsAndOffers/Offers"
)
HISTORICAL_START = date(2016, 1, 1)
S3_BUCKET = os.environ.get("S3_BUCKET_NAME", "nz-electricity-generation")
S3_PREFIX = os.environ.get("NZEG_OFFERS_S3_PREFIX", "raw/offers").strip("/")
CHUNK_SIZE = 8 * 1024 * 1024


def _parse_day(value: str) -> date:
    cleaned = value.replace("-", "")
    if len(cleaned) != 8 or not cleaned.isdigit():
        raise ValueError(f"date must be YYYYMMDD or YYYY-MM-DD, got {value!r}")
    return date(int(cleaned[:4]), int(cleaned[4:6]), int(cleaned[6:]))


def _month_range(year_month: str) -> tuple[date, date]:
    if len(year_month) != 6 or not year_month.isdigit():
        raise ValueError(f"year_month must be YYYYMM, got {year_month!r}")
    year = int(year_month[:4])
    month = int(year_month[4:])
    start = date(year, month, 1)
    end = date(year, month, calendar.monthrange(year, month)[1])
    return start, end


def _latest_complete_day() -> date:
    return date.today() - timedelta(days=1)


def _days_between(start: date, end: date) -> list[date]:
    days: list[date] = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def _resolve_days(conf: dict[str, object]) -> list[date]:
    latest = _latest_complete_day()

    year_month = conf.get("year_month")
    if year_month:
        start, end = _month_range(str(year_month))
    else:
        start = _parse_day(str(conf.get("start_date", f"{HISTORICAL_START:%Y%m%d}")))
        end = _parse_day(str(conf.get("end_date", f"{latest:%Y%m%d}")))

    start = max(start, HISTORICAL_START)
    end = min(end, latest)
    if end < start:
        return []
    return _days_between(start, end)


def _offers_filename(trading_day: date) -> str:
    return f"{trading_day:%Y%m%d}_Offers.csv"


def _offers_url(trading_day: date) -> str:
    return f"{OFFERS_BASE}/{trading_day:%Y}/{_offers_filename(trading_day)}"


def _source_content_length(trading_day: date) -> int | None:
    url = _offers_url(trading_day)
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


def _download_offer(trading_day: date, destination: Path) -> None:
    url = _offers_url(trading_day)
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            with requests.get(url, stream=True, timeout=(10, 300)) as response:
                if response.status_code == 404:
                    raise AirflowSkipException(f"offer file not found: {_offers_filename(trading_day)}")
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


def backfill_offers_to_s3(**kwargs) -> dict[str, int]:
    conf = kwargs["dag_run"].conf or {}
    force = bool(conf.get("force", False))
    days = _resolve_days(conf)
    if not days:
        raise AirflowSkipException("no complete Offers days resolved for this run")

    hook = S3Hook(aws_conn_id="aws_default")
    local_dir = Path(tempfile.mkdtemp(prefix="offers_s3_"))
    uploaded = 0
    skipped_existing = 0
    missing_source = 0
    uploaded_bytes = 0

    try:
        for trading_day in days:
            filename = _offers_filename(trading_day)
            s3_key = f"{S3_PREFIX}/{filename}"
            source_len = _source_content_length(trading_day)
            if source_len is None:
                logger.warning("source missing, skip: %s", filename)
                missing_source += 1
                continue

            existing_len = _s3_content_length(hook, s3_key)
            if not force and existing_len == source_len:
                logger.info("skip existing: s3://%s/%s (%d bytes)", S3_BUCKET, s3_key, existing_len)
                skipped_existing += 1
                continue

            local_path = local_dir / filename
            _download_offer(trading_day, local_path)
            actual_len = local_path.stat().st_size
            if actual_len != source_len:
                raise ValueError(f"{filename}: downloaded {actual_len} bytes, expected {source_len}")

            hook.load_file(filename=str(local_path), key=s3_key, bucket_name=S3_BUCKET, replace=True)
            local_path.unlink(missing_ok=True)
            uploaded += 1
            uploaded_bytes += actual_len
            logger.info("uploaded s3://%s/%s (%d bytes)", S3_BUCKET, s3_key, actual_len)
    finally:
        shutil.rmtree(local_dir, ignore_errors=True)

    result = {
        "days": len(days),
        "uploaded": uploaded,
        "skipped_existing": skipped_existing,
        "missing_source": missing_source,
        "uploaded_bytes": uploaded_bytes,
    }
    logger.info("offers S3 backfill summary: %s", result)
    return result


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nz_offers_s3_backfill",
    default_args=default_args,
    description="S3-only historical backfill for EMI daily Offers files",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=2,
    tags=["nz-electricity", "offers", "s3", "backfill"],
) as dag:
    PythonOperator(
        task_id="offers_to_s3",
        python_callable=backfill_offers_to_s3,
        pool="emi_download_pool",
        execution_timeout=timedelta(hours=4),
    )

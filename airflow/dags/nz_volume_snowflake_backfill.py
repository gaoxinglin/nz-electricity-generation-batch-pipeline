"""
Load already-backfilled reconciled volume files from S3 into Snowflake RAW.

This DAG is intentionally load-only:
S3 raw/reconciled_volumes/*.csv.gz -> Snowflake RAW.RAW_MARKET_VOLUME.
It does not download public source files and does not run dbt.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# Make scripts/ importable from the DAG runtime.
_REPO_ROOT = os.environ.get("NZEG_REPO_ROOT", "/opt/airflow")
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

HISTORICAL_START = (2016, 1)
DEFAULT_END = (2026, 4)


def _parse_year_month(value: str) -> tuple[int, int]:
    if len(value) != 6 or not value.isdigit():
        raise ValueError(f"year_month must be YYYYMM, got {value!r}")
    month = int(value[4:])
    if not 1 <= month <= 12:
        raise ValueError(f"year_month has invalid month: {value!r}")
    return int(value[:4]), month


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


def _resolve_months(conf: dict[str, object]) -> list[tuple[int, int]]:
    if conf.get("year_month"):
        return [_parse_year_month(str(conf["year_month"]))]

    start = _parse_year_month(str(conf.get("start_month", "201601")))
    end = _parse_year_month(str(conf.get("end_month", f"{DEFAULT_END[0]}{DEFAULT_END[1]:02d}")))
    start = max(start, HISTORICAL_START)
    if end < start:
        return []
    return _month_range(start, end)


def load_volume_to_snowflake(**kwargs) -> dict[str, int]:
    from scripts.load_snowflake_volume import load_volume_month

    conf = kwargs["dag_run"].conf or {}
    months = _resolve_months(conf)
    if not months:
        raise AirflowSkipException("no reconciled volume months resolved for this run")

    loaded_months = 0
    for year, month in months:
        year_month = f"{year}{month:02d}"
        s3_key = f"reconciled_volumes/{year_month}_ReconciledInjectionAndOfftake.csv.gz"
        logger.info("loading reconciled volume month %s from @raw_stage/%s", year_month, s3_key)
        load_volume_month(year_month, s3_key)
        loaded_months += 1

    result = {"months": len(months), "loaded_months": loaded_months}
    logger.info("Snowflake volume backfill summary: %s", result)
    return result


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nz_volume_snowflake_backfill",
    default_args=default_args,
    description="Load reconciled volume S3 files into Snowflake RAW only",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=2,
    tags=["nz-electricity", "volume", "snowflake", "backfill"],
) as dag:
    PythonOperator(
        task_id="volume_to_snowflake",
        python_callable=load_volume_to_snowflake,
        execution_timeout=timedelta(hours=1),
    )

"""
NZ Electricity V2 — Monthly Batch DAG (Generation + Price + NSP).

Topology (PRD §6.1):

    ┌───────────────────────────────┐
    │ Core (ALL_SUCCESS)            │
    │ ┌──────────┐  ┌────────────┐  │
    │ │ generation │  │ price      │  │
    │ │ branch     │  │ branch     │  │
    │ │ (V1 reuse) │  │ (Phase 1)  │  │
    │ └─────┬──────┘  └─────┬──────┘  │
    │       └───────┬───────┘         │
    └───────────────┼─────────────────┘
                    │
              ┌─────▼──────┐
              │ nsp_branch │  (NONE_FAILED — best-effort)
              │ (Phase 2)  │
              └─────┬──────┘
                    │
              ┌─────▼──────┐
              │ run_dbt    │
              └─────┬──────┘
                    │
              ┌─────▼──────┐
              │ test_dbt   │
              └────────────┘

V1 DAG `nz_electricity_monthly` is kept untouched as a fallback path during
the migration window (PRD §14, §16.3). Switch traffic by pausing V1 and
enabling V2 in Airflow once V2 has run cleanly for one cycle.

Generation tasks reuse V1's python callables to avoid drift. Price + NSP
tasks call into the standalone scripts that ship with the repo so the same
code works for local Makefile flows.
"""

from __future__ import annotations

import logging
import os
import sys
import tempfile
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule

# V1 generation callables are inline-copied below (small, stable functions).
# Importing from nz_electricity_monthly would re-execute its top-level DAG
# instantiation and trigger AirflowDagDuplicatedIdException, so duplication
# is the pragmatic trade-off here. Keep the two copies in sync on V1 changes.

logger = logging.getLogger(__name__)

# Make scripts/ importable from the DAG runtime.
_REPO_ROOT = os.environ.get("NZEG_REPO_ROOT", "/opt/airflow")
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

S3_BUCKET = os.environ.get("S3_BUCKET_NAME", "nz-electricity-generation")
PRICE_S3_PREFIX = "raw/final_energy_prices"
VOLUME_S3_PREFIX = "raw/reconciled_volumes"
NSP_S3_PREFIX = "raw/nsp"
HYDRO_S3_PREFIX = "raw/hydro_storage"

EMI_BASE_URL = "https://www.emi.ea.govt.nz/Wholesale/Datasets/Generation/Generation_MD"
S3_RAW_PREFIX = "raw/generation_md"
EXPECTED_COLUMNS = 57
KNOWN_FUEL_CODES = {
    "Gas", "Gas&Oil", "Hydro", "HYD", "Geo", "GEO", "Wind", "WIN",
    "Diesel", "Coal", "Wood", "Solar", "SOL", "ELE",
}


# ─── Generation branch (inline V1 callables) ──────────────────────────


def _get_year_month(**kwargs) -> str:
    dag_run = kwargs["dag_run"]
    return dag_run.conf.get("year_month") or kwargs["logical_date"].strftime("%Y%m")


def _csv_filename(year_month: str) -> str:
    return f"{year_month}_Generation_MD.csv"


def generation_download_csv(**kwargs) -> None:
    import time

    import requests
    year_month = _get_year_month(**kwargs)
    filename = _csv_filename(year_month)
    url = f"{EMI_BASE_URL}/{filename}"
    last_exc = None
    for attempt in range(3):
        try:
            resp = requests.get(url, timeout=60)
            if resp.status_code == 404:
                raise AirflowSkipException(f"EMI 404 for {filename}")
            resp.raise_for_status()
            break
        except AirflowSkipException:
            raise
        except Exception as exc:
            last_exc = exc
            time.sleep(30 * (2 ** attempt))
    else:
        raise last_exc  # type: ignore[misc]
    content = resp.content.replace(b"\x00", b"")
    local_path = os.path.join(tempfile.gettempdir(), filename)
    with open(local_path, "wb") as f:
        f.write(content)
    kwargs["ti"].xcom_push(key="local_path", value=local_path)
    kwargs["ti"].xcom_push(key="year_month", value=year_month)


def generation_validate_csv(**kwargs) -> None:
    import csv
    ti = kwargs["ti"]
    local_path = ti.xcom_pull(key="local_path", task_ids="generation_download")
    year_month = ti.xcom_pull(key="year_month", task_ids="generation_download")
    file_size = os.path.getsize(local_path)
    if not (1_024 <= file_size <= 10 * 1024 * 1024):
        raise ValueError(f"File size {file_size} outside [1KB, 10MB]")
    with open(local_path, encoding="utf-8") as f:
        reader = csv.reader(line.replace("\x00", "") for line in f)
        header = next(reader)
        if len(header) != EXPECTED_COLUMNS:
            raise ValueError(f"Expected {EXPECTED_COLUMNS} cols, got {len(header)}")
        row_count = 0
        for row in reader:
            row_count += 1
            td = row[6]
            if len(td) != 10 or td[4] != "-":
                raise ValueError(f"Row {row_count}: bad Trading_Date {td!r}")
            if td[:7].replace("-", "") != year_month:
                raise ValueError(f"Row {row_count}: month mismatch")
            if row[4] not in KNOWN_FUEL_CODES:
                raise ValueError(f"Row {row_count}: unknown fuel {row[4]!r}")
            for i in range(7, EXPECTED_COLUMNS):
                v = row[i].strip()
                if v == "" or v.upper() == "NULL":
                    continue
                float(v)  # raises ValueError on bad numerics
    if not (100 <= row_count <= 50_000):
        raise ValueError(f"row count {row_count} outside [100, 50000]")
    ti.xcom_push(key="rows_validated", value=row_count)


def generation_upload_to_s3(**kwargs) -> None:
    ti = kwargs["ti"]
    local_path = ti.xcom_pull(key="local_path", task_ids="generation_download")
    year_month = ti.xcom_pull(key="year_month", task_ids="generation_download")
    filename = _csv_filename(year_month)
    s3_key = f"{S3_RAW_PREFIX}/{filename}"
    S3Hook(aws_conn_id="aws_default").load_file(
        filename=local_path, key=s3_key, bucket_name=S3_BUCKET, replace=True
    )
    os.remove(local_path)
    ti.xcom_push(key="s3_key", value=s3_key)


def generation_load_to_snowflake(**kwargs) -> None:
    import snowflake.connector
    from cryptography.hazmat.primitives.serialization import (
        Encoding,
        NoEncryption,
        PrivateFormat,
        load_pem_private_key,
    )
    ti = kwargs["ti"]
    year_month = ti.xcom_pull(key="year_month", task_ids="generation_download")
    filename = _csv_filename(year_month)
    with open(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"], "rb") as f:
        pkey = load_pem_private_key(f.read(), password=None).private_bytes(
            Encoding.DER, PrivateFormat.PKCS8, NoEncryption()
        )
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=pkey,
        database=os.environ["SNOWFLAKE_DATABASE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.environ["SNOWFLAKE_ROLE"],
        schema="RAW",
    )
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("DELETE FROM raw_generation WHERE trading_month = %s", (year_month,))
        copy_sql = f"""
            COPY INTO raw_generation
            FROM (
                SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,
                       $18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,
                       $33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,
                       $48,$49,$50,$51,$52,$53,$54,$55,$56,$57,
                       '{year_month}' AS trading_month,
                       METADATA$FILE_LAST_MODIFIED AS _source_file_modified_at
                FROM @raw_stage/generation_md/{filename}
            )
            FILE_FORMAT = (FORMAT_NAME = 'csv_format')
            ON_ERROR = 'ABORT_STATEMENT'
            FORCE = TRUE
        """
        cur.execute(copy_sql)
        cur.execute("COMMIT")
    except Exception:
        conn.cursor().execute("ROLLBACK")
        raise
    finally:
        conn.close()


def check_run_dbt(**kwargs) -> bool:
    return not kwargs["dag_run"].conf.get("skip_dbt", False)


# ─── Price branch ─────────────────────────────────────────────────────


def price_download(**kwargs) -> None:
    """Fetch one month of EMI Final Energy Prices into /tmp."""
    from pathlib import Path

    from scripts.download_price import DEFAULT_BYMONTH_CUTOFF, fetch_month

    dag_run = kwargs["dag_run"]
    ym = dag_run.conf.get("year_month") or kwargs["logical_date"].strftime("%Y%m")
    year, month = int(ym[:4]), int(ym[4:])
    out = Path(tempfile.gettempdir())
    path = fetch_month(year, month, out, DEFAULT_BYMONTH_CUTOFF)
    if path is None:
        raise AirflowSkipException(f"price month {ym} not yet published")
    kwargs["ti"].xcom_push(key="local_path", value=str(path))
    kwargs["ti"].xcom_push(key="year_month", value=ym)


def price_validate(**kwargs) -> None:
    from pathlib import Path

    from scripts.validate_price import validate_file

    local_path = kwargs["ti"].xcom_pull(key="local_path", task_ids="price_download")
    rows = validate_file(Path(local_path))
    logger.info("price validation OK — %d rows", rows)
    kwargs["ti"].xcom_push(key="rows_validated", value=rows)


def price_upload(**kwargs) -> None:
    ti = kwargs["ti"]
    local_path = ti.xcom_pull(key="local_path", task_ids="price_download")
    ym = ti.xcom_pull(key="year_month", task_ids="price_download")
    filename = f"{ym}_FinalEnergyPrices.csv"
    s3_key = f"{PRICE_S3_PREFIX}/{filename}"

    hook = S3Hook(aws_conn_id="aws_default")
    hook.load_file(filename=local_path, key=s3_key, bucket_name=S3_BUCKET, replace=True)
    os.remove(local_path)
    logger.info("uploaded to s3://%s/%s", S3_BUCKET, s3_key)
    ti.xcom_push(key="s3_key", value=s3_key)


def price_load(**kwargs) -> None:
    from scripts.load_snowflake_price import load_price_month

    ti = kwargs["ti"]
    ym = ti.xcom_pull(key="year_month", task_ids="price_download")
    s3_key = ti.xcom_pull(key="s3_key", task_ids="price_upload")
    rows = load_price_month(ym, s3_key.removeprefix("raw/"))  # raw_stage already at raw/
    ti.xcom_push(key="rows_loaded", value=rows)


# ─── Reconciled market volume branch ─────────────────────────────────


def volume_download(**kwargs) -> None:
    """Fetch one month of EMI reconciled injection/offtake volumes into /tmp."""
    from pathlib import Path

    from scripts.download_volume import fetch_month

    dag_run = kwargs["dag_run"]
    ym = dag_run.conf.get("year_month") or kwargs["logical_date"].strftime("%Y%m")
    year, month = int(ym[:4]), int(ym[4:])
    path = fetch_month(year, month, Path(tempfile.gettempdir()), force=True)
    if path is None:
        raise AirflowSkipException(f"volume month {ym} not yet published")
    kwargs["ti"].xcom_push(key="local_path", value=str(path))
    kwargs["ti"].xcom_push(key="year_month", value=ym)


def volume_validate(**kwargs) -> None:
    from pathlib import Path

    from scripts.validate_volume import validate_file

    local_path = kwargs["ti"].xcom_pull(key="local_path", task_ids="volume_download")
    rows = validate_file(Path(local_path))
    logger.info("volume validation OK — %d rows", rows)
    kwargs["ti"].xcom_push(key="rows_validated", value=rows)


def volume_upload(**kwargs) -> None:
    ti = kwargs["ti"]
    local_path = ti.xcom_pull(key="local_path", task_ids="volume_download")
    ym = ti.xcom_pull(key="year_month", task_ids="volume_download")
    filename = f"{ym}_ReconciledInjectionAndOfftake.csv.gz"
    s3_key = f"{VOLUME_S3_PREFIX}/{filename}"

    hook = S3Hook(aws_conn_id="aws_default")
    hook.load_file(filename=local_path, key=s3_key, bucket_name=S3_BUCKET, replace=True)
    os.remove(local_path)
    logger.info("uploaded to s3://%s/%s", S3_BUCKET, s3_key)
    ti.xcom_push(key="s3_key", value=s3_key)


def volume_load(**kwargs) -> None:
    from scripts.load_snowflake_volume import load_volume_month

    ti = kwargs["ti"]
    ym = ti.xcom_pull(key="year_month", task_ids="volume_download")
    s3_key = ti.xcom_pull(key="s3_key", task_ids="volume_upload")
    rows = load_volume_month(ym, s3_key.removeprefix("raw/"))  # raw_stage already at raw/
    ti.xcom_push(key="rows_loaded", value=rows)


# ─── NSP branch (optional) ────────────────────────────────────────────


def nsp_download(**kwargs) -> None:
    from pathlib import Path

    from scripts.download_nsp import find_latest

    out = Path(tempfile.gettempdir())
    path = find_latest(out)
    if path is None:
        raise AirflowSkipException("NSP file not found in last 14 days")
    kwargs["ti"].xcom_push(key="local_path", value=str(path))


def nsp_upload(**kwargs) -> None:
    ti = kwargs["ti"]
    local_path = ti.xcom_pull(key="local_path", task_ids="nsp_download")
    s3_key = f"{NSP_S3_PREFIX}/NetworkSupplyPointsTable.csv"
    hook = S3Hook(aws_conn_id="aws_default")
    hook.load_file(filename=local_path, key=s3_key, bucket_name=S3_BUCKET, replace=True)
    os.remove(local_path)
    ti.xcom_push(key="s3_key", value=s3_key)


def nsp_load(**kwargs) -> None:
    """COPY INTO raw_nsp from S3 — full reload via TRUNCATE + COPY."""
    import snowflake.connector
    from cryptography.hazmat.primitives.serialization import (
        Encoding,
        NoEncryption,
        PrivateFormat,
        load_pem_private_key,
    )

    key_path = os.path.expanduser(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"])
    with open(key_path, "rb") as f:
        pkey = load_pem_private_key(f.read(), password=None).private_bytes(
            Encoding.DER, PrivateFormat.PKCS8, NoEncryption()
        )

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=pkey,
        database=os.environ["SNOWFLAKE_DATABASE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.environ["SNOWFLAKE_ROLE"],
        schema="RAW",
    )
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("TRUNCATE TABLE raw_nsp")
        cur.execute("""
            COPY INTO raw_nsp
            FROM @raw_stage/nsp/NetworkSupplyPointsTable.csv
            FILE_FORMAT = (FORMAT_NAME = 'csv_format')
            ON_ERROR = 'ABORT_STATEMENT'
            FORCE = TRUE
        """)
        loaded = cur.fetchone()[0]
        cur.execute("COMMIT")
        logger.info("NSP loaded — %s rows", loaded)
    except Exception:
        conn.cursor().execute("ROLLBACK")
        raise
    finally:
        conn.close()


# ─── Hydro branch (optional, best-effort) ────────────────────────────
# HMD is a periodic snapshot (≈ annual). Each monthly DAG run tries to
# download the latest release; the download callable skips files already
# present in the temp dir so re-runs on the same release are cheap.


def hydro_download(**kwargs) -> None:
    """Download latest HMD storage CSVs into temp dir."""
    from pathlib import Path

    from scripts.download_hydro import download_hydro_storage

    out = Path(tempfile.gettempdir()) / "hydro_hmd"
    ok = download_hydro_storage(out)
    if not ok:
        raise AirflowSkipException("No HMD release found or download failed")
    kwargs["ti"].xcom_push(key="hydro_dir", value=str(out / "hydro"))


def hydro_upload(**kwargs) -> None:
    """Upload each downloaded lake CSV to S3."""
    import re
    from pathlib import Path

    ti = kwargs["ti"]
    hydro_dir = Path(ti.xcom_pull(key="hydro_dir", task_ids="hydro_download"))
    hook = S3Hook(aws_conn_id="aws_default")
    pattern = re.compile(r"^[A-Z]{2}_[A-Z]{2,4}_Storage_.*\.csv$")
    files = [f for f in hydro_dir.iterdir() if pattern.match(f.name)]
    if not files:
        raise ValueError(f"No HMD storage CSVs found in {hydro_dir}")
    for f in files:
        s3_key = f"{HYDRO_S3_PREFIX}/{f.name}"
        hook.load_file(filename=str(f), key=s3_key, bucket_name=S3_BUCKET, replace=True)
        logger.info("uploaded %s to s3://%s/%s", f.name, S3_BUCKET, s3_key)
    ti.xcom_push(key="file_count", value=len(files))


def hydro_load(**kwargs) -> None:
    """COPY INTO raw_hydro_storage from S3 — full reload via TRUNCATE + COPY."""
    import re

    import snowflake.connector
    from cryptography.hazmat.primitives.serialization import (
        Encoding,
        NoEncryption,
        PrivateFormat,
        load_pem_private_key,
    )

    hook = S3Hook(aws_conn_id="aws_default")
    pattern = re.compile(r"([A-Z]{2}_[A-Z]{2,4})_Storage_.*\.csv$")
    keys = [
        key for key in hook.list_keys(bucket_name=S3_BUCKET, prefix=HYDRO_S3_PREFIX) or []
        if pattern.search(key)
    ]
    if not keys:
        raise ValueError(f"No HMD storage CSVs found at s3://{S3_BUCKET}/{HYDRO_S3_PREFIX}")

    key_path = os.path.expanduser(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"])
    with open(key_path, "rb") as f:
        pkey = load_pem_private_key(f.read(), password=None).private_bytes(
            Encoding.DER, PrivateFormat.PKCS8, NoEncryption()
        )
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=pkey,
        database=os.environ["SNOWFLAKE_DATABASE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.environ["SNOWFLAKE_ROLE"],
        schema="RAW",
    )
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("TRUNCATE TABLE raw_hydro_storage")
        total = 0
        for key in keys:
            m = pattern.search(key)
            site_code = m.group(1)
            copy_sql = f"""
                COPY INTO raw_hydro_storage (
                    site_code, date_str, time_str, level_m,
                    active_storage_mm3, contingent_storage_mm3, quality_code
                )
                FROM (
                    SELECT '{site_code}', $1, $2, $3, $4, $5, $6
                    FROM @raw_stage/hydro_storage/{key.split('/')[-1]}
                )
                FILE_FORMAT = (FORMAT_NAME = 'csv_format' SKIP_HEADER = 1)
                ON_ERROR = 'CONTINUE'
                FORCE = TRUE
            """
            cur.execute(copy_sql)
            total += cur.fetchone()[0]
        cur.execute("COMMIT")
        logger.info("hydro loaded — %d rows across %d files", total, len(keys))
    except Exception:
        conn.cursor().execute("ROLLBACK")
        raise
    finally:
        conn.close()


# ─── DAG ──────────────────────────────────────────────────────────────


def slack_alert(context) -> None:
    """on_failure_callback — POST a concise failure summary to Slack.

    Webhook URL via SLACK_WEBHOOK_URL env var. If unset, this is a no-op
    (email_on_failure stays as the fallback channel).
    """
    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook:
        return
    import json
    import urllib.request

    ti = context.get("task_instance")
    dag_id = context.get("dag").dag_id if context.get("dag") else "?"
    task_id = ti.task_id if ti else "?"
    run_id = context.get("run_id", "?")
    exception = context.get("exception", "")
    log_url = ti.log_url if ti and hasattr(ti, "log_url") else ""

    payload = {
        "text": f":rotating_light: *{dag_id}.{task_id}* failed",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*DAG:* `{dag_id}`\n"
                        f"*Task:* `{task_id}`\n"
                        f"*Run:* `{run_id}`\n"
                        f"*Exception:* ```{str(exception)[:500]}```\n"
                        f"{f'<{log_url}|View logs>' if log_url else ''}"
                    ),
                },
            }
        ],
    }
    try:
        req = urllib.request.Request(
            webhook,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception as exc:  # don't let alerting break the DAG
        logger.warning("slack alert failed: %s", exc)


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": slack_alert,
}

with DAG(
    dag_id="nz_electricity_v2",
    default_args=default_args,
    description="V2 monthly pipeline: Generation + Price + NSP + dbt",
    schedule="0 0 15 * *",
    start_date=datetime(2016, 1, 15),
    catchup=False,
    max_active_runs=3,
    tags=["nz-electricity", "elt", "v2"],
) as dag:

    # Generation branch — V1 callables reused
    g_download = PythonOperator(
        task_id="generation_download",
        python_callable=generation_download_csv,
        pool="emi_download_pool",
    )
    g_validate = PythonOperator(
        task_id="generation_validate", python_callable=generation_validate_csv
    )
    g_upload = PythonOperator(
        task_id="generation_upload", python_callable=generation_upload_to_s3
    )
    g_load = PythonOperator(
        task_id="generation_load", python_callable=generation_load_to_snowflake
    )

    # Price branch
    p_download = PythonOperator(
        task_id="price_download",
        python_callable=price_download,
        pool="emi_download_pool",
    )
    p_validate = PythonOperator(task_id="price_validate", python_callable=price_validate)
    p_upload = PythonOperator(task_id="price_upload", python_callable=price_upload)
    p_load = PythonOperator(task_id="price_load", python_callable=price_load)

    # Reconciled market volume branch
    v_download = PythonOperator(
        task_id="volume_download",
        python_callable=volume_download,
        pool="emi_download_pool",
    )
    v_validate = PythonOperator(task_id="volume_validate", python_callable=volume_validate)
    v_upload = PythonOperator(task_id="volume_upload", python_callable=volume_upload)
    v_load = PythonOperator(task_id="volume_load", python_callable=volume_load)

    # NSP branch — best-effort (NONE_FAILED downstream so dbt still runs)
    n_download = PythonOperator(
        task_id="nsp_download",
        python_callable=nsp_download,
        pool="emi_download_pool",
    )
    n_upload = PythonOperator(task_id="nsp_upload", python_callable=nsp_upload)
    n_load = PythonOperator(task_id="nsp_load", python_callable=nsp_load)

    # Hydro branch — best-effort (annual HMD snapshot; skip gracefully if unavailable)
    h_download = PythonOperator(
        task_id="hydro_download",
        python_callable=hydro_download,
        pool="emi_download_pool",
    )
    h_upload = PythonOperator(task_id="hydro_upload", python_callable=hydro_upload)
    h_load = PythonOperator(task_id="hydro_load", python_callable=hydro_load)

    # dbt
    t_check_dbt = ShortCircuitOperator(
        task_id="check_run_dbt", python_callable=check_run_dbt
    )
    # Pre-create raw_dbt_run table so the first DAG run on a new SF account
    # doesn't fail stg_dbt_run/fct_dbt_run on missing source — that failure
    # would leave a permanent 92.6% baseline in fct_dbt_run model-success KPI.
    # See PRD §10.1 Known Limitations and changelog 6.1-impl.
    t_init_raw_dbt_run = BashOperator(
        task_id="prepare_raw_dbt_run",
        bash_command=(
            "python /opt/airflow/scripts/ingest_dbt_artifacts.py "
            "--init --target snowflake"
        ),
        trigger_rule=TriggerRule.NONE_FAILED,
    )
    t_dbt_run = BashOperator(
        task_id="run_dbt",
        bash_command="cd /opt/dbt && dbt seed --target prod && dbt run --target prod",
        pool="dbt_pool",
        trigger_rule=TriggerRule.NONE_FAILED,  # OK even if NSP skipped
    )
    t_dbt_test = BashOperator(
        task_id="run_dbt_tests",
        bash_command="cd /opt/dbt && dbt test --target prod",
        pool="dbt_pool",
    )

    # Ingest dbt artifacts → raw_dbt_run (Tier-1 observability).
    # Runs after every test execution regardless of test pass/fail so we
    # capture failure runs too (trigger_rule=ALL_DONE).
    t_ingest_artifacts = BashOperator(
        task_id="ingest_dbt_artifacts",
        bash_command=(
            "python /opt/airflow/scripts/ingest_dbt_artifacts.py "
            "--artifact /opt/dbt/target/run_results.json "
            "--target snowflake"
        ),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Wiring
    g_download >> g_validate >> g_upload >> g_load
    p_download >> p_validate >> p_upload >> p_load
    v_download >> v_validate >> v_upload >> v_load
    n_download >> n_upload >> n_load
    h_download >> h_upload >> h_load

    [g_load, p_load, v_load, n_load, h_load] >> t_check_dbt >> t_init_raw_dbt_run >> t_dbt_run >> t_dbt_test >> t_ingest_artifacts

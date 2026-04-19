"""
NZ Electricity Generation — Monthly Batch DAG

Downloads monthly generation CSV from EMI, validates, uploads to S3,
loads into Snowflake via transactional COPY INTO, then runs dbt.

Schedule: 0 0 15 * * (15th of each month — EMI publishes ~10th, 5-day buffer)
7 tasks: download → validate → upload → load → check_run_dbt → dbt_models → dbt_tests
"""

import csv
import logging
import os
import tempfile
from datetime import datetime, timedelta

import requests
import snowflake.connector
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)

EMI_BASE_URL = (
    "https://www.emi.ea.govt.nz/Wholesale/Datasets/Generation/Generation_MD"
)
S3_BUCKET = os.environ.get("S3_BUCKET_NAME", "nz-electricity-generation")
S3_RAW_PREFIX = "raw/generation_md"

EXPECTED_COLUMNS = 57
KNOWN_FUEL_CODES = {
    "Gas", "Hydro", "HYD", "Geo", "GEO", "Wind",
    "Diesel", "Coal", "Wood", "Solar", "SOL", "ELE",
}


def _get_year_month(**kwargs) -> str:
    """Derive YYYYMM: conf takes precedence, then logical_date."""
    dag_run = kwargs["dag_run"]
    return (
        dag_run.conf.get("year_month")
        or kwargs["logical_date"].strftime("%Y%m")
    )


def _csv_filename(year_month: str) -> str:
    return f"{year_month}_Generation_MD.csv"


# ──────────────────────────────────────────────
# Task 1: download_csv
# ──────────────────────────────────────────────
def download_csv(**kwargs) -> None:
    """Download a single month's CSV from the EMI website.

    - Exponential backoff: 3 attempts, 30s base.
    - 404 → AirflowSkipException (no alert, all downstream auto-skipped).
    - Timeout 60s per request.
    """
    year_month = _get_year_month(**kwargs)
    filename = _csv_filename(year_month)
    url = f"{EMI_BASE_URL}/{filename}"

    last_exc = None
    for attempt in range(3):
        try:
            resp = requests.get(url, timeout=60)
            if resp.status_code == 404:
                raise AirflowSkipException(
                    f"EMI returned 404 for {filename} — file not published yet"
                )
            resp.raise_for_status()
            break
        except AirflowSkipException:
            raise
        except Exception as exc:
            last_exc = exc
            wait = 30 * (2 ** attempt)
            logger.warning(
                "Attempt %d failed for %s: %s. Retrying in %ds...",
                attempt + 1, url, exc, wait,
            )
            import time
            time.sleep(wait)
    else:
        raise last_exc  # type: ignore[misc]

    local_path = os.path.join(tempfile.gettempdir(), filename)
    with open(local_path, "wb") as f:
        f.write(resp.content)

    file_size = os.path.getsize(local_path)
    logger.info("Downloaded %s (%d bytes)", filename, file_size)

    ti = kwargs["ti"]
    ti.xcom_push(key="local_path", value=local_path)
    ti.xcom_push(key="year_month", value=year_month)
    ti.xcom_push(key="file_size_bytes", value=file_size)


# ──────────────────────────────────────────────
# Task 2: validate_csv
# ──────────────────────────────────────────────
def validate_csv(**kwargs) -> None:
    """Validate structural and content integrity of the downloaded CSV.

    Structural: 57 columns, 100–50,000 rows, 1KB–10MB file size.
    Content: Trading_Date format + month match, Fuel_Code domain check
             (fail on unknown), TP columns full numeric-or-null check.
    """
    ti = kwargs["ti"]
    local_path = ti.xcom_pull(key="local_path", task_ids="download_csv")
    year_month = ti.xcom_pull(key="year_month", task_ids="download_csv")

    file_size = os.path.getsize(local_path)
    if not (1_024 <= file_size <= 10 * 1024 * 1024):
        raise ValueError(
            f"File size {file_size} bytes outside expected range [1KB, 10MB]"
        )

    with open(local_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)

        if len(header) != EXPECTED_COLUMNS:
            raise ValueError(
                f"Expected {EXPECTED_COLUMNS} columns, got {len(header)}"
            )

        row_count = 0
        for row in reader:
            row_count += 1

            # Trading_Date format (column index 6) — YYYY-MM-DD
            trading_date = row[6]
            if len(trading_date) != 10 or trading_date[4] != "-":
                raise ValueError(
                    f"Row {row_count}: invalid Trading_Date format: {trading_date}"
                )
            # Month match
            row_month = trading_date[:7].replace("-", "")
            if row_month != year_month:
                raise ValueError(
                    f"Row {row_count}: Trading_Date month {row_month} "
                    f"does not match expected {year_month}"
                )

            # Fuel_Code domain check (column index 4)
            fuel_code = row[4]
            if fuel_code not in KNOWN_FUEL_CODES:
                raise ValueError(
                    f"Row {row_count}: unknown Fuel_Code '{fuel_code}'. "
                    f"Known codes: {sorted(KNOWN_FUEL_CODES)}"
                )

            # TP columns (indices 7–56) — numeric or empty
            for i in range(7, EXPECTED_COLUMNS):
                val = row[i].strip()
                if val == "" or val.upper() == "NULL":
                    continue
                try:
                    float(val)
                except ValueError:
                    raise ValueError(
                        f"Row {row_count}, column {i} (TP{i - 6}): "
                        f"non-numeric value '{val}'"
                    )

    if not (100 <= row_count <= 50_000):
        raise ValueError(
            f"Row count {row_count} outside expected range [100, 50000]"
        )

    logger.info(
        "Validation passed: %d rows, %d columns, %d bytes",
        row_count, EXPECTED_COLUMNS, file_size,
    )
    ti.xcom_push(key="rows_validated", value=row_count)


# ──────────────────────────────────────────────
# Task 3: upload_to_s3
# ──────────────────────────────────────────────
def upload_to_s3(**kwargs) -> None:
    """Upload the validated CSV to S3 (overwrite = idempotent)."""
    ti = kwargs["ti"]
    local_path = ti.xcom_pull(key="local_path", task_ids="download_csv")
    year_month = ti.xcom_pull(key="year_month", task_ids="download_csv")
    filename = _csv_filename(year_month)
    s3_key = f"{S3_RAW_PREFIX}/{filename}"

    hook = S3Hook(aws_conn_id="aws_default")
    hook.load_file(
        filename=local_path,
        key=s3_key,
        bucket_name=S3_BUCKET,
        replace=True,
    )

    os.remove(local_path)
    logger.info("Uploaded to s3://%s/%s", S3_BUCKET, s3_key)
    ti.xcom_push(key="s3_key", value=s3_key)


# ──────────────────────────────────────────────
# Task 4: load_to_snowflake
# ──────────────────────────────────────────────
def load_to_snowflake(**kwargs) -> None:
    """Load CSV from S3 into Snowflake raw_generation table.

    Uses snowflake-connector-python (not SnowflakeOperator) for
    transactional DELETE + COPY INTO with explicit ROLLBACK on failure.
    Captures METADATA$FILE_LAST_MODIFIED as _source_file_modified_at.
    """
    ti = kwargs["ti"]
    year_month = ti.xcom_pull(key="year_month", task_ids="download_csv")
    filename = _csv_filename(year_month)

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.environ["SNOWFLAKE_ROLE"],
        schema="RAW",
    )

    try:
        cur = conn.cursor()
        cur.execute("BEGIN")

        cur.execute(
            "DELETE FROM raw_generation WHERE trading_month = %s",
            (year_month,),
        )
        logger.info("Deleted existing rows for trading_month=%s", year_month)

        copy_sql = f"""
            COPY INTO raw_generation
            FROM (
                SELECT
                    $1, $2, $3, $4, $5, $6, $7,
                    $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
                    $18, $19, $20, $21, $22, $23, $24, $25, $26, $27,
                    $28, $29, $30, $31, $32, $33, $34, $35, $36, $37,
                    $38, $39, $40, $41, $42, $43, $44, $45, $46, $47,
                    $48, $49, $50, $51, $52, $53, $54, $55, $56, $57,
                    '{year_month}' AS trading_month,
                    METADATA$FILE_LAST_MODIFIED AS _source_file_modified_at
                FROM @raw_stage/generation_md/{filename}
            )
            FILE_FORMAT = (FORMAT_NAME = 'csv_format')
            ON_ERROR = 'ABORT_STATEMENT'
        """
        result = cur.execute(copy_sql)
        rows_loaded = cur.fetchone()[0] if result else 0

        cur.execute("COMMIT")
        logger.info(
            "Loaded %s rows for trading_month=%s", rows_loaded, year_month
        )
        ti.xcom_push(key="rows_loaded", value=rows_loaded)

    except Exception:
        conn.cursor().execute("ROLLBACK")
        logger.error(
            "ROLLBACK executed for trading_month=%s", year_month
        )
        raise
    finally:
        conn.close()


# ──────────────────────────────────────────────
# Task 5: check_run_dbt (ShortCircuitOperator)
# ──────────────────────────────────────────────
def check_run_dbt(**kwargs) -> bool:
    """Return False to skip dbt when skip_dbt=true in DAG conf."""
    skip = kwargs["dag_run"].conf.get("skip_dbt", False)
    if skip:
        logger.info("skip_dbt=True — skipping dbt tasks")
    return not skip


# ──────────────────────────────────────────────
# DAG definition
# ──────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nz_electricity_monthly",
    default_args=default_args,
    description="Monthly NZ electricity generation data pipeline",
    schedule="0 0 15 * *",
    start_date=datetime(2016, 1, 15),
    catchup=False,
    max_active_runs=3,
    tags=["nz-electricity", "elt"],
) as dag:

    t_download = PythonOperator(
        task_id="download_csv",
        python_callable=download_csv,
        pool="emi_download_pool",
    )

    t_validate = PythonOperator(
        task_id="validate_csv",
        python_callable=validate_csv,
    )

    t_upload = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )

    t_load = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake,
    )

    t_check_dbt = ShortCircuitOperator(
        task_id="check_run_dbt",
        python_callable=check_run_dbt,
    )

    t_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command="cd /opt/dbt && dbt seed && dbt run",
        pool="dbt_pool",
    )

    t_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command="cd /opt/dbt && dbt test",
        pool="dbt_pool",
    )

    (
        t_download
        >> t_validate
        >> t_upload
        >> t_load
        >> t_check_dbt
        >> t_dbt_models
        >> t_dbt_tests
    )

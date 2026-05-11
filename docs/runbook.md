# Runbook — Failure Diagnosis & Response

This runbook covers known failure modes in the NZ Electricity Wholesale Market pipeline (V1 generation + V2 price + NSP). Each section describes what triggers the failure, how to detect it, and the exact steps to resolve it.

---

## 0. V1 → V2 migration

V1 (`nz_electricity_monthly`, generation only) and V2 (`nz_electricity_v2`, generation + price + NSP) **coexist** during the migration window. The V1 DAG is the fallback path; V2 is the source of truth once enabled.

**Cutover checklist:**
1. `terraform apply` to provision `raw_price` + `raw_nsp` tables and the new S3 prefixes (`raw/final_energy_prices/`, `raw/nsp/`).
2. `docker compose build` to pick up the updated `requirements-airflow.txt` (adds `dbt-duckdb`, `requests`, `pandas`, `duckdb`).
3. In Airflow UI, unpause `nz_electricity_v2`. Trigger one manual run with `{"year_month": "<last complete month>"}` and confirm all three ingest branches finish + dbt run/test pass.
4. Pause `nz_electricity_monthly`. Keep the file in `airflow/dags/` for ~1 month as a hot fallback.
5. `make cloud-dbt-full` once to materialise all Phase 1+2 marts on Snowflake.

**Rollback:** unpause V1, pause V2. Generation continues from V1's `raw_generation` table — V2 marts won't refresh but the V1 dashboard pages remain functional.

---

## 0.1 Dual-mode operation troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `dbt debug --target dev` fails with "no profiles" | `~/.dbt/profiles.yml` from another project | `--profiles-dir ./dbt` |
| `Catalog "NZ_ELECTRICITY_DB" does not exist` on DuckDB | `sources.yml` not target-aware | Already fixed Phase 0 (`{{ target.database }}`) |
| `read_only=True` collides with concurrent dbt run | Streamlit + dbt both touch `.duckdb` | dbt has the write lock; refresh Streamlit after dbt finishes |
| `[Errno 2] /opt/airflow/secrets/snowflake_rsa_key.p8` on host | `.env` has the Docker container path | `SNOWFLAKE_PRIVATE_KEY_PATH=~/.ssh/snowflake_rsa_key.p8 uv run dbt …` |
| `Table Function "flatten" not in catalog` on DuckDB | model uses SF-only `LATERAL FLATTEN` | Use `unpivot_trading_periods` macro or target-aware inline |
| `incremental_strategy 'merge' is not valid` | `merge` is SF-only | Switch to `delete+insert` |

---

## 1. EMI 404 — File Not Published Yet

**Detection**: `download_csv` raises `AirflowSkipException`. All downstream tasks auto-skip.

**Response**: No action needed. This is expected behaviour — EMI publishes monthly files with a variable delay. The dbt source freshness monitor will warn at 35 days and error at 45 days if the file remains unavailable.

**When to escalate**: If the file hasn't appeared after 45 days, check the [EMI website](https://www.emi.ea.govt.nz/) directly — the URL format may have changed.

---

## 2. EMI Timeout / 5xx

**Detection**: `download_csv` exhausts retry attempts (3 retries with exponential backoff). Airflow sends email alert.

**Response**:
1. Check if EMI website is accessible in a browser.
2. If temporary outage — wait and manually re-trigger the DAG run for the affected month.
3. If persistent — check EMI status/announcements for maintenance windows.

**Re-trigger**:
```bash
docker compose exec airflow-scheduler \
  airflow dags trigger nz_electricity_monthly \
  --conf '{"year_month":"202603"}'
```

---

## 3. CSV Schema Change

**Detection**: `validate_csv` structural check fails (expected 57 columns, wrong column names, or unexpected file format). Airflow alert fires.

**Response**:
1. Download the file manually and inspect the schema change.
2. Update the validation logic in `airflow/dags/nz_electricity_monthly.py` (`validate_csv` task).
3. If column count changed — update the expected column count and names.
4. If the wide-format structure changed fundamentally — the dbt LATERAL FLATTEN logic in `stg_generation.sql` may also need updating.
5. Re-trigger the DAG for the affected month.

---

## 4. Unknown Fuel Code

**Detection**: `validate_csv` content check fails — a fuel code in the CSV is not in the known set. Airflow alert fires.

**Response**:
1. Identify the new fuel code from the error message.
2. Add the new code to `dbt/seeds/fuel_codes.csv` with the correct `fuel_type` mapping.
3. Update the known fuel code set in `validate_csv` (in the DAG file) to include the new code.
4. `dbt seed` runs automatically as part of the `run_dbt_models` task — no manual seed step needed.
5. Re-trigger the DAG for the affected month.

**Example** — adding a new fuel code:
```csv
# dbt/seeds/fuel_codes.csv
fuel_code,fuel_type,is_renewable
NEW_CODE,New Fuel Type,false
```

---

## 5. Snowflake COPY Failure

**Detection**: `load_to_snowflake` task fails. The explicit ROLLBACK in the except handler ensures the preceding DELETE is reverted — no data loss.

**Response**:
1. Check the Airflow task log for the specific error.
2. Common causes:
   - **Credential expiry**: Check `.env` file and Snowflake user status.
   - **Warehouse suspended**: Snowflake auto-suspends after idle timeout — the query should auto-resume it. If not, check warehouse settings.
   - **File format mismatch**: Inspect `COPY_HISTORY` in Snowflake:
     ```sql
     SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
       TABLE_NAME => 'RAW_GENERATION',
       START_TIME => DATEADD(hours, -24, CURRENT_TIMESTAMP())
     ));
     ```
3. Fix the root cause and re-trigger the DAG.

**Data integrity**: The transactional `BEGIN → DELETE → COPY INTO → COMMIT` pattern with explicit ROLLBACK in the except handler guarantees atomicity. If COPY fails, DELETE is rolled back. No partial state.

---

## 6. dbt Model Failure

**Detection**: `run_dbt_models` task fails. `run_dbt_tests` is automatically skipped.

**Response**:
1. Check the Airflow task log — it includes the dbt output with the failing model and SQL error.
2. Common causes:
   - **SQL syntax error**: Fix the model SQL and re-trigger.
   - **Source table missing**: Verify `load_to_snowflake` completed for the required months.
   - **Schema drift**: Check if Snowflake table structure has changed.
3. For a quick check, compile locally:
   ```bash
   docker compose exec airflow-scheduler bash -c \
     "cd /opt/dbt && dbt compile"
   ```
4. Re-trigger the DAG or run dbt manually:
   ```bash
   docker compose exec airflow-scheduler bash -c \
     "cd /opt/dbt && dbt run --select <model_name>"
   ```

---

## 7. dbt Test Failure

**Detection**: `run_dbt_tests` task fails. Data has been loaded and transformed but is flagged as untested / potentially invalid.

**Response**:
1. Check which test(s) failed in the Airflow task log.
2. **not_null / unique failures**: Likely a data quality issue in the source file — inspect the raw data for the affected month.
3. **Accepted values failure**: A new fuel code or unexpected value — see §4.
4. **NULL anomaly ratio**: `test_unexpected_null_ratio.sql` warns if TP1-46 NULL ratio exceeds 0.1%. Investigate whether these are genuine DST nulls or data issues.
5. **Cross-layer reconciliation**: `test_fct_mart_monthly_reconciliation.sql` checks `SUM(generation_kwh)` in fct = `SUM(total_generation_kwh)` in mart. Failure indicates a bug in mart aggregation logic.
6. Run tests in isolation:
   ```bash
   docker compose exec airflow-scheduler bash -c \
     "cd /opt/dbt && dbt test --select <test_name>"
   ```

---

## 8. Data Freshness Breach

**Detection**: `dbt source freshness` reports warn (35 days) or error (45 days) based on `_source_file_modified_at` timestamp.

**Response**:
1. Check if EMI has published the month's file on their website.
2. If published but not loaded — the DAG may have failed or been paused. Check Airflow DAG runs.
3. If not published — this is expected for recent months. Monitor and wait.

**Note**: The freshness check uses `_source_file_modified_at` (TIMESTAMP_NTZ from S3 file metadata), NOT `trading_date` (VARCHAR). Using `trading_date` would cause a type error.

---

## 9. Historical Data Correction

**Scenario**: EMI republishes a corrected file for a past month.

**Response**:
1. Re-trigger the DAG for the affected month(s):
   ```bash
   docker compose exec airflow-scheduler \
     airflow dags trigger nz_electricity_monthly \
     --conf '{"year_month":"202401","skip_dbt":true}'
   ```
2. **Critical**: Run `dbt run --full-refresh` for all marts:
   ```bash
   docker compose exec airflow-scheduler bash -c \
     "cd /opt/dbt && dbt run --full-refresh && dbt test"
   ```
   Incremental lookback windows (3 days / 1 month) will **NOT** pick up older corrections. `stg_generation` is a full rebuild and will reflect the fix automatically, but marts need an explicit full refresh.

---

## 10. Infrastructure Issues

### Terraform State Lock

**Symptom**: `terraform apply` hangs or reports lock error.

**Response**:
1. Check if another `terraform apply` is running.
2. If the lock is stale (e.g., from a crashed process):
   ```bash
   terraform force-unlock <LOCK_ID>
   ```

### Docker Compose Issues

**Symptom**: Airflow containers fail to start.

**Response**:
1. Check container logs: `make logs`
2. Common causes:
   - **Port conflict**: 8080 already in use. Stop the conflicting process.
   - **Database migration**: Run `docker compose exec airflow-init bash` to re-run init.
   - **Volume permissions**: Ensure `airflow/dags/` and `dbt/` directories are readable.

### AWS Budget Alert

**Threshold**: $5/month (configured in `terraform/budget.tf`).

**Response**: Review S3 storage usage. At ~100 MB total, costs should be negligible. Check for unexpected data duplication or lifecycle policy misconfiguration.

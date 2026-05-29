# S3 + Local Airflow Smoke Test Evidence - 2026-05-28

## Scope

- DAG: `nz_electricity_v2`
- Manual run id: `smoke_202401_20260528_094908`
- Test month: `202401`
- Config: `{"year_month":"202401","skip_dbt":true}`
- Airflow UI: `http://localhost:8080`

## S3 Upload Evidence

The following raw files are present in S3 after the one-month smoke run:

```text
2026-05-28 09:49:15     724720 202401_Generation_MD.csv
2026-05-28 09:49:18   10205884 202401_FinalEnergyPrices.csv
2026-05-28 09:49:35   30153656 202401_ReconciledInjectionAndOfftake.csv.gz
2026-05-28 09:49:19     432486 NetworkSupplyPointsTable.csv
```

S3 locations:

```text
s3://nz-electricity-generation/raw/generation_md/202401_Generation_MD.csv
s3://nz-electricity-generation/raw/final_energy_prices/202401_FinalEnergyPrices.csv
s3://nz-electricity-generation/raw/reconciled_volumes/202401_ReconciledInjectionAndOfftake.csv.gz
s3://nz-electricity-generation/raw/nsp/NetworkSupplyPointsTable.csv
```

## Airflow Task State Evidence

Successful one-month local Airflow tasks:

```text
generation_download   success
generation_validate   success
generation_upload     success
price_download        success
price_validate        success
price_upload          success
volume_download       success
volume_validate       success
volume_upload         success
nsp_download          success
nsp_upload            success
hydro_download        success
hydro_upload          success
offers_enabled        success
```

Expected skipped tasks because `include_offers` was not enabled:

```text
offers_download       skipped
offers_validate       skipped
offers_upload         skipped
offers_load           skipped
```

Blocked Snowflake load tasks:

```text
generation_load       up_for_retry
price_load            up_for_retry
volume_load           up_for_retry
nsp_load              up_for_retry
hydro_load            up_for_retry
```

Downstream dbt tasks did not start because upstream Snowflake load tasks did not complete:

```text
check_run_dbt         none
prepare_raw_dbt_run   none
run_dbt               none
run_dbt_tests         none
ingest_dbt_artifacts  none
```

## Blocker

Snowflake connection fails before any table load:

```text
snowflake.connector.errors.DatabaseError: 250001 (08001): None:
Failed to connect to DB: YXSAXNV-TP81717.snowflakecomputing.com:443.
JWT token is invalid.
```

Interpretation: the Snowflake user/account is reachable, but key-pair authentication is invalid. The most likely cause is that the private key mounted into Airflow does not match the public key registered on the Snowflake user, or `.env` points to the wrong Snowflake user/account.

## Current Local Container State

```text
airflow-webserver   Up, healthy, exposed on 8080
postgres            Up, healthy
airflow-scheduler   Stopped intentionally to prevent repeated Snowflake retries
```

## Decision

Do not start the full historical run yet. The one-month S3/Airflow upload path is proven, but Snowflake authentication must be fixed before full backfill to avoid repeated failed cloud loads and wasted runtime.

## Follow-up Smoke After Snowflake Key and Schema Fix

- Manual run id: `smoke_202401_schemafix_20260528_2238`
- Test month: `202401`
- Config: `{"year_month":"202401","skip_dbt":false}`
- Result: all non-offer tasks succeeded; offer tasks were skipped because offers were not enabled for this smoke run.

Airflow end-to-end task result:

```text
generation_load       success
price_load            success
volume_load           success
nsp_load              success
hydro_load            success
run_dbt               success
run_dbt_tests         success
ingest_dbt_artifacts  success
```

dbt run result:

```text
PASS=36 WARN=0 ERROR=0 SKIP=0 TOTAL=36
```

dbt test result:

```text
PASS=211 WARN=1 ERROR=0 SKIP=0 TOTAL=212
```

Known dbt warning:

```text
unique_dim_plant_gen_code: 20 duplicate gen_code values, configured as warn
```

Snowflake schemas verified after the dbt schema naming fix:

```text
RAW
STAGING
ANALYTICS
```

Representative Snowflake row counts:

```text
RAW.RAW_GENERATION                 291504
RAW.RAW_HYDRO_STORAGE              164380
RAW.RAW_MARKET_VOLUME              11651076
RAW.RAW_NSP                        2573
RAW.RAW_PRICE                      43843043
STAGING.STG_GENERATION             12699996
STAGING.STG_HYDRO_STORAGE          32880
STAGING.STG_MARKET_VOLUME          11651076
STAGING.STG_NSP                    766
STAGING.STG_PRICE                  43843043
ANALYTICS.FCT_GENERATION           12699996
ANALYTICS.FCT_HYDRO                32880
ANALYTICS.FCT_MARKET_VOLUME        628132
ANALYTICS.FCT_PRICE                43843043
ANALYTICS.MART_PRICE_DAILY         913394
ANALYTICS.MART_RENEWABLE_RATIO     124
```

Legacy schemas from earlier misconfigured dbt runs were cleaned up after verification:

```text
RAW_RAW
RAW_ANALYTICS
```

Final verified Snowflake schemas:

```text
ANALYTICS
RAW
STAGING
```

## Offers One-Day Smoke

- Manual run id: `offers_20240101_20260528_1336`
- Offer date: `20240101`
- Config: `{"year_month":"202401","offer_date":"20240101","include_offers":true,"skip_dbt":true}`
- Result: success

Airflow task result:

```text
offers_download      success
offers_validate      success
offers_upload        success
offers_load          success
run_dbt              skipped
run_dbt_tests        skipped
```

S3 object:

```text
s3://nz-electricity-generation/raw/offers/20240101_Offers.csv
size: 31,384,398 bytes
```

Snowflake raw row count:

```text
RAW.RAW_OFFERS trading_date=2024-01-01 trading_month=202401 rows=200614
```

Implementation note: the Offers validator now allows blank `Unit` for non-Energy/Injection rows, because reserve/offtake offer rows in the public EMI file can have an empty Unit. Energy/Injection rows still require Unit for downstream offer-stack analytics.

## Offers Full S3 Backfill Queued

- DAG: `nz_offers_s3_backfill`
- Purpose: S3-only backfill for daily EMI Offers files; no Snowflake load and no dbt run.
- Coverage queued: `201601` through `202605` (May 2026 run only includes complete days through yesterday).
- Runs queued: 125 monthly DAG runs.
- Concurrency controls: `max_active_runs=2`, `emi_download_pool`.

Initial Airflow state after queuing:

```text
running: 2
queued: 123
```

Initial S3 progress:

```text
prefix: s3://nz-electricity-generation/raw/offers/
objects: 44
bytes: 708,157,408
```

Example successful uploads from first run:

```text
s3://nz-electricity-generation/raw/offers/20160101_Offers.csv
s3://nz-electricity-generation/raw/offers/20160102_Offers.csv
s3://nz-electricity-generation/raw/offers/20160103_Offers.csv
```

## Reconciled Volumes Full S3 Backfill Queued

- DAG: `nz_volume_s3_backfill`
- Purpose: S3-only backfill for monthly EMI reconciled injection/offtake volume files; no Snowflake load and no dbt run.
- File format: keep source `.csv.gz` files in S3. Snowflake can COPY gzip files directly, and keeping gzip avoids unnecessary storage and transfer cost.
- Coverage queued: `201601` through `202604`.
- Runs queued: 124 monthly DAG runs.

Initial Airflow state after queuing:

```text
running: 2
queued: 122
```

Initial S3 progress:

```text
prefix: s3://nz-electricity-generation/raw/reconciled_volumes/
objects: 3
bytes: 84,811,981
```

First probe run verified:

```text
run_id: volume_s3_201601_probe_20260528
object: s3://nz-electricity-generation/raw/reconciled_volumes/201601_ReconciledInjectionAndOfftake.csv.gz
size: 24,748,457 bytes
result: success
```

Current safety state:

```text
nz_electricity_v2       paused
nz_electricity_monthly  paused
nz_offers_s3_backfill   active
nz_volume_s3_backfill   active
```

## Full S3 Backfill Completion

Airflow DAG run summary:

```text
nz_offers_s3_backfill   success: 125
nz_volume_s3_backfill   success: 124
```

Verified S3 object counts:

```text
s3://nz-electricity-generation/raw/reconciled_volumes/
objects: 124
bytes: 4,026,216,158

s3://nz-electricity-generation/raw/offers/
objects: 3,801
bytes: 177,773,392,097
```

## Reconciled Volumes Snowflake RAW Backfill

- DAG: `nz_volume_snowflake_backfill`
- Purpose: load existing S3 reconciled volume gzip files into `RAW.RAW_MARKET_VOLUME`; no dbt run.
- Probe: `volume_sf_202401_probe2_20260529`
- Full run suffix: `20260529112126`
- Result: all Airflow runs succeeded.

Airflow DAG run summary:

```text
nz_volume_snowflake_backfill success: 125
```

Snowflake RAW validation:

```text
table: RAW.RAW_MARKET_VOLUME
loaded_months: 124
min_month: 201601
max_month: 202604
raw_rows: 646,903,307
min_date: 2016-01-01
max_date: 2026-04-30
```

Sample month row counts:

```text
201601  3,920,696
202401  5,899,262
202604  5,759,040
```

Operational note:

```text
ALTER WAREHOUSE TRANSFORM_WH SUSPEND failed for TRANSFORMER_SVC:
Insufficient privileges to operate on warehouse 'TRANSFORM_WH'.
```

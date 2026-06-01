.PHONY: demo local-full local-subset market-subset offer-sample dbt-test terraform-init terraform-plan terraform-apply \
        cloud-up cloud-backfill cloud-offers-s3-backfill cloud-volume-s3-backfill cloud-dbt-full cloud-dashboard build up down restart logs \
        backfill dbt-full

# ==================== Interview demo ====================
demo:                    ## ~90s startup: latest 1 month + Hydro -> DuckDB -> Streamlit
	uv sync
	mkdir -p data/raw
	uv run python scripts/download_generation.py --output data/raw/ --months 1
	uv run python scripts/download_price.py --output data/raw/ --months 1 || true
	uv run python scripts/download_nsp.py --output data/raw/ || true
	uv run python scripts/download_hydro.py --output data/raw/ || true
	uv run python scripts/load_local.py --db data/nzeg.duckdb --source data/raw/
	uv run python scripts/ingest_dbt_artifacts.py --init --target duckdb --db data/nzeg.duckdb
	cd dbt && uv run dbt seed --profiles-dir . --target dev \
	    && uv run dbt run --profiles-dir . --target dev
	uv run python scripts/ingest_dbt_artifacts.py \
	    --artifact dbt/target/run_results.json \
	    --target duckdb --db data/nzeg.duckdb
	cd dbt && uv run dbt run --profiles-dir . --target dev --select stg_dbt_run fct_dbt_run
	NZEG_MODE=local uv run streamlit run streamlit/app.py

# ==================== Local mode ====================
local-full:              ## Full local pipeline: complete 2016-present history + Hydro
	uv sync
	mkdir -p data/raw
	uv run python scripts/download_generation.py --output data/raw/
	uv run python scripts/download_price.py --output data/raw/ || true
	uv run python scripts/download_nsp.py --output data/raw/ || true
	uv run python scripts/download_hydro.py --output data/raw/ || true
	uv run python scripts/load_local.py --db data/nzeg.duckdb --source data/raw/
	uv run python scripts/ingest_dbt_artifacts.py --init --target duckdb --db data/nzeg.duckdb
	cd dbt && uv run dbt seed --profiles-dir . --target dev \
	    && uv run dbt run --profiles-dir . --target dev \
	    && uv run dbt test --profiles-dir . --target dev
	uv run python scripts/ingest_dbt_artifacts.py \
	    --artifact dbt/target/run_results.json \
	    --target duckdb --db data/nzeg.duckdb
	cd dbt && uv run dbt run --profiles-dir . --target dev --select stg_dbt_run fct_dbt_run
	NZEG_MODE=local uv run streamlit run streamlit/app.py

local-subset:            ## One-year data subset for medium-scale validation
	uv run python scripts/download_generation.py --output data/raw/ --years 1
	uv run python scripts/download_price.py --output data/raw/ --years 1 || true
	uv run python scripts/download_hydro.py --output data/raw/ || true
	uv run python scripts/load_local.py --db data/nzeg.duckdb --source data/raw/
	uv run python scripts/ingest_dbt_artifacts.py --init --target duckdb --db data/nzeg.duckdb
	cd dbt && uv run dbt seed --profiles-dir . --target dev \
	    && uv run dbt run --profiles-dir . --target dev
	uv run python scripts/ingest_dbt_artifacts.py \
	    --artifact dbt/target/run_results.json \
	    --target duckdb --db data/nzeg.duckdb
	cd dbt && uv run dbt run --profiles-dir . --target dev --select stg_dbt_run fct_dbt_run
	NZEG_MODE=local uv run streamlit run streamlit/app.py

market-subset:           ## Market analytics subset: 1 month of generation + price + reconciled volume
	uv run python scripts/download_generation.py --output data/raw/ --months 1
	uv run python scripts/download_price.py --output data/raw/ --months 1 || true
	uv run python scripts/download_volume.py --output data/raw/ --months 1 || true
	uv run python scripts/download_nsp.py --output data/raw/ || true
	uv run python scripts/load_local.py --db data/nzeg.duckdb --source data/raw/
	uv run python scripts/ingest_dbt_artifacts.py --init --target duckdb --db data/nzeg.duckdb
	cd dbt && uv run dbt seed --profiles-dir . --target dev \
	    && uv run dbt run --profiles-dir . --target dev \
	    && uv run dbt test --profiles-dir . --target dev

offer-sample:            ## Offers sample: latest complete trading day, large file
	uv run python scripts/download_offers.py --output data/raw/ --days 1 || true
	uv run python scripts/load_local.py --db data/nzeg.duckdb --source data/raw/
	uv run python scripts/ingest_dbt_artifacts.py --init --target duckdb --db data/nzeg.duckdb
	cd dbt && uv run dbt seed --profiles-dir . --target dev \
	    && uv run dbt run --profiles-dir . --target dev --select stg_energy_offer fct_offer_stack mart_offer_curve mart_price_offer_context mart_price_spike_features \
	    && uv run dbt test --profiles-dir . --target dev --select stg_energy_offer fct_offer_stack mart_offer_curve mart_price_offer_context test_fct_offer_stack_staging_reconciliation

dbt-test:                ## Run dbt tests only (DuckDB)
	cd dbt && uv run dbt test --profiles-dir . --target dev

# ==================== Terraform ====================
terraform-init:          ## Load .env and initialize Terraform backend
	scripts/terraform-env.sh init -reconfigure

terraform-plan:          ## Load .env and run Terraform plan
	scripts/terraform-env.sh plan

terraform-apply:         ## Load .env and apply Terraform changes
	scripts/terraform-env.sh apply

# ==================== Cloud mode (Snowflake + Airflow Docker) ====================
cloud-up:                ## Start Airflow with Docker Compose
	docker compose up -d --build

cloud-backfill:          ## Initial historical backfill: trigger monthly ingestion-only V2 DAGs from 2016-01 through last month
	docker compose exec airflow-scheduler bash -c '\
	    end_ym=$$(date -d "$$(date +%Y-%m-01) -1 day" +%Y%m); \
	    for year in $$(seq 2016 "$${end_ym:0:4}"); do \
	      for month in $$(seq -w 1 12); do \
	        ym="$$year$$month"; \
	        if [[ "$$ym" -le "$$end_ym" ]]; then \
	          airflow dags trigger nz_electricity_v2 \
	            --conf "{\"year_month\":\"$$ym\",\"skip_dbt\":true}"; \
	        fi; \
	      done; \
	    done; \
	    echo "Queued V2 ingestion-only runs through $$end_ym. After they finish, run: make cloud-dbt-full"'

cloud-offers-s3-backfill: ## Offers full S3-only backfill: trigger monthly runs from 2016-01 through yesterday's month
	docker compose exec airflow-scheduler bash -c '\
	    end_ym=$$(date -d "yesterday" +%Y%m); \
	    suffix=$$(date -u +%Y%m%d%H%M%S); \
	    triggered=0; \
	    for year in $$(seq 2016 "$${end_ym:0:4}"); do \
	      for month in $$(seq -w 1 12); do \
	        ym="$$year$$month"; \
	        if [[ "$$ym" -le "$$end_ym" ]]; then \
	          airflow dags trigger nz_offers_s3_backfill \
	            --run-id "offers_s3_$${ym}_$${suffix}" \
	            --conf "{\"year_month\":\"$$ym\"}" >/dev/null; \
	          triggered=$$((triggered + 1)); \
	        fi; \
	      done; \
	    done; \
	    echo "Queued $$triggered monthly Offers S3-only backfill runs through $$end_ym"'

cloud-volume-s3-backfill: ## Reconciled volumes full S3-only backfill: trigger monthly runs from 2016-01 through last month
	docker compose exec airflow-scheduler bash -c '\
	    end_ym=$$(date -d "$$(date +%Y-%m-01) -1 day" +%Y%m); \
	    suffix=$$(date -u +%Y%m%d%H%M%S); \
	    triggered=0; \
	    for year in $$(seq 2016 "$${end_ym:0:4}"); do \
	      for month in $$(seq -w 1 12); do \
	        ym="$$year$$month"; \
	        if [[ "$$ym" -le "$$end_ym" ]]; then \
	          airflow dags trigger nz_volume_s3_backfill \
	            --run-id "volume_s3_$${ym}_$${suffix}" \
	            --conf "{\"year_month\":\"$$ym\"}" >/dev/null; \
	          triggered=$$((triggered + 1)); \
	        fi; \
	      done; \
	    done; \
	    echo "Queued $$triggered monthly reconciled volume S3-only backfill runs through $$end_ym"'

cloud-dbt-full:          ## Full Snowflake refresh, run once after initial backfill
	bash -lc 'set -a; source .env; set +a; \
	    if [[ "$$SNOWFLAKE_PRIVATE_KEY_PATH" == "/opt/airflow/secrets/snowflake_rsa_key.p8" && -f "$$HOME/.ssh/snowflake_rsa_key.p8" ]]; then \
	      export SNOWFLAKE_PRIVATE_KEY_PATH="$$HOME/.ssh/snowflake_rsa_key.p8"; \
	    fi; \
	    cd dbt && uv run dbt seed --profiles-dir . --target prod \
	      && uv run dbt run --profiles-dir . --target prod --full-refresh \
	      && uv run dbt test --profiles-dir . --target prod'

cloud-dashboard:         ## Streamlit -> Snowflake
	bash -lc 'set -a; source .env; set +a; \
	    if [[ "$$SNOWFLAKE_PRIVATE_KEY_PATH" == "/opt/airflow/secrets/snowflake_rsa_key.p8" && -f "$$HOME/.ssh/snowflake_rsa_key.p8" ]]; then \
	      export SNOWFLAKE_PRIVATE_KEY_PATH="$$HOME/.ssh/snowflake_rsa_key.p8"; \
	    fi; \
	    NZEG_MODE=cloud uv run streamlit run streamlit/app.py'

# ==================== V1 legacy (compose lifecycle) ====================
build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose down && docker compose up -d

logs:
	docker compose logs -f airflow-scheduler airflow-webserver

backfill:
	@for year in $$(seq 2016 2026); do \
	  for month in $$(seq -w 1 12); do \
	    docker compose exec airflow-scheduler \
	      airflow dags trigger nz_electricity_monthly \
	      --conf '{"year_month":"'$$year$$month'","skip_dbt":true}'; \
	  done; \
	done
	@echo "Ingestion complete. Run 'make dbt-full' to rebuild all models."

dbt-full:
	docker compose exec airflow-scheduler bash -c \
	  "cd /opt/dbt && dbt seed && dbt run --full-refresh && dbt test"

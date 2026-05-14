.PHONY: demo local-full local-subset dbt-test cloud-up cloud-backfill cloud-dbt-full cloud-dashboard \
        build up down restart logs backfill dbt-full

# ==================== 面试演示 ====================
demo:                    ## ~90s 启动：下 1 个月数据 + Hydro → DuckDB → Streamlit
	uv sync
	mkdir -p data/raw
	uv run python scripts/download_generation.py --output data/raw/ --months 1
	uv run python scripts/download_price.py --output data/raw/ --months 1 || true
	uv run python scripts/download_nsp.py --output data/raw/ || true
	uv run python scripts/download_hydro.py --output data/raw/ || true
	uv run python scripts/load_local.py --db data/nzeg.duckdb --source data/raw/
	cd dbt && uv run dbt seed --profiles-dir . --target dev \
	    && uv run dbt run --profiles-dir . --target dev
	uv run python scripts/ingest_dbt_artifacts.py \
	    --artifact dbt/target/run_results.json \
	    --target duckdb --db data/nzeg.duckdb
	cd dbt && uv run dbt run --profiles-dir . --target dev --select stg_dbt_run fct_dbt_run
	NZEG_MODE=local uv run streamlit run streamlit/app.py

# ==================== Local 模式 ====================
local-full:              ## 一键全流程（全量历史 2016-至今 + Hydro）
	uv sync
	mkdir -p data/raw
	uv run python scripts/download_generation.py --output data/raw/
	uv run python scripts/download_price.py --output data/raw/ || true
	uv run python scripts/download_nsp.py --output data/raw/ || true
	uv run python scripts/download_hydro.py --output data/raw/ || true
	uv run python scripts/load_local.py --db data/nzeg.duckdb --source data/raw/
	cd dbt && uv run dbt seed --profiles-dir . --target dev \
	    && uv run dbt run --profiles-dir . --target dev \
	    && uv run dbt test --profiles-dir . --target dev
	uv run python scripts/ingest_dbt_artifacts.py \
	    --artifact dbt/target/run_results.json \
	    --target duckdb --db data/nzeg.duckdb
	cd dbt && uv run dbt run --profiles-dir . --target dev --select stg_dbt_run fct_dbt_run
	NZEG_MODE=local uv run streamlit run streamlit/app.py

local-subset:            ## 1 年数据子集（中等规模验证）
	uv run python scripts/download_generation.py --output data/raw/ --years 1
	uv run python scripts/download_price.py --output data/raw/ --years 1 || true
	uv run python scripts/download_hydro.py --output data/raw/ || true
	uv run python scripts/load_local.py --db data/nzeg.duckdb --source data/raw/
	cd dbt && uv run dbt seed --profiles-dir . --target dev \
	    && uv run dbt run --profiles-dir . --target dev
	uv run python scripts/ingest_dbt_artifacts.py \
	    --artifact dbt/target/run_results.json \
	    --target duckdb --db data/nzeg.duckdb
	cd dbt && uv run dbt run --profiles-dir . --target dev --select stg_dbt_run fct_dbt_run
	NZEG_MODE=local uv run streamlit run streamlit/app.py

dbt-test:                ## 单独跑 dbt test (DuckDB)
	cd dbt && uv run dbt test --profiles-dir . --target dev

# ==================== Cloud 模式（Snowflake + Airflow Docker） ====================
cloud-up:                ## 启动 Airflow (Docker Compose)
	docker compose up -d --build

cloud-backfill:          ## 触发 Airflow 回填（2016-01-01 至上月）
	docker compose exec airflow-scheduler airflow dags backfill \
	    nz_electricity_v2 \
	    --start-date 2016-01-01 \
	    --end-date $(shell date -v-1m +%Y-%m-01)

cloud-dbt-full:          ## Snowflake 全量刷新（首次回填后调用一次）
	cd dbt && uv run dbt seed --profiles-dir . --target prod \
	    && uv run dbt run --profiles-dir . --target prod --full-refresh \
	    && uv run dbt test --profiles-dir . --target prod

cloud-dashboard:         ## Streamlit → Snowflake
	NZEG_MODE=cloud uv run streamlit run streamlit/app.py

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

.PHONY: build up down restart logs backfill dbt-full

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

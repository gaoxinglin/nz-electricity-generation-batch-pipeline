FROM apache/airflow:2.9.0-python3.11

USER airflow

COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt

# dbt packages installed to /opt/dbt_build/ — NOT overridden by ./dbt volume mount
COPY --chown=airflow:root dbt/packages.yml dbt/dbt_project.yml /opt/dbt_build/
RUN cd /opt/dbt_build \
    && printf 'nz_electricity:\n  target: ci\n  outputs:\n    ci:\n      type: snowflake\n      account: ci\n      user: ci\n      password: ci\n      role: ci\n      warehouse: ci\n      database: ci\n      schema: RAW\n      threads: 1\n' > profiles.yml \
    && DBT_PROFILES_DIR=/opt/dbt_build dbt deps --project-dir /opt/dbt_build \
    && rm profiles.yml

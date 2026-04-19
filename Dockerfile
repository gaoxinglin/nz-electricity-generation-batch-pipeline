FROM apache/airflow:2.9.0-python3.11

USER root
RUN pip install uv

USER airflow

COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN uv pip install --no-cache-dir -r /tmp/requirements-airflow.txt --system

# dbt packages installed to /opt/dbt_build/ — NOT overridden by ./dbt volume mount
COPY dbt/packages.yml dbt/dbt_project.yml /opt/dbt_build/
RUN cd /opt/dbt_build && dbt deps --project-dir /opt/dbt_build

"""DAG integrity tests — validates structure without requiring Airflow runtime."""

import pytest
from airflow.models import DagBag


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="airflow/dags", include_examples=False)


@pytest.fixture(scope="module")
def dag(dagbag):
    dag = dagbag.get_dag("nz_electricity_monthly")
    assert dag is not None, "DAG 'nz_electricity_monthly' not found"
    return dag


def test_dagbag_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"


def test_dag_has_7_tasks(dag):
    assert len(dag.tasks) == 7, (
        f"Expected 7 tasks, got {len(dag.tasks)}: "
        f"{[t.task_id for t in dag.tasks]}"
    )


def test_dag_task_names(dag):
    expected_tasks = {
        "download_csv",
        "validate_csv",
        "upload_to_s3",
        "load_to_snowflake",
        "check_run_dbt",
        "run_dbt_models",
        "run_dbt_tests",
    }
    actual_tasks = {t.task_id for t in dag.tasks}
    assert actual_tasks == expected_tasks, (
        f"Task mismatch. Missing: {expected_tasks - actual_tasks}, "
        f"Extra: {actual_tasks - expected_tasks}"
    )


def test_dag_task_dependencies(dag):
    """Verify the linear dependency chain."""
    expected_chain = [
        ("download_csv", "validate_csv"),
        ("validate_csv", "upload_to_s3"),
        ("upload_to_s3", "load_to_snowflake"),
        ("load_to_snowflake", "check_run_dbt"),
        ("check_run_dbt", "run_dbt_models"),
        ("run_dbt_models", "run_dbt_tests"),
    ]
    for upstream_id, downstream_id in expected_chain:
        upstream = dag.get_task(upstream_id)
        downstream_ids = [t.task_id for t in upstream.downstream_list]
        assert downstream_id in downstream_ids, (
            f"{upstream_id} should have {downstream_id} as downstream, "
            f"but has {downstream_ids}"
        )

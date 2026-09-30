from airq.stores.airflow import dag_keys


def test_dag_keys_select_only_this_projects_files():
    """Verify that only the airq package and the airq_*.py DAG files are selected."""
    keys = [
        "dags/airq/__init__.py",
        "dags/airq/feature/daily.py",
        "dags/dags/airq_daily_pipeline.py",
        "dags/dags/other_project.py",
        "dags/other/airq_like.py",
        "dags/airq_feature_pipeline.py",
    ]
    assert dag_keys(keys) == [
        "dags/airq/__init__.py",
        "dags/airq/feature/daily.py",
        "dags/dags/airq_daily_pipeline.py",
    ]

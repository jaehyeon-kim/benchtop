from airq.stores.airflow import _DAGS_PREFIXES


def test_dag_prefixes_select_only_this_projects_files():
    """Verify that the clean-up prefixes match the airq package and the airq_*.py DAG files, and nothing else."""
    keys = [
        "dags/airq/__init__.py",
        "dags/airq/feature/load.py",
        "dags/dags/airq_feature_pipeline.py",
        "dags/dags/other_project.py",
        "dags/other/airq_like.py",
        "dags/airq_feature_pipeline.py",
    ]
    assert [k for k in keys if k.startswith(_DAGS_PREFIXES)] == [
        "dags/airq/__init__.py",
        "dags/airq/feature/load.py",
        "dags/dags/airq_feature_pipeline.py",
    ]

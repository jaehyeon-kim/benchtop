"""Removes everything this project has written, and keeps the services running.

It returns the stack to the state straight after `odctl up`, so the steps can be run
again from the start. It removes only this project's objects:

- Airflow: the DAG files in `s3://airflow/dags`, then the DAGs' records and run history.
- MLflow: the registered model, and the runs, logged models and artifact files of the
  experiment. The experiment itself stays, empty, because MLflow does not let a deleted
  experiment's name be used again.
- Feast: the project's entities and views.
- Iceberg: every table in the namespace, the namespace, and their files in SeaweedFS.

Each part is skipped when it is already gone, so running it twice is safe.

Run: python -m airq.cleanup
"""

import fnmatch
import logging
import os
import time
from collections.abc import Iterable

import boto3
import mlflow
import requests
from feast.errors import ProjectNotFoundException, ProjectObjectNotFoundException
from feast.infra.registry.sql import feature_view_version_history
from mlflow.exceptions import MlflowException
from pyiceberg.exceptions import NoSuchNamespaceError
from sqlalchemy import delete

from airq.config import (
    AIRFLOW_PASSWORD,
    AIRFLOW_URL,
    AIRFLOW_USER,
    DAG_IDS,
    EXPERIMENT,
    FEAST_PROJECT,
    MODEL_NAME,
    NAMESPACE,
)
from airq.feature_store import store
from airq.iceberg import catalog

logger = logging.getLogger("airq.cleanup")  # __name__ is "__main__" under python -m

_DAGS_BUCKET, _DAGS_PREFIX = "airflow", "dags/"
_WAREHOUSE_BUCKET = "warehouse"
_MLFLOW_BUCKET = "mlflow"
_STALE_TIMEOUT = 90  # seconds to wait for Airflow to notice the removed DAG files


def dag_keys(keys: Iterable[str]) -> list[str]:
    """
    Selects this project's files among the keys in `s3://airflow/dags`.

    The upload puts the `airq` package under `dags/airq/` and the DAG files under
    `dags/dags/`. Only the DAG files named `airq_*.py` belong to this project.

    Args:
        keys (Iterable[str]): Object keys in the `airflow` bucket.

    Returns:
        list[str]: The keys to delete.
    """
    return [
        k
        for k in keys
        if k.startswith(f"{_DAGS_PREFIX}airq/")
        or fnmatch.fnmatch(k, f"{_DAGS_PREFIX}dags/airq_*.py")
    ]


def _s3():
    """
    Returns an S3 client for SeaweedFS, with the settings `airq.config` gives PyIceberg.

    Returns:
        botocore.client.S3: The client.
    """
    return boto3.client(
        "s3",
        endpoint_url=os.environ["PYICEBERG_CATALOG__ODCTL__S3__ENDPOINT"],
        aws_access_key_id=os.environ["PYICEBERG_CATALOG__ODCTL__S3__ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ[
            "PYICEBERG_CATALOG__ODCTL__S3__SECRET_ACCESS_KEY"
        ],
        region_name=os.environ["PYICEBERG_CATALOG__ODCTL__S3__REGION"],
    )


def _keys(s3, bucket: str, prefix: str) -> list[str]:
    """
    Lists every object key under a prefix.

    Args:
        s3 (botocore.client.S3): The S3 client.
        bucket (str): The bucket.
        prefix (str): The key prefix.

    Returns:
        list[str]: The keys, across every page of the listing.
    """
    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
    return [o["Key"] for page in pages for o in page.get("Contents", [])]


def _delete(s3, bucket: str, keys: list[str]) -> int:
    """
    Deletes objects, a thousand at a time, which is the most one request takes.

    Args:
        s3 (botocore.client.S3): The S3 client.
        bucket (str): The bucket.
        keys (list[str]): The keys to delete.

    Returns:
        int: How many objects were deleted.
    """
    for i in range(0, len(keys), 1000):
        batch = [{"Key": k} for k in keys[i : i + 1000]]
        s3.delete_objects(Bucket=bucket, Delete={"Objects": batch, "Quiet": True})
    return len(keys)


def _airflow(s3) -> None:
    """
    Deletes the DAG files, waits for Airflow to mark the DAGs stale, then deletes them.

    A DAG whose file still exists would be added back, so the files go first.

    Args:
        s3 (botocore.client.S3): The S3 client.
    """
    removed = _delete(s3, _DAGS_BUCKET, dag_keys(_keys(s3, _DAGS_BUCKET, _DAGS_PREFIX)))
    logger.info(
        "Airflow: deleted %d files from s3://%s/%s", removed, _DAGS_BUCKET, _DAGS_PREFIX
    )
    token = requests.post(
        f"{AIRFLOW_URL}/auth/token",
        json={"username": AIRFLOW_USER, "password": AIRFLOW_PASSWORD},
        timeout=30,
    ).json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}
    for dag_id in DAG_IDS:
        url = f"{AIRFLOW_URL}/api/v2/dags/{dag_id}"
        deadline = time.monotonic() + _STALE_TIMEOUT
        while True:
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code == 404:
                break  # already deleted
            if response.json().get("is_stale") or time.monotonic() > deadline:
                requests.delete(url, headers=headers, timeout=30).raise_for_status()
                logger.info("Airflow: deleted %s and its runs", dag_id)
                break
            time.sleep(5)


def _all_pages(search) -> list:
    """
    Collects every page of an MLflow search.

    Args:
        search (Callable[[str | None], PagedList]): Runs the search for a page token.

    Returns:
        list: The results of every page.
    """
    results, token = [], None
    while True:
        page = search(token)
        results += list(page)
        token = page.token
        if not token:
            return results


def _mlflow(s3) -> None:
    """
    Deletes the registered model, the experiment's runs and logged models, and its files.

    Args:
        s3 (botocore.client.S3): The S3 client.
    """
    client = mlflow.MlflowClient()
    try:
        client.delete_registered_model(MODEL_NAME)
        logger.info("MLflow: deleted the registered model %s", MODEL_NAME)
    except MlflowException:
        pass  # not registered
    experiment = client.get_experiment_by_name(EXPERIMENT)
    if experiment is None:
        return
    ids = [experiment.experiment_id]
    models = _all_pages(
        lambda token: client.search_logged_models(ids, page_token=token)
    )
    for model in models:
        client.delete_logged_model(model.model_id)
    runs = _all_pages(lambda token: client.search_runs(ids, page_token=token))
    for run in runs:
        client.delete_run(run.info.run_id)
    removed = _delete(
        s3, _MLFLOW_BUCKET, _keys(s3, _MLFLOW_BUCKET, f"{experiment.experiment_id}/")
    )
    logger.info(
        "MLflow: deleted %d runs of %s and %d files from s3://%s/%s/",
        len(runs),
        EXPERIMENT,
        removed,
        _MLFLOW_BUCKET,
        experiment.experiment_id,
    )


def _feast() -> None:
    """
    Deletes the Feast project's entities and views from the registry.

    A project that is not registered is skipped, and the log counts the objects it held. The views' version
    history is deleted too.
    """
    st = store()
    views = st.list_feature_views() + st.list_on_demand_feature_views()
    entities = st.list_entities()
    try:
        st.delete_project(FEAST_PROJECT)
    except (ProjectNotFoundException, ProjectObjectNotFoundException):
        pass  # not registered
    # delete_project leaves the views' version history, and registering the same view
    # again then fails on a duplicate key.
    with st.registry.write_engine.begin() as conn:
        conn.execute(
            delete(feature_view_version_history).where(
                feature_view_version_history.c.project_id == FEAST_PROJECT
            )
        )
    logger.info(
        "Feast: deleted the project %s, with %d views and %d entities",
        FEAST_PROJECT,
        len(views),
        len(entities),
    )


def _iceberg(s3) -> None:
    """
    Drops every table in the namespace and the namespace, then deletes their files.

    Dropping a table removes it from the catalog. The files are deleted separately, so
    none are left in SeaweedFS whatever the catalog does with them.

    Args:
        s3 (botocore.client.S3): The S3 client.
    """
    cat = catalog()
    try:
        tables = cat.list_tables(NAMESPACE)
    except NoSuchNamespaceError:
        tables = []
    for identifier in tables:
        cat.drop_table(identifier)
    if tables or NAMESPACE in {n[0] for n in cat.list_namespaces()}:
        cat.drop_namespace(NAMESPACE)
    removed = _delete(
        s3, _WAREHOUSE_BUCKET, _keys(s3, _WAREHOUSE_BUCKET, f"{NAMESPACE}/")
    )
    logger.info(
        "Iceberg: dropped %d tables and %d files from s3://%s/%s/",
        len(tables),
        removed,
        _WAREHOUSE_BUCKET,
        NAMESPACE,
    )


def cleanup() -> None:
    """
    Removes this project's objects from Airflow, MLflow, Feast and Iceberg.

    Airflow goes first, so no scheduled run writes to the tables while they are removed.
    """
    s3 = _s3()
    _airflow(s3)
    _mlflow(s3)
    _feast()
    _iceberg(s3)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    cleanup()

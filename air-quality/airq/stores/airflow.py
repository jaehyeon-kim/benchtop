"""The Airflow deployment of the pipelines: the DAG files in SeaweedFS and the DAGs' records."""

import fnmatch
import logging
import time
from collections.abc import Iterable

import requests

from airq.core.config import AIRFLOW_PASSWORD, AIRFLOW_URL, AIRFLOW_USER, DAG_IDS
from airq.stores import s3 as s3_store

logger = logging.getLogger(__name__)

_DAGS_BUCKET, _DAGS_PREFIX = "airflow", "dags/"
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


def delete_dags(s3) -> None:
    """
    Deletes the DAG files, waits for Airflow to mark the DAGs stale, then deletes them.

    A DAG whose file still exists would be added back, so the files go first.

    Args:
        s3 (botocore.client.S3): The S3 client.
    """
    keys = dag_keys(s3_store.keys(s3, _DAGS_BUCKET, _DAGS_PREFIX))
    removed = s3_store.delete(s3, _DAGS_BUCKET, keys)
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

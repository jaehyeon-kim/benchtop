"""Decides which model versions are in use, through two MLflow aliases.

`champion` is the version the forecast comes from, and `challenger` is a version
predicted beside it for comparison. Training gives each new version its alias, inference
predicts with both, and promotion swaps them.

Run: python -m airq.stores.model_registry promote
"""

import argparse
import logging

import mlflow
from mlflow.entities.model_registry import ModelVersion
from mlflow.exceptions import MlflowException

from airq.core.config import CHALLENGER, CHAMPION, EXPERIMENT, MODEL_NAME
from airq.stores import s3 as s3_store

# __name__ is "__main__" under -m
logger = logging.getLogger("airq.stores.model_registry")

_MLFLOW_BUCKET = "mlflow"


def _version(alias: str, client=None) -> ModelVersion | None:
    """
    Returns the model version an alias points at.

    Args:
        alias (str): `champion` or `challenger`.
        client (MlflowClient, optional): The MLflow client. A new one is made if None.

    Returns:
        ModelVersion | None: The version, or None when the alias is not set.
    """
    try:
        return (client or mlflow.MlflowClient()).get_model_version_by_alias(
            MODEL_NAME, alias
        )
    except MlflowException:
        return None


def served() -> list[tuple[str, ModelVersion]]:
    """
    Returns the versions inference predicts with: the champion, then the challenger.

    Returns:
        list[tuple[str, ModelVersion]]: Each alias that is set, with its MLflow model
            version. Empty before any training.
    """
    pairs = [(alias, _version(alias)) for alias in (CHAMPION, CHALLENGER)]
    return [(alias, v) for alias, v in pairs if v is not None]


def role(feature_set: str) -> str:
    """
    Decides which alias a newly trained version of a feature set gets.

    The first version becomes the champion. After that, a new version of the champion's
    feature set becomes the new champion, and the other feature set is the challenger.

    Args:
        feature_set (str): `v1` or `v2`.

    Returns:
        str: `champion` or `challenger`.
    """
    champion = _version(CHAMPION)
    if champion is None or champion.tags["feature_set"] == feature_set:
        return CHAMPION
    return CHALLENGER


def promote(client=None) -> tuple[str, str]:
    """
    Swaps the champion and challenger aliases.

    Args:
        client (MlflowClient, optional): The MLflow client. A new one is made if None.

    Returns:
        tuple[str, str]: The new champion's version and the new challenger's version.

    Raises:
        SystemExit: If there is no challenger.
    """
    client = client or mlflow.MlflowClient()
    champion, challenger = _version(CHAMPION, client), _version(CHALLENGER, client)
    if champion is None or challenger is None:
        raise SystemExit("There is no challenger to promote: train v2 first.")
    client.set_registered_model_alias(MODEL_NAME, CHAMPION, challenger.version)
    client.set_registered_model_alias(MODEL_NAME, CHALLENGER, champion.version)
    return challenger.version, champion.version


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


def delete_experiment(s3) -> None:
    """
    Deletes the registered model, the experiment's runs and logged models, and its files.

    The experiment itself stays, empty, because MLflow does not let a deleted
    experiment's name be used again.

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
    prefix = f"{experiment.experiment_id}/"
    removed = s3_store.delete(
        s3, _MLFLOW_BUCKET, s3_store.keys(s3, _MLFLOW_BUCKET, prefix)
    )
    logger.info(
        "MLflow: deleted %d runs of %s and %d files from s3://%s/%s",
        len(runs),
        EXPERIMENT,
        removed,
        _MLFLOW_BUCKET,
        prefix,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="Manages the model aliases.")
    parser.add_argument("command", choices=["promote"])
    parser.parse_args()
    new_champion, new_challenger = promote()
    logger.info(
        "%s version %s is now @%s, and version %s is @%s",
        MODEL_NAME,
        new_champion,
        CHAMPION,
        new_challenger,
        CHALLENGER,
    )

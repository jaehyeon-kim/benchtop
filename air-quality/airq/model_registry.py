"""Decides which model versions are in use, through two MLflow aliases.

`champion` is the version the forecast comes from, and `challenger` is a version
predicted beside it for comparison. Training gives each new version its alias, inference
predicts with both, and promotion swaps them.

Run: python -m airq.model_registry promote
"""

import argparse
import logging

import mlflow
from mlflow.entities.model_registry import ModelVersion
from mlflow.exceptions import MlflowException

from airq.config import CHALLENGER, CHAMPION, MODEL_NAME

logger = logging.getLogger("airq.model_registry")  # __name__ is "__main__" under -m


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

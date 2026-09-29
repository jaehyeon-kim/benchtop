"""Promotes the challenger: it becomes the champion, and the champion becomes the challenger.

Run: python -m airq.training.promote
"""

import logging

import mlflow
from mlflow.exceptions import MlflowException

from airq.config import CHALLENGER, CHAMPION, MODEL_NAME

logger = logging.getLogger("airq.training.promote")


def promote(client=None) -> tuple[str, str]:
    """
    Swaps the champion and challenger aliases.

    Inference keeps predicting with both versions, so they can still be compared.

    Args:
        client (MlflowClient, optional): The MLflow client. A new one is made if None.

    Returns:
        tuple[str, str]: The new champion's version and the new challenger's version.

    Raises:
        SystemExit: If there is no challenger, or it is already the champion.
    """
    client = client or mlflow.MlflowClient()
    try:
        champion = client.get_model_version_by_alias(MODEL_NAME, CHAMPION).version
        challenger = client.get_model_version_by_alias(MODEL_NAME, CHALLENGER).version
    except MlflowException:
        raise SystemExit("There is no challenger to promote: train v2 first.") from None
    if champion == challenger:
        raise SystemExit(f"Version {champion} is already the champion.")
    client.set_registered_model_alias(MODEL_NAME, CHAMPION, challenger)
    client.set_registered_model_alias(MODEL_NAME, CHALLENGER, champion)
    return challenger, champion


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    new_champion, new_challenger = promote()
    logger.info(
        "%s version %s is now @%s, and version %s is @%s",
        MODEL_NAME,
        new_champion,
        CHAMPION,
        new_challenger,
        CHALLENGER,
    )

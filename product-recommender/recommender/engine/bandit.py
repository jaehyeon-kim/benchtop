"""LinUCB, the contextual bandit that ranks products for a user and learns from clicks.

LinUCB keeps one linear model per product. It ranks products by the predicted click
chance plus a bonus for uncertainty, so products it knows little about still get shown.
"""

import logging

import numpy as np
import pandas as pd
from mabwiser.mab import MAB, LearningPolicy

from recommender.core.config import ALPHA, TOP_K, TRAINING_LOG

logger = logging.getLogger(__name__)


def pretrained(products: list[int], schema: list[str]) -> MAB:
    """
    Returns a LinUCB model trained on the history in `training_log.csv`.

    Args:
        products (list[int]): The product ids, one arm each.
        schema (list[str]): The feature columns, in the model's order.

    Returns:
        MAB: The trained model.
    """
    model = MAB(arms=products, learning_policy=LearningPolicy.LinUCB(alpha=ALPHA))
    logger.info("Pre-training model from history...")
    history = pd.read_csv(TRAINING_LOG)
    decisions = history["product_id"].tolist()
    model.fit(
        decisions=decisions,
        rewards=history["response"].tolist(),
        contexts=history[schema],
    )
    logger.info(f"Model pre-trained on {len(decisions)} events.")
    return model


def _frame(context: dict, schema: list[str]) -> pd.DataFrame:
    """Returns the context as one row with the schema's columns, missing ones as 0."""
    row = {k: v for k, v in context.items() if k != "user_id"}
    return pd.DataFrame([row]).reindex(columns=schema, fill_value=0)


def rank(model: MAB, context: dict, schema: list[str]) -> list[int]:
    """
    Returns the `TOP_K` products with the highest LinUCB score for a context.

    Args:
        model (MAB): The model.
        context (dict): The user and time features.
        schema (list[str]): The feature columns, in the model's order.

    Returns:
        list[int]: The product ids, best first.
    """
    scores = model.predict_expectations(contexts=_frame(context, schema))
    return sorted(scores, key=scores.get, reverse=True)[:TOP_K]


def learn(
    model: MAB, context: dict, item_id: int, reward: int, schema: list[str]
) -> None:
    """
    Updates the chosen product's model with the user's reaction.

    Args:
        model (MAB): The model.
        context (dict): The user and time features.
        item_id (int): The product the user reacted to.
        reward (int): 1 for a click, 0 for none.
        schema (list[str]): The feature columns, in the model's order.
    """
    model.partial_fit(
        decisions=[item_id], rewards=[reward], contexts=_frame(context, schema)
    )


def score(a_inv: np.ndarray, b: np.ndarray, x: np.ndarray) -> float:
    """
    Returns a product's LinUCB score: x.T @ A_inv @ b + alpha * sqrt(x.T @ A_inv @ x).

    Args:
        a_inv (np.ndarray): The inverse of the product's A matrix.
        b (np.ndarray): The product's b vector.
        x (np.ndarray): The context vector.

    Returns:
        float: The predicted click chance plus the uncertainty bonus.
    """
    return x.dot(a_inv @ b) + ALPHA * np.sqrt(x.dot(a_inv).dot(x))

"""Reads the products' LinUCB models, which the Flink job keeps up to date in Valkey, and deletes them."""

import json
import logging

import numpy as np
import redis

from recommender.core.config import MODEL_KEY, VALKEY

logger = logging.getLogger(__name__)


def connect() -> redis.Redis:
    """
    Connects to odctl's Valkey.

    Returns:
        redis.Redis: The client, returning strings.
    """
    return redis.Redis(**VALKEY, decode_responses=True)


def read_models(
    client: redis.Redis, product_ids: list[str], dim: int
) -> dict[str, tuple[np.ndarray, np.ndarray]]:
    """
    Reads the models of the given products in one call.

    A product without a model yet gets the untrained one: an identity A_inv and a
    zero b.

    Args:
        client (redis.Redis): The Valkey client.
        product_ids (list[str]): The product ids.
        dim (int): The number of features, for the untrained model.

    Returns:
        dict[str, tuple[np.ndarray, np.ndarray]]: Each product's A_inv and b.
    """
    models = {}
    for pid, stored in zip(
        product_ids, client.mget([MODEL_KEY.format(p) for p in product_ids])
    ):
        if stored:
            data = json.loads(stored)
            models[pid] = (np.array(data["A_inv"]), np.array(data["b"]))
        else:
            models[pid] = (np.eye(dim), np.zeros(dim))
    return models


def delete_models() -> None:
    """Deletes every product's model, so the next Flink job starts from the history alone."""
    client = connect()
    keys = list(client.scan_iter(MODEL_KEY.format("*")))
    if keys:
        client.delete(*keys)
    logger.info(f"Valkey: deleted {len(keys)} models")

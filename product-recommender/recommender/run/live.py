"""Serves simulated visits from the models in Valkey, and sends each reaction to Kafka.

The Flink job reads the reactions, updates the models and writes them back to Valkey,
so later visits are ranked with what earlier ones taught. It runs until Ctrl+C.

Run: python -m recommender.run.live [--seed 1237]
"""

import argparse
import logging
import random
import time

import numpy as np
import pandas as pd
from faker import Faker

from recommender.core.config import SEED, TIME_COLUMNS, TOP_K, USERS, setup_logging
from recommender.core.models import FeedbackEvent
from recommender.data.features import load_artifacts, load_products
from recommender.engine.bandit import score
from recommender.engine.simulation import visit
from recommender.stores import valkey
from recommender.stores.kafka import FeedbackProducer

# __name__ is "__main__" under python -m
logger = logging.getLogger("recommender.run.live")


def run(seed: int, steps: int | None = None) -> None:
    """
    Serves visits, one every 0.1 seconds, and prints each.

    Args:
        seed (int): The random seed.
        steps (int, optional): How many visits to serve. Without it, it runs until
            Ctrl+C.
    """
    fake = Faker()
    Faker.seed(seed)
    random.seed(seed)

    users = pd.read_csv(USERS).to_dict("records")
    logger.info(f"Loaded {len(users)} users")
    client = valkey.connect()
    producer = FeedbackProducer()
    logger.info("Loading artifacts...")
    artifacts = load_artifacts()
    products = load_products()
    logger.info(f"Loaded artifacts and {len(products)} products.")
    schema = artifacts["user_columns"] + TIME_COLUMNS
    product_ids = [str(pid) for pid in products]

    def recommend(context: dict) -> list[int]:
        """Ranks every product with its latest model from Valkey."""
        x = np.array([context.get(col, 0) for col in schema])
        models = valkey.read_models(client, product_ids, len(schema))
        scores = {int(pid): score(a_inv, b, x) for pid, (a_inv, b) in models.items()}
        return sorted(scores, key=scores.__getitem__, reverse=True)[:TOP_K]

    print(
        f"\n--- STARTING EVENT-DRIVEN LOOP ({steps if steps is not None else 'infinite'} visits) ---\n"
    )
    served = 0
    while True:
        v = visit(users, products, artifacts, recommend, fake)
        producer.send(
            FeedbackEvent(
                event_id=f"evt_{int(time.time() * 1000)}",
                product_id=str(v.chosen),
                reward=v.reward,
                context_vector=[v.context.get(col, 0) for col in schema],
                timestamp=int(v.time.timestamp() * 1000),
            )
        )
        print(v)
        served += 1
        if steps is not None and served >= steps:
            break
        time.sleep(0.1)


def main() -> None:
    """Runs the loop until Ctrl+C."""
    parser = argparse.ArgumentParser(
        description="Serve visits from the models in Valkey."
    )
    parser.add_argument("--seed", type=int, default=SEED, help="Random seed.")
    args = parser.parse_args()
    setup_logging()
    try:
        run(args.seed)
    except KeyboardInterrupt:
        print("\n--- END SIMULATION ---")


if __name__ == "__main__":
    main()

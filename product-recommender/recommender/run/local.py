"""Serves simulated visits from a LinUCB model held in this process, which learns after each visit.

Run: python -m recommender.run.local [--seed 1237]
"""

import argparse
import logging
import random

import pandas as pd
from faker import Faker

from recommender.core.config import (
    LOCAL_VISITS,
    SEED,
    TIME_COLUMNS,
    USERS,
    setup_logging,
)
from recommender.data.features import load_artifacts, load_products
from recommender.engine import bandit
from recommender.engine.simulation import visit

# __name__ is "__main__" under python -m
logger = logging.getLogger("recommender.run.local")


def main() -> None:
    """Pre-trains the model on the history, then prints each visit it serves."""
    parser = argparse.ArgumentParser(
        description="Serve visits from an in-process model."
    )
    parser.add_argument("--seed", type=int, default=SEED, help="Random seed.")
    args = parser.parse_args()
    setup_logging()
    fake = Faker()
    Faker.seed(args.seed)
    random.seed(args.seed)

    users = pd.read_csv(USERS).to_dict("records")
    logger.info(f"Loaded {len(users)} users")
    logger.info("Loading artifacts...")
    artifacts = load_artifacts()
    products = load_products()
    logger.info(f"Loaded {len(products)} products.")
    schema = artifacts["user_columns"] + TIME_COLUMNS
    model = bandit.pretrained(list(products), schema)

    print(f"\n--- STARTING LIVE LOOP ({LOCAL_VISITS} visits) ---\n")
    for _ in range(LOCAL_VISITS):
        v = visit(
            users,
            products,
            artifacts,
            lambda ctx: bandit.rank(model, ctx, schema),
            fake,
        )
        bandit.learn(model, v.context, v.chosen, v.reward, schema)
        print(v)
    print("\n--- END LOOP ---")


if __name__ == "__main__":
    main()

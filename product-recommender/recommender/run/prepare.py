"""Generates the users, their features and the click history the models learn from.

Run: python -m recommender.run.prepare [--seed 1237]
"""

import argparse
import logging

from faker import Faker

from recommender.core.config import SEED, setup_logging
from recommender.data.features import build_features
from recommender.data.history import generate_history
from recommender.data.users import generate_users

# __name__ is "__main__" under python -m
logger = logging.getLogger("recommender.run.prepare")


def main() -> None:
    """Writes every file in `data/` that the other steps read."""
    parser = argparse.ArgumentParser(
        description="Generate the users, features and history."
    )
    parser.add_argument("--seed", type=int, default=SEED, help="Random seed.")
    args = parser.parse_args()
    setup_logging()
    Faker.seed(args.seed)
    fake = Faker()
    generate_users(fake)
    build_features()
    generate_history(fake)
    logger.info("Data Preparation Complete.")


if __name__ == "__main__":
    main()

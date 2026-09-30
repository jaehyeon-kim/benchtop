"""Builds the users, features and transforms once, in a temporary folder."""

import pytest
from faker import Faker

from recommender.data import features, users


@pytest.fixture(scope="session")
def built(tmp_path_factory):
    """Runs the users and features steps with seed 1237, writing to a temporary folder."""
    out = tmp_path_factory.mktemp("data")
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(users, "USERS", out / "users.csv")
        for name in ["USERS", "USER_FEATURES", "PRODUCT_FEATURES", "ARTIFACTS"]:
            mp.setattr(features, name, out / getattr(features, name).name)
        Faker.seed(1237)
        users.generate_users(Faker())
        features.build_features()
        yield out, features.load_artifacts()

import pandas as pd

from recommender.core.models import User
from recommender.data.features import user_features


def test_features_are_one_hot_and_scaled(built):
    """Verify that the user features are one-hot categories and ages scaled from 0 to 1."""
    out, _ = built
    df = pd.read_csv(out / "user_features.csv")
    assert df.shape == (1000, 11)
    assert df["age"].min() == 0 and df["age"].max() == 1
    assert set(df["gender_M"]) <= {0, 1}
    products = pd.read_csv(out / "product_features.csv")
    assert products.shape == (200, 21)
    assert (
        products.loc[products["product_id"].between(189, 194), "is_coffee"].eq(1).all()
    )


def test_a_new_user_gets_the_training_columns(built):
    """Verify that a user with an unseen traffic source gets every training column, as 0."""
    _, artifacts = built
    user = User(1, "A", "B", "a@b.c", 16, "F", "1 St", "3000", "Melbourne", "Victoria", "Australia", -37.8, 145.0, "Unseen")  # fmt: skip
    feats = user_features(user, artifacts)
    assert list(feats) == ["user_id", *artifacts["user_columns"]]
    assert feats["gender_F"] == 1 and feats["gender_M"] == 0
    assert all(
        feats[c] == 0
        for c in artifacts["user_columns"]
        if c.startswith("traffic_source_")
    )

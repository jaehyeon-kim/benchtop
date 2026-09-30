import numpy as np
from mabwiser.mab import MAB, LearningPolicy

from recommender.engine.bandit import learn, rank, score

SCHEMA = ["a", "b"]


def test_score_is_the_mean_plus_the_confidence_bound():
    """Verify the LinUCB score: x.theta plus alpha times the square root of x.A_inv.x."""
    assert score(np.eye(2), np.array([1.0, 0.0]), np.array([1.0, 0.0])) == 2.0


def test_more_uncertainty_gives_a_higher_score():
    """Verify that the same mean with a larger A_inv scores higher, which drives exploration."""
    x, b = np.array([1.0, 0.0]), np.zeros(2)
    assert score(4 * np.eye(2), b, x) > score(np.eye(2), b, x)


def test_learning_from_clicks_moves_a_product_up():
    """Verify that clicks on one product in a context rank it first there."""
    model = MAB(arms=[1, 2, 3], learning_policy=LearningPolicy.LinUCB(alpha=1.0))
    model.fit(decisions=[1, 2, 3], rewards=[0, 0, 0], contexts=[[0, 1]] * 3)
    context = {"user_id": 9, "a": 1, "b": 0}
    for _ in range(5):
        learn(model, context, 3, 1, SCHEMA)
    assert rank(model, context, SCHEMA)[0] == 3

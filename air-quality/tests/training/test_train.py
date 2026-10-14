import pytest

from airq.stores.feature_store import V1_COLUMNS
from airq.training.train import fit_and_score, scores
from tests.conftest import daily_frame


def test_split_keeps_time_order_and_puts_the_last_days_in_test():
    """Verify that the split sorts by day and puts the last 20% of the days in the test set."""
    frame = daily_frame(31)
    shuffled = frame.sample(frac=1, random_state=0)
    _, train, test, _ = fit_and_score(shuffled, V1_COLUMNS)
    assert len(train) == 24 and len(test) == 6
    assert train["event_timestamp"].max() < test["event_timestamp"].min()
    assert list(test["event_timestamp"]) == sorted(frame["event_timestamp"])[-6:]


def test_scores_on_a_known_error():
    """Verify MAE, RMSE and R² on predictions that are all off by one."""
    result = scores([1.0, 2.0, 3.0, 4.0], [2.0, 3.0, 4.0, 5.0])
    assert result["mae"] == pytest.approx(1.0)
    assert result["rmse"] == pytest.approx(1.0)
    assert result["r2"] == pytest.approx(0.2)

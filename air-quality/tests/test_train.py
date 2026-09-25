import pandas as pd
import pytest

from airq.train import scores, split


def test_split_keeps_time_order_and_puts_the_last_days_in_test():
    days = pd.date_range("2026-01-01", periods=10, tz="UTC")
    frame = pd.DataFrame({"event_timestamp": days[::-1], "pm2_5": range(10)})
    train, test = split(frame, test_fraction=0.2)
    assert len(train) == 8 and len(test) == 2
    assert train["event_timestamp"].max() < test["event_timestamp"].min()
    assert list(test["event_timestamp"]) == list(days[-2:])


def test_scores_on_a_known_error():
    result = scores([1.0, 2.0, 3.0, 4.0], [2.0, 3.0, 4.0, 5.0])
    assert result["mae"] == pytest.approx(1.0)
    assert result["rmse"] == pytest.approx(1.0)
    assert result["r2"] == pytest.approx(0.2)

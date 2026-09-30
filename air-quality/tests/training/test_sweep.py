from datetime import date

import pandas as pd

from airq.training.sweep import add_lags, choose


def test_choose_stops_when_the_next_candidate_gains_little():
    """Verify that the sweep stops when the next candidate lowers the error by less than 5%."""
    assert choose({"a": 2.0, "b": 0.7, "c": 0.69}) == "b"


def test_choose_takes_the_largest_when_every_step_gains():
    """Verify that the sweep takes the largest set when every candidate lowers the error enough."""
    assert choose({"a": 2.0, "b": 1.0, "c": 0.5}) == "c"


def test_add_lags_reads_the_reading_k_days_before():
    """Verify that each lag reads the reading k days before, and rows without one are dropped."""
    daily = pd.Series([1.0, 2.0, 3.0, 4.0], index=[date(2026, 9, d) for d in (20, 21, 22, 23)])  # fmt: skip
    frame = pd.DataFrame({"day": [date(2026, 9, 22), date(2026, 9, 23)]})
    lagged = add_lags(frame, daily, [1, 3])
    assert list(lagged["day"]) == [
        date(2026, 9, 23)
    ]  # 22 Sep has no reading 3 days before
    assert lagged.iloc[0]["lag1"] == 3.0 and lagged.iloc[0]["lag3"] == 1.0

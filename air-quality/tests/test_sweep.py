from datetime import date

import pandas as pd

from airq.sweep import CANDIDATES, add_lags, choose, generated_frame
from tests.conftest import ORIGIN


def test_choose_stops_when_the_next_candidate_gains_little():
    assert choose({"a": 2.0, "b": 0.7, "c": 0.69}) == "b"


def test_choose_takes_the_largest_when_every_step_gains():
    assert choose({"a": 2.0, "b": 1.0, "c": 0.5}) == "c"


def test_add_lags_reads_the_reading_k_days_before():
    daily = pd.Series([1.0, 2.0, 3.0, 4.0], index=[date(2026, 9, d) for d in (20, 21, 22, 23)])  # fmt: skip
    frame = pd.DataFrame({"day": [date(2026, 9, 22), date(2026, 9, 23)]})
    lagged = add_lags(frame, daily, [1, 3])
    assert list(lagged["day"]) == [
        date(2026, 9, 23)
    ]  # 22 Sep has no reading 3 days before
    assert lagged.iloc[0]["lag1"] == 3.0 and lagged.iloc[0]["lag3"] == 1.0


def test_generated_frame_has_every_candidate_column_once_lagged():
    frame, daily = generated_frame(ORIGIN, n_days=10, seed=1)
    lagged = add_lags(frame, daily, [1, 2, 3])
    columns = {c for cols in CANDIDATES.values() for c in cols}
    assert columns <= set(lagged.columns)
    assert len(frame) == 9  # the first day has no day before it
    assert len(lagged) == 6  # and lag 3 needs three days before

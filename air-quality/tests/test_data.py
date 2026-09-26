from sklearn.metrics import mean_absolute_error, mean_squared_error
from xgboost import XGBRegressor

from airq.config import DEFAULT_SEED
from airq.feast_repo import FEATURE_SETS
from airq.sweep import generated_frame
from tests.conftest import ORIGIN


def test_same_seed_gives_the_same_rows(simulate):
    assert simulate(days=3, seed=7) == simulate(days=3, seed=7)
    assert simulate(days=3, seed=7) != simulate(days=3, seed=8)


def test_v2_beats_v1_beats_predicting_yesterday():
    """The ordering the project rests on, with the book's XGBRegressor() defaults,
    on the registered feature sets: v1 is the lead-1 weather, v2 adds the weekend
    flag. The features come from the feature pipeline's own code."""
    frame, _ = generated_frame(ORIGIN, n_days=720, seed=DEFAULT_SEED)
    frame = frame.sort_values("day", ignore_index=True)
    v1_columns, v2_columns = FEATURE_SETS["v1"][1], FEATURE_SETS["v2"][1]
    target, yesterday = frame["pm2_5"], frame["pm2_5_lag1"]

    for cut in (len(frame) - 30, int(len(frame) * 0.8)):
        train, test = frame.iloc[:cut], frame.iloc[cut:]
        v1 = (
            XGBRegressor()
            .fit(train[v1_columns], train["pm2_5"])
            .predict(test[v1_columns])
        )
        v2 = (
            XGBRegressor()
            .fit(train[v2_columns], train["pm2_5"])
            .predict(test[v2_columns])
        )
        for metric in (mean_absolute_error, mean_squared_error):
            e1 = metric(target[cut:], v1)
            e2 = metric(target[cut:], v2)
            baseline = metric(target[cut:], yesterday[cut:])
            assert e2 < e1 < baseline, (metric.__name__, cut, e2, e1, baseline)


def test_rows_out_of_range_are_refused():
    """The book's expectation: PM2.5 between 0 and 500."""
    import pytest
    from pydantic import ValidationError

    from airq.models import Observation

    now = ORIGIN
    with pytest.raises(ValidationError):
        Observation(
            location_id="station-1", measured_at=now, pm2_5=-1.0, ingested_at=now
        )
    with pytest.raises(ValidationError):
        Observation(
            location_id="station-1", measured_at=now, pm2_5=501.0, ingested_at=now
        )

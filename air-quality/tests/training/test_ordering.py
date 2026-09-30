"""The ordering the project rests on: v2 beats v1, which beats the baseline."""

from sklearn.metrics import mean_absolute_error, mean_squared_error
from xgboost import XGBRegressor

from airq.feature_store import FEATURE_SETS
from tests.conftest import daily_frame


def test_v2_beats_v1_beats_predicting_yesterday():
    """Verify that v2 beats v1, and v1 beats predicting yesterday's value, on generated data."""
    frame = daily_frame(n_days=720)
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

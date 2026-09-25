from collections import defaultdict

import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error
from xgboost import XGBRegressor

from airq.models import Observation


def test_same_seed_gives_the_same_rows(simulate):
    assert simulate(days=3, seed=7) == simulate(days=3, seed=7)
    assert simulate(days=3, seed=7) != simulate(days=3, seed=8)


def test_v2_beats_v1_beats_predicting_yesterday(simulate):
    """The ordering the project rests on, with the book's XGBRegressor() defaults.

    v1: daily mean temperature, mean wind speed and wet hours from the lead-1
    forecast. v2: v1 plus a weekend flag and yesterday's PM2.5.
    """
    pm, weather = defaultdict(list), defaultdict(list)
    for row in simulate(days=720):
        if isinstance(row, Observation):
            pm[row.measured_at.date()].append(row.pm2_5)
        elif row.lead_days == 1:
            weather[row.forecast_for.date()].append(row)
    days = sorted(d for d in pm if len(weather[d]) == 24)
    w = np.array(
        [
            [
                np.mean([r.temperature_2m for r in weather[d]]),
                np.mean([r.wind_speed_10m for r in weather[d]]),
                sum(r.precipitation > 0 for r in weather[d]),
            ]
            for d in days
        ]
    )
    weekend = np.array([d.weekday() >= 5 for d in days], dtype=float)
    y = np.array([np.mean(pm[d]) for d in days])
    x1, x2 = w[1:], np.column_stack([w[1:], weekend[1:], y[:-1]])
    target, yesterday = y[1:], y[:-1]

    for cut in (len(target) - 30, int(len(target) * 0.8)):
        v1 = XGBRegressor().fit(x1[:cut], target[:cut]).predict(x1[cut:])
        v2 = XGBRegressor().fit(x2[:cut], target[:cut]).predict(x2[cut:])
        for metric in (mean_absolute_error, mean_squared_error):
            e1 = metric(target[cut:], v1)
            e2 = metric(target[cut:], v2)
            baseline = metric(target[cut:], yesterday[cut:])
            assert e2 < e1 < baseline, (metric.__name__, cut, e2, e1, baseline)

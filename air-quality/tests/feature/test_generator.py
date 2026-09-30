from collections import defaultdict
from datetime import timedelta

from airq.core.config import LEADS
from airq.core.models import Observation, WeatherForecast


def test_each_hour_issues_one_forecast_per_lead(simulate):
    """Verify that each hour issues seven forecasts, each for the hour `lead_days` later."""
    forecasts = [r for r in simulate(days=3) if isinstance(r, WeatherForecast)]
    by_issue = defaultdict(list)
    for row in forecasts:
        by_issue[row.issued_at].append(row)
    assert len(by_issue) == 3 * 24
    for rows in by_issue.values():
        assert sorted(r.lead_days for r in rows) == list(LEADS)
        for row in rows:
            assert row.forecast_for - row.issued_at == timedelta(days=row.lead_days)
            assert row.precipitation >= 0 and row.wind_speed_10m >= 0
            assert 0 <= row.wind_direction_10m < 360


def test_observations_are_hourly(simulate):
    """Verify that readings are on the hour and never negative."""
    observations = [r for r in simulate(days=3) if isinstance(r, Observation)]
    assert observations, "no observations in three days"
    for row in observations:
        assert row.measured_at.minute == 0 and row.pm2_5 >= 0


def test_same_seed_gives_the_same_rows(simulate):
    """Verify that the same seed gives the same rows and a different seed does not."""
    assert simulate(days=3, seed=7) == simulate(days=3, seed=7)
    assert simulate(days=3, seed=7) != simulate(days=3, seed=8)

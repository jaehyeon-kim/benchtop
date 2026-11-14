from collections import defaultdict
from datetime import timedelta

from airq.core.models import DailyAirQuality, DailyWeather, Observation, WeatherForecast
from tests.conftest import ORIGIN, run_rows


def _counts(rows: list) -> list[int]:
    """The number of rows of each table, in the order forecasts, readings, daily weather, daily air quality."""
    kinds = (WeatherForecast, Observation, DailyWeather, DailyAirQuality)
    return [sum(isinstance(r, kind) for r in rows) for kind in kinds]


def test_daily_run_writes_what_the_backfill_writes():
    """Verify that the daily run writes the same rows for a day as the backfill does."""
    backfill = run_rows(6)
    daily = run_rows(1, start=ORIGIN + timedelta(days=5))
    assert set(map(repr, daily)) <= set(map(repr, backfill))
    assert _counts(daily) == [7 * 24, 24, 7, 1]


def test_the_first_day_has_no_air_quality_row():
    """Verify that the first simulated day writes no air quality row, because it has no day before."""
    assert _counts(run_rows(1)) == [7 * 24, 24, 7, 0]


def test_daily_rows_average_the_hourly_rows():
    """Verify that each daily row averages its day's 24 hourly rows, and holds the day before's mean."""
    rows = run_rows(6)
    forecasts = defaultdict(list)
    for r in rows:
        if isinstance(r, WeatherForecast):
            forecasts[r.forecast_for.date(), r.lead_days].append(r)
    weather = [r for r in rows if isinstance(r, DailyWeather)]
    assert len(weather) == 6 * 7
    for row in weather:
        hours = forecasts[row.day, row.lead_days]
        assert len(hours) == 24 and row.issued_on == row.day - timedelta(
            days=row.lead_days
        )
        assert row.temperature_2m == round(sum(h.temperature_2m for h in hours) / 24, 2)
        assert row.wind_speed_10m == round(sum(h.wind_speed_10m for h in hours) / 24, 2)
        assert row.wet_hours == sum(h.precipitation > 0 for h in hours)

    readings = defaultdict(list)
    for r in rows:
        if isinstance(r, Observation):
            readings[r.measured_at.date()].append(r.pm2_5)
    mean = {day: sum(v) / len(v) for day, v in readings.items()}
    air_quality = [r for r in rows if isinstance(r, DailyAirQuality)]
    assert [r.day for r in air_quality] == sorted(mean)[1:]
    for row in air_quality:
        assert row.pm2_5 == round(mean[row.day], 2)
        assert row.pm2_5_lag1 == round(mean[row.day - timedelta(days=1)], 2)
        assert row.is_weekend == (row.day.weekday() >= 5)

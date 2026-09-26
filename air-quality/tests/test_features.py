from datetime import UTC, datetime, time, timedelta

from airq.config import DEFAULT_SEED, STATION
from airq.daily import day_rows
from airq.features import daily_features, in_window
from airq.models import DailyAirQuality, DailyWeather, Observation, WeatherForecast
from tests.conftest import ORIGIN


def test_daily_run_writes_what_the_backfill_writes(simulate):
    """The daily run's rows for a day equal the feature code's rows for that day
    over the whole generated span, which the backfill writes. The backfill's
    Iceberg writing itself is not run here."""
    rows = simulate(days=6)
    forecasts = [r for r in rows if isinstance(r, WeatherForecast)]
    observations = [r for r in rows if isinstance(r, Observation)]
    weather, air_quality = daily_features(forecasts, observations)
    day = (ORIGIN + timedelta(days=5)).date()
    start = datetime.combine(day, time(), UTC)
    end = start + timedelta(days=1)
    backfill = {
        WeatherForecast: forecasts,
        Observation: observations,
        DailyWeather: weather,
        DailyAirQuality: air_quality,
    }
    expected = {
        m: [r for r in v if in_window(r, start, end)] for m, v in backfill.items()
    }
    assert day_rows(day, ORIGIN, DEFAULT_SEED) == expected
    assert [len(v) for v in expected.values()] == [7 * 24, 24, 7, 1]


def test_daily_features_average_complete_days():
    day = datetime(2026, 9, 5, tzinfo=UTC)  # a Saturday
    observations = [
        Observation(location_id=STATION, measured_at=day + timedelta(hours=h),
                    pm2_5=10.0 if h < 0 else 4.0, ingested_at=day)
        for h in range(-24, 24)
    ]  # fmt: skip
    forecasts = [
        WeatherForecast(
            location_id=STATION, forecast_for=day + timedelta(hours=h), issued_at=day - timedelta(days=1),
            lead_days=1, temperature_2m=float(h), precipitation=0.5 if h < 3 else 0.0,
            wind_speed_10m=10.0, wind_direction_10m=0.0, ingested_at=day,
        )
        for h in range(24)
    ]  # fmt: skip
    weather, air_quality = daily_features(forecasts, observations[:-1])
    assert (
        air_quality == []
    )  # the Saturday lacks an hour, the Friday lacks its day before
    weather, air_quality = daily_features(forecasts, observations)
    assert weather == [
        DailyWeather(location_id=STATION, day=day.date(), lead_days=1, issued_on=(day - timedelta(days=1)).date(),
                     temperature_2m=11.5, wind_speed_10m=10.0, wet_hours=3)
    ]  # fmt: skip
    assert air_quality == [
        DailyAirQuality(location_id=STATION, day=day.date(), pm2_5=4.0, is_weekend=True, pm2_5_lag1=10.0)
    ]  # fmt: skip

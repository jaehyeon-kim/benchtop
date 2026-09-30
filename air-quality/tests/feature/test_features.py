from datetime import UTC, datetime, timedelta

from airq.core.config import DEFAULT_SEED, STATION
from airq.core.models import DailyAirQuality, DailyWeather, Observation, WeatherForecast
from airq.feature.features import daily_features, in_window
from airq.feature.generator import generate
from airq.feature.load import advance
from airq.feature.simulation import HourlyPublisher
from tests.conftest import ORIGIN


def test_daily_run_writes_what_the_backfill_writes():
    """Verify that the daily run publishes the same rows for a day as the backfill does."""
    start = ORIGIN + timedelta(days=5)

    backfill_records, backfill = generate(ORIGIN, DEFAULT_SEED), HourlyPublisher()
    published = [
        r for _ in range(6 * 24) for r in backfill.step(next(backfill_records))
    ]
    expected = [r for r in published if in_window(r, start, start + timedelta(days=1))]

    records = generate(ORIGIN, DEFAULT_SEED)
    daily = HourlyPublisher(advance(records, start))
    actual = [r for _ in range(24) for r in daily.step(next(records))]

    assert actual == expected
    counts = {m: sum(isinstance(r, m) for r in actual) for m in (WeatherForecast, Observation, DailyWeather, DailyAirQuality)}  # fmt: skip
    assert list(counts.values()) == [7 * 24, 24, 7, 1]


def test_the_first_day_has_no_air_quality_row():
    """Verify that the first simulated day publishes no air quality row, because it has no day before."""
    records, publisher = generate(ORIGIN, DEFAULT_SEED), HourlyPublisher()
    rows = [r for _ in range(24) for r in publisher.step(next(records))]
    assert not any(isinstance(r, DailyAirQuality) for r in rows)
    assert sum(isinstance(r, DailyWeather) for r in rows) == 7


def test_daily_features_average_complete_days():
    """Verify that the daily features average a complete day and use the day before for the previous day's mean."""
    day = datetime(2026, 9, 5, tzinfo=UTC)  # a Saturday
    observations = [
        Observation(location_id=STATION, measured_at=day + timedelta(hours=h),
                    pm2_5=10.0 if h < 0 else 4.0)
        for h in range(-24, 24)
    ]  # fmt: skip
    forecasts = [
        WeatherForecast(
            location_id=STATION, forecast_for=day + timedelta(hours=h), issued_at=day - timedelta(days=1),
            lead_days=1, temperature_2m=float(h), precipitation=0.5 if h < 3 else 0.0,
            wind_speed_10m=10.0, wind_direction_10m=0.0,
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

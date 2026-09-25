"""Daily features: the hourly rows turned into the daily tables Feast reads.

DailyWeather averages the 24 hourly forecasts for a day issued `lead_days`
before it. DailyAirQuality averages the day's PM2.5 and adds the weekend flag and
the previous day's mean.

The backfill and the daily run both call `daily_features` and `in_window`, so
the two write the same rows for the same day.
"""

from collections import defaultdict
from datetime import datetime, timedelta

import numpy as np
from pydantic import BaseModel

from airq.config import STATION
from airq.models import DailyAirQuality, DailyWeather, Observation, WeatherForecast

# The column that places each row on a day, for choosing and replacing a day's rows.
DAY_COLUMN = {
    WeatherForecast: "issued_at",
    Observation: "measured_at",
    DailyWeather: "issued_on",
    DailyAirQuality: "day",
}


def in_window(row: BaseModel, start: datetime, end: datetime) -> bool:
    """Whether the row's day column falls in [start, end)."""
    value = getattr(row, DAY_COLUMN[type(row)])
    if isinstance(value, datetime):
        return start <= value < end
    return start.date() <= value < end.date()


def daily_features(
    forecasts: list[WeatherForecast], observations: list[Observation]
) -> tuple[list[DailyWeather], list[DailyAirQuality]]:
    """Daily rows for every day with all 24 hours; a day also needs the day before for its lag."""
    weather = defaultdict(list)
    for row in forecasts:
        weather[row.forecast_for.date(), row.lead_days].append(row)
    daily_weather = [
        DailyWeather(
            location_id=STATION,
            day=day,
            lead_days=lead,
            issued_on=day - timedelta(days=lead),
            temperature_2m=round(float(np.mean([r.temperature_2m for r in rows])), 2),
            wind_speed_10m=round(float(np.mean([r.wind_speed_10m for r in rows])), 2),
            wet_hours=sum(r.precipitation > 0 for r in rows),
        )
        for (day, lead), rows in sorted(weather.items())
        if len(rows) == 24
    ]
    pm = defaultdict(list)
    for row in observations:
        pm[row.measured_at.date()].append(row.pm2_5)
    mean = {day: float(np.mean(v)) for day, v in pm.items() if len(v) == 24}
    daily_air_quality = [
        DailyAirQuality(
            location_id=STATION,
            day=day,
            pm2_5=round(mean[day], 2),
            is_weekend=day.weekday() >= 5,
            pm2_5_lag1=round(mean[day - timedelta(days=1)], 2),
        )
        for day in sorted(mean)
        if day - timedelta(days=1) in mean
    ]
    return daily_weather, daily_air_quality

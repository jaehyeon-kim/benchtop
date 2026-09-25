"""Data generation: the values that fill the hourly records.

The values only need to look real. Each hour's weather carries over part of the
last hour's, follows daily and yearly cycles, and a forecast is the true value
plus an error that grows with the lead time.

`generate` produces every hour's records from an origin with a seed. The same
origin and seed give the same values, which is how the backfill and the daily run
write the same values for the same hour.
"""

import math
from collections.abc import Iterator
from datetime import datetime, timedelta

import numpy as np
from numpy.random import Generator
from pydantic import BaseModel

from airq.config import LEADS, STATION
from airq.models import Observation, WeatherForecast

# Temperature and wind are deviations from their cycles.
_CALM = {"temperature": 0.0, "wind": 0.0, "wet": False, "rain": 0.0, "direction": 180.0}  # fmt: skip


def wave(t: datetime, period_hours: float, peak_hour: float) -> float:
    """A cosine between -1 and 1 that peaks `peak_hour` hours into each period."""
    return math.cos(2 * math.pi * (t.timestamp() / 3600 - peak_hour) / period_hours)


def actual(hour: datetime, truth: dict) -> tuple[float, float]:
    """Temperature and wind speed in `hour`: the cycles plus the true deviations."""
    temperature = 18 + 5 * wave(hour, 8766, 15 * 24) + 3 * wave(hour, 24, 4.5)
    wind = max(10 + 2.4 * wave(hour, 24, 5.5) + truth["wind"], 0.0)
    return temperature + truth["temperature"], wind


def next_hour(rng: Generator, last: dict) -> dict:
    """The true state of the next hour, carrying over part of the last one."""
    wet = bool(rng.random() < (0.78 if last["wet"] else 0.06))
    return {
        "temperature": 0.96 * last["temperature"] + rng.normal(0, 0.8),
        "wind": 0.9 * last["wind"] + rng.normal(0, 1.8),
        "wet": wet,
        "rain": float(rng.exponential(0.6)) if wet else 0.0,
        "direction": (last["direction"] + rng.normal(0, 27)) % 360,
    }


def weather_forecast(
    rng: Generator, issued: datetime, lead: int, truth: dict
) -> WeatherForecast:
    """The forecast issued at `issued` for the hour `lead` days later."""
    hour = issued + timedelta(days=lead)
    temperature, wind = actual(hour, truth)
    return WeatherForecast(
        location_id=STATION,
        forecast_for=hour,
        issued_at=issued,
        lead_days=lead,
        temperature_2m=round(temperature + rng.normal(0, 1 + 0.2 * lead), 1),
        precipitation=round(truth["rain"] * math.exp(rng.normal(0, 0.3 + 0.1 * lead)), 1),
        wind_speed_10m=round(wind * math.exp(rng.normal(0, 0.1 + 0.03 * lead)), 1),
        wind_direction_10m=round(truth["direction"] + rng.normal(0, 35 + 5 * lead)) % 360,
        ingested_at=issued,
    )  # fmt: skip


def observation(rng: Generator, hour: datetime, truth: dict) -> Observation:
    """PM2.5 measured in `hour`: higher on weekdays and in the evening, lower when windy or wet.

    Weather changes from day to day and the forecast sees it, so a weather model
    beats predicting yesterday. The weekday traffic effect is invisible to weather
    and wrong for yesterday on Saturdays and Mondays, so a model that also knows
    the weekend beats both.
    """
    _, wind = actual(hour, truth)
    log_pm = (2.05 + 0.4 * (hour.weekday() < 5) + 0.12 * wave(hour, 24, 17) - 0.1 * (wind - 10)
              - 0.4 * truth["wet"] + rng.normal(0, 0.1))  # fmt: skip
    pm = math.expm1(log_pm)
    return Observation(
        location_id=STATION,
        measured_at=hour,
        pm2_5=round(max(pm, 0.0), 2),
        ingested_at=hour + timedelta(hours=1),  # sent when the hour is complete
    )


def generate(origin: datetime, seed: int) -> Iterator[list[BaseModel]]:
    """Each hour's records from `origin`, in order: its seven forecasts, then its reading."""
    rng = np.random.default_rng(seed)
    hours = [
        _CALM
    ]  # true state per hour since origin, a week ahead of the current hour
    k = 0
    while True:
        now = origin + timedelta(hours=k)
        while len(hours) <= k + 24 * len(LEADS):
            hours.append(next_hour(rng, hours[-1]))
        forecasts = [
            weather_forecast(rng, now, lead, hours[k + 24 * lead]) for lead in LEADS
        ]
        yield [*forecasts, observation(rng, now, hours[k])]
        k += 1

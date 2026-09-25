"""The four Iceberg tables' rows, one pydantic model each. The Iceberg schemas are
derived from these models, and the generator and the feature code build them.

- Observation: the PM2.5 measured in an hour.
- WeatherForecast: the weather forecast for an hour, made 1 to 7 days before it.
  `forecast_for` is the hour being forecast, `issued_at` is when it was made.
- DailyWeather: one day's forecast weather at one lead.
- DailyAirQuality: one day's mean PM2.5, the target, with its daily features.
- Prediction: one day's predicted PM2.5, from a run for an as-of date.
"""

from datetime import date

from pydantic import AwareDatetime, BaseModel


class Observation(BaseModel):
    """One hour of measured PM2.5 at one station."""

    location_id: str
    measured_at: AwareDatetime
    pm2_5: float
    ingested_at: AwareDatetime


class WeatherForecast(BaseModel):
    """Weather forecast for one hour, as issued `lead_days` before that hour."""

    location_id: str
    forecast_for: AwareDatetime
    issued_at: AwareDatetime
    lead_days: int
    temperature_2m: float
    precipitation: float
    wind_speed_10m: float
    wind_direction_10m: float
    ingested_at: AwareDatetime


class DailyWeather(BaseModel):
    """Forecast weather for one day, as issued `lead_days` before it."""

    location_id: str
    day: date
    lead_days: int
    issued_on: date
    temperature_2m: float
    wind_speed_10m: float
    wet_hours: int


class DailyAirQuality(BaseModel):
    """Measured PM2.5 for one day, with the features known from the date and the day before."""

    location_id: str
    day: date
    pm2_5: float
    is_weekend: bool
    pm2_5_lag1: float


class Prediction(BaseModel):
    """Predicted PM2.5 for one day, made by a run standing on `as_of`."""

    location_id: str
    as_of: date
    day: date
    lead_days: int
    pm2_5: float
    model_version: str

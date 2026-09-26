"""The four Iceberg tables' rows, one pydantic model each. The Iceberg schemas are
derived from these models, and the generator and the feature code build them.

- Observation: the PM2.5 measured in an hour.
- WeatherForecast: the weather forecast for an hour, made 1 to 7 days before it.
  `forecast_for` is the hour being forecast, `issued_at` is when it was made.
- DailyWeather: one day's forecast weather at one lead.
- DailyAirQuality: one day's mean PM2.5, the target, with its daily features.
- Prediction: one day's predicted PM2.5, from a run for an as-of date.

Value bounds validate every row as it is built, before anything is written, in
place of the book's Great Expectations suites: PM2.5 between 0 and 500 as in the
book, and physically possible weather.
"""

from datetime import date

from pydantic import AwareDatetime, BaseModel, Field

_PM2_5 = Field(ge=0, le=500)  # µg/m³, the book's expectation
_LEAD = Field(ge=1, le=7)  # days


class Observation(BaseModel):
    """One hour of measured PM2.5 at one station."""

    location_id: str
    measured_at: AwareDatetime
    pm2_5: float = _PM2_5
    ingested_at: AwareDatetime


class WeatherForecast(BaseModel):
    """Weather forecast for one hour, as issued `lead_days` before that hour."""

    location_id: str
    forecast_for: AwareDatetime
    issued_at: AwareDatetime
    lead_days: int = _LEAD
    temperature_2m: float = Field(ge=-60, le=60)  # °C
    precipitation: float = Field(ge=0, le=500)  # mm in an hour
    wind_speed_10m: float = Field(ge=0, le=400)  # km/h
    wind_direction_10m: float = Field(ge=0, lt=360)  # degrees
    ingested_at: AwareDatetime


class DailyWeather(BaseModel):
    """Forecast weather for one day, as issued `lead_days` before it."""

    location_id: str
    day: date
    lead_days: int = _LEAD
    issued_on: date
    temperature_2m: float = Field(ge=-60, le=60)
    wind_speed_10m: float = Field(ge=0, le=400)
    wet_hours: int = Field(ge=0, le=24)


class DailyAirQuality(BaseModel):
    """Measured PM2.5 for one day, with the features known from the date and the day before."""

    location_id: str
    day: date
    pm2_5: float = _PM2_5
    is_weekend: bool
    pm2_5_lag1: float = _PM2_5


class Prediction(BaseModel):
    """Predicted PM2.5 for one day, made by a run standing on `as_of`."""

    location_id: str
    as_of: date
    day: date
    lead_days: int = _LEAD
    pm2_5: float
    model_version: str

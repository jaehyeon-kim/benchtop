"""Feast's point-in-time join as the pipelines use it, on the weather_v1 view's own
entities, schema and ttl over a small Parquet copy of `daily_weather`."""

from datetime import UTC, datetime

import pandas as pd
from feast import FeatureStore, FeatureView, FileSource, RepoConfig

from airq.config import STATION
from airq.feast_repo import lead, station, weather_v1


def _day(d: int) -> datetime:
    return datetime(2026, 9, d, tzinfo=UTC)


def test_each_request_gets_the_forecast_for_its_own_day_and_lead(tmp_path):
    rows = pd.DataFrame(
        {"location_id": STATION,
         "day": [_day(21), _day(22), _day(22), _day(23)],
         "lead_days": [1, 1, 2, 2],
         "temperature_2m": [21.0, 22.0, 122.0, 123.0],
         "wind_speed_10m": 10.0, "wet_hours": 0}
    )  # fmt: skip
    rows.to_parquet(tmp_path / "daily_weather.parquet")
    view = FeatureView(
        name="weather_v1",
        entities=[station, lead],
        ttl=weather_v1.ttl,
        online=False,
        schema=weather_v1.features,
        source=FileSource(
            path=str(tmp_path / "daily_weather.parquet"), timestamp_field="day"
        ),
    )
    store = FeatureStore(
        config=RepoConfig(
            project="airq_test",
            provider="local",
            registry=str(tmp_path / "registry.db"),
            offline_store={"type": "duckdb"},
            online_store=None,
            entity_key_serialization_version=3,
        )
    )
    store.apply([station, lead, view])
    entities = pd.DataFrame(
        {"location_id": STATION,
         "event_timestamp": [_day(22), _day(23), _day(23)],
         "lead_days": [1, 1, 2]}
    )  # fmt: skip
    found = (
        store.get_historical_features(
            entity_df=entities, features=["weather_v1:temperature_2m"]
        )
        .to_df()
        .sort_values(["event_timestamp", "lead_days"], ignore_index=True)
    )
    assert found.loc[0, "temperature_2m"] == 22.0  # 22 Sep, lead 1: its own row
    # 23 Sep has no lead-1 row. The 22 Sep row must not answer for it: a
    # ttl of a whole day did, which served the previous day's forecast.
    assert pd.isna(found.loc[1, "temperature_2m"])
    assert found.loc[2, "temperature_2m"] == 123.0  # 23 Sep, lead 2

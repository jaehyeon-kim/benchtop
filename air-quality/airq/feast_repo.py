"""Feast definitions: the station and lead entities, and the v1 weather view.

The feature store reads `daily_weather` as the feature pipeline wrote it, so
training and inference get the same values. A row for (day, lead) is the
forecast for that day issued `lead_days` before it. Training asks for lead 1;
inference standing on day D asks for lead N on day D+N.

The registry is in PostgreSQL, where the Feast UI reads it. There is no online
store, because nothing is served online.

Run once after changing a definition: python -m airq.feast_repo
"""

import os
from datetime import timedelta

from feast import Entity, FeatureStore, FeatureView, Field, RepoConfig, ValueType
from feast.infra.data_sources.contrib.iceberg_catalog.iceberg_source import (
    IcebergSource,
)
from feast.types import Float64, Int32

from airq.config import CATALOG, FEAST_PROJECT, NAMESPACE

station = Entity(name="station", join_keys=["location_id"], value_type=ValueType.STRING)
lead = Entity(name="lead", join_keys=["lead_days"], value_type=ValueType.INT32)

# No endpoint and an empty warehouse, so PyIceberg takes both from the
# PYICEBERG_CATALOG__ODCTL__* variables: 127.0.0.1 on the host, service names in
# Airflow. The registry stores this definition once for both.
daily_weather = IcebergSource(
    name="daily_weather",
    warehouse="",
    namespace=NAMESPACE,
    table="daily_weather",
    catalog_name=CATALOG,
    timestamp_field="day",
)

weather_v1 = FeatureView(
    name="weather_v1",
    entities=[station, lead],
    ttl=timedelta(days=1),  # a row answers for its own day only
    online=False,
    schema=[
        Field(name="temperature_2m", dtype=Float64),
        Field(name="wind_speed_10m", dtype=Float64),
        Field(name="wet_hours", dtype=Int32),
    ],
    source=daily_weather,
)

# Model input columns in a fixed order: the order changes XGBoost's result slightly.
V1_COLUMNS = ["temperature_2m", "wind_speed_10m", "wet_hours"]
WEATHER_V1 = [f"weather_v1:{c}" for c in V1_COLUMNS]


def store() -> FeatureStore:
    return FeatureStore(
        config=RepoConfig(
            project=FEAST_PROJECT,
            provider="local",
            registry={
                "registry_type": "sql",
                "path": os.environ["FEAST_REGISTRY"],
                "cache_ttl_seconds": 60,
            },
            offline_store={"type": "duckdb"},
            online_store=None,
            entity_key_serialization_version=3,
        )
    )


if __name__ == "__main__":
    store().apply([station, lead, daily_weather, weather_v1])
    print(f"Applied project {FEAST_PROJECT}: entities station, lead; view weather_v1")

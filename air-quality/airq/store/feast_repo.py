"""Defines the Feast entities and feature views that training and inference read.

The station and lead entities identify a row. The v1 view, `weather_v1`, reads the
weather features from `daily_weather`. A row for (day, lead) is the forecast for that
day, issued `lead_days` before it. Training asks for lead 1. Inference on day D asks for
lead N on day D+N.

The v2 view, `calendar_v2`, adds the weekend flag, which the feature sweep chose.
Inference needs the flag for future days, which have no row in `daily_air_quality` yet.
So `calendar_v2` is an on-demand view that computes the flag from the `day` each request
names.

The registry is in PostgreSQL, where the Feast UI reads it. There is no online store,
because nothing is served online.

Run after changing a definition: python -m airq.store --version v1   (or v2)
"""

import argparse
import os
import warnings
from datetime import timedelta

import pandas as pd
from feast import (
    Entity,
    FeatureStore,
    FeatureView,
    Field,
    RepoConfig,
    RequestSource,
    ValueType,
)
from feast.infra.data_sources.contrib.iceberg_catalog.iceberg_source import (
    IcebergSource,
)
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float64, Int32, Int64, UnixTimestamp

from airq.config import CATALOG, FEAST_PROJECT, NAMESPACE

# calendar_v2 is an on-demand view, which Feast labels experimental.
warnings.filterwarnings(
    "ignore", "On demand feature view is an experimental feature", RuntimeWarning
)

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
    # A row answers for its own day only. Feast keeps a row while it is at most
    # `ttl` old, so a whole day would answer the next day with this day's forecast.
    ttl=timedelta(hours=12),
    online=False,
    schema=[
        Field(name="temperature_2m", dtype=Float64),
        Field(name="wind_speed_10m", dtype=Float64),
        Field(name="wet_hours", dtype=Int32),
    ],
    source=daily_weather,
)

# Each request names the day it is for in a `day` column.
calendar = RequestSource(
    name="calendar", schema=[Field(name="day", dtype=UnixTimestamp)]
)


@on_demand_feature_view(
    sources=[calendar], schema=[Field(name="is_weekend", dtype=Int64)], mode="pandas"
)
def calendar_v2(inputs: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the weekend flag for each requested day.

    Args:
        inputs (pd.DataFrame): The request rows, with the `day` each one is for.

    Returns:
        pd.DataFrame: An `is_weekend` column: 1 for a Saturday or Sunday, otherwise 0.
    """
    return pd.DataFrame({"is_weekend": (inputs["day"].dt.weekday >= 5).astype("int64")})


# Model input columns in a fixed order: the order changes XGBoost's result slightly.
V1_COLUMNS = ["temperature_2m", "wind_speed_10m", "wet_hours"]
V2_COLUMNS = [*V1_COLUMNS, "is_weekend"]
# Each feature set's Feast references and model input columns. A registered model
# version carries its set's name in the `feature_set` tag.
FEATURE_SETS = {
    "v1": ([f"weather_v1:{c}" for c in V1_COLUMNS], V1_COLUMNS),
    "v2": ([f"weather_v1:{c}" for c in V1_COLUMNS] + ["calendar_v2:is_weekend"], V2_COLUMNS),
}  # fmt: skip


def store() -> FeatureStore:
    """
    Returns the Feast feature store of the `airq` project.

    The offline store is DuckDB, and there is no online store.

    Returns:
        FeatureStore: The store, with its registry in the PostgreSQL database that
            `FEAST_REGISTRY` names.

    Raises:
        KeyError: If `FEAST_REGISTRY` is not set.
    """
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


# The Feast objects each version needs. v2 adds calendar_v2 to v1's.
VERSION_OBJECTS = {
    "v1": [station, lead, daily_weather, weather_v1],
    "v2": [station, lead, daily_weather, weather_v1, calendar, calendar_v2],
}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Registers the Feast entities and views a model version needs."
    )
    parser.add_argument("--version", required=True, choices=list(VERSION_OBJECTS))
    args = parser.parse_args()
    objects = VERSION_OBJECTS[args.version]
    store().apply(objects)
    views = ", ".join(
        o.name for o in objects if o.name.startswith(("weather", "calendar_"))
    )
    print(f"Applied project {FEAST_PROJECT} for {args.version}: views {views}")

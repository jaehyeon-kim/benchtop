"""Iceberg: the catalog, the tables, and their schemas derived from the pydantic models."""

import os

import pyarrow as pa
from pydantic import AwareDatetime, BaseModel
from pyiceberg.catalog.rest import RestCatalog

from airq.generator import Observation, WeatherForecast

NAMESPACE = "airq"
TABLES = {
    Observation: f"{NAMESPACE}.observations",
    WeatherForecast: f"{NAMESPACE}.weather_forecasts",
}
_TS = pa.timestamp("us", tz="UTC")
_ARROW = {str: pa.string(), float: pa.float64(), int: pa.int32(), AwareDatetime: _TS}


def arrow_schema(model: type[BaseModel]) -> pa.Schema:
    """Arrow schema, and so Iceberg schema, derived from the model."""
    return pa.schema(
        pa.field(name, _ARROW[info.annotation], nullable=False)
        for name, info in model.model_fields.items()
    )


def catalog() -> RestCatalog:
    """Iceberg REST catalog. Defaults reach the odctl `catalog` profile from the
    host; inside an odctl container set ICEBERG_URI=http://catalog:8181 and
    S3_ENDPOINT=http://seaweed:8333. The rest is fixed by odctl."""
    return RestCatalog(
        "odctl",
        **{
            "uri": os.getenv("ICEBERG_URI", "http://localhost:8181"),
            "warehouse": "s3://warehouse/",
            "s3.endpoint": os.getenv("S3_ENDPOINT", "http://localhost:8333"),
            "s3.access-key-id": "user",
            "s3.secret-access-key": "password",
            "s3.region": "us-east-1",
        },
    )


def recreate_tables(cat) -> None:
    cat.create_namespace_if_not_exists(NAMESPACE)
    for model, identifier in TABLES.items():
        if cat.table_exists(identifier):
            cat.drop_table(identifier)
        cat.create_table(identifier, schema=arrow_schema(model))

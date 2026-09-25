"""Iceberg: the catalog, the tables, and their schemas derived from the pydantic models."""

from datetime import date

import pyarrow as pa
from pydantic import AwareDatetime, BaseModel
from pyiceberg.catalog import Catalog, load_catalog

from airq.config import CATALOG, NAMESPACE, TABLES

_TS = pa.timestamp("us", tz="UTC")
_ARROW = {
    str: pa.string(),
    float: pa.float64(),
    int: pa.int32(),
    bool: pa.bool_(),
    date: pa.date32(),
    AwareDatetime: _TS,
}


def arrow_schema(model: type[BaseModel]) -> pa.Schema:
    """Arrow schema, and so Iceberg schema, derived from the model."""
    return pa.schema(
        pa.field(name, _ARROW[info.annotation], nullable=False)
        for name, info in model.model_fields.items()
    )


def catalog() -> Catalog:
    """The Iceberg REST catalog, configured by the PYICEBERG_CATALOG__ODCTL__* variables."""
    return load_catalog(CATALOG)


def recreate_tables(cat, properties: dict[str, str]) -> None:
    cat.create_namespace_if_not_exists(NAMESPACE)
    for model, identifier in TABLES.items():
        if cat.table_exists(identifier):
            cat.drop_table(identifier)
        cat.create_table(identifier, schema=arrow_schema(model), properties=properties)


def to_arrow(model: type[BaseModel], rows: list[BaseModel]) -> pa.Table:
    return pa.Table.from_pylist(
        [r.model_dump() for r in rows], schema=arrow_schema(model)
    )

"""The loader against a local SQLite catalog, so no test needs the odctl stack."""

from datetime import date

import pyarrow as pa
import pyiceberg.catalog
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.utils.config import Config

from airq.core.config import TABLES
from airq.core.models import DailyAirQuality
from airq.feature.load import load
from airq.stores.iceberg import catalog

# The first day has no daily air quality row, which needs the day before
UNTIL = date(2024, 10, 2)


@pytest.fixture
def local_catalog(tmp_path, monkeypatch):
    """
    Points the `odctl` catalog, which the loader and its blueprint read, at SQLite.

    PyIceberg reads the variables once, at import, so its copy is read again here. A
    catalog that is not the SQLite one stops the test before it loads anything.
    """
    monkeypatch.setenv("PYICEBERG_CATALOG__ODCTL__TYPE", "sql")
    monkeypatch.setenv("PYICEBERG_CATALOG__ODCTL__URI", f"sqlite:///{tmp_path}/c.db")
    monkeypatch.setenv("PYICEBERG_CATALOG__ODCTL__WAREHOUSE", f"file://{tmp_path}")
    monkeypatch.setattr(pyiceberg.catalog, "_ENV_CONFIG", Config())
    cat = catalog()
    if not isinstance(cat, SqlCatalog):
        pytest.fail(f"the test catalog is {type(cat).__name__}, not SQLite")
    return cat


def test_a_load_writes_every_table(local_catalog):
    """Verify that a load of two days writes rows to all four tables."""
    load(2, UNTIL, reset=True)
    for identifier in TABLES.values():
        assert local_catalog.load_table(identifier).scan().count() > 0


def test_a_table_commit_that_fails_fails_the_load(local_catalog):
    """Verify that the load exits when the sink cannot write one table."""
    load(2, UNTIL, reset=True)
    identifier = TABLES[DailyAirQuality]
    properties = local_catalog.load_table(identifier).properties
    local_catalog.drop_table(identifier)
    # a table without the blueprint's columns refuses every write
    local_catalog.create_table(
        identifier, schema=pa.schema([("other", pa.string())]), properties=properties
    )
    with pytest.raises(SystemExit, match="The load is incomplete"):
        load(2, UNTIL)

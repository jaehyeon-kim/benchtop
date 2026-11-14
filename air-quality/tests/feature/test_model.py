from collections import defaultdict
from datetime import timedelta

import pyarrow as pa
import yaml

from airq.core.config import DEFAULT_SEED, LEADS, TABLES
from airq.core.models import Observation, WeatherForecast
from airq.feature.load import BLUEPRINT, variables
from airq.stores.iceberg import arrow_schema
from tests.conftest import ORIGIN, desflow, run_rows

# The Iceberg type name the blueprint's `schemas` gives each Arrow type of iceberg.py
_TYPE_NAMES = {
    pa.string(): "string",
    pa.float64(): "double",
    pa.int32(): "int",
    pa.bool_(): "boolean",
    pa.date32(): "date",
    pa.timestamp("us", tz="UTC"): "timestamptz",
}


def test_blueprint_tests_pass():
    """Verify the blueprint's own tests: the rows a 30-day run writes, each inside the bounds of models.py."""
    result = desflow("test", variables(ORIGIN, ORIGIN, 30, DEFAULT_SEED))
    assert result.returncode == 0, result.stdout + result.stderr[-2000:]


def test_blueprint_schemas_match_the_tables():
    """Verify that the blueprint names every table with the columns and types iceberg.py creates it with."""
    egress = yaml.safe_load(BLUEPRINT.read_text())["egress"][0]["config"]
    for model, identifier in TABLES.items():
        expected = {f.name: _TYPE_NAMES[f.type] for f in arrow_schema(model)}
        assert egress["schemas"][identifier] == expected
        assert identifier in egress["upsert_keys"]


def test_each_hour_issues_one_forecast_per_lead():
    """Verify that each hour issues seven forecasts, each for the hour `lead_days` later."""
    forecasts = [r for r in run_rows(3) if isinstance(r, WeatherForecast)]
    by_issue = defaultdict(list)
    for row in forecasts:
        by_issue[row.issued_at].append(row)
    assert len(by_issue) == 3 * 24
    for rows in by_issue.values():
        assert sorted(r.lead_days for r in rows) == list(LEADS)
        for row in rows:
            assert row.forecast_for - row.issued_at == timedelta(days=row.lead_days)


def test_observations_are_hourly():
    """Verify that a backfill writes one reading for each hour of its days."""
    observations = [r for r in run_rows(3) if isinstance(r, Observation)]
    assert [r.measured_at for r in observations] == [
        ORIGIN + timedelta(hours=h) for h in range(3 * 24)
    ]


def test_same_seed_gives_the_same_rows():
    """Verify that the same seed gives the same rows and a different seed does not."""

    def output(seed: int) -> str:  # not cached, unlike run_rows
        return desflow("run", variables(ORIGIN, ORIGIN, 3, seed)).stdout

    assert output(7) == output(7)
    assert output(7) != output(8)

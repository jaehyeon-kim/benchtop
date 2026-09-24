"""Shared helpers. Records are collected in memory, so no test needs the odctl stack."""

import pytest

from airq.simulator import HOUR, build


@pytest.fixture
def simulate():
    """Runs the station for `days` from ORIGIN and returns every record."""

    def run(days: int, seed: int = 42) -> list:
        rows: list = []
        build(lambda ctx, row: rows.append(row), seed).run(until=days * 24 * HOUR)
        return rows

    return run

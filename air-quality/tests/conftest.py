"""Shared helpers. Records are collected in memory, so no test needs the odctl stack."""

from datetime import UTC, datetime

import pytest

from airq.config import DEFAULT_SEED
from airq.generator import generate

ORIGIN = datetime(2024, 10, 1, tzinfo=UTC)  # a fixed first hour, so results do not move


@pytest.fixture
def simulate():
    """Generates `days` of records from ORIGIN, in the order the backfill writes them."""

    def run(days: int, seed: int = DEFAULT_SEED) -> list:
        records = generate(ORIGIN, seed)
        return [row for _ in range(days * 24) for row in next(records)]

    return run

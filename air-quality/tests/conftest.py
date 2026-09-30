"""Shared helpers. Records are collected in memory, so no test needs the odctl stack."""

from datetime import UTC, datetime

import pandas as pd
import pytest

from airq.core.config import DEFAULT_SEED
from airq.feature.features import daily_features
from airq.feature.generator import generate

ORIGIN = datetime(2024, 10, 1, tzinfo=UTC)  # a fixed first hour, so results do not move


@pytest.fixture
def simulate():
    """Generates `days` of records from ORIGIN, in the order the backfill writes them."""

    def run(days: int, seed: int = DEFAULT_SEED) -> list:
        records = generate(ORIGIN, seed)
        return [row for _ in range(days * 24) for row in next(records)]

    return run


def daily_frame(n_days: int, seed: int = DEFAULT_SEED, lead: int = 1) -> pd.DataFrame:
    """
    Builds a training frame in memory, with the feature pipeline's own code.

    Args:
        n_days (int): How many days to generate from ORIGIN.
        seed (int): The seed for the generated values.
        lead (int): The lead of the weather forecast to join.

    Returns:
        pd.DataFrame: One row per day, with the label, the baseline and the features.
    """
    forecasts, observations = [], []
    records = generate(ORIGIN, seed)
    for _ in range(n_days * 24):
        hour = next(records)
        forecasts += hour[:-1]
        observations.append(hour[-1])
    weather, air_quality = daily_features(forecasts, observations)
    weather = pd.DataFrame([r.model_dump() for r in weather if r.lead_days == lead])
    frame = pd.DataFrame([r.model_dump() for r in air_quality]).merge(
        weather, on=["location_id", "day"]
    )
    frame["event_timestamp"] = pd.to_datetime(frame["day"], utc=True)
    return frame.astype({"is_weekend": "float64", "wet_hours": "float64"})

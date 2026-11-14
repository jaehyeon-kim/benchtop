"""Shared helpers. The blueprint's rows are read from `desflow run --stdout`, so no test needs the odctl stack."""

import functools
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from pydantic import BaseModel

from airq.core.config import DEFAULT_SEED
from airq.core.models import DailyAirQuality, DailyWeather, Observation, WeatherForecast
from airq.feature.load import BLUEPRINT, variables

ORIGIN = datetime(2024, 10, 1, tzinfo=UTC)  # a fixed first hour, so results do not move
_DESFLOW = str(Path(sys.executable).with_name("desflow"))
# The blueprint task that writes each table's rows. The console drops `__table__`.
_TASKS: dict[str, type[BaseModel]] = {
    "reading": Observation,
    "forecast_row": WeatherForecast,
    "daily_weather": DailyWeather,
    "daily_air_quality": DailyAirQuality,
}


def desflow(command: str, values: dict[str, str]) -> subprocess.CompletedProcess:
    """
    Runs a desflow command on the blueprint, with the variables `load` would pass.

    Args:
        command (str): The desflow command, such as `run` or `test`.
        values (dict[str, str]): The blueprint's variables, by name.

    Returns:
        subprocess.CompletedProcess: The finished command, with its output as text.
    """
    options = [f"--var={name}={value}" for name, value in values.items()]
    extra = ["--stdout"] if command == "run" else []
    return subprocess.run(
        [_DESFLOW, command, str(BLUEPRINT), *extra, *options],
        capture_output=True,
        text=True,
        check=False,  # the caller reads the return code
    )


@functools.cache
def _rows(start: datetime, n_days: int, seed: int) -> tuple[BaseModel, ...]:
    """The rows of one run, cached, because several tests read the same run."""
    result = desflow("run", variables(ORIGIN, start, n_days, seed))
    assert result.returncode == 0, result.stderr[-2000:]
    records = (json.loads(line) for line in result.stdout.splitlines())
    return tuple(_TASKS[r["task"]].model_validate(r["value"]) for r in records)


def run_rows(
    n_days: int, seed: int = DEFAULT_SEED, start: datetime = ORIGIN
) -> list[BaseModel]:
    """
    Returns the rows a run of the blueprint writes, as `load` runs it, in the order written.

    The run simulates from ORIGIN and writes the `n_days` from `start`. Each row is
    validated with its table's model, so a row that breaks a bound or a type fails here.

    Args:
        n_days (int): How many days to load.
        seed (int): The seed for the generated values.
        start (datetime): The first day to load, at or after ORIGIN.

    Returns:
        list[BaseModel]: The rows of the four tables.
    """
    return list(_rows(start, n_days, seed))


def daily_frame(n_days: int, seed: int = DEFAULT_SEED, lead: int = 1) -> pd.DataFrame:
    """
    Builds a training frame from the daily rows a backfill of `n_days` writes.

    Args:
        n_days (int): How many days to load from ORIGIN.
        seed (int): The seed for the generated values.
        lead (int): The lead of the weather forecast to join.

    Returns:
        pd.DataFrame: One row per day, with the label, the baseline and the features.
    """
    rows = run_rows(n_days, seed)
    weather = pd.DataFrame(
        [r.model_dump() for r in rows if isinstance(r, DailyWeather) and r.lead_days == lead]
    )  # fmt: skip
    air_quality = pd.DataFrame(
        [r.model_dump() for r in rows if isinstance(r, DailyAirQuality)]
    )
    frame = air_quality.merge(weather, on=["location_id", "day"])
    frame["event_timestamp"] = pd.to_datetime(frame["day"], utc=True)
    return frame.astype({"is_weekend": "float64", "wet_hours": "float64"})

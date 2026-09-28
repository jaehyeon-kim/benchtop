"""Writes one day's rows to the four Iceberg tables, with PyIceberg only.

The run reads the first hour and the seed that the backfill stored on the tables. It
generates the data from that hour to the end of the day, which gives the same values the
backfill writes, and keeps only that day. A different `seed` gives different values,
which do not continue from the backfill's days.

The day's rows are the forecasts issued on it, its readings, the daily weather issued on
it and its air quality. In each table they replace any rows for that day in one commit,
so running a day again replaces it.

Run: python -m airq.feature.daily --date "3 days ago" --seed 42
Defaults: yesterday (UTC) and the backfill's seed.
"""

import argparse
import logging
from datetime import UTC, date, datetime, time, timedelta

from pydantic import BaseModel

from airq import days
from airq.config import ORIGIN_PROPERTY, SEED_PROPERTY, TABLES
from airq.feature.features import DAY_COLUMN, daily_features, in_window
from airq.feature.generator import generate
from airq.iceberg import catalog, to_arrow
from airq.models import DailyAirQuality, DailyWeather, Observation, WeatherForecast

logger = logging.getLogger(
    "airq.feature.daily"
)  # __name__ is "__main__" under python -m


def day_rows(
    day: date, origin: datetime, seed: int
) -> dict[type[BaseModel], list[BaseModel]]:
    """
    Returns the rows that one day adds to each table.

    The readings of the day before are generated too, because the day's air quality row
    needs the previous day's mean. Only the day's own rows are returned.

    Args:
        day (date): The day to generate.
        origin (datetime): The first hour of the generated data, as stored by the
            backfill.
        seed (int): The seed for the generated values.

    Returns:
        dict[type[BaseModel], list[BaseModel]]: The day's rows for each table, keyed by
            the table's model.
    """
    start = datetime.combine(day, time(), UTC)
    end = start + timedelta(days=1)
    forecasts, observations = [], []
    for records in generate(origin, seed):
        hour = records[-1].measured_at
        if hour >= end:
            break
        if hour >= start:
            forecasts += records[:-1]
        if hour >= start - timedelta(days=1):  # the day before, for the lag
            observations.append(records[-1])
    weather, air_quality = daily_features(forecasts, observations)
    rows = {
        WeatherForecast: forecasts,
        Observation: observations,
        DailyWeather: weather,
        DailyAirQuality: air_quality,
    }
    return {m: [r for r in v if in_window(r, start, end)] for m, v in rows.items()}


def run(day: date, seed: int | None = None) -> None:
    """
    Writes the rows for `day` to the four tables, replacing any rows for that day.

    Args:
        day (date): The day to write.
        seed (int, optional): The seed for the generated values. Defaults to the seed
            the backfill stored.

    Raises:
        SystemExit: If the tables do not exist, have no stored first hour, or start
            after `day`.
    """
    cat = catalog()
    if not cat.table_exists(TABLES[Observation]):
        raise SystemExit("There are no tables yet: run the backfill first.")
    properties = cat.load_table(TABLES[Observation]).properties
    if ORIGIN_PROPERTY not in properties:
        raise SystemExit("The tables have no origin: run the backfill first.")
    origin = datetime.fromisoformat(properties[ORIGIN_PROPERTY])
    if datetime.combine(day, time(), UTC) < origin:
        raise SystemExit(f"{day} is before the backfill's first day, {origin.date()}.")
    start = datetime.combine(day, time(), UTC)
    seed = int(properties[SEED_PROPERTY]) if seed is None else seed
    for model, rows in day_rows(day, origin, seed).items():
        column = DAY_COLUMN[model]
        if model in (WeatherForecast, Observation):
            where = f"{column} >= '{start.isoformat()}' AND {column} < '{(start + timedelta(days=1)).isoformat()}'"
        else:
            where = f"{column} = '{day.isoformat()}'"
        table = cat.load_table(TABLES[model])
        table.overwrite(to_arrow(model, rows), overwrite_filter=where)
        logger.info("%s: %d rows for %s", TABLES[model], len(rows), day)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--date",
        type=days.argument,
        default="yesterday",
        help=f"day to load: {days.FORMS} (default: yesterday, UTC)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="seed for the generated values (default: the backfill's)",
    )
    args = parser.parse_args()
    run(args.date, args.seed)

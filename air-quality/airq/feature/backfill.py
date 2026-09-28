"""Writes the last `n_days` of simulated history to the Iceberg tables.

Each run drops and recreates the four tables. The history starts `n_days` before today
and ends at the end of yesterday, UTC. The first hour and `seed` are stored as table
properties, so the daily run can continue the same data.

The hourly rows are published through dynamic-des, which writes them to Iceberg. The
daily features are computed from the same rows and written with PyIceberg.

Run: python -m airq.feature.backfill --n-days 730 --seed 42
"""

import argparse
import logging
from datetime import UTC, datetime, timedelta

from dynamic_des import IcebergStorageEgress, SimulationContext
from pydantic import BaseModel

from airq.config import DEFAULT_SEED, ORIGIN_PROPERTY, SEED_PROPERTY, TABLES
from airq.feature.features import daily_features
from airq.feature.generator import generate
from airq.iceberg import catalog, recreate_tables, to_arrow
from airq.models import DailyAirQuality, DailyWeather

logger = logging.getLogger(
    "airq.feature.backfill"
)  # __name__ is "__main__" under python -m

_MODELS = {model.__name__: model for model in TABLES}

_HOUR = 3600.0


def route(r: dict) -> str:
    """
    Returns the table for a published event, and turns the event back into a row.

    dynamic-des publishes each row as `model_dump(mode="json")`, so its timestamps
    arrive as strings. Validating the row with its model turns them back into datetimes.
    The row replaces the contents of `r`, because the egress writes the same dict it
    passes here.

    Args:
        r (dict): The published event. `key` holds the model name and `value` the row.

    Returns:
        str: The identifier of the Iceberg table the row belongs to.
    """
    model = _MODELS[r["key"]]
    row = model.model_validate(r["value"]).model_dump()
    r.clear()
    r.update(row)
    return TABLES[model]


def backfill(n_days: int = 730, seed: int = DEFAULT_SEED) -> None:
    """
    Recreates the tables and writes `n_days` of generated history to them.

    The hourly forecasts and readings go through a dynamic-des simulation, one simulated
    hour at a time, and are written in one Iceberg commit per table. The daily weather
    and air quality rows are then computed from the same hourly rows and appended.

    Args:
        n_days (int): How many days to write, ending with yesterday.
        seed (int): The seed for the generated values.
    """
    end = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    origin = end - timedelta(days=n_days)
    cat = catalog()
    recreate_tables(
        cat, {ORIGIN_PROPERTY: origin.isoformat(), SEED_PROPERTY: str(seed)}
    )
    app = SimulationContext(
        "air_quality", factor=0.0, logical_start_time=origin.replace(tzinfo=None)
    )
    records = generate(origin, seed)
    forecasts, observations = [], []

    @app.telemetry_loop(interval=_HOUR)
    def hourly(ctx):
        # dynamic-des publishes a lag metric every simulated second by default,
        # which is 63 million records over two years at factor=0.
        ctx.env.egress_lag_monitor_interval = _HOUR
        rows = next(records)
        forecasts.extend(rows[:-1])
        observations.append(rows[-1])
        for row in rows:
            ctx.env.publish_event(type(row).__name__, row)

    # One buffer for the whole run, so one Iceberg commit per table.
    app.add_egress(
        IcebergStorageEgress(catalog=cat, table_router=route),
        when=lambda r: r["stream_type"] == "event",
        batch_size=500_000,
    )
    app.run(until=(end - origin).total_seconds())

    weather, air_quality = daily_features(forecasts, observations)
    daily: list[tuple[type[BaseModel], list]] = [
        (DailyWeather, weather),
        (DailyAirQuality, air_quality),
    ]
    for model, rows in daily:
        cat.load_table(TABLES[model]).append(to_arrow(model, rows))
    for identifier in TABLES.values():
        logger.info(
            "%s: %d rows", identifier, cat.load_table(identifier).scan().count()
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--n-days", type=int, default=730, help="days to write, ending yesterday"
    )
    parser.add_argument(
        "--seed", type=int, default=DEFAULT_SEED, help="seed for the generated values"
    )
    args = parser.parse_args()
    backfill(args.n_days, args.seed)

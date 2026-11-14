"""Loads simulated days into the four Iceberg tables, for a backfill or a daily run.

A run covers `n_days` ending with `until`. With `reset`, or when the tables do not exist
yet, it recreates the tables and stores the window's first hour and `seed` on them.
Otherwise it reads the stored first hour and seed. It then runs the desflow blueprint
`model.yaml` from the stored first hour, which writes only the days asked for and upserts
on each table's keys, so running a day again replaces it.

Run: python -m airq.feature.load --n-days 730 --reset   (backfill)
     python -m airq.feature.load --until today          (one day; default: yesterday)
"""

import argparse
import logging
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

from desflow import SimulationContext

from airq.core import days
from airq.core.config import DEFAULT_SEED, ORIGIN_PROPERTY, SEED_PROPERTY, TABLES
from airq.core.models import Observation
from airq.stores.iceberg import catalog, recreate_tables

logger = logging.getLogger(
    "airq.feature.load"
)  # __name__ is "__main__" under python -m

BLUEPRINT = Path(__file__).with_name("model.yaml")
# desflow's sinks log a write that fails and go on, so the load listens for these errors
SINK_LOGGER = "desflow.connectors.egress"
_FORECAST_HOURS = 7 * 24  # a forecast is written at the hour it is for


def variables(
    origin: datetime, start: datetime, n_days: int, seed: int
) -> dict[str, str]:
    """
    Returns the values of the variables the blueprint reads, for one run.

    A run always simulates from the stored first hour with the stored seed, so a day
    gets the same values in the backfill and in a daily run. The hours before `start`
    are the warm-up, which desflow simulates but does not write.

    Args:
        origin (datetime): The stored first hour, in UTC.
        start (datetime): The first hour to load, in UTC.
        n_days (int): How many days to load.
        seed (int): The seed for the generated values.

    Returns:
        dict[str, str]: START, SEED, HOURS, RUN_HOURS and WARMUP_HOURS, by name.
    """
    warmup = int((start - origin).total_seconds()) // 3600
    hours = warmup + n_days * 24
    return {
        "START": origin.isoformat(),
        "SEED": str(seed),
        "HOURS": str(hours),
        "RUN_HOURS": str(hours + _FORECAST_HOURS),
        "WARMUP_HOURS": str(warmup),
    }


class _SinkErrors(logging.Handler):
    """Keeps the errors that desflow's sinks log during a run."""

    def __init__(self) -> None:
        super().__init__(logging.ERROR)
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


def load(
    n_days: int = 1,
    until: date | None = None,
    seed: int | None = None,
    reset: bool = False,
) -> None:
    """
    Loads `n_days` of simulated data, ending with `until`, into the four tables.

    Args:
        n_days (int): How many days to load.
        until (date, optional): The last day to load. Defaults to yesterday, UTC.
        seed (int, optional): The seed for the generated values. Defaults to the stored
            seed, or to the default seed when the tables are recreated.
        reset (bool): Whether to recreate the tables first.

    Raises:
        SystemExit: If the window starts before the stored first hour, or a sink logs
            an error, such as a table commit that failed.
    """
    until = until or days.resolve("yesterday")
    start = datetime.combine(until, time(), UTC) - timedelta(days=n_days - 1)
    cat = catalog()
    reset = reset or not cat.table_exists(TABLES[Observation])
    if reset:
        seed = DEFAULT_SEED if seed is None else seed
        origin = start
        recreate_tables(
            cat, {ORIGIN_PROPERTY: origin.isoformat(), SEED_PROPERTY: str(seed)}
        )
    else:
        properties = cat.load_table(TABLES[Observation]).properties
        origin = datetime.fromisoformat(properties[ORIGIN_PROPERTY])
        if start < origin:
            raise SystemExit(
                f"{start.date()} is before the first day, {origin.date()}."
            )
        seed = int(properties[SEED_PROPERTY]) if seed is None else seed

    app = SimulationContext.from_yaml(
        BLUEPRINT, variables=variables(origin, start, n_days, seed)
    )
    errors = _SinkErrors()
    sink_logger = logging.getLogger(SINK_LOGGER)
    sink_logger.addHandler(errors)
    try:
        app.run()
    finally:
        sink_logger.removeHandler(errors)
    if errors.messages:
        raise SystemExit(
            f"The load is incomplete: {len(errors.messages)} sink errors, the first: "
            f"{errors.messages[0]}"
        )
    for identifier in TABLES.values():
        logger.info(
            "%s: %d rows", identifier, cat.load_table(identifier).scan().count()
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--n-days", type=int, default=1, help="days to load (default: 1)"
    )
    parser.add_argument(
        "--until",
        type=days.argument,
        default="yesterday",
        help=f"last day to load: {days.FORMS} (default: yesterday, UTC)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="seed for the generated values (default: the stored one, or 42 on reset)",
    )
    parser.add_argument(
        "--reset", action="store_true", help="recreate the tables before loading"
    )
    args = parser.parse_args()
    load(args.n_days, args.until, args.seed, args.reset)

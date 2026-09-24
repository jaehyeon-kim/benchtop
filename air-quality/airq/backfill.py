"""Backfill: the simulated history from ORIGIN to the end of yesterday, into Iceberg.

Each run drops and recreates both tables.

Run: python -m airq.backfill
"""

import logging
from datetime import UTC, datetime

from dynamic_des import IcebergStorageEgress
from pydantic import BaseModel

from airq.iceberg import TABLES, catalog, recreate_tables
from airq.simulator import HOUR, ORIGIN, build

logger = logging.getLogger(__name__)

MODELS = {model.__name__: model for model in TABLES}


def publish(ctx, row: BaseModel) -> None:
    # dynamic-des publishes a lag metric every simulated second by default,
    # which is 63 million records over two years at factor=0.
    ctx.env.egress_lag_monitor_interval = HOUR
    ctx.env.publish_event(type(row).__name__, row)


def route(r: dict) -> str:
    """Turns a published event back into its model's row.

    dynamic-des serialises the model with model_dump(mode="json"), so timestamps
    arrive as ISO strings; validating restores them to datetimes. The row replaces
    the record in place because the egress writes the dict the router was given.
    """
    model = MODELS[r["key"]]
    row = model.model_validate(r["value"]).model_dump()
    r.clear()
    r.update(row)
    return TABLES[model]


def backfill() -> None:
    cat = catalog()
    recreate_tables(cat)
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    app = build(publish)
    # One buffer for the whole run, so one Iceberg commit per table.
    app.add_egress(
        IcebergStorageEgress(catalog=cat, table_router=route),
        when=lambda r: r["stream_type"] == "event",
        batch_size=500_000,
    )
    app.run(until=(today - ORIGIN).total_seconds())
    for identifier in TABLES.values():
        logger.info(
            "%s: %d rows", identifier, cat.load_table(identifier).scan().count()
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    backfill()

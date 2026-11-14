"""The clean-up of PostgreSQL: the six tables in the `cdc` schema, and the replication slot.

The simulation creates the tables, from the `tables` of its sinks in `shop.yaml`.
"""

import asyncio
import time

import asyncpg

from ecommerce.core.config import DSN, REPLICATION_SLOT, SCHEMA
from ecommerce.core.models import TABLES


def _execute(*statements: str) -> None:
    """Runs statements in one connection."""

    async def run() -> None:
        conn = await asyncpg.connect(DSN)
        try:
            for statement in statements:
                await conn.execute(statement)
        finally:
            await conn.close()

    asyncio.run(run())


def drop_tables() -> None:
    """Drops the six tables, if they exist."""
    _execute(*(f"DROP TABLE IF EXISTS {SCHEMA}.{t}" for t in TABLES.values()))


def drop_slot(wait: float = 30) -> None:
    """
    Drops the Debezium connector's replication slot, once the connector has let go of it.

    Args:
        wait (float): How many seconds to wait for the slot to become inactive.
    """
    query = f"SELECT active FROM pg_replication_slots WHERE slot_name = '{REPLICATION_SLOT}'"

    async def active() -> bool | None:
        conn = await asyncpg.connect(DSN)
        try:
            return await conn.fetchval(query)
        finally:
            await conn.close()

    deadline = time.monotonic() + wait
    while (state := asyncio.run(active())) and time.monotonic() < deadline:
        time.sleep(1)  # the slot stays active until the connector's task ends
    if state is False:
        _execute(f"SELECT pg_drop_replication_slot('{REPLICATION_SLOT}')")

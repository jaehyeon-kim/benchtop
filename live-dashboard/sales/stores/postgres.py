"""The store: the dashboards' query, live changes and clean-up in odctl's PostgreSQL.

desflow creates the four tables when `model.yaml` starts, and the parameter table when
its ingress starts. Timestamps are ISO strings, as desflow publishes them, so queries
cast them.
"""

import asyncpg

from sales.core.config import DSN, PARAMS_TABLE, SCHEMA

# The order items of the last $1 minutes, with their users and products.
# clock_timestamp() is the time now; current_timestamp would stay at the transaction's start.
RECENT_ITEMS = f"""
SELECT u.id AS user_id, u.age, u.gender, u.country, u.traffic_source,
    o.order_id, o.id AS item_id, p.category, p.cost, o.status AS item_status,
    o.sale_price, o.created_at
FROM {SCHEMA}.order_items AS o
JOIN {SCHEMA}.users AS u ON u.id = o.user_id
JOIN {SCHEMA}.products AS p ON p.id = o.product_id
WHERE o.created_at::timestamptz >= clock_timestamp() - make_interval(mins => $1)
"""


async def drop_schema() -> None:
    """Drops the schema with every table in it, if it exists."""
    conn = await asyncpg.connect(DSN)
    try:
        await conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    finally:
        await conn.close()


async def send_change(path: str, value: float) -> None:
    """
    Adds a parameter change for the running simulation to pick up.

    Args:
        path (str): The registry path, such as `sales.arrival.visitor.rate`.
        value (float): Its new value.
    """
    conn = await asyncpg.connect(DSN)
    try:
        await conn.execute(
            f"INSERT INTO {PARAMS_TABLE} (path, value) VALUES ($1, $2)",
            path,
            str(value),
        )
    finally:
        await conn.close()


async def recent_items(conn: asyncpg.Connection, minutes: int) -> list[dict]:
    """
    Reads the order items of the last `minutes`, with their users and products.

    Args:
        conn (asyncpg.Connection): An open connection.
        minutes (int): The lookback window.

    Returns:
        list[dict]: One record per order item.
    """
    return [dict(row) for row in await conn.fetch(RECENT_ITEMS, minutes)]

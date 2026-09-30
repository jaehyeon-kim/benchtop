"""The leaderboard tables in PostgreSQL: create them, read one, drop them all."""

from pathlib import Path

import psycopg

from leaderboard.core.config import DATABASE, SCHEMA

# The tables Flink writes to, defined beside the jobs that write them.
_TABLES_SQL = Path(__file__).parent.parent / "jobs" / "tables.sql"


def create() -> None:
    """Creates the schema and the four tables in `jobs/tables.sql`, leaving any that exist."""
    with psycopg.connect(DATABASE) as conn:
        conn.execute(_TABLES_SQL.read_text())


def query(table: str, label: str, value: str) -> str:
    """
    Returns the SQL that reads a leaderboard's labels and values, best first.

    Args:
        table (str): The leaderboard table, such as `top_teams`.
        label (str): The column that names each row, such as `team_name`.
        value (str): The column that ranks them, such as `total_score`.

    Returns:
        str: The query.
    """
    return f"SELECT {label}, {value} FROM {SCHEMA}.{table} ORDER BY rnk"


def read(table: str, label: str, value: str) -> list[tuple[str, float]]:
    """
    Reads a leaderboard.

    Args:
        table (str): The leaderboard table.
        label (str): The column that names each row.
        value (str): The column that ranks them.

    Returns:
        list[tuple[str, float]]: The label and value of each rank, best first.
    """
    with psycopg.connect(DATABASE) as conn:
        return conn.execute(query(table, label, value)).fetchall()


def drop() -> None:
    """Drops the schema with its tables, if it exists."""
    with psycopg.connect(DATABASE) as conn:
        conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")

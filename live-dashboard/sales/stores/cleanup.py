"""Removes everything this project wrote: the `dashboard` schema and its tables.

The rest of odctl's PostgreSQL is left alone, and the services keep running.

Run: python -m sales.stores.cleanup
"""

import asyncio

from sales.core.config import SCHEMA
from sales.stores import postgres

if __name__ == "__main__":
    asyncio.run(postgres.drop_schema())
    print(f"Dropped the schema {SCHEMA} and its tables, if they existed")

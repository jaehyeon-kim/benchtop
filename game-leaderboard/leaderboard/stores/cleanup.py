"""Removes everything this project created, and keeps the services running.

It cancels the four Flink jobs, deletes the Kafka topics and the schema subject, and
drops the PostgreSQL schema. Other projects' objects are left alone.
Run: python -m leaderboard.stores.cleanup
"""

import logging

from leaderboard.stores import flink, kafka, postgres

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    flink.cancel()
    kafka.delete()
    postgres.drop()
    logging.getLogger("leaderboard.stores.cleanup").info(
        "Removed the Flink jobs, Kafka topics, schema subject and tables"
    )

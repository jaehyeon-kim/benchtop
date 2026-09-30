"""Removes everything this project created, and keeps the odctl services running.

It deletes the connectors and their offsets, the replication slot, the six tables, this
project's Kafka topics and their schemas, and the sink's files. It is safe to run again.

Run: python -m ecommerce.stores.cleanup
"""

import logging

from ecommerce.stores import connect, kafka, postgres, s3

# __name__ is "__main__" under python -m
logger = logging.getLogger("ecommerce.stores.cleanup")


def cleanup() -> None:
    """Removes the connectors, the slot, the tables, the topics, the schemas and the files, in that order."""
    logger.info("Connect: deleted %s", connect.delete())
    postgres.drop_slot()  # only once the source connector has let go of it
    postgres.drop_tables()
    logger.info("PostgreSQL: dropped the slot and the tables")
    logger.info("Kafka: deleted %s", kafka.delete_topics())
    logger.info("Schema registry: deleted %s", kafka.delete_subjects())
    logger.info("S3: deleted %d files", s3.delete_files())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    logging.getLogger("kafka").setLevel(logging.WARNING)  # a line per connection
    cleanup()

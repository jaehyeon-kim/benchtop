"""Removes what the steps have written to the services, so the steps can be run again from the start.

It cancels the Flink job first, so it cannot write a model after the models are deleted.
Then it deletes the models from Valkey, the feedback topic and its schema. Each part is
skipped when it is already gone, so running it twice is safe.

Run: python -m recommender.stores.cleanup
"""

import logging

from recommender.core.config import setup_logging
from recommender.stores import flink, kafka, valkey

if __name__ == "__main__":
    setup_logging()
    logging.getLogger("httpx").setLevel(logging.WARNING)  # a line per request
    flink.cancel_job()
    valkey.delete_models()
    kafka.delete_topic()
    kafka.delete_subject()

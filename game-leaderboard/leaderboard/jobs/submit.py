"""Creates the leaderboard tables and submits the four Flink jobs.

Run: python -m leaderboard.jobs.submit
"""

import logging

from leaderboard.stores import flink, postgres

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    postgres.create()
    flink.submit()

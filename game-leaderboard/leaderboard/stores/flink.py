"""Submits the four leaderboard jobs in `jobs/` to Flink, and cancels them.

Each job file runs with `00-ddl.sql` as its init file, because odctl's Flink keeps table
definitions only for one SQL client session.

"""

import logging
import subprocess
from pathlib import Path

import httpx

from leaderboard.core.config import FLINK_CONTAINER, FLINK_REST, JOB_NAMES

logger = logging.getLogger(
    "leaderboard.stores.flink"
)  # __name__ is "__main__" under -m

# Flink's JDBC connector needs the OpenLineage client, which odctl's image holds but not
# in Flink's lib folder.
_OPENLINEAGE = "/mnt/shared-deps/connect/debezium-postgres/debezium-openlineage-core/openlineage-java-1.31.0.jar"  # fmt: skip
SQL_DIR = Path(__file__).parent.parent / "jobs"
_ENDED = {"FINISHED", "CANCELED", "FAILED"}


def submit() -> None:
    """Copies the SQL files into the Flink container and submits the four jobs."""
    exec_ = ["docker", "exec", FLINK_CONTAINER]
    subprocess.run([*exec_, "cp", _OPENLINEAGE, "/opt/flink/lib/"], check=True)
    subprocess.run(["docker", "cp", f"{SQL_DIR}/.", f"{FLINK_CONTAINER}:/tmp/game-sql"], check=True)  # fmt: skip
    for job in sorted(SQL_DIR.glob("0[1-4]-*.sql")):
        subprocess.run(
            [*exec_, "./bin/sql-client.sh", "-i", "/tmp/game-sql/00-ddl.sql", "-f", f"/tmp/game-sql/{job.name}"],
            check=True,
            capture_output=True,
        )  # fmt: skip
        logger.info("Submitted %s", job.stem)


def running(jobs: list[dict]) -> list[str]:
    """
    Picks this project's jobs that are still running from Flink's job list.

    Args:
        jobs (list[dict]): The `jobs` of Flink's `/jobs/overview`, each with a `jid`, a
            `name` and a `state`.

    Returns:
        list[str]: The ids of the jobs named in `JOB_NAMES` that have not ended.
    """
    return [
        j["jid"] for j in jobs if j["name"] in JOB_NAMES and j["state"] not in _ENDED
    ]


def cancel() -> None:
    """Cancels this project's running jobs, leaving other projects' jobs alone."""
    for jid in running(httpx.get(f"{FLINK_REST}/jobs/overview").json()["jobs"]):
        httpx.patch(f"{FLINK_REST}/jobs/{jid}", params={"mode": "cancel"})

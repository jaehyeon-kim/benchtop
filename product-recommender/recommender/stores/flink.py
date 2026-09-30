"""Cancels the Flink job through Flink's REST API."""

import json
import logging
import time
import urllib.request

from recommender.core.config import FLINK_JOB, FLINK_URL

logger = logging.getLogger(__name__)

_CANCEL_TIMEOUT = 60  # seconds to wait for Flink to cancel the job


def _call(path: str, method: str = "GET") -> dict:
    """Calls Flink's REST API and returns the JSON response."""
    request = urllib.request.Request(FLINK_URL + path, method=method)
    with urllib.request.urlopen(request) as r:
        return json.loads(r.read() or "{}")


def cancel_job() -> None:
    """Cancels every running `RecommenderParameterUpdate` job, and waits until each is cancelled."""
    running = [
        j["jid"]
        for j in _call("/jobs/overview")["jobs"]
        if j["name"] == FLINK_JOB
        and j["state"] not in ("CANCELED", "FAILED", "FINISHED")
    ]
    for jid in running:
        _call(f"/jobs/{jid}?mode=cancel", method="PATCH")
    deadline = time.monotonic() + _CANCEL_TIMEOUT
    while any(_call(f"/jobs/{jid}")["state"] != "CANCELED" for jid in running):
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"Flink did not cancel {FLINK_JOB} within {_CANCEL_TIMEOUT} seconds"
            )
        time.sleep(1)
    logger.info(f"Flink: cancelled {len(running)} {FLINK_JOB} job(s)")

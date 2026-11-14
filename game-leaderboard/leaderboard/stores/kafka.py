"""Deletes the Kafka topics and the schema subject that the game created."""

import httpx
from desflow import KafkaAdminConnector

from leaderboard.core.config import (
    BOOTSTRAP_SERVERS,
    CONTROL_TOPIC,
    SCHEMA_REGISTRY,
    SUBJECT,
    TELEMETRY_TOPIC,
    TOPIC,
)


def delete() -> None:
    """Deletes the three topics and the schema subject, if they exist."""
    KafkaAdminConnector(BOOTSTRAP_SERVERS).drop_topics(
        [TOPIC, CONTROL_TOPIC, TELEMETRY_TOPIC]
    )
    # A subject is deleted in two steps: marked deleted, then removed for good.
    subject = f"{SCHEMA_REGISTRY}/subjects/{SUBJECT}"
    if httpx.delete(subject).status_code == 200:
        httpx.delete(subject, params={"permanent": "true"})

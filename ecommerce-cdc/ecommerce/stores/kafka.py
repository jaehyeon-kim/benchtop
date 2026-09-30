"""The Kafka store: this project's topics, and their schemas in the schema registry."""

import json
import urllib.request

from kafka.admin import KafkaAdminClient

from ecommerce.core.config import CONTROL_TOPIC, KAFKA, SCHEMA_REGISTRY, TOPIC_PREFIX


def own_topics(topics: set[str]) -> list[str]:
    """
    Picks this project's topics from all the topics in Kafka.

    Args:
        topics (set[str]): Every topic's name.

    Returns:
        list[str]: Debezium's `ecommerce.` topics and the control topic, sorted.
    """
    return sorted(
        t for t in topics if t.startswith(f"{TOPIC_PREFIX}.") or t == CONTROL_TOPIC
    )


def delete_topics() -> list[str]:
    """
    Deletes this project's topics.

    Returns:
        list[str]: The topics deleted.
    """
    admin = KafkaAdminClient(bootstrap_servers=KAFKA)
    try:
        topics = own_topics(set(admin.list_topics()))
        if topics:
            admin.delete_topics(topics)
        return topics
    finally:
        admin.close()


def delete_subjects() -> list[str]:
    """
    Deletes the schemas the connectors registered for this project's topics.

    Each subject is deleted in two steps, as the registry requires: marked deleted,
    then removed for good.

    Returns:
        list[str]: The subjects deleted.
    """
    with urllib.request.urlopen(f"{SCHEMA_REGISTRY}/subjects") as response:
        subjects = [s for s in json.load(response) if s.startswith(f"{TOPIC_PREFIX}.")]
    for subject in subjects:
        for query in ("", "?permanent=true"):
            request = urllib.request.Request(
                f"{SCHEMA_REGISTRY}/subjects/{subject}{query}", method="DELETE"
            )
            urllib.request.urlopen(request).close()
    return subjects

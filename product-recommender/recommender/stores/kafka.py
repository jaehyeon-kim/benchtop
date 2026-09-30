"""Sends feedback events to Kafka in Avro, for the Flink job to learn from, and deletes the topic."""

import dataclasses
import logging
import time

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from recommender.core.config import BOOTSTRAP_SERVERS, FEEDBACK_TOPIC, SCHEMA_REGISTRY
from recommender.core.models import FEEDBACK_SCHEMA, FeedbackEvent

logger = logging.getLogger(__name__)


class FeedbackProducer:
    """Sends feedback events to the feedback topic, one at a time, keyed by product."""

    def __init__(self):
        """Connects to Kafka and the schema registry."""
        registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY})
        self._serialize = AvroSerializer(
            registry, FEEDBACK_SCHEMA, lambda obj, ctx: dataclasses.asdict(obj)
        )
        self._context = SerializationContext(FEEDBACK_TOPIC, MessageField.VALUE)
        self._producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    def send(self, event: FeedbackEvent) -> None:
        """
        Sends one event and waits until Kafka has it.

        Args:
            event (FeedbackEvent): The event.
        """
        self._producer.produce(
            topic=FEEDBACK_TOPIC,
            key=event.product_id.encode("utf-8"),
            value=self._serialize(event, self._context),
            on_delivery=_log_failure,
        )
        self._producer.flush()


def _log_failure(err, msg) -> None:
    """Logs a message Kafka could not take."""
    if err is not None:
        logger.error(f"Delivery failed for record {msg.key()}: {err}")


def delete_topic() -> None:
    """
    Deletes the feedback topic and waits until Kafka no longer lists it.

    The Flink job reads the topic from its first event, so a new job would otherwise
    train on the last run's feedback. The job creates the topic again when it starts.
    """
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    if FEEDBACK_TOPIC not in admin.list_topics(timeout=10).topics:
        logger.info(f"Kafka: {FEEDBACK_TOPIC} is already gone")
        return
    admin.delete_topics([FEEDBACK_TOPIC])[FEEDBACK_TOPIC].result()
    while FEEDBACK_TOPIC in admin.list_topics(timeout=10).topics:
        time.sleep(1)
    logger.info(f"Kafka: deleted {FEEDBACK_TOPIC}")


def delete_subject() -> None:
    """Deletes the feedback events' schema from the schema registry, soft then permanently."""
    registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY})
    subject = f"{FEEDBACK_TOPIC}-value"
    if subject not in registry.get_subjects():
        logger.info(f"Schema registry: {subject} is already gone")
        return
    registry.delete_subject(subject)
    registry.delete_subject(subject, permanent=True)
    logger.info(f"Schema registry: deleted {subject}")

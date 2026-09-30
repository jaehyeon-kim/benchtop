import json
import re
from pathlib import Path

from ecommerce.core.config import (
    BUCKET,
    REPLICATION_SLOT,
    S3_PREFIX,
    SCHEMA,
    TOPIC_PREFIX,
)
from ecommerce.core.models import TABLES

FOLDER = Path(__file__).parents[2] / "ecommerce" / "cdc"
SOURCE = json.loads((FOLDER / "source.json").read_text())
SINK = json.loads((FOLDER / "s3-sink.json").read_text())


def test_source_reads_the_odctl_database_through_the_existing_publication():
    """Verify that Debezium reads database odctl through cdc_pub with pgoutput, and never creates a publication."""
    assert SOURCE["database.dbname"] == "odctl"
    assert SOURCE["plugin.name"] == "pgoutput"
    assert SOURCE["publication.name"] == "cdc_pub"
    assert SOURCE["publication.autocreate.mode"] == "disabled"
    assert SOURCE["slot.name"] == REPLICATION_SLOT


def test_source_captures_exactly_this_projects_tables():
    """Verify that Debezium captures the six tables in the cdc schema and no others."""
    tables = SOURCE["table.include.list"].split(",")
    assert sorted(tables) == sorted(f"{SCHEMA}.{t}" for t in TABLES.values())
    assert SOURCE["topic.prefix"] == TOPIC_PREFIX


def test_both_connectors_use_avro_with_the_schema_registry():
    """Verify that both connectors read and write Avro, with the schemas in Karapace."""
    for config in (SOURCE, SINK):
        for side in ("key", "value"):
            assert (
                config[f"{side}.converter"] == "io.confluent.connect.avro.AvroConverter"
            )
            assert (
                config[f"{side}.converter.schema.registry.url"]
                == "http://karapace:8081"
            )


def test_sink_reads_only_this_projects_topics():
    """Verify that the sink's topic pattern matches this project's topics and no others."""
    pattern = re.compile(SINK["topics.regex"])
    assert all(
        pattern.fullmatch(f"{TOPIC_PREFIX}.{SCHEMA}.{t}") for t in TABLES.values()
    )
    assert not pattern.fullmatch("game.scores") and not pattern.fullmatch(
        "ecommerce-other"
    )


def test_sink_writes_under_this_projects_prefix():
    """Verify that the sink writes JSON lines to SeaweedFS under ecommerce-cdc/."""
    assert SINK["aws.s3.bucket.name"] == BUCKET
    assert SINK["aws.s3.endpoint"] == "http://seaweed:8333"
    assert SINK["file.name.template"].startswith(S3_PREFIX)
    assert SINK["format.output.type"] == "jsonl"

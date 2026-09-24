import pyarrow as pa

from airq.generator import Observation
from airq.iceberg import TABLES, arrow_schema


def test_schema_matches_each_model():
    """Every model field becomes a required column; timestamps are UTC."""
    for model in TABLES:
        schema = arrow_schema(model)
        assert schema.names == list(model.model_fields)
        assert not any(field.nullable for field in schema)
    assert arrow_schema(Observation).field("event_time").type == pa.timestamp(
        "us", tz="UTC"
    )

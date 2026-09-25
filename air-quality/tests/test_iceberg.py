import pyarrow as pa

from airq.config import TABLES
from airq.iceberg import arrow_schema
from airq.models import Observation


def test_schema_matches_each_model():
    """Every model field becomes a required column; timestamps are UTC."""
    for model in TABLES:
        schema = arrow_schema(model)
        assert schema.names == list(model.model_fields)
        assert not any(field.nullable for field in schema)
    assert arrow_schema(Observation).field("measured_at").type == pa.timestamp(
        "us", tz="UTC"
    )

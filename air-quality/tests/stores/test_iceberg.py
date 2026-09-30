import pyarrow as pa

from airq.core.config import TABLES
from airq.core.models import Observation, Prediction
from airq.stores.iceberg import arrow_schema


def test_schema_matches_each_model():
    """Verify that every model field becomes a required column, with timestamps in UTC."""
    for model in [*TABLES, Prediction]:
        schema = arrow_schema(model)
        assert schema.names == list(model.model_fields)
        assert not any(field.nullable for field in schema)
    assert arrow_schema(Observation).field("measured_at").type == pa.timestamp(
        "us", tz="UTC"
    )

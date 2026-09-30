import io
import json

import fastavro
from dynamic_des.connectors.egress.kafka import ConfluentAvroSerializer

from leaderboard.core.models import AVRO_SCHEMA, ScoreEvent
from leaderboard.stores.kafka import ScoreSerializer

EVENT = ScoreEvent(user_id="USR-000001", team_id="0000000001", team_name="Amber-Koala", score=7, event_time_millis=1, event_type="normal").model_dump()  # fmt: skip


def test_serializer_sends_only_the_event(monkeypatch):
    """Verify that the serializer encodes dynamic-des's `value` only, not its envelope."""
    sent = []
    monkeypatch.setattr(ConfluentAvroSerializer, "serialize", lambda self, topic, data: sent.append((topic, data)) or b"")  # fmt: skip
    ScoreSerializer().serialize("game-scores", {"sim_ts": 1.0, "timestamp": "t", "key": "k", "value": EVENT})  # fmt: skip
    assert sent == [("game-scores", EVENT)]


def test_avro_schema_matches_the_event_and_round_trips():
    """Verify that the Avro schema has the event's fields in order, and an event encodes and decodes unchanged."""
    schema = json.loads(AVRO_SCHEMA)
    assert [f["name"] for f in schema["fields"]] == list(ScoreEvent.model_fields)
    parsed = fastavro.parse_schema(schema)
    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, parsed, EVENT)
    buffer.seek(0)
    assert fastavro.schemaless_reader(buffer, parsed) == EVENT

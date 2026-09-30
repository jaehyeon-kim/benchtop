import json
import re

from leaderboard.app.reports import LEADERBOARDS
from leaderboard.core.config import JOB_NAMES, SCHEMA, TOPIC
from leaderboard.core.models import AVRO_SCHEMA
from leaderboard.stores.flink import SQL_DIR

SQL = SQL_DIR
DDL = (SQL / "00-ddl.sql").read_text()
JOBS = {p.name: p.read_text() for p in sorted(SQL.glob("0[1-4]-*.sql"))}
TABLES = (SQL / "tables.sql").read_text()


def _with_options(table: str) -> dict[str, str]:
    """Returns the WITH options of a Flink table in the job file."""
    body = re.search(rf"CREATE TABLE {table} \(.*?\) WITH \((.*?)\);", DDL, re.DOTALL)
    assert body, table
    return dict(re.findall(r"'([^']+)' = '([^']*)'", body[1]))


def _columns(sql: str, table: str) -> list[str]:
    """Returns the column names of a CREATE TABLE statement."""
    body = re.search(rf"CREATE TABLE (?:IF NOT EXISTS )?{table} \((.*?)\)\s*(?:WITH|;)", sql, re.DOTALL)  # fmt: skip
    assert body, table
    parts = [p.strip() for p in re.split(r",(?![^(]*\))", body[1])]
    return [p.split()[0] for p in parts if p and not p.startswith(("PRIMARY", "WATERMARK")) and " AS " not in p]  # fmt: skip


def test_each_leaderboard_is_its_own_named_job():
    """Verify that there are four job files, each naming its job from JOB_NAMES, which the clean-up cancels by."""
    names = [
        re.search(r"SET 'pipeline.name' = '([^']+)';", sql)[1] for sql in JOBS.values()
    ]
    assert names == list(JOB_NAMES)


def test_each_job_checkpoints_and_bounds_its_state():
    """Verify that each job checkpoints every 10 seconds, runs at parallelism 1, and sets a state TTL and mini-batching."""
    for name, sql in JOBS.items():
        settings = dict(re.findall(r"SET '([^']+)' = '([^']*)';", sql))
        assert settings["execution.checkpointing.interval"] == "10s", name
        assert settings["parallelism.default"] == "1", name
        assert settings["table.exec.mini-batch.enabled"] == "true", name
        assert "table.exec.state.ttl" in settings, name
        assert "CREATE TABLE" not in sql, name  # the tables come from 00-ddl.sql


def test_source_reads_the_topic_in_avro_through_karapace():
    """Verify that the source table reads TOPIC from odctl's broker in Avro through the schema registry."""
    options = _with_options("scores")
    assert options["connector"] == "kafka"
    assert options["topic"] == TOPIC
    assert options["properties.bootstrap.servers"] == "broker-1:19092"
    assert options["format"] == "avro-confluent"
    assert options["avro-confluent.url"] == "http://karapace:8081"


def test_source_columns_match_the_avro_schema():
    """Verify that the source table's columns are the Avro record's fields, in order."""
    fields = [f["name"] for f in json.loads(AVRO_SCHEMA)["fields"]]
    assert _columns(DDL, "scores") == fields


def test_every_leaderboard_has_a_jdbc_sink_to_its_postgres_table():
    """Verify that each leaderboard has a JDBC sink writing to its table in SCHEMA, keyed by rank."""
    for table in LEADERBOARDS:
        options = _with_options(table)
        assert options["connector"] == "jdbc"
        assert options["url"] == "jdbc:postgresql://postgres:5432/odctl"
        assert options["table-name"] == f"{SCHEMA}.{table}"
        assert sum(f"INSERT INTO {table}\n" in sql for sql in JOBS.values()) == 1


def test_sink_columns_match_the_postgres_tables():
    """Verify that each Flink sink has the same columns as its PostgreSQL table."""
    for table in LEADERBOARDS:
        assert _columns(DDL, table) == _columns(TABLES, f"{SCHEMA}.{table}")


def test_dashboard_reads_columns_the_tables_have():
    """Verify that the dashboard's label and value columns exist in each PostgreSQL table."""
    for table, (_, label, value) in LEADERBOARDS.items():
        assert {label, value} <= set(_columns(TABLES, f"{SCHEMA}.{table}"))

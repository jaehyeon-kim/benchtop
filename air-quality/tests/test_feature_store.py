import pandas as pd

from airq.feature_store import FEATURE_SETS, VERSION_OBJECTS, calendar_v2


def test_calendar_view_flags_saturday_and_sunday():
    """Verify that the calendar view flags Saturday and Sunday and no other day."""
    days = pd.DataFrame({"day": pd.to_datetime(["2026-09-25", "2026-09-26", "2026-09-27", "2026-09-28"], utc=True)})  # fmt: skip
    flags = calendar_v2.feature_transformation.udf(days)
    assert list(flags["is_weekend"]) == [0, 1, 1, 0]


def test_v2_adds_the_weekend_flag_to_v1():
    """Verify that v2's features are v1's features plus the weekend flag."""
    v1_refs, v1_columns = FEATURE_SETS["v1"]
    v2_refs, v2_columns = FEATURE_SETS["v2"]
    assert v2_refs[: len(v1_refs)] == v1_refs
    assert v2_columns == [*v1_columns, "is_weekend"]


def test_v1_registers_only_the_weather_view():
    """Verify that v1's Feast objects include weather_v1 and not calendar_v2."""
    names = {o.name for o in VERSION_OBJECTS["v1"]}
    assert "weather_v1" in names and "calendar_v2" not in names


def test_v2_registers_both_views():
    """Verify that v2's Feast objects include both feature views."""
    assert {"weather_v1", "calendar_v2"} <= {o.name for o in VERSION_OBJECTS["v2"]}

import pandas as pd

from airq.feast_repo import FEATURE_SETS, calendar_v2


def test_calendar_view_flags_saturday_and_sunday():
    days = pd.DataFrame({"day": pd.to_datetime(["2026-09-25", "2026-09-26", "2026-09-27", "2026-09-28"], utc=True)})  # fmt: skip
    flags = calendar_v2.feature_transformation.udf(days)
    assert list(flags["is_weekend"]) == [0, 1, 1, 0]


def test_v2_adds_the_weekend_flag_to_v1():
    v1_refs, v1_columns = FEATURE_SETS["v1"]
    v2_refs, v2_columns = FEATURE_SETS["v2"]
    assert v2_refs[: len(v1_refs)] == v1_refs
    assert v2_columns == [*v1_columns, "is_weekend"]

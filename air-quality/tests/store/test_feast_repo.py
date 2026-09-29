import runpy
import sys

import feast
import pandas as pd
import pytest

from airq.store.feast_repo import FEATURE_SETS, calendar_v2


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


def _apply(monkeypatch, version):
    applied = []

    class FakeStore:
        def __init__(self, config):
            pass

        def apply(self, objects):
            applied.extend(o.name for o in objects)

    monkeypatch.setattr(feast, "FeatureStore", FakeStore)
    monkeypatch.setenv("FEAST_REGISTRY", "sqlite://")
    monkeypatch.setattr(sys, "argv", ["airq.store", "--version", version])
    runpy.run_module("airq.store", run_name="__main__", alter_sys=True)
    return set(applied)


# This file already imported feast_repo, so runpy warns when it runs it again.
@pytest.mark.filterwarnings("ignore:.*found in sys.modules:RuntimeWarning")
def test_python_m_airq_store_v1_applies_only_the_weather_view(monkeypatch):
    """Verify that `python -m airq.store --version v1` applies weather_v1 and not calendar_v2."""
    applied = _apply(monkeypatch, "v1")
    assert "weather_v1" in applied and "calendar_v2" not in applied


@pytest.mark.filterwarnings("ignore:.*found in sys.modules:RuntimeWarning")
def test_python_m_airq_store_v2_applies_both_views(monkeypatch):
    """Verify that `python -m airq.store --version v2` applies both feature views."""
    assert {"weather_v1", "calendar_v2"} <= _apply(monkeypatch, "v2")

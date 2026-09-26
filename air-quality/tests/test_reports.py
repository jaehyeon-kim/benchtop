from datetime import date

import pandas as pd

from airq import reports


def _predictions():
    return pd.DataFrame(
        {"location_id": "station-1",
         "as_of": [date(2026, 9, 20)] * 2 + [date(2026, 9, 22)] * 2,
         "day": [date(2026, 9, 21), date(2026, 9, 22), date(2026, 9, 23), date(2026, 9, 24)],
         "lead_days": [1, 2, 1, 2], "pm2_5": [10.0, 11.0, 12.0, 13.0], "model_version": "3"}
    )  # fmt: skip


def test_forecast_is_the_latest_run_on_or_before_the_date(monkeypatch):
    monkeypatch.setattr(reports, "_scan", lambda identifier: _predictions())
    monkeypatch.setattr(reports, "served_models", lambda: [{"alias": "champion", "version": "3", "feature_set": "v1"}])  # fmt: skip
    assert set(reports.forecast(date(2026, 9, 21))["as_of"]) == {date(2026, 9, 20)}
    latest = reports.forecast()
    assert set(latest["as_of"]) == {date(2026, 9, 22)}
    assert set(latest["alias"]) == {"champion"}
    assert reports.forecast(date(2026, 9, 1)).empty


def test_lower_names_the_smaller_error_and_handles_a_missing_model():
    assert reports._lower(1.8, 0.8) == "challenger"
    assert reports._lower(0.7, 0.8) == "champion"
    assert reports._lower(0.8, 0.8) == "equal"
    assert reports._lower(1.0, float("nan")) == "champion"


def test_a_version_holding_both_aliases_appears_under_each(monkeypatch):
    served = [{"alias": "champion", "version": "6", "feature_set": "v2"},
              {"alias": "challenger", "version": "6", "feature_set": "v2"}]  # fmt: skip
    monkeypatch.setattr(reports, "served_models", lambda: served)
    rows = reports._with_alias(pd.DataFrame({"model_version": ["6", "5"]}))
    assert sorted(rows[rows["model_version"] == "6"]["alias"]) == [
        "challenger",
        "champion",
    ]
    assert list(rows[rows["model_version"] == "5"]["alias"]) == [""]


def test_a_missing_table_reads_as_no_rows(monkeypatch):
    """Straight after a backfill there is no predictions table yet."""

    class NoTables:
        def table_exists(self, identifier):
            return False

    monkeypatch.setattr(reports, "catalog", lambda: NoTables())
    monkeypatch.setattr(reports, "served_models", list)
    empty = reports._scan("airq.predictions")
    assert empty.empty and "model_version" in empty.columns
    assert reports.forecast().empty

from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

import pandas as pd

from airq.core.config import LEADS, STATION
from airq.inference.infer import _entity_rows, _feature_request, _predictions, errors


def test_entity_rows_ask_for_the_forecast_issued_on_the_as_of_date():
    """Verify that every entity row asks for a forecast issued on the as-of date."""
    rows = _entity_rows(date(2026, 9, 20))
    assert list(rows.lead_days) == list(LEADS)
    assert (rows.location_id == STATION).all()
    issued = [
        ts.date() - timedelta(days=int(n))
        for ts, n in zip(rows.event_timestamp, rows.lead_days)
    ]
    assert set(issued) == {date(2026, 9, 20)}
    assert rows.event_timestamp.iloc[0] == datetime(2026, 9, 21, tzinfo=UTC)


def test_predictions_carry_the_day_lead_and_model_version():
    """Verify that each prediction carries its day, lead and model version, rounded to two decimals."""
    features = _entity_rows(date(2026, 9, 20))
    rows = _predictions(date(2026, 9, 20), features, [10.123] * 7, "3")
    assert [r.day for r in rows][-1] == date(2026, 9, 27)
    assert {r.model_version for r in rows} == {"3"}
    assert rows[0].pm2_5 == 10.12


def test_errors_cover_only_days_with_a_reading():
    """Verify that errors are computed only for days that have a reading."""
    predictions = pd.DataFrame(
        {"location_id": STATION, "as_of": date(2026, 9, 20), "lead_days": [1, 2],
         "day": [date(2026, 9, 21), date(2026, 9, 22)], "pm2_5": [10.0, 12.0]}
    )  # fmt: skip
    observed = pd.DataFrame(
        {"location_id": [STATION], "day": [date(2026, 9, 21)], "pm2_5": [13.0]}
    )
    joined = errors(predictions, observed)
    assert list(joined.lead_days) == [1]
    assert joined.abs_error.iloc[0] == 3.0


def test_v1_alone_requests_only_the_weather_view():
    """Verify that with only v1 served, inference asks Feast for no calendar_v2 feature."""
    references, columns = _feature_request(
        [SimpleNamespace(tags={"feature_set": "v1"})]
    )
    assert references and all(r.startswith("weather_v1:") for r in references)
    assert "is_weekend" not in columns


def test_v1_and_v2_request_each_feature_once():
    """Verify that with v1 and v2 served, each feature is requested once."""
    served = [
        SimpleNamespace(tags={"feature_set": "v1"}),
        SimpleNamespace(tags={"feature_set": "v2"}),
    ]
    references, columns = _feature_request(served)
    assert "calendar_v2:is_weekend" in references
    assert len(references) == len(set(references)) and columns[-1] == "is_weekend"

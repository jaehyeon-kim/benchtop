from datetime import UTC, date, datetime, timedelta

import pandas as pd

from airq.config import LEADS, STATION
from airq.infer import _entity_rows, _errors, _predictions


def test_entity_rows_ask_for_the_forecast_issued_on_the_as_of_date():
    """Lead N on day D+N: every row is the forecast made on the as-of date."""
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
    features = _entity_rows(date(2026, 9, 20))
    rows = _predictions(date(2026, 9, 20), features, [10.123] * 7, "3")
    assert [r.day for r in rows][-1] == date(2026, 9, 27)
    assert {r.model_version for r in rows} == {"3"}
    assert rows[0].pm2_5 == 10.12


def test_errors_cover_only_days_with_a_reading():
    predictions = pd.DataFrame(
        {"location_id": STATION, "as_of": date(2026, 9, 20), "lead_days": [1, 2],
         "day": [date(2026, 9, 21), date(2026, 9, 22)], "pm2_5": [10.0, 12.0]}
    )  # fmt: skip
    observed = pd.DataFrame(
        {"location_id": [STATION], "day": [date(2026, 9, 21)], "pm2_5": [13.0]}
    )
    errors = _errors(predictions, observed)
    assert list(errors.lead_days) == [1]
    assert errors.abs_error.iloc[0] == 3.0

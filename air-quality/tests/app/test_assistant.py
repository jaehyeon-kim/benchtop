from datetime import date

import pandas as pd
from strands.models.ollama import OllamaModel

from airq.app import assistant, reports
from airq.app.assistant import _model
from airq.core.config import OLLAMA_HOST

SATURDAY = date(2026, 9, 26)


def test_model_is_the_named_local_ollama_model():
    """Verify that the assistant uses the named model on the local Ollama host."""
    model = _model("qwen3:4b-instruct")
    assert isinstance(model, OllamaModel)
    assert model.config["model_id"] == "qwen3:4b-instruct"
    assert model.host == OLLAMA_HOST


def test_forecast_text_picks_the_model_and_day(monkeypatch):
    """Verify that the forecast text shows the chosen model and day, and names a day the forecast does not cover."""
    rows = pd.DataFrame(
        {"as_of": SATURDAY, "day": [date(2026, 9, 27), date(2026, 9, 27)],
         "lead_days": 1, "pm2_5": [9.11, 6.5], "model_version": ["5", "6"],
         "alias": ["champion", "challenger"]}
    )  # fmt: skip
    monkeypatch.setattr(reports, "forecast", lambda as_of=None: rows)
    text = assistant.forecast_text("tomorrow", "challenger", SATURDAY)
    assert "model version 6" in text and "2026-09-27 (Sunday): 6.5" in text
    assert "not 2026-09-29" in assistant.forecast_text(
        "in 3 days", "champion", SATURDAY
    )


def test_observed_text_works_out_the_highest(monkeypatch):
    """Verify that the text for a range of readings names the highest one and its day."""
    rows = pd.DataFrame({"day": [date(2026, 9, 24), date(2026, 9, 25)], "pm2_5": [10.0, 13.76]})  # fmt: skip
    monkeypatch.setattr(reports, "observed", lambda start, end: rows)
    text = assistant.observed_text("2 days ago", "yesterday", SATURDAY)
    assert "Highest: 13.76 µg/m³ on 2026-09-25 (Friday)." in text


def test_model_error_text_names_the_lower_model(monkeypatch):
    """Verify that the error text says which model has the lower error at every lead."""
    table = pd.DataFrame(
        {"lead_days": [1, 2], "champion_mae": [1.81, 2.06],
         "challenger_mae": [0.8, 1.01], "lower_error": ["challenger", "challenger"]}
    )  # fmt: skip
    monkeypatch.setattr(reports, "error_by_lead", lambda days: table)
    assert (
        "The challenger has the lower error at every lead."
        in assistant.model_error_text(30)
    )


def test_observed_text_names_a_missing_day_once(monkeypatch):
    """Verify that a day with no reading is named once, with the last day that has one."""
    monkeypatch.setattr(reports, "observed", lambda start, end: pd.DataFrame(columns=["day", "pm2_5"]))  # fmt: skip
    monkeypatch.setattr(reports, "last_measured_day", lambda: date(2026, 9, 25))
    text = assistant.observed_text("tomorrow", "", SATURDAY)
    assert (
        text
        == "No reading for 2026-09-27 (Sunday). Readings run to 2026-09-25 (Friday)."
    )


def test_compare_text_names_the_higher_day(monkeypatch):
    """Verify that the comparison gives both values and names the higher day, and names a day the forecast does not cover."""
    rows = pd.DataFrame(
        {"as_of": SATURDAY, "day": [date(2026, 9, 27), date(2026, 10, 3)],
         "lead_days": [1, 7], "pm2_5": [9.13, 15.92], "model_version": "5", "alias": "champion"}
    )  # fmt: skip
    monkeypatch.setattr(reports, "forecast", lambda as_of=None: rows)
    text = assistant.compare_text("next saturday", "tomorrow", "champion", SATURDAY)
    assert text.endswith("2026-10-03 (Saturday) is higher than 2026-09-27 (Sunday), by 6.79 µg/m³.")  # fmt: skip
    assert "not 2026-09-28 (Monday)" in assistant.compare_text("monday", "tomorrow", "champion", SATURDAY)  # fmt: skip


def test_observed_text_covers_the_last_days(monkeypatch):
    """Verify that the last N days run from N days ago to yesterday."""
    asked = {}
    monkeypatch.setattr(reports, "observed", lambda start, end: asked.update(start=start, end=end) or pd.DataFrame(columns=["day", "pm2_5"]))  # fmt: skip
    monkeypatch.setattr(reports, "last_measured_day", lambda: date(2026, 9, 25))
    for phrase in ("last 7 days", "past seven days", "last_7_days"):
        assistant.observed_text(phrase, "", SATURDAY)
        assert asked == {"start": date(2026, 9, 19), "end": date(2026, 9, 25)}

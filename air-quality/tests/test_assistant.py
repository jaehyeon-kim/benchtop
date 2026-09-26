from datetime import date

import pandas as pd
import pytest
from strands.models.ollama import OllamaModel

from airq import assistant, reports
from airq.assistant import _model
from airq.config import OLLAMA_HOST
from airq.days import resolve

SATURDAY = date(2026, 9, 26)


def test_model_is_the_named_local_ollama_model():
    model = _model("qwen3:4b-instruct")
    assert isinstance(model, OllamaModel)
    assert model.config["model_id"] == "qwen3:4b-instruct"
    assert model.host == OLLAMA_HOST


@pytest.mark.parametrize(
    ("phrase", "forward", "expected"),
    [
        ("today", True, date(2026, 9, 26)),
        ("Tomorrow", True, date(2026, 9, 27)),
        ("yesterday", False, date(2026, 9, 25)),
        ("3 days ago", False, date(2026, 9, 23)),
        ("in 2 days", True, date(2026, 9, 28)),
        ("5 days ahead", True, date(2026, 10, 1)),
        ("monday", True, date(2026, 9, 28)),
        ("monday", False, date(2026, 9, 21)),
        ("saturday", True, SATURDAY),
        ("next saturday", True, date(2026, 10, 3)),
        ("last saturday", False, date(2026, 9, 19)),
        ("2026-09-20", False, date(2026, 9, 20)),
    ],
)
def test_resolve_day(phrase, forward, expected):
    assert resolve(phrase, SATURDAY, forward) == expected


def test_unreadable_day_raises():
    with pytest.raises(ValueError):
        resolve("the other day", SATURDAY)


def test_forecast_text_picks_the_model_and_day(monkeypatch):
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
    rows = pd.DataFrame({"day": [date(2026, 9, 24), date(2026, 9, 25)], "pm2_5": [10.0, 13.76]})  # fmt: skip
    monkeypatch.setattr(reports, "observed", lambda start, end: rows)
    text = assistant.observed_text("2 days ago", "yesterday", SATURDAY)
    assert "Highest: 13.76 µg/m³ on 2026-09-25 (Friday)." in text


def test_model_error_text_names_the_lower_model(monkeypatch):
    table = pd.DataFrame(
        {"lead_days": [1, 2], "champion_mae": [1.81, 2.06],
         "challenger_mae": [0.8, 1.01], "lower_error": ["challenger", "challenger"]}
    )  # fmt: skip
    monkeypatch.setattr(reports, "error_by_lead", lambda days: table)
    assert (
        "The challenger has the lower error at every lead."
        in assistant.model_error_text(30)
    )


@pytest.mark.parametrize(
    ("phrase", "forward", "expected"),
    [
        ("three days ago", False, date(2026, 9, 23)),
        ("a week ago", False, date(2026, 9, 19)),
        ("day before yesterday", False, date(2026, 9, 24)),
        ("this weekend", True, SATURDAY),
        ("last weekend", False, date(2026, 9, 19)),
        ("20 September", False, date(2026, 9, 20)),
    ],
)
def test_resolve_wider_phrases(phrase, forward, expected):
    assert resolve(phrase, SATURDAY, forward) == expected

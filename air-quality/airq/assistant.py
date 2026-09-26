"""Forecast assistant: a Strands agent answering questions about the PM2.5 forecast.
The app's Assistant tab (airq.app) chats with it.

Three tools, over the same queries the Monitoring tab draws from: `get_forecast`,
`get_observed` and `get_model_error`. The model is a local Ollama model, named
by `AIRQ_MODEL`, so nothing leaves the machine.

The tools are built for a small model. They take days in the user's own words
("tomorrow", "3 days ago", "saturday") and work out the date in Python, and they
return a few short lines with any comparison already made, so the model only
copies numbers.
"""

from datetime import date

from strands import Agent, tool
from strands.models.ollama import OllamaModel

from airq import reports
from airq.config import ASSISTANT_MODEL, CHALLENGER, CHAMPION, OLLAMA_HOST
from airq.days import FORMS, WEEKDAYS, resolve
from airq.days import today as utc_today

_PROMPT = """You answer questions about the daily PM2.5 forecast for one simulated
air quality station. Today is {today} ({weekday}, UTC). PM2.5 is in µg/m³.

- Always call a tool before answering, and give only numbers a tool returned,
  copied exactly.
- Pass days to the tools in the user's own words, such as "tomorrow",
  "3 days ago" or "saturday". Do not work out dates yourself.
- The champion is the model in use; the challenger is predicted beside it for
  comparison. Say which model and forecast date a prediction came from.
- Answer in one or two full sentences that answer the question directly,
  naming the day and the value. For a yes or no question, start with yes or no.
- Answer only what was asked. If a tool says there is no data, say so."""


def _label(day: date) -> str:
    return f"{day.isoformat()} ({WEEKDAYS[day.weekday()].capitalize()})"


def forecast_text(day: str, model: str, today: date) -> str:
    rows = reports.forecast(today)
    rows = rows[rows["alias"] == model] if not rows.empty else rows
    if rows.empty:
        return f"No {model} forecast has been made yet."
    head = f"{model.capitalize()} (model version {rows['model_version'].iloc[0]}), forecast made on {rows['as_of'].iloc[0]}:"  # fmt: skip
    if day:
        target = resolve(day, today, forward=True)
        chosen = rows[rows["day"] == target]
        if chosen.empty:
            return f"{head} it covers {_label(rows['day'].min())} to {_label(rows['day'].max())}, not {_label(target)}."  # fmt: skip
        rows = chosen
    lines = [f"{_label(r.day)}: {r.pm2_5} µg/m³" for r in rows.itertuples()]
    return "\n".join([head, *lines])


def observed_text(day: str, until: str, today: date) -> str:
    start = resolve(day or "yesterday", today)
    end = resolve(until, today) if until else start
    start, end = min(start, end), max(start, end)
    rows = reports.observed(start, end)
    if rows.empty:
        last = reports.last_measured_day()
        if last is None:
            return "No readings have been measured yet."
        return f"No reading for {_label(start)} to {_label(end)}. Readings run to {_label(last)}."  # fmt: skip
    lines = [f"{_label(r.day)}: {r.pm2_5} µg/m³" for r in rows.itertuples()]
    if len(rows) > 1:
        high = rows.loc[rows["pm2_5"].idxmax()]
        low = rows.loc[rows["pm2_5"].idxmin()]
        lines += [
            f"Highest: {high.pm2_5} µg/m³ on {_label(high.day)}.",
            f"Lowest: {low.pm2_5} µg/m³ on {_label(low.day)}.",
            f"Mean: {round(float(rows['pm2_5'].mean()), 2)} µg/m³.",
        ]
    return "\n".join(["Measured daily mean PM2.5:", *lines])


def model_error_text(days: int) -> str:
    table = reports.error_by_lead(days)
    if table.empty:
        return "No predictions have been scored yet."
    lines = [
        f"{r.lead_days} day(s) ahead: champion {r.champion_mae}, "
        f"challenger {r.challenger_mae}, lower error: {r.lower_error}"
        for r in table.itertuples()
    ]
    lower = set(table["lower_error"])
    if len(lower) == 1:
        lines.append(f"The {lower.pop()} has the lower error at every lead.")
    return "\n".join([f"Mean absolute error in µg/m³ over the last {days} measured days:", *lines])  # fmt: skip


def _safely(make) -> str:
    try:
        return make()
    except ValueError:
        return f"Could not read the day. Use {FORMS}."


@tool
def get_forecast(day: str = "", model: str = CHAMPION) -> str:
    """The latest PM2.5 forecast: one day's prediction, or the next seven days.

    Args:
        day: the day predicted, in the user's words: "tomorrow", "in 3 days",
            "saturday" or YYYY-MM-DD. Leave empty for all seven days.
        model: "champion" (the model in use, the default) or "challenger".
    """
    model = model if model in (CHAMPION, CHALLENGER) else CHAMPION
    return _safely(lambda: forecast_text(day, model, utc_today()))


@tool
def get_observed(day: str = "", until: str = "") -> str:
    """Measured daily mean PM2.5 for one day, or for a range with the highest,
    lowest and mean already worked out.

    Args:
        day: the day, or the first day of a range, in the user's words:
            "yesterday", "3 days ago", "last monday" or YYYY-MM-DD.
        until: the last day of a range, in the same forms; empty for one day.
    """
    return _safely(lambda: observed_text(day, until, utc_today()))


@tool
def get_model_error(days: int = 30) -> str:
    """How accurate the champion and the challenger have been: mean absolute
    error by lead (days ahead), and which model has the lower error.

    Args:
        days: how many recent measured days to score, default 30.
    """
    return model_error_text(days)


def _model(model_id: str = ASSISTANT_MODEL) -> OllamaModel:
    return OllamaModel(host=OLLAMA_HOST, model_id=model_id)


def build(**kwargs) -> Agent:
    """A new agent, holding its own conversation."""
    today = utc_today()
    weekday = WEEKDAYS[today.weekday()].capitalize()
    return Agent(
        model=_model(),
        tools=[get_forecast, get_observed, get_model_error],
        system_prompt=_PROMPT.format(today=today, weekday=weekday),
        **kwargs,
    )

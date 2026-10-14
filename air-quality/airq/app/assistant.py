"""Forecast assistant: a Strands agent that answers questions about the PM2.5 forecast.

The Assistant tab in `airq.app.ui` chats with it. The model runs locally in Ollama,
and `AIRQ_MODEL` names it, so no data leaves the machine.

The agent has four tools: `get_forecast`, `compare_forecast`, `get_observed` and
`get_model_error`.
They use the same queries as the Monitoring tab, so the answers match the charts.

The tools are written for a small model. They accept days in the user's own words,
such as "tomorrow" or "saturday", and work out the date in Python. They return a
few short lines with any comparison already made, so the model only copies numbers.
"""

import re
from datetime import date, timedelta

from strands import Agent, tool
from strands.models.ollama import OllamaModel

from airq.app import reports
from airq.core.config import ASSISTANT_MODEL, CHALLENGER, CHAMPION, OLLAMA_HOST
from airq.core.days import FORMS, resolve
from airq.core.days import today as utc_today

_PROMPT = """You answer questions about the daily PM2.5 forecast for one simulated
air quality station. Today is {today} ({weekday}, UTC). PM2.5 is in µg/m³.

- Always call a tool before answering, and give only numbers a tool returned,
  copied exactly.
- Pass days to the tools in the user's own words, such as "tomorrow",
  "3 days ago" or "saturday". Do not work out dates yourself.
- Give a forecast without naming the model. Mention the champion (the model in
  use) or the challenger (predicted beside it for comparison) only when the user
  asks about models, accuracy or a comparison.
- Answer in one or two full sentences that answer the question directly,
  naming the day and the value. For a yes or no question, start with yes or no.
- Answer only what was asked. If a tool says there is no data, say so."""


def _label(day: date) -> str:
    """
    Formats a day for the model, with its weekday.

    Args:
        day (date): The day to format.

    Returns:
        str: The date and its weekday, such as "2026-09-26 (Saturday)".
    """
    return f"{day.isoformat()} ({day:%A})"


def _latest(model: str, today: date):
    """
    Returns one model's latest forecast made by `today`.

    Args:
        model (str): The alias of the model, "champion" or "challenger".
        today (date): The date the forecast must have been made by.

    Returns:
        pd.DataFrame: One row per day predicted. Empty if the model has no forecast.
    """
    rows = reports.forecast(today)
    return rows[rows["alias"] == model] if not rows.empty else rows


def forecast_text(day: str, model: str, today: date) -> str:
    """
    Describes the latest forecast of one model, for one day or for all seven days.

    Args:
        day (str): The day predicted, in the user's words. Empty for all seven days.
        model (str): The alias of the model, "champion" or "challenger".
        today (date): The date that phrases such as "tomorrow" count from.

    Returns:
        str: A heading with the model version and forecast date, then one line per
            day. If the forecast does not cover `day`, the days it does cover.

    Raises:
        ValueError: If `day` cannot be read as a day.
    """
    rows = _latest(model, today)
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


def compare_text(day: str, other: str, model: str, today: date) -> str:
    """
    Compares one model's latest forecast for two days, and says which is higher.

    A small model gets such comparisons wrong when it makes them itself, so the
    answer states the result.

    Args:
        day (str): The first day, in the user's words.
        other (str): The day to compare it with, in the same forms.
        model (str): The alias of the model, "champion" or "challenger".
        today (date): The date that phrases such as "tomorrow" count from.

    Returns:
        str: Both values and which day is higher. If the forecast does not cover a
            day, the days it does cover.

    Raises:
        ValueError: If `day` or `other` cannot be read as a day.
    """
    rows = _latest(model, today)
    if rows.empty:
        return f"No {model} forecast has been made yet."
    first, second = resolve(day, today, forward=True), resolve(other, today, forward=True)  # fmt: skip
    values = dict(zip(rows["day"], rows["pm2_5"]))
    missing = [_label(d) for d in (first, second) if d not in values]
    if missing:
        return f"The forecast covers {_label(rows['day'].min())} to {_label(rows['day'].max())}, not {' or '.join(missing)}."  # fmt: skip
    a, b = values[first], values[second]
    if a == b:
        result = "They are the same."
    else:
        high, low = (first, second) if a > b else (second, first)
        result = f"{_label(high)} is higher than {_label(low)}, by {round(abs(a - b), 2)} µg/m³."  # fmt: skip
    return f"{_label(first)}: {a} µg/m³. {_label(second)}: {b} µg/m³. {result}"


_NUMBERS = {w: n for n, w in enumerate(["one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"], 1)}  # fmt: skip


def _last_days(phrase: str) -> int:
    """
    Reads how many days a phrase such as "last 7 days" or "past_seven_days" covers.

    Args:
        phrase (str): The day phrase a tool received.

    Returns:
        int: The number of days, or 0 if the phrase is not of that form.
    """
    words = re.sub(r"[_\s]+", " ", phrase.lower()).strip()
    match = re.fullmatch(r"(?:the )?(?:last|past) (\w+) days?", words)
    if not match:
        return 0
    return int(match[1]) if match[1].isdigit() else _NUMBERS.get(match[1], 0)


def observed_text(day: str, until: str, today: date) -> str:
    """
    Describes the measured daily mean PM2.5 for one day or for a range of days.

    A range also gets its highest, lowest and mean values.

    Args:
        day (str): The day, or the first day of a range, in the user's words.
            Empty for yesterday.
        until (str): The last day of a range, in the same forms. Empty for one day.
        today (date): The date that phrases such as "yesterday" count from.

    Returns:
        str: One line per measured day, then the summary lines for a range. If
            nothing was measured, the last day that has a reading.

    Raises:
        ValueError: If `day` or `until` cannot be read as a day.
    """
    if n := _last_days(day):  # "last 7 days": the seven days up to yesterday
        start, end = today - timedelta(days=n), today - timedelta(days=1)
    else:
        start = resolve(day or "yesterday", today)
        end = resolve(until, today) if until else start
    start, end = min(start, end), max(start, end)
    rows = reports.observed(start, end)
    if rows.empty:
        last = reports.last_measured_day()
        if last is None:
            return "No readings have been measured yet."
        days = _label(start) if start == end else f"{_label(start)} to {_label(end)}"
        return f"No reading for {days}. Readings run to {_label(last)}."
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
    """
    Describes the error of the champion and the challenger for each lead.

    Args:
        days (int): How many recent measured days to score.

    Returns:
        str: One line per lead with both mean absolute errors and the lower one,
            and a closing line when one model is lower at every lead.
    """
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
    """
    Runs a text function and turns an unreadable day into a message for the model.

    Args:
        make (Callable[[], str]): The function that builds the tool's answer.

    Returns:
        str: The answer, or a message that lists the day forms the tools accept.
    """
    try:
        return make()
    except ValueError:
        return f"Could not read the day. Use {FORMS}."


@tool
def get_forecast(day: str = "", model: str = CHAMPION) -> str:
    """
    Gets the latest PM2.5 forecast, for one day or for all seven days ahead.

    Args:
        day (str): The day to forecast, in the user's words, such as "tomorrow",
            "in 3 days", "saturday" or YYYY-MM-DD. Leave empty for all seven days.
        model (str): "champion" for the model in use (the default), or "challenger".
    """
    model = model if model in (CHAMPION, CHALLENGER) else CHAMPION
    return _safely(lambda: forecast_text(day, model, utc_today()))


@tool
def compare_forecast(day: str, other: str, model: str = CHAMPION) -> str:
    """
    Compares the latest PM2.5 forecast for two days, and says which day is higher.

    Use it for any question that compares two days, such as "will Saturday be higher
    than tomorrow?".

    Args:
        day (str): The first day, in the user's words, such as "saturday" or
            "tomorrow".
        other (str): The day to compare it with, in the same forms.
        model (str): "champion" for the model in use (the default), or "challenger".
    """
    model = model if model in (CHAMPION, CHALLENGER) else CHAMPION
    return _safely(lambda: compare_text(day, other, model, utc_today()))


@tool
def get_observed(day: str = "", until: str = "") -> str:
    """
    Gets the measured daily mean PM2.5 for one day or for a range of days.

    A range also gets its highest, lowest and mean values.

    Args:
        day (str): The day, or the first day of a range, in the user's words, such
            as "yesterday", "last monday" or YYYY-MM-DD. For a recent range, pass
            it as "last 7 days" and leave `until` empty.
        until (str): The last day of a range, in the same forms. Leave empty for
            one day.
    """
    return _safely(lambda: observed_text(day, until, utc_today()))


@tool
def get_model_error(days: int = 30) -> str:
    """
    Gets how accurate the champion and the challenger have been.

    The error is the mean absolute error for each number of days ahead, with the
    model that has the lower error named.

    Args:
        days (int): How many recent measured days to score. The default is 30.
    """
    return model_error_text(days)


def _model(model_id: str = ASSISTANT_MODEL) -> OllamaModel:
    """
    Connects to a model served by the local Ollama server.

    Args:
        model_id (str): The Ollama model name. Defaults to `AIRQ_MODEL`.

    Returns:
        OllamaModel: The model for a Strands agent.
    """
    return OllamaModel(host=OLLAMA_HOST, model_id=model_id)


def build(**kwargs) -> Agent:
    """
    Builds a new agent with its own conversation.

    The system prompt names today's date and weekday in UTC, so the model can say
    which day an answer is about.

    Args:
        **kwargs: Passed on to `Agent`, such as `callback_handler`.

    Returns:
        Agent: The agent, with the four tools.
    """
    today = utc_today()
    return Agent(
        model=_model(),
        tools=[get_forecast, compare_forecast, get_observed, get_model_error],
        system_prompt=_PROMPT.format(today=today, weekday=f"{today:%A}"),
        **kwargs,
    )

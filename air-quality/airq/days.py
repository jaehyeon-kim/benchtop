"""Day phrases turned into a date in UTC: "yesterday", "3 days ago", "a week ago",
"day before yesterday", "last monday", "this weekend", "20 September" or
YYYY-MM-DD. The command-line tools, the Airflow DAG parameters and the forecast
assistant all accept them, so a command never needs a date written out.

The common forms are read here; anything else goes to the dateparser library,
which reads many more, such as "three days ago" or "2 weeks ago".
"""

import argparse
import re
from datetime import UTC, date, datetime, timedelta

WEEKDAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]  # fmt: skip
FORMS = 'a phrase such as yesterday, "3 days ago", "a week ago", "last monday", "this weekend" or "20 September", or YYYY-MM-DD'  # fmt: skip


def today() -> date:
    return datetime.now(UTC).date()


def resolve(text: str, on: date | None = None, forward: bool = False) -> date:
    """The date a phrase names, counted from `on` (default: today, UTC). A bare
    weekday is the next one when `forward` (a forecast), otherwise the last one
    (a reading); `on` itself counts for both."""
    on = on or today()
    phrase = " ".join(text.lower().split())
    fixed = {"today": 0, "yesterday": -1, "tomorrow": 1}
    if phrase in fixed:
        return on + timedelta(days=fixed[phrase])
    if match := re.fullmatch(r"(\d+) days? ago", phrase):
        return on - timedelta(days=int(match[1]))
    if match := re.fullmatch(r"in (\d+) days?|(\d+) days? (?:from now|ahead)", phrase):
        return on + timedelta(days=int(match[1] or match[2]))
    phrase = {"this weekend": "saturday", "next weekend": "next saturday", "last weekend": "last saturday"}.get(phrase, phrase)  # fmt: skip
    if (match := re.fullmatch(r"(?:(this|next|last) )?(\w+)", phrase)) and match[2] in WEEKDAYS:  # fmt: skip
        ahead = (WEEKDAYS.index(match[2]) - on.weekday()) % 7
        if match[1] == "next":
            return on + timedelta(days=ahead or 7)
        if match[1] == "last":
            return on - timedelta(days=(7 - ahead) % 7 or 7)
        if forward:
            return on + timedelta(days=ahead)
        return on - timedelta(days=(7 - ahead) % 7)
    try:
        return date.fromisoformat(phrase)
    except ValueError:
        return _parse(phrase, on, forward)


def _parse(phrase: str, on: date, forward: bool) -> date:
    import dateparser  # imported here: it takes about a second to load

    settings = {
        "RELATIVE_BASE": datetime.combine(on, datetime.min.time()),
        "PREFER_DATES_FROM": "future" if forward else "past",
    }
    parsed = dateparser.parse(phrase, languages=["en"], settings=settings)
    if parsed is None:
        raise ValueError(f"cannot read the day {phrase!r}")
    return parsed.date()


def argument(text: str) -> date:
    """argparse type: a day phrase, or an error naming the accepted forms."""
    try:
        return resolve(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"cannot read {text!r}; use {FORMS}") from None

from datetime import date

import pytest

from airq.days import resolve

SATURDAY = date(2026, 9, 26)


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
    """Verify that the common day phrases resolve to the right date."""
    assert resolve(phrase, SATURDAY, forward) == expected


def test_unreadable_day_raises():
    """Verify that an unreadable day phrase raises ValueError."""
    with pytest.raises(ValueError):
        resolve("the other day", SATURDAY)


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
    """Verify that phrases passed on to dateparser resolve to the right date."""
    assert resolve(phrase, SATURDAY, forward) == expected

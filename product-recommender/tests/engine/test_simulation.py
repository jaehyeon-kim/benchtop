import random
from datetime import datetime

import pandas as pd
from faker import Faker

from recommender.core.models import User
from recommender.engine.simulation import (
    Visit,
    click_probability,
    time_context,
    visit,
    will_click,
)

# The simulation uses local wall-clock time, so the times have no time zone.
_SATURDAY_8AM = datetime(2026, 1, 3, 8)  # noqa: DTZ001
_FRIDAY_9AM = datetime(2026, 1, 2, 9, 5)  # noqa: DTZ001


def test_time_context_of_a_saturday_morning():
    """Verify that a Saturday at 8am is morning and weekend, and nothing else."""
    assert time_context(_SATURDAY_8AM) == {
        "is_morning": 1,
        "is_afternoon": 0,
        "is_evening": 0,
        "is_weekend": 1,
        "is_weekday": 0,
    }


def test_the_hidden_rules_move_the_click_probability():
    """Verify that morning coffee raises the chance, and an expensive item for a young user lowers it."""
    base = click_probability({}, {})
    assert click_probability({"is_morning": 1}, {"is_coffee": 1}) > base
    assert click_probability({"age": 0.1}, {"price": 0.9}) < base
    assert click_probability({"traffic_source_Search": 1}, {}) > base


def test_will_click_is_repeatable_with_a_seed():
    """Verify that the same seed gives the same clicks."""
    runs = []
    for _ in range(2):
        fake = Faker()
        Faker.seed(3)
        runs.append(
            [will_click({"is_morning": 1}, {"is_coffee": 1}, fake) for _ in range(50)]
        )
    assert runs[0] == runs[1] and 0 < sum(runs[0]) < 50


def test_visit_prints_as_the_posts_show():
    """Verify the printed line of a visit."""
    user = User(7, "A", "B", "a@b.c", 30, "F", "1 St", "3000", "Melbourne", "Victoria", "Australia", -37.8, 145.0, "Search")  # fmt: skip
    v = Visit(user, _FRIDAY_9AM, {}, [5, 12, 150], 12, 1)
    assert (
        str(v)
        == "User 0007 (30 yo) @ Fri 09:05 -> Recs: [005, 012, 150] -> Clicked: 012 (✅)"
    )


def test_a_visit_stops_at_the_first_click(built):
    """Verify that a visit takes the first clicked product, or the first recommendation."""
    out, artifacts = built
    users = pd.read_csv(out / "users.csv").to_dict("records")
    products = {1: {}, 2: {}}
    for draw, expected in [(0.0, (2, 1)), (1.0, (2, 0))]:
        random.seed(1)
        fake = Faker()
        fake.random.random = lambda d=draw: (
            d
        )  # 0 is below every chance, 1 above every one
        v = visit(users, products, artifacts, lambda ctx: [2, 1], fake)
        assert (v.chosen, v.reward) == expected
        assert {"user_id", "age", "is_morning"} <= set(v.context)

from collections import defaultdict
from datetime import datetime

from conftest import run

from sales.core.config import SIM_ID

LIVES = [  # the status sequences an order can go through
    ["Processing"],
    ["Processing", "Cancelled"],
    ["Processing", "Shipped"],
    ["Processing", "Shipped", "Complete"],
    ["Processing", "Shipped", "Complete", "Returned"],
]


def _rows(rows, table):
    return [r for r in rows if r["table"] == table]


def _lives(rows):
    lives = defaultdict(list)
    for order in _rows(rows, "orders"):
        lives[order["id"]].append(order["status"])
    return lives


def _orders_a_minute(rows, first, last):
    minutes = [
        datetime.fromisoformat(o["created_at"]).minute
        for o in _rows(rows, "orders")
        if o["status"] == "Processing"
    ]
    return sum(first <= m < last for m in minutes) / (last - first)


def test_the_catalogue_is_written_first():
    """Verify that the 260 products are written before anything else."""
    assert [r["table"] for r in run(seconds=60)[:260]] == ["products"] * 260


def test_the_same_seed_gives_the_same_rows():
    """Verify that a seeded run repeats exactly."""
    assert run(seed=3, seconds=600) == run(seed=3, seconds=600)


def test_orders_move_only_through_valid_statuses(hour):
    """Verify that orders are cancelled only before shipping, and returned only after completion."""
    lives = _lives(hour)
    assert len(lives) > 1000 and all(life in LIVES for life in lives.values())
    assert LIVES[-1] in lives.values()


def test_buyers_sign_up_before_they_order_and_come_back(hour):
    """Verify that every order's user was written before it, and that users buy more than once."""
    seen: set[str] = set()
    for row in hour:
        if row["table"] == "users":
            seen.add(row["id"])
        elif row["table"] == "orders":
            assert row["user_id"] in seen
    assert len(seen) < 0.6 * len(_lives(hour))


def test_pickers_limit_how_many_orders_are_packed_at_once():
    """Verify that no more orders are packed at once than there are pickers."""
    busy: list[int] = []
    run(
        seconds=900,
        changes=[(0, f"{SIM_ID}.arrival.visitor.rate", 20.0)],
        watch=lambda app: busy.append(app.get_resource("pickers").in_use),
    )
    assert max(busy) == 10


def test_cutting_the_pickers_cancels_orders():
    """Verify that with no pickers, orders wait past their patience and are cancelled."""
    rows = run(
        seconds=1800, changes=[(600, f"{SIM_ID}.resources.pickers.current_cap", 0)]
    )
    assert sum(life[-1] == "Cancelled" for life in _lives(rows).values()) > 50


def test_raising_the_visitor_rate_raises_the_orders():
    """Verify that doubling the visitor rate mid-run roughly doubles the orders a minute."""
    rows = run(seconds=1200, changes=[(600, f"{SIM_ID}.arrival.visitor.rate", 8.0)])
    assert 1.6 < _orders_a_minute(rows, 11, 20) / _orders_a_minute(rows, 1, 10) < 2.5

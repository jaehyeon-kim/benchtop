from collections import defaultdict
from itertools import pairwise

from conftest import simulate

from ecommerce.simulation.shop import NEXT_STATUS


def _statuses(events: list[dict]) -> dict[str, list[str]]:
    """Returns each order's statuses in the order they were published."""
    seen = defaultdict(list)
    for e in events:
        if e["key"] == "orders":
            seen[e["value"]["id"]].append(e["value"]["status"])
    return seen


def test_every_order_moves_only_along_allowed_statuses(half_hour):
    """Verify that each order starts in Processing and each later status is allowed after the one before."""
    for statuses in _statuses(half_hour).values():
        assert statuses[0] == "Processing"
        assert all(b in NEXT_STATUS[a] for a, b in pairwise(statuses))


def test_orders_finish_and_items_follow_their_order(half_hour):
    """Verify that most orders are delivered within the half hour, and each item carries its order's status."""
    finished = [
        s for s in _statuses(half_hour).values() if s[-1] in {"Delivered", "Returned"}
    ]
    assert len(finished) > 0.8 * len(_statuses(half_hour))
    latest = {
        e["value"]["id"]: e["value"]["status"]
        for e in half_hour
        if e["key"] == "orders"
    }
    items = {
        e["value"]["id"]: e["value"] for e in half_hour if e["key"] == "order_items"
    }
    assert all(i["status"] == latest[i["order_id"]] for i in items.values())


def test_one_picker_makes_orders_wait_and_cancels_some(half_hour):
    """Verify that the warehouse resource shapes the data: with one picker, orders queue past their patience and are cancelled, and none is cancelled with three."""
    three, one = _statuses(half_hour), _statuses(simulate(30, pickers=1))
    assert not any("Cancelled" in s for s in three.values())
    cancelled = [s for s in one.values() if "Cancelled" in s]
    assert cancelled and all("Shipped" not in s for s in cancelled)


def test_the_same_seed_repeats_the_run(half_hour):
    """Verify that a seeded run publishes the same rows every time."""
    assert [e["value"] for e in simulate(30)] == [e["value"] for e in half_hour]

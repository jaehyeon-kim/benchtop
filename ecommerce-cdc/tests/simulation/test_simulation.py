import subprocess
from collections import defaultdict
from itertools import pairwise

from conftest import BLUEPRINT, DESFLOW, simulate

from ecommerce.core.models import TABLES

# The statuses an order can move to from each one. An order is cancelled only before it
# ships, and returned only after it is delivered.
NEXT_STATUS = {
    "Processing": {"Shipped", "Cancelled"},
    "Shipped": {"Delivered"},
    "Delivered": {"Returned"},
    "Cancelled": set(),
    "Returned": set(),
}
MODELS = {table: model for model, table in TABLES.items()}


def _rows(rows: list[tuple[str, dict]], table: str) -> list[dict]:
    """Returns the rows written to one table, in the order they were written."""
    return [row for name, row in rows if name == table]


def _statuses(rows: list[tuple[str, dict]]) -> dict[str, list[str]]:
    """Returns each order's statuses in the order they were published."""
    seen = defaultdict(list)
    for order in _rows(rows, "orders"):
        seen[order["id"]].append(order["status"])
    return seen


def _sessions(rows: list[tuple[str, dict]]) -> dict[str, list[dict]]:
    """Returns each session's page views in the order they were published."""
    seen = defaultdict(list)
    for event in _rows(rows, "events"):
        seen[event["session_id"]].append(event)
    return seen


def test_the_blueprint_tests_pass():
    """Verify that the statistics of the blueprint's `tests` section hold."""
    subprocess.run([str(DESFLOW), "test", str(BLUEPRINT)], check=True)


def test_every_row_has_its_table_columns_and_bounds(half_hour):
    """Verify that each row has exactly its model's columns and passes the model's bounds."""
    for table, row in half_hour:
        model = MODELS[table]
        assert set(row) == set(model.model_fields)
        model(**row)
    assert len(_rows(half_hour, "dist_centers")) == 10
    assert len(_rows(half_hour, "products")) == 260


def test_every_order_moves_only_along_allowed_statuses(half_hour):
    """Verify that each order starts in Processing and each later status is allowed after the one before."""
    for statuses in _statuses(half_hour).values():
        assert statuses[0] == "Processing"
        assert all(b in NEXT_STATUS[a] for a, b in pairwise(statuses))


def test_orders_finish_and_items_follow_their_order(half_hour):
    """Verify that most orders are delivered within the half hour, and each item carries its order's status and price."""
    finished = [
        s for s in _statuses(half_hour).values() if s[-1] in {"Delivered", "Returned"}
    ]
    assert len(finished) > 0.8 * len(_statuses(half_hour))
    latest = {o["id"]: o for o in _rows(half_hour, "orders")}
    items = {i["id"]: i for i in _rows(half_hour, "order_items")}
    prices = {p["id"]: p["retail_price"] for p in _rows(half_hour, "products")}
    for item in items.values():
        assert item["status"] == latest[item["order_id"]]["status"]
        assert item["sale_price"] == prices[item["product_id"]]
    counts = defaultdict(int)
    for item in items.values():
        counts[item["order_id"]] += 1
    assert all(counts[o["id"]] == o["num_of_items"] for o in latest.values())


def test_visits_are_returning_anonymous_or_signed_up(half_hour):
    """Verify that a visit starts at home, a registered visitor's pages carry the user's current address, and an anonymous buyer signs up before the purchase page."""
    users: dict[str, dict] = {}
    kinds = defaultdict(int)
    for table, row in half_hour:
        if table == "users":
            users[row["id"]] = row
        if table == "events" and row["sequence_number"] == 1 and row["user_id"]:
            user = users[row["user_id"]]
            where = (user["city"], user["state"], user["postal_code"])
            assert (row["city"], row["state"], row["postal_code"]) == where
    for pages in _sessions(half_hour).values():
        assert pages[0]["uri"] == "/home"
        assert [p["sequence_number"] for p in pages] == list(range(1, len(pages) + 1))
        if pages[-1]["event_type"] == "purchase":
            assert [p["event_type"] for p in pages[-2:]] == ["cart", "purchase"]
            assert pages[-1]["user_id"] in users
        kind = "registered" if pages[0]["user_id"] else "anonymous"
        if kind == "anonymous" and pages[-1]["user_id"]:
            kind = "signed up"
        kinds[kind] += 1
    assert kinds["registered"] and kinds["anonymous"] and kinds["signed up"]


def test_a_move_changes_the_address_and_keeps_the_user(half_hour):
    """Verify that a moved user keeps the id and the personal columns, and gets a new street address and update time."""
    first: dict[str, dict] = {}
    moves = 0
    for row in _rows(half_hour, "users"):
        if row["id"] not in first:
            first[row["id"]] = row
            continue
        moves += 1
        before = first[row["id"]]
        kept = ("first_name", "last_name", "email", "age", "gender", "created_at")
        assert all(row[k] == before[k] for k in kept)
        assert row["street_address"] != before["street_address"]
        assert row["updated_at"] > before["updated_at"]
    assert moves > 0


def test_one_picker_cancels_orders_that_wait_too_long(half_hour):
    """Verify that the warehouse resource shapes the data: with one picker, orders queue past their patience and are cancelled, and none is cancelled with three."""
    three, one = _statuses(half_hour), _statuses(simulate(30, PICKERS="1"))
    assert not any("Cancelled" in s for s in three.values())
    cancelled = [s for s in one.values() if "Cancelled" in s]
    assert cancelled and all("Shipped" not in s for s in cancelled)


def test_the_same_seed_repeats_the_run(half_hour):
    """Verify that a seeded run publishes the same rows every time, apart from the times, which start at the wall clock's now."""

    def untimed(rows: list[tuple[str, dict]]) -> list[tuple[str, dict]]:
        return [
            (t, {k: v for k, v in r.items() if not k.endswith("_at")}) for t, r in rows
        ]

    assert untimed(simulate(30)) == untimed(half_hour)

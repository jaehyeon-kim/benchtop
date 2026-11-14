"""Checks model.yaml with desflow's own commands, which contact no service.

`desflow test` runs the statistics in the file's `tests:` section. `desflow run --sample`
prints the first events of a seeded run, unpaced and with no ingress or sinks, and the
tests below check the rows in them against the bounds and rules of the four tables.
"""

import json
import shutil
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

import pytest

DESFLOW = str(Path(sys.executable).with_name("desflow"))
PROJECT = Path(__file__).parents[2]
MODEL = str(PROJECT / "model.yaml")
STATUSES = {"Processing", "Shipped", "Complete", "Cancelled", "Returned"}
LIVES = [  # the status sequences an order can go through
    ["Processing"],
    ["Processing", "Cancelled"],
    ["Processing", "Shipped"],
    ["Processing", "Shipped", "Complete"],
    ["Processing", "Shipped", "Complete", "Returned"],
]
# The table of each task's rows. A task's queued and started records carry other fields.
TABLES = {
    "product_row": ("products", "department"),
    "new_user": ("users", "age"),
    "order": ("orders", "num_of_item"),
    "pick": ("orders", "num_of_item"),
    "cancelled": ("orders", "num_of_item"),
    "transit": ("orders", "num_of_item"),
    "returned": ("orders", "num_of_item"),
    "order_item": ("order_items", "sale_price"),
}


@pytest.fixture(scope="module")
def rows() -> list[dict]:
    """The rows of the first 30,000 events of a run with seed 1, each tagged with its table."""
    out = subprocess.run(
        [DESFLOW, "run", MODEL, "--sample", "30000", "--seed", "1"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    rows = []
    for line in out.splitlines():
        record = json.loads(line)
        table, field = TABLES.get(record["task"], (None, None))
        if table and field in record["value"]:
            rows.append({"table": table, **record["value"]})
    return rows


def _rows(rows: list[dict], table: str) -> list[dict]:
    return [r for r in rows if r["table"] == table]


def test_the_blueprint_gives_its_expected_statistics():
    """Verify that `desflow test` passes the statistics in the file's tests section."""
    result = subprocess.run(
        [DESFLOW, "test", MODEL], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.startswith(f"ok {MODEL}: an hour at the defaults")


def test_the_catalogue_is_written_first(rows):
    """Verify that the 260 products come before any other row, with prices above their cost."""
    products = rows[:260]
    assert [p["id"] for p in products] == list(range(1, 261))
    assert len(_rows(rows, "products")) == 260
    for p in products:
        assert p["department"] in {"Men", "Women"}
        assert 0 < p["cost"] < p["retail_price"]


def test_rows_fall_within_their_bounds(rows):
    """Verify every user's age and gender, every order's item count, and every item's price."""
    for user in _rows(rows, "users"):
        assert 12 <= user["age"] <= 70 and user["gender"] in {"M", "F"}
    for order in _rows(rows, "orders"):
        assert 1 <= order["num_of_item"] <= 4 and order["status"] in STATUSES
    for item in _rows(rows, "order_items"):
        assert item["sale_price"] > 0 and item["status"] in STATUSES


def test_orders_move_only_through_valid_statuses(rows):
    """Verify that orders are cancelled only before shipping, and returned only after completion."""
    lives = defaultdict(list)
    for order in _rows(rows, "orders"):
        lives[order["id"]].append(order["status"])
    assert len(lives) > 500 and all(life in LIVES for life in lives.values())
    assert LIVES[-1] in lives.values()


def test_an_order_holds_its_count_of_different_products(rows):
    """Verify that each order's items belong to it and its user, and are different products."""
    orders = {o["id"]: o for o in _rows(rows, "orders") if o["status"] == "Processing"}
    items = defaultdict(dict)
    for item in _rows(rows, "order_items"):
        order = orders[item["order_id"]]
        assert item["user_id"] == order["user_id"]
        items[item["order_id"]][item["id"]] = item["product_id"]
    # An order's items are all written before its next status, which the sample may cut off
    moved = {o["id"] for o in _rows(rows, "orders") if o["status"] != "Processing"}
    assert len(moved) > 500
    for order_id in moved:
        products = items[order_id]
        assert len(products) == orders[order_id]["num_of_item"]
        assert len(set(products.values())) == len(products)


def test_buyers_sign_up_before_they_order_and_come_back(rows):
    """Verify that every order's user was written before it, and that users buy more than once."""
    seen: set[str] = set()
    orders = 0
    for row in rows:
        if row["table"] == "users":
            seen.add(row["id"])
        elif row["table"] == "orders" and row["status"] == "Processing":
            assert row["user_id"] in seen
            orders += 1
    assert len(seen) < 0.6 * orders


def run_with_changes(tmp_path: Path, until: int, changes: list[tuple]) -> list[dict]:
    """
    Runs a copy of model.yaml with a scenario of live changes and returns every event.

    A scenario step makes a change at a set simulation time through the same registry
    path that a row of `dashboard.params` names, so these tests also check that the paths
    `sales.simulation.control` sends exist in the blueprint.
    """
    shutil.copy(PROJECT / "products.csv", tmp_path)
    steps = [{"at": at, "path": path, "value": value} for at, path, value in changes]
    blueprint = tmp_path / "model.yaml"
    blueprint.write_text(
        (PROJECT / "model.yaml").read_text() + f"\nscenario: {json.dumps(steps)}\n"
    )
    out = subprocess.run(
        [DESFLOW, "run", str(blueprint), "--sample", "10000000", "--seed", "1"]
        + ["--until", str(until)],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return [json.loads(line) for line in out.splitlines()]


def _order_rows(events: list[dict], status: str) -> list[dict]:
    return [
        e
        for e in events
        if e["task"] in TABLES
        and TABLES[e["task"]][0] == "orders"
        and e["value"].get("status") == status
    ]


def test_pickers_limit_how_many_orders_are_packed_at_once(tmp_path):
    """Verify that at five times the visitor rate no more than 10 orders are packed at once."""
    events = run_with_changes(tmp_path, 900, [(0, "sales.arrival.visitor.rate", 20.0)])
    packing = most = 0
    for e in events:
        if e["task"] == "pick" and e["value"].get("status") == "started":
            packing += 1
        elif e["task"] == "pick" and e["value"].get("status") == "Shipped":
            packing -= 1
        most = max(most, packing)
    assert most == 10
    assert _order_rows(events, "Cancelled")


def test_cutting_the_pickers_cancels_orders(tmp_path):
    """Verify that with no pickers from 600 seconds nothing ships and waiting orders cancel."""
    events = run_with_changes(
        tmp_path, 1800, [(600, "sales.resources.pickers.current_cap", 0)]
    )
    late = 700  # orders being packed at 600 seconds have shipped by then
    assert _order_rows(events, "Shipped")
    assert not [e for e in _order_rows(events, "Shipped") if e["sim_ts"] > late]
    cancelled = [e for e in _order_rows(events, "Cancelled") if e["sim_ts"] > late]
    assert len(cancelled) > 100


def test_raising_the_visitor_rate_raises_the_orders(tmp_path):
    """Verify that doubling the visitor rate at 600 seconds roughly doubles the orders."""
    events = run_with_changes(
        tmp_path, 1200, [(600, "sales.arrival.visitor.rate", 8.0)]
    )
    orders = _order_rows(events, "Processing")
    before = len([e for e in orders if e["sim_ts"] < 600])
    after = len(orders) - before
    assert 1.6 < after / before < 2.4

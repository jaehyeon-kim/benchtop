"""A fast, seeded run of the shop blueprint, so no test needs the odctl stack."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

BLUEPRINT = Path(__file__).parents[2] / "ecommerce" / "simulation" / "shop.yaml"
DESFLOW = Path(sys.executable).parent / "desflow"

# The task that writes each table's rows, as shop.yaml names them
TABLE_OF_TASK = {
    "dist_center": "dist_centers",
    "product": "products",
    "user": "users",
    "order_row": "orders",
    "item_row": "order_items",
    "page": "events",
}


def simulate(minutes: float, **variables: str) -> list[tuple[str, dict]]:
    """
    Runs the blueprint for `minutes` simulated minutes with seed 7, unpaced, and
    returns its rows.

    `desflow run --sample` prints the records as JSON lines and contacts no service:
    it leaves out the ingress and puts a console in place of the sinks.

    Args:
        minutes (float): How long to run, in simulated minutes.
        **variables (str): Values for the blueprint's `${NAME}`, such as PICKERS="1".

    Returns:
        list[tuple[str, dict]]: Each row's table and its columns, in the order written.
    """
    command = [str(DESFLOW), "run", str(BLUEPRINT), "--seed", "7"]
    command += ["--until", f"{minutes} min", "--sample", "10000000"]
    for name, value in variables.items():
        command += ["--var", f"{name}={value}"]
    out = subprocess.run(command, capture_output=True, text=True, check=True).stdout
    records = (json.loads(line) for line in out.splitlines())
    return [
        (TABLE_OF_TASK[r["task"]], r["value"])
        for r in records
        if r["task"] in TABLE_OF_TASK and "id" in r["value"]
    ]


@pytest.fixture(scope="session")
def half_hour() -> list[tuple[str, dict]]:
    """The rows of 30 simulated minutes with the default three pickers."""
    return simulate(30)

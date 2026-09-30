"""A fast, seeded run of the shop, collected in memory, so no test needs the odctl stack."""

import asyncio
import queue
from datetime import UTC, datetime

import pytest
from dynamic_des.connectors.egress.base import BaseEgress

from ecommerce.simulation.run import build

START = datetime(2026, 9, 1, tzinfo=UTC)


class Collect(BaseEgress):
    """Keeps every published event in a list."""

    def __init__(self):
        self.events: list[dict] = []

    async def run(self, egress_queue: queue.Queue) -> None:
        while True:
            try:
                self.events += egress_queue.get_nowait()
            except queue.Empty:
                await asyncio.sleep(0.01)


def simulate(minutes: float, **resources: int) -> list[dict]:
    """Runs the shop for `minutes` simulated minutes, as fast as possible, and returns its events."""
    app = build(START, seed=7, factor=0.0)
    for name, capacity in resources.items():
        app.add_resource(name, current_cap=capacity, max_cap=10)
    collect = Collect()
    app.add_egress(collect, when=lambda r: r["stream_type"] == "event")
    app.run(until=minutes * 60)
    return collect.events


@pytest.fixture(scope="session")
def half_hour() -> list[dict]:
    """The events of 30 simulated minutes with the default three pickers."""
    return simulate(30)

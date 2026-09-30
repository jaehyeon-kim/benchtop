"""Runs the model at full speed and collects every row it publishes, with no services."""

import asyncio
import queue
from datetime import UTC, datetime

import pytest
from dynamic_des.connectors.egress.base import BaseEgress

from sales.simulation.run import build

START = datetime(2026, 9, 1, tzinfo=UTC)


class Collect(BaseEgress):
    """Keeps every published row in memory, tagged with its table."""

    def __init__(self):
        self.rows: list[dict] = []

    async def run(self, egress_queue: queue.Queue) -> None:
        try:
            while True:
                try:
                    self.rows += [
                        {"table": r["key"], **r["value"]}
                        for r in egress_queue.get_nowait()
                    ]
                except queue.Empty:
                    await asyncio.sleep(0.01)
        except asyncio.CancelledError:  # teardown: take what is left
            while not egress_queue.empty():
                self.rows += [
                    {"table": r["key"], **r["value"]} for r in egress_queue.get_nowait()
                ]


def run(seed=1, seconds=3600, changes=(), watch=None) -> list[dict]:
    """Runs the model for `seconds` at full speed, applying `(at, path, value)` changes."""
    app = build(seed, START)
    app.factor = 0.0
    collect = Collect()
    app.add_egress(collect, when=lambda r: r["stream_type"] == "event")

    @app.arrival_loop("control")  # starts a process when the run begins
    def control(ctx):
        for at, path, value in sorted(changes):
            yield ctx.env.timeout(at - ctx.env.now)
            ctx.env.registry.update(path, value)

    if watch:

        @app.arrival_loop("observe")
        def observe(ctx):
            while True:
                watch(app)
                yield ctx.env.timeout(1)

    app.run(until=seconds)
    return collect.rows


@pytest.fixture(scope="session")
def hour() -> list[dict]:
    """One simulated hour with the default parameters."""
    return run()

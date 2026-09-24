"""Data simulation: runs the generator on a dynamic-des clock.

The clock starts at ORIGIN and ticks hourly. Each hour the station issues its
forecasts and records PM2.5, and an outage loop decides when readings are lost.
Every record goes to `emit`, so the caller decides where records go: backfill.py
publishes them to Iceberg.

The simulation always starts at ORIGIN with a fixed seed, so every run produces the
same values.
"""

from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import numpy as np
from dynamic_des import SimulationContext
from pydantic import BaseModel

from airq.generator import (
    CALM,
    LEADS,
    next_hour,
    observation,
    weather_forecast,
)

ORIGIN = datetime(2024, 10, 1, tzinfo=UTC)
SEED = 42
HOUR = 3600.0

Emit = Callable[[SimulationContext, BaseModel], None]


def build(emit: Emit, seed: int = SEED) -> SimulationContext:
    """Builds the station as a dynamic-des context, starting at ORIGIN."""
    rng = np.random.default_rng(seed)
    app = SimulationContext(
        "air_quality",
        factor=0.0,
        random_seed=seed,
        logical_start_time=ORIGIN.replace(tzinfo=None),
    ).add_arrival("outage", dist="exponential", rate=1 / (30 * 24 * HOUR))
    hours = [CALM]  # true state per hour since ORIGIN, a week ahead of the clock
    down_until = -1.0

    @app.arrival_loop("outage")
    def outages(ctx):
        nonlocal down_until
        while True:
            yield ctx.wait_for_arrival("outage")
            down_until = ctx.env.now + float(ctx.sampler.rng.uniform(6, 48)) * HOUR

    @app.telemetry_loop(interval=HOUR)
    def hourly(ctx):
        k = round(ctx.env.now / HOUR)
        now = ORIGIN + timedelta(hours=k)
        while len(hours) <= k + 24 * len(LEADS):
            hours.append(next_hour(rng, hours[-1]))
        for lead in LEADS:
            emit(ctx, weather_forecast(rng, now, lead, hours[k + 24 * lead]))
        if ctx.env.now >= down_until:
            emit(ctx, observation(rng, now, hours[k]))

    return app

from collections import defaultdict
from itertools import pairwise

import numpy as np
import pytest
from dynamic_des import SimulationContext
from dynamic_des.core.environment import DynamicRealtimeEnvironment

from leaderboard.core.config import SIM_ID
from leaderboard.simulation.run import build


def _simulate(monkeypatch, seconds: float, seed: int = 1, change=None) -> list:
    """Runs the game as fast as possible, and returns each (send time, event) it publishes."""
    sent: list = []
    monkeypatch.setattr(DynamicRealtimeEnvironment, "publish_event", lambda env, key, event: sent.append((env.now, event)))  # fmt: skip
    app = SimulationContext(SIM_ID, factor=0.0, random_seed=seed)
    build(app)
    if change:  # (time, registry path, value) to set while the game runs

        @app.telemetry_loop(interval=change[0])
        def _change(ctx):
            if ctx.env.now >= change[0]:
                ctx.env.registry.update(change[1], change[2])

    app.run(until=seconds)
    return sent


@pytest.fixture(scope="module")
def hour():
    """An hour of the game with a fixed seed."""
    with pytest.MonkeyPatch.context() as monkeypatch:
        yield _simulate(monkeypatch, 3600)


def test_sessions_end_and_players_keep_arriving(hour):
    """Verify that sessions last minutes, not the whole hour, while new players keep arriving."""
    first, last = {}, {}
    for t, e in hour:
        first.setdefault(e.user_id, t)
        last[e.user_id] = t
    assert len(first) > 1000  # about a player every 2 seconds
    assert np.median([last[p] - first[p] for p in first]) < 600
    assert max(first.values()) > 3500


def test_robots_play_rounds_about_four_times_as_often(hour):
    """Verify that robots play a round every 1.5 seconds on average and people every 6."""
    times = defaultdict(list)
    for t, e in hour:
        if e.event_type == "normal":
            times[e.user_id].append(t)

    def gap(prefix: str) -> float:
        return float(np.mean([b - a for p, ts in times.items() if p.startswith(prefix) for a, b in pairwise(ts)]))  # fmt: skip

    assert 1.0 < gap("RBT") < 2.5 and 4.5 < gap("USR") < 7.5


def test_late_scores_carry_the_time_they_were_earned(hour):
    """Verify that a late score is sent minutes after the time it carries, and others at once."""
    start_ms = hour[0][1].event_time_millis - int(hour[0][0] * 1000)
    lag = defaultdict(list)
    for t, e in hour:
        lag[e.event_type].append(t - (e.event_time_millis - start_ms) / 1000)
    assert 0.005 < len(lag["late"]) / len(hour) < 0.02  # late_share is 0.01
    assert np.mean(lag["late"]) == pytest.approx(420, rel=0.15)
    assert max(abs(x) for x in lag["normal"]) < 0.01


def test_a_player_stays_in_one_team(hour):
    """Verify that every score of a player carries the same team."""
    team_of = {}
    for _, e in hour:
        assert team_of.setdefault(e.user_id, e.team_id) == e.team_id


def test_a_live_change_to_the_robot_share_takes_effect(monkeypatch):
    """Verify that raising the robot share while the game runs makes later arrivals robots."""
    sent = _simulate(monkeypatch, 1200, seed=3, change=(600, f"{SIM_ID}.variables.robot_share", 1.0))  # fmt: skip
    arrived = {}
    for t, e in sent:
        arrived.setdefault(e.user_id, t)
    after = [
        p for p, t in arrived.items() if t > 700
    ]  # first seen well after the change
    assert len(after) > 100 and all(p.startswith("RBT") for p in after)

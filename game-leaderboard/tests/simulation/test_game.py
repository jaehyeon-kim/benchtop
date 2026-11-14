import json
import re
import shutil
import subprocess
import sys
from collections import defaultdict
from datetime import datetime
from itertools import pairwise
from pathlib import Path

import fastavro
import numpy as np
import pytest

from leaderboard.core.config import BLUEPRINT, SCORE_SCHEMA

DESFLOW = str(Path(sys.executable).parent / "desflow")
# The bounds of each field, which the Flink jobs and the dashboard rely on
BOUNDS = {
    "user_id": r"(USR|RBT)-\d{6,}",
    "team_id": r"\d{10}",
    "team_name": r"[A-Z][a-z]+-[A-Z][a-z]+",
    "event_type": r"normal|late",
}


def _ms(timestamp: str) -> int:
    """Gives an ISO time as milliseconds since the Unix epoch."""
    return int(datetime.fromisoformat(timestamp).timestamp() * 1000)


def run(blueprint: Path, seed: int, until: int) -> list[dict]:
    """Runs a blueprint unpaced with a seed and gives each score's record."""
    out = subprocess.run(
        [DESFLOW, "run", str(blueprint), "--sample", "1000000", "--seed", str(seed), "--until", str(until)],
        capture_output=True, text=True, check=True,
    ).stdout  # fmt: skip
    records = [json.loads(line) for line in out.splitlines()]
    return [r for r in records if r["value"].get("status") != "started"]


@pytest.fixture(scope="module")
def scores() -> list[dict]:
    """Half an hour of the game with a fixed seed, run unpaced: each score's record."""
    return run(BLUEPRINT, 1, 1800)


def _spans(scores: list[dict]) -> dict[str, tuple[float, float, str, str]]:
    """
    Each player's first and last score time, with the team id and name it played for.

    Only scores sent when earned count: a late score is sent minutes later, after its
    player may have left the team.
    """
    spans: dict[str, tuple[float, float, str, str]] = {}
    for r in scores:
        v = r["value"]
        if v["event_type"] == "late":
            continue
        first = spans.get(v["user_id"], (r["sim_ts"],))[0]
        spans[v["user_id"]] = (first, r["sim_ts"], v["team_id"], v["team_name"])
    return spans


def test_blueprint_tests_pass():
    """Verify that the blueprint's own `tests:` give the counts they state."""
    subprocess.run([DESFLOW, "test", str(BLUEPRINT)], check=True)


def test_only_scores_are_sent_and_each_fits_the_schema(scores):
    """Verify that joining, leaving and rounds send nothing, and every score fits score.avsc and its bounds."""
    schema = fastavro.parse_schema(json.loads(SCORE_SCHEMA.read_text()))
    assert {r["task"] for r in scores} == {"round", "late"}
    for r in scores:
        value = r["value"]
        assert fastavro.validate(value, schema, strict=True)
        assert all(re.fullmatch(p, value[f]) for f, p in BOUNDS.items())
        assert 0 <= value["score"] <= 20 and value["event_time_millis"] > 0
        assert r["key"] == value["user_id"]


def test_a_player_stays_in_one_team_and_a_team_id_has_one_name(scores):
    """Verify that every score of a player carries the same team, and an id one name."""
    team_of, name_of = {}, {}
    for r in scores:
        v = r["value"]
        assert team_of.setdefault(v["user_id"], v["team_id"]) == v["team_id"]
        assert name_of.setdefault(v["team_id"], v["team_name"]) == v["team_name"]


def test_robots_play_rounds_about_four_times_as_often(scores):
    """Verify that robots play a round every 1.5 seconds on average and people every 6."""
    times = defaultdict(list)
    for r in scores:
        if r["value"]["event_type"] == "normal":
            times[r["key"]].append(r["sim_ts"])

    def gap(prefix: str) -> float:
        return float(np.mean([b - a for p, ts in times.items() if p.startswith(prefix) for a, b in pairwise(ts)]))  # fmt: skip

    assert 1.0 < gap("RBT") < 2.5 and 4.5 < gap("USR") < 7.5


def test_late_scores_carry_the_time_they_were_earned(scores):
    """Verify that a late score is sent minutes after the time it carries, and others at once."""
    lag = defaultdict(list)
    for r in scores:
        sent = _ms(r["timestamp"])
        lag[r["value"]["event_type"]].append((sent - r["value"]["event_time_millis"]) / 1000)  # fmt: skip
    assert 0.005 < len(lag["late"]) / len(scores) < 0.02  # late_share is 0.01
    assert np.mean(lag["late"]) == pytest.approx(420, rel=0.15)
    assert max(abs(x) for x in lag["normal"]) < 0.01


def test_a_team_holds_at_most_15_players_and_teams_playing_together_differ_in_name(
    scores,
):
    """Verify the team rules: 15 players at most, and no two teams at once with one name."""
    spans = _spans(scores)
    # A player belongs to the team for at least the time from its first to its last score
    for t in sorted({first for first, *_ in spans.values()}):
        playing = [s for s in spans.values() if s[0] <= t <= s[1]]
        teams = defaultdict(int)
        for _, _, team_id, _ in playing:
            teams[team_id] += 1
        assert max(teams.values()) <= 15
        names = {team_id: name for _, _, team_id, name in playing}
        assert len(set(names.values())) == len(names)


def test_sessions_end_while_players_keep_arriving(scores):
    """Verify that players leave long before the end, and new players arrive near it."""
    spans = _spans(scores)
    assert sum(last < 1200 for _, last, *_ in spans.values()) > 100
    assert sum(first > 1500 for first, *_ in spans.values()) > 50


def test_a_live_change_to_the_robot_share_takes_effect(tmp_path):
    """Verify that a change of `game.variables.robot_share` to 1 makes every new player a robot."""
    shutil.copy(SCORE_SCHEMA, tmp_path)
    blueprint = tmp_path / BLUEPRINT.name
    change = [{"at": 600, "path": "game.variables.robot_share", "value": 1.0}]
    blueprint.write_text(BLUEPRINT.read_text() + f"\nscenario: {json.dumps(change)}\n")
    spans = _spans(run(blueprint, 3, 1200))
    late = [player for player, (first, *_) in spans.items() if first > 700]
    early = [player for player, (first, *_) in spans.items() if first < 600]
    assert len(late) > 50 and all(player.startswith("RBT-") for player in late)
    assert sum(player.startswith("USR-") for player in early) > 50

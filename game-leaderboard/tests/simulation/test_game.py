import numpy as np

from leaderboard.simulation.game import TEAM_SIZE, Game


def test_teams_fill_to_their_size_and_names_stay_unique():
    """Verify that no team takes more than TEAM_SIZE players, and playing teams have different names."""
    game = Game(np.random.default_rng(2))
    for i in range(200):
        game.join(f"USR-{i:06}")
    assert all(len(t.members) <= TEAM_SIZE for t in game.teams.values())
    names = [t.name for t in game.teams.values()]
    assert len(names) == len(set(names))


def test_a_team_dissolves_when_its_last_player_leaves():
    """Verify that leaving empties and removes a team, and team ids are not reused."""
    game = Game(np.random.default_rng(3))
    joined = [(game.join(f"USR-{i:06}"), f"USR-{i:06}") for i in range(50)]
    ids = {t.team_id for t, _ in joined}
    for team, player in joined:
        game.leave(team, player)
    assert game.teams == {}
    assert game.join("USR-999999").team_id not in ids

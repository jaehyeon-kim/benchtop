from leaderboard.app.reports import LEADERBOARDS
from leaderboard.stores.postgres import query


def test_each_leaderboard_is_read_best_first_from_its_table():
    """Verify that each leaderboard reads its label and value columns from its table, ordered by rank."""
    assert set(LEADERBOARDS) == {"top_teams", "top_players", "hot_streaks", "team_mvps"}
    _, label, value = LEADERBOARDS["top_teams"]
    assert (
        query("top_teams", label, value)
        == "SELECT team_name, total_score FROM game.top_teams ORDER BY rnk"
    )

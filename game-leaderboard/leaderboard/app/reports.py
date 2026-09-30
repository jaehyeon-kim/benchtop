"""The four leaderboards the dashboard shows, read from PostgreSQL."""

from leaderboard.stores import postgres

# Each leaderboard's table, its title, the column naming each bar and the column it plots.
LEADERBOARDS = {
    "top_teams": ("Top teams: highest total score", "team_name", "total_score"),
    "top_players": ("Top players: highest total score", "user_id", "total_score"),
    "hot_streaks": ("Hot streaks: last 10 seconds against the last minute", "user_id", "hotness"),
    "team_mvps": ("Team MVPs: share of their team's score", "user_id", "contrib_ratio"),
}  # fmt: skip


def leaderboard(table: str) -> list[tuple[str, float]]:
    """
    Reads one leaderboard.

    Args:
        table (str): A key of `LEADERBOARDS`.

    Returns:
        list[tuple[str, float]]: The label and value of each rank, best first.
    """
    _, label, value = LEADERBOARDS[table]
    return postgres.read(table, label, value)

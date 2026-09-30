"""The dashboard: the four leaderboards as bar charts, redrawn every 2 seconds.

Run: python -m leaderboard.app.ui   (then open http://127.0.0.1:8091)
"""

from typing import Any

from nicegui import ui

from leaderboard.app.reports import LEADERBOARDS, leaderboard
from leaderboard.core.config import APP_PORT


def chart(table: str) -> dict[str, Any]:
    """
    Returns the chart options for one leaderboard, the best at the top.

    Args:
        table (str): A key of `LEADERBOARDS`.

    Returns:
        dict[str, Any]: ECharts options for a horizontal bar chart.
    """
    rows = leaderboard(table)[::-1]  # ECharts draws the first category at the bottom
    return {
        "title": {"text": LEADERBOARDS[table][0], "textStyle": {"fontSize": 14}},
        "grid": {"left": 160, "right": 30, "top": 40, "bottom": 20},
        "xAxis": {"type": "value"},
        "yAxis": {"type": "category", "data": [str(label) for label, _ in rows]},
        "series": [
            {
                "type": "bar",
                "colorBy": "data",  # a colour from the palette for each bar
                "data": [round(float(v), 2) for _, v in rows],
            }
        ],
        "tooltip": {"trigger": "axis"},
    }


@ui.page("/")
def page() -> None:
    """Builds the page, with a chart per leaderboard redrawn every 2 seconds."""
    ui.page_title("Game leaderboard")
    ui.label("Game leaderboard").classes("text-xl")
    with ui.grid(columns=2).classes("w-full"):
        charts = {
            table: ui.echart(chart(table)).classes("h-80") for table in LEADERBOARDS
        }

    def refresh() -> None:
        for table, echart in charts.items():
            echart.options.update(chart(table))
            echart.update()

    ui.timer(2.0, refresh)


if __name__ == "__main__":
    ui.run(
        host="127.0.0.1",
        port=APP_PORT,
        title="Game leaderboard",
        reload=False,
        show=False,
    )

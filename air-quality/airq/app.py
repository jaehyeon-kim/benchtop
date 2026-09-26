"""The air quality app: one NiceGUI page with two tabs.

- Monitoring: the models in use, the forecast against what was measured, and
  each model's error over time and by lead.
- Assistant: a chat with the forecast agent in `airq.assistant`, which runs in
  the same process.

Everything is read from Iceberg and MLflow through `airq.reports`.

Run: python -m airq.app   (then open http://127.0.0.1:8090)
"""

from datetime import timedelta
from typing import Any

import pandas as pd
from nicegui import ui

from airq import assistant, reports
from airq.config import APP_PORT

_HISTORY_DAYS = 60
_ERROR_DAYS = 30


def _points(frame: pd.DataFrame, value: str) -> list[list]:
    return [[str(d), round(float(v), 2)] for d, v in zip(frame["day"], frame[value])]


def _label(version: str, served: dict[str, str]) -> str:
    alias = served.get(version)
    return f"v{version} ({alias})" if alias else f"v{version}"


def _aliases() -> dict[str, str]:
    """Each served version's alias, or both aliases when one version holds both."""
    served: dict[str, list[str]] = {}
    for m in reports.served_models():
        served.setdefault(m["version"], []).append(m["alias"])
    return {version: " and ".join(names) for version, names in served.items()}


def forecast_chart() -> dict:
    """Measured PM2.5, each version's lead-1 predictions, and the latest forecast."""
    served = _aliases()
    history = reports.history(_HISTORY_DAYS)
    latest = reports.forecast()
    if not latest.empty:  # one row per version and day, whatever its aliases
        latest = latest.drop_duplicates(["model_version", "day"])
    end = reports.last_measured_day()
    measured = (
        reports.observed(end - timedelta(days=_HISTORY_DAYS - 1), end)
        if end
        else pd.DataFrame(columns=["day", "pm2_5"])
    )
    series: list[dict[str, Any]] = [
        {"name": "measured", "type": "line", "data": _points(measured, "pm2_5"),
         "lineStyle": {"width": 3}, "color": "#333"},
    ]  # fmt: skip
    for version, rows in history.groupby("model_version"):
        series.append(
            {"name": f"{_label(version, served)}, 1 day ahead", "type": "line",
             "data": _points(rows, "pm2_5_predicted"), "showSymbol": False}
        )  # fmt: skip
    for version, rows in latest.groupby("model_version") if not latest.empty else []:
        series.append(
            {"name": f"{_label(version, served)}, forecast", "type": "line",
             "data": _points(rows, "pm2_5"), "lineStyle": {"type": "dashed"}}
        )  # fmt: skip
    return {
        "tooltip": {"trigger": "axis"},
        "legend": {"top": 0},
        "grid": {"top": 60, "left": 50, "right": 20, "bottom": 30},
        "xAxis": {"type": "time"},
        "yAxis": {"type": "value", "name": "PM2.5 µg/m³", "scale": True},
        "series": series,
    }


def error_chart() -> dict:
    """Absolute error of each version's lead-1 prediction, day by day."""
    served = _aliases()
    history = reports.history(_HISTORY_DAYS)
    return {
        "tooltip": {"trigger": "axis"},
        "legend": {"top": 0},
        "grid": {"top": 40, "left": 50, "right": 20, "bottom": 30},
        "xAxis": {"type": "time"},
        "yAxis": {"type": "value", "name": "absolute error"},
        "series": [
            {"name": _label(version, served), "type": "bar",
             "data": _points(rows, "abs_error")}
            for version, rows in history.groupby("model_version")
        ],
    }  # fmt: skip


def _error_table() -> tuple[list[dict], list[dict]]:
    """Columns and rows: one row per lead, one column per model version."""
    table = reports.model_error(_ERROR_DAYS)
    wide = table.pivot(index="lead_days", columns="model_version", values="mae")
    names = ["lead_days", *(f"v{v}" for v in wide.columns)]
    columns = [{"name": n, "label": n, "field": n} for n in names]
    rows = [
        {"lead_days": int(lead), **{f"v{v}": float(row[v]) for v in wide.columns}}
        for lead, row in wide.iterrows()
    ]
    return columns, rows


@ui.page("/")
def page() -> None:
    agent = assistant.build(callback_handler=None)  # one conversation per browser tab
    ui.page_title("Air quality")
    with ui.header().classes("items-center"):
        ui.label("PM2.5 forecast, station-1").classes("text-xl")
        models = ui.row()
        ui.space()
        with ui.tabs() as tabs:
            monitoring_tab = ui.tab("Monitoring", icon="insights")
            assistant_tab = ui.tab("Assistant", icon="chat")

    with ui.tab_panels(tabs, value=monitoring_tab).classes("w-full"):
        with ui.tab_panel(monitoring_tab):
            refresh = ui.button("Refresh", icon="refresh")
            forecast = ui.echart(forecast_chart()).classes("w-full h-96")
            errors = ui.echart(error_chart()).classes("w-full h-64")
            ui.label(f"Mean absolute error by lead, last {_ERROR_DAYS} days").classes(
                "text-lg"
            )
            columns, rows = _error_table()
            table = ui.table(columns=columns, rows=rows, row_key="lead_days")
            table.classes("w-full")
        with (
            ui.tab_panel(assistant_tab),
            ui.column().classes("w-full max-w-3xl mx-auto"),
        ):
            messages = ui.column().classes("w-full h-[65vh] overflow-y-auto")
            question = ui.input(
                placeholder="Ask about the forecast, such as: what is the forecast for tomorrow?"
            ).classes("w-full")

    def show_models() -> None:
        models.clear()
        with models:
            for m in reports.served_models():
                ui.badge(f"@{m['alias']}: v{m['version']}, {m['feature_set']} features")

    def reload() -> None:
        show_models()
        forecast.options.clear()
        forecast.options.update(forecast_chart())
        errors.options.clear()
        errors.options.update(error_chart())
        forecast.update()
        errors.update()
        table.columns, table.rows = _error_table()
        table.update()

    async def ask() -> None:
        text = question.value.strip()
        if not text:
            return
        question.value = ""
        with messages:
            ui.chat_message(text, name="You", sent=True)
            with ui.chat_message(name="Assistant"):
                answer = ui.markdown("…")
        reply = ""
        try:
            async for event in agent.stream_async(text):
                if "data" in event:
                    reply += event["data"]
                    answer.content = reply
        except Exception as error:  # noqa: BLE001, show a stopped Ollama server or a missing model in the chat
            answer.content = f"The assistant failed: {error}"

    show_models()
    refresh.on_click(reload)
    question.on("keydown.enter", ask)


if __name__ == "__main__":
    ui.run(
        host="127.0.0.1",
        port=APP_PORT,
        title="Air quality",
        reload=False,
        show=False,
    )

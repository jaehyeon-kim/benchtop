"""Inference pipeline DAG.

- airq_inference: runs each time airq_daily loads a day, and predicts the seven
  days after that day. Airflow may combine several daily runs into one inference
  run, so it predicts for every day they loaded. It also runs after airq_training,
  for the day before, so newly registered versions have a forecast. A manual run
  can name the as-of date.

The `airq` package is uploaded beside this file.
"""

from datetime import UTC, date, datetime, timedelta

from airflow.sdk import Asset, Param, dag, task

from airq.config import DAILY_FEATURES_ASSET, MODELS_ASSET

_DAILY_FEATURES = Asset(DAILY_FEATURES_ASSET)
_MODELS = Asset(MODELS_ASSET)


@dag(
    schedule=(_DAILY_FEATURES | _MODELS),
    start_date=datetime(2026, 9, 1, tzinfo=UTC),
    catchup=False,
    params={"as_of": Param(None, type=["null", "string"], description='day the run stands on, such as "3 days ago" or YYYY-MM-DD; empty uses the days the daily runs loaded')},
    tags=["airq"],
)  # fmt: skip
def airq_inference():
    @task
    def infer(params=None, dag_run=None, triggering_asset_events=None):
        from airq.days import resolve
        from airq.infer import run

        yesterday = (dag_run.run_after - timedelta(days=1)).date()
        if params["as_of"]:
            days = {resolve(params["as_of"])}
        else:
            events = triggering_asset_events or {}
            days = {
                date.fromisoformat(event.extra["day"])
                for event in events.get(_DAILY_FEATURES, [])
            }
            if events.get(_MODELS) or not days:  # after training, or by hand
                days.add(yesterday)
        for as_of in sorted(days):
            run(as_of)

    infer()


airq_inference()

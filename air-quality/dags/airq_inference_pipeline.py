"""Inference pipeline DAG.

- airq_inference: runs each time airq_daily loads a day, and predicts the seven
  days after that day; a manual run can name the as-of date.

The `airq` package is uploaded beside this file.
"""

from datetime import UTC, date, datetime, timedelta

from airflow.sdk import Asset, Param, dag, task

from airq.config import DAILY_FEATURES_ASSET

_DAILY_FEATURES = Asset(DAILY_FEATURES_ASSET)


@dag(
    schedule=[_DAILY_FEATURES],
    start_date=datetime(2026, 9, 1, tzinfo=UTC),
    catchup=False,
    params={"as_of": Param(None, type=["null", "string"], format="date", description="day the run stands on; empty uses the day the daily run loaded")},
    tags=["airq"],
)  # fmt: skip
def airq_inference():
    @task
    def infer(params=None, dag_run=None, triggering_asset_events=None):
        from airq.infer import run

        if params["as_of"]:
            as_of = date.fromisoformat(params["as_of"])
        elif triggering_asset_events and triggering_asset_events[_DAILY_FEATURES]:
            as_of = date.fromisoformat(
                triggering_asset_events[_DAILY_FEATURES][-1].extra["day"]
            )
        else:  # a manual run with no date: the day before it
            as_of = (dag_run.run_after - timedelta(days=1)).date()
        run(as_of)

    infer()


airq_inference()

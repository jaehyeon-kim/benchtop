"""Defines the feature pipeline DAGs.

- airq_backfill: started by hand. It writes the last `n_days` of history, generated with
  `seed`.
- airq_daily: runs once a day and loads the day before. A manual run can name the day.
  Each run marks the daily features asset as updated, which starts airq_inference.

The `airq` package is uploaded next to this file. `_AIRFLOW_PIP_DEPS` installs
dynamic-des in the Airflow container.
"""

from datetime import UTC, datetime, timedelta

from airflow.sdk import Asset, Param, dag, task

from airq.config import DAILY_FEATURES_ASSET

_DAILY_FEATURES = Asset(DAILY_FEATURES_ASSET)


@dag(
    schedule=None,
    params={
        "n_days": Param(730, type="integer", minimum=1, description="days to write, ending yesterday"),
        "seed": Param(42, type="integer", description="seed for the generated values"),
    },
    tags=["airq"],
)  # fmt: skip
def airq_backfill():
    @task
    def backfill(params=None):
        from airq.feature.backfill import backfill

        backfill(params["n_days"], params["seed"])

    backfill()


@dag(
    schedule="@daily",
    start_date=datetime(2026, 9, 1, tzinfo=UTC),
    catchup=False,
    params={
        "date": Param(None, type=["null", "string"], description='day to load, such as "3 days ago" or YYYY-MM-DD; empty loads the day before the run'),
        "seed": Param(None, type=["null", "integer"], description="seed; empty uses the backfill's"),
    },
    tags=["airq"],
)  # fmt: skip
def airq_daily():
    # Marks the daily features updated, which starts airq_inference for the day.
    @task(outlets=[_DAILY_FEATURES])
    def daily(params=None, dag_run=None, outlet_events=None):
        from airq.days import resolve
        from airq.feature.daily import run

        yesterday = (dag_run.run_after - timedelta(days=1)).date()
        day = resolve(params["date"]) if params["date"] else yesterday
        run(day, params["seed"])
        outlet_events[_DAILY_FEATURES].extra = {"day": day.isoformat()}

    daily()


airq_backfill()
airq_daily()

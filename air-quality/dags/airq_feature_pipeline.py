"""Feature pipeline DAGs.

- airq_backfill: triggered by hand, writes the last `n_days` of history with `seed`.
- airq_daily: runs once a day and loads the day before; a manual run can name the day.

The `airq` package is uploaded beside this file, and dynamic-des is installed
through `_AIRFLOW_PIP_DEPS`.
"""

from datetime import UTC, date, datetime, timedelta

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
        from airq.backfill import backfill

        backfill(params["n_days"], params["seed"])

    backfill()


@dag(
    schedule="@daily",
    start_date=datetime(2026, 9, 1, tzinfo=UTC),
    catchup=False,
    params={
        "date": Param(None, type=["null", "string"], format="date", description="day to load; empty loads the day before the run"),
        "seed": Param(None, type=["null", "integer"], description="seed; empty uses the backfill's"),
    },
    tags=["airq"],
)  # fmt: skip
def airq_daily():
    # Marks the daily features updated, which starts airq_inference for the day.
    @task(outlets=[_DAILY_FEATURES])
    def daily(params=None, dag_run=None, outlet_events=None):
        from airq.daily import run

        yesterday = (dag_run.run_after - timedelta(days=1)).date()
        day = date.fromisoformat(params["date"]) if params["date"] else yesterday
        run(day, params["seed"])
        outlet_events[_DAILY_FEATURES].extra = {"day": day.isoformat()}

    daily()


airq_backfill()
airq_daily()

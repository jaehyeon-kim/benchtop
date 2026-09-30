"""Defines the feature pipeline DAG.

- airq_features: runs once a day and loads the day before. Triggered by hand, it loads
  `n_days` ending with `until`, and `reset` recreates the tables first, which is a
  backfill. Each run marks the daily features asset as updated, which starts
  airq_inference for the last day loaded.

The `airq` package is uploaded next to this file. `_AIRFLOW_PIP_DEPS` installs
dynamic-des in the Airflow container.
"""

from datetime import UTC, datetime, timedelta

from airflow.sdk import Asset, Param, dag, task

from airq.core.config import DAILY_FEATURES_ASSET

_DAILY_FEATURES = Asset(DAILY_FEATURES_ASSET)


@dag(
    schedule="@daily",
    start_date=datetime(2026, 9, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,  # runs write the same tables, so one at a time
    params={
        "n_days": Param(1, type="integer", minimum=1, description="days to load"),
        "until": Param(None, type=["null", "string"], description='last day to load, such as "today" or YYYY-MM-DD; empty loads the day before the run'),
        "seed": Param(None, type=["null", "integer"], description="seed; empty uses the stored one, or 42 on reset"),
        "reset": Param(False, type="boolean", description="recreate the tables first, for a backfill"),
    },
    tags=["airq"],
)  # fmt: skip
def airq_features():
    @task(outlets=[_DAILY_FEATURES])
    def load(params=None, dag_run=None, outlet_events=None):
        from airq.core.days import resolve
        from airq.feature.load import load

        yesterday = (dag_run.run_after - timedelta(days=1)).date()
        until = resolve(params["until"]) if params["until"] else yesterday
        load(params["n_days"], until, params["seed"], params["reset"])
        outlet_events[_DAILY_FEATURES].extra = {"day": until.isoformat()}

    load()


airq_features()

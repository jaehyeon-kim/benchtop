"""Chooses the daily features v2 adds to v1, and tests past PM2.5 values as features.

It reads the stored features only: the weather from Feast's `weather_v1` view, and the
weekend flag and past PM2.5 from `daily_air_quality`, because Feast has no view for them
until the sweep has chosen. Every candidate is scored with the same model on the same
test days, as training scores a model.

Part 1 works at lead 1. Each candidate feature set adds one feature to the set before:
weather only, then the weekend flag, then the PM2.5 of 1, 2 and 3 days before (lags 1 to
3). The chosen set is the smallest one where adding the next candidate lowers the MAE by
less than `MIN_GAIN`.

Part 2 works at every lead. On day D, the latest reading is D's own, so a forecast for
day D+N can use a lag of N days at best. For each lead N, it scores the weather and the
weekend flag with and without the N-day lag.

The results are logged to MLflow in one run named `feature-sweep`. No model is
registered.

Run: python -m airq.training.sweep
"""

import logging
from datetime import timedelta
from itertools import pairwise

import mlflow
import pandas as pd

from airq.config import EXPERIMENT, LEADS, TABLES
from airq.feature_store import FEATURE_SETS, V1_COLUMNS
from airq.iceberg import catalog
from airq.models import DailyAirQuality
from airq.training.train import fit_and_score, training_frame

logger = logging.getLogger(
    "airq.training.sweep"
)  # __name__ is "__main__" under python -m

MIN_GAIN = 0.05  # a smaller relative drop in MAE does not matter
_WEATHER_WEEKEND = [*V1_COLUMNS, "is_weekend"]
CANDIDATES = {
    "weather": V1_COLUMNS,
    "weather+weekend": _WEATHER_WEEKEND,
    "weather+weekend+lag1": [*_WEATHER_WEEKEND, "lag1"],
    "weather+weekend+lag1-2": [*_WEATHER_WEEKEND, "lag1", "lag2"],
    "weather+weekend+lag1-3": [*_WEATHER_WEEKEND, "lag1", "lag2", "lag3"],
}


def add_lags(frame: pd.DataFrame, daily_pm: pd.Series, lags: list[int]) -> pd.DataFrame:
    """
    Adds past PM2.5 values as `lag<k>` columns.

    `lag<k>` is the measured daily PM2.5 k days before the row's day. Rows where any lag
    is missing are dropped.

    Args:
        frame (pd.DataFrame): The training frame, with a `day` column.
        daily_pm (pd.Series): The measured daily PM2.5, indexed by day.
        lags (list[int]): The lags to add, in days.

    Returns:
        pd.DataFrame: A copy of the frame with the lag columns added.
    """
    frame = frame.copy()
    for k in lags:
        frame[f"lag{k}"] = (frame["day"] - timedelta(days=k)).map(daily_pm)
    return frame.dropna(subset=[f"lag{k}" for k in lags])


def _stored_frame(lead: int = 1):
    """
    Builds a training frame from the stored tables, through Feast.

    Args:
        lead (int): How many days before each day its weather forecast was issued.

    Returns:
        tuple[pd.DataFrame, pd.Series]: The training frame with the weekend flag, and
            the measured daily PM2.5 indexed by day.
    """
    frame = training_frame(*FEATURE_SETS["v1"], extra_labels=("is_weekend",), lead=lead)
    frame["day"] = frame["event_timestamp"].dt.date
    daily = catalog().load_table(TABLES[DailyAirQuality]).scan().to_pandas()
    return frame.astype({"is_weekend": "float64"}), daily.set_index("day")["pm2_5"]


def choose(mean_mae: dict[str, float]) -> str:
    """
    Chooses the smallest candidate set worth using.

    It goes through the candidates in order. It stops at the first one where adding the
    next candidate lowers the MAE by less than `MIN_GAIN`, as a share of the current
    MAE. If every step gains enough, it returns the last candidate.

    Args:
        mean_mae (dict[str, float]): The mean MAE of each candidate, in candidate order.

    Returns:
        str: The name of the chosen candidate.
    """
    names = list(mean_mae)
    for name, larger in pairwise(names):
        if (mean_mae[name] - mean_mae[larger]) / mean_mae[name] < MIN_GAIN:
            return name
    return names[-1]


def _mae(frame: pd.DataFrame, columns: list[str]) -> float:
    """
    Returns the test MAE of a model fitted on the given columns.

    Args:
        frame (pd.DataFrame): A training frame with the columns.
        columns (list[str]): The feature columns.

    Returns:
        float: The MAE on the time-ordered test days.
    """
    return fit_and_score(frame, columns)[3]["mae"]


def sweep() -> str:
    """
    Runs both parts of the sweep and logs them to MLflow.

    Returns:
        str: The name of the chosen candidate set.
    """
    frame, daily_pm = _stored_frame()
    # Lags 1 to 3 are added first, so every candidate is scored on the same days.
    lagged = add_lags(frame, daily_pm, [1, 2, 3])
    lags = {name: round(_mae(lagged, cols), 2) for name, cols in CANDIDATES.items()}
    chosen = choose(lags)

    horizon = []
    for lead in LEADS:
        frame, daily_pm = _stored_frame(lead)
        lagged = add_lags(frame, daily_pm, [lead])
        horizon.append(
            {
                "lead_days": lead,
                "mae_without_lag": round(_mae(lagged, _WEATHER_WEEKEND), 2),
                "mae_with_lag": round(
                    _mae(lagged, [*_WEATHER_WEEKEND, f"lag{lead}"]), 2
                ),
            }
        )
    horizon_table = pd.DataFrame(horizon)

    mlflow.set_experiment(EXPERIMENT)
    with mlflow.start_run(run_name="feature-sweep") as run:
        mlflow.log_param("min_gain", MIN_GAIN)
        # MLflow metric names cannot contain "+".
        mlflow.log_metrics({f"mae_{n.replace('+', '_')}": m for n, m in lags.items()})
        mlflow.set_tag("chosen", chosen)
        mlflow.log_table(horizon_table, "horizon.json")
    logger.info("Lags at lead 1, MAE (run %s): %s", run.info.run_id, lags)
    logger.info("Chosen: %s", chosen)
    logger.info("Lags at every lead, MAE:\n%s", horizon_table.to_string(index=False))
    return chosen


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    sweep()

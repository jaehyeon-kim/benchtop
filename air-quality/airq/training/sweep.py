"""Chooses the daily features v2 adds to v1, and tests past PM2.5 values as features.

Part 1 works at lead 1. Each candidate feature set adds one feature to the set before:
weather only, then the weekend flag, then the PM2.5 of 1, 2 and 3 days before (lags 1 to
3). Each set is scored on the time-ordered split and on a random split, with the same
model and the same test days. The weather comes from Feast's `weather_v1` view. The
weekend flag and the lags come from `daily_air_quality`, because Feast has no view for
them until the sweep has chosen.

The stored tables hold data from one seed. So each set is also scored on data generated
in memory with other seeds. The chosen set is the smallest one where adding the next
candidate lowers the mean MAE by less than `MIN_GAIN`.

Part 2 works at every lead. On day D, the latest reading is D's own, so a forecast for
day D+N can use a lag of N days at best. For each lead N, it scores the weather and the
weekend flag with and without the N-day lag.

The results are logged to MLflow. No model is registered.

Run: python -m airq.training.sweep --seeds 0 1 2 3 4
"""

import argparse
import logging
from datetime import datetime, timedelta
from itertools import pairwise

import mlflow
import pandas as pd

from airq.config import EXPERIMENT, LEADS, ORIGIN_PROPERTY, TABLES
from airq.feature.features import daily_features
from airq.feature.generator import generate
from airq.iceberg import catalog
from airq.models import DailyAirQuality, Observation
from airq.store.feast_repo import FEATURE_SETS, V1_COLUMNS
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


def generated_frame(origin: datetime, n_days: int, seed: int, lead: int = 1):
    """
    Builds a training frame from data generated in memory with another seed.

    It generates `n_days` of hourly records from `origin`, and turns them into daily
    features with the feature pipeline's code. Nothing is written.

    Args:
        origin (datetime): The first hour of the data.
        n_days (int): How many days to generate.
        seed (int): The seed for the generator.
        lead (int): How many days before each day its weather forecast was issued.

    Returns:
        tuple[pd.DataFrame, pd.Series]: The training frame, and the measured daily PM2.5
            indexed by day.
    """
    forecasts, observations = [], []
    records = generate(origin, seed)
    for _ in range(n_days * 24):
        hour = next(records)
        forecasts += hour[:-1]
        observations.append(hour[-1])
    weather, air_quality = daily_features(forecasts, observations)
    weather = pd.DataFrame([r.model_dump() for r in weather if r.lead_days == lead])
    air_quality = pd.DataFrame([r.model_dump() for r in air_quality])
    frame = air_quality.merge(weather, on=["location_id", "day"])
    frame["event_timestamp"] = pd.to_datetime(frame["day"], utc=True)
    frame = frame.astype({"is_weekend": "float64", "wet_hours": "float64"})
    return frame, air_quality.set_index("day")["pm2_5"]


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


def lag_table(frames: dict) -> pd.DataFrame:
    """
    Scores every candidate set on every data set, for part 1.

    Each frame gets lags 1 to 3 first, and rows missing any of them are dropped. So
    every candidate is scored on the same days.

    Args:
        frames (dict): The training frame and daily PM2.5 of each data set, by name:
            `stored` and `seed_<n>`.

    Returns:
        pd.DataFrame: One row per candidate, with its MAE on each data set on both
            splits, and the mean on each split.
    """
    rows = {}
    for name, columns in CANDIDATES.items():
        row = {}
        for data, (frame, daily_pm) in frames.items():
            metrics = fit_and_score(add_lags(frame, daily_pm, [1, 2, 3]), columns)[3]
            row[data] = metrics["mae"]
            row[f"{data}_random"] = metrics["random_split_mae"]
        row["mean"] = sum(row[d] for d in frames) / len(frames)
        row["mean_random"] = sum(row[f"{d}_random"] for d in frames) / len(frames)
        rows[name] = row
    return pd.DataFrame(rows).T.round(2)


def horizon_table(frames_by_lead: dict[int, dict]) -> pd.DataFrame:
    """
    Scores each lead with and without its lag, for part 2.

    At lead N, the only usable lag is N days. For each lead, every data set is scored
    with the weather and the weekend flag, first without the lag and then with it.

    Args:
        frames_by_lead (dict[int, dict]): The data sets of each lead, in the same form
            as for `lag_table`.

    Returns:
        pd.DataFrame: One row per lead, with the mean MAE without and with the lag.
    """
    rows = []
    for lead, frames in frames_by_lead.items():
        without, with_lag = [], []
        for frame, daily_pm in frames.values():
            lagged = add_lags(frame, daily_pm, [lead])
            without.append(fit_and_score(lagged, _WEATHER_WEEKEND)[3]["mae"])
            columns = [*_WEATHER_WEEKEND, f"lag{lead}"]
            with_lag.append(fit_and_score(lagged, columns)[3]["mae"])
        rows.append(
            {
                "lead_days": lead,
                "lag_available": f"lag{lead}",
                "mae_without_lag": sum(without) / len(without),
                "mae_with_lag": sum(with_lag) / len(with_lag),
            }
        )
    return pd.DataFrame(rows).round(2)


def sweep(seeds: list[int]) -> str:
    """
    Runs both parts of the sweep and logs them to MLflow.

    The results go to one run named `feature-sweep`, with a nested run per candidate
    set. Both tables are also written to the log.

    Args:
        seeds (list[int]): The seeds used to generate the extra data sets.

    Returns:
        str: The name of the chosen candidate set.
    """
    stored = _stored_frame()
    properties = catalog().load_table(TABLES[Observation]).properties
    origin = datetime.fromisoformat(properties[ORIGIN_PROPERTY])
    n_days = (stored[0]["event_timestamp"].max().date() - origin.date()).days + 1
    frames_by_lead = {
        lead: {"stored": stored if lead == 1 else _stored_frame(lead)}
        | {f"seed_{s}": generated_frame(origin, n_days, s, lead) for s in seeds}
        for lead in LEADS
    }

    mlflow.set_experiment(EXPERIMENT)
    with mlflow.start_run(run_name="feature-sweep") as run:
        mlflow.log_params({"seeds": ",".join(map(str, seeds)), "min_gain": MIN_GAIN})
        lags = lag_table(frames_by_lead[1])
        for name, row in lags.iterrows():
            with mlflow.start_run(run_name=name, nested=True):
                mlflow.log_param("features", ",".join(CANDIDATES[name]))
                mlflow.log_metrics({f"mae_{k}": float(v) for k, v in row.items()})
        chosen = choose(lags["mean"].to_dict())
        mlflow.set_tag("chosen", chosen)
        mlflow.log_table(lags.reset_index(names="candidate"), "lags.json")
        horizon = horizon_table(frames_by_lead)
        mlflow.log_table(horizon, "horizon.json")
    columns = ["stored", "mean", "stored_random", "mean_random"]
    logger.info(
        "Lags at lead 1, MAE (run %s):\n%s", run.info.run_id, lags[columns].to_string()
    )
    logger.info("Chosen: %s", chosen)
    logger.info("Lags at every lead, mean MAE:\n%s", horizon.to_string(index=False))
    return chosen


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--seeds",
        type=int,
        nargs="*",
        default=[0, 1, 2, 3, 4],
        help="seeds to generate extra data with (default: 0 to 4)",
    )
    sweep(parser.parse_args().seeds)

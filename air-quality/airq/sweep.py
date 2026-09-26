"""Feature sweep: which daily features v2 adds to v1's weather features, and the
book's exercise 1 on lagged PM2.5.

Part 1, lags at lead 1: one nested MLflow run per candidate set, each adding one
feature to the last: weather only, the weekend flag, then yesterday's PM2.5
(lag 1), the day before (lag 2) and the day before that (lag 3). The model and
the test days are the same in all, and each is scored on the time-ordered split
and on a random split of the same size. The weather comes from Feast's
`weather_v1` view; the weekend flag and the lags come straight from
`daily_air_quality`, because no view over them exists until the sweep has
chosen.

The stored tables hold one seed's data, so each candidate is also scored on
data generated in memory with other seeds, by the same generator and feature
code. The chosen set is the smallest one after which adding the next candidate
improves the mean MAE by less than `MIN_GAIN`.

Part 2, lags at every lead: standing on day D, the latest reading is D's own, so
a forecast for D+N can use a lag of N days at best. For each lead N, the weather
forecast at lead N and the weekend flag are scored with and without that lag.

Nothing is registered.

Run: python -m airq.sweep --seeds 0 1 2 3 4
"""

import argparse
import logging
from datetime import datetime, timedelta
from itertools import pairwise

import mlflow
import pandas as pd

from airq.config import EXPERIMENT, LEADS, ORIGIN_PROPERTY, TABLES
from airq.feast_repo import FEATURE_SETS, V1_COLUMNS
from airq.features import daily_features
from airq.generator import generate
from airq.iceberg import catalog
from airq.models import DailyAirQuality, Observation
from airq.train import fit_and_score, training_frame

logger = logging.getLogger("airq.sweep")  # __name__ is "__main__" under python -m

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
    """Adds `lag<k>`, the measured PM2.5 k days before each row's day, and drops
    rows where one is missing. `daily_pm` is indexed by day."""
    frame = frame.copy()
    for k in lags:
        frame[f"lag{k}"] = (frame["day"] - timedelta(days=k)).map(daily_pm)
    return frame.dropna(subset=[f"lag{k}" for k in lags])


def generated_frame(origin: datetime, n_days: int, seed: int, lead: int = 1):
    """The training frame's columns, and the daily PM2.5 by day, for `n_days`
    generated in memory from `origin`, with the weather forecast at `lead`."""
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
    """The stored data's training frame at `lead`, and its daily PM2.5 by day."""
    frame = training_frame(*FEATURE_SETS["v1"], extra_labels=("is_weekend",), lead=lead)
    frame["day"] = frame["event_timestamp"].dt.date
    daily = catalog().load_table(TABLES[DailyAirQuality]).scan().to_pandas()
    return frame.astype({"is_weekend": "float64"}), daily.set_index("day")["pm2_5"]


def choose(mean_mae: dict[str, float]) -> str:
    """The smallest candidate after which the next one gains less than MIN_GAIN."""
    names = list(mean_mae)
    for name, larger in pairwise(names):
        if (mean_mae[name] - mean_mae[larger]) / mean_mae[name] < MIN_GAIN:
            return name
    return names[-1]


def lag_table(frames: dict) -> pd.DataFrame:
    """Part 1: time-ordered and random-split MAE of each candidate on each data set,
    over the same days for every candidate."""
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
    """Part 2: mean MAE at each lead N, without a lag and with the lag of N days."""
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

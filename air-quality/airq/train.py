"""Training pipeline: v1 and v2, XGBoost on Feast features, registered in MLflow.

The labels come from `daily_air_quality`: `pm2_5` is the target and
`pm2_5_lag1`, yesterday's mean, is the baseline's prediction. v1's features are
Feast's `weather_v1` view, the forecast for each day issued the day before; v2
adds the weekend flag from the `calendar_v2` view. The days are split in time
order, earlier days for training and the last 20% for testing, and each model
and the baseline are scored on the same test days. Each is also scored on a
random split of the same size, which the comparison table sets beside the
time-ordered score.

Both versions are registered as `airq_pm25`. The new version of the feature set
the champion already uses gets the `champion` alias, which the inference
pipeline serves, and the other gets `challenger`, which it predicts beside it.
The champion is v1 until v2 is promoted, and a promotion survives retraining.
Each run logs a feature importance plot per model.

Feast does not pin Iceberg snapshots, so the run reads each table's snapshot id
before and after reading the data, stops if a commit landed in between, and tags
the snapshots it read so they outlive snapshot expiry.

Run: python -m airq.train
"""

import logging

import matplotlib
import matplotlib.pyplot as plt
import mlflow
import numpy as np
import pandas as pd
from mlflow.exceptions import MlflowException
from mlflow.models import infer_signature
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor, plot_importance

from airq.config import CHALLENGER, CHAMPION, EXPERIMENT, MODEL_NAME, TABLES
from airq.feast_repo import FEATURE_SETS, store
from airq.iceberg import catalog
from airq.models import DailyAirQuality, DailyWeather

matplotlib.use("Agg")  # plots go to files only, so training needs no display

logger = logging.getLogger("airq.train")  # __name__ is "__main__" under python -m

_TEST_FRACTION = 0.2
_RANDOM_STATE = 42


def split(frame: pd.DataFrame, test_fraction: float = _TEST_FRACTION):
    """Earlier days for training, the last `test_fraction` of days for testing."""
    frame = frame.sort_values("event_timestamp").reset_index(drop=True)
    cut = int(len(frame) * (1 - test_fraction))
    return frame.iloc[:cut], frame.iloc[cut:]


def random_split(frame: pd.DataFrame, test_fraction: float = _TEST_FRACTION):
    """The same sizes as `split`, with the test days drawn at random. The rows are
    sorted first, because Feast returns them in no fixed order."""
    frame = frame.sort_values("event_timestamp").reset_index(drop=True)
    return train_test_split(frame, test_size=test_fraction, random_state=_RANDOM_STATE)


def scores(actual, predicted) -> dict[str, float]:
    return {
        "mae": float(mean_absolute_error(actual, predicted)),
        "rmse": float(np.sqrt(mean_squared_error(actual, predicted))),
        "r2": float(r2_score(actual, predicted)),
    }


def training_frame(
    features: list[str],
    columns: list[str],
    extra_labels: tuple[str, ...] = (),
    lead: int = 1,
) -> pd.DataFrame:
    """One row per day: the label, the baseline, `extra_labels` from
    `daily_air_quality`, and the named Feast features at `lead` (1 for training:
    the forecast issued the day before)."""
    labels = catalog().load_table(TABLES[DailyAirQuality]).scan().to_pandas()
    days = pd.to_datetime(labels["day"], utc=True)
    entities = pd.DataFrame(
        {
            "location_id": labels["location_id"],
            "lead_days": lead,
            "event_timestamp": days,
            "day": days,  # the calendar view's request field
            "pm2_5": labels["pm2_5"],
            "pm2_5_lag1": labels["pm2_5_lag1"],
            **{name: labels[name] for name in extra_labels},
        }
    )
    frame = (
        store().get_historical_features(entity_df=entities, features=features).to_df()
    )
    # The first day has no forecast issued the day before it. Features are
    # floats, so the model's input schema accepts integer and float columns alike.
    frame = frame.dropna(subset=columns)
    return frame.astype({column: "float64" for column in columns})


def fit_and_score(frame: pd.DataFrame, columns: list[str]):
    """Fits on the time-ordered split; returns the model, its training rows and
    the scores of the model and the baseline on both splits."""
    train_set, test_set = split(frame)
    model = XGBRegressor().fit(train_set[columns], train_set["pm2_5"])
    metrics = {
        **scores(test_set["pm2_5"], model.predict(test_set[columns])),
        **{f"baseline_{k}": v for k, v in scores(test_set["pm2_5"], test_set["pm2_5_lag1"]).items()},
    }  # fmt: skip
    random_train, random_test = random_split(frame)
    random_model = XGBRegressor().fit(random_train[columns], random_train["pm2_5"])
    metrics["random_split_mae"] = scores(
        random_test["pm2_5"], random_model.predict(random_test[columns])
    )["mae"]
    metrics["random_split_baseline_mae"] = scores(
        random_test["pm2_5"], random_test["pm2_5_lag1"]
    )["mae"]
    return model, train_set, test_set, metrics


def _current_snapshots() -> dict[str, int]:
    """Each table's current snapshot id."""
    return {
        TABLES[model]: catalog()
        .load_table(TABLES[model])
        .current_snapshot()
        .snapshot_id
        for model in (DailyWeather, DailyAirQuality)
    }


def _tag_snapshots(run_id: str, snapshots: dict[str, int]) -> dict[str, str]:
    """Tags the snapshots read, so they outlive snapshot expiry; returns MLflow tags."""
    logged, tag = {}, f"mlflow-{run_id}"
    for identifier, snapshot_id in snapshots.items():
        catalog().load_table(identifier).manage_snapshots().create_tag(
            snapshot_id, tag
        ).commit()
        name = identifier.split(".")[-1]
        logged[f"{name}_snapshot_id"] = str(snapshot_id)
        logged[f"{name}_tag"] = tag
    return logged


def roles() -> dict[str, str]:
    """The alias each feature set's new version gets: the champion keeps the
    feature set it has (v1 until v2 is promoted), the other set is the challenger."""
    try:
        version = mlflow.MlflowClient().get_model_version_by_alias(MODEL_NAME, CHAMPION)
        current = version.tags.get("feature_set", "v1")
    except MlflowException:  # nothing registered yet
        current = "v1"
    return {name: CHAMPION if name == current else CHALLENGER for name in FEATURE_SETS}


def _register(name: str, alias: str, frame: pd.DataFrame, snapshots: dict[str, str]):
    """Trains, scores and registers one feature set in a nested run."""
    columns = FEATURE_SETS[name][1]
    model, train_set, test_set, metrics = fit_and_score(frame, columns)
    with mlflow.start_run(run_name=name, nested=True):
        mlflow.log_params(
            {
                "feature_set": name,
                "features": ",".join(columns),
                "train_days": len(train_set),
                "test_days": len(test_set),
                "test_start": str(test_set["event_timestamp"].min().date()),
            }
        )
        mlflow.log_metrics(metrics)
        mlflow.set_tags(snapshots)
        figure = plot_importance(model).figure
        mlflow.log_figure(figure, "feature_importance.png")
        plt.close(figure)
        info = mlflow.xgboost.log_model(
            model,
            name="model",
            signature=infer_signature(
                train_set[columns], model.predict(train_set[columns])
            ),
            input_example=train_set[columns].head(3),
            registered_model_name=MODEL_NAME,
        )
    version = str(info.registered_model_version)
    client = mlflow.MlflowClient()
    client.set_model_version_tag(MODEL_NAME, version, "feature_set", name)
    client.set_registered_model_alias(MODEL_NAME, alias, version)
    logger.info(
        "Registered %s version %s (%s) as @%s",
        MODEL_NAME,
        version,
        name,
        alias,
    )
    return version, metrics


def split_table(results: dict[str, dict[str, float]]) -> pd.DataFrame:
    """MAE on the time-ordered and the random split, for the baseline and each model."""
    first = next(iter(results.values()))
    rows = [("baseline", first["baseline_mae"], first["random_split_baseline_mae"])]
    rows += [(name, m["mae"], m["random_split_mae"]) for name, m in results.items()]
    return pd.DataFrame(
        rows, columns=["model", "time_ordered_mae", "random_split_mae"]
    ).round(2)


def train() -> dict[str, str]:
    """Trains, scores and registers v1 and v2; returns each one's new version."""
    before = _current_snapshots()
    frame = training_frame(*FEATURE_SETS["v2"])  # v2's features include v1's
    if _current_snapshots() != before:
        raise SystemExit("A table changed while training read it: run training again.")
    mlflow.set_experiment(EXPERIMENT)
    with mlflow.start_run(run_name="training") as run:
        snapshots = _tag_snapshots(run.info.run_id, before)
        mlflow.set_tags(snapshots)
        versions, results = {}, {}
        for name, alias in roles().items():
            versions[name], results[name] = _register(name, alias, frame, snapshots)
        table = split_table(results)
        mlflow.log_table(table, "split_comparison.json")
    logger.info(
        "MAE by split (run %s):\n%s", run.info.run_id, table.to_string(index=False)
    )
    return versions


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    train()

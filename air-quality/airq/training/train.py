"""Trains one model version, v1 or v2, on Feast features and registers it in MLflow.

The labels come from `daily_air_quality`. `pm2_5` is the target. `pm2_5_lag1`, the
previous day's mean, is the baseline's prediction. v1 uses Feast's `weather_v1` view,
the forecast for each day issued the day before. v2 adds the weekend flag from the
`calendar_v2` view.

The days are split in time order: the earlier 80% train the model and the last 20% test
it. The model and the baseline are scored on the same test days.

Each version is registered under `airq_pm25`, with the alias `airq.model_registry`
gives it. The run also logs a feature importance plot.

Feast does not pin Iceberg snapshots. So the run reads each table's snapshot id before
and after reading the data, and stops if they differ. It then tags the snapshots it
read, so that snapshot expiry keeps them.

Run: python -m airq.training.train --version v1   (or v2)
"""

import argparse
import logging

import matplotlib
import matplotlib.pyplot as plt
import mlflow
import numpy as np
import pandas as pd
from mlflow.models import infer_signature
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from xgboost import XGBRegressor, plot_importance

from airq.config import EXPERIMENT, MODEL_NAME, TABLES
from airq.feature_store import FEATURE_SETS, store
from airq.iceberg import catalog
from airq.model_registry import role
from airq.models import DailyAirQuality, DailyWeather

matplotlib.use("Agg")  # plots go to files only, so training needs no display

logger = logging.getLogger(
    "airq.training.train"
)  # __name__ is "__main__" under python -m

_TEST_FRACTION = 0.2


def split(frame: pd.DataFrame, test_fraction: float = _TEST_FRACTION):
    """
    Splits the rows in time order, keeping the later days for testing.

    Args:
        frame (pd.DataFrame): The training frame, with an `event_timestamp` column.
        test_fraction (float): The share of days kept for testing. The default is 0.2.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: The training rows and the test rows.
    """
    frame = frame.sort_values("event_timestamp").reset_index(drop=True)
    cut = int(len(frame) * (1 - test_fraction))
    return frame.iloc[:cut], frame.iloc[cut:]


def scores(actual, predicted) -> dict[str, float]:
    """
    Scores predictions against the measured values.

    Args:
        actual (array-like): The measured PM2.5.
        predicted (array-like): The predicted PM2.5.

    Returns:
        dict[str, float]: The mean absolute error (`mae`), the root mean squared error
            (`rmse`) and R² (`r2`).
    """
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
    """
    Builds the training frame: one row per day, with its label, baseline and features.

    It reads the labels from `daily_air_quality` and asks Feast for the features at
    `lead`. Rows with a missing feature are dropped. The first day is one of them,
    because no forecast was issued the day before it. The feature columns are converted
    to floats.

    Args:
        features (list[str]): The Feast feature references, such as
            `weather_v1:temperature_2m`.
        columns (list[str]): The feature column names the model uses.
        extra_labels (tuple[str, ...]): Other `daily_air_quality` columns to keep, such
            as `is_weekend`.
        lead (int): How many days before each day its forecast was issued. Training uses
            1.

    Returns:
        pd.DataFrame: One row per day with `pm2_5`, `pm2_5_lag1`, the extra labels and
            the features.
    """
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
    """
    Fits a model on the earlier days, and scores it and the baseline on the last days.

    Args:
        frame (pd.DataFrame): The training frame from `training_frame`.
        columns (list[str]): The feature columns the model uses.

    Returns:
        tuple: The model, its training rows, its test rows, and the scores of the model
            and of the baseline (`baseline_mae` and so on).
    """
    train_set, test_set = split(frame)
    model = XGBRegressor().fit(train_set[columns], train_set["pm2_5"])
    metrics = {
        **scores(test_set["pm2_5"], model.predict(test_set[columns])),
        **{f"baseline_{k}": v for k, v in scores(test_set["pm2_5"], test_set["pm2_5_lag1"]).items()},
    }  # fmt: skip
    return model, train_set, test_set, metrics


def _current_snapshots() -> dict[str, int]:
    """
    Reads the current snapshot id of each daily table.

    Returns:
        dict[str, int]: The snapshot id of `daily_weather` and of `daily_air_quality`,
            by table identifier.
    """
    return {
        TABLES[model]: catalog()
        .load_table(TABLES[model])
        .current_snapshot()
        .snapshot_id
        for model in (DailyWeather, DailyAirQuality)
    }


def _tag_snapshots(run_id: str, snapshots: dict[str, int]) -> dict[str, str]:
    """
    Tags the snapshots that training read, so that snapshot expiry keeps them.

    Each snapshot gets the tag `mlflow-<run_id>`.

    Args:
        run_id (str): The MLflow run that read the snapshots.
        snapshots (dict[str, int]): The snapshot id of each table, by table identifier.

    Returns:
        dict[str, str]: MLflow tags that record each table's snapshot id and tag name.
    """
    logged, tag = {}, f"mlflow-{run_id}"
    for identifier, snapshot_id in snapshots.items():
        catalog().load_table(identifier).manage_snapshots().create_tag(
            snapshot_id, tag
        ).commit()
        name = identifier.split(".")[-1]
        logged[f"{name}_snapshot_id"] = str(snapshot_id)
        logged[f"{name}_tag"] = tag
    return logged


def train(name: str) -> str:
    """
    Trains, scores and registers one feature set, in one MLflow run named after it.

    The run logs the parameters, the scores of the model and the baseline, the snapshot
    tags, a feature importance plot and the model. The new version gets a `feature_set`
    tag and the alias `model_registry.role` gives it.

    Args:
        name (str): The feature set, `v1` or `v2`.

    Returns:
        str: The new model version.

    Raises:
        SystemExit: If a table changed while training read it.
    """
    before = _current_snapshots()
    frame = training_frame(*FEATURE_SETS[name])
    if _current_snapshots() != before:
        raise SystemExit("A table changed while training read it: run training again.")
    columns = FEATURE_SETS[name][1]
    model, train_set, test_set, metrics = fit_and_score(frame, columns)
    mlflow.set_experiment(EXPERIMENT)
    with mlflow.start_run(run_name=name) as run:
        mlflow.set_tags(_tag_snapshots(run.info.run_id, before))
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
    version, alias = str(info.registered_model_version), role(name)
    client = mlflow.MlflowClient()
    client.set_model_version_tag(MODEL_NAME, version, "feature_set", name)
    client.set_registered_model_alias(MODEL_NAME, alias, version)
    logger.info(
        "Registered %s version %s (%s) as @%s: MAE %.2f, baseline %.2f",
        MODEL_NAME,
        version,
        name,
        alias,
        metrics["mae"],
        metrics["baseline_mae"],
    )
    return version


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(
        description="Trains and registers one model version."
    )
    parser.add_argument("--version", required=True, choices=list(FEATURE_SETS))
    train(parser.parse_args().version)

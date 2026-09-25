"""Training pipeline: v1, XGBoost on the lead-1 weather features, registered in MLflow.

The labels come from `daily_air_quality`: `pm2_5` is the target and
`pm2_5_lag1`, yesterday's mean, is the baseline's prediction. The features come
from Feast's `weather_v1` view, the forecast for each day issued the day before.
The days are split in time order, earlier days for training and the last 20%
for testing, and v1 and the baseline are scored on the same test days.

The new version is registered as `airq_pm25` and gets the `champion` alias,
which the inference pipeline loads. Feast does not pin Iceberg snapshots, so the
run tags the snapshot of each table it read and logs the snapshot ids and tags.

Run: python -m airq.train
"""

import logging

import mlflow
import numpy as np
import pandas as pd
from mlflow.models import infer_signature
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from xgboost import XGBRegressor

from airq.config import CHAMPION, MODEL_NAME, TABLES
from airq.feast_repo import V1_COLUMNS, WEATHER_V1, store
from airq.iceberg import catalog
from airq.models import DailyAirQuality, DailyWeather

logger = logging.getLogger("airq.train")  # __name__ is "__main__" under python -m

_EXPERIMENT = "airq"
_TEST_FRACTION = 0.2


def split(frame: pd.DataFrame, test_fraction: float = _TEST_FRACTION):
    """Earlier days for training, the last `test_fraction` of days for testing."""
    frame = frame.sort_values("event_timestamp").reset_index(drop=True)
    cut = int(len(frame) * (1 - test_fraction))
    return frame.iloc[:cut], frame.iloc[cut:]


def scores(actual, predicted) -> dict[str, float]:
    return {
        "mae": float(mean_absolute_error(actual, predicted)),
        "rmse": float(np.sqrt(mean_squared_error(actual, predicted))),
        "r2": float(r2_score(actual, predicted)),
    }


def _training_set() -> pd.DataFrame:
    """One row per day: the label, the baseline and the lead-1 weather features."""
    labels = catalog().load_table(TABLES[DailyAirQuality]).scan().to_pandas()
    entities = pd.DataFrame(
        {
            "location_id": labels["location_id"],
            "lead_days": 1,
            "event_timestamp": pd.to_datetime(labels["day"], utc=True),
            "pm2_5": labels["pm2_5"],
            "pm2_5_lag1": labels["pm2_5_lag1"],
        }
    )
    frame = (
        store().get_historical_features(entity_df=entities, features=WEATHER_V1).to_df()
    )
    # The first day has no forecast issued the day before it. Features are
    # floats, so the model's input schema accepts integer and float columns alike.
    frame = frame.dropna(subset=V1_COLUMNS)
    return frame.astype({column: "float64" for column in V1_COLUMNS})


def _tag_snapshots(run_id: str) -> dict[str, str]:
    """Tags the current snapshot of each table read, so it outlives snapshot expiry."""
    logged = {}
    for model in (DailyWeather, DailyAirQuality):
        table = catalog().load_table(TABLES[model])
        snapshot_id = table.current_snapshot().snapshot_id
        tag = f"mlflow-{run_id}"
        table.manage_snapshots().create_tag(snapshot_id, tag).commit()
        name = TABLES[model].split(".")[-1]
        logged[f"{name}_snapshot_id"] = str(snapshot_id)
        logged[f"{name}_tag"] = tag
    return logged


def train() -> str:
    """Trains, scores and registers v1; returns the new model version."""
    frame = _training_set()
    train_set, test_set = split(frame)
    model = XGBRegressor().fit(train_set[V1_COLUMNS], train_set["pm2_5"])
    predicted = model.predict(test_set[V1_COLUMNS])

    mlflow.set_experiment(_EXPERIMENT)
    with mlflow.start_run(run_name="v1") as run:
        mlflow.log_params(
            {
                "features": ",".join(V1_COLUMNS),
                "train_days": len(train_set),
                "test_days": len(test_set),
                "test_start": str(test_set["event_timestamp"].min().date()),
            }
        )
        mlflow.log_metrics(
            {f"v1_{k}": v for k, v in scores(test_set["pm2_5"], predicted).items()}
        )
        mlflow.log_metrics(
            {
                f"baseline_{k}": v
                for k, v in scores(test_set["pm2_5"], test_set["pm2_5_lag1"]).items()
            }
        )
        mlflow.set_tags(_tag_snapshots(run.info.run_id))
        info = mlflow.xgboost.log_model(
            model,
            name="model",
            signature=infer_signature(train_set[V1_COLUMNS], predicted),
            input_example=train_set[V1_COLUMNS].head(3),
            registered_model_name=MODEL_NAME,
        )
    version = str(info.registered_model_version)
    mlflow.MlflowClient().set_registered_model_alias(MODEL_NAME, CHAMPION, version)
    logger.info(
        "Registered %s version %s as @%s (run %s)",
        MODEL_NAME,
        version,
        CHAMPION,
        run.info.run_id,
    )
    return version


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    train()

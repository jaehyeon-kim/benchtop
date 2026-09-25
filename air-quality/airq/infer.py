"""Inference: PM2.5 for the seven days after an as-of date, into Iceberg.

Standing on as-of date D, the forecast available is the one issued on D: lead N
for day D+N. The run reads those seven rows of `daily_weather` through Feast,
predicts with the champion model and replaces the predictions made for D in one
overwrite, so a rerun replaces the run. No observed value after D is read.

The hindcast joins the predictions to the measured daily PM2.5 once it arrives
and reports the mean absolute error by lead and by as-of date.

Run: python -m airq.infer --as-of 2026-09-20   (default: yesterday, UTC)
     python -m airq.infer --hindcast
"""

import argparse
import logging
from datetime import UTC, date, datetime, time, timedelta

import mlflow
import pandas as pd
from mlflow import MlflowClient

from airq.config import CHAMPION, LEADS, MODEL_NAME, PREDICTIONS, STATION, TABLES
from airq.feast_repo import V1_COLUMNS, WEATHER_V1, store
from airq.iceberg import arrow_schema, catalog, to_arrow
from airq.models import DailyAirQuality, Prediction

logger = logging.getLogger("airq.infer")  # __name__ is "__main__" under python -m


def _entity_rows(as_of: date) -> pd.DataFrame:
    """One Feast entity row per lead: the forecast issued on `as_of` for `as_of` + lead."""
    return pd.DataFrame(
        {
            "location_id": STATION,
            "lead_days": list(LEADS),
            "event_timestamp": [
                datetime.combine(as_of + timedelta(days=n), time(), UTC) for n in LEADS
            ],
        }
    )


def _predictions(
    as_of: date, features: pd.DataFrame, pm2_5: list[float], version: str
) -> list[Prediction]:
    return [
        Prediction(
            location_id=row.location_id,
            as_of=as_of,
            day=row.event_timestamp.date(),
            lead_days=row.lead_days,
            pm2_5=round(float(p), 2),
            model_version=version,
        )
        for row, p in zip(features.itertuples(), pm2_5)
    ]


def run(as_of: date, model_name: str = MODEL_NAME) -> list[Prediction]:
    features = (
        store()
        .get_historical_features(entity_df=_entity_rows(as_of), features=WEATHER_V1)
        .to_df()
        .sort_values("lead_days", ignore_index=True)
    )
    if features[V1_COLUMNS].isna().any().any():
        raise SystemExit(
            f"daily_weather has no complete forecast issued on {as_of}: "
            "run the feature pipeline for that day first."
        )
    version = MlflowClient().get_model_version_by_alias(model_name, CHAMPION).version
    model = mlflow.pyfunc.load_model(f"models:/{model_name}/{version}")
    rows = _predictions(as_of, features, model.predict(features[V1_COLUMNS]), version)

    cat = catalog()
    table = cat.create_table_if_not_exists(PREDICTIONS, schema=arrow_schema(Prediction))
    table.overwrite(
        to_arrow(Prediction, rows), overwrite_filter=f"as_of = '{as_of.isoformat()}'"
    )
    logger.info(
        "%s: %d rows as of %s, %s version %s",
        PREDICTIONS,
        len(rows),
        as_of,
        model_name,
        version,
    )
    return rows


def _errors(predictions: pd.DataFrame, observed: pd.DataFrame) -> pd.DataFrame:
    """Predictions joined to the measured daily PM2.5, for the days that have one."""
    joined = predictions.merge(
        observed[["location_id", "day", "pm2_5"]],
        on=["location_id", "day"],
        suffixes=("_predicted", "_observed"),
    )
    joined["abs_error"] = (joined["pm2_5_predicted"] - joined["pm2_5_observed"]).abs()
    return joined


def hindcast() -> dict[str, pd.Series]:
    cat = catalog()
    errors = _errors(
        cat.load_table(PREDICTIONS).scan().to_pandas(),
        cat.load_table(TABLES[DailyAirQuality]).scan().to_pandas(),
    )
    return {
        "lead_days": errors.groupby("lead_days")["abs_error"].mean().round(2),
        "as_of": errors.groupby("as_of")["abs_error"].mean().round(2),
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--as-of",
        type=date.fromisoformat,
        default=datetime.now(UTC).date() - timedelta(days=1),
        help="day the run stands on, YYYY-MM-DD (default: yesterday, UTC)",
    )
    parser.add_argument(
        "--hindcast", action="store_true", help="report the error of past predictions"
    )
    args = parser.parse_args()
    if args.hindcast:
        for by, mae in hindcast().items():
            print(f"MAE by {by}:\n{mae.to_string()}\n")
    else:
        run(args.as_of)

"""Predicts PM2.5 for the seven days after an as-of date and writes it to Iceberg.

On as-of date D, the latest weather forecast is the one issued on D. So day D+N uses the
forecast issued N days before it. The run reads those seven `daily_weather` rows and
each day's weekend flag through Feast. No value measured after D is read.

The run predicts with the champion and, when one is registered, the challenger. Each
model version reads the feature set named in its `feature_set` tag. The predictions for
D replace any earlier predictions for D in one commit, so a rerun replaces the run.

The hindcast joins the predictions to the measured daily PM2.5 once it exists. It
reports each model version's mean absolute error by lead and by as-of date.

Run: python -m airq.inference --as-of "3 days ago"   (default: yesterday, UTC)
     python -m airq.inference --days 30   (the 30 as-of dates ending yesterday)
     python -m airq.inference --hindcast
"""

import argparse
import logging
from datetime import UTC, date, datetime, time, timedelta

import mlflow
import pandas as pd
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

from airq.config import (
    CHALLENGER,
    CHAMPION,
    LEADS,
    MODEL_NAME,
    PREDICTIONS,
    STATION,
    TABLES,
)
from airq.days import FORMS
from airq.days import argument as day_argument
from airq.iceberg import arrow_schema, catalog, to_arrow
from airq.models import DailyAirQuality, Prediction
from airq.store.feast_repo import FEATURE_SETS, store

logger = logging.getLogger(
    "airq.inference.infer"
)  # __name__ is "__main__" under python -m


def _entity_rows(as_of: date) -> pd.DataFrame:
    """
    Builds the Feast entity rows for a run on `as_of`, one row per lead.

    The row for lead N asks for the forecast issued on `as_of` for the day N days later.
    Its `day` column is the request field of the `calendar_v2` view.

    Args:
        as_of (date): The date the run treats as today.

    Returns:
        pd.DataFrame: One row per lead, with `location_id`, `lead_days`,
            `event_timestamp` and `day`.
    """
    days = [datetime.combine(as_of + timedelta(days=n), time(), UTC) for n in LEADS]
    return pd.DataFrame(
        {
            "location_id": STATION,
            "lead_days": list(LEADS),
            "event_timestamp": days,
            "day": days,  # the calendar view's request field
        }
    )


def _predictions(
    as_of: date, features: pd.DataFrame, pm2_5: list[float], version: str
) -> list[Prediction]:
    """
    Turns one model version's predicted values into `Prediction` rows.

    Args:
        as_of (date): The date the run treats as today.
        features (pd.DataFrame): The feature rows, one per lead, in the order of
            `pm2_5`.
        pm2_5 (list[float]): The predicted PM2.5 for each row of `features`.
        version (str): The model version that made the predictions.

    Returns:
        list[Prediction]: One row per lead, with PM2.5 rounded to two decimal places.
    """
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


def _served_versions(model_name: str) -> list:
    """
    Returns the model versions that inference predicts with.

    The champion comes first. The challenger follows when it is set and is a different
    version. Straight after a promotion, both aliases can point at the same version.

    Args:
        model_name (str): The registered model in MLflow.

    Returns:
        list: The MLflow model version of the champion, then of the challenger if there
            is one.

    Raises:
        SystemExit: If no version holds the champion alias.
    """
    client = MlflowClient()
    try:
        versions = [client.get_model_version_by_alias(model_name, CHAMPION)]
    except MlflowException:
        raise SystemExit(
            f"No {model_name} version is the {CHAMPION}: run the training pipeline first."
        ) from None
    try:
        challenger = client.get_model_version_by_alias(model_name, CHALLENGER)
    except MlflowException:  # no challenger registered
        return versions
    if challenger.version != versions[0].version:
        versions.append(challenger)
    return versions


def run(as_of: date, model_name: str = MODEL_NAME) -> list[Prediction]:
    """
    Predicts PM2.5 for the seven days after `as_of` with every served model version.

    It reads the features of both feature sets through Feast. Each served model version
    predicts from its own feature set. A version without a `feature_set` tag was
    registered before v2, so it reads the v1 features. The predictions then replace
    those stored for `as_of`.

    Args:
        as_of (date): The date the run treats as today.
        model_name (str): The registered model in MLflow.

    Returns:
        list[Prediction]: The predictions written, seven for each model version.

    Raises:
        SystemExit: If the forecast issued on `as_of` is incomplete, or no version is
            the champion.
    """
    references = list(
        dict.fromkeys(r for refs, _ in FEATURE_SETS.values() for r in refs)
    )
    columns = list(dict.fromkeys(c for _, cols in FEATURE_SETS.values() for c in cols))
    features = (
        store()
        .get_historical_features(entity_df=_entity_rows(as_of), features=references)
        .to_df()
        .sort_values("lead_days", ignore_index=True)
    )
    if features[columns].isna().any().any():
        raise SystemExit(
            f"daily_weather has no complete forecast issued on {as_of}: "
            "run the feature pipeline for that day first."
        )
    features = features.astype({column: "float64" for column in columns})
    rows = []
    for version in _served_versions(model_name):
        # Versions registered before v2 carry no tag; they are v1.
        inputs = FEATURE_SETS[version.tags.get("feature_set", "v1")][1]
        model = mlflow.pyfunc.load_model(f"models:/{model_name}/{version.version}")
        predicted = model.predict(features[inputs])
        rows += _predictions(as_of, features, predicted, version.version)

    cat = catalog()
    table = cat.create_table_if_not_exists(PREDICTIONS, schema=arrow_schema(Prediction))
    table.overwrite(
        to_arrow(Prediction, rows), overwrite_filter=f"as_of = '{as_of.isoformat()}'"
    )
    logger.info(
        "%s: %d rows as of %s, %s versions %s",
        PREDICTIONS,
        len(rows),
        as_of,
        model_name,
        sorted({r.model_version for r in rows}),
    )
    return rows


def errors(predictions: pd.DataFrame, observed: pd.DataFrame) -> pd.DataFrame:
    """
    Joins predictions to the measured daily PM2.5 and adds the absolute error.

    Only the days that have a measurement are kept.

    Args:
        predictions (pd.DataFrame): Rows of the predictions table.
        observed (pd.DataFrame): Rows of the `daily_air_quality` table.

    Returns:
        pd.DataFrame: The joined rows, with `pm2_5_predicted`, `pm2_5_observed` and
            `abs_error`.
    """
    joined = predictions.merge(
        observed[["location_id", "day", "pm2_5"]],
        on=["location_id", "day"],
        suffixes=("_predicted", "_observed"),
    )
    joined["abs_error"] = (joined["pm2_5_predicted"] - joined["pm2_5_observed"]).abs()
    return joined


def hindcast() -> dict[str, pd.Series]:
    """
    Scores the stored predictions against the measured daily PM2.5.

    Returns:
        dict[str, pd.Series]: The mean absolute error of each model version, rounded to
            two decimal places. The `lead_days` entry groups it by lead, and the `as_of`
            entry groups it by as-of date.

    Raises:
        SystemExit: If the predictions table does not exist yet.
    """
    cat = catalog()
    if not cat.table_exists(PREDICTIONS):
        raise SystemExit("No predictions yet: run the inference pipeline first.")
    joined = errors(
        cat.load_table(PREDICTIONS).scan().to_pandas(),
        cat.load_table(TABLES[DailyAirQuality]).scan().to_pandas(),
    )
    return {
        by: joined.groupby(["model_version", by])["abs_error"].mean().round(2)
        for by in ("lead_days", "as_of")
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--as-of",
        type=day_argument,
        default="yesterday",
        help=f"day the run stands on: {FORMS} (default: yesterday, UTC)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=1,
        help="run for this many as-of dates, ending at --as-of (default: 1)",
    )
    parser.add_argument(
        "--hindcast", action="store_true", help="report the error of past predictions"
    )
    args = parser.parse_args()
    if args.hindcast:
        for by, mae in hindcast().items():
            print(f"MAE by {by}:\n{mae.to_string()}\n")
    else:
        for n in reversed(range(args.days)):
            run(args.as_of - timedelta(days=n))

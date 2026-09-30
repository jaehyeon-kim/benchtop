"""Predicts PM2.5 for the seven days after an as-of date and writes it to Iceberg.

On as-of date D, the latest weather forecast is the one issued on D. So day D+N uses the
forecast issued N days before it. The run reads those seven `daily_weather` rows and
each day's weekend flag through Feast. No value measured after D is read.

The run predicts with the champion and, when one is registered, the challenger. Each
model version reads the feature set named in its `feature_set` tag. The predictions for
D replace any earlier predictions for D in one commit, so a rerun replaces the run.

The hindcast joins the predictions to the measured daily PM2.5 once it exists. It
reports each model version's mean absolute error by lead.

Run: python -m airq.inference.infer --as-of "3 days ago"   (default: yesterday, UTC)
     python -m airq.inference.infer --days 30   (the 30 as-of dates ending yesterday)
     python -m airq.inference.infer --hindcast
"""

import argparse
import logging
from datetime import UTC, date, datetime, time, timedelta

import mlflow
import pandas as pd

from airq.core.config import (
    LEADS,
    MODEL_NAME,
    PREDICTIONS,
    STATION,
    TABLES,
)
from airq.core.days import FORMS
from airq.core.days import argument as day_argument
from airq.core.models import DailyAirQuality, Prediction
from airq.stores import model_registry
from airq.stores.feature_store import FEATURE_SETS, store
from airq.stores.iceberg import arrow_schema, catalog, to_arrow

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


def _feature_request(served: list) -> tuple[list[str], list[str]]:
    """
    Lists the Feast features and columns the served versions need.

    Only these views are requested, so inference works before v2's view is registered.

    Args:
        served (list): The served model versions, from `model_registry.served`.

    Returns:
        tuple[list[str], list[str]]: The feature references and the column names, each
            listed once.
    """
    sets = [FEATURE_SETS[v.tags["feature_set"]] for v in served]
    references = list(dict.fromkeys(r for refs, _ in sets for r in refs))
    columns = list(dict.fromkeys(c for _, cols in sets for c in cols))
    return references, columns


def run(as_of: date) -> list[Prediction]:
    """
    Predicts PM2.5 for the seven days after `as_of` with every served model version.

    It reads through Feast only the features the served versions need. Each version
    predicts from its own feature set. The predictions then replace those stored for
    `as_of`.

    Args:
        as_of (date): The date the run treats as today.

    Returns:
        list[Prediction]: The predictions written, seven for each model version.

    Raises:
        SystemExit: If the forecast issued on `as_of` is incomplete, or no version is
            the champion.
    """
    served = [version for _, version in model_registry.served()]
    if not served:
        raise SystemExit(
            f"No {MODEL_NAME} version is the champion: run the training pipeline first."
        )
    references, columns = _feature_request(served)
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
    for version in served:
        inputs = FEATURE_SETS[version.tags["feature_set"]][1]
        model = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}/{version.version}")
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
        MODEL_NAME,
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


def score(
    predictions: pd.DataFrame, observed: pd.DataFrame, days: int | None = None
) -> pd.DataFrame:
    """
    Returns each model version's mean absolute error for each lead.

    This is the one scoring of predictions: the hindcast prints it, and the app's error
    table shows it.

    Args:
        predictions (pd.DataFrame): Rows of the predictions table.
        observed (pd.DataFrame): Rows of the `daily_air_quality` table.
        days (int, optional): Score only the last `days` measured days. None scores all.

    Returns:
        pd.DataFrame: One row per model version and lead, with the error (`mae`) and the
            number of days scored (`days`).
    """
    joined = errors(predictions, observed)
    if days is not None and not observed.empty:
        joined = joined[joined["day"] > observed["day"].max() - timedelta(days=days)]
    return (
        joined.groupby(["model_version", "lead_days"])["abs_error"]
        .agg(mae="mean", days="count")
        .round(2)
        .reset_index()
    )


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
        cat = catalog()
        if not cat.table_exists(PREDICTIONS):
            raise SystemExit("No predictions yet: run the inference pipeline first.")
        table = score(
            cat.load_table(PREDICTIONS).scan().to_pandas(),
            cat.load_table(TABLES[DailyAirQuality]).scan().to_pandas(),
        )
        print(table.to_string(index=False))
    else:
        for n in reversed(range(args.days)):
            run(args.as_of - timedelta(days=n))

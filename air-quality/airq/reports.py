"""Queries shared by the app's Monitoring tab and the assistant: the models in
use, the forecast, the observed PM2.5 and the error of past predictions. They
read Iceberg and MLflow and write nothing.
"""

from datetime import date, timedelta

import pandas as pd
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

from airq.config import CHALLENGER, CHAMPION, MODEL_NAME, PREDICTIONS, TABLES
from airq.iceberg import catalog
from airq.infer import errors
from airq.models import DailyAirQuality, Prediction


def _scan(identifier: str) -> pd.DataFrame:
    """The table's rows, or no rows with its columns when it does not exist yet,
    as with predictions straight after a backfill."""
    cat = catalog()
    if not cat.table_exists(identifier):
        model = {name: model for model, name in TABLES.items()}.get(
            identifier, Prediction
        )
        return pd.DataFrame(columns=list(model.model_fields))
    return cat.load_table(identifier).scan().to_pandas()


def served_models() -> list[dict]:
    """The champion and, when set, the challenger: alias, version and feature set."""
    client, served = MlflowClient(), []
    for alias in (CHAMPION, CHALLENGER):
        try:
            version = client.get_model_version_by_alias(MODEL_NAME, alias)
        except MlflowException:  # alias not set
            continue
        served.append(
            {
                "alias": alias,
                "version": version.version,
                "feature_set": version.tags.get("feature_set", "v1"),
            }
        )
    return served


def _with_alias(frame: pd.DataFrame) -> pd.DataFrame:
    """Adds each row's alias. A version holding both aliases, as straight after a
    promotion, appears once under each; a version with none gets an empty alias."""
    served = pd.DataFrame(served_models(), columns=["alias", "version", "feature_set"])
    served = served[["alias", "version"]].rename(columns={"version": "model_version"})
    joined = frame.merge(served, on="model_version", how="left")
    return joined.assign(alias=joined["alias"].fillna(""))


def forecast(as_of: date | None = None) -> pd.DataFrame:
    """The forecast available on `as_of`: the latest run made on or before it, or
    the latest run of all when it is None."""
    predictions = _scan(PREDICTIONS)
    if as_of is not None:
        predictions = predictions[predictions["as_of"] <= as_of]
    if predictions.empty:
        return predictions
    rows = predictions[predictions["as_of"] == predictions["as_of"].max()]
    columns = ["as_of", "day", "lead_days", "pm2_5", "model_version"]
    return _with_alias(rows[columns].sort_values(["model_version", "day"]))


def observed(start: date, end: date) -> pd.DataFrame:
    """Measured daily mean PM2.5 from `start` to `end`, both included."""
    daily = _scan(TABLES[DailyAirQuality])
    rows = daily[(daily["day"] >= start) & (daily["day"] <= end)]
    return rows[["day", "pm2_5"]].sort_values("day", ignore_index=True)


def last_measured_day() -> date | None:
    """The latest day with a measured daily mean, or None before any reading."""
    days = _scan(TABLES[DailyAirQuality])["day"]
    return None if days.empty else days.max()


def history(days: int) -> pd.DataFrame:
    """Lead-1 predictions beside the measured value, for the last `days` measured days."""
    daily = _scan(TABLES[DailyAirQuality])
    joined = errors(_scan(PREDICTIONS), daily)
    if daily.empty:
        return joined
    start = daily["day"].max() - timedelta(days=days - 1)
    rows = joined[(joined["lead_days"] == 1) & (joined["day"] >= start)]
    return rows.sort_values(["model_version", "day"], ignore_index=True)


def model_error(days: int = 30) -> pd.DataFrame:
    """Each model version's mean absolute error by lead, over predictions for the
    last `days` measured days."""
    daily = _scan(TABLES[DailyAirQuality])
    joined = errors(_scan(PREDICTIONS), daily)
    if not daily.empty:
        joined = joined[joined["day"] > daily["day"].max() - timedelta(days=days)]
    table = (
        joined.groupby(["model_version", "lead_days"])["abs_error"]
        .agg(mae="mean", days="count")
        .round(2)
        .reset_index()
    )
    return _with_alias(table)


def error_by_lead(days: int = 30) -> pd.DataFrame:
    """One row per lead: the champion's and the challenger's MAE and which is lower,
    so a reader need not compare the numbers."""
    table = model_error(days)
    table = table[table["alias"] != ""]
    wide = table.pivot(index="lead_days", columns="alias", values="mae")
    for alias in (CHAMPION, CHALLENGER):
        if alias not in wide:
            wide[alias] = float("nan")
    wide = wide[[CHAMPION, CHALLENGER]].add_suffix("_mae").reset_index()
    wide["lower_error"] = [
        _lower(c, h) for c, h in zip(wide[f"{CHAMPION}_mae"], wide[f"{CHALLENGER}_mae"])
    ]
    return wide


def _lower(champion: float, challenger: float) -> str:
    if pd.isna(challenger):
        return CHAMPION
    if pd.isna(champion):
        return CHALLENGER
    if champion == challenger:
        return "equal"
    return CHAMPION if champion < challenger else CHALLENGER

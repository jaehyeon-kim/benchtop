"""Queries shared by the Monitoring tab and the assistant.

They return the models in use, the forecast, the measured PM2.5 and the error of
past predictions. They read Iceberg and MLflow and never write.
"""

from datetime import date, timedelta

import pandas as pd

from airq import model_registry
from airq.config import CHALLENGER, CHAMPION, PREDICTIONS, TABLES
from airq.iceberg import catalog
from airq.inference.infer import errors, score
from airq.models import DailyAirQuality, Prediction


def _scan(identifier: str) -> pd.DataFrame:
    """
    Reads a whole Iceberg table.

    A table that does not exist yet gives an empty frame with its columns. This
    happens to the predictions table straight after a backfill.

    Args:
        identifier (str): The table name, such as `airq.predictions`.

    Returns:
        pd.DataFrame: The table's rows.
    """
    cat = catalog()
    if not cat.table_exists(identifier):
        model = {name: model for model, name in TABLES.items()}.get(
            identifier, Prediction
        )
        return pd.DataFrame(columns=list(model.model_fields))
    return cat.load_table(identifier).scan().to_pandas()


def served_models() -> list[dict]:
    """
    Lists the models the aliases point at.

    Returns:
        list[dict]: The champion, then the challenger when one is set. Each has its
            `alias`, `version` and `feature_set`.
    """
    return [
        {"alias": alias, "version": v.version, "feature_set": v.tags["feature_set"]}
        for alias, v in model_registry.served()
    ]


def _with_alias(frame: pd.DataFrame) -> pd.DataFrame:
    """
    Adds an `alias` column to rows that have a `model_version`.

    A version with no alias gets an empty string.

    Args:
        frame (pd.DataFrame): Rows with a `model_version` column.

    Returns:
        pd.DataFrame: The rows with an `alias` column.
    """
    served = pd.DataFrame(served_models(), columns=["alias", "version", "feature_set"])
    served = served[["alias", "version"]].rename(columns={"version": "model_version"})
    joined = frame.merge(served, on="model_version", how="left")
    return joined.assign(alias=joined["alias"].fillna(""))


def forecast(as_of: date | None = None) -> pd.DataFrame:
    """
    Returns the forecast that was available on a date.

    Args:
        as_of (date | None): The date. The latest run made on or before it is
            returned. If None, the latest run of all is returned.

    Returns:
        pd.DataFrame: One row per model version and day predicted, with its alias.
            Empty if no run was made by `as_of`.
    """
    predictions = _scan(PREDICTIONS)
    if as_of is not None:
        predictions = predictions[predictions["as_of"] <= as_of]
    if predictions.empty:
        return predictions
    rows = predictions[predictions["as_of"] == predictions["as_of"].max()]
    columns = ["as_of", "day", "lead_days", "pm2_5", "model_version"]
    return _with_alias(rows[columns].sort_values(["model_version", "day"]))


def observed(start: date, end: date) -> pd.DataFrame:
    """
    Returns the measured daily mean PM2.5 for a range of days.

    Args:
        start (date): The first day, included.
        end (date): The last day, included.

    Returns:
        pd.DataFrame: The `day` and `pm2_5` columns, sorted by day.
    """
    daily = _scan(TABLES[DailyAirQuality])
    rows = daily[(daily["day"] >= start) & (daily["day"] <= end)]
    return rows[["day", "pm2_5"]].sort_values("day", ignore_index=True)


def last_measured_day() -> date | None:
    """
    Returns the latest day that has a measured daily mean.

    Returns:
        date | None: The day, or None before the first reading.
    """
    days = _scan(TABLES[DailyAirQuality])["day"]
    return None if days.empty else days.max()


def history(days: int) -> pd.DataFrame:
    """
    Returns the 1-day-ahead predictions beside the measured values.

    Args:
        days (int): How many days to cover, counted back from the last measured day.

    Returns:
        pd.DataFrame: One row per model version and day, with the prediction, the
            measured value and the absolute error.
    """
    daily = _scan(TABLES[DailyAirQuality])
    joined = errors(_scan(PREDICTIONS), daily)
    if daily.empty:
        return joined
    start = daily["day"].max() - timedelta(days=days - 1)
    rows = joined[(joined["lead_days"] == 1) & (joined["day"] >= start)]
    return rows.sort_values(["model_version", "day"], ignore_index=True)


def model_error(days: int = 30) -> pd.DataFrame:
    """
    Returns each model version's mean absolute error for each lead, with its alias.

    Args:
        days (int): How many days to score, counted back from the last measured day.

    Returns:
        pd.DataFrame: The rows of `infer.score`, with each version's alias.
    """
    return _with_alias(score(_scan(PREDICTIONS), _scan(TABLES[DailyAirQuality]), days))


def error_by_lead(days: int = 30) -> pd.DataFrame:
    """
    Returns the champion's and the challenger's error side by side, for each lead.

    The lower one is named, so the reader does not have to compare the numbers.

    Args:
        days (int): How many days to score, counted back from the last measured day.

    Returns:
        pd.DataFrame: One row per lead, with `champion_mae`, `challenger_mae` and
            `lower_error`. A missing model gives NaN.
    """
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
    """
    Names the model with the lower error.

    Args:
        champion (float): The champion's error, NaN when it has none.
        challenger (float): The challenger's error, NaN when it has none.

    Returns:
        str: "champion", "challenger" or "equal". A model without an error loses.
    """
    if pd.isna(challenger):
        return CHAMPION
    if pd.isna(champion):
        return CHALLENGER
    if champion == challenger:
        return "equal"
    return CHAMPION if champion < challenger else CHALLENGER

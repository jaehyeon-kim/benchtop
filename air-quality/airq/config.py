"""Settings shared by the pipelines and the tests.

On the terminal, the block below points the code at the odctl services as seen
from the host, as 127.0.0.1 rather than localhost: with IPv6 enabled in Docker,
some services reset connections to the IPv6 localhost. Inside a container it
does nothing, because odctl sets the container addresses there.
"""

import os

from airq.models import DailyAirQuality, DailyWeather, Observation, WeatherForecast

if not os.path.exists("/.dockerenv"):  # Docker creates this file in every container
    os.environ.update(
        {
            "PYICEBERG_CATALOG__ODCTL__TYPE": "rest",
            "PYICEBERG_CATALOG__ODCTL__URI": "http://127.0.0.1:8181",
            "PYICEBERG_CATALOG__ODCTL__WAREHOUSE": "s3://warehouse/",
            "PYICEBERG_CATALOG__ODCTL__S3__ENDPOINT": "http://127.0.0.1:8333",
            "PYICEBERG_CATALOG__ODCTL__S3__ACCESS_KEY_ID": "user",
            "PYICEBERG_CATALOG__ODCTL__S3__SECRET_ACCESS_KEY": "password",
            "PYICEBERG_CATALOG__ODCTL__S3__REGION": "us-east-1",
            "PYICEBERG_CATALOG__ODCTL__S3__PATH_STYLE_ACCESS": "true",
            "MLFLOW_TRACKING_URI": "http://127.0.0.1:5004",
            # The server hands out download links to http://seaweed:8333, which
            # the host cannot resolve; this sends model downloads through it.
            "MLFLOW_ENABLE_PROXY_MULTIPART_DOWNLOAD": "false",
            "FEAST_REGISTRY": "postgresql+psycopg://user:password@127.0.0.1:5432/feast",
        }
    )

CATALOG = "odctl"  # the PyIceberg catalog name the variables above configure
DEFAULT_SEED = 42
STATION = "station-1"
LEADS = range(1, 8)  # forecast lead times, in days

NAMESPACE = "airq"
# The feature pipeline's tables; the backfill recreates exactly these.
TABLES = {
    Observation: f"{NAMESPACE}.observations",
    WeatherForecast: f"{NAMESPACE}.weather_forecasts",
    DailyWeather: f"{NAMESPACE}.daily_weather",
    DailyAirQuality: f"{NAMESPACE}.daily_air_quality",
}
PREDICTIONS = f"{NAMESPACE}.predictions"  # written by the inference pipeline
# Table properties the backfill writes and the daily run reads, so the daily run
# generates from the same first hour with the same seed as the backfill did.
ORIGIN_PROPERTY = "airq.origin"
SEED_PROPERTY = "airq.seed"

FEAST_PROJECT = "airq"
MODEL_NAME = "airq_pm25"  # registered model in MLflow
CHAMPION = "champion"  # alias of the version inference loads
# Airflow asset the daily run updates and the inference DAG is scheduled on.
DAILY_FEATURES_ASSET = "airq_daily_features"

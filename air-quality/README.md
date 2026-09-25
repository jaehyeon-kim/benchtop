# air-quality

An ML system that forecasts daily PM2.5, a measure of fine-particle air pollution, for the next seven days from weather forecasts. It runs entirely on local services:

- **Data:** hourly weather forecasts and PM2.5 readings are simulated with [dynamic-des](https://jaehyeon-kim.github.io/dynamic-des/), a Python library that runs discrete-event simulations and writes their output to sinks such as Kafka, Parquet and Iceberg. Nothing calls an external service.
- **Storage and features:** the data lands in Apache Iceberg tables, an open table format, stored on SeaweedFS, an S3-compatible object store. Feast, a feature store, defines daily features over those tables and serves the same values to training and prediction.
- **Models:** XGBoost models are tracked in MLflow, whose model registry keeps each trained version and marks the one in use.
- **Scheduling:** Apache Airflow runs the pipelines on a schedule.
- **Infrastructure:** [odctl](https://github.com/jaehyeon-kim/odctl), a command-line tool that starts a local data stack (Kafka, Spark, Flink, Iceberg, Airflow, MLflow, Feast and more) with Docker Compose, runs every service here.

The design follows the air quality project in chapter 3 of Jim Dowling's [*Building Machine Learning Systems with a Feature Store*](https://www.oreilly.com/library/view/building-machine-learning/9781098165222/).

## Architecture

The system is split into three pipelines that never call each other. The feature pipeline turns raw data into features, the inputs a model learns from. The training pipeline turns features into a model. The inference pipeline turns features and a model into predictions. They share data only through the feature store, which holds the features, and the model registry, which holds the models, so each can be run, scheduled and changed on its own.

Two terms in the diagram need explaining. The baseline is the simplest possible forecast, that today will be like yesterday, and every model has to beat it. An as-of date is the date a run treats as today, so a run for a past day sees only the data that existed then.

```
+-----------------------------------------------------------------------------+
| AIRFLOW: runs the feature, training and inference pipelines                 |
+-----------------------------------------------------------------------------+
           |                          |                           |
           | triggers                 | triggers                  | triggers
           v                          v                           v
+-----------------------+  +-----------------------+  +-----------------------+
| FEATURE PIPELINE      |  | TRAINING PIPELINE     |  | INFERENCE PIPELINE    |
| dynamic-des backfill, |  | XGBoost v1 and v2,    |  | batch per as-of date, |
| then daily runs       |  | scored vs yesterday   |  | next 7 days           |
+-----------------------+  +-----------------------+  +-----------------------+
      |                       ^             |             ^       ^         |
      | writes                | reads       | logs        | loads | reads   | writes
      |                       |             v             |       |         |
      |                       |     +------------------------+    |         |
      |                       |     | MODEL REGISTRY: MLflow |    |         |
      |                       |     +------------------------+    |         |
      v                       |                                   |         |
+--------------------------------------------------------------------+      |
| FEATURE STORE: Feast views, DuckDB, registry in                    |      |
| Postgres, over Iceberg tables (REST catalog, SeaweedFS)            |      |
+--------------------------------------------------------------------+      |
                                                                            v
                                                          predictions (Iceberg)
                                                                    |
                                                                    v
                                       monitoring dashboard, forecast assistant
```

### Data

The simulation writes two hourly Apache Iceberg tables in the `airq` namespace of the REST catalog, with their files on SeaweedFS under `s3://warehouse/airq/<table>`. Both are keyed by `location_id`, a single simulated station. The pipelines build everything else from them.

`weather_forecasts`: each hour issues a forecast for the same hour 1 to 7 days ahead, so every hour ends up with seven forecasts. `issued_at` is when a forecast was made and `forecast_for` is the hour it is for. Training reads only the forecasts that existed at the time, which keeps the evaluation honest.

| Column | Type | Unit | Description |
|---|---|---|---|
| `location_id` | string | | Station the forecast is for |
| `forecast_for` | timestamp (UTC) | | Hour the forecast is for |
| `issued_at` | timestamp (UTC) | | Time the forecast was made, `lead_days` before `forecast_for` |
| `lead_days` | integer | days | How far ahead the forecast was made, 1 to 7 |
| `temperature_2m` | double | °C | Forecast air temperature 2 m above the ground |
| `precipitation` | double | mm | Forecast rain falling during the hour |
| `wind_speed_10m` | double | km/h | Forecast wind speed 10 m above the ground |
| `wind_direction_10m` | double | degrees | Forecast direction the wind blows from, 0 to 360 |
| `ingested_at` | timestamp (UTC) | | Time the row was stored, the same as `issued_at` |

`observations`: the measured PM2.5, the source of the target.

| Column | Type | Unit | Description |
|---|---|---|---|
| `location_id` | string | | Station that took the reading |
| `measured_at` | timestamp (UTC) | | Hour the reading was measured |
| `pm2_5` | double | µg/m³ | Mean concentration of fine particles, 2.5 µm or smaller, during the hour |
| `ingested_at` | timestamp (UTC) | | Time the reading was stored, an hour after `measured_at` |

## Environment setup

### Python environment

Everything Python-side installs into one virtual environment from `requirements.txt`, including the `odctl` CLI. It also installs Airflow, Feast and MLflow at the versions the containers run, so the pipelines can run locally as well. Use Python 3.13. With [uv](https://docs.astral.sh/uv/), from this directory:

```bash
uv venv                             # create .venv
source .venv/bin/activate           # activate it (each new shell; Windows: .venv\Scripts\activate)
uv pip install -r requirements.txt
```

### Services

odctl groups services into profiles, and `odctl up` starts the profiles you name. Three steps, in this order:

```bash
# 1. copy the stack config into ./.odctl
odctl init

# 2. make the Airflow container install dynamic-des, before the first `odctl up` creates it
echo '_AIRFLOW_PIP_DEPS="dynamic-des[iceberg]>=0.14.0"' >> .odctl/.env

# 3. start the profiles this project uses
odctl up catalog feast mlflow airflow
```

`odctl init` copies odctl's compose files, configs and `.env` into `./.odctl`, and `odctl up` uses that copy. Step 2 must come before the first `odctl up`, because the setting reaches the Airflow container only when it is created. Run it again after `odctl init --force`, which restores the shipped `.env`. The profiles also start PostgreSQL and SeaweedFS, and three services this project does not use: Valkey, Feast's online feature server and MLflow's model server. The model server waits idle while `MODEL_URI` in `.odctl/.env` is empty.

| Service | Internal (Docker) | External (host) | Credentials |
|---|---|---|---|
| Iceberg REST catalog | `http://catalog:8181` | `http://127.0.0.1:8181` | none |
| SeaweedFS S3 API | `http://seaweed:8333` | `http://127.0.0.1:8333` | `user` / `password` |
| PostgreSQL | `postgres:5432` | `127.0.0.1:5432` | `user` / `password` |
| Feast UI | `http://feast-ui:8888` | `http://127.0.0.1:8890` | none |
| MLflow | `http://mlflow:5000` | `http://127.0.0.1:5004` | none |
| Airflow UI | `http://airflow:8080` | `http://127.0.0.1:8085` | `user` / `password` |

### Airflow

Airflow runs every pipeline as a DAG, Airflow's name for a workflow. The DAG files are in `dags`, and Airflow reads them from `s3://airflow/dags` every 15 seconds. Upload `dags` and the `airq` package the tasks import, and upload again after changing either. `--delete` removes files you have deleted locally:

```bash
export AWS_ACCESS_KEY_ID=user AWS_SECRET_ACCESS_KEY=password AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://127.0.0.1:8333 s3 sync . s3://airflow/dags --exclude "*" --include "airq/*.py" --include "dags/*.py" --delete
```

In the Airflow UI at http://127.0.0.1:8085 (`user` / `password`), the DAGs appear with the tag `airq`. New DAGs start paused: unpause one, then trigger it and fill in the form. The Airflow CLI in the container does the same from the terminal, and shows why a DAG is missing:

```bash
docker exec airflow airflow dags list-import-errors
```

## Step 1: v1 pipelines

### Feature pipeline

The feature pipeline writes the two hourly tables and turns them into two daily tables of features, which Feast will read.

`daily_weather`: one row per day and lead, averaged from the 24 hourly forecasts for that day. Training uses lead 1. Predicting N days ahead uses lead N, the forecast that existed at the time.

| Column | Type | Unit | Description |
|---|---|---|---|
| `location_id` | string | | Station the forecast is for |
| `day` | date | | Day the forecast is for |
| `lead_days` | integer | days | How far ahead the forecast was made, 1 to 7 |
| `issued_on` | date | | Day the forecast was made, `lead_days` before `day` |
| `temperature_2m` | double | °C | Mean forecast air temperature 2 m above the ground |
| `wind_speed_10m` | double | km/h | Mean forecast wind speed 10 m above the ground |
| `wet_hours` | integer | hours | Hours with forecast rain |

`daily_air_quality`: one row per day, with the target and the features known from the date and the day before.

| Column | Type | Unit | Description |
|---|---|---|---|
| `location_id` | string | | Station that took the readings |
| `day` | date | | Day the readings were measured |
| `pm2_5` | double | µg/m³ | Mean PM2.5 over the day's 24 readings, the target |
| `is_weekend` | boolean | | Whether the day is a Saturday or Sunday |
| `pm2_5_lag1` | double | µg/m³ | Mean PM2.5 of the day before, and the baseline's prediction |

It runs in two ways, each from the terminal or from Airflow. The same first day and seed always generate the same values. The backfill stores both on the tables, and the daily run reads them back, so the two write the same values for the same day.

- **Backfill:** drops and recreates the four tables, then generates the last `n_days` up to the end of yesterday (UTC), with `seed` (default 42). The hourly rows go through dynamic-des's Iceberg writer, and the daily rows are written with PyIceberg.
- **Daily run:** loads one day into all four tables, yesterday (UTC) by default, with the backfill's seed unless `--seed` names another. A different seed gives the day values that do not continue from the days around it. The day cannot be before the backfill's first day. Each table gets one overwrite filtered to that day, so running a day again replaces it.

From the terminal:

```bash
python -m airq.backfill --n-days 730 --seed 42
python -m airq.daily                      # yesterday
python -m airq.daily --date 2026-09-20
python -m airq.daily --date 2026-09-20 --seed 7
```

From Airflow, `dags/airq_feature_pipeline.py` defines `airq_backfill`, which runs only when triggered, with `n_days` and `seed` parameters, and `airq_daily`, which runs at 00:00 UTC for the day before, or for the day in its `date` parameter when triggered, with an optional `seed`. When `airq_daily` is first unpaused, it runs straight away for the most recent day. From the terminal:

```bash
docker exec airflow airflow dags unpause airq_backfill
docker exec airflow airflow dags trigger airq_backfill --conf '{"n_days": 730, "seed": 42}'
docker exec airflow airflow dags unpause airq_daily
docker exec airflow airflow dags trigger airq_daily --conf '{"date": "2026-09-20"}'
docker exec airflow airflow dags list-runs airq_backfill       # the state of each run
```

Feast reads the daily tables through the feature view in `airq/feast_repo.py`, `weather_v1`, over `daily_weather`. Register it once, and again after changing it:

```bash
python -m airq.feast_repo
```

The Feast UI at http://127.0.0.1:8890 then shows the project `airq` with the `station` and `lead` entities and the `weather_v1` view.

### Querying with Trino (optional)

Trino is a SQL query engine, and odctl's Trino reads the same Iceberg catalog. Starting a profile adds it to what is already running:

```bash
odctl up trino
docker exec -it trino trino --catalog iceberg --schema airq
```

Without `--catalog` and `--schema`, name tables in full as `iceberg.airq.<table>`, or run `USE iceberg.airq;` first. `SHOW TABLES;` and `DESCRIBE observations;` show what is there. Run `exit` or `quit` to leave the shell.

<details>
<summary>Example queries</summary>

The span of the backfill. The last hour should be 23:00 yesterday (UTC):

```sql
SELECT count(*) AS rows, min(measured_at) AS first_hour, max(measured_at) AS last_hour
FROM observations;
```

The forecast issued at midnight yesterday, one row per lead:

```sql
SELECT lead_days, forecast_for, temperature_2m, precipitation, wind_speed_10m
FROM weather_forecasts
WHERE issued_at = date_trunc('day', current_timestamp AT TIME ZONE 'UTC') - INTERVAL '1' DAY
ORDER BY lead_days;
```

The two clocks: seven forecasts for noon yesterday, each issued on a different day:

```sql
SELECT issued_at, lead_days, temperature_2m, wind_speed_10m
FROM weather_forecasts
WHERE forecast_for = date_trunc('day', current_timestamp AT TIME ZONE 'UTC') - INTERVAL '1' DAY + INTERVAL '12' HOUR
ORDER BY issued_at;
```

Daily mean PM2.5 over the last week:

```sql
SELECT date_trunc('day', measured_at) AS day, round(avg(pm2_5), 1) AS pm2_5
FROM observations
WHERE measured_at >= date_trunc('day', current_timestamp AT TIME ZONE 'UTC') - INTERVAL '7' DAY
GROUP BY 1
ORDER BY 1;
```

PM2.5 beside the lead-1 forecast, a preview of the daily features: lower on windy or wet days:

```sql
SELECT date_trunc('day', o.measured_at) AS day,
       round(avg(o.pm2_5), 1) AS pm2_5,
       round(avg(f.wind_speed_10m), 1) AS wind,
       count_if(f.precipitation > 0) AS wet_hours
FROM observations o
JOIN weather_forecasts f ON f.forecast_for = o.measured_at AND f.lead_days = 1
WHERE o.measured_at >= date_trunc('day', current_timestamp AT TIME ZONE 'UTC') - INTERVAL '7' DAY
GROUP BY 1
ORDER BY 1;
```

The weekday effect, which weather cannot explain:

```sql
SELECT day_of_week(measured_at) >= 6 AS weekend, round(avg(pm2_5), 1) AS pm2_5
FROM observations
GROUP BY 1;
```

</details>

### Training pipeline

The training pipeline trains v1 and scores it against the baseline, which predicts today as yesterday.

- **Data:** the target, `pm2_5`, and the baseline's prediction, `pm2_5_lag1`, come from `daily_air_quality`. The features come from Feast's `weather_v1` view: for each day, the forecast issued the day before (lead 1).
- **Split:** the days are split in time order. The model trains on the earlier 80% and is tested on the last 20%, so it never sees the future.
- **Model:** an XGBoost regressor on default settings. MLflow records MAE, RMSE and R² for v1 and for the baseline on the same test days.
- **Registry:** each run registers a new version of `airq_pm25` in MLflow and gives it the alias `champion`, which the inference pipeline loads.
- **Reproducibility:** Feast does not pin table versions, so the run tags the Iceberg snapshot of each table it read and logs the snapshot ids to MLflow.

From the terminal:

```bash
python -m airq.train
```

From Airflow, `dags/airq_training_pipeline.py` defines `airq_training`, which runs only when triggered:

```bash
docker exec airflow airflow dags unpause airq_training
docker exec airflow airflow dags trigger airq_training
```

The runs and registered versions are in MLflow at http://127.0.0.1:5004, under the experiment `airq` and the model `airq_pm25`.

### Inference pipeline

The inference pipeline predicts PM2.5 for the seven days after an as-of date, the date a run treats as today. Standing on day D, the forecast available is the one issued on D: lead N for day D+N. The run reads those seven rows through Feast, predicts with the `champion` model and writes them to the Iceberg table `predictions`. Running a date again replaces its predictions.

`predictions`: one row per as-of date and day predicted.

| Column | Type | Unit | Description |
|---|---|---|---|
| `location_id` | string | | Station the prediction is for |
| `as_of` | date | | Date the run treated as today |
| `day` | date | | Day predicted, `lead_days` after `as_of` |
| `lead_days` | integer | days | How far ahead, 1 to 7 |
| `pm2_5` | double | µg/m³ | Predicted daily mean PM2.5 |
| `model_version` | string | | Version of `airq_pm25` that made it |

When the readings for a predicted day arrive, the hindcast compares them with the predictions made earlier and reports the mean absolute error by lead and by as-of date.

From the terminal, for yesterday (UTC) or a named date, then the hindcast:

```bash
python -m airq.infer
python -m airq.infer --as-of 2026-09-20
python -m airq.infer --hindcast
```

A backtest is a few runs for past dates, for example three days back, two days back and yesterday. The hindcast then scores the days whose readings have arrived.

From Airflow, `dags/airq_inference_pipeline.py` defines `airq_inference`. It runs each time `airq_daily` finishes loading a day, for that day, because the daily task marks the daily features updated (an Airflow asset) and the inference DAG is scheduled on it. Triggered by hand, it runs for the day in its `as_of` parameter:

```bash
docker exec airflow airflow dags unpause airq_inference
docker exec airflow airflow dags trigger airq_inference --conf '{"as_of": "2026-09-20"}'
```

## Step 2: v2 pipelines

### Feature pipeline

Under construction.

### Training pipeline

Under construction.

### Inference pipeline

Under construction.

## Step 3: Monitoring and assistant

### Monitoring dashboard

Under construction.

### Forecast assistant

Under construction.

## Tests

```bash
python -m pytest tests
```

## Tear down environment

```bash
odctl down --all --volumes
deactivate
rm -rf .venv
```

`--volumes` deletes the data with the containers.

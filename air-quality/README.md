# air-quality

An ML system that forecasts daily PM2.5, a measure of fine-particle air pollution, for the next seven days from weather forecasts. It runs entirely on local services:

- **Data:** hourly weather forecasts and PM2.5 readings are simulated with [dynamic-des](https://jaehyeon-kim.github.io/dynamic-des/), a Python library that runs discrete-event simulations and writes their output to sinks such as Kafka, Postgres and Iceberg. Nothing calls an external service.
- **Storage and features:** the data lands in Apache Iceberg tables, an open table format, stored on SeaweedFS, an S3-compatible object store. Feast, a feature store, defines daily features over those tables and serves the same values to training and prediction.
- **Models:** XGBoost models are tracked in MLflow, whose model registry keeps each trained version and marks the one in use.
- **Monitoring and assistant:** a NiceGUI web app with two tabs: charts of the forecast against what was measured, and a chat with a Strands agent, running on a local open model, that answers questions about it.
- **Scheduling:** Apache Airflow runs the pipelines on a schedule.
- **Infrastructure:** [odctl](https://github.com/jaehyeon-kim/odctl), a command-line tool that starts a local data stack (Kafka, Spark, Flink, Iceberg, Airflow, MLflow, Feast and more) with Docker Compose, runs every service here.

The design follows the air quality project in Jim Dowling's [*Building Machine Learning Systems with a Feature Store*](https://www.oreilly.com/library/view/building-machine-learning/9781098165222/). [Concepts behind the design](docs/concepts.md) explains the ideas the system is built on and where the code uses them.

## Architecture

Three pipelines turn raw data into features, features into models, and features and a model into predictions. They never call each other; they share only a feature store and a model registry ([more in the concepts](docs/concepts.md#feature-training-and-inference-pipelines)). The **baseline** in the diagram is the simplest forecast, that today will be like yesterday; every model has to beat it.

![Feature, training and inference pipelines around the feature store and the model registry](images/architecture.png)

### Data

The simulation writes two hourly Apache Iceberg tables, `weather_forecasts` and `observations` (the measured PM2.5), in the `airq` namespace of the REST catalog, with their files on SeaweedFS under `s3://warehouse/airq/<table>`. Both are keyed by `location_id`, a single simulated station. The pipelines build everything else from them. [data.md](docs/data.md) lists every table's columns and has example queries with Trino.

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

# 2. make the Airflow container install dynamic-des and dateparser, before the first `odctl up` creates it
echo '_AIRFLOW_PIP_DEPS="dynamic-des[iceberg]>=0.14.0 dateparser==1.4.3"' >> .odctl/.env

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

Step 1 describes v1, the weather-only model. The code already includes step 2: registering the Feast views also registers v2's `calendar_v2`, and training also trains v2. [Step 2](#step-2-v2-pipelines) explains both.

### Feature pipeline

The feature pipeline writes the two hourly tables and turns them into two daily tables of features, which Feast reads: `daily_weather` (one row per day and lead) and `daily_air_quality` (the day's mean PM2.5, the target). Their columns are in [data.md](docs/data.md#daily-tables).

It runs in two ways, each from the terminal or from Airflow. The same first day and seed always generate the same values. The backfill stores both on the tables, and the daily run reads them back, so the two write the same values for the same day.

- **Backfill:** drops and recreates the four tables, and drops any predictions, which were made from the data being replaced. It then generates the last `n_days` up to the end of yesterday (UTC), with `seed` (default 42). The hourly rows go through dynamic-des's Iceberg writer, and the daily rows are written with PyIceberg.
- **Daily run:** loads one day into all four tables, yesterday (UTC) by default, with the backfill's seed unless `--seed` names another. A different seed gives the day values that do not continue from the days around it. The day cannot be before the backfill's first day. Each table gets one overwrite filtered to that day, so running a day again replaces it.

Every row is checked against the bounds in `airq/models.py` as it is built, such as PM2.5 between 0 and 500, so a run stops before writing an impossible value.

Days can be named the way people say them, as well as `YYYY-MM-DD`: `yesterday`, `"3 days ago"`, `"a week ago"`, `"last monday"` or `"20 September"`, all in UTC. The same phrases work in the Airflow parameters and in the assistant. From the terminal:

```bash
python -m airq.feature.backfill --n-days 730 --seed 42
python -m airq.feature.daily                              # yesterday
python -m airq.feature.daily --date "3 days ago"
python -m airq.feature.daily --date "3 days ago" --seed 7
```

From Airflow, `dags/airq_feature_pipeline.py` defines `airq_backfill`, which runs only when triggered, with `n_days` and `seed` parameters, and `airq_daily`, which runs at 00:00 UTC for the day before, or for the day in its `date` parameter when triggered, with an optional `seed`. When `airq_daily` is first unpaused, it runs straight away for the most recent day. From the terminal:

```bash
docker exec airflow airflow dags unpause airq_backfill
docker exec airflow airflow dags trigger airq_backfill --conf '{"n_days": 730, "seed": 42}'
docker exec airflow airflow dags unpause airq_daily
docker exec airflow airflow dags trigger airq_daily --conf '{"date": "3 days ago"}'
docker exec airflow airflow dags list-runs airq_backfill       # the state of each run
```

Feast reads the daily tables through the feature view in `airq/store/feast_repo.py`, `weather_v1`, over `daily_weather`. Register it once, and again after changing it:

```bash
python -m airq.store
```

The Feast UI at http://127.0.0.1:8890 then shows the project `airq` with the `station` and `lead` entities and the `weather_v1` view.

### Training pipeline

The training pipeline trains v1 and scores it against the baseline, which predicts today as yesterday.

- **Data:** the target, `pm2_5`, and the baseline's prediction, `pm2_5_lag1`, come from `daily_air_quality`. The features come from Feast's `weather_v1` view: for each day, the forecast issued the day before (lead 1).
- **Split:** the days are split in time order. The model trains on the earlier 80% and is tested on the last 20%, so it never sees the future.
- **Model:** an XGBoost regressor on default settings. MLflow records MAE, RMSE and R² for v1 and for the baseline on the same test days, and a feature importance plot.
- **Registry:** each run registers a new version of `airq_pm25` in MLflow and gives it the alias `champion`, which the inference pipeline loads. Step 2 adds v2 to the same run.
- **Reproducibility:** Feast does not pin table versions, so the run records the Iceberg snapshot of each table before and after reading, stops if a commit landed in between, and tags the snapshots it read so they outlive cleanup.

From the terminal:

```bash
python -m airq.training
python -m airq.inference        # so the new versions have a forecast
```

From Airflow, `dags/airq_training_pipeline.py` defines `airq_training`, which runs only when triggered. When it finishes, it starts the inference pipeline for the day before by itself:

```bash
docker exec airflow airflow dags unpause airq_training
docker exec airflow airflow dags trigger airq_training
```

The runs and registered versions are in MLflow at http://127.0.0.1:5004, under the experiment `airq` and the model `airq_pm25`.

### Inference pipeline

The inference pipeline predicts PM2.5 for the seven days after an **as-of date**, the date a run treats as today. It reads the forecasts made on that date through Feast, predicts with the `champion` model (and, from step 2, the `challenger` beside it), and writes the results to the Iceberg table `predictions` ([columns](docs/data.md#predictions)). Running a date again replaces its predictions.

When the readings for a predicted day arrive, the hindcast compares them with the predictions made earlier and reports each model version's mean absolute error by lead and by as-of date.

From the terminal, for yesterday (UTC) or a named date, then the hindcast:

```bash
python -m airq.inference
python -m airq.inference --as-of "3 days ago"
python -m airq.inference --hindcast
```

A backtest is a run for each of a range of past dates: `--days 3` runs the three as-of dates ending at `--as-of`, or at yesterday without it. The hindcast then scores the days whose readings have arrived.

From Airflow, `dags/airq_inference_pipeline.py` defines `airq_inference`. It starts by itself after each `airq_daily` run, for the day loaded, and after each `airq_training` run, for the day before. Triggered by hand, it runs for the day in its `as_of` parameter:

```bash
docker exec airflow airflow dags unpause airq_inference
docker exec airflow airflow dags trigger airq_inference --conf '{"as_of": "3 days ago"}'
```

## Step 2: v2 pipelines

Step 2 adds a second model, v2, beside v1. The measurements behind every choice below are in [results.md](docs/results.md).

### Feature pipeline

A feature sweep tests which daily features are worth adding to the weather. It records its results in MLflow as a run named `feature-sweep`:

```bash
python -m airq.training.sweep                   # the stored data plus seeds 0 to 4
```

It finds that a weekend flag cuts the error by nearly two thirds, because the simulation raises PM2.5 on weekdays. Past PM2.5 readings, from one to seven days back, add nothing. So v2 is the weather plus the weekend flag.

Feast serves the flag through a second view, `calendar_v2`, which works it out from each day's date ([why](docs/concepts.md#three-kinds-of-data-transformation)). Register both views:

```bash
python -m airq.store
```

### Training pipeline

The same command and DAG as in step 1 train both models, v1 on the weather and v2 on the weather and the weekend flag. The new version of the feature set the champion already uses becomes the new `champion`, and the other becomes the `challenger`. On the held-out test days of the run in [results.md](docs/results.md#test-scores), the mean absolute error is 3.40 for the baseline, 2.06 for v1 and 0.71 for v2. Other backfills give other figures, in the same order.

v1 stays the champion until you promote v2, in the MLflow UI or with:

```bash
python -c "import airq.config, mlflow; mlflow.MlflowClient().set_registered_model_alias('airq_pm25', 'champion', '<v2 version>')"
```

A promotion survives retraining.

### Inference pipeline

Inference predicts with both the champion and the challenger, and both write to `predictions`, told apart by `model_version`. A backtest runs inference for a range of past dates, so the charts in step 3 have history:

```bash
python -m airq.inference --days 60
python -m airq.inference --hindcast
```

## Step 3: Monitoring and assistant

One web app, built with [NiceGUI](https://nicegui.io/), a Python framework that serves the page and the code behind it from one process. It has two tabs:

- **Monitoring:** the forecast against what was measured, and how accurate each model has been.
- **Assistant:** a chat that answers questions about the forecast, such as "what is the forecast for tomorrow?".

![The app's two tabs, the assistant, the local model and the shared queries over Iceberg and MLflow](images/app.png)

Both tabs use the same queries in `airq/app/reports.py`, so a number in the chat always matches the charts. The page is in `airq/app/ui.py` and the agent in `airq/app/assistant.py`.

### Before you start

1. **The odctl services running,** as in [Services](#services).
2. **Predictions to show:** run the backtest from step 2, `python -m airq.inference --days 60`.
3. **Ollama running with the assistant's model.** [Ollama](https://ollama.com/) runs open language models on your own machine. Start the desktop app or `ollama serve`, then pull the model once:

   ```bash
   ollama pull qwen3:4b-instruct
   ```

### Running the app

```bash
python -m airq.app
```

Open http://127.0.0.1:8090. The header shows the model versions in use. To use another Ollama model that supports tool calls, pull it and start the app with `AIRQ_MODEL=<model> python -m airq.app`.

### Monitoring tab

- **Forecast against measured:** the measured PM2.5 over the last 60 days, each model's one-day-ahead predictions for those days, and each model's latest seven-day forecast as a dashed line.
- **Daily error:** each model's one-day-ahead error, one bar per day.
- **Error by lead:** each model's average error over the last 30 days, for predictions made 1 to 7 days ahead.

**Refresh** reloads the charts after a new daily run.

### Assistant tab

Type a question and press Enter. The chat is a [Strands](https://strandsagents.com/) agent with three tools: `get_forecast`, `get_observed` and `get_model_error`. The model picks a tool, the tool reads the data, and the model answers from the result, so every number comes from the data. Days can be named as people say them, such as "tomorrow", "3 days ago", "last Monday" or "this weekend".

Questions it answers well:

- What is the forecast for tomorrow?
- Will PM2.5 be higher on Saturday than tomorrow?
- What was the highest PM2.5 over the last seven days?
- Which model has been more accurate over the last 30 days?

The default model, `qwen3:4b-instruct`, is small and answers without a reasoning pass. If an answer is wrong, improve the tools or the prompt in `airq/app/assistant.py` rather than switching to a larger model.

### Troubleshooting

| What you see | Cause and fix |
|---|---|
| The chat shows `The assistant failed: All connection attempts failed` | Ollama is not running: start the desktop app or `ollama serve`. |
| The chat shows `The assistant failed: model '...' not found` | The model has not been pulled: run `ollama pull` with the name in `AIRQ_MODEL`, or the default. |
| The charts or the error table are empty | No predictions yet: run the inference pipeline or the backtest in [Before you start](#before-you-start). |
| The header shows no model versions | No model is registered: run the training pipeline. |

## Tests

The tests run without the odctl services:

```bash
python -m pytest tests
```

GitHub runs them, with the repository's lint checks, on every push to `main` (see [Checks](../README.md#checks)).

They cover the simulation and feature code, value bounds, Feast's point-in-time join, the training splits, the sweep's choice, inference requests, the champion and challenger handling, day phrases and the assistant's tools.

## Tear down environment

```bash
odctl down --all --volumes
deactivate
rm -rf .venv
```

`--volumes` deletes the data with the containers.

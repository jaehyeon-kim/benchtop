# Concepts behind the design

The ideas the [air-quality](../README.md) system is built on, each explained briefly and tied to where the code uses it.

## A prediction service with a clear problem

Start with the problem, not the model. Decide what to predict, who uses the prediction, and which data exists. Then pick a number that shows whether the model is good.

Here the prediction is daily PM2.5 for the next seven days at one station. People use it through a dashboard and a chat assistant. The data is simulated weather forecasts and PM2.5 readings. The number is the mean absolute error (MAE): how far predictions were from what was later measured, on average.

Build the smallest version that works end to end first, then improve it. Here that version is v1, which predicts from the weather forecast alone. v2 then adds a weekend flag, and the app adds monitoring and a chat assistant.

## Feature, training and inference pipelines

An ML system splits into three kinds of pipeline, known as FTI pipelines:
- a **feature pipeline** turns raw data into features and stores them;
- a **training pipeline** reads features and labels and produces a model;
- an **inference pipeline** reads features and a model and produces predictions.

The pipelines never call each other. They share state through two stores: the **feature store**, which holds features, and the **model registry**, which holds models. So each pipeline has clear inputs and outputs and can be run, scheduled, tested and changed on its own. Here the feature store is Feast over Iceberg tables, the model registry is MLflow, and Airflow schedules the pipelines.

## Feature groups, feature views, entities and labels

These terms are easiest to see on an example row of the `daily_weather` table:

| location_id | day | lead_days | issued_on | temperature_2m | wind_speed_10m | wet_hours |
|---|---|---|---|---|---|---|
| station-1 | 2026-09-20 | 1 | 2026-09-19 | 18.2 | 11.4 | 3 |

It says: for station-1, the forecast made on 19 September for the next day expected 18.2 °C, wind of 11.4 km/h and 3 hours of rain.

- **Feature:** one input a model learns from, such as `temperature_2m`.
- **Feature group:** a table of features, like `daily_weather` above.
- **Entity:** what a row is about. Here: the station (`location_id`), plus how many days ahead the forecast was made (`lead_days`).
- **Event time:** the time the values describe. In the row above it is `day`, 20 September: the forecast is about the weather on 20 September. It is not when the forecast was made (`issued_on`, 19 September), and not when the row was written to the table, which is later. When training asks for the features of 20 September, Feast finds this row by its event time.
- **Feature view:** a named set of features that a model reads. `weather_v1` picks the three weather columns above, and v1 reads only that view. `calendar_v2` holds the weekend flag, and v2 reads both views. A feature view stores no data; it only says which columns to read.
- **Label:** the answer the model learns to predict: here `pm2_5`, the day's measured air pollution, from the `daily_air_quality` table. It is kept apart from the features and joined to them only when training data is built.

`location_id`, `day`, `lead_days` and `issued_on` find the right row; they are never fed to the model.

## Three kinds of data transformation

- **Model-independent transformations** produce features any model can reuse. They run once, in the feature pipeline, and their output is stored. Here: the hourly forecasts and readings averaged into daily rows, wet hours counted, and yesterday's mean.
- **Model-dependent transformations** depend on one model and its training data, such as scaling or encoding. They are applied in both the training and the inference pipeline, never stored. Here there are none: XGBoost needs no scaling, and the weekend flag is already 0 or 1.
- **On-demand transformations** calculate a feature at the moment it is asked for, instead of reading it from a stored table. They are for features that cannot be stored in advance. Here that is the weekend flag. A day's row in `daily_air_quality` is written only after its readings arrive, so the seven days being forecast have no row yet. So when training or inference asks Feast for a day's features, the on-demand view `calendar_v2` works out the weekend flag from that day's date.

## Backfill and incremental runs

A **backfill** creates feature data from history, for a new system or to fill a gap. An **incremental** run processes only what is new since the last run. Both should be safe to rerun. Here the backfill writes 730 days, the daily run writes one day, both use the same feature code, and a rerun of a day replaces it.

## Data validation on write

Validate data before it is written, because one bad row can break a training or inference run later. Here every row is built from a pydantic model with bounds such as PM2.5 between 0 and 500 (`airq/core/models.py`), so a pipeline stops before writing an impossible value.

## Point-in-time correct training data

A model should learn only from what was known at the time. To learn the PM2.5 of a day, it sees the weather forecast made the day before, never anything that came later. Using later information is called **leakage**: it makes a model look better in testing than it will be in real use.

Each forecast row records the day it was made, so Feast always picks the right one. If that forecast is missing, Feast returns nothing rather than an older one (`tests/stores/test_point_in_time.py` checks this).

## No skew between training and inference

**Skew** means the features seen in training differ from those seen when predicting, which silently lowers accuracy. Here training and inference both read features through the same Feast views, so they cannot compute a feature differently.

## Reproducible training data

Every write to an Iceberg table creates a new version of it, called a **snapshot**. So reading a table next week can return different data from reading it today. To be able to read a model's training data again, training pins the snapshots it read.

Who does what:

| Part | Role |
|---|---|
| Iceberg | creates a snapshot on every write, and keeps tags: names that point at one snapshot each |
| Feast | reads the training data from the current snapshot of `daily_weather` and `daily_air_quality`. It does not record which snapshot that was |
| Training pipeline (`airq/training/train.py`) | reads the current snapshot id of both tables before and after Feast reads them, and stops if either changed. It then tags both snapshots `mlflow-<run id>` |
| MLflow | stores each snapshot id and tag name on the training run, so a model's training data can be found from its run |

Iceberg's maintenance jobs (compaction, snapshot expiry and orphan file removal) never delete a snapshot that a tag points to, or the files it uses. So a run's training data can be read again for as long as its tag exists. Two side effects remain. The tagged files take up space until the tag is removed. And a compaction that commits while training is reading changes the snapshot id, so training stops and has to be run again. This project runs none of these jobs.

## Model registry, versions and evaluation

Every training run saves its model in MLflow as a new numbered **version** (1, 2, 3 and so on). A version never changes. It keeps its scores (MAE, RMSE, R²), a feature importance plot, which feature set it used, and which table snapshots it was trained on.

An **alias** is a name pointing at one version:
- `champion`: the version in use;
- `challenger`: a version predicted beside it, for comparison.

Here v1 is trained first and becomes the champion. v2 is trained later and becomes the challenger. Promoting v2 swaps the aliases, so v2 becomes the champion and v1 the challenger. Changing or rolling back the model in use means moving an alias to another version. Nothing is redeployed.

## Batch inference and hindcasts

Batch inference runs on a schedule, once a day here. Each run treats one date as "today", called the **as-of date**, and predicts the seven days after it.

Every prediction is kept as a row in the Iceberg table `airq.predictions`. A row records the as-of date, the day predicted, how many days ahead that is, and the model version.

When the readings for those days arrive, a **hindcast** compares them with the predictions. It shows how accurate each model was, and how accuracy changes with how far ahead it predicted.

## Drift

**Drift** means the world has changed since the model was trained, so the model gets worse. Two kinds matter most:
- **Feature drift:** the inputs change, such as a hotter season than any in the training data.
- **Concept drift:** the same inputs lead to a different outcome, such as a new traffic rule that lowers pollution in the same weather.

Watch for rising error, and have a person check an alert before retraining.

Here readings arrive the next day, so the hindcast measures error daily, and the Monitoring tab charts it. Feature drift is not measured, because the simulation keeps its data the same over time.

## Language models with tools

A language model cannot see the project's tables. Instead it is given **tools**: named functions it may call. For each question, the model picks a tool and its inputs, the code runs it, and the model writes its answer from the result.

Here the assistant has three tools, over the same queries the Monitoring tab uses.

## Versioning

Features and models are versioned so a change can be added beside the old one and rolled back. Here v2's weekend flag came as a new view, `calendar_v2`, beside an unchanged `weather_v1`, so v1's training data can still be rebuilt and both models keep working.

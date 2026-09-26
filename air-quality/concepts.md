# Concepts behind the design

The ideas the [air-quality](README.md) system is built on, each explained briefly and tied to where the code uses it.

## A prediction service with a clear problem

Start from the prediction problem, the people who use the predictions and the data sources, before any model. Measure the model with an ML metric that moves with what users care about, then build the smallest service that delivers predictions end to end: a feature pipeline that can backfill and run daily, a training pipeline, an inference pipeline and a dashboard. Improve it afterwards.

Here the problem is daily PM2.5 for the next seven days at one station, the users read a dashboard or ask the assistant, the data is simulated weather forecasts and readings, and the ML metric is the mean absolute error (MAE) against what was later measured. Step 1 is that smallest service; steps 2 and 3 improve it.

## Feature, training and inference pipelines

An ML system splits into three kinds of pipeline, known as FTI pipelines:
- a **feature pipeline** turns raw data into features and stores them;
- a **training pipeline** reads features and labels and produces a model;
- an **inference pipeline** reads features and a model and produces predictions.

The pipelines never call each other. They share state through two stores: the **feature store**, which holds features, and the **model registry**, which holds models. So each pipeline has clear inputs and outputs and can be run, scheduled, tested and changed on its own. Here the feature store is Feast over Iceberg tables, the model registry is MLflow, and Airflow schedules the pipelines.

## Feature groups, feature views, entities and labels

A **feature group** is a table of features, keyed by an **entity** (the thing the features describe) and an **event time** (when the values were true, not when the row was written). A **feature view** selects the features a model uses from one or more feature groups; it holds no data of its own. The **label** is the value a model predicts, chosen when training data is created rather than stored as a feature.

Here the daily Iceberg tables play the part of feature groups, keyed by the station (`location_id`) and, for forecasts, the lead. The Feast views `weather_v1` and `calendar_v2` are the feature views. The label is `pm2_5`, the daily mean reading. Key columns such as `location_id`, `day` and `lead_days` identify rows and are not model inputs.

## Feature types

Features are classified by what can be done with them: **numerical** features have meaningful distances (temperature, wind speed, wet hours, yesterday's PM2.5), and **categorical** features are labels without order or with an order but no distances (the weekend flag). The type decides which transformations are valid, such as scaling a numerical feature or encoding a categorical one.

## Three kinds of data transformation

- **Model-independent transformations** produce features any model can reuse. They run once, in the feature pipeline, and their output is stored. Here: the hourly forecasts and readings averaged into daily rows, wet hours counted, and yesterday's mean.
- **Model-dependent transformations** depend on one model and its training data, such as scaling or encoding. They are applied in both the training and the inference pipeline, never stored. Here there are none: XGBoost needs no scaling, and the weekend flag is already 0 or 1.
- **On-demand transformations** need data that is only known when a prediction is requested. Here the weekend flag is computed by the on-demand view `calendar_v2` from the day being predicted. Batch inference normally has no request-time data. The day being predicted plays that part here, because the flag has to exist for days that have not happened yet. Feast runs the same function when it builds training data, so training and inference share one definition.

## Backfill and incremental runs

A **backfill** creates feature data from history, for a new system or to fill a gap. An **incremental** run processes only what is new since the last run. Both should be safe to rerun. Here the backfill writes 730 days, the daily run writes one day, both use the same feature code, and a rerun of a day replaces it.

## Data validation on write

Validate data before it is written, because one bad row can break a training or inference run later. Here every row is built from a pydantic model with bounds such as PM2.5 between 0 and 500 (`airq/models.py`), so a pipeline stops before writing an impossible value.

## Point-in-time correct training data

Every feature value joined to a label must be the one that was available at the label's event time: no values from the future (**leakage**) and no values older than they should be (**stale features**). Here each daily forecast row carries its lead, training asks for the forecast issued the day before each day, and inference standing on day D asks for lead N on day D+N. The view's time to live is 12 hours, so a missing forecast comes back empty rather than being filled from the day before (`tests/test_point_in_time.py`).

## No skew between training and inference

**Skew** means the features seen in training differ from those seen when predicting, which silently lowers accuracy. Here training and inference both read features through the same Feast views, so they cannot compute a feature differently.

## Reproducible training data

Reading the same tables later can return different data. Here each training run records the Iceberg snapshot of each table it read and tags it, so the snapshot survives cleanup and the exact training data can be read again. The run also stops if a table changed while it was reading.

## Model registry, versions and evaluation

The model registry keeps every trained model as an immutable, numbered version with its metrics and lineage. Here MLflow holds each version with its MAE, RMSE, R², feature importance plot, feature set tag and snapshot tags. The aliases `champion` (the model in use) and `challenger` (predicted beside it) decide what inference serves, so switching or rolling back is a change of alias, not a new deployment.

## Batch inference, prediction logs and hindcasts

A batch inference pipeline reads precomputed features, predicts on a schedule, and stores the predictions with what is needed to monitor them. Here the `predictions` table records each prediction with its as-of date, lead and model version. A **hindcast** later compares the predictions with the readings that arrived, which shows how well each model did at each lead.

## Drift

**Drift** is a change in the distribution of features, labels or the relationship between them. It comes in several kinds: **data ingestion drift** (new data differs from what the feature pipeline received before), **feature drift** (what inference sees differs from what the model was trained on), **prediction drift** (predictions shift while features do not), **concept drift** (the relationship between features and the outcome changes, seen as rising error once outcomes arrive) and a fall in the business metric. A sensible start is alerts that a person checks, with no automatic retraining until those alerts are trusted.

Here outcomes arrive the next day, so the hindcast is the monitor for concept drift: the Monitoring tab charts each model's daily error and its error by lead. Feature and prediction drift are not measured, because the simulation keeps its distributions fixed, so a drift report would show nothing.

## Language models with tools

A language model answers from data it is given at request time. With **function calling**, the model chooses a function and its arguments, the code runs it against the system's data, and the model writes its answer from the result, which works as retrieval without a vector database. Here the assistant has three tools over the same queries the dashboard uses.

## Versioning

Features and models are versioned so a change can be added beside the old one and rolled back. Here v2's weekend flag came as a new view, `calendar_v2`, beside an unchanged `weather_v1`, so v1's training data can still be rebuilt and both models keep working.

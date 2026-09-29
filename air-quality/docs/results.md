# Results

How well each forecast in the [README](../README.md) predicts daily PM2.5, and why v2 has the features it has.

Every error here is the mean absolute error (MAE) in µg/m³: how far a prediction is from the measured value, on average.

The figures come from one run over 730 days of simulated data with seed 42. A backfill on another date covers other days, so its figures differ. The order does not change: the baseline has the highest error and v2 the lowest. `tests/training/test_ordering.py` checks that.

## Models

| Model | Predicts from |
|---|---|
| baseline | yesterday's measured PM2.5, used as the prediction for today |
| v1 | the weather forecast issued the day before: temperature, wind speed and hours of rain |
| v2 | v1's weather features plus a weekend flag |

v1 and v2 are XGBoost models with default settings. The baseline is not a trained model. It shows the error a model has to beat.

## Test scores

Training holds out the last 20% of days (146 days) for testing. v1 and v2 are trained in separate runs, on the same test days, and each run also scores the baseline on them:

| Model | MAE |
|---|---|
| baseline | 3.40 |
| v1 | 2.06 |
| v2 | 0.71 |

v1's error is well below the baseline's. PM2.5 follows the weather, and the forecast sees the weather change before it happens.

v2's error is less than half of v1's. The simulation raises PM2.5 on weekdays, from traffic. The weather cannot show that, but the weekend flag can.

Each training run also scores its model and the baseline on a random 20% of days:

| Model | last 20% of days | random 20% of days |
|---|---|---|
| baseline | 3.40 | 3.75 |
| v1 | 2.06 | 1.87 |
| v2 | 0.71 | 0.71 |

The two splits give similar scores here, and which one is lower changes from one backfill to the next. The first column is still the one to trust. A real forecast only predicts days after the ones it learnt from, and only the time-ordered split tests that.

## Error by days ahead

The backtest predicts seven days ahead from each of 60 past dates. All 60 fall inside the held-out test days, so neither model trained on them. Over the last 30 measured days:

| Days ahead | 1 | 2 | 3 | 4 | 5 | 6 | 7 |
|---|---|---|---|---|---|---|---|
| v1 | 1.75 | 1.64 | 1.71 | 1.83 | 2.07 | 1.66 | 1.86 |
| v2 | 0.64 | 0.78 | 0.75 | 0.59 | 0.94 | 0.93 | 1.06 |

v2 has the lower error at every lead. Its error mostly grows with the lead, because a weather forecast is less accurate further ahead. v1's error shows no clear trend.

## Choosing v2's features

`python -m airq.training.sweep` tests which daily features to add to v1's weather features. It adds one candidate at a time, and scores each set with the same model on the same days. It records everything in MLflow as a run named `feature-sweep`.

The stored data comes from one seed, so the sweep also scores each set on data generated with five other seeds:

| Features | MAE on the stored data | mean MAE over 6 data sets |
|---|---|---|
| weather | 1.98 | 1.87 |
| weather and weekend flag | 0.72 | 0.69 |
| plus yesterday's PM2.5 | 0.74 | 0.70 |
| plus the two days before | 0.77 | 0.70 |
| plus the three days before | 0.73 | 0.72 |

The sweep keeps adding features while each one lowers the mean error by at least 5%. The weekend flag cuts the error by nearly two thirds. Adding yesterday's PM2.5 then raises the mean error slightly, so the sweep stops there, and v2 is the weather and the weekend flag.

## Past PM2.5 readings

A past reading can only be used if it exists when the forecast is made. On day D, the latest reading is D's own. So a forecast for day D+N can use a reading from N days before it at best. The sweep scores each lead with the reading that is actually available (mean MAE over the six data sets):

| Days ahead | 1 | 2 | 3 | 4 | 5 | 6 | 7 |
|---|---|---|---|---|---|---|---|
| without the reading | 0.71 | 0.75 | 0.81 | 0.80 | 0.91 | 0.89 | 0.89 |
| with the reading | 0.70 | 0.74 | 0.81 | 0.81 | 0.88 | 0.85 | 0.92 |

The reading changes the error by 0.04 at most, up or down. In this simulation, PM2.5 depends on the day's weather and weekday plus random noise. Nothing carries over from the day before, so a past reading tells the model nothing new.

Leaving past readings out also avoids three problems:
- **One model per lead.** Each lead would use a reading from a different day, so each would need its own model.
- **Sensor outages.** A missing reading would stop the forecast, or need a rule to fill it in.
- **Timing.** Day D's last reading arrives at 00:00 UTC, when the daily run starts, so a late reading would be missed.

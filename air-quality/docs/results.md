# Results

How well each forecast in the [README](../README.md) predicts daily PM2.5, and why v2 has the features it has.

Every error here is the mean absolute error (MAE) in µg/m³: how far a prediction is from the measured value, on average.

The figures come from one run over 730 days of simulated data with seed 42. A backfill on another date covers other days, so its figures differ. The order does not change: the baseline has the highest error and v2 the lowest. `tests/training/test_ordering.py` checks that.

The three forecasts are described in the [README](../README.md#what-you-will-build). v1 and v2 are XGBoost models with default settings.

## Test scores

Training holds out the last 20% of days (146 days) for testing. v1 and v2 are trained in separate runs, on the same test days, and each run also scores the baseline on them:

| Model | MAE |
|---|---|
| baseline | 3.70 |
| v1 | 1.95 |
| v2 | 0.77 |

v1's error is well below the baseline's. PM2.5 follows the weather, and the forecast sees the weather change before it happens.

v2's error is less than half of v1's. The simulation raises PM2.5 on weekdays, from traffic. The weather cannot show that, but the weekend flag can.

The test days are the last ones, not a random sample. A real forecast only predicts days after the ones it learnt from, and only a split in time order tests that.

## Error by days ahead

The backtest predicts seven days ahead from each of 60 past dates. All 60 fall inside the held-out test days, so neither model trained on them. Over the last 30 measured days:

| Days ahead | 1 | 2 | 3 | 4 | 5 | 6 | 7 |
|---|---|---|---|---|---|---|---|
| v1 | 1.81 | 1.95 | 1.66 | 1.63 | 2.01 | 1.88 | 2.09 |
| v2 | 0.91 | 0.90 | 0.99 | 0.75 | 1.14 | 1.12 | 1.22 |

v2 has the lower error at every lead. Its error mostly grows with the lead, because a weather forecast is less accurate further ahead. v1's error shows no clear trend.

## Choosing v2's features

`python -m airq.training.sweep` tests which daily features to add to v1's weather features. It reads the stored features, adds one candidate at a time, and scores each set with the same model on the same test days as training. It records the results in MLflow as a run named `feature-sweep`:

| Features | MAE |
|---|---|
| weather | 1.94 |
| weather and weekend flag | 0.77 |
| plus yesterday's PM2.5 | 0.75 |
| plus the two days before | 0.77 |
| plus the three days before | 0.76 |

The sweep keeps adding features while each one lowers the error by at least 5%. The weekend flag cuts the error by about 60%. Yesterday's PM2.5 then lowers it by less than 3%, so the sweep stops there, and v2 is the weather and the weekend flag.

## Past PM2.5 readings

A past reading can only be used if it exists when the forecast is made. On day D, the latest reading is D's own. So a forecast for day D+N can use a reading from N days before it at best. The sweep scores each lead with the reading that is actually available:

| Days ahead | 1 | 2 | 3 | 4 | 5 | 6 | 7 |
|---|---|---|---|---|---|---|---|
| without the reading | 0.76 | 0.83 | 0.86 | 0.85 | 0.93 | 0.96 | 0.94 |
| with the reading | 0.77 | 0.81 | 0.87 | 0.82 | 0.94 | 0.88 | 0.95 |

The reading moves the error by 0.08 at most, up at some leads and down at others, with no consistent gain. In this simulation, PM2.5 depends on the day's weather and weekday plus random noise. Nothing carries over from the day before, so a past reading tells the model nothing new.

Leaving past readings out also avoids three problems:
- **One model per lead.** Each lead would use a reading from a different day, so each would need its own model.
- **Sensor outages.** A missing reading would stop the forecast, or need a rule to fill it in.
- **Timing.** Day D's last reading arrives at 00:00 UTC, when the daily run starts, so a late reading would be missed.

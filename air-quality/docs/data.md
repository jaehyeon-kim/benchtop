# Data

Every table the [air-quality](../README.md) pipelines write. The tables are Iceberg tables in the `airq` namespace, with their files on SeaweedFS under `s3://warehouse/airq/<table>`.

## Hourly tables

`weather_forecasts`: the simulated weather forecasts. Every hour has seven forecasts, made 1 to 7 days before it. `forecast_for` is the hour a forecast is for, and `issued_at` is when it was made.

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

`observations`: the simulated PM2.5 readings, one per hour. Their daily mean is what the models predict.

| Column | Type | Unit | Description |
|---|---|---|---|
| `location_id` | string | | Station that took the reading |
| `measured_at` | timestamp (UTC) | | Hour the reading was measured |
| `pm2_5` | double | µg/m³ | Mean concentration of fine particles, 2.5 µm or smaller, during the hour |

## Daily tables

`daily_weather`: the forecast for each day, averaged over its 24 hours. There is one row for each day and lead. Training uses lead 1, the forecast made the day before. A prediction N days ahead uses lead N.

| Column | Type | Unit | Description |
|---|---|---|---|
| `location_id` | string | | Station the forecast is for |
| `day` | date | | Day the forecast is for |
| `lead_days` | integer | days | How far ahead the forecast was made, 1 to 7 |
| `issued_on` | date | | Day the forecast was made, `lead_days` before `day` |
| `temperature_2m` | double | °C | Mean forecast air temperature 2 m above the ground |
| `wind_speed_10m` | double | km/h | Mean forecast wind speed 10 m above the ground |
| `wet_hours` | integer | hours | Hours with forecast rain |

`daily_air_quality`: one row per day, with the day's mean PM2.5, which the models predict, the weekend flag and the previous day's mean.

| Column | Type | Unit | Description |
|---|---|---|---|
| `location_id` | string | | Station that took the readings |
| `day` | date | | Day the readings were measured |
| `pm2_5` | double | µg/m³ | Mean PM2.5 over the day's 24 readings, the target |
| `is_weekend` | boolean | | Whether the day is a Saturday or Sunday |
| `pm2_5_lag1` | double | µg/m³ | Mean PM2.5 of the day before, and the baseline's prediction |

## Predictions

`predictions`: every forecast the inference pipeline has made. There is one row for each as-of date, day predicted and model version.

| Column | Type | Unit | Description |
|---|---|---|---|
| `location_id` | string | | Station the prediction is for |
| `as_of` | date | | Date the run treated as today |
| `day` | date | | Day predicted, `lead_days` after `as_of` |
| `lead_days` | integer | days | How far ahead, 1 to 7 |
| `pm2_5` | double | µg/m³ | Predicted daily mean PM2.5 |
| `model_version` | string | | Version of `airq_pm25` that made it |

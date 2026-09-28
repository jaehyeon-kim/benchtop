# Data

The Iceberg tables in the `airq` namespace, as the [README](../README.md) describes them. Files are on SeaweedFS under `s3://warehouse/airq/<table>`.

## Hourly tables

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

## Daily tables

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

## Predictions

`predictions`: one row per as-of date and day predicted.

| Column | Type | Unit | Description |
|---|---|---|---|
| `location_id` | string | | Station the prediction is for |
| `as_of` | date | | Date the run treated as today |
| `day` | date | | Day predicted, `lead_days` after `as_of` |
| `lead_days` | integer | days | How far ahead, 1 to 7 |
| `pm2_5` | double | µg/m³ | Predicted daily mean PM2.5 |
| `model_version` | string | | Version of `airq_pm25` that made it |

## Querying with Trino

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

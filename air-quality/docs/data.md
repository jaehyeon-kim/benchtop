# Data

Every table the [air-quality](../README.md) pipelines write, and example queries to look at them. The tables are Iceberg tables in the `airq` namespace, with their files on SeaweedFS under `s3://warehouse/airq/<table>`.

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

## Querying with Trino

Trino is a SQL query engine. odctl's Trino reads the same Iceberg catalog, and `odctl up trino` starts it beside the services already running:

```bash
odctl up trino
docker exec -it trino trino --catalog iceberg --schema airq
```

Without `--catalog` and `--schema`, name tables in full as `iceberg.airq.<table>`, or run `USE iceberg.airq;` first. `SHOW TABLES;` and `DESCRIBE observations;` show what is there. Run `exit` or `quit` to leave the shell.

### Example queries

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

Seven forecasts for noon yesterday, each issued on a different day:

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

Yesterday's daily weather at every lead, each averaged from the forecasts issued on a different day:

```sql
SELECT lead_days, issued_on, temperature_2m, wind_speed_10m, wet_hours
FROM daily_weather
WHERE day = CAST(current_timestamp AT TIME ZONE 'UTC' AS date) - INTERVAL '1' DAY
ORDER BY lead_days;
```

The last week of the target, with the weekend flag and the previous day's mean:

```sql
SELECT day, pm2_5, is_weekend, pm2_5_lag1
FROM daily_air_quality
ORDER BY day DESC
LIMIT 7;
```

The rows training learns from: each day's PM2.5 beside the lead-1 weather and the weekend flag:

```sql
SELECT a.day, a.pm2_5, w.temperature_2m, w.wind_speed_10m, w.wet_hours, a.is_weekend
FROM daily_air_quality a
JOIN daily_weather w ON w.day = a.day AND w.lead_days = 1
ORDER BY a.day DESC
LIMIT 7;
```

The latest forecast, seven days from each model version:

```sql
SELECT model_version, day, lead_days, pm2_5
FROM predictions
WHERE as_of = (SELECT max(as_of) FROM predictions)
ORDER BY model_version, day;
```

Every prediction made for the last measured day, from seven days ahead to one, beside the measured value:

```sql
SELECT p.as_of, p.lead_days, p.model_version, p.pm2_5 AS predicted, a.pm2_5 AS measured
FROM predictions p
JOIN daily_air_quality a ON a.day = p.day
WHERE p.day = (SELECT max(day) FROM daily_air_quality)
ORDER BY p.model_version, p.lead_days DESC;
```

Each model version's mean absolute error by lead, over every day that has a measurement. This is what the hindcast reports:

```sql
SELECT p.model_version, p.lead_days, round(avg(abs(p.pm2_5 - a.pm2_5)), 2) AS mae, count(*) AS days
FROM predictions p
JOIN daily_air_quality a ON a.day = p.day
GROUP BY 1, 2
ORDER BY 1, 2;
```

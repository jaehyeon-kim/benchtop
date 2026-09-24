# air-quality

Chapter 3 of Dowling's air quality project, rebuilt on odctl with simulated data. This directory covers the first two steps: the record contracts, and a backfill of two years of hourly weather forecasts and PM2.5 readings into Iceberg. Every value comes from a `dynamic-des` simulation, so nothing calls an external service.

## Layout

| File | Purpose |
|------|---------|
| `airq/generator.py` | Record models and the functions that generate each hour's weather, forecasts and PM2.5 |
| `airq/simulator.py` | The `dynamic-des` context: an hourly clock from a fixed origin, and outages |
| `airq/iceberg.py` | Catalog, table names, and table schemas derived from the models |
| `airq/backfill.py` | Recreates both tables and writes the simulated history to them |
| `tests/` | Unit tests, which run without the stack |

Two tables in the `airq` namespace:

- `weather_forecasts`: one row per hour per lead, 1 to 7 days. `event_time` is the hour forecast, `issued_at` is when the forecast was made.
- `observations`: one PM2.5 reading per hour. Outage hours have no row.

## Run

```bash
odctl up catalog
python3.13 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m airq.backfill
.venv/bin/python -m pytest tests
```

The backfill runs from the host against `localhost`. Inside an odctl container, set `ICEBERG_URI=http://catalog:8181` and `S3_ENDPOINT=http://seaweed:8333`.

## Tear down

```bash
odctl down catalog
```

## Versions

Last run against odctl 0.6.0 (`apache/iceberg-rest-fixture:1.10.1`, `chrislusf/seaweedfs:4.40`), Python 3.13.14, dynamic-des 0.14.0, pyiceberg 0.12.0, pyarrow 25.0.1, pydantic 2.13.5, xgboost 3.4.1, scikit-learn 1.9.1 and pytest 9.1.1.

## Licences

dynamic-des, pydantic and pytest are MIT. pyiceberg, pyarrow and xgboost are Apache-2.0. scikit-learn is BSD-3-Clause.

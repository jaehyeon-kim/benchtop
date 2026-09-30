# Data

Every file, topic and key the [product-recommender](../README.md) steps read and write. The files are in `data/`. Only `products.csv` and `world_pop.csv` are kept in git; `python -m recommender.run.prepare` writes the rest, the same for the same seed.

## Input files

`products.csv`: the 200 products, in 8 categories such as `Pizzas` and `Drinks & Desserts`.

| Column | Description |
|---|---|
| `product_id` | 1 to 200. The coffees are 189 to 194 |
| `name`, `description` | the text TextWiser turns into numbers |
| `price` | in dollars, from 3.00 to 38.99 |
| `category` | one of the 8 categories |

`world_pop.csv`: postal areas with their location and population. Users are placed in Melbourne's 159 postal areas, each chosen in proportion to its population.

## Files that prepare writes

`users.csv`: 1,000 users, with `user_id`, a name and email, `age` (16 to 70), `gender` (`M` or `F`), a street address and postal area, `latitude`, `longitude` and `traffic_source` (`Search`, `Organic`, `Facebook`, `Email` or `Display`, about 70% `Search`).

`user_features.csv`: each user as 10 features, keyed by `user_id`.

| Columns | Description |
|---|---|
| `age`, `latitude`, `longitude` | scaled to 0 to 1 |
| `gender_F`, `gender_M` | one-hot gender |
| `traffic_source_Display` to `traffic_source_Search` | one-hot traffic source |

`product_features.csv`: each product's features, keyed by `product_id`.

| Columns | Description |
|---|---|
| `txt_0` to `txt_9` | the text as 10 numbers |
| `cat_Appetizers & Sides` to `cat_Salads & Healthy Options` | one-hot category |
| `is_coffee` | 1 for products 189 to 194 |
| `price` | scaled to 0 to 1 |

`training_log.csv`: the history, 10,000 visits over the 90 days to 1 January 2026. Each row has an `event_id`, the user's 10 features, the 5 time flags (`is_morning` to `is_weekday`), the `product_id` shown and the `response`, 1 for a click. Its mean response is 0.1365.

`preprocessing_artifacts.pkl`: the fitted transformations, so later users get the same features: `user_scaler`, `user_columns`, `product_text_model`, `product_price_scaler` and `product_columns`.

[Concepts](concepts.md#features-and-the-context-vector) explains each transformation.

## Step 2

| Where | What |
|---|---|
| `s3://odctl-dev/recsys/training_log.csv` | the history, uploaded by `./submit-job.sh` for the Flink job to read |
| Kafka topic `feedback-events` | one Avro event per visit, keyed by product: `event_id`, `product_id`, `reward` (1 or 0), `context_vector` (the 15 numbers) and `timestamp` (the visit time, in milliseconds) |
| schema registry subject `feedback-events-value` | the event's Avro schema |
| Valkey keys `linucb:<product id>` | each product's model as JSON: `A_inv`, 15 by 15, and `b`, 15 numbers |

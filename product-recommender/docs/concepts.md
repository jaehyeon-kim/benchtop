# Concepts behind the design

The ideas the [product-recommender](../README.md) is built on, each tied to where the code uses it.

## Cold start

A recommender that learns from each user's past clicks has nothing to go on for a new user or product. This is the **cold-start problem**.

This project avoids it by learning from what is known about each visit instead. A first-time visitor still has an age, a way of arriving at the shop and a time of day, so a model that links those to clicks can recommend straight away.

## Bandits

A **multi-armed bandit** chooses between options whose value it can only learn by trying them. Here each product is an arm, showing it is a pull, and a click is the **reward**, 1 for a click and 0 for none.

On every visit a bandit balances two goals:

- **Exploitation:** show the products it expects to be clicked, to collect clicks now.
- **Exploration:** show products it knows little about, to learn whether they are better.

A plain bandit learns one click rate per product. A **contextual bandit** learns how the click rate depends on the **context**, what is known about the visit. For example, a coffee can be a good choice in the morning and a poor one at night.

## Features and the context vector

A model needs numbers, so [`features.py`](../recommender/data/features.py) turns each user and product into **features**:

- **Categories**, such as gender, become **one-hot** columns: one per value, 1 for the row's value and 0 for the rest. `gender_M` is 1 for a man.
- **Numbers**, such as age and price, are **min-max scaled** to lie between 0 and 1, so no feature outweighs another because of its unit.
- **Product text** becomes 10 numbers, `txt_0` to `txt_9`. TextWiser counts each word with TF-IDF, which weights down words common to many products, then reduces the counts to their 10 strongest patterns with SVD, singular value decomposition.
- **Visit time** becomes five flags: `is_morning` (06:00 to 12:00), `is_afternoon` (12:00 to 18:00), `is_evening` (18:00 to 24:00), `is_weekend` and `is_weekday`.

The fitted transformations are saved, so a new user gets the same ones. The **context vector** `x` the bandit sees is the user's 10 features and the 5 time flags, 15 numbers. Only the hidden formula reads the product features.

## Hidden click formula

`click_probability` in [`simulation.py`](../recommender/engine/simulation.py) decides the simulated clicks, and the bandit never sees it. It starts from a score of −2.5 and applies four rules:

| Rule | When | Change |
|---|---|---|
| Morning coffee | a morning visit and a coffee | +2.5 |
| Weekend comfort food | a weekend visit and a pizza or burger | +1.8 |
| Budget | a young user (scaled age below 0.25) and an expensive product (scaled price above 0.8) | −3.0 |
| Traffic source | the user arrived from a search | +0.5 |

A sigmoid, `1 / (1 + e^(−score))`, turns the score into a chance, and `will_click` draws against it. A visit no rule applies to has a 7.6% chance, and a morning coffee 50%. So even the right product is often not clicked. The history pairs random users with random products, so most visits match no rule, and its click rate is 13.65%.

## LinUCB

LinUCB keeps one linear model per product, made of two values:

- `A`, a 15 by 15 matrix. It starts as the identity matrix and grows by `x xᵀ` each time the product is shown, so it records the contexts the product has been shown in.
- `b`, 15 numbers. It starts at 0 and grows by the reward times `x`, so it records the contexts the product was clicked in.

From these come `A_inv`, the inverse of `A`, and the weights `θ = A_inv b`. A product's score for context `x` is:

`score = xᵀθ + α √(xᵀ A_inv x)`

- `xᵀθ` is the predicted reward, the exploitation part.
- `α √(xᵀ A_inv x)` is the **upper confidence bound**, a bonus that is large when the product has rarely been shown in contexts like `x`. This is the exploration part.
- `α` sets the bonus's weight. It is 1.0 here, `ALPHA` in [`config.py`](../recommender/core/config.py), and also MABWiser's default.

LinUCB shows the five highest scores. The prototype uses MABWiser's LinUCB ([`bandit.py`](../recommender/engine/bandit.py)). The Flink job makes the same updates in Kotlin, and the live client scores with the same formula in `bandit.score`.

## Online learning

**Online learning** means learning from each result as it arrives, instead of retraining on all the data now and then. After each visit, `recommender.run.local` calls MABWiser's `partial_fit` with the product the result is recorded against, the clicked one or else the first one shown. It adds the visit to that product's `A` and `b`, so the next visit is ranked with what this one taught.

Both the prototype and the Flink job first train on the whole history, a **warm start**, so they do not begin from nothing.

## Offline policy evaluation

A **policy** is the rule that picks what to show. **Offline policy evaluation** scores a policy on a record of past visits instead of on real users. `recommender.run.evaluate` trains each policy with Mab2Rec on the first 8,000 visits, in order, and scores it with Jurity on the last 2,000. Each visit counts as its own user.

The history records only the one product each visit was shown. What the user would have done with any other product is unknown. So a visit is scored only when its logged product is among the policy's five recommendations, which Jurity calls **matching**. With 5 of 200 products recommended, about 1 visit in 40 matches, some 50 of the 2,000.

| Column | Measures, on the matched visits |
|---|---|
| `AUC(score)@5` | how well the policy's scores separate clicked products from the rest: 0.5 is chance, 1.0 is perfect |
| `CTR(score)@5` | the click rate |
| `Precision@5` | for visits that ended in a click, the share of the five that were clicked. One product is logged per visit, so it is at most 0.2 |
| `Recall@5` | for the same visits, whether the clicked product was among the five. Here it is exactly five times the precision |

| Policy | How it picks |
|---|---|
| Random | products at random, the baseline |
| Popularity | products drawn at random, each weighted by its overall click rate |
| LinGreedy | a linear model per product with no bonus. It picks the best prediction, and a random product 10% of the time |
| LinUCB | as above |
| LinTS | a linear model per product whose weights are drawn at random around their estimate, which is called Thompson sampling |
| ClustersTS | visits grouped into 10 clusters by their context with k-means, and Thompson sampling within the nearest cluster |

## Splitting serving from training

One process that recommends and learns does not scale: the next user waits while it learns, and copies of it would each learn a different model. Step 2 splits the work:

- **Serving**, the live client, keeps nothing between visits: it reads the models, scores and sends the result. So copies of it could run side by side.
- **Training**, the Flink job, holds every product's `A` and `b` and updates them.
- **Kafka** sits between them. The client writes an event and moves on, and a burst of visits waits in the topic rather than slowing the client.
- **Valkey** is where training hands the models to serving.

The cost is that the client ranks with models a few seconds old.

## Flink job

The trainer in [`recsys-trainer`](../recsys-trainer) is a Flink job, which runs until it is cancelled and processes each event as it arrives.

- **Hybrid source:** Flink's `HybridSource` reads its sources one after another. The job reads the history file from SeaweedFS, then switches to the topic `feedback-events` when the file ends.
- **Keyed state per product:** `keyBy { it.productId }` sends every event for a product to the same task, and Flink's **keyed state** keeps a value per key. So each event updates exactly its own product's `A` and `b`, with no lookup.
- **Checkpoints:** odctl keeps the state in RocksDB, an embedded database on each TaskManager. Every 10 seconds the job saves a **checkpoint**, a consistent copy of the state, to SeaweedFS. After a failure it restarts from the last one, and no feedback is counted twice.
- **Writes every 5 seconds:** [`LinUCBUpdater`](../recsys-trainer/src/main/kotlin/me/jaehyeon/topology/processing/LinUCBUpdater.kt) updates `A` and `b` on every event. The first update after a write starts a 5-second timer for that product. When it fires, the job inverts `A` with an LU decomposition and writes `A_inv` and `b`. A product with hundreds of events in that time is written once, and the client never inverts anything.
- **Sink:** each model is written with a Valkey `SET` of `linucb:<product id>`. `SET` replaces the old value, so writing a model twice after a restart does no harm.

## Valkey as the model store

Valkey is an in-memory key-value store, forked from Redis, and Redis clients work with it: the client uses `redis-py` and the Flink job Jedis. Each product's model is one key holding JSON with `A_inv` and `b`.

The client reads every model on every visit with one `MGET` of all 200 keys ([`valkey.py`](../recommender/stores/valkey.py)). It has to: the score depends on the visit's context, so finding the five best means scoring every product for this visit. `MGET` returns them in one round trip.

A product with no key yet is read as an untrained model, `A_inv` the identity and `b` zeros, which is where LinUCB starts. Its bonus is large, so it gets shown and gets its first feedback.

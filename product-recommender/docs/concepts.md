# Concepts behind the design

The ideas the [product-recommender](../README.md) is built on, each tied to where the code uses it.

## Cold start

A recommender that learns from each user's past clicks has nothing to go on for a new user or product. This is the **cold-start problem**.

This project avoids it by learning from the visit's context instead: a first-time visitor still has an age, a traffic source and a time of day.

## Bandits

A **multi-armed bandit** is a problem in which a learner chooses again and again between options, and learns what each option is worth only by trying it. The terms:

- **Arm:** one option. Here each of the 200 products is an arm.
- **Pull:** choosing an arm once. Here, showing a product on a visit.
- **Reward:** what a pull returns. Here 1 for a click and 0 for none.
- **Policy:** the rule that picks the arm, using what has been learned so far.
- **Bandit feedback:** only the reward of the pulled arm is seen. A visit shows whether the user clicked the product they were shown, and nothing about the others.

On every visit a bandit balances two goals:

- **Exploitation:** show the products it expects to be clicked, to collect clicks now.
- **Exploration:** show products it knows little about, to learn whether they are better.

Three common ways to balance the two, each used by a policy here:

- **ε-greedy (epsilon-greedy):** show the product with the best expected reward, but with a small probability ε show a random one. LinGreedy uses ε = 0.1.
- **Upper confidence bound (UCB):** add to each product's expected reward a bonus that is large when the product has been tried little, and show the highest total. LinUCB does this.
- **Thompson sampling:** keep a probability distribution over each product's value, draw one value from each distribution, and show the highest draw. An uncertain product sometimes draws high, so it gets tried. LinTS and ClustersTS do this.

A plain bandit learns one click rate per product. A **contextual bandit** learns how the click rate depends on the **context**, what is known about the visit. For example, a coffee can be a good choice in the morning and a poor one at night.

Each policy scores all 200 products and shows the five highest, the **top-k** with k = 5 (`TOP_K` in [`config.py`](../recommender/core/config.py)).

## Features and the context vector

A model needs numbers, so [`features.py`](../recommender/data/features.py) turns each user and product into **features**:

- **Categories**, such as gender, become one-hot columns, such as `gender_M`.
- **Numbers**, such as age and price, are min-max scaled to lie between 0 and 1.
- **Product text** becomes 10 numbers, `txt_0` to `txt_9`: TextWiser weights the words with TF-IDF and reduces them to 10 dimensions with SVD.
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

These are the two sums of a **ridge regression**, a linear regression that adds λ times the identity matrix to keep its weights small. MABWiser uses λ = 1, which is why `A` starts as the identity. From these come `A_inv`, the inverse of `A`, and the weights `θ = A_inv b`. A product's score for context `x` is:

`score = xᵀθ + α √(xᵀ A_inv x)`

- `xᵀθ` is the predicted reward, the exploitation part.
- `α √(xᵀ A_inv x)` is the **upper confidence bound**, a bonus that is large when the product has rarely been shown in contexts like `x`. This is the exploration part.
- `α` sets the bonus's weight. It is 1.0 here, `ALPHA` in [`config.py`](../recommender/core/config.py), and also MABWiser's default.

LinUCB shows the five highest scores. The prototype uses MABWiser's LinUCB ([`bandit.py`](../recommender/engine/bandit.py)). The Flink job makes the same updates in Kotlin, and the live client scores with the same formula in `bandit.score`.

## Online learning

**Online learning** means learning from each result as it arrives, instead of retraining on all the data now and then. After each visit, `recommender.run.local` calls MABWiser's `partial_fit` with the product the result is recorded against, the clicked one or else the first one shown. It adds the visit to that product's `A` and `b`, so the next visit is ranked with what this one taught.

Both the prototype and the Flink job first train on the whole history, a **warm start**, so they do not begin from nothing.

## Offline policy evaluation

A new policy could be tested on real users, but a poor one would lose clicks while it was tested. **Offline policy evaluation** scores a policy on **logged data** instead: a record of past visits made under another policy, called the **logging policy**. Each logged visit has its context, the one product shown, and the reward.

The log holds the reward of the product that was shown, and nothing about the 199 that were not.

### Replay

The **replay** method, from Li, Chu, Langford and Wang's paper [Unbiased Offline Evaluation of Contextual-bandit-based News Article Recommendation Algorithms](https://arxiv.org/abs/1003.5956) (WSDM 2011), steps through the log one visit at a time. For each visit it asks the policy what it would show. If that is the logged product, the visit is kept and its reward counted. Otherwise the visit is skipped. Keeping the visits that pass a test and discarding the rest is a form of **rejection sampling**.

Replay gives an unbiased estimate when two conditions hold:

- The visits are independent of each other.
- The logging policy chose the product **uniformly at random**, every product with the same chance.

Then the kept visits are a fair sample, and with K products about one visit in K is kept.

For example, a log shows each visit coffee, pizza or salad at random. The policy shows coffee in the morning and pizza otherwise.

| Visit | Time | Logged product | Clicked | Policy would show | Result |
|---|---|---|---|---|---|
| 1 | morning | coffee | yes | coffee | kept, 1 click |
| 2 | morning | pizza | no | coffee | skipped |
| 3 | evening | salad | no | pizza | skipped |
| 4 | evening | pizza | no | pizza | kept, 0 clicks |
| 5 | morning | salad | yes | coffee | skipped |
| 6 | evening | coffee | no | pizza | skipped |

Two visits are kept and one was clicked, so the estimated click rate is 50%. Visit 5 was clicked, but it does not count, because the policy would not have shown salad. If the log had always shown coffee in the morning, morning visits would be kept far more often, and the sample would no longer be fair.

This project meets both conditions. [`history.py`](../recommender/data/history.py) pairs a random user with a product drawn uniformly from the 200, at a random time. A real shop's log would not, and for that case Jurity also offers **inverse propensity scoring (IPS)** and **doubly robust** estimates, which weight each kept visit by how likely the logging policy was to show its product.

### How this project applies it

`recommender.run.evaluate` ([`evaluate.py`](../recommender/run/evaluate.py)) passes the six policies to Mab2Rec's `benchmark`. It trains each policy on the first 8,000 visits and scores it on the last 2,000 with Jurity's metrics. The visit times are random, so this is a random split. Each visit counts as its own user.

It differs from the paper's replay in two ways:

- **Five products, not one.** A visit is kept when the logged product is among the policy's five, which Jurity calls **matching**. The chance is 5 in 200, or 1 in 40, whatever the policy, so about 50 of the 2,000 visits are kept. The click rate on kept visits estimates the click rate of one product picked at random from the five.
- **A fixed policy.** Each policy learns only from the 8,000 training visits. Unlike the paper's replay, it does not learn from the kept visits.

Four of LinUCB's test visits, from the run with the default seed, 1237:

| Visit | Time | Logged product | Clicked | Among LinUCB's five | Result |
|---|---|---|---|---|---|
| 8221 | weekend evening | Buffalo Chicken Pizza | yes | yes | kept, 1 click |
| 8231 | weekday morning | Grilled Chicken Sandwich | no | yes | kept, 0 clicks |
| 8003 | weekend evening | Dim Sims | yes | no | skipped |
| 8024 | weekday morning | Strawberry Milkshake | yes | no | skipped |

On weekend evenings LinUCB's five are mostly pizzas, and on weekday mornings they include coffees, as the [hidden click formula](#hidden-click-formula) rewards. Across the 2,000 test visits, 44 are kept and 9 of those were clicked, so its `CTR(score)@5` is 9 / 44 = 20.5%.

### Support and uncertainty

The number of visits a score is based on is its **support**. A click rate p measured on n visits has a **standard error** of about √(p(1 − p) / n), the typical distance between the measured rate and the true one. With p = 0.2 and n = 50 that is about 0.06. So two click rates a few points apart may differ by chance alone. AUC depends on the few clicks among them, so it is less certain still.

### Metrics

| Column | Measured on | Measures |
|---|---|---|
| `AUC(score)@5` | the kept visits | how well the policy's score for the logged product separates clicked visits from the rest. **AUC**, the area under the ROC curve, is the chance that a clicked visit gets a higher score than one that was not clicked: 0.5 is chance, 1.0 is perfect |
| `CTR(score)@5` | the kept visits | the **click-through rate (CTR)**, the share of visits that ended in a click |
| `Precision@5` | the test visits that ended in a click | the share of the five recommended products that were the clicked one. One product is logged per visit, so it is at most 0.2 |
| `Recall@5` | the same visits | 1 if the clicked product was among the five and 0 if not, averaged. One product is logged per visit, so it is exactly five times the precision |

### Policies

| Policy | How it picks |
|---|---|
| Random | products at random, the baseline |
| Popularity | products drawn at random, each weighted by its overall click rate. It ignores the context |
| LinGreedy | a linear model per product, with ε-greedy exploration: the best prediction, or a random product 10% of the time |
| LinUCB | a linear model per product, with an upper confidence bound, as above |
| LinTS | a linear model per product, with Thompson sampling: the weights are drawn at random around their estimate |
| ClustersTS | visits grouped into 10 clusters by their context with k-means, and Thompson sampling on the click rates within the visit's nearest cluster |

## Splitting serving from training

One process that recommends and learns does not scale: the next user waits while it learns, and copies of it would learn different models. Step 2 splits the work:

- **Serving**, the live client, keeps nothing between visits: it reads the models, scores and sends the result. So copies of it could run side by side.
- **Training**, the Flink job, holds every product's `A` and `b` and updates them.
- **Kafka** sits between them. The client writes an event and moves on, and a burst of visits waits in the topic rather than slowing the client.
- **Valkey** is where training hands the models to serving.

The cost is that the client ranks with models a few seconds old.

This makes Step 2 an **event-driven** system: the client writes an **event**, a record of something that happened, and the Flink job reacts to it. Neither calls the other.

## Feedback events

A **feedback event** is the result of one visit, which the live client ([`live.py`](../recommender/run/live.py)) sends to the topic `feedback-events`, keyed by product id. It holds the product the result is recorded against (the clicked one, or else the first one shown), the reward, the 15-number context vector, an id and the visit time. It is encoded in Avro with `FEEDBACK_SCHEMA` ([`models.py`](../recommender/core/models.py)), which the serializer registers in Karapace under the subject `feedback-events-value`. The Flink job reads the schema id at the start of each message to fetch the schema ([`KafkaSourceFactory.kt`](../recsys-trainer/src/main/kotlin/me/jaehyeon/infrastructure/kafka/KafkaSourceFactory.kt)).

## Flink job

The trainer in [`recsys-trainer`](../recsys-trainer) is a Flink job, which runs until it is cancelled and processes each event as it arrives.

A Flink cluster's **JobManager** plans the job and coordinates its checkpoints, and its **TaskManagers**, three in odctl's `flink-full`, run the tasks. The job runs each stage as six tasks, its parallelism in [`AppConfig.kt`](../recsys-trainer/src/main/kotlin/me/jaehyeon/config/AppConfig.kt).

- **Hybrid source:** Flink's `HybridSource` reads its sources one after another. The job reads the history file from SeaweedFS, then switches to the topic `feedback-events` when the file ends.
- **Keyed state per product:** `keyBy { it.productId }` sends every event for a product to the same task, and Flink's **keyed state** keeps a value per key. So each event updates exactly its own product's `A` and `b`, with no lookup.
- **Checkpoints:** odctl keeps the state in RocksDB, an embedded database on each TaskManager. Every 10 seconds the job saves a **checkpoint**, a consistent copy of the state, to SeaweedFS. After a failure it restarts from the last one, and no feedback is counted twice.
- **Writes every 5 seconds:** [`LinUCBUpdater`](../recsys-trainer/src/main/kotlin/me/jaehyeon/topology/processing/LinUCBUpdater.kt) updates `A` and `b` on every event. The first update after a write starts a 5-second timer for that product. When it fires, the job inverts `A` with an LU decomposition and writes `A_inv` and `b`. A product with hundreds of events in that time is written once, and the client never inverts anything.
- **Sink:** each model is written with a Valkey `SET` of `linucb:<product id>`. `SET` replaces the old value, so writing a model twice after a restart does no harm.

## Valkey as the model store

Valkey is a fork of Redis, so the client uses `redis-py` and the Flink job Jedis. Each product's model is one key holding JSON with `A_inv` and `b`.

The client reads every model on every visit with one `MGET` of all 200 keys ([`valkey.py`](../recommender/stores/valkey.py)). It has to: the score depends on the visit's context, so finding the five best means scoring every product for this visit. `MGET` returns them in one round trip.

A product with no key yet is read as an untrained model, `A_inv` the identity and `b` zeros, which is where LinUCB starts. Its bonus is large, so it gets shown and gets its first feedback.

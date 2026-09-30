# Benchtop

Small projects, built and tested on a bench. Each one is self-contained, runs from a cold clone, and takes about a day to build.

## Layout

One directory per project. Each carries its own `README.md` saying what it builds and how to run it, and can be read, run and understood without the others. Only the code checks are shared, at the root.

```
benchtop/
  <project>/
    README.md      what it builds, how to run it
    ...
```

## Projects

Every project runs on your own machine, with Docker, [uv](https://docs.astral.sh/uv/) and [odctl](https://github.com/jaehyeon-kim/odctl), which starts the services each one needs. They are listed from the fewest services to the most, and each adds one new idea, so reading them in order is the gentlest path.

- [live-dashboard](live-dashboard/README.md): two dashboards that update themselves as a simulated shop takes orders. You learn how a server pushes new data to a web page as it arrives (WebSockets), and build the same dashboard twice, in Python (Streamlit) and in TypeScript (Next.js). Needs only PostgreSQL. A series of three posts starts with the [data producer](https://jaehyeon.me/blog/2025-02-18-realtime-dashboard-1/).
- [ecommerce-cdc](ecommerce-cdc/README.md): every change to a shop's database, captured as it happens and saved as files. You learn change data capture: reading a database's own log of changes instead of querying it (Debezium, Kafka Connect, Avro).
- [order-streams](order-streams/README.md): a stream of orders, first sent and read by small programs, then summarised every few seconds. You learn how Kafka moves messages, and two ways to process a stream as it flows (Kafka Streams and Flink), all in Kotlin. A series of five posts starts with [Kafka clients with JSON](https://jaehyeon.me/blog/2025-05-20-kotlin-getting-started-kafka-json-clients/).
- [game-leaderboard](game-leaderboard/README.md): live leaderboards for a simulated mobile game. You learn to write SQL queries that never finish and keep their answer up to date as scores arrive (Flink SQL), including late scores and top-10 rankings.
- [product-recommender](product-recommender/README.md): a shop that learns which products to show each visitor from what they click. You learn contextual bandits, a way to recommend that balances trying new products with showing proven ones, first in plain Python, then split into a live service and a streaming trainer (Flink, Valkey). The [prototype](https://jaehyeon.me/blog/2026-01-29-prototype-recommender-with-python/) and [production](https://jaehyeon.me/blog/2026-02-23-productionize-recommender-with-eda/) posts explain it.
- **MLOps with a Feature Store**: a series that rebuilds the three projects in Jim Dowling's book on feature stores with open-source tools. You learn how a machine learning system is split into pipelines that share a feature store and a model registry. The [introduction](https://jaehyeon.me/blog/2026-09-28-mlops-with-a-feature-store/) explains the series.
  1. [air-quality](air-quality/README.md): forecasts daily air pollution (PM2.5) for the next seven days from weather forecasts, with a monitoring dashboard and a chat assistant.
  2. Credit card fraud detection: real-time features from a stream of transactions. Planned.
  3. Video recommender: retrieves and ranks videos for each user in real time. Planned.

## Licence

MIT. See [LICENSE](LICENSE).

# Benchtop

Projects built and tested on a bench. Each one is self-contained, runs from a cold clone, and takes about a day to build.

## Projects

Every project runs on your own machine, with Docker, [uv](https://docs.astral.sh/uv/) and [odctl](https://github.com/jaehyeon-kim/odctl), which starts the services each one needs. Start with whichever you want. New projects are added over time. The [introduction](https://jaehyeon.me/blog/2026-09-30-introducing-benchtop/) explains how the projects are organised.

### Streaming and data engineering

- [Change Data Capture with Debezium](ecommerce-cdc/README.md): every change to a shop's database, captured as it happens and saved as files. You learn change data capture: reading a database's own log of changes instead of querying it (Debezium, Kafka Connect, Avro). The [post](https://jaehyeon.me/blog/2026-10-01-ecommerce-cdc-debezium-kafka-connect/) walks through it.
- [Order Streams with Kafka and Flink](order-streams/README.md): a stream of orders, first sent and read by small programs, then summarised every few seconds. You learn how Kafka moves messages, and two ways to process a stream as it flows (Kafka Streams and Flink), all in Kotlin. A series of five posts starts with [Kafka clients with JSON](https://jaehyeon.me/blog/2025-05-20-kotlin-getting-started-kafka-json-clients/).
- [Live Game Leaderboards with Flink SQL](game-leaderboard/README.md): live leaderboards for a simulated mobile game. You learn to write SQL queries that never finish and keep their answer up to date as scores arrive (Flink SQL), including late scores and top-10 rankings. The [post](https://jaehyeon.me/blog/2026-10-02-game-leaderboard-flink-sql/) walks through it.

### Live applications

- [Live Sales Dashboards](live-dashboard/README.md): two dashboards that update themselves as a simulated shop takes orders. You learn how a server pushes new data to a web page as it arrives (WebSockets), and build the same dashboard twice, in Python (Streamlit) and in TypeScript (Next.js). Needs only PostgreSQL. A series of three posts starts with the [data producer](https://jaehyeon.me/blog/2025-02-18-realtime-dashboard-1/).

### Machine learning and MLOps

- [Online Product Recommender](product-recommender/README.md): a shop that learns which products to show each visitor from what they click. You learn contextual bandits, a way to recommend that balances trying new products with showing proven ones, first in plain Python, then split into a live service and a streaming trainer (Flink, Valkey). The [prototype](https://jaehyeon.me/blog/2026-01-29-prototype-recommender-with-python/) and [production](https://jaehyeon.me/blog/2026-02-23-productionize-recommender-with-eda/) posts explain it. It also uses the streaming stack.
- **MLOps with a Feature Store**: most models need recent history that a prediction request does not carry, such as a user's last clicks. Training and serving must also compute that history the same way, or the model sees different inputs in production than it learned from. A feature store computes the inputs once and serves the same values to both. You learn how a machine learning system is split into pipelines that share a feature store and a model registry. In this series, we rebuild the three projects in [Jim Dowling's book on MLOps with feature stores](https://www.oreilly.com/library/view/building-machine-learning/9781098165222/), using open-source tools.
  1. [Air Quality Forecasting with a Feature Store (WIP)](air-quality/README.md): forecasts daily air pollution (PM2.5) for the next seven days from weather forecasts, with a monitoring dashboard and a chat assistant.
  2. Credit card fraud detection: real-time features from a stream of transactions. Planned.
  3. Video recommender: retrieves and ranks videos for each user in real time. Planned.

## Licence

MIT. See [LICENSE](LICENSE).

# Change Data Capture with Debezium

Change data capture (CDC) on a simulated online shop. A simulation writes the shop's activity to PostgreSQL in real time: new users, orders and page views, and updates when an order changes status or a user moves address. Debezium reads every change from PostgreSQL's log and sends it to Kafka, and a sink connector saves the changes as files in object storage. Everything runs on your own machine.

The shop's tables follow the [theLook eCommerce](https://console.cloud.google.com/marketplace/product/bigquery-public-data/thelook-ecommerce) dataset.

It is described in a post: [Change Data Capture on a Simulated Online Shop with Debezium and Kafka Connect](https://jaehyeon.me/blog/2026-10-01-ecommerce-cdc-debezium-kafka-connect/).

More detail is in two documents:

- [Concepts](docs/concepts.md): how CDC, Debezium, Kafka Connect, Avro and the simulation work, explained from the start.
- [Data](docs/data.md): every table's columns, the topics and schemas, and the shape of a change event.

## Architecture

![The simulation writes to PostgreSQL, Debezium streams each change to Kafka with its schema in Karapace, and the S3 sink saves the changes to SeaweedFS](images/architecture.png)

Three parts do the work:

- **Simulation:** the shop itself. It writes rows to six PostgreSQL tables, and changes them as orders move on.
- **Debezium:** reads each change from PostgreSQL's write-ahead log, the record of every change the database makes, and writes it to a Kafka topic for its table.
- **S3 sink:** reads those topics and saves the changes as files in SeaweedFS.

The simulation never writes to Kafka. It only writes to its database, and every change still reaches Kafka. That is the point of CDC, which [Concepts](docs/concepts.md#change-data-capture-and-the-write-ahead-log) compares with querying the tables for changes.

### What you will build

1. Run the simulation, which fills the tables and keeps changing them.
2. Deploy the two [connectors](docs/concepts.md#kafka-connect). Debezium takes a snapshot of the existing rows, then streams every insert and update to Kafka. The S3 sink saves them as files.
3. Look at the change events in Kafka UI, their schemas in the [schema registry](docs/concepts.md#avro-and-the-schema-registry), and the files in SeaweedFS.
4. Cut the warehouse's pickers while the simulation runs, and watch cancelled orders appear in the stream.

To start again at any point, `python -m ecommerce.stores.cleanup` removes everything this project has created and keeps the services running. See [Clean up](#clean-up).

### Tools

| Tool | Role here |
|---|---|
| [dynamic-des](https://github.com/jaehyeon-kim/dynamic-des) | simulates the shop, and writes its rows to PostgreSQL |
| PostgreSQL | the shop's database; its write-ahead log is what Debezium reads |
| Debezium | the source connector that turns each change into an event |
| Kafka and Kafka Connect | Kafka holds one topic of changes per table; Connect runs the two connectors |
| Karapace | the schema registry, which stores the Avro schema of each topic |
| Aiven S3 sink | the sink connector that writes the changes to files |
| SeaweedFS | an S3-compatible object store for the files |
| Kafka UI | a web page for the topics, messages, schemas and connectors |
| [odctl](https://github.com/jaehyeon-kim/odctl) | starts all the services above with Docker Compose |

## Environment setup

You need Docker, [uv](https://docs.astral.sh/uv/) and Python 3.13. Run every command from this folder.

### Python environment

One virtual environment holds everything, including the `odctl` command:

```bash
uv venv                             # create .venv
source .venv/bin/activate           # activate it, in each new shell
uv pip install -r requirements.txt
```

### Services

```bash
odctl up postgres kafka-lite storage
```

`odctl ps --all` lists the containers:

```text
🌟 Active Profiles: kafka-lite, postgres, storage

┏━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Container       ┃ Service      ┃ State   ┃ Health  ┃ Ports                                                   ┃
┡━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ connect         │ connect      │ running │ -       │ 8083 ➡️  8083/tcp                                       │
│ kafka           │ kafka        │ running │ -       │ 29092 ➡️  29092/tcp, 9092 ➡️  9092/tcp                  │
│ kafka-ui        │ kafka-ui     │ running │ -       │ 8086 ➡️  8080/tcp                                       │
│ karapace        │ karapace     │ running │ healthy │ 8081 ➡️  8081/tcp                                       │
│ odctl-init-deps │ init-deps    │ exited  │ -       │ -                                                       │
│ postgres        │ postgres     │ running │ healthy │ 5432 ➡️  5432/tcp                                       │
│ seaweed         │ seaweed      │ running │ healthy │ 8333 ➡️  8333/tcp, 8889 ➡️  8888/tcp, 9333 ➡️  9333/tcp │
│ seaweed-init    │ seaweed-init │ exited  │ -       │ -                                                       │
└─────────────────┴──────────────┴─────────┴─────────┴─────────────────────────────────────────────────────────┘
```

odctl starts three profiles:

- `postgres`: PostgreSQL, already set up for CDC. It runs with `wal_level=logical`, and has the schema `cdc` with the publication `cdc_pub`, which covers every table in that schema.
- `kafka-lite`: one Kafka broker, Kafka Connect with the Debezium and Aiven S3 connectors, Karapace and Kafka UI.
- `storage`: SeaweedFS.

The web UIs:

- Kafka UI: http://127.0.0.1:8086
- SeaweedFS file browser: http://127.0.0.1:8889

[`config.py`](ecommerce/core/config.py) sets the addresses of PostgreSQL, Kafka, Connect, Karapace and SeaweedFS, so there is nothing to configure.

## Step 1: run the simulation

```bash
python -m ecommerce.simulation.run --minutes 16     # or leave out --minutes, and stop with Ctrl + C
```

It creates the six tables in the `cdc` schema if they are missing, and writes the 10 distribution centres, the 260 products and 100 users. It then runs in real time. With the default parameters it places about 18 orders a minute. Each order is then updated when it ships and when it is delivered, and one in ten is later returned. `--seed <n>` makes every random choice repeat. [Data](docs/data.md#tables) lists every table's columns.

The model is in [`run.py`](ecommerce/simulation/run.py), and [Concepts](docs/concepts.md#simulation-model) describes it. The shop's rules are plain functions in [`shop.py`](ecommerce/simulation/shop.py), and the distribution centres, cities and products are in [`catalogue.py`](ecommerce/simulation/catalogue.py). Each row is built from a model in [`models.py`](ecommerce/core/models.py), with its bounds.

Leave it running, and use a second terminal for the next steps.

## Step 2: deploy the connectors

A connector is a job that Kafka Connect runs: a source connector copies data into Kafka, and a sink connector copies it out.

```bash
python -m ecommerce.cdc.connectors
```

[`connectors.py`](ecommerce/cdc/connectors.py) sends [`source.json`](ecommerce/cdc/source.json) and [`s3-sink.json`](ecommerce/cdc/s3-sink.json) to Connect's REST API, which creates each connector, or updates it if it exists.

[`source.json`](ecommerce/cdc/source.json) tells Debezium to read the six tables through the publication `cdc_pub` and its own replication slot, `ecommerce_cdc` ([Concepts](docs/concepts.md#publications-and-replication-slots)). It takes a snapshot of the existing rows, then streams each new change to `ecommerce.cdc.<table>`, in Avro.

[`s3-sink.json`](ecommerce/cdc/s3-sink.json) tells the S3 sink to read every `ecommerce.cdc.*` topic, decode each message with its schema, and write JSON lines files to `odctl-dev/ecommerce-cdc/<topic>/`.

Check that each connector and its task show `RUNNING`:

```bash
curl -s http://127.0.0.1:8083/connectors/ecommerce-cdc-source/status
curl -s http://127.0.0.1:8083/connectors/ecommerce-cdc-s3/status
```

Kafka UI shows the same under **Kafka Connect**, with the topics each connector reads or writes:

![Kafka UI listing the Debezium source and the S3 sink, both running](images/kafka-ui-connectors.png)

## Step 3: look at the changes

### Topics

In Kafka UI, open **Topics** and search for `ecommerce`. There is one topic per table, and `ecommerce-control`, which Step 4 uses:

![Kafka UI listing the six ecommerce.cdc topics and the control topic, with their message counts](images/kafka-ui-topics.png)

`orders` and `order_items` keep growing, because every change of status is a new message.

### Schemas

Open **Schema Registry** and search for `ecommerce`. Each topic has a key subject and a value subject, which Debezium registered when it first wrote to the topic:

![Kafka UI's schema registry page listing a key and a value subject for each ecommerce.cdc topic](images/kafka-ui-schemas.png)

Kafka UI and the S3 sink look each message's schema up here to decode it.

### Change events

Open `ecommerce.cdc.orders`, then **Messages**, and set both the key and value **Serde** to `SchemaRegistry` so the messages are decoded:

![Kafka UI showing the newest ecommerce.cdc.orders messages, decoded with their Avro schemas](images/kafka-ui-orders-messages.png)

Each message is one change, and its `op` field says which kind: `r` for a snapshot read, `c` for a create, `u` for an update. Open a few messages with the same key. They are the steps of one order: a create as `Processing`, then updates to `Shipped` and `Delivered`, each with the time of its step in `after`. `before` is null in every update. [Concepts](docs/concepts.md#debezium-change-events) explains why, and [Data](docs/data.md#change-events) describes the event's fields.

<details><summary>An order moving from <code>Processing</code> to <code>Shipped</code>, decoded with its schema</summary>

```json
{
  "before": null,
  "after": {
    "id": "23b2d4a9-360d-4973-a6da-fd1cc394ca10",
    "user_id": "862dd089-c746-4127-89d7-db4fcab59a2e",
    "status": "Shipped",
    "num_of_items": 1,
    "created_at": "2026-09-30T13:38:56+00:00",
    "updated_at": "2026-09-30T13:39:01+00:00",
    "shipped_at": "2026-09-30T13:39:01+00:00",
    "delivered_at": null,
    "cancelled_at": null,
    "returned_at": null
  },
  "source": {
    "version": "3.5.1.Final",
    "connector": "postgresql",
    "name": "ecommerce",
    "ts_ms": 1790775541329,
    "snapshot": "false",
    "db": "odctl",
    "schema": "cdc",
    "table": "orders",
    "txId": 45922,
    "lsn": 161004832
  },
  "op": "u",
  "ts_ms": 1790775541830
}
```

</details>

Some `source` and time fields are left out. `ts_ms` minus `source.ts_ms` shows Debezium sent the change about half a second after PostgreSQL made it.

### Files

In the SeaweedFS file browser, open `buckets/odctl-dev/ecommerce-cdc/`. There is a folder per topic. Each file is named after its [partition and the offset](docs/concepts.md#kafka-topics-partitions-and-offsets) of its first message, so `0-41.jsonl` holds partition 0 from offset 41:

![SeaweedFS file browser listing the JSON lines files the sink wrote for the orders topic](images/seaweedfs-files.png)

Each line is one change event, with its key, offset and time. The first line of the first `orders` file is the create event of the same order as above:

```json
{"offset":0,"value":{"before":null,"after":{...},"source":{...},"op":"c",...},"key":{"id":"23b2d4a9-360d-4973-a6da-fd1cc394ca10"},"timestamp":"..."}
```

Debezium creates `ecommerce.cdc.orders` only when the first order is placed, after the sink has started. In a test run with the consumer's default metadata refresh of five minutes, the first `orders` file took about six minutes. With [`s3-sink.json`](ecommerce/cdc/s3-sink.json) setting it to 30 seconds, it took about a minute ([Concepts](docs/concepts.md#s3-sink)).

## Step 4: change the simulation while it runs

The simulation reads parameter changes from the Kafka topic `ecommerce-control`, and uses each new value from the next time it draws one. [`control.py`](ecommerce/simulation/control.py) sends the changes. With the simulation and both connectors running, cut the pickers from three to one:

```bash
python -m ecommerce.simulation.control ecommerce.resources.pickers.current_cap 1
```

Three pickers pack about 36 orders a minute, twice as many as are placed. One picker packs about 12, fewer than the 18 or so placed, so orders queue for it. The `Shipped` updates in `ecommerce.cdc.orders` fall, and `Cancelled` updates appear as customers' patience runs out, which does not happen with three pickers. In one run, with the change sent at 12:45, the changes per minute were:

| Minute (UTC) | Pickers | New orders | Shipped | Cancelled |
|---|---|---|---|---|
| 12:41 | 3 | 18 | 18 | 0 |
| 12:42 | 3 | 23 | 22 | 0 |
| 12:43 | 3 | 20 | 21 | 0 |
| 12:44 | 3 | 27 | 27 | 0 |
| 12:46 | 1 | 18 | 10 | 3 |
| 12:47 | 1 | 10 | 13 | 6 |
| 12:48 | 1 | 20 | 10 | 0 |
| 12:49 | 1 | 16 | 13 | 4 |
| 12:50 | 1 | 24 | 12 | 7 |
| 12:51 | 1 | 16 | 9 | 4 |
| 12:52 | 1 | 17 | 12 | 10 |
| 12:53 | 1 | 13 | 10 | 8 |

![Kafka UI showing the ecommerce.cdc.orders messages filtered on Cancelled, with one update decoded to the status Cancelled](images/kafka-ui-orders-cancelled.png)

Send `3` to set it back. Other parameters work the same way, such as `ecommerce.arrival.visitor.rate`. `python -m ecommerce.simulation.control --help` lists them all. A change lasts until the simulation stops.

## Tests

The tests need none of the services:

```bash
python -m pytest tests
```

They check the order statuses, the catalogue, the row bounds, the connector settings and which topics the clean-up deletes. They also run the model at full speed with a fixed seed: every order finishes, one picker causes cancellations while three do not, and a seed repeats exactly. GitHub runs them, after the repository's lint checks, on every push to `main`.

## Clean up

`python -m ecommerce.stores.cleanup` removes everything this project has created, and keeps the services running. [`cleanup.py`](ecommerce/stores/cleanup.py) calls the delete function of each store in [`ecommerce/stores/`](ecommerce/stores), in this order:

- **Connectors:** stops both, deletes their stored offsets, so a new source connector takes a fresh snapshot, then deletes them.
- **Replication slot:** drops `ecommerce_cdc`, once the connector has disconnected from it. A slot left behind makes PostgreSQL keep its log forever.
- **Tables:** drops the six tables in `cdc`.
- **Kafka:** deletes the topics starting with `ecommerce.`, and `ecommerce-control`.
- **Schema registry:** deletes the 12 `ecommerce.cdc.*` subjects.
- **SeaweedFS:** deletes the files under `odctl-dev/ecommerce-cdc/`.

It is safe to run twice:

```bash
python -m ecommerce.stores.cleanup
```

## Tear down

```bash
odctl down --all --volumes          # answer y; --volumes also deletes the data
deactivate
rm -rf .venv
```

# Concepts behind the design

The ideas the [ecommerce-cdc](../README.md) project is built on, each explained from the start and tied to where the project uses it.

## Change data capture and the write-ahead log

Other systems often need to know what changed in a database, such as a search index or a data lake. **Polling** finds out by querying again and again for rows updated since the last check. It needs an `updated_at` column that every writer keeps right, it sees only the latest value of a row that changed twice between checks, and it never sees a deleted row.

**Change data capture (CDC)** reads the changes themselves, in the order the database made them. Here, when an order goes from `Processing` to `Shipped` to `Delivered`, each step arrives as its own event.

PostgreSQL writes every change to its **write-ahead log (WAL)** before it changes the table, so it can replay the log after a crash. The WAL is in PostgreSQL's internal format. **Logical decoding** turns it into row changes another program can read. It needs `wal_level=logical`, which odctl's PostgreSQL sets, and an output plugin that decides the format. This project uses `pgoutput`, which Debezium's documentation calls "the standard logical decoding output plug-in in PostgreSQL 10+". It is built into PostgreSQL.

## Publications and replication slots

A **publication** names the tables whose changes are sent. odctl's `cdc_pub` is created `FOR TABLES IN SCHEMA cdc`, which PostgreSQL's documentation says covers "all tables in the specified list of schemas, including tables created in the future". Debezium's `publication.autocreate.mode` is `disabled`, so it uses `cdc_pub` and never creates its own.

A **replication slot** records how far one reader has got in the WAL. Debezium reads through the slot `ecommerce_cdc`, so after a restart it carries on where it stopped. The cost: PostgreSQL keeps every WAL file the slot has not read, even with no reader connected. odctl's `max_slot_wal_keep_size` is the default, `-1`, with which a slot "may retain an unlimited amount of WAL files". A forgotten slot therefore grows the WAL until the disk is full, and PostgreSQL's documentation says "if a slot is no longer required it should be dropped". The clean-up drops it.

## Debezium change events

Debezium is a **source connector**: it copies data from a database into Kafka, one topic per table, named `topicPrefix.schemaName.tableName`. The orders table's changes go to `ecommerce.cdc.orders`.

A new connector with `snapshot.mode` set to `initial` first reads every existing row (the **snapshot**), then streams each new change from the slot. Here the simulation starts first, so every row written before the connectors are deployed, such as the products and the first 100 users, arrives as a snapshot read.

Each message is one change, in a fixed **envelope**:

| Field | Holds |
|---|---|
| `op` | `r` for a snapshot read, `c` for create, `u` for update, `d` for delete |
| `before` | the row before the change, when PostgreSQL sends it |
| `after` | the row after the change |
| `source` | the database, table, transaction id and WAL position (`lsn`) the change came from |
| `ts_ms` | when the connector processed it; `source.ts_ms` is when the database made it |

The key is the row's primary key. Kafka's producer picks the partition "based on a hash of the key", so all the changes of one order land in one partition, in order. [Data](data.md#change-events) describes the event, and [Step 3](../README.md#change-events) of the README shows a full one.

`before` is null in every update here. A table's **replica identity** decides what PostgreSQL logs about the old row. With the default, the old key is sent only "if the update changed data in any of the column(s) that are part of the REPLICA IDENTITY index", and the whole old row only with `REPLICA IDENTITY FULL`. No update here changes an `id`, so no old row is sent. `after` still holds the full new row.

## Kafka Connect

**Kafka Connect** runs connectors, so the copying code does not have to be a separate application.

- A **worker** is one Connect process. odctl runs one in distributed mode, which keeps its settings, statuses and offsets in Kafka topics, so a restarted worker carries on.
- A **connector** is one job. It is created by sending its settings as JSON to the REST API: `PUT /connectors/<name>/config` creates or updates it.
- A **task** does the copying. A connector runs up to `tasks.max` tasks; both here run one.
- An **offset** records how far a connector has got: a WAL position for Debezium, a position in each partition for the sink. Connect commits offsets every `offset.flush.interval.ms`, one minute by default, which odctl does not change.

The clean-up calls `PUT /connectors/<name>/stop`, then `DELETE /connectors/<name>/offsets`, which Kafka allows only on a stopped connector. Otherwise a new connector with the same name would resume from the old position and skip the snapshot.

## Avro and the schema registry

A **converter** turns each record into bytes when it is written to Kafka. Both connectors use `io.confluent.connect.avro.AvroConverter`. **Avro** is a binary format: a message holds only the values, not the field names, so it is much smaller than JSON, and a reader needs the **schema** to decode it.

Schemas are kept in **Karapace**, a schema registry:

- Each schema is stored under a **subject**, by default the topic name plus `-key` or `-value`, such as `ecommerce.cdc.orders-value`.
- The registry numbers each schema. A message starts with a zero byte and the four-byte **schema id**, then the Avro data, so a reader looks the schema up once by its id.
- The converter registers schemas itself (`auto.register.schemas` defaults to `true`). If a table's columns change, the new schema becomes a new version of the subject, and the registry checks it against the last one. The default rule, `BACKWARD`, means "consumers using the new schema can read data produced with the last schema": adding an optional field passes, adding a required one fails.

## S3 sink

The Aiven S3 sink reads topics and writes their messages to files. Its README says it "flushes grouped records in one file per `offset.flush.interval.ms` setting for partitions that have received new messages", so each busy partition gets a new file about once a minute.

The file name template is `ecommerce-cdc/{{topic}}/{{partition}}-{{start_offset}}.jsonl`, where `start_offset` is "the Kafka offset of the first record in the file". With `format.output.type` set to `jsonl`, each line is one message, with the fields in `format.output.fields`: key, value, offset and timestamp.

The sink reads every topic matching `topics.regex`, so a new table's topic is picked up without a change. A Kafka consumer learns of new topics only when it refreshes its **metadata**, every `metadata.max.age.ms`, five minutes by default. Debezium creates `ecommerce.cdc.orders` only when the first order is placed, after the sink has started, so the sink can take up to five minutes to see it. [`s3-sink.json`](../ecommerce/cdc/s3-sink.json) sets `consumer.override.metadata.max.age.ms` to 30 seconds. The `consumer.override.` prefix passes a setting to the connector's own consumer, which the worker allows because its override policy is the default, `All`. [Step 3](../README.md#files) of the README gives the times measured in a test run.

## Simulation model

The shop is a **discrete-event simulation (DES)**, built with [dynamic-des](https://github.com/jaehyeon-kim/dynamic-des). A DES jumps a clock from one event to the next, such as a visitor arriving or an order being packed; here the clock keeps pace with real time. `build` in [`run.py`](../ecommerce/simulation/run.py) defines the model, and [`config.py`](../ecommerce/core/config.py) holds its values:

| Part | dynamic-des feature | Here |
|---|---|---|
| random arrivals | `add_arrival`, `arrival_loop` | visitors, one a second on average, and address moves, one every 20 seconds; the gaps are exponential |
| step durations | `add_service` | page view 4 s, packing 5 s, transit 60 s, time to a return 60 s, patience 120 s, each a lognormal mean, so a few steps take much longer and none is negative |
| limited capacity | `add_resource` | three pickers; an order waits in a queue while all are busy |
| chances | `add_variable` | 0.6 a visitor is registered, 0.3 a visitor buys, 0.1 an order is returned |
| a process of its own | `spawn` | each visit, and each order |
| live changes | the registry and `KafkaIngress` | any value above, while it runs |
| writes | `PostgresEgress` | one per table, upserting on `id` |

An order waits for a picker or for its customer's patience to run out, whichever comes first, and is cancelled in the second case. [Step 4](../README.md#step-4-change-the-simulation-while-it-runs) shows what happens with one picker.

Every value lives in the **registry** under a path, such as `ecommerce.resources.pickers.current_cap`, and a process reads the live value each time it draws one. `KafkaIngress` applies each `{"path_id": ..., "value": ...}` message on `ecommerce-control` to the registry. A lower picker count takes effect as pickers finish their current orders, because dynamic-des takes back only free capacity.

A changed order is published again with the same `id`, so PostgreSQL updates the row and Debezium sends an update. The rules that do not depend on time, such as which status may follow which, are plain functions in [`shop.py`](../ecommerce/simulation/shop.py), so the tests check them without running the model.

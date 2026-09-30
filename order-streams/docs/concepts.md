# Concepts

The ideas the [order-streams](../README.md) applications are built on, each with one example from the code. The behaviour described is that of the versions the build uses: Kafka clients and Kafka Streams 3.9, the Confluent serializers 7.9 and Flink 1.20.

## Topics, partitions and keys

A **topic** is a named stream of records that Kafka keeps on disk, so any number of applications can read it, each at its own pace. A topic is split into **partitions**, and each record in a partition has an **offset**, its position in that partition. A record is appended to one partition. When it has a **key**, the producer picks the partition from a hash of the key, so records with the same key always go to the same partition. Kafka keeps order within a partition only: a consumer reads each partition's records in the order they were written, but there is no order across partitions.

Example: `orders-json` has three partitions, and each order's key is its order id. Every id is new, so the orders spread over the three partitions, and two orders sent a second apart can be read in either order if they land in different partitions.

## Producers

A producer sends records to the broker and waits for an **acknowledgement**. The `acks` setting says what the broker must do first. The default, `acks=all`, waits until every in-sync copy of the partition has the record, which is the strongest guarantee. With the default **idempotence**, a retried send never writes a second copy. A producer also **batches** records for the same partition into one request. `batch.size` (16 KB by default) caps a batch, and `linger.ms` (0 by default) is how long to wait for more records before sending.

Example: the order producers keep the default `acks`, idempotence and batching, and call `.get()` on every send, so each order is acknowledged before the next one is sent, and every batch holds one record. The Flink job's stats sink instead sets `linger.ms` to 100 and `batch.size` to 64 KB, so it sends fewer, larger requests.

## Consumers, groups and offsets

Consumers that share a `group.id` form a **consumer group**. Kafka gives each partition to exactly one member, so a topic with three partitions can be read by up to three members in parallel. If a member stops, its partitions move to the others. A group **commits** the offsets it has processed, and after a restart it carries on from them. With no committed offset, `auto.offset.reset` decides where to start: `latest` by default, or `earliest`.

Example: the consumers turn off automatic commits and call `commitSync()` after processing each batch from `poll()`. If a consumer stops after processing a batch but before committing it, it reads that batch again on restart. So no order is lost, but one can be processed twice. This is called at-least-once processing.

## Serialization: JSON and Avro

Kafka stores bytes, so a producer needs a **serializer** to turn a record into bytes, and a consumer a **deserializer** to turn them back.

- **JSON** is text, and each message repeats its field names. Nothing checks that the producer and the consumer agree on the fields. Here a small Jackson serializer writes `{"order_id": ..., "bid_time": ...}`.
- **Avro** is binary and leaves the field names out, so it needs the **schema** to be read. The schema is kept in a **schema registry**, here Karapace. Each message starts with a magic byte (0) and a 4-byte schema id, followed by the Avro data. A consumer fetches the schema by id.

The schemas of one topic's values are stored under a **subject**, by default the topic name plus `-value`. The Confluent serializer registers a schema on first use, and the registry gives it a version number. Before accepting a new version, the registry checks it against the subject's **compatibility** level. Here it is `BACKWARD`, the default: a consumer using the new schema must be able to read data written with the previous one.

Example: the Avro producer's first send registers `Order.avsc` as version 1 of `orders-avro-value`. The Flink jobs later read that version back from the registry to decode the topic.

## Kafka Streams

Kafka Streams is a library, not a separate service: the stream processing runs inside the application. The processing steps form a **topology**. The application's `application.id` is also its consumer group and the prefix of the topics it creates.

- **Timestamps and stream time.** A timestamp extractor gives each record its timestamp. The work is split into **tasks**, one for each partition of the input. **Stream time** is the largest timestamp a task has seen, and it moves forward only when a record arrives in that task.
- **Tumbling windows** are fixed-size, non-overlapping and aligned to the epoch. With a size of 5 seconds, the windows are `[0, 5s)`, `[5s, 10s)` and so on, so each record belongs to exactly one window.
- **Grace period.** A window accepts out-of-order records until stream time passes its end plus the grace period. After that the window is closed, and a record for it is dropped.
- **Results.** A windowed aggregation updates its result as records arrive, and sends each update downstream. A record cache merges updates to the same key, and is flushed when the application commits (every 30 seconds by default) or when the cache is full. `suppress(untilWindowCloses(...))` would instead send one final result per window.
- **Repartitioning.** Grouping after a key change, such as a `map` to a new key, makes Kafka Streams write the records to an internal topic keyed by the new key, so that all records with one key reach the same task.

Example: `BidTimeTimestampExtractor` uses the bid time. The topology maps each order to the key `supplier`, groups it into 5-second windows with a 5-second grace period, and writes each window's updates to `orders-avro-stats`. It does not use `suppress`, so a window's result can appear more than once, each time with a larger count. Because the aggregation drops late records silently, `LateRecordProcessor` repeats the same check first, and sends late orders to `orders-avro-skipped` instead.

## Flink: watermarks and windows

Flink also uses event time, the time in the record. It tracks progress with **watermarks**. A watermark with time t says that no more records with a timestamp at or before t should arrive. An event-time window emits its result when the watermark passes the window's end.

- **Bounded out-of-orderness.** `forBoundedOutOfOrderness(5 seconds)` emits watermarks at the largest timestamp seen minus 5 seconds (minus 1 millisecond). So a record up to 5 seconds out of order is still on time.
- **Parallel inputs.** An operator with several inputs takes the smallest of their watermarks. An idle Kafka partition would hold the watermark back, so `withIdleness(10 seconds)` marks a partition idle when it has had no records for 10 seconds.
- **Allowed lateness.** A record that arrives after the watermark has passed its window's end, but before it passes the end plus the allowed lateness, is still added, and the window emits an updated result. A record later than that is dropped.
- **Side outputs.** `sideOutputLateData(tag)` sends the dropped records to a second stream instead, which the job reads with `getSideOutput(tag)`.

Example: in the DataStream job, the window for 12:00:00 to 12:00:05 first emits once the job has seen a bid time of 12:00:10. An order for that window still counts until the job has seen a bid time of 12:00:15. After that it goes to the side output and to `orders-avro-kds-skipped`.

## DataStream API and Table API

The **DataStream API** builds a job from operators, such as `map`, `keyBy`, `window` and `aggregate`, and gives control over each one. The **Table API** describes the result as a query over a table, like SQL, and Flink plans the operators. In Flink's table queries, a query grouped by a window computes a single result row for each window.

A table made from a DataStream does not carry the stream's event time by default. The table's schema has to name a time column and a watermark. `SOURCE_WATERMARK()` tells the table to use the watermarks the DataStream already has. So the value in that column must be the timestamp the stream's watermarks were built from.

Example: the Table API job puts the bid time into each row as an `Instant`, in the column `bid_time`. It then assigns timestamps and watermarks from that field (`RowWatermarkStrategy`), and declares `bid_time` as `TIMESTAMP_LTZ(3)` with `SOURCE_WATERMARK()`. The query `Tumble.over(lit(5).seconds()).on(col("bid_time"))` then windows on the same time the watermarks follow. The Table API window has no side output, so the job routes late orders with a DataStream step first (`LateDataRouter`): any order older than the watermark minus 5 seconds goes to `orders-avro-ktl-skipped`.

## Why DELAY_SECONDS makes orders late

The Avro producer sets each bid time to a random moment up to `DELAY_SECONDS` before it sends the order. Kafka Streams closes a window when stream time passes its end plus the 5-second grace period, and an order that arrives after that is late.

Example: with the default of 5 seconds, all bid times are within 5 seconds of each other, so no order is late. With `DELAY_SECONDS=15`, an order sent at 12:00:16 can carry the bid time 12:00:02. Its window, 12:00:00 to 12:00:05, closes at 12:00:10. If an earlier order in the same partition had the bid time 12:00:15, that partition's stream time has passed 12:00:10, and this order goes to the skipped topic.

The Flink jobs accept an order for longer. Their watermark trails the latest bid time by 5 seconds, and the DataStream job then allows 5 more seconds of lateness. So it drops an order only once it has seen a bid time about 10 seconds past the order's window end. The Table API job's router sends an order to the skipped topic when it is more than about 10 seconds behind the latest bid time. With `DELAY_SECONDS=15`, few orders are that far behind, and a run can end with the Flink skipped topics empty. With 30, many orders are.

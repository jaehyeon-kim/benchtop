# Concepts behind the design

The ideas behind [game-leaderboard](../README.md), in the order a score meets them.

## A discrete-event simulation of the game

A **discrete-event simulation** models a system as events at points in time, such as "a player arrives", and moves the clock from one event to the next. Each actor is a **process** that waits for an event, then carries on.

The model is in [`run.py`](../leaderboard/simulation/run.py), built with [dynamic-des](https://github.com/jaehyeon-kim/dynamic-des), and the teams in [`game.py`](../leaderboard/simulation/game.py). It runs in real time (`factor=1.0`): a simulated second takes a real second.

| Part of the game | dynamic-des part | Default |
|---|---|---|
| Players arrive at random | an **arrival**, `player`, with exponential gaps | one every 2 seconds on average |
| A player's session | a **process** per player, started for each arrival | lognormal, 5 minutes on average |
| Time between rounds | a **service**, `round` for people and `robot_round` for robots | 6 seconds for people, 1.5 for robots |
| A phone goes offline | a service, `offline`: the score is held, then sent | about 7 minutes |
| Robots and late scores | **variables**, `robot_share` and `late_share` | 5% of players, 1% of scores |
| Sending the scores | a **Kafka egress**, whose serializer sends only the score | about 27 scores a second |
| Live changes | a **Kafka ingress** on the topic `game-control` | |

An **arrival** draws the time until the next arrival, and a **service** draws how long something takes. An **egress** sends events out of the simulation, and an **ingress** reads changes in.

A session joins a team with room, or forms a new one: a team holds up to 15 players, and one arrival in ten forms a new team anyway. The player plays rounds, each scoring 0 to 20, until the session ends, then leaves the team. An empty team dissolves.

Every parameter is kept in dynamic-des's **registry** under a path, such as `game.variables.robot_share`, and read again at each draw. [`leaderboard.simulation.control`](../leaderboard/simulation/control.py) sends a new value to `game-control`, and the ingress writes it into the registry.

## Event time and processing time

- **Event time** is when an event happened, on the device that produced it. It travels in the event, as `event_time_millis`.
- **Processing time** is the clock of the machine handling the event, when it handles it.

They differ when an event is delayed, as the late scores are. The hot streaks measure "the last 10 seconds" in event time, so a delay cannot move a score into the wrong window. The totals ignore time: a `SUM(score)` adds every score whenever it arrives, late ones included.

## Watermarks and late events

Events arrive out of order, so a processor working in event time cannot know when it has seen everything up to a point. A **watermark** is a marker in the stream that says event time has reached `t`, and no more events at or before `t` are expected. [`00-ddl.sql`](../leaderboard/jobs/00-ddl.sql) declares it on the source table:

```sql
event_time AS TO_TIMESTAMP_LTZ(event_time_millis, 3),
WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
```

The watermark stays 5 seconds behind the latest event time seen, so an event may arrive up to 5 seconds out of order and still be in time. An event that arrives after the watermark has passed its time is **late**, as the scores held for 7 minutes are.

## Continuous queries and dynamic tables

A stream never ends, so Flink treats it as a **dynamic table**, one that changes as rows arrive. A query over it is a **continuous query**: it never finishes, and Flink keeps its result up to date. Each job is one. [`02-top-players.sql`](../leaderboard/jobs/02-top-players.sql) reads as ordinary SQL: it totals each player's scores, ranks the totals and keeps the top 10. The difference is that every new score can change its result.

## Top-N with ROW_NUMBER

A **Top-N** query keeps the N best rows by some order. Flink recognises this pattern:

```sql
SELECT ..., ROW_NUMBER() OVER ([PARTITION BY ...] ORDER BY ...) AS rnk
FROM ...
WHERE rnk <= N
```

`ROW_NUMBER()` numbers the rows in order from 1, separately for each group if there is a `PARTITION BY`. All four jobs end with it and no partition, for one ranking of everyone. Two use it once before that: the hot streaks with `PARTITION BY user_id`, to keep each player's latest row, and the team MVPs with `PARTITION BY team_id`, to find each team's top scorer. Flink keeps the ranking in state and sends on only the ranks that change.

## Changelogs and sinks keyed on the rank

A continuous query's result changes, so Flink sends it on as a **changelog**: insert (`+I`), update before (`-U`, the old row), update after (`+U`, the new row) and delete (`-D`). A total going from 100 to 112 is an update before for 100 and an update after for 112. It can be passed on two ways:

- a **retract** stream sends an update as two messages: take back the old row, add the new one;
- an **upsert** stream sends one message: the new row, which replaces the row with the same unique key.

The JDBC sink accepts inserts, update afters and deletes, so it works as an upsert sink. For PostgreSQL it writes `INSERT ... ON CONFLICT ... DO UPDATE`. Its key is the table's primary key, and here that is the rank:

```sql
CREATE TABLE top_players (
  rnk BIGINT, user_id STRING, team_name STRING, total_score BIGINT,
  PRIMARY KEY (rnk) NOT ENFORCED
) WITH ('connector' = 'jdbc', ...);
```

So each table has 10 rows, one per rank. `NOT ENFORCED` means Flink does not check the key. Flink's documentation recommends keying on the item, such as `user_id`, so a player moving up changes one row rather than every rank that shifts. Here that is at most 10 rows.

## Job settings: state, mini-batch and checkpoints

A job remembers things between events, such as every player's total. This is **state**, kept per key. odctl's Flink keeps it in RocksDB, a key-value store on the TaskManager's disk. Each job file sets:

| Setting | Value | What it does |
|---|---|---|
| `table.exec.state.ttl` | `60 min`; `5 min` for hot streaks | drops a key's state once it has not been updated for at least this long; the default, 0, keeps it for ever |
| `table.exec.mini-batch.enabled`, `.allow-latency`, `.size` | `true`, `1s`, `2000`; `1000` for hot streaks | collects rows and processes them together, so each key's state is read and written once per batch; a batch runs after 1 second or at the size, whichever comes first |
| `execution.checkpointing.interval` | `10s` | saves a consistent copy of the state with the job's position in the topic; checkpoints are off by default |

- **TTL:** a session lasts about 5 minutes and a late score arrives about 7 minutes after it was earned, so an hour never drops a total that can still change.
- **Mini-batch:** the three settings must be set together. At a few dozen scores a second a batch never reaches its size, so the leaderboards change about once a second.
- **Checkpoints:** after a failure, Flink restores the last checkpoint and reads on from its position. The JDBC sink writes its buffered rows every second (`sink.buffer-flush.interval`), at 100 rows (`sink.buffer-flush.max-rows`), and at each checkpoint. Rows processed again after a failure are written twice, which the upsert on `rnk` makes harmless.

## OVER windows for the hot streaks

Unlike `GROUP BY`, which turns many rows into one, an **OVER window** keeps every row and adds a value computed over nearby rows:

```sql
AVG(CAST(score AS DOUBLE)) OVER (
  PARTITION BY user_id ORDER BY event_time
  RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW
) AS short_term_avg
```

This averages the player's scores over the 10 seconds up to each score. `RANGE` measures the window in values of the `ORDER BY` column, here seconds, rather than in rows. A second window does the same over 60 seconds, and [`03-hot-streaks.sql`](../leaderboard/jobs/03-hot-streaks.sql) keeps each player's latest row, divides the two averages and ranks the result. Because it is ordered by event time:

- **Results lag by about 5 seconds.** Flink holds each row until the watermark passes its time, then computes it.
- **Late scores are left out.** A score whose time is not after the player's last computed row is dropped, and counted in the job's `numLateRecordsDropped` metric.

## Joining two aggregates

[`04-team-mvps.sql`](../leaderboard/jobs/04-team-mvps.sql) computes each player's total and each team's total as two aggregates, and joins them on `team_id` to get the share. This is a **regular join**: Flink keeps both sides in state, and a change on either side updates every joined row that uses it.

Each score changes both totals, and the changes arrive one after the other. In between, a player's new total can meet the team's old one, and the share goes above 1. One run briefly held a player total of 420 against a team total of 416. A later change on the team side corrects it.

## Why small teams lead the team MVPs

A team's top scorer has a share of at least 1 divided by the team's size: 1.0 for a team of one, 0.5 for two. A team that has had 15 or more players gives its top scorer about 0.1 to 0.3. One arrival in ten forms a new team of one, so the newest teams often lead. In one run, after 20 minutes, all ten shares were 1.0, and recomputing them from the topic matched the table.

## Four jobs and one init file

One job with four `INSERT`s would share one set of settings. As four jobs, each has its own settings and its own `pipeline.name`, and can be cancelled alone.

All four use the tables in [`00-ddl.sql`](../leaderboard/jobs/00-ddl.sql). Flink's default catalog keeps table definitions in memory for one SQL client session only, so [`leaderboard.jobs.submit`](../leaderboard/jobs/submit.py) runs each job in its own session with [`00-ddl.sql`](../leaderboard/jobs/00-ddl.sql) as the **init file**:

```bash
./bin/sql-client.sh -i /tmp/game-sql/00-ddl.sql -f /tmp/game-sql/02-top-players.sql
```

`-i` runs a setup file, which may hold `CREATE TABLE` and `SET` but no queries. `-f` runs the job file, whose `INSERT` starts the job.

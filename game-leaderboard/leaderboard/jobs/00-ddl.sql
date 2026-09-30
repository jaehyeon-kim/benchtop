-- The tables every leaderboard job reads and writes: the Kafka source of score events
-- and the four PostgreSQL sinks. odctl's Flink keeps table definitions only for a SQL
-- client session, so each job loads this file first with `sql-client.sh -i`.
-- The Kafka, Avro and JDBC connectors are already in odctl's Flink image.

-- The score events. Event time comes from the event itself, and the watermark lets
-- scores arrive up to 5 seconds out of order. Late events, minutes behind, still count
-- towards the totals, but the hot streaks' time windows leave them out.
CREATE TABLE scores (
  user_id           STRING,
  team_id           STRING,
  team_name         STRING,
  score             INT,
  event_time_millis BIGINT,
  event_type        STRING,
  event_time AS TO_TIMESTAMP_LTZ(event_time_millis, 3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'game-scores',
  'properties.bootstrap.servers' = 'broker-1:19092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.url' = 'http://karapace:8081'
);

-- The four sinks. The primary key makes each an upsert: Flink replaces the row at a
-- rank when it changes.
CREATE TABLE top_teams (
  rnk BIGINT, team_id STRING, team_name STRING, total_score BIGINT,
  PRIMARY KEY (rnk) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/odctl',
  'table-name' = 'game.top_teams',
  'username' = 'user',
  'password' = 'password',
  'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE top_players (
  rnk BIGINT, user_id STRING, team_name STRING, total_score BIGINT,
  PRIMARY KEY (rnk) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/odctl',
  'table-name' = 'game.top_players',
  'username' = 'user',
  'password' = 'password',
  'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE hot_streaks (
  rnk BIGINT, user_id STRING, short_term_avg DOUBLE, long_term_avg DOUBLE, hotness DOUBLE,
  PRIMARY KEY (rnk) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/odctl',
  'table-name' = 'game.hot_streaks',
  'username' = 'user',
  'password' = 'password',
  'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE team_mvps (
  rnk BIGINT, user_id STRING, team_name STRING, player_total BIGINT, team_total BIGINT,
  contrib_ratio DOUBLE,
  PRIMARY KEY (rnk) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/odctl',
  'table-name' = 'game.team_mvps',
  'username' = 'user',
  'password' = 'password',
  'sink.buffer-flush.interval' = '1s'
);

-- One leaderboard job. Submit it with the shared table definitions:
--   ./bin/sql-client.sh -i /tmp/game-sql/00-ddl.sql -f /tmp/game-sql/03-hot-streaks.sql

SET 'pipeline.name' = 'game-hot-streaks';
SET 'parallelism.default' = '1';
-- Checkpoint every 10 seconds. The JDBC sink flushes its buffered rows at each
-- checkpoint, and after a restart the job resumes from the last checkpoint instead of
-- rebuilding its state from the start of the topic. odctl sets the checkpoint folder
-- and RocksDB state, but no interval, so the job sets it.
SET 'execution.checkpointing.interval' = '10s';
-- The windows look back 60 seconds, so a player's rows are dropped 5 minutes after
-- their last score.
SET 'table.exec.state.ttl' = '5 min';
-- Collect updates for up to a second before writing, so a busy leaderboard is written
-- once a second rather than once per event.
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '1s';
SET 'table.exec.mini-batch.size' = '1000';

-- Hot streaks: each player's average score over the last 10 seconds, divided by their
-- average over the last 60 seconds, as of their latest score. The 10 highest ratios are
-- the players scoring well above their usual rate right now.
INSERT INTO hot_streaks
SELECT rnk, user_id, short_term_avg, long_term_avg, hotness
FROM (
  SELECT *, ROW_NUMBER() OVER (ORDER BY hotness DESC, user_id) AS rnk
  FROM (
    -- Keep each player's latest row only.
    SELECT user_id, short_term_avg, long_term_avg, short_term_avg / long_term_avg AS hotness
    FROM (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_time DESC) AS latest
      FROM (
        SELECT
          user_id, event_time, short_term_avg,
          AVG(CAST(score AS DOUBLE)) OVER (
            PARTITION BY user_id ORDER BY event_time
            RANGE BETWEEN INTERVAL '60' SECOND PRECEDING AND CURRENT ROW
          ) AS long_term_avg
        FROM (
          SELECT
            user_id, event_time, score,
            AVG(CAST(score AS DOUBLE)) OVER (
              PARTITION BY user_id ORDER BY event_time
              RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW
            ) AS short_term_avg
          FROM scores
        )
      )
    )
    WHERE latest = 1 AND long_term_avg > 0
  )
)
WHERE rnk <= 10;

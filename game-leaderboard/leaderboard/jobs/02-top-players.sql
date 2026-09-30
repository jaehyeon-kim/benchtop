-- One leaderboard job. Submit it with the shared table definitions:
--   ./bin/sql-client.sh -i /tmp/game-sql/00-ddl.sql -f /tmp/game-sql/02-top-players.sql

SET 'pipeline.name' = 'game-top-players';
SET 'parallelism.default' = '1';
-- Checkpoint every 10 seconds. The JDBC sink flushes its buffered rows at each
-- checkpoint, and after a restart the job resumes from the last checkpoint instead of
-- rebuilding its state from the start of the topic. odctl sets the checkpoint folder
-- and RocksDB state, but no interval, so the job sets it.
SET 'execution.checkpointing.interval' = '10s';
-- Keep a running total for an hour after its last score. A session lasts about 5
-- minutes and a late score arrives about 7 minutes after it was earned, so no total is
-- dropped while it can still change.
SET 'table.exec.state.ttl' = '60 min';
-- Collect updates for up to a second before writing, so a busy leaderboard is written
-- once a second rather than once per event.
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '1s';
SET 'table.exec.mini-batch.size' = '2000';

-- Top players: the 10 players with the highest total score.
INSERT INTO top_players
SELECT rnk, user_id, team_name, total_score
FROM (
  SELECT *, ROW_NUMBER() OVER (ORDER BY total_score DESC, user_id) AS rnk
  FROM (
    SELECT user_id, MAX(team_name) AS team_name, CAST(SUM(score) AS BIGINT) AS total_score
    FROM scores
    GROUP BY user_id
  )
)
WHERE rnk <= 10;

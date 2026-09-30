-- One leaderboard job. Submit it with the shared table definitions:
--   ./bin/sql-client.sh -i /tmp/game-sql/00-ddl.sql -f /tmp/game-sql/04-team-mvps.sql

SET 'pipeline.name' = 'game-team-mvps';
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

-- Team MVPs: each team's top scorer and their share of the team's total, then the 10
-- players with the largest shares.
INSERT INTO team_mvps
SELECT rnk, user_id, team_name, player_total, team_total, contrib_ratio
FROM (
  SELECT *, ROW_NUMBER() OVER (ORDER BY contrib_ratio DESC, user_id) AS rnk
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY contrib_ratio DESC, user_id) AS team_rnk
    FROM (
      SELECT
        p.user_id, t.team_id, t.team_name, p.player_total, t.team_total,
        CAST(p.player_total AS DOUBLE) / t.team_total AS contrib_ratio
      FROM (
        SELECT user_id, team_id, CAST(SUM(score) AS BIGINT) AS player_total
        FROM scores
        GROUP BY user_id, team_id
      ) AS p
      JOIN (
        SELECT team_id, MAX(team_name) AS team_name, CAST(SUM(score) AS BIGINT) AS team_total
        FROM scores
        GROUP BY team_id
      ) AS t ON p.team_id = t.team_id
      WHERE t.team_total > 0
    )
  )
  WHERE team_rnk = 1
)
WHERE rnk <= 10;

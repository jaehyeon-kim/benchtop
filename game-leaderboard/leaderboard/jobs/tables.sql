-- The leaderboard tables Flink writes to. Each holds the top 10, one row per rank, and
-- Flink updates a row whenever the player or team at that rank changes.
CREATE SCHEMA IF NOT EXISTS game;

CREATE TABLE IF NOT EXISTS game.top_teams (
    rnk         BIGINT PRIMARY KEY,
    team_id     TEXT,
    team_name   TEXT,
    total_score BIGINT
);

CREATE TABLE IF NOT EXISTS game.top_players (
    rnk         BIGINT PRIMARY KEY,
    user_id     TEXT,
    team_name   TEXT,
    total_score BIGINT
);

CREATE TABLE IF NOT EXISTS game.hot_streaks (
    rnk            BIGINT PRIMARY KEY,
    user_id        TEXT,
    short_term_avg DOUBLE PRECISION,
    long_term_avg  DOUBLE PRECISION,
    hotness        DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS game.team_mvps (
    rnk           BIGINT PRIMARY KEY,
    user_id       TEXT,
    team_name     TEXT,
    player_total  BIGINT,
    team_total    BIGINT,
    contrib_ratio DOUBLE PRECISION
);

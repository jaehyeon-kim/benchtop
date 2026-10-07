# Data

The [game-leaderboard](../README.md) events and tables.

## Score events

One event per round goes to the topic `game-scores` in Avro, under the Karapace subject `game-scores-value`. The model, with bounds checked before sending, is in [`models.py`](../leaderboard/core/models.py).

| Field | Type | Description |
|---|---|---|
| `user_id` | string | The player: `USR-` for a person, `RBT-` for a robot, then a number |
| `team_id` | string | The team's 10-digit id, unique across the run |
| `team_name` | string | A colour and an animal, such as `Amber-Koala`, unique among the teams playing at one time |
| `score` | int | The points the round earned, from 0 to 20 |
| `event_time_millis` | long | When the round was played, in Unix milliseconds |
| `event_type` | string | `normal`, or `late` for a score held while the phone was offline |

A new team can reuse a dissolved team's name, so the jobs group by `team_id`, and two rows can show the same name.

In Flink, [`00-ddl.sql`](../leaderboard/jobs/00-ddl.sql) reads the topic as the table `scores`, with `event_time_millis` as a timestamp column, `event_time`, and a [watermark](concepts.md#watermarks-and-late-events) 5 seconds behind.

## Leaderboard tables

The four tables are in the PostgreSQL schema `game`, defined in [`tables.sql`](../leaderboard/jobs/tables.sql), with one row per rank, keyed on `rnk`. Each has `rnk` (bigint, 1 to 10) as its first column.

`top_teams`: the 10 teams with the highest total score, written by [`01-top-teams.sql`](../leaderboard/jobs/01-top-teams.sql).

| Column | Type | Description |
|---|---|---|
| `team_id` | text | The team's id |
| `team_name` | text | The team's name |
| `total_score` | bigint | Every score the team's players have earned, including those who left |

`top_players`: the 10 players with the highest total score, written by [`02-top-players.sql`](../leaderboard/jobs/02-top-players.sql).

| Column | Type | Description |
|---|---|---|
| `user_id` | text | The player |
| `team_name` | text | The player's team |
| `total_score` | bigint | Every score the player has earned |

`hot_streaks`: the 10 players scoring furthest above their usual rate, as of each player's latest score, written by [`03-hot-streaks.sql`](../leaderboard/jobs/03-hot-streaks.sql).

| Column | Type | Description |
|---|---|---|
| `user_id` | text | The player |
| `short_term_avg` | double precision | The player's average score over the 10 seconds up to their latest score |
| `long_term_avg` | double precision | The player's average score over the 60 seconds up to their latest score |
| `hotness` | double precision | `short_term_avg` divided by `long_term_avg`; above 1 is above the player's usual rate |

`team_mvps`: each team's top scorer and their share of the team's total, then the 10 players with the largest shares, written by [`04-team-mvps.sql`](../leaderboard/jobs/04-team-mvps.sql).

| Column | Type | Description |
|---|---|---|
| `user_id` | text | The team's top scorer |
| `team_name` | text | The team |
| `player_total` | bigint | Every score the player has earned for the team |
| `team_total` | bigint | Every score the team's players have earned |
| `contrib_ratio` | double precision | `player_total` divided by `team_total` |

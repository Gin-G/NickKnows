# Prompt: NFL-API additions to complete the NickKnows migration

> Copy everything below this line into a session working on the NFL-API repo.

---

Goal: add four endpoint groups to this NFL-API (FastAPI, DB-first with
nflreadpy fallback) so the NickKnows Flask app can retire its last
Celery/CSV pipelines. NickKnows has already migrated teams, schedules,
rosters, and projections to this API; the routes below are the remaining
gaps. The consumer-side semantics come from NickKnows'
`app/nickknows/celery_setup/` tasks — match them exactly so the pages
render unchanged when NickKnows swaps its CSV reads for these endpoints.

Follow the API's existing conventions throughout:
- Response envelope: `{"status": "success"|"no_data", "data": [...]}` plus
  a count field (`total_records`, `total_players`, or `total_teams`).
- Collection routes take a TRAILING SLASH (`/stats/leaders/` etc.) like
  the existing `/schedules/` and `/projections/`.
- `season` is the NFL season year (e.g. 2024 = the 2024–25 season).
- DB-first; fall back to computing from nflreadpy when the DB hasn't been
  loaded for that season, and return `"no_data"` only when neither source
  can serve the query.

## 1. Season stat leaders — `GET /stats/leaders/?season=&stat=&limit=`

Replaces NickKnows' six `*_top10_data.csv` files (see
`stat_aggregation_tasks.py`).

- `stat` is one of: `passing_yards`, `passing_tds`, `rushing_yards`,
  `rushing_tds`, `receiving_yards`, `receiving_tds`. `limit` defaults to 10.
- Semantics (must match the Celery tasks): filter weekly player stats to
  `season_type == 'REG'` and non-null values of the requested stat, group
  by player display name, SUM the stat, sort descending, take `limit`.
- Each data row: `{"player_name": <player_display_name>, "player_id":
  <gsis id>, "team": <recent team>, "position": ..., "value": <summed
  stat>}`. NickKnows renders `player_name` + `value` as a two-column
  table titled per stat; the extra fields are for future linking.

## 2. Fantasy points allowed (FPA) — two endpoints

Replaces `{year}_FPA.csv` and the per-team `{team}_data.csv` FPA math
(see `team_analysis_tasks.py::process_team_fpa` and `save_fpa_summary`).

### `GET /fpa/?season=`
One row per defense (32 rows):
`{"team": "BUF", "team_name": "Buffalo Bills", "qb": ..., "rb": ...,
"wr": ..., "te": ...}`.

Definition (must match the Celery math): for each defense, collect every
opposing player's `fantasy_points_ppr` from weekly stats for the games
that defense played; group by (week, position); sum PPR points per
position per week; then average those weekly sums across weeks played.
Positions covered: QB, RB, WR, TE. A position with no data is 0, not null.

### `GET /teams/{abbr}/fpa?season=&position=`
The detailed breakdown behind the averages — one row per opposing
player-week against this defense:
`{"week": ..., "opponent": ..., "player_id": ..., "player_name": ...,
"position": ..., "fantasy_points_ppr": ..., "fantasy_points": ...}` plus
the main weekly stat columns (passing/rushing/receiving yards and TDs).
Optional `position` filters to one of QB/RB/WR/TE. NickKnows'
`/NFL/Team/FPA/<team>` page shows these rows per position and computes
position means from them, so the rows must be complete (every opposing
skill player who logged stats that week, matched via the opponent's
roster the way `update_weekly_team_data` does — by player id primarily,
name as fallback).

## 3. Opportunities — two endpoints

Replaces `{year}_opportunity_data.csv` and `{year}_opportunity_trends.csv`
(see `opportunity_tasks.py`). These are computed from play-by-play.

### `GET /opportunities/?season=&week=&team=`
One row per player-week, computed from REG-season PBP exactly as
`process_week_opportunities` does:

- Pass plays: credit the `receiver_player_id` with `targets` +1 and
  `touches` +1; accumulate `air_yards`; `red_zone_targets` when
  `yardline_100 <= 20`; `end_zone_targets` and `goal_line_touches` when
  `yardline_100 <= 10`; `third_down_targets` when `down == 3`;
  `deep_targets` when `air_yards >= 20`; `short_targets` when
  `air_yards < 10`.
- Run plays: credit the `rusher_player_id` with `carries` +1 and
  `touches` +1; `red_zone_carries` when `yardline_100 <= 20`;
  `goal_line_carries` and `goal_line_touches` when `yardline_100 <= 5`.
- Shares: `target_share` = player targets / team total targets that week
  × 100; `carry_share` likewise. (Percentages 0–100, not fractions.)
- Enrich each row with `player_name`, `position`, `team` from weekly
  rosters (roster team wins over PBP posteam when both exist; position
  defaults to `"Unknown"`).

Row shape: `{"player_id", "player_name", "position", "team", "season",
"week", "targets", "red_zone_targets", "end_zone_targets", "carries",
"red_zone_carries", "goal_line_carries", "air_yards", "touches",
"goal_line_touches", "third_down_targets", "deep_targets",
"short_targets", "target_share", "carry_share"}`.

### `GET /opportunities/trends/?season=&team=`
One row per player with >= 2 weeks played, matching
`calculate_opportunity_trends`: base fields `{"player_id", "player_name",
"position", "team", "weeks_played", "latest_week"}` and, for each metric
in `[targets, carries, touches, target_share, carry_share,
red_zone_targets, red_zone_carries, goal_line_touches, deep_targets,
short_targets]`, four derived fields:

- `{metric}_avg` — mean across weeks played
- `{metric}_latest` — most recent week's value
- `{metric}_max` — max across weeks
- `{metric}_trend` — percent change of recent vs early: with >= 3 weeks,
  mean of last 2 weeks vs mean of all earlier weeks; with exactly 2,
  last vs first; `((recent - early) / max(early, 0.1)) * 100`, 0 when
  early <= 0.
- `{metric}_consistency` — coefficient of variation × 100 (std/mean), 0
  when mean is 0.

NickKnows' leaderboard/trending views key on these exact column names
(`targets_avg`, `touches_trend`, `weeks_played`, ...), so naming is load-
bearing.

## 4. Team game results — `GET /teams/{abbr}/results?season=`

Replaces the per-team `{year}_{team}_schedule.csv` (see
`update_team_schedule`). This is just the season schedule filtered to
games involving the team, completed games only (non-null scores), sorted
by week — same row shape as `/schedules/` (`game_id`, `week`,
`home_team`, `away_team`, `home_score`, `away_score`, `result`, `total`,
...). Include an `is_home` boolean per row; NickKnows uses it with
`result` to compute the team's W-L record.

## Not needed

- Player-page lookup by display name: NickKnows will resolve name →
  `player_id` via `/players/rosters` and then use the existing
  `/players/{player_id}` — no new endpoint required.
- Coaches and depth charts: endpoints already exist; NickKnows is adding
  consumers for them.

## Acceptance

For season 2024, each endpoint's output must match the corresponding CSV
NickKnows' Celery pipeline produces (same rows, same values within float
tolerance). The CSVs live on the NickKnows data PVC
(`app/nickknows/nfl/data/`); `2024_FPA.csv` and the six
`2024_*_top10_data.csv` files are checked into the NickKnows repo and can
be used directly as fixtures. Keep responses under the existing
envelope/no_data conventions so NickKnows' `nfl_api_client` needs no
changes.

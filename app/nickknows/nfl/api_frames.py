"""
NFL-API data, shaped like the CSVs the Celery worker used to write.

The NFL pages were built on files the worker produced — weekly stats, team
schedules, snap counts, opportunity tables, top-10 leaderboards — and refreshed
only when a page found a file missing or more than a week old. Every one of
those tables is now served by the NFL-API, so each function here fetches the
endpoint and returns a DataFrame with the columns and scale the old file had.
The views keep their shaping code unchanged; only the source moved.

Each returns None when the API fails or has nothing for the query, and the
caller says so on the page rather than falling back to a file nothing refreshes.
"""
import logging

import pandas as pd

from . import nfl_api_client

logger = logging.getLogger(__name__)

# In-season tables move daily at most; a page view should not cost a round trip.
TTL = 900


def _frame(path, **params):
    try:
        payload = nfl_api_client.get_cached(path, ttl=TTL, **params)
    except nfl_api_client.NflApiError as e:
        logger.warning("NFL-API %s failed: %s", path, e)
        return None
    if nfl_api_client.is_no_data(payload):
        return None
    data = payload.get('data') if isinstance(payload, dict) else payload
    if not data:
        return None
    return pd.DataFrame(data)


def weekly_stats(season):
    """Weekly player stats, regular season and post, nfl_data_py column names.

    The stats endpoint carries no headshot, which the player page leads with;
    the season's rosters do, keyed by the same gsis id.
    """
    df = _frame('/players/stats', season=season)
    if df is None or 'headshot_url' in df:
        return df
    ros = _frame('/players/rosters', season=season)
    if ros is not None and 'headshot_url' in ros:
        heads = (ros.dropna(subset=['headshot_url'])
                 .drop_duplicates('player_id')[['player_id', 'headshot_url']])
        df = df.merge(heads, on='player_id', how='left')
    if 'headshot_url' not in df:
        df['headshot_url'] = None
    df['headshot_url'] = df['headshot_url'].fillna('')
    return df


def team_results(team, season):
    """The team's games, with ``is_home``. Scores are null for games not played."""
    return _frame(f'/teams/{team}/results', season=season)


def team_schedule(team, season, site=''):
    """Completed games, game_id rendered as a link to the play-by-play page.

    Same transformation the worker applied before writing the file.
    """
    df = team_results(team, season)
    if df is None:
        return None
    df = df.dropna(subset=['away_score']).sort_values('week')
    df['game_id'] = ('<a href="' + site + '/NFL/PbP/' + df['game_id'] + '">'
                     + df['away_team'] + ' vs. ' + df['home_team'] + '</a>')
    return df


def points_allowed(team, season):
    """Opponent players' weekly lines in games against ``team``.

    The worker built this by joining the schedule to opponent rosters and
    weekly stats; the API serves it directly. ``player_display_name`` is the
    column the FPA charts and tables were keyed on.
    """
    df = _frame(f'/teams/{team}/fpa', season=season)
    if df is None:
        return None
    return df.rename(columns={'player_name': 'player_display_name'})


def fpa_summary(season):
    """Average fantasy points allowed per position, one row per team."""
    df = _frame('/fpa/', season=season)
    if df is None:
        return None
    return df.rename(columns={'team': 'Team Name', 'qb': 'QB', 'rb': 'RB',
                              'wr': 'WR', 'te': 'TE'})[['Team Name', 'QB', 'RB', 'WR', 'TE']]


def snap_counts(team, season):
    """Snap counts for one team.

    The API reports percentages as whole numbers (6.0) where nflverse's file
    had fractions (0.06), and has no opponent column; both are put back.
    """
    df = _frame(f'/teams/{team}/snap-counts', season=season)
    if df is None:
        return None
    df = df.rename(columns={'player_name': 'player'})
    for col in ('offense_pct', 'defense_pct', 'st_pct'):
        if col in df:
            df[col] = pd.to_numeric(df[col], errors='coerce') / 100.0
    games = team_results(team, season)
    if games is not None and len(games):
        opp = {int(r.week): (r.away_team if r.home_team == team else r.home_team)
               for r in games.itertuples()}
        df['opponent'] = df['week'].map(lambda w: opp.get(int(w)))
    else:
        df['opponent'] = None
    return df


def opportunities(season):
    """Weekly opportunity rows. Same columns and scale as the worker's file."""
    df = _frame('/opportunities/', season=season)
    if df is None:
        return None
    if 'player_display_name' not in df:
        df['player_display_name'] = df['player_name']
    return df


def opportunity_trends(season):
    return _frame('/opportunities/trends/', season=season)


LEADERS = {
    'pass_agg': ('passing_yards', 'Total Passing Yards'),
    'pass_td_agg': ('passing_tds', "Total Passing TD's"),
    'rush_yds_agg': ('rushing_yards', 'Total Rushing Yards'),
    'rush_td_agg': ('rushing_tds', "Total Rushing TD's"),
    'rec_yds_agg': ('receiving_yards', 'Total Receiving Yards'),
    'rec_td_agg': ('receiving_tds', "Total Receiving TD's"),
}


def leaders(season, n=10):
    """The home page's six top-10 tables, from regular-season weekly stats.

    The worker's calculation exactly: filter to REG, sum by display name, take
    the top ten, rename for display.
    """
    df = weekly_stats(season)
    if df is None:
        return None
    if 'season_type' in df:
        df = df[df['season_type'] == 'REG']
    name = 'player_display_name' if 'player_display_name' in df else 'player_name'
    out = {}
    for key, (col, label) in LEADERS.items():
        t = (df[df[col].notna()].groupby(name)[col].sum().reset_index()
             .sort_values(col, ascending=False).head(n))
        out[key] = t.rename(columns={name: 'Player Name', col: label})
    return out

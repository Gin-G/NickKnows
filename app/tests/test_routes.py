"""
Route endpoint tests for NickKnows Flask app.
Tests that all registered routes return non-500 responses.
With no NFL-API reachable, the NFL pages must degrade to a message, not a 500.
"""
import pytest
from unittest.mock import patch, MagicMock
import sys
import os

# Ensure the app package is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

@pytest.fixture(scope='module')
def client():
    from nickknows import app as flask_app
    flask_app.config['TESTING'] = True
    flask_app.config['SECRET_KEY'] = 'test-secret-key'

    with flask_app.test_client() as c:
        yield c


# ---------------------------------------------------------------------------
# Main routes
# ---------------------------------------------------------------------------

def test_home(client):
    r = client.get('/')
    assert r.status_code == 200


def test_nickco7(client):
    r = client.get('/nickco7')
    assert r.status_code == 200


def test_arcade(client):
    r = client.get('/arcade')
    assert r.status_code == 200


def test_lemans(client):
    r = client.get('/lemans')
    assert r.status_code == 200


def test_guitar(client):
    r = client.get('/guitar')
    assert r.status_code == 200


def test_skiing(client):
    r = client.get('/skiing')
    assert r.status_code == 200


def test_values(client):
    r = client.get('/values')
    assert r.status_code == 200


def test_privacy(client):
    r = client.get('/privacy')
    assert r.status_code == 200


def test_brown_banana(client):
    r = client.get('/brown-banana')
    assert r.status_code == 200


def test_fishing(client):
    r = client.get('/fishing')
    assert r.status_code == 200


def test_job_parse_get(client):
    r = client.get('/job_parse')
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# NFL routes
# ---------------------------------------------------------------------------

def test_nfl_home(client):
    r = client.get('/NFL')
    assert r.status_code in (200, 302)


def test_nfl_set_year_valid(client):
    r = client.get('/NFL/set_year/2024')
    assert r.status_code == 302


def test_nfl_set_year_invalid(client):
    r = client.get('/NFL/set_year/1900')
    assert r.status_code == 302


def test_nfl_schedule(client):
    r = client.get('/NFL/schedule/')
    assert r.status_code in (200, 302)


def test_nfl_roster(client):
    r = client.get('/NFL/Roster/BUF/Buffalo Bills')
    assert r.status_code in (200, 302)


def test_nfl_fpa(client):
    r = client.get('/NFL/FPA')
    assert r.status_code in (200, 302)


def test_nfl_team_schedule(client):
    r = client.get('/NFL/Team/Schedule/BUF/Buffalo Bills')
    assert r.status_code in (200, 302)


def test_nfl_team_results(client):
    r = client.get('/NFL/Team/Results/BUF/Buffalo Bills')
    assert r.status_code in (200, 302)


def test_nfl_team_fpa(client):
    r = client.get('/NFL/Team/FPA/BUF/Buffalo Bills')
    assert r.status_code in (200, 302)


def test_nfl_team_page(client):
    r = client.get('/NFL/Team/BUF')
    assert r.status_code in (200, 302)


def test_nfl_snap_counts_home(client):
    r = client.get('/NFL/SnapCounts')
    assert r.status_code in (200, 302)


def test_nfl_snap_counts_team(client):
    r = client.get('/NFL/SnapCounts/BUF/Buffalo Bills')
    assert r.status_code in (200, 302)


def test_nfl_opportunities_home(client):
    r = client.get('/NFL/Opportunities')
    assert r.status_code in (200, 302)


def test_nfl_team_opportunities(client):
    r = client.get('/NFL/Opportunities/BUF')
    assert r.status_code in (200, 302)


def test_nfl_opportunity_charts_degrade_without_the_api(client):
    r = client.get('/NFL/Opportunities/Trends')
    assert r.status_code == 200
    assert b'not available' in r.data


_WEEKLY = {
    'status': 'success', 'season': 2026, 'weeks': [1, 2, 3, 4, 5, 6],
    'metrics': ['opportunities', 'targets', 'carries'],
    'trend_rule': {'recent_games': 3, 'min_games': 5,
                   'min_change': {'opportunities': 2.0, 'targets': 1.5, 'carries': 1.5},
                   'note': ''},
    'data': [{
        'player_id': '00-1', 'player_name': "Ja'Marr <b>Chase</b>", 'position': 'WR',
        'team': 'CIN', 'games': 5, 'latest_week': 6,
        'metrics': {m: {'values': [5.0, 6.0, None, 5.0, 10.0, 11.0], 'games': 5,
                        'per_game': 7.4, 'recent': 8.67, 'earlier': 5.5,
                        'change': 3.17, 'noise': 2.0, 'direction': 'up'}
                    for m in ('opportunities', 'targets', 'carries')},
    }],
}


def test_nfl_opportunity_charts_render_the_payload(client):
    with patch('nickknows.nfl.views.api_frames.opportunity_weekly',
               return_value=_WEEKLY):
        r = client.get('/NFL/Opportunities/Trends?metric=targets&direction=up&team=cin')
    assert r.status_code == 200
    body = r.data.decode()
    assert 'id="opp-data"' in body
    # The page opens on what the link asked for.
    assert '"metric": "targets"' in body and '"team": "CIN"' in body
    # A player name is data: it must reach the page escaped, never as markup.
    assert '<b>Chase</b>' not in body


def _weekly_with(*players):
    payload = dict(_WEEKLY)
    payload['data'] = list(players)
    return payload


def _player(name, team, position, direction, change, metric='opportunities'):
    metrics = {m: {'values': [5.0] * 6, 'games': 6, 'per_game': 5.0, 'recent': 5.0,
                   'earlier': 5.0, 'change': 0.0, 'noise': 1.0, 'direction': 'steady'}
               for m in ('opportunities', 'targets', 'carries', 'pass_attempts')}
    metrics[metric] = dict(metrics[metric], direction=direction, change=change,
                           earlier=10.0, recent=10.0 + change)
    return {'player_id': name, 'player_name': name, 'position': position, 'team': team,
            'games': 6, 'latest_week': 6, 'metrics': metrics}


def test_movers_follow_the_api_call_not_a_percent():
    from nickknows.nfl.views import _weekly_movers
    payload = _weekly_with(
        _player('Big Mover', 'CIN', 'RB', 'up', 6.0),
        _player('Small Mover', 'CIN', 'WR', 'up', 2.5),
        # A huge percent change the API judged to be noise stays off the list.
        _player('Noisy Backup', 'CIN', 'RB', 'steady', 0.0),
        _player('Faller', 'BUF', 'RB', 'down', -7.0),
    )
    up = _weekly_movers(payload, 'opportunities', 'up')
    assert [r['player_name'] for r in up] == ['Big Mover', 'Small Mover']
    assert [r['player_name'] for r in _weekly_movers(payload, 'opportunities', 'down')] == ['Faller']
    assert _weekly_movers(payload, 'opportunities', 'up', team='BUF') == []
    # Quarterbacks are charted, not listed: their attempts swamp a shared table.
    qb = _weekly_with(_player('Big Arm', 'CIN', 'QB', 'up', 20.0))
    assert _weekly_movers(qb, 'opportunities', 'up') == []


def test_the_opportunities_home_lists_the_movers(client):
    payload = _weekly_with(_player('Big Mover', 'CIN', 'RB', 'up', 6.0))
    with patch('nickknows.nfl.views.api_frames.opportunity_weekly', return_value=payload), \
         patch('nickknows.nfl.views.api_frames.opportunity_trends', return_value=None):
        r = client.get('/NFL/Opportunities')
    assert r.status_code == 200
    # The leaderboards' endpoint is down here; the movers come from another.
    assert b'Big Mover' in r.data


_STAT_COLUMNS = [
    'passing_yards', 'interceptions', 'sacks', 'sack_yards', 'passing_air_yards',
    'passing_yards_after_catch', 'passing_first_downs', 'rushing_yards', 'sack_fumbles_lost',
    'rushing_fumbles', 'rushing_fumbles_lost', 'rushing_first_downs', 'receiving_yards',
    'receiving_fumbles', 'receiving_fumbles_lost', 'receiving_air_yards',
    'receiving_yards_after_catch', 'receiving_first_downs', 'special_teams_tds',
    'passing_epa', 'pacr', 'rushing_epa', 'receiving_epa', 'fantasy_points',
    'fantasy_points_ppr', 'racr', 'wopr']


def _stat_row(**fields):
    """One weekly-stats row with every column the player page formats."""
    import pandas as pd
    row = {c: 0.0 for c in _STAT_COLUMNS}
    row.update(fields)
    return pd.DataFrame([row])


def test_the_player_page_draws_his_chart(client):
    weekly = _stat_row(
        player_display_name="Ja'Marr Chase", player_name='J.Chase', player_id='00-1',
        headshot_url='x.png', position='WR', position_group='WR', season=2026,
        season_type='REG', week=1, recent_team='CIN', target_share=0.3,
        air_yards_share=0.4, dakota=None)
    payload = _weekly_with(_player("Ja'Marr Chase", 'CIN', 'WR', 'up', 3.0, 'targets'))
    with patch('nickknows.nfl.views.api_frames.weekly_stats', return_value=weekly), \
         patch('nickknows.nfl.views.api_frames.opportunity_weekly', return_value=payload):
        r = client.get("/NFL/Player/Ja'Marr Chase")
    assert r.status_code == 200
    body = r.data.decode()
    assert 'data-opp-charts="player"' in body
    assert 'opportunity-charts.js' in body


def test_a_player_with_no_series_gets_no_chart_section(client):
    weekly = _stat_row(
        player_display_name='Some Kicker', player_name='S.Kicker', player_id='00-2',
        headshot_url='x.png', position='K', position_group='SPEC', season=2026,
        season_type='REG', week=1, recent_team='CIN', target_share=0.0,
        air_yards_share=0.0, dakota=None)
    with patch('nickknows.nfl.views.api_frames.weekly_stats', return_value=weekly), \
         patch('nickknows.nfl.views.api_frames.opportunity_weekly', return_value=_WEEKLY):
        r = client.get('/NFL/Player/Some Kicker')
    assert r.status_code == 200
    assert b'data-opp-charts' not in r.data


def test_player_names_match_through_punctuation_and_suffixes():
    from nickknows.nfl.views import _weekly_player
    payload = _weekly_with(_player('Omar Cooper Jr.', 'NYJ', 'WR', 'steady', 0.0))
    assert _weekly_player(payload, 'Omar Cooper Jr.')['data'][0]['team'] == 'NYJ'
    assert _weekly_player(payload, 'Omar Cooper') is not None
    assert _weekly_player(payload, 'Omar Coopers') is None
    assert _weekly_player(None, 'Anyone') is None


def test_nfl_opportunity_charts_open_on_pass_attempts(client):
    with patch('nickknows.nfl.views.api_frames.opportunity_weekly',
               return_value=_WEEKLY):
        r = client.get('/NFL/Opportunities/Trends?metric=pass_attempts&position=QB')
    body = r.data.decode()
    assert '"metric": "pass_attempts"' in body and '"position": "QB"' in body
    assert 'opportunity-charts.js' in body


def test_nfl_opportunity_charts_ignore_a_bad_metric(client):
    with patch('nickknows.nfl.views.api_frames.opportunity_weekly',
               return_value=_WEEKLY):
        r = client.get('/NFL/Opportunities/Trends?metric=drop%20table&direction=sideways')
    assert r.status_code == 200
    body = r.data.decode()
    assert '"metric": "opportunities"' in body and '"direction": "all"' in body


# ---------------------------------------------------------------------------
# Navbar partials (loaded via jQuery .load())
# ---------------------------------------------------------------------------

def test_nfl_navbar_partial(client):
    r = client.get('/templates/nfl_navbar.html')
    assert r.status_code == 200

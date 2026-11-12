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

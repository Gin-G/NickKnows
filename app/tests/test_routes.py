"""
Route endpoint tests for NickKnows Flask app.
Tests that all registered routes return non-500 responses.
Celery task .delay() calls are mocked to avoid requiring a running Redis/Celery instance.
"""
import pytest
from unittest.mock import patch, MagicMock
import sys
import os

# Ensure the app package is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Patch where the names are actually used (imported into nfl.views), not where defined.
TASK_PATCHES = [
    'nickknows.nfl.views.update_full_season_data',
    'nickknows.nfl.views.update_opportunity_data',
    'nickknows.nfl.views.update_all_teams_snap_counts',
    'nickknows.nfl.views.update_single_team_full',
    'nickknows.nfl.views.calculate_all_stat_leaders',
    'nickknows.nfl.views.calculate_opportunity_data',
    'nickknows.nfl.views.update_all_team_fpa',
    'nickknows.nfl.views.process_team_data',
    'nickknows.nfl.views.update_snap_count_data',
]


@pytest.fixture(scope='module')
def client():
    patches = [patch(p) for p in TASK_PATCHES]
    mocks = [p.start() for p in patches]
    for m in mocks:
        m.delay = MagicMock(return_value=MagicMock(id='test-task-id'))
        m.s = MagicMock(return_value=MagicMock())

    from nickknows import app as flask_app
    flask_app.config['TESTING'] = True
    flask_app.config['SECRET_KEY'] = 'test-secret-key'

    with flask_app.test_client() as c:
        yield c

    for p in patches:
        p.stop()


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


def test_brown_banana(client):
    r = client.get('/brown-banana')
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


# ---------------------------------------------------------------------------
# Navbar partials (loaded via jQuery .load())
# ---------------------------------------------------------------------------

def test_nfl_navbar_partial(client):
    r = client.get('/templates/nfl_navbar.html')
    assert r.status_code == 200

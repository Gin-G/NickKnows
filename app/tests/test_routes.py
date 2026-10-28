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


# ---------------------------------------------------------------------------
# Navbar partials (loaded via jQuery .load())
# ---------------------------------------------------------------------------

def test_nfl_navbar_partial(client):
    r = client.get('/templates/nfl_navbar.html')
    assert r.status_code == 200

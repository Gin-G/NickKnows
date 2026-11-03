"""The schedule-adjusted FPA frame the FPA page reads next to the raw one.

Raw FPA ranks a defence that faced four of the best tight ends the same as one
that faced replacements. These cover the shaping of the fixed version, and the
two ways it has to decline: a season without enough football for a baseline,
and an API image older than the adjustment.
"""
import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def _payload(adjusted=True, rows=None, note=None):
    return {
        'status': 'success', 'season': 2026, 'adjusted': adjusted,
        'adjusted_note': note,
        'data': rows if rows is not None else [{
            'team': 'PHI', 'qb': 18.2, 'rb': 12.1, 'wr': 29.7, 'te': 8.4,
            'qb_grade': 44.0, 'qb_faced': 17.1,
            'rb_grade': 51.2, 'rb_faced': 11.8,
            'wr_grade': 28.8, 'wr_faced': 32.4,
            'te_grade': 50.0, 'te_faced': 8.0,
        }],
    }


def _frames():
    from nickknows.nfl import api_frames
    return api_frames


def test_it_keeps_the_grade_and_the_schedule_faced():
    api_frames = _frames()
    with patch.object(api_frames.nfl_api_client, 'get_cached',
                      return_value=_payload()):
        frame, note = api_frames.fpa_adjusted(2026)
    assert note is None
    assert list(frame.columns) == [
        'Team Name',
        'QB Grade', 'QB Faced', 'RB Grade', 'RB Faced',
        'WR Grade', 'WR Faced', 'TE Grade', 'TE Faced',
    ]
    row = frame.iloc[0]
    assert row['Team Name'] == 'PHI'
    # The pair that makes the grade readable: a lot allowed, as much faced.
    assert row['WR Grade'] == 28.8
    assert row['WR Faced'] == 32.4


def test_early_season_hands_back_the_reason():
    api_frames = _frames()
    reason = ('Not enough football yet: a player needs three prior games '
              'before his own baseline can be subtracted.')
    with patch.object(api_frames.nfl_api_client, 'get_cached',
                      return_value=_payload(adjusted=False, note=reason)):
        frame, note = api_frames.fpa_adjusted(2026)
    assert frame is None
    assert note == reason


def test_an_api_without_the_new_columns_declines_quietly():
    api_frames = _frames()
    old = [{'team': 'PHI', 'qb': 18.2, 'rb': 12.1, 'wr': 29.7, 'te': 8.4}]
    with patch.object(api_frames.nfl_api_client, 'get_cached',
                      return_value=_payload(rows=old)):
        frame, note = api_frames.fpa_adjusted(2026)
    assert frame is None and note is None


def test_an_api_error_is_not_raised_at_the_view():
    api_frames = _frames()
    err = api_frames.nfl_api_client.NflApiError('connection refused')
    with patch.object(api_frames.nfl_api_client, 'get_cached', side_effect=err):
        assert api_frames.fpa_adjusted(2026) == (None, None)

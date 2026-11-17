"""Reads the AI Dev portfolio from IdeaBRD's public page.

IdeaBRD serves `GET /api/public/<github login>`: the ideas its owner flagged
public, read from their (private) board repo with their GitHub OAuth token,
and trimmed to what a portfolio may show. This page is a presentation layer
over that, the same way the NFL pages are over NFL-API.

Talk to the in-cluster Service, not ideabrd.nickknows.net, for the reason
nfl_api_client gives: the public hostname sits behind Cloudflare, which
challenges server-to-server requests. Override IDEABRD_URL for local dev.
"""
import os
import threading
import time

import requests

DEFAULT_BASE_URL = "http://ideabrd-backend.ideabrd.svc.cluster.local:8000"
BASE_URL = os.environ.get("IDEABRD_URL", DEFAULT_BASE_URL).rstrip("/")
LOGIN = os.environ.get("IDEABRD_LOGIN", "Gin-G")

# The page render waits on this call, so keep it short: a slow IdeaBRD should
# cost a visitor a moment, then the fallback list.
TIMEOUT = 3
CACHE_TTL = int(os.environ.get("IDEABRD_CACHE_TTL", "600"))
# A failure is remembered briefly, so an IdeaBRD that is down (or not yet
# serving /api/public) isn't asked again on every page view.
FAILURE_TTL = 60

_cache = {}
_lock = threading.Lock()


class IdeaBrdError(Exception):
    """IdeaBRD was unreachable or answered with something other than a board."""


def _fetch():
    url = f"{BASE_URL}/api/public/{LOGIN}"
    try:
        response = requests.get(url, timeout=TIMEOUT)
    except requests.RequestException as e:
        raise IdeaBrdError(f"IdeaBRD request failed: {url} ({e})") from e
    if not response.ok:
        raise IdeaBrdError(f"IdeaBRD returned {response.status_code} for {url}")
    try:
        board = response.json()
        ideas = board["ideas"]
    except (ValueError, KeyError, TypeError) as e:
        raise IdeaBrdError(f"IdeaBRD sent an unexpected body for {url}") from e
    return [_as_project(i) for i in ideas]


def _as_project(idea):
    """An IdeaBRD public idea, in the shape templates/ai-dev.html renders."""
    return {
        'name': idea.get('title') or idea.get('slug', ''),
        'status': idea.get('status') or 'idea',
        'progress': idea.get('progress'),
        'summary': idea.get('summary') or '',
        'live': idea.get('site_url'),
        'repo': idea.get('repo_url'),
        'todos_done': idea.get('todos_done', 0),
        'todos_total': idea.get('todos_total', 0),
        'stack': [],
        'started': None,
    }


def public_projects():
    """The public ideas, in board order. Raises IdeaBrdError on any failure."""
    now = time.time()
    with _lock:
        hit = _cache.get('projects')
        if hit and hit[0] > now:
            if isinstance(hit[1], IdeaBrdError):
                raise hit[1]
            return hit[1]
    try:
        projects = _fetch()  # outside the lock
    except IdeaBrdError as e:
        with _lock:
            _cache['projects'] = (now + FAILURE_TTL, e)
        raise
    with _lock:
        _cache['projects'] = (now + CACHE_TTL, projects)
    return projects


def clear_cache():
    with _lock:
        _cache.clear()

"""
Thin client for the NFL-API service.

Always talk to the in-cluster Service DNS, not the public hostname —
nfl-api.nickknows.net sits behind a Cloudflare bot-challenge that 403s
server-to-server requests. Override NFL_API_URL for local dev against
the public host.
"""
import os

import requests

DEFAULT_BASE_URL = "http://nfl-api.nfl-api.svc.cluster.local:8000"
BASE_URL = os.environ.get("NFL_API_URL", DEFAULT_BASE_URL).rstrip("/")

DEFAULT_TIMEOUT = 10


class NflApiError(Exception):
    """Raised when the NFL-API returns a non-2xx response or is unreachable."""


def get(path, timeout=DEFAULT_TIMEOUT, **params):
    """
    GET a path from the NFL-API and return parsed JSON.

    `params` are passed as query string args (None values are dropped so
    callers can pass optional filters like season=None without polluting
    the query string). Collection endpoints require a trailing slash on
    `path` (e.g. "/schedules/") or FastAPI 307-redirects; requests follows
    redirects by default so this only matters for correctness of caller intent.
    """
    url = f"{BASE_URL}{path}"
    query = {k: v for k, v in params.items() if v is not None}

    try:
        response = requests.get(url, params=query, timeout=timeout)
    except requests.RequestException as e:
        raise NflApiError(f"NFL-API request failed: {url} ({e})") from e

    if not response.ok:
        raise NflApiError(
            f"NFL-API returned {response.status_code} for {url}: {response.text[:200]}"
        )

    return response.json()


def is_no_data(payload):
    """True when the API responded successfully but has nothing for the query yet."""
    return isinstance(payload, dict) and payload.get("status") == "no_data"

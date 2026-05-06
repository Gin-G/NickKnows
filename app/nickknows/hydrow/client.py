import os
import threading
import time
from datetime import datetime, timezone

import requests

API_BASE = "https://v2.api.prod.hydrow-external.net"
FEATURE_FLAGS = "new-music,preferences-filter,top-left,single-sets"
BASE_HEADERS = {
    "Content-Type": "application/json",
    "x-hydrow-feature-flags": FEATURE_FLAGS,
}
REFRESH_LEEWAY_SECONDS = 5 * 60
REQUEST_TIMEOUT = 10

_lock = threading.Lock()
_session = {
    "access_token": None,
    "refresh_token": None,
    "expires_at": 0.0,  # epoch seconds
    "rower_id": None,
}


class HydrowError(RuntimeError):
    """Raised when the Hydrow API cannot be reached or returns an error."""


def _parse_expires_at(value):
    """Hydrow may return ISO 8601, epoch seconds, or epoch ms. Normalize to epoch seconds."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        # Heuristic: anything past year 5000 in seconds is almost certainly ms.
        return float(value) / 1000.0 if value > 1e11 else float(value)
    if isinstance(value, str):
        try:
            normalized = value.replace("Z", "+00:00")
            dt = datetime.fromisoformat(normalized)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except ValueError:
            return 0.0
    return 0.0


def _store_session(payload):
    rower = payload.get("rower") or {}
    _session["access_token"] = payload.get("accessToken")
    _session["refresh_token"] = payload.get("refreshToken") or _session.get("refresh_token")
    _session["expires_at"] = _parse_expires_at(payload.get("expiresAt"))
    if rower.get("id"):
        _session["rower_id"] = rower["id"]


def _login():
    email = os.environ.get("HYDROW_EMAIL")
    password = os.environ.get("HYDROW_PASSWORD")
    if not email or not password:
        raise HydrowError("HYDROW_EMAIL and HYDROW_PASSWORD must be set")
    resp = requests.post(
        f"{API_BASE}/rower/auth/login/unpw",
        json={"username": email, "password": password},
        headers=BASE_HEADERS,
        timeout=REQUEST_TIMEOUT,
    )
    if resp.status_code >= 400:
        raise HydrowError(f"login failed: {resp.status_code} {resp.text[:200]}")
    _store_session(resp.json())


def _refresh():
    refresh_token = _session.get("refresh_token")
    if not refresh_token:
        return False
    try:
        resp = requests.post(
            f"{API_BASE}/rower/auth/refresh",
            json={"refreshToken": refresh_token},
            headers=BASE_HEADERS,
            timeout=REQUEST_TIMEOUT,
        )
    except requests.RequestException:
        return False
    if resp.status_code >= 400:
        return False
    _store_session(resp.json())
    return True


def _ensure_token():
    """Caller must hold _lock."""
    now = time.time()
    if not _session.get("access_token"):
        _login()
        return
    if now >= _session["expires_at"] - REFRESH_LEEWAY_SECONDS:
        if not _refresh():
            _login()


def _auth_headers():
    return {**BASE_HEADERS, "Authorization": f"Bearer {_session['access_token']}"}


def _get(path):
    """GET path with auth, retrying once on 401 by re-authenticating."""
    with _lock:
        _ensure_token()
        rower_id = _session["rower_id"]
        headers = _auth_headers()

    url = f"{API_BASE}{path}"
    resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    if resp.status_code == 401:
        with _lock:
            _login()
            headers = _auth_headers()
            rower_id = _session["rower_id"]
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    if resp.status_code >= 400:
        raise HydrowError(f"GET {path} failed: {resp.status_code} {resp.text[:200]}")
    return resp.json(), rower_id


def fetch_stats(today_iso):
    """Fetch summary, personal records, and recent progress. Returns a dict."""
    # Prime the session so we know the rower id before formatting paths.
    with _lock:
        _ensure_token()
        rower_id = _session["rower_id"]
    if not rower_id:
        raise HydrowError("login succeeded but no rower id was returned")

    summary, _ = _get(f"/progress/{rower_id}/summary")
    personal_records, _ = _get(f"/progress/{rower_id}/personal_records")
    recent, _ = _get(f"/rower/{rower_id}/v2/progress?today={today_iso}")
    return {
        "summary": summary,
        "personal_records": personal_records,
        "recent": recent,
    }

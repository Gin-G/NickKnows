"""
Web-pod-side Hydrow client.

This module never sees the Hydrow password and never refreshes tokens.
It calls the in-cluster credential broker for short-lived access tokens
and uses them to make read-only data calls against the Hydrow API.
"""
import hashlib
import hmac
import os
import threading
import time

import requests

BROKER_URL = os.environ.get("HYDROW_BROKER_URL", "http://hydrow-broker")
BROKER_HMAC_SECRET = os.environ.get("HYDROW_BROKER_HMAC_SECRET", "")
HYDROW_API_BASE = "https://v2.api.prod.hydrow-external.net"
FEATURE_FLAGS = "new-music,preferences-filter,top-left,single-sets"
REQUEST_TIMEOUT = 10
TOKEN_LEEWAY_SECONDS = 5 * 60

_lock = threading.Lock()
_token_cache = {"access_token": None, "expires_at": 0.0, "rower_id": None}


class HydrowError(RuntimeError):
    """Raised when the broker or the Hydrow API cannot satisfy a request."""


def _sign(timestamp, body):
    msg = f"{timestamp}\n{body}".encode("utf-8")
    return hmac.new(BROKER_HMAC_SECRET.encode("utf-8"), msg, hashlib.sha256).hexdigest()


def _parse_expires_at(value):
    if isinstance(value, (int, float)):
        # Heuristic: large values are milliseconds.
        return float(value) / 1000.0 if value > 1e11 else float(value)
    return 0.0


def _fetch_token_from_broker():
    if not BROKER_HMAC_SECRET:
        raise HydrowError("HYDROW_BROKER_HMAC_SECRET is not set")
    timestamp = str(int(time.time()))
    body = ""
    headers = {
        "Content-Type": "application/json",
        "X-Broker-Timestamp": timestamp,
        "X-Broker-Signature": _sign(timestamp, body),
    }
    try:
        resp = requests.post(
            f"{BROKER_URL}/token",
            headers=headers,
            data=body,
            timeout=REQUEST_TIMEOUT,
        )
    except requests.RequestException as exc:
        raise HydrowError(f"broker unreachable: {exc}") from exc
    if resp.status_code >= 400:
        raise HydrowError(f"broker /token returned status={resp.status_code}")
    data = resp.json()
    if "accessToken" not in data or "rowerId" not in data:
        raise HydrowError("broker response missing required fields")
    return {
        "access_token": data["accessToken"],
        "expires_at": _parse_expires_at(data.get("expiresAt")),
        "rower_id": data["rowerId"],
    }


def _ensure_token():
    """Caller must hold _lock."""
    now = time.time()
    if _token_cache["access_token"] and now < _token_cache["expires_at"] - TOKEN_LEEWAY_SECONDS:
        return
    _token_cache.update(_fetch_token_from_broker())


def _auth_headers():
    return {
        "Content-Type": "application/json",
        "x-hydrow-feature-flags": FEATURE_FLAGS,
        "Authorization": f"Bearer {_token_cache['access_token']}",
    }


def _get(path):
    """GET against the Hydrow API. Retries once on 401 by forcing a token refresh."""
    with _lock:
        _ensure_token()
        rower_id = _token_cache["rower_id"]
        headers = _auth_headers()

    url = f"{HYDROW_API_BASE}{path}"
    resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    if resp.status_code == 401:
        with _lock:
            _token_cache["access_token"] = None
            _ensure_token()
            headers = _auth_headers()
            rower_id = _token_cache["rower_id"]
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    if resp.status_code >= 400:
        # Hydrow error bodies can echo identifiers; log status only.
        raise HydrowError(f"GET {path} failed: status={resp.status_code}")
    return resp.json(), rower_id


def fetch_stats(today_iso):
    """Fetch summary, personal records, and recent progress."""
    with _lock:
        _ensure_token()
        rower_id = _token_cache["rower_id"]
    if not rower_id:
        raise HydrowError("broker did not return a rower id")

    summary, _ = _get(f"/progress/{rower_id}/summary")
    recent, _ = _get(f"/rower/{rower_id}/v2/progress?today={today_iso}")
    profile, _ = _get(f"/rower/{rower_id}")
    return {
        "summary": summary,
        "recent": recent,
        "profile": profile,
    }

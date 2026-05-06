"""
Hydrow credential broker.

Owns HYDROW_EMAIL / HYDROW_PASSWORD. Issues short-lived access tokens to
the web pod over an HMAC-signed POST. Single-flights refreshes through a
Redis lock because Hydrow invalidates the entire refresh-token chain on
reuse — concurrent refreshes would force a re-login every time.

Logs status codes only. Never logs tokens, the password, signatures, or
Hydrow response bodies (which can echo identifiers).
"""
import hashlib
import hmac
import json
import logging
import os
import time

import redis
import requests
from flask import Flask, jsonify, request

HYDROW_API_BASE = "https://v2.api.prod.hydrow-external.net"
FEATURE_FLAGS = "new-music,preferences-filter,top-left,single-sets"
BASE_HEADERS = {
    "Content-Type": "application/json",
    "x-hydrow-feature-flags": FEATURE_FLAGS,
}

REDIS_HOST = os.environ.get("REDIS_ENV", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
HMAC_SECRET = os.environ.get("HYDROW_BROKER_HMAC_SECRET", "")
HYDROW_EMAIL = os.environ.get("HYDROW_EMAIL", "")
HYDROW_PASSWORD = os.environ.get("HYDROW_PASSWORD", "")

REFRESH_LOCK_KEY = "hydrow:refresh_lock"
REFRESH_TOKEN_KEY = "hydrow:refresh_token"
ACCESS_TOKEN_KEY = "hydrow:access_token"  # JSON {accessToken, expiresAt, rowerId}
ROWER_ID_KEY = "hydrow:rower_id"

REFRESH_LOCK_TTL = 5  # seconds
REFRESH_LEEWAY = 5 * 60  # serve a fresh token if at least this long until expiry
HMAC_SKEW_SECONDS = 60
WAIT_FOR_LOCK_TIMEOUT = 4.5  # less than REFRESH_LOCK_TTL
REQUEST_TIMEOUT = 10

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = app.logger

_redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


class BrokerError(RuntimeError):
    pass


# ── HMAC ───────────────────────────────────────────────────────────────────────

def _expected_signature(timestamp, body):
    msg = f"{timestamp}\n{body}".encode("utf-8")
    return hmac.new(HMAC_SECRET.encode("utf-8"), msg, hashlib.sha256).hexdigest()


def _verify_request():
    if not HMAC_SECRET:
        return False, "broker not configured"
    ts = request.headers.get("X-Broker-Timestamp", "")
    sig = request.headers.get("X-Broker-Signature", "")
    if not ts or not sig:
        return False, "missing signature headers"
    try:
        ts_int = int(ts)
    except ValueError:
        return False, "bad timestamp"
    if abs(time.time() - ts_int) > HMAC_SKEW_SECONDS:
        return False, "stale timestamp"
    body = request.get_data(as_text=True) or ""
    if not hmac.compare_digest(sig, _expected_signature(ts, body)):
        return False, "bad signature"
    return True, None


# ── Hydrow auth ────────────────────────────────────────────────────────────────

def _parse_expires_at(value):
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value) // 1000 if value > 1e11 else int(value)
    # ISO 8601 fallback
    from datetime import datetime, timezone
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except ValueError:
        return 0


def _persist_session(session):
    """Persist refresh token first (it's the chain's anchor), then access token.
    If we crash between, the next call will refresh successfully from the new
    refresh token rather than reusing the old (dead) one."""
    if session.get("refreshToken"):
        _redis.set(REFRESH_TOKEN_KEY, session["refreshToken"])
    rower = (session.get("rower") or {})
    if rower.get("id"):
        _redis.set(ROWER_ID_KEY, rower["id"])
    expires_at = _parse_expires_at(session.get("expiresAt"))
    public_payload = {
        "accessToken": session.get("accessToken"),
        "expiresAt": expires_at,
        "rowerId": _redis.get(ROWER_ID_KEY) or rower.get("id"),
    }
    ttl = max(1, expires_at - int(time.time()) - REFRESH_LEEWAY)
    _redis.set(ACCESS_TOKEN_KEY, json.dumps(public_payload), ex=ttl)
    return public_payload


def _password_login():
    if not HYDROW_EMAIL or not HYDROW_PASSWORD:
        raise BrokerError("password fallback unavailable: credentials not set")
    try:
        resp = requests.post(
            f"{HYDROW_API_BASE}/rower/auth/login/unpw",
            json={"username": HYDROW_EMAIL, "password": HYDROW_PASSWORD},
            headers=BASE_HEADERS,
            timeout=REQUEST_TIMEOUT,
        )
    except requests.RequestException as exc:
        raise BrokerError(f"login network error: {type(exc).__name__}") from exc
    if resp.status_code >= 400:
        raise BrokerError(f"login failed: status={resp.status_code}")
    log.info("password login succeeded")
    return _persist_session(resp.json())


def _refresh_with(refresh_token):
    try:
        resp = requests.post(
            f"{HYDROW_API_BASE}/rower/auth/refresh",
            json={"refreshToken": refresh_token},
            headers=BASE_HEADERS,
            timeout=REQUEST_TIMEOUT,
        )
    except requests.RequestException:
        return None
    if resp.status_code >= 400:
        log.info("refresh rejected: status=%s", resp.status_code)
        return None
    log.info("refresh succeeded")
    return _persist_session(resp.json())


def _do_refresh_or_login():
    refresh_token = _redis.get(REFRESH_TOKEN_KEY)
    if refresh_token:
        result = _refresh_with(refresh_token)
        if result:
            return result
        # Refresh failed — chain is dead. Drop the stale token before login.
        _redis.delete(REFRESH_TOKEN_KEY)
    return _password_login()


def _read_cached_access_token():
    raw = _redis.get(ACCESS_TOKEN_KEY)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def _wait_for_holder(deadline):
    """Another worker is refreshing. Poll for the new access token to land."""
    while time.time() < deadline:
        time.sleep(0.1)
        cached = _read_cached_access_token()
        if cached:
            return cached
    return None


def get_access_token():
    """Single-flight token retrieval. Concurrent callers funnel through one
    refresh; everyone else waits for the result rather than refreshing in
    parallel (which would trigger Hydrow's reuse-detection)."""
    cached = _read_cached_access_token()
    if cached:
        return cached

    # Acquire the refresh lock. If we get it, do the refresh; otherwise wait.
    acquired = _redis.set(REFRESH_LOCK_KEY, "1", nx=True, ex=REFRESH_LOCK_TTL)
    if not acquired:
        result = _wait_for_holder(time.time() + WAIT_FOR_LOCK_TIMEOUT)
        if result:
            return result
        raise BrokerError("timed out waiting for in-flight refresh")
    try:
        # Re-check: another holder may have published just before we locked.
        cached = _read_cached_access_token()
        if cached:
            return cached
        return _do_refresh_or_login()
    finally:
        _redis.delete(REFRESH_LOCK_KEY)


# ── HTTP routes ────────────────────────────────────────────────────────────────

@app.route("/healthz")
def healthz():
    return ("ok", 200)


@app.route("/token", methods=["POST"])
def token():
    ok, err = _verify_request()
    if not ok:
        log.warning("rejected request: %s", err)
        return ("", 401)
    try:
        payload = get_access_token()
    except BrokerError as exc:
        log.error("token issuance failed: %s", exc)
        return ("", 502)
    return jsonify(payload), 200

import json
import os
import time
from datetime import date

import redis
from flask import render_template

from nickknows import app
from nickknows.hydrow import client

CACHE_KEY = "hydrow:stats:cache"
CACHE_TTL_SECONDS = 10 * 60

_redis = redis.Redis(
    host=os.environ.get("REDIS_ENV", "redis"),
    port=6379,
    decode_responses=True,
    socket_timeout=2,
    socket_connect_timeout=2,
)


def _to_number(value):
    """Hydrow returns numbers as strings (e.g. "11523"). Coerce."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value
    try:
        s = str(value)
        return float(s) if "." in s else int(s)
    except (TypeError, ValueError):
        return None


def _format_meters(value):
    n = _to_number(value)
    if n is None:
        return None
    return f"{n / 1000:,.1f} km" if n >= 1000 else f"{n:,.0f} m"


def _format_meters_raw(value):
    """Lifetime distance only — keep the big-meters look ("2,402,198 m")."""
    n = _to_number(value)
    return f"{int(n):,} m" if n is not None else None


def _format_seconds(value):
    n = _to_number(value)
    if n is None:
        return None
    n = int(n)
    h, rem = divmod(n, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m}m"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def _format_int(value):
    n = _to_number(value)
    return f"{int(n):,}" if n is not None else None


def _stats_by_type(stats_list):
    """[{type, value, delta?}, ...] → {type: {value, delta}}."""
    out = {}
    for s in stats_list or []:
        t = s.get("type")
        if t:
            out[t] = {"value": s.get("value"), "delta": s.get("delta")}
    return out


def _summary_for_range(workout_summaries, name):
    for s in workout_summaries or []:
        # /summary uses `range`, /v2/progress uses `timeFrame` for the same field.
        if s.get("range") == name or s.get("timeFrame") == name:
            return _stats_by_type(s.get("stats"))
    return {}


def _card(label, value):
    return {"label": label, "value": value} if value is not None else None


def _period_cards(stats):
    cards = [
        _card("Distance", _format_meters((stats.get("meters") or {}).get("value"))),
        _card("Time", _format_seconds((stats.get("timeWorkedOut") or {}).get("value"))),
        _card("Calories", _format_int((stats.get("calories") or {}).get("value"))),
        _card("Active Days", _format_int((stats.get("daysActive") or {}).get("value"))),
    ]
    return [c for c in cards if c]


def _profile_view(profile):
    """Whitelist the public-safe fields. /rower/{id} returns email, weight,
    birthDate, etc. — surface only what's meant for a public bio."""
    if not isinstance(profile, dict):
        return None
    location = (profile.get("location") or "").strip() or None
    return {
        "screen_name": profile.get("screenName"),
        "image_url": profile.get("profileImageUrl512"),
        "location": location,
    }


def _build_view_model(raw):
    summary = raw.get("summary") or {}
    recent = raw.get("recent") or {}

    workouts = summary.get("workoutSummaries") or []
    lifetime = _summary_for_range(workouts, "lifetime")

    lifetime_cards = [c for c in [
        _card("Total Distance", _format_meters_raw((lifetime.get("meters") or {}).get("value"))),
        _card("Total Time", _format_seconds((lifetime.get("timeWorkedOut") or {}).get("value"))),
        _card("Total Calories", _format_int((lifetime.get("calories") or {}).get("value"))),
        _card("Active Days", _format_int((lifetime.get("daysActive") or {}).get("value"))),
    ] if c]

    last7 = _period_cards(_summary_for_range(workouts, "last7"))
    last30 = _period_cards(_summary_for_range(workouts, "last30"))
    recent_periods = []
    if last7:
        recent_periods.append({"label": "Last 7 Days", "stats": last7})
    if last30:
        recent_periods.append({"label": "Last 30 Days", "stats": last30})

    streak = ((recent.get("weeklyStreak") or {}).get("number"))
    active_days = (recent.get("activeDaysSummary") or {}).get("calendarDatesActive") or []

    return {
        "lifetime_cards": lifetime_cards,
        "recent_periods": recent_periods,
        "streak_weeks": _to_number(streak),
        "active_days_count": len(active_days),
        "active_days_lookback": (recent.get("activeDaysSummary") or {}).get("lookbackWeeks"),
        "profile": _profile_view(raw.get("profile")),
    }


def get_cached_profile():
    """Read the cached profile from Redis without triggering a Hydrow fetch.
    Used by the home page tile so the home render never blocks on Hydrow."""
    cached = _read_cache()
    if not cached:
        return None
    vm = cached.get("view_model") or {}
    return vm.get("profile")


def _read_cache():
    try:
        raw = _redis.get(CACHE_KEY)
    except redis.RedisError as exc:
        app.logger.warning("hydrow cache read failed: %s", exc)
        return None
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return None


def _write_cache(view_model):
    payload = json.dumps({"fetched_at": time.time(), "view_model": view_model})
    try:
        # No Redis TTL — we keep the last-good payload indefinitely so we can
        # serve it stale on broker errors. Freshness is decided by fetched_at.
        _redis.set(CACHE_KEY, payload)
    except redis.RedisError as exc:
        app.logger.warning("hydrow cache write failed: %s", exc)


def _get_cached_stats():
    """Returns (view_model, error_message, stale_flag)."""
    cached = _read_cache()
    if cached and (time.time() - cached.get("fetched_at", 0)) < CACHE_TTL_SECONDS:
        return cached["view_model"], None, False

    today_iso = date.today().isoformat()
    try:
        raw = client.fetch_stats(today_iso)
        view_model = _build_view_model(raw)
        _write_cache(view_model)
        return view_model, None, False
    except Exception as exc:  # noqa: BLE001 — keep the page up regardless of failure mode
        app.logger.warning("hydrow stats fetch failed: %s", exc)
        if cached:
            return cached["view_model"], "Showing cached stats — Hydrow API is unreachable.", True
        return None, "Hydrow stats are temporarily unavailable.", False


@app.route("/hydrow-stats")
def hydrow_stats():
    view_model, error, stale = _get_cached_stats()
    return render_template(
        "hydrow-stats.html",
        stats=view_model,
        error=error,
        stale=stale,
    )

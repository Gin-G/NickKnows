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


def _humanize_seconds(total_seconds):
    try:
        total = int(total_seconds)
    except (TypeError, ValueError):
        return None
    hours, rem = divmod(total, 3600)
    minutes, seconds = divmod(rem, 60)
    if hours:
        return f"{hours}h {minutes}m {seconds}s"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def _humanize_meters(meters):
    try:
        m = float(meters)
    except (TypeError, ValueError):
        return None
    if m >= 1000:
        return f"{m / 1000:,.1f} km"
    return f"{m:,.0f} m"


def _label(key):
    parts = []
    word = ""
    for ch in str(key).replace("_", " "):
        if ch.isupper() and word and not word[-1].isspace():
            parts.append(word)
            word = ch
        else:
            word += ch
    if word:
        parts.append(word)
    return " ".join(parts).strip().title()


def _format_value(key, value):
    if value is None:
        return "—"
    lower = str(key).lower()
    if "meter" in lower or lower.endswith("distance"):
        formatted = _humanize_meters(value)
        if formatted is not None:
            return formatted
    if "second" in lower or "duration" in lower or lower.endswith("time"):
        formatted = _humanize_seconds(value)
        if formatted is not None:
            return formatted
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return f"{value:,}" if isinstance(value, int) else f"{value:,.2f}"
    return str(value)


def _flatten_stats(data):
    if not isinstance(data, dict):
        return []
    rows = []
    for key, value in data.items():
        if isinstance(value, (dict, list)):
            continue
        rows.append((_label(key), _format_value(key, value)))
    return rows


def _normalize_personal_records(data):
    records = []
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        for wrapper in ("records", "items", "data", "personalRecords"):
            if isinstance(data.get(wrapper), list):
                items = data[wrapper]
                break
        else:
            items = [{"name": str(k), **(v if isinstance(v, dict) else {"value": v})}
                     for k, v in data.items()]
    else:
        return records

    for item in items:
        if not isinstance(item, dict):
            continue
        name = item.get("name") or item.get("title") or item.get("distance") or item.get("type") or "Record"
        rows = [(_label(k), _format_value(k, v))
                for k, v in item.items()
                if k not in ("name", "title") and not isinstance(v, (dict, list))]
        records.append({"name": str(name), "rows": rows})
    return records


def _build_view_model(raw):
    return {
        "lifetime": _flatten_stats(raw.get("summary")),
        "personal_records": _normalize_personal_records(raw.get("personal_records")),
        "recent": _flatten_stats(raw.get("recent")),
    }


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

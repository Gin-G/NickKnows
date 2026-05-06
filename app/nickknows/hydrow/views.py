import threading
import time
from datetime import date

from flask import render_template

from nickknows import app
from nickknows.hydrow import client

CACHE_TTL_SECONDS = 10 * 60

_cache_lock = threading.Lock()
_cache = {"fetched_at": 0.0, "payload": None}


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
    # Convert camelCase / snake_case API keys into a human label.
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
    """Turn a dict of scalar API fields into [(label, formatted_value), ...]."""
    if not isinstance(data, dict):
        return []
    rows = []
    for key, value in data.items():
        if isinstance(value, (dict, list)):
            continue
        rows.append((_label(key), _format_value(key, value)))
    return rows


def _normalize_personal_records(data):
    """Personal records may arrive as a list or a dict keyed by record name."""
    records = []
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        # Some Hydrow responses wrap the records under a key like "records" or "items".
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


def _get_cached_stats():
    """Returns (view_model, error_message, stale_flag)."""
    now = time.time()
    with _cache_lock:
        cached = _cache["payload"]
        fresh = cached is not None and (now - _cache["fetched_at"]) < CACHE_TTL_SECONDS
    if fresh:
        return cached, None, False

    today_iso = date.today().isoformat()
    try:
        raw = client.fetch_stats(today_iso)
        view_model = _build_view_model(raw)
        with _cache_lock:
            _cache["payload"] = view_model
            _cache["fetched_at"] = time.time()
        return view_model, None, False
    except Exception as exc:  # noqa: BLE001 — keep the page up regardless of failure mode
        app.logger.warning("hydrow stats fetch failed: %s", exc)
        with _cache_lock:
            stale = _cache["payload"]
        if stale is not None:
            return stale, "Showing cached stats — Hydrow API is unreachable.", True
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

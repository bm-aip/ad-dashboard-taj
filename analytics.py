"""
Analytics blueprint for Bharathi Meraki dashboards.
Drop-in module: register on the Flask app and you're done.

Usage in app.py:
    from analytics import analytics_bp, init_analytics_db
    init_analytics_db()
    app.register_blueprint(analytics_bp)

Env vars required:
    ADMIN_TOKEN        - secret string to access /_admin/analytics
    ANALYTICS_DB_PATH  - optional, defaults to ./analytics.db
    DASHBOARD_NAME     - optional label shown in admin view (e.g. "GTB/BM")
"""

import os
import sqlite3
import uuid
from datetime import datetime, timedelta
from functools import wraps
from contextlib import contextmanager

from flask import Blueprint, request, jsonify, render_template, abort, make_response

DB_PATH = os.environ.get("ANALYTICS_DB_PATH", "analytics.db")
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")
DASHBOARD_NAME = os.environ.get("DASHBOARD_NAME", "Dashboard")
COOKIE_NAME = "_bm_visitor_id"
COOKIE_MAX_AGE = 60 * 60 * 24 * 365  # 1 year

analytics_bp = Blueprint(
    "analytics",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/_analytics/static",
)


# ---------- DB ----------

@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_analytics_db():
    """Create tables if missing. Safe to call on every app start."""
    with get_db() as db:
        db.executescript("""
            CREATE TABLE IF NOT EXISTS sessions (
                visitor_id TEXT PRIMARY KEY,
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL,
                total_active_seconds INTEGER NOT NULL DEFAULT 0,
                pageview_count INTEGER NOT NULL DEFAULT 0,
                browser TEXT
            );

            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                visitor_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                tab_name TEXT,
                timestamp TEXT NOT NULL,
                active_seconds INTEGER DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_events_visitor ON events(visitor_id);
            CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);
            CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
        """)


# ---------- Helpers ----------

def _get_or_set_visitor_id(response_setter):
    """Read visitor_id cookie, mint a new one if missing."""
    vid = request.cookies.get(COOKIE_NAME)
    is_new = False
    if not vid:
        vid = str(uuid.uuid4())
        is_new = True
        response_setter(vid)
    return vid, is_new


def _detect_browser(ua: str) -> str:
    if not ua:
        return "unknown"
    ua = ua.lower()
    if "edg/" in ua:
        return "edge"
    if "chrome" in ua and "safari" in ua:
        return "chrome"
    if "firefox" in ua:
        return "firefox"
    if "safari" in ua:
        return "safari"
    return "other"


def _require_admin_token():
    token = request.args.get("token") or request.headers.get("X-Admin-Token")
    if not ADMIN_TOKEN or token != ADMIN_TOKEN:
        abort(404)  # 404 not 403 so the route is undiscoverable


# ---------- Event ingestion ----------

@analytics_bp.route("/_analytics/event", methods=["POST"])
def record_event():
    """Browser POSTs events here. Returns the visitor_id cookie if new."""
    payload = request.get_json(silent=True) or {}
    event_type = payload.get("event_type", "pageview")
    tab_name = payload.get("tab_name")
    session_id = payload.get("session_id", "")
    active_seconds = int(payload.get("active_seconds", 0) or 0)

    if event_type not in ("pageview", "tab_view", "heartbeat", "session_end"):
        return jsonify({"ok": False, "error": "bad event_type"}), 400

    new_cookie_value = {"value": None}

    def setter(vid):
        new_cookie_value["value"] = vid

    visitor_id, is_new = _get_or_set_visitor_id(setter)
    now = datetime.utcnow().isoformat()
    browser = _detect_browser(request.headers.get("User-Agent", ""))

    with get_db() as db:
        # Upsert session
        row = db.execute(
            "SELECT visitor_id FROM sessions WHERE visitor_id = ?",
            (visitor_id,),
        ).fetchone()

        if row is None:
            db.execute(
                """INSERT INTO sessions
                   (visitor_id, first_seen, last_seen, total_active_seconds,
                    pageview_count, browser)
                   VALUES (?, ?, ?, 0, 0, ?)""",
                (visitor_id, now, now, browser),
            )

        # Update aggregates
        if event_type == "pageview":
            db.execute(
                """UPDATE sessions SET last_seen = ?, pageview_count = pageview_count + 1
                   WHERE visitor_id = ?""",
                (now, visitor_id),
            )
        elif event_type == "heartbeat":
            db.execute(
                """UPDATE sessions
                   SET last_seen = ?, total_active_seconds = total_active_seconds + ?
                   WHERE visitor_id = ?""",
                (now, active_seconds, visitor_id),
            )
        else:
            db.execute(
                "UPDATE sessions SET last_seen = ? WHERE visitor_id = ?",
                (now, visitor_id),
            )

        db.execute(
            """INSERT INTO events
               (visitor_id, session_id, event_type, tab_name, timestamp, active_seconds)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (visitor_id, session_id, event_type, tab_name, now, active_seconds),
        )

    resp = make_response(jsonify({"ok": True, "visitor_id": visitor_id}))
    if new_cookie_value["value"]:
        resp.set_cookie(
            COOKIE_NAME,
            new_cookie_value["value"],
            max_age=COOKIE_MAX_AGE,
            samesite="Lax",
            httponly=False,  # JS needs to read it for session_id pairing
        )
    return resp


@analytics_bp.route("/_analytics/tracker.js")
def tracker_js():
    """Serve the tracker JS so you only have one <script> tag to add."""
    from flask import send_from_directory
    return send_from_directory(
        analytics_bp.static_folder, "tracker.js", mimetype="application/javascript"
    )


# ---------- Admin view ----------

def admin_required(fn):
    @wraps(fn)
    def wrapper(*a, **kw):
        _require_admin_token()
        return fn(*a, **kw)
    return wrapper


@analytics_bp.route("/_admin/analytics")
@admin_required
def admin_view():
    return render_template(
        "admin_analytics.html",
        dashboard_name=DASHBOARD_NAME,
        token=request.args.get("token", ""),
    )


@analytics_bp.route("/_admin/analytics/data")
@admin_required
def admin_data():
    """JSON endpoint for the admin dashboard charts."""
    now = datetime.utcnow()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    week_start = (now - timedelta(days=7)).isoformat()
    month_start = (now - timedelta(days=30)).isoformat()

    with get_db() as db:
        def count_unique(since):
            return db.execute(
                "SELECT COUNT(DISTINCT visitor_id) FROM events WHERE timestamp >= ?",
                (since,),
            ).fetchone()[0]

        unique_today = count_unique(today_start)
        unique_7d = count_unique(week_start)
        unique_30d = count_unique(month_start)

        pageviews_7d = db.execute(
            "SELECT COUNT(*) FROM events WHERE event_type='pageview' AND timestamp >= ?",
            (week_start,),
        ).fetchone()[0]

        avg_seconds = db.execute(
            """SELECT AVG(total_active_seconds) FROM sessions
               WHERE last_seen >= ? AND total_active_seconds > 0""",
            (week_start,),
        ).fetchone()[0] or 0

        # Tab engagement (last 7d)
        tab_rows = db.execute(
            """SELECT tab_name,
                      COUNT(*) AS views,
                      SUM(active_seconds) AS seconds
               FROM events
               WHERE timestamp >= ? AND tab_name IS NOT NULL
               GROUP BY tab_name
               ORDER BY views DESC""",
            (week_start,),
        ).fetchall()

        # Daily visitor trend (last 14 days)
        trend_rows = db.execute(
            """SELECT substr(timestamp, 1, 10) AS day,
                      COUNT(DISTINCT visitor_id) AS visitors,
                      COUNT(*) FILTER (WHERE event_type='pageview') AS pageviews
               FROM events
               WHERE timestamp >= ?
               GROUP BY day
               ORDER BY day""",
            ((now - timedelta(days=14)).isoformat(),),
        ).fetchall()

        # Recent visitors
        recent = db.execute(
            """SELECT visitor_id, first_seen, last_seen,
                      total_active_seconds, pageview_count, browser
               FROM sessions
               ORDER BY last_seen DESC
               LIMIT 25"""
        ).fetchall()

        # Browser breakdown
        browser_rows = db.execute(
            """SELECT browser, COUNT(*) AS n FROM sessions
               WHERE last_seen >= ?
               GROUP BY browser ORDER BY n DESC""",
            (month_start,),
        ).fetchall()

    return jsonify({
        "summary": {
            "unique_today": unique_today,
            "unique_7d": unique_7d,
            "unique_30d": unique_30d,
            "pageviews_7d": pageviews_7d,
            "avg_session_seconds": int(avg_seconds),
        },
        "tabs": [dict(r) for r in tab_rows],
        "trend": [dict(r) for r in trend_rows],
        "recent": [
            {
                "visitor_id": r["visitor_id"][:8],
                "first_seen": r["first_seen"],
                "last_seen": r["last_seen"],
                "active_seconds": r["total_active_seconds"],
                "pageviews": r["pageview_count"],
                "browser": r["browser"],
            }
            for r in recent
        ],
        "browsers": [dict(r) for r in browser_rows],
    })

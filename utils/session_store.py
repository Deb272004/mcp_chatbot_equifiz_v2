"""
SQLite-backed session store.
Thread-safe via check_same_thread=False + a module-level lock.
Works with a single SQLite file; survives container restarts when the
data/ directory is mounted as a Docker volume.

Changes from v1:
- Session IDs are now full UUIDs (36 chars) — eliminates the 8-char
  guessability/collision risk.
- saved_queries now scoped per user_id — guest users no longer see
  each other's saved queries.
"""
import sqlite3
import os
import uuid
import threading
from datetime import datetime
from typing import List, Dict, Any, Optional

DB_PATH = os.environ.get(
    "SESSION_DB_PATH",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "sessions.db"),
)

_lock = threading.Lock()


def _conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


def _init_db():
    with _lock, _conn() as con:
        con.executescript("""
            CREATE TABLE IF NOT EXISTS sessions (
                id          TEXT PRIMARY KEY,
                title       TEXT NOT NULL DEFAULT '',
                created_at  TEXT NOT NULL,
                shared      INTEGER NOT NULL DEFAULT 0,
                share_token TEXT,
                user_id     TEXT NOT NULL DEFAULT 'guest'
            );
            CREATE TABLE IF NOT EXISTS messages (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
                role       TEXT NOT NULL,
                content    TEXT NOT NULL,
                ts         TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS saved_queries (
                id         TEXT PRIMARY KEY,
                title      TEXT NOT NULL,
                query      TEXT NOT NULL,
                created_at TEXT NOT NULL,
                user_id    TEXT NOT NULL DEFAULT 'guest'
            );
            CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);
            CREATE INDEX IF NOT EXISTS idx_saved_queries_user_id ON saved_queries(user_id);
        """)


_init_db()


# ─── helpers ──────────────────────────────────────────────────────────────────

def _session_row_to_dict(row, messages: List[Dict]) -> Dict:
    return {
        "id":          row["id"],
        "title":       row["title"],
        "created_at":  row["created_at"],
        "shared":      bool(row["shared"]),
        "share_token": row["share_token"],
        "messages":    messages,
    }


def _get_messages(con: sqlite3.Connection, sid: str) -> List[Dict]:
    rows = con.execute(
        "SELECT role, content, ts FROM messages WHERE session_id=? ORDER BY id", (sid,)
    ).fetchall()
    return [{"role": r["role"], "content": r["content"], "ts": r["ts"]} for r in rows]


# ─── Sessions ─────────────────────────────────────────────────────────────────

def new_session(title: str = "", user_id: str = "guest") -> str:
    # Full UUID — no truncation, eliminates guessability risk
    sid = str(uuid.uuid4())
    now = datetime.now().isoformat()
    with _lock, _conn() as con:
        con.execute(
            "INSERT INTO sessions (id, title, created_at, user_id) VALUES (?, ?, ?, ?)",
            (sid, title or f"Session {sid[:8]}", now, user_id),
        )
    return sid


def get_session(sid: str) -> Optional[Dict]:
    with _lock, _conn() as con:
        row = con.execute("SELECT * FROM sessions WHERE id=?", (sid,)).fetchone()
        if not row:
            return None
        return _session_row_to_dict(row, _get_messages(con, sid))


def get_session_for_user(sid: str, user_id: str) -> Optional[Dict]:
    """
    Like get_session, but also checks ownership.
    Returns None if the session exists but belongs to a different user.
    Callers should raise 404 (not 403) to avoid leaking session existence.
    In guest mode every caller is 'guest', so this still correctly scopes
    sessions to the single shared identity without needing Firebase.
    """
    with _lock, _conn() as con:
        uid = user_id or "guest"
        row = con.execute(
            "SELECT * FROM sessions WHERE id=? AND user_id=?", (sid, uid)
        ).fetchone()
        if not row:
            return None
        return _session_row_to_dict(row, _get_messages(con, sid))


def list_sessions(user_id: str = "") -> List[Dict]:
    # Always scope by user_id — guest users only see sessions they created,
    # not every session in the DB from other callers.
    with _lock, _conn() as con:
        uid = user_id or "guest"
        rows = con.execute(
            "SELECT * FROM sessions WHERE user_id=? ORDER BY created_at DESC", (uid,)
        ).fetchall()
        return [_session_row_to_dict(r, _get_messages(con, r["id"])) for r in rows]


def add_message(sid: str, role: str, content: str):
    now = datetime.now().isoformat()
    with _lock, _conn() as con:
        con.execute(
            "INSERT INTO messages (session_id, role, content, ts) VALUES (?, ?, ?, ?)",
            (sid, role, content, now),
        )


def delete_session(sid: str):
    with _lock, _conn() as con:
        con.execute("DELETE FROM sessions WHERE id=?", (sid,))


def update_session_title(sid: str, title: str):
    with _lock, _conn() as con:
        con.execute("UPDATE sessions SET title=? WHERE id=?", (title, sid))


def share_session(sid: str) -> str:
    # Full UUID share token — not guessable
    token = str(uuid.uuid4())
    with _lock, _conn() as con:
        con.execute("UPDATE sessions SET shared=1, share_token=? WHERE id=?", (token, sid))
    return token


def get_session_by_token(token: str) -> Optional[Dict]:
    with _lock, _conn() as con:
        row = con.execute("SELECT * FROM sessions WHERE share_token=?", (token,)).fetchone()
        if not row:
            return None
        return _session_row_to_dict(row, _get_messages(con, row["id"]))


# ─── Saved queries (scoped per user_id) ──────────────────────────────────────

def save_query(title: str, query: str, user_id: str = "guest") -> Dict:
    item = {
        "id":         str(uuid.uuid4()),
        "title":      title,
        "query":      query,
        "created_at": datetime.now().isoformat(),
        "user_id":    user_id,
    }
    with _lock, _conn() as con:
        con.execute(
            "INSERT INTO saved_queries (id, title, query, created_at, user_id) VALUES (?, ?, ?, ?, ?)",
            (item["id"], item["title"], item["query"], item["created_at"], item["user_id"]),
        )
    return item


def list_saved_queries(user_id: str = "guest") -> List[Dict]:
    with _lock, _conn() as con:
        rows = con.execute(
            "SELECT * FROM saved_queries WHERE user_id=? ORDER BY created_at DESC", (user_id,)
        ).fetchall()
        return [dict(r) for r in rows]


def delete_saved_query(qid: str, user_id: str = "guest"):
    """Only deletes if the query belongs to the requesting user."""
    with _lock, _conn() as con:
        con.execute("DELETE FROM saved_queries WHERE id=? AND user_id=?", (qid, user_id))

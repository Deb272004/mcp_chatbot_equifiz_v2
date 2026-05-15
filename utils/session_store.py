

"""
PostgreSQL-backed session store.
Replaces SQLite for better concurrency and scalability.
Requires: pip install psycopg2-binary
"""
import os
import uuid
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from typing import List, Dict, Optional

# Database Configuration (Environment variables preferred)
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB   = os.environ.get("POSTGRES_DB", "equifiz")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "1234")

def _get_connection():
    """Creates a new PostgreSQL connection with a RealDictCursor."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        cursor_factory=RealDictCursor
    )

def _init_db():
    """Initializes tables using PostgreSQL schema."""
    with _get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    id          TEXT PRIMARY KEY,
                    title       TEXT NOT NULL DEFAULT '',
                    created_at  TIMESTAMP WITH TIME ZONE NOT NULL,
                    shared      BOOLEAN NOT NULL DEFAULT FALSE,
                    share_token TEXT,
                    user_id     TEXT NOT NULL DEFAULT 'guest'
                );
                CREATE TABLE IF NOT EXISTS messages (
                    id         SERIAL PRIMARY KEY,
                    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
                    role       TEXT NOT NULL,
                    content    TEXT NOT NULL,
                    ts         TIMESTAMP WITH TIME ZONE NOT NULL
                );
                CREATE TABLE IF NOT EXISTS saved_queries (
                    id         TEXT PRIMARY KEY,
                    title      TEXT NOT NULL,
                    query      TEXT NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    user_id    TEXT NOT NULL DEFAULT 'guest'
                );
                CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);
                CREATE INDEX IF NOT EXISTS idx_saved_queries_user_id ON saved_queries(user_id);
            """)
        conn.commit()

# Run migrations on startup
_init_db()

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _session_row_to_dict(row, messages: List[Dict]) -> Dict:
    """Standardizes row output for JSON responses."""
    return {
        "id":          row["id"],
        "title":       row["title"],
        "created_at":  row["created_at"].isoformat() if isinstance(row["created_at"], datetime) else row["created_at"],
        "shared":      row["shared"],
        "share_token": row["share_token"],
        "messages":    messages,
    }

def _get_messages(cur, sid: str) -> List[Dict]:
    """Fetches messages for a specific session using an active cursor."""
    cur.execute(
        "SELECT role, content, ts FROM messages WHERE session_id=%s ORDER BY id", (sid,)
    )
    rows = cur.fetchall()
    return [{
        "role": r["role"], 
        "content": r["content"], 
        "ts": r["ts"].isoformat() if isinstance(r["ts"], datetime) else r["ts"]
    } for r in rows]

# ─── Sessions ─────────────────────────────────────────────────────────────────

def new_session(title: str = "", user_id: str = "guest") -> str:
    sid = str(uuid.uuid4())
    now = datetime.now()
    with _get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO sessions (id, title, created_at, user_id) VALUES (%s, %s, %s, %s)",
                (sid, title or f"Session {sid[:8]}", now, user_id),
            )
        conn.commit()
    return sid

def get_session(sid: str) -> Optional[Dict]:
    with _get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM sessions WHERE id=%s", (sid,))
            row = cur.fetchone()
            if not row:
                return None
            return _session_row_to_dict(row, _get_messages(cur, sid))

def get_session_for_user(sid: str, user_id: str) -> Optional[Dict]:
    with _get_connection() as conn:
        with conn.cursor() as cur:
            uid = user_id or "guest"
            cur.execute(
                "SELECT * FROM sessions WHERE id=%s AND user_id=%s", (sid, uid)
            )
            row = cur.fetchone()
            if not row:
                return None
            return _session_row_to_dict(row, _get_messages(cur, sid))

def list_sessions(user_id: str = "") -> List[Dict]:
    with _get_connection() as conn:
        with conn.cursor() as cur:
            uid = user_id or "guest"
            cur.execute(
                "SELECT * FROM sessions WHERE user_id=%s ORDER BY created_at DESC", (uid,)
            )
            rows = cur.fetchall()
            return [_session_row_to_dict(r, _get_messages(cur, r["id"])) for r in rows]

def add_message(sid: str, role: str, content: str):
    now = datetime.now()
    with _get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO messages (session_id, role, content, ts) VALUES (%s, %s, %s, %s)",
                (sid, role, content, now),
            )
        conn.commit()

def delete_session(sid: str):
    with _get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM sessions WHERE id=%s", (sid,))
        conn.commit()

def update_session_title(sid: str, title: str):
    with _get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE sessions SET title=%s WHERE id=%s", (title, sid))
        conn.commit()

def share_session(sid: str) -> str:
    token = str(uuid.uuid4())
    with _get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE sessions SET shared=TRUE, share_token=%s WHERE id=%s", (token, sid))
        conn.commit()
    return token

def get_session_by_token(token: str) -> Optional[Dict]:
    with _get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM sessions WHERE share_token=%s", (token,))
            row = cur.fetchone()
            if not row:
                return None
            return _session_row_to_dict(row, _get_messages(cur, row["id"]))

# ─── Saved queries ───────────────────────────────────────────────────────────

def save_query(title: str, query: str, user_id: str = "guest") -> Dict:
    qid = str(uuid.uuid4())
    now = datetime.now()
    item = {
        "id":         qid,
        "title":      title,
        "query":      query,
        "created_at": now,
        "user_id":    user_id,
    }
    with _get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO saved_queries (id, title, query, created_at, user_id) VALUES (%s, %s, %s, %s, %s)",
                (item["id"], item["title"], item["query"], item["created_at"], item["user_id"]),
            )
        conn.commit()
    
    # Format for JSON response
    item["created_at"] = item["created_at"].isoformat()
    return item

def list_saved_queries(user_id: str = "guest") -> List[Dict]:
    with _get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, title, query, created_at, user_id FROM saved_queries WHERE user_id=%s ORDER BY created_at DESC", 
                (user_id,)
            )
            rows = cur.fetchall()
            results = []
            for r in rows:
                d = dict(r)
                if isinstance(d["created_at"], datetime):
                    d["created_at"] = d["created_at"].isoformat()
                results.append(d)
            return results

def delete_saved_query(qid: str, user_id: str = "guest"):
    with _get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM saved_queries WHERE id=%s AND user_id=%s", (qid, user_id))
        conn.commit()
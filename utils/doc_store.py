"""
Persistent doc store — replaces the in-memory dict in main.py.
Stores uploaded document chunks in SQLite so they survive restarts.
Each session can have at most one active document at a time.
"""
import sqlite3
import json
import os
import threading
from typing import List, Optional

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


def _init():
    with _lock, _conn() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS doc_store (
                session_id TEXT PRIMARY KEY,
                filename   TEXT NOT NULL,
                chunks_json TEXT NOT NULL
            )
        """)


_init()


def set_doc(session_id: str, filename: str, chunks: List[str]):
    with _lock, _conn() as con:
        con.execute(
            "INSERT OR REPLACE INTO doc_store (session_id, filename, chunks_json) VALUES (?, ?, ?)",
            (session_id, filename, json.dumps(chunks)),
        )


def get_doc(session_id: str) -> Optional[List[str]]:
    with _lock, _conn() as con:
        row = con.execute(
            "SELECT chunks_json FROM doc_store WHERE session_id=?", (session_id,)
        ).fetchone()
    return json.loads(row["chunks_json"]) if row else None


def delete_doc(session_id: str):
    with _lock, _conn() as con:
        con.execute("DELETE FROM doc_store WHERE session_id=?", (session_id,))

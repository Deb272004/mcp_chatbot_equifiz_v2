

"""
PostgreSQL-backed doc store.
Replaces SQLite to consolidate data into the equifiz database.
Each session can have at most one active document at a time.
"""
import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Optional

# Database Configuration
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB   = os.environ.get("POSTGRES_DB", "equifiz")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "1234")

def _get_connection():
    """Creates a new PostgreSQL connection."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        cursor_factory=RealDictCursor
    )

def _init():
    """Initializes the doc_store table in PostgreSQL."""
    with _get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS doc_store (
                    session_id TEXT PRIMARY KEY REFERENCES sessions(id) ON DELETE CASCADE,
                    filename   TEXT NOT NULL,
                    chunks_json TEXT NOT NULL
                )
            """)
        conn.commit()

# Initialize on module load
_init()

def set_doc(session_id: str, filename: str, chunks: List[str]):
    """Stores or updates the document for a session."""
    with _get_connection() as conn:
        with conn.cursor() as cur:
            # PostgreSQL equivalent of "INSERT OR REPLACE"
            cur.execute("""
                INSERT INTO doc_store (session_id, filename, chunks_json) 
                VALUES (%s, %s, %s)
                ON CONFLICT (session_id) 
                DO UPDATE SET filename = EXCLUDED.filename, chunks_json = EXCLUDED.chunks_json
            """, (session_id, filename, json.dumps(chunks)))
        conn.commit()

def get_doc(session_id: str) -> Optional[List[str]]:
    """Retrieves document chunks for a session."""
    with _get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT chunks_json FROM doc_store WHERE session_id=%s", (session_id,)
            )
            row = cur.fetchone()
    return json.loads(row["chunks_json"]) if row else None

def delete_doc(session_id: str):
    """Removes a document from the store."""
    with _get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM doc_store WHERE session_id=%s", (session_id,))
        conn.commit()
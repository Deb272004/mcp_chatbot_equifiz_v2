"""
db.py – PostgreSQL company-master cache (local mirror of EQUIFIZ CompanyMaster)
Provides fast co_code lookups via BSE code, NSE symbol, or fuzzy company name.

NOTE: Uses the existing 'companies' table — no sync required.
Schema expected:
    companies (
        co_code, bsecode, nsesymbol, companyname, companyshortname,
        categoryname, isin, bsegroup, mcaptype, sectorcode, sectorname,
        industrycode, industryname, bselistedflag, nselistedflag,
        displaytype, synced_at
    )
"""

from __future__ import annotations
import logging
from typing import Optional

import psycopg2
import psycopg2.extras
from rapidfuzz import process, fuzz

from config.config import DB_CONFIG

logger = logging.getLogger(__name__)

TABLE = "companies"  # ← existing table name in your PG database


# ── Connection helper ────────────────────────────────────────────────────────

def _get_conn():
    return psycopg2.connect(**DB_CONFIG)


# ── Schema check (read-only verification, no DDL) ────────────────────────────

def init_db():
    """Verify the companies table is reachable. Does NOT create or alter it."""
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {TABLE};")
            count = cur.fetchone()[0]
    logger.info(f"DB ready. '{TABLE}' table has {count:,} rows.")
    return count


# ── Lookup helpers ────────────────────────────────────────────────────────────

def _row_to_dict(cur) -> Optional[dict]:
    cols = [d[0] for d in cur.description]
    row  = cur.fetchone()
    return dict(zip(cols, row)) if row else None


def lookup_by_co_code(co_code: int) -> Optional[dict]:
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {TABLE} WHERE co_code = %s", (co_code,))
            return _row_to_dict(cur)


def lookup_by_nse_symbol(symbol: str) -> Optional[dict]:
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM {TABLE} WHERE UPPER(nsesymbol) = %s",
                (symbol.upper(),),
            )
            return _row_to_dict(cur)


def lookup_by_bse_code(bsecode: str) -> Optional[dict]:
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM {TABLE} WHERE bsecode = %s",
                (str(bsecode),),
            )
            return _row_to_dict(cur)


def fuzzy_search_company(query: str, limit: int = 5) -> list[dict]:
    """Return top-N matches by fuzzy company name search."""
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"SELECT co_code, companyname, companyshortname, nsesymbol, bsecode "
                f"FROM {TABLE}"
            )
            all_rows = cur.fetchall()

    if not all_rows:
        return []

    names   = [r["companyname"] or "" for r in all_rows]
    matches = process.extract(query, names, scorer=fuzz.WRatio, limit=limit)

    results = []
    for _name, score, idx in matches:
        if score >= 60:
            results.append({**dict(all_rows[idx]), "match_score": score})
    return results


def get_all_companies() -> list[dict]:
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"SELECT co_code, companyname, companyshortname, nsesymbol, "
                f"bsecode, sectorname, mcaptype FROM {TABLE}"
            )
            return [dict(r) for r in cur.fetchall()]

####################################################################### 140 mcp tools ########################################################################
"""
mf_equifiz_server.py — Equifiz Unified MCP Server (Stocks + Mutual Funds)

Design principles:
  - Every tool returns ONLY the fields the LLM needs to answer the user.
  - Internal IDs (mf_cocode, mf_schcode, co_code, etc.) are plumbing only.
  - Resolvers translate human names → internal codes before any data tool.
  - Stock tools from equifiz_server.py are fully integrated here.
  - Company Fundamental, Financial Statement, and new APIs added from API spec.
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Optional

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv
from fastmcp import FastMCP
from rapidfuzz import fuzz

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("equifiz")

# ── Config ──────────────────────────────────────────────────────────────────────

FUZZY_THRESHOLD = int(os.getenv("FUZZY_THRESHOLD", "85"))
BASE_URL        = os.getenv("EQUIFIZ_API_BASE_URL", "https://equifizapis.cmots.com/api")
TOKEN           = os.getenv("EQUIFIZ_TOKEN", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1bmlxdWVfbmFtZSI6ImVxdWlmaXphcGlzIiwicm9sZSI6IkFkbWluIiwibmJmIjoxNzc2OTMyODcyLCJleHAiOjE4MDkxNjAwNzIsImlhdCI6MTc3NjkzMjg3MiwiaXNzIjoiaHR0cDovL2xvY2FsaG9zdDo1MDE5MSIsImF1ZCI6Imh0dHA6Ly9sb2NhbGhvc3Q6NTAxOTEifQ.lz6do_yCsDQTFz5E-qi4w825YvjFY7lWv_l1qWG4W9I")

DB = {
    "host":     os.getenv("POSTGRES_HOST",     "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname":   os.getenv("POSTGRES_DB",       "equifiz"),
    "user":     os.getenv("POSTGRES_USER",     "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "1234"),
}


# ── Alias maps ──────────────────────────────────────────────────────────────────

AMC_ALIAS_MAP: dict[str, str] = {
    "hdfc mf": "HDFC Mutual Fund", "hdfc": "HDFC Mutual Fund",
    "sbi mf": "SBI Mutual Fund", "sbi": "SBI Mutual Fund",
    "icici pru": "ICICI Prudential Mutual Fund",
    "icici prudential": "ICICI Prudential Mutual Fund",
    "icici": "ICICI Prudential Mutual Fund",
    "axis mf": "Axis Mutual Fund", "axis": "Axis Mutual Fund",
    "mirae": "Mirae Asset Mutual Fund", "mirae asset": "Mirae Asset Mutual Fund",
    "nippon": "Nippon India Mutual Fund", "nippon india": "Nippon India Mutual Fund",
    "reliance mf": "Nippon India Mutual Fund",
    "kotak": "Kotak Mahindra Mutual Fund", "kotak mahindra": "Kotak Mahindra Mutual Fund",
    "dsp": "DSP Mutual Fund",
    "franklin": "Franklin Templeton Mutual Fund",
    "franklin templeton": "Franklin Templeton Mutual Fund",
    "aditya birla": "Aditya Birla Sun Life Mutual Fund",
    "absl": "Aditya Birla Sun Life Mutual Fund",
    "birla sun life": "Aditya Birla Sun Life Mutual Fund",
    "uti": "UTI Mutual Fund",
    "ppfas": "Parag Parikh Financial Advisory Services",
    "parag parikh": "Parag Parikh Financial Advisory Services",
    "quant": "Quant Mutual Fund", "tata": "Tata Mutual Fund",
    "invesco": "Invesco Mutual Fund",
    "motilal": "Motilal Oswal Mutual Fund", "motilal oswal": "Motilal Oswal Mutual Fund",
    "canara": "Canara Robeco Mutual Fund", "canara robeco": "Canara Robeco Mutual Fund",
    "edelweiss": "Edelweiss Mutual Fund", "bandhan": "Bandhan Mutual Fund",
    "navi": "Navi Mutual Fund", "pgim": "PGIM India Mutual Fund",
    "whiteoak": "WhiteOak Capital Mutual Fund", "white oak": "WhiteOak Capital Mutual Fund",
    "360 one": "360 ONE Mutual Fund", "360one": "360 ONE Mutual Fund",
}

SCHEME_ALIAS_MAP: dict[str, str] = {
    "ppfas flexi": "Parag Parikh Flexi Cap Fund",
    "parag parikh flexi": "Parag Parikh Flexi Cap Fund",
    "sbi bluechip": "SBI Bluechip Fund",
    "hdfc flexi": "HDFC Flexi Cap Fund",
    "hdfc top 100": "HDFC Top 100 Fund",
    "icici bluechip": "ICICI Prudential Bluechip Fund",
    "mirae bluechip": "Mirae Asset Large Cap Fund",
    "mirae emerging": "Mirae Asset Emerging Bluechip Fund",
    "mirae tax saver": "Mirae Asset Tax Saver Fund",
    "axis bluechip": "Axis Bluechip Fund",
    "axis small cap": "Axis Small Cap Fund",
    "nippon small cap": "Nippon India Small Cap Fund",
    "sbi small cap": "SBI Small Cap Fund",
    "kotak flexi": "Kotak Flexi Cap Fund",
    "dsp flexi": "DSP Flexi Cap Fund",
    "quant small cap": "Quant Small Cap Fund",
    "quant flexi": "Quant Flexi Cap Fund",
    "motilal midcap": "Motilal Oswal Midcap Fund",
}

BRAND_MAP: dict[str, str] = {
    "maggi": "Nestle India", "jio": "Reliance Industries",
    "swiggy": "Bundl Technologies", "zomato": "Zomato",
    "paytm": "One97 Communications", "nykaa": "FSN E-Commerce Ventures",
    "dmart": "Avenue Supermarts", "airtel": "Bharti Airtel",
    "vi": "Vodafone Idea", "idea": "Vodafone Idea",
    "tcs": "Tata Consultancy Services", "infy": "Infosys",
    "ril": "Reliance Industries", "reliance": "Reliance Industries",
    "hdfc": "HDFC Bank", "icici": "ICICI Bank", "sbi": "State Bank of India",
    "ongc": "Oil and Natural Gas Corporation", "ntpc": "NTPC",
    "maruti": "Maruti Suzuki India", "hero": "Hero MotoCorp",
    "bajaj": "Bajaj Auto", "tvs": "TVS Motor Company",
    "asian paints": "Asian Paints", "titan": "Titan Company",
    "policybazaar": "PB Fintech", "hcl": "HCL Technologies",
}

VALID_EXCHANGES  = {"BSE", "NSE"}
EXCHANGE_ALIASES = {"BOMBAY": "BSE", "NATIONAL": "NSE", "B": "BSE", "N": "NSE"}


# ── HTTP session ────────────────────────────────────────────────────────────────

_session = requests.Session()
_session.headers.update({
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type":  "application/json",
})

# ── DB caches ───────────────────────────────────────────────────────────────────

_company_cache:    list[dict] = []
_mf_amc_cache:     list[dict] = []
_mf_scheme_cache:  list[dict] = []
_group_cache:      list[dict] = []
_index_cache: list[dict] = []
_etf_master_cache: list[dict] = []  

def _db():
    return psycopg2.connect(**DB)


def _load_company_cache() -> list[dict]:
    global _company_cache
    if _company_cache:
        return _company_cache
    try:
        conn = _db()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT co_code, companyname, nsesymbol, bsecode, "
                "sectorname, industryname, isin FROM companies"
            )
            _company_cache = [dict(r) for r in cur.fetchall()]
        conn.close()
    except Exception as e:
        logger.error("Company cache load failed: %s", e)
    return _company_cache


def _load_mf_amc_cache() -> list[dict]:
    global _mf_amc_cache
    if _mf_amc_cache:
        return _mf_amc_cache
    try:
        conn = _db()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT mf_cocode, lname, nameamc, fund_type FROM fund_house")
            _mf_amc_cache = [dict(r) for r in cur.fetchall()]
        conn.close()
    except Exception as e:
        logger.error("MF AMC cache load failed: %s", e)
    return _mf_amc_cache


def _load_mf_scheme_cache() -> list[dict]:
    global _mf_scheme_cache
    if _mf_scheme_cache:
        return _mf_scheme_cache
    try:
        conn = _db()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT mf_schcode, mf_cocode, sch_name, category, isin, amficode "
                "FROM scheme_master"
            )
            _mf_scheme_cache = [dict(r) for r in cur.fetchall()]
        conn.close()
    except Exception as e:
        logger.error("MF Scheme cache load failed: %s", e)
    return _mf_scheme_cache


def _load_group_cache() -> list[dict]:
    global _group_cache
    if _group_cache:
        return _group_cache
    try:
        conn = _db()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT group_name, exchange FROM group_master")
            _group_cache = [dict(r) for r in cur.fetchall()]
        conn.close()
    except Exception as e:
        logger.error("Group cache load failed: %s", e)
    return _group_cache


def _by_symbol(sym: str) -> dict | None:
    try:
        conn = _db()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM companies WHERE UPPER(nsesymbol) = %s", (sym.upper().strip(),))
            row = cur.fetchone()
        conn.close()
        return dict(row) if row else None
    except Exception as e:
        logger.error("Symbol lookup failed: %s", e)
        return None


def _by_co_code(co_code: int) -> dict | None:
    try:
        conn = _db()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM companies WHERE co_code = %s", (co_code,))
            row = cur.fetchone()
        conn.close()
        return dict(row) if row else None
    except Exception as e:
        logger.error("co_code lookup failed: %s", e)
        return None


# ── Query cleaning ──────────────────────────────────────────────────────────────

_STOCK_NOISE = [
    r"^(what is|what are|what's|give me|show me|tell me about|find|get|check|analyse|analyze)\s+(the\s+)?",
    r"^(pe ratio|eps|mcap|revenue|profit|share price|price|financials|details?)\s+(of|for|about)\s+",
    r"^(details?\s+(of|about|for)\s+)",
    r"^(how is|is|are)\s+",
]
_STOCK_NOISE_WORDS = r"\b(company|co|corp|ltd|limited|inc|stock|share|equity|nse|bse|listed|group)\b"

_MF_NOISE = [
    r"^(what is|what are|what's|give me|show me|tell me|find|get|check|fetch)\s+(the\s+)?",
    r"^(nav|returns?|aum|performance|details?|info|information)\s+(of|for|about)\s+",
    r"^(details?\s+(of|about|for)\s+)",
    r"\b(fund|mutual fund|scheme|plan|option|growth|idcw|direct|regular|dividend)\b",
]


def _clean_stock_query(query: str) -> str:
    q = query.strip().rstrip("?!.")
    for p in _STOCK_NOISE:
        q = re.sub(p, "", q, flags=re.IGNORECASE).strip()
    q = re.sub(_STOCK_NOISE_WORDS, "", q, flags=re.IGNORECASE)
    return re.sub(r"\s{2,}", " ", q).strip() or query


def _clean_mf_query(query: str) -> str:
    q = query.strip().rstrip("?!.")
    for p in _MF_NOISE:
        q = re.sub(p, " ", q, flags=re.IGNORECASE).strip()
    return re.sub(r"\s{2,}", " ", q).strip() or query


# ── HTTP & field helpers ────────────────────────────────────────────────────────

def _get(url: str, label: str = "") -> tuple[list | dict | None, str | None]:
    try:
        r = _session.get(url, timeout=15)
        r.raise_for_status()
        return r.json(), None
    except requests.HTTPError as e:
        return None, f"API error {e.response.status_code} for {label or url}: {e.response.text[:200]}"
    except Exception as e:
        return None, f"Request failed ({label or url}): {e}"


def _pick(record: dict, fields: list[str]) -> dict:
    low = {k.lower(): v for k, v in record.items()}
    out = {}
    for f in fields:
        v = low.get(f.lower())
        if v is not None and v != "":
            out[f] = v
    return out


def _rows(data) -> list[dict]:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("data", [data])
    return []


def _as_list(data) -> list[dict]:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    return []


def _require_int(value, param_name: str, resolver: str) -> tuple[int | None, str | None]:
    if value is None:
        return None, f"'{param_name}' is required. Call {resolver} first to get it."
    try:
        return int(value), None
    except (TypeError, ValueError):
        return None, f"'{param_name}' must be an integer, got: {value!r}"


def _normalise_exchange(exchange: str) -> str:
    resolved = EXCHANGE_ALIASES.get(exchange.strip().upper(), exchange.strip().upper())
    if resolved not in VALID_EXCHANGES:
        raise ValueError(f"Invalid exchange '{exchange}'. Use 'NSE' or 'BSE'.")
    return resolved


def _normalise_period(period: str) -> str:
    p = period.strip().upper()
    return "M" if p in ("M", "MONTH", "MONTHS") else "Y"


def _fmt_company(row: dict) -> str:
    return "\n".join([
        f"Company    : {row.get('companyname', 'N/A')}",
        f"NSE Symbol : {row.get('nsesymbol', 'N/A')}",
        f"BSE Code   : {row.get('bsecode', 'N/A')}",
        f"co_code    : {row.get('co_code', 'N/A')}",
        f"Sector     : {row.get('sectorname', 'N/A')}",
        f"Industry   : {row.get('industryname', 'N/A')}",
        f"ISIN       : {row.get('isin', 'N/A')}",
    ])


def _fmt(data, indent="  ") -> str:
    lines = []
    if isinstance(data, dict):
        for k, v in data.items():
            if isinstance(v, (dict, list)):
                lines.append(f"{indent}{k}:")
                lines.append(_fmt(v, indent + "  "))
            else:
                lines.append(f"{indent}{k:<30}: {v}")
    elif isinstance(data, list):
        for i, item in enumerate(data):
            lines.append(f"{indent}[{i+1}]")
            lines.append(_fmt(item, indent + "  "))
    else:
        lines.append(f"{indent}{data}")
    return "\n".join(lines)


# ── Fuzzy matchers ──────────────────────────────────────────────────────────────

def _fuzzy_company(query: str, limit: int = 5, threshold: int = FUZZY_THRESHOLD) -> list[dict]:
    scored = []
    q_lower, q_upper = query.lower(), query.upper()
    for co in _load_company_cache():
        score = max(
            fuzz.token_set_ratio(q_lower, (co.get("companyname") or "").lower()),
            fuzz.ratio(q_upper, (co.get("nsesymbol") or "").upper()),
        )
        if score >= threshold:
            scored.append((co, score))
    scored.sort(key=lambda x: x[1], reverse=True)
    return [co for co, _ in scored[:limit]]


def _fuzzy_mf_amc(query: str, limit: int = 5, threshold: int = FUZZY_THRESHOLD) -> list[dict]:
    q = query.lower().strip()
    scored = []
    for amc in _load_mf_amc_cache():
        nl = (amc.get("lname") or "").lower()
        ns = (amc.get("nameamc") or "").lower()
        score = max(
            fuzz.token_set_ratio(q, nl), fuzz.token_set_ratio(q, ns),
            fuzz.partial_ratio(q, nl),   fuzz.partial_ratio(q, ns),
        )
        if score >= threshold:
            scored.append((amc, score))
    scored.sort(key=lambda x: x[1], reverse=True)
    return [a for a, _ in scored[:limit]]


def _fuzzy_mf_scheme(query: str, mf_cocode: int | None = None,
                     limit: int = 5, threshold: int = FUZZY_THRESHOLD) -> list[dict]:
    q = query.lower().strip()
    scored = []
    for sch in _load_mf_scheme_cache():
        if mf_cocode and sch.get("mf_cocode") != mf_cocode:
            continue
        name = (sch.get("sch_name") or "").lower()
        score = max(fuzz.token_set_ratio(q, name), fuzz.partial_ratio(q, name))
        if score >= threshold:
            scored.append((sch, score))
    scored.sort(key=lambda x: x[1], reverse=True)
    return [s for s, _ in scored[:limit]]


def _fuzzy_group(query: str, exchange: str | None = None,
                 limit: int = 1, threshold: int = 55) -> list[dict]:
    scored: list[tuple[dict, int]] = []
    q = query.strip().upper()
    q_lower = q.lower()
    query_acronym = "".join(w[0] for w in q.split() if w)
    for grp in _load_group_cache():
        if exchange and (grp.get("exchange") or "").upper() != exchange.upper():
            continue
        g_name    = (grp.get("group_name") or "").upper()
        g_display = (grp.get("display_name") or "").lower()
        if q == g_name:
            scored.append((grp, 100))
            continue
        score_name = max(
            fuzz.token_set_ratio(q_lower, g_name.lower()),
            fuzz.ratio(q, g_name),
            fuzz.partial_ratio(q, g_name),
        )
        score_display = fuzz.token_set_ratio(q_lower, g_display)
        g_acronym = "".join(w[0] for w in g_name.split() if w)
        g_display_acronym = "".join(w[0].upper() for w in g_display.split() if w)
        score_acronym = max(
            fuzz.ratio(query_acronym, g_acronym),
            fuzz.ratio(query_acronym, g_display_acronym),
            100 if g_name.startswith(q) and len(q) >= 3 else 0,
        )
        final = max(score_name, score_display, score_acronym)
        if final >= threshold:
            scored.append((grp, final))
    scored.sort(key=lambda x: x[1], reverse=True)
    return [g for g, _ in scored[:limit]]


def _resolve_group(query: str, exchange: str | None = None) -> str | None:
    results = _fuzzy_group(query, exchange=exchange, limit=1)
    if not results:
        logger.warning("Could not resolve group from query: %r", query)
        return None
    return results[0]["group_name"]

################# etf resolve ##########################

def _load_etf_master_cache() -> list[dict]:
    global _etf_master_cache
    if _etf_master_cache:
        return _etf_master_cache
    try:
        conn = _db()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT isin, etfname FROM etf_master")
            _etf_master_cache = [dict(r) for r in cur.fetchall()]
        conn.close()
    except Exception as e:
        logger.error("ETF master cache load failed: %s", e)
    return _etf_master_cache


def _fuzzy_etf(
    query: str,
    limit: int = 5,
    threshold: int = FUZZY_THRESHOLD,
) -> list[dict]:
    q       = query.strip()
    q_lower = q.lower()
    q_upper = q.upper()
    scored: list[tuple[dict, int]] = []

    for etf in _load_etf_master_cache():
        isin = (etf.get("isin")    or "").upper()
        name = (etf.get("etfname") or "").lower()

        if q_upper == isin:
            scored.append((etf, 100))
            continue

        score_name = max(
            fuzz.token_set_ratio(q_lower, name),
            fuzz.partial_ratio(q_lower, name),
        )
        score_isin = fuzz.ratio(q_upper, isin)

        final = max(score_name, score_isin)
        if final >= threshold:
            scored.append((etf, final))

    scored.sort(key=lambda x: x[1], reverse=True)
    return [e for e, _ in scored[:limit]]


def _resolve_etf_isin(query: str) -> str | None:
    results = _fuzzy_etf(query, limit=1)
    if not results:
        logger.warning("Could not resolve ETF ISIN from query: %r", query)
        return None
    return results[0]["isin"]


#################################### index code resolve #######################################



def _load_index_cache() -> list[dict]:
    global _index_cache
    if _index_cache:
        return _index_cache
    try:
        conn = _db()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT indexcode, group_name FROM group_master")
            _index_cache = [dict(r) for r in cur.fetchall()]
        conn.close()
    except Exception as e:
        logger.error("Index cache load failed: %s", e)
    return _index_cache


def _fuzzy_index(
    query: str,
    limit: int = 5,
    threshold: int = FUZZY_THRESHOLD,
) -> list[dict]:
    q       = query.strip()
    q_lower = q.lower()
    scored: list[tuple[dict, int]] = []

    for idx in _load_index_cache():
        name = (idx.get("group_name") or "").lower()

        if q_lower == name:
            scored.append((idx, 100))
            continue

        score = max(
            fuzz.token_set_ratio(q_lower, name),
            fuzz.partial_ratio(q_lower, name),
        )
        if score >= threshold:
            scored.append((idx, score))

    scored.sort(key=lambda x: x[1], reverse=True)
    return [i for i, _ in scored[:limit]]


def _resolve_index_code(query: str) -> int | None:
    results = _fuzzy_index(query, limit=1)
    if not results:
        logger.warning("Could not resolve index code from query: %r", query)
        return None
    return results[0]["indexcode"]


# ── API endpoint map ────────────────────────────────────────────────────────────

EP = {
    # ── Stock: Master ──────────────────────────────────────────────────────────
    "index_list":           f"{BASE_URL}/IndexList",
    "sector_wise_comp":     f"{BASE_URL}/SectorWiseComp/{{sector_code}}",
    "index_wise_comp":      f"{BASE_URL}/IndexWiseComp/{{index_code}}",
    "exchange_holidays":    f"{BASE_URL}/ExchangeHolidays/{{ex}}",
    "result_declarations":  f"{BASE_URL}/ResultDataDeclarations/{{date}}",
    "annual_declarations":  f"{BASE_URL}/AnnualDataDeclarations/{{date}}",
    "results_today":        f"{BASE_URL}/Today-Results",
    # ── Stock: Company Fundamental ─────────────────────────────────────────────
    "company_profile":      f"{BASE_URL}/CompanyProfile/{{co_code}}",
    "comp_background":      f"{BASE_URL}/CompBackground/{{co_code}}",
    "board_of_directors":   f"{BASE_URL}/BoardOfDirectors/{{co_code}}",
    "bankers":              f"{BASE_URL}/Bankers/{{co_code}}",
    "biodata":              f"{BASE_URL}/Biodata/{{co_code}}",
    "subsidiaries":         f"{BASE_URL}/Subsidiaries_JVs_Collaborations/{{co_code}}",
    "chrono_history":       f"{BASE_URL}/ChronologicalHistory/{{co_code}}",
    "company_history":      f"{BASE_URL}/CompanyHistory/{{co_code}}",
    "employee_count":       f"{BASE_URL}/EmployeeCount/{{co_code}}",
    "capital_structure":    f"{BASE_URL}/capital-structure/{{co_code}}",
    "pledge_shares":        f"{BASE_URL}/Pledgesharesdetails/{{co_code}}",
    "substantial_acq":      f"{BASE_URL}/SubstantialAcquisition/{{co_code}}",
    "segment_geography":    f"{BASE_URL}/SegmentGeographyWiseNew/{{co_code}}/{{report_type}}",
    "segment_product":      f"{BASE_URL}/SegmentProductWiseNew/{{co_code}}/{{report_type}}",
    "r_and_d":              f"{BASE_URL}/R_and_D/{{co_code}}",
    "finished_products":    f"{BASE_URL}/FinishedProducts/{{co_code}}",
    "raw_materials":        f"{BASE_URL}/RawMaterials/{{co_code}}",
    "related_party_transactions" :f"{BASE_URL}/Related_Party_transaction/{{co_code}}",
    # ── Stock: Financials ──────────────────────────────────────────────────────
    "quarterly_results":    f"{BASE_URL}/QuarterlyResults/{{co_code}}/{{t}}",
    "profit_loss":          f"{BASE_URL}/ProftandLoss/{{co_code}}/{{t}}",
    "balance_sheet":        f"{BASE_URL}/BalanceSheet/{{co_code}}/{{t}}",
    "cash_flow":            f"{BASE_URL}/CashFlow/{{co_code}}/{{t}}",
    "shareholding_detailed":f"{BASE_URL}/ShareHoldingPatternDetailed/{{co_code}}",
    "shareholding_1pct":    f"{BASE_URL}/ShareholdingMorethanOnePercent/{{co_code}}",
    "q_trend_revenue":      f"{BASE_URL}/QuarterlyTrendsrevenue/{{co_code}}",
    "half_yearly_results": f"{BASE_URL}//Half-Yearly-Results/{{co_code}}/{{t}}",
    "nine_months_results" : f"{BASE_URL}/Nine-Month-Result/{{co_code}}/{{t}}",
    "get_yearly_results" : f"{BASE_URL}/Yearly-Results/{{co_code}}/{{t}}",
    "get_quarterly_balance_sheet" :f"{BASE_URL}/QuarterlyResults-BalanceSheet/{{co_code}}/{{t}}",
    "get_half_yearly_balance_sheet" :f"{BASE_URL}/Results-BalanceSheet-Half-yearly/{{co_code}}/{{t}}",
    "get_annual_balance_sheet" :f"{BASE_URL}/Results-BalanceSheet-Yearly/{{co_code}}/{{t}}",
    "get_ttm_growth_trends" :f"{BASE_URL}/TTMPATNetSales/{{co_code}}/{{t}}",
    "get_quarterly_revenue_trends" :f"{BASE_URL}/QuarterlyTrendsrevenue/{{co_code}}",
    "get_quarterly_ebitda_trends" :f"{BASE_URL}/QuarterlyTrendEBITDA/{{co_code}}",
    "get_quarterly_ebit_trends" :f"{BASE_URL}/QuarterlyTrendEBIT/{{co_code}}",
    "get_growth_data_quarterly" :f"{BASE_URL}/GrowthDataQuarterly/{{co_code}}/{{t}}",
    "get_growth_data_yearly" :f"{BASE_URL}/GrowthDataYearly/{{co_code}}/{{t}}",

    # ── Stock: Ratios ──────────────────────────────────────────────────────────
    "key_ratios":           f"{BASE_URL}/KeyFinancialRatios/{{co_code}}/{{t}}",
    "daily_ratios":         f"{BASE_URL}/DailyRatios/{{co_code}}/{{t}}",
    "margin_ratios":        f"{BASE_URL}/MarginRatios/{{co_code}}/{{t}}",
    "valuation_ratios":     f"{BASE_URL}/ValuationRatios/{{co_code}}/{{t}}",
    "all_basic_ratios":     f"{BASE_URL}/Allbasicratio/{{co_code}}/{{t}}",
    "return_ratios":        f"{BASE_URL}/RatiosReturn/{{co_code}}/{{t}}",
    "growth_ratio":         f"{BASE_URL}/GrowthRatio/{{co_code}}/{{t}}",
    "efficiency_ratios":    f"{BASE_URL}/EfficiencyRatios/{{co_code}}/{{t}}",
    "liquidity_ratios":     f"{BASE_URL}/LiquidityRatios/{{co_code}}/{{t}}",
    "solvency_ratios":      f"{BASE_URL}/RatiosSolvency/{{co_code}}/{{t}}",
    "cashflow_ratios":      f"{BASE_URL}/CashFlowRatios/{{co_code}}/{{t}}",
    "financial_stability_ratios": f"{BASE_URL}/FinancialStabilityRatios/{{co_code}}/{{t}}",
    "performance_ratios" : f"{BASE_URL}/PerformanceRatios/{{co_code}}/{{t}}",
    "quarterly_ratios" : f"{BASE_URL}/QuarterlyRatio/{{co_code}}/{{t}}",
    "yearly_ratios" : f"{BASE_URL}/YearlyRatio/{{co_code}}/{{t}}",
    "yearly_result_based_ratios" : f"{BASE_URL}/YearlyResultBasedRatios/{{co_code}}/{{t}}",
    # ── Stock: Price / Market ──────────────────────────────────────────────────
    "company_quotes":       f"{BASE_URL}/GetQuotes/{{co_code}}/{{ex}}",
    "nse_bse_current_stock_price" : f"{BASE_URL}/BseNseDelayedPrice/{{ex}}",
    "indices":              f"{BASE_URL}/Indices",
    "active_performer":     f"{BASE_URL}/MostActiveToppers/{{ex}}/{{group}}/value/{{record_count}}",
    "gainers":              f"{BASE_URL}/Gainers/{{ex}}/{{group}}/{{record_count}}",
    "losers":               f"{BASE_URL}/losers/{{ex}}/{{group}}/{{record_count}}",
    "out_under_performers": f"{BASE_URL}/OutUnderPerformers/{{ex}}/{{group}}/{{performer}}/{{record_count}}",
    "advance_decline":      f"{BASE_URL}/AdvancesDeclines/{{ex}}",
    "52w_high":             f"{BASE_URL}/FiftyTwoWeekHighEOD/{{ex}}/{{group}}/{{record_count}}",
    "52w_low":              f"{BASE_URL}/FiftyTwoWeekLowEOD/{{ex}}/{{group}}/{{record_count}}",
    "new_high_low":         f"{BASE_URL}/NewHigh-NewLowEOD/{{group}}/{{high_or_low}}/{{period}}/{{record_count}}",
    # ── Stock: Announcements ───────────────────────────────────────────────────
    "bse_announcement":     f"{BASE_URL}/BSEAnnouncement",
    "nse_announcement":     f"{BASE_URL}/NSEAnnouncement",
    "corporate_news":       f"{BASE_URL}/CapitalMarketLiveNews/corporate-news/{{n}}",
    # ── Stock: IPO ─────────────────────────────────────────────────────────────
    "forthcoming_ipo":      f"{BASE_URL}/forthcomingipo/{{ex}}/Ipo/{{n}}",
    "open_ipo":             f"{BASE_URL}/OpenIssues/{{ex}}/Ipo/{{n}}",
    "closed_ipo":           f"{BASE_URL}/ClosedIssues/{{ex}}/IPO/{{n}}",
    "new_listing":          f"{BASE_URL}/Newlisting/{{ex}}/{{n}}",
    "best_ipo":             f"{BASE_URL}/BestPerformerIpo/{{ex}}/{{n}}",
    "ipo_details":          f"{BASE_URL}/IPODetails/{{co_code}}",
    "subscription_status":  f"{BASE_URL}/SubscriptionStatus/{{co_code}}",
    "forthcoming_drh":      f"{BASE_URL}/forthcomingDRHFiling/{{ex}}/IPO/{{n}}",
    "ipo_master":               f"{BASE_URL}/ipomaster",
    "ipo_synopsis":             f"{BASE_URL}/IPOSynopsis/{{co_code}}",
    "ipo_timeline":             f"{BASE_URL}/IPOTimeline/{{co_code}}",
    "ipo_promoter_details":     f"{BASE_URL}/IPOPromoterDetails/{{co_code}}",
    "ipo_listing_info":         f"{BASE_URL}/IPOListingInfo/{{co_code}}",
    "ipo_registrar":            f"{BASE_URL}/IPORegistrar/{{co_code}}",
    "ipo_lead_manager":         f"{BASE_URL}/IPOLeadmanager/{{co_code}}",
    "ipo_prospectus":           f"{BASE_URL}/IPOProspectus/sebi",
    "ipo_allocation_details":   f"{BASE_URL}/IPOAllocationDetails/{{co_code}}",
    "ipo_selling_shareholders": f"{BASE_URL}/IPOSellingShareholderDetails/{{co_code}}",
    "ipo_industry_peers":       f"{BASE_URL}/IPOIndustryPeerDetails/{{co_code}}",
    "ipo_risk_details":         f"{BASE_URL}/IPORiskDetails/{{co_code}}",
    "ipo_strategy_details":     f"{BASE_URL}/IPOStrategyDetails/{{co_code}}",
    "ipo_strength_details":     f"{BASE_URL}/IPOStrengthDetails/{{co_code}}",
    "ipo_product_services":     f"{BASE_URL}/IPOProductServicesDetails/{{co_code}}",
    "ipo_customer_details":     f"{BASE_URL}/IPOCustomerDetails/{{co_code}}",
    "ipo_financials":           f"{BASE_URL}/IPOFInancials/{{co_code}}/{{report_type}}",
    "anchor_investor_details":  f"{BASE_URL}/AnchorInvestorDetails/{{co_code}}",
    "objects_of_issue":         f"{BASE_URL}/ObjectsoftheIssue/{{co_code}}",
    "basis_of_allotment":       f"{BASE_URL}/BasisOfAllotment/{{n}}",
    "ipo_logo":                 f"{BASE_URL}/IPOCompanyLogo",

    # ── MF: Masters ────────────────────────────────────────────────────────────
    "fund_house":           f"{BASE_URL}/Fund_House",
    "fund_category_amc":    f"{BASE_URL}/FundCategoryAMCWise/{{mf_cocode}}/{{category}}",
    "scheme_master":        f"{BASE_URL}/SchemeMaster/{{mf_cocode}}",
    "amfi_master":          f"{BASE_URL}/AMFIMaster",
    "fund_manager":         f"{BASE_URL}/FundManager",
    "sip_dates":            f"{BASE_URL}/SIP_Dates/{{plan}}",
    "bse_star_scheme":      f"{BASE_URL}/BSEStarSchemeMaster/{{mf_schcode}}",
    "fund_profile":         f"{BASE_URL}/fund-profile/{{mf_cocode}}",
    # ── MF: Scheme Data ────────────────────────────────────────────────────────
    "investment_details":   f"{BASE_URL}/InvestmentDetails/{{mf_schcode}}",
    "expense_ratio":        f"{BASE_URL}/ExpenseRatios",
    "avg_maturity":         f"{BASE_URL}/AvgerageMaturityData",
    "scheme_aum":           f"{BASE_URL}/SchemeAUMHist",
    "new_fund_offer":       f"{BASE_URL}/NewFundOffer",
    "scheme_sip_swp":       f"{BASE_URL}/SchemeSIPSWPdetails/{{mf_schcode}}",
    "scheme_comparison":    f"{BASE_URL}/SchemeComparsion/{{schcodes}}",
    "fund_performance":     f"{BASE_URL}/FundPerformance/{{top}}/{{type}}/{{category}}",
    "daily_nav":            f"{BASE_URL}/DailyNAV",
    "nav_historical":       f"{BASE_URL}/SchemeNAVHistorical/{{mf_schcode}}/{{period}}/{{periodval}}",
    "scheme_returns":       f"{BASE_URL}/SchemeReturns/{{mf_schcode}}",
    "category_performance": f"{BASE_URL}/CategoryPerformance/{{type}}/{{top}}",
    "mf_holding":           f"{BASE_URL}/MFHolding/{{mf_schcode}}",
    "company_mf_holding":   f"{BASE_URL}/CompanyWiseMFHolding/{{co_code}}/{{top}}",
    "sector_allocation":    f"{BASE_URL}/SchemeSectorAllocation/{{mf_schcode}}",
    "asset_allocation":     f"{BASE_URL}/SchemeAssetAllocation/{{mf_schcode}}",
    "whats_in_out":         f"{BASE_URL}/Whats_InOut/{{type}}/{{mf_schcode}}",
    "mcap_equity":          f"{BASE_URL}/MCAP_EquityHolding/{{mf_schcode}}",
    "most_sold_bought":     f"{BASE_URL}/MostsoldBought/{{mf_schcode}}",
    "lumpsum_return":       f"{BASE_URL}/LumpSumSchemereturnDetails/{{mf_schcode}}",
    "mf_activities":        f"{BASE_URL}/MFActivities",
    "scheme_ratios":        f"{BASE_URL}/Scheme_Ratios",
    "dividend_details":     f"{BASE_URL}/DividendDetails/{{mf_schcode}}",
    "mf_news":              f"{BASE_URL}/MF_News/{{sno}}",

    #--------------------- etf : data ------------------------------
    "get_etf_quotes" : f"{BASE_URL}/ETFGetQuotes/{{ex}}/{{isin}}",
    "get_etf_returns":f"{BASE_URL}/ETFReturns/{{isin}}",
    "get_etf_fundamentals":f"{BASE_URL}/ETFFundamentals/{{isin}}",
    "get_etf_about":f"{BASE_URL}/ETFAboutus/{{isin}}",
    "etf_equity_holdings":f"{BASE_URL}/ETFShareHoldingEquity/{{isin}}",
    "get_etf_monthly_portfolio" : f"{BASE_URL}/ETFMonthlyPortfolioAllHoldings/{{isin}}",
    "get_etf_sector_allocation" :f"{BASE_URL}/ETFSectorAllocation/{{isin}}",
    "get_etf_asset_allocation":f"{BASE_URL}/ETFAssetAllocation/{{isin}}"
}

# ══════════════════════════════════════════════════════════════════════════════
mcp = FastMCP("Equifiz Unified Market Server")
# ══════════════════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — STOCK RESOLVERS
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.tool(description=(
    "Resolve any company name, brand, or ticker into NSE symbol + co_code. "
    "CALL THIS FIRST before any stock or financial-ratio tool. "
    "Accepts tickers (TCS), brand names (jio, dmart), or natural phrases."
))
def resolve_nse_symbol(query: str) -> str:
    cleaned   = _clean_stock_query(query)
    canonical = BRAND_MAP.get(cleaned.lower().strip(), cleaned).strip()
    if canonical.isupper() and len(canonical) <= 12 and " " not in canonical:
        row = _by_symbol(canonical)
        if row:
            return "RESOLVED (exact)\n" + _fmt_company(row)
    hits = _fuzzy_company(canonical, limit=1, threshold=FUZZY_THRESHOLD)
    if hits:
        return "RESOLVED\n" + _fmt_company(hits[0])
    hits = _fuzzy_company(canonical, limit=1, threshold=70)
    if hits:
        return "RESOLVED (low confidence — verify)\n" + _fmt_company(hits[0])
    return f"NOT FOUND: '{canonical}'. Check the company name or NSE ticker."


@mcp.tool(description=(
    "Get full company profile by exact NSE symbol or co_code. "
    "Use when you already have a precise identifier."
))
def get_company_details(nse_symbol: Optional[str] = None, co_code: Optional[int] = None) -> str:
    if not nse_symbol and co_code is None:
        return "Error: provide nse_symbol or co_code."
    row = _by_symbol(nse_symbol) if nse_symbol else None
    if row is None and co_code is not None:
        row = _by_co_code(co_code)
    if row is None:
        return f"NOT FOUND. Try resolve_nse_symbol(query='{nse_symbol or co_code}')."
    return "COMPANY DETAILS\n" + _fmt_company(row)


@mcp.tool(description=(
    "Search companies by partial name, sector, or industry keyword "
    "(e.g. 'pharma', 'banks', 'steel'). Returns ranked matches with co_code."
))
def search_companies(query: str, limit: int = 5) -> str:
    hits = _fuzzy_company(query, limit=min(max(limit, 1), 10), threshold=60)
    if not hits:
        return f"No companies found for '{query}'."
    lines = [f"Results for '{query}':"]
    for i, co in enumerate(hits, 1):
        lines.append(
            f"  {i}. {co.get('companyname')}"
            f"  |  NSE: {co.get('nsesymbol')}"
            f"  |  co_code: {co.get('co_code')}"
            f"  |  Sector: {co.get('sectorname')}"
        )
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — STOCK PRICE & MARKET TOOLS
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.tool(description=(
    "Get current stock price and trading data: LTP (Last Traded Price), open, high, low, "
    "previous close, volume, and 52-week high/low. "
    "REQUIRES co_code — call resolve_nse_symbol first. exchange: 'NSE' (default) or 'BSE'."
))
def get_company_stock_price(co_code: int, exchange: str = "NSE") -> str:

    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
        
    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)
        
    # Endpoint derived from your image: GetQuotes/{co_code}/{exchange}
    url = EP["company_quotes"].format(co_code=co_code,ex=ex)
    data, err = _get(url, f"StockPrice[{val}/{ex}]")
    
    if err:
        return err
        
    rows = _rows(data)
    if not rows:
        return "No price data found."
    
    r = rows[0]
    
    # Updated mapping to match the actual API return keys (case-sensitive)
    metrics = {
        "Company": r.get("CompLname", "N/A"),
        "LTP": r.get("price", 0.0),            # API returns 'price'
        "Open": r.get("Open_Price", 0.0),      # API returns 'Open_Price'
        "High": r.get("High_Price", 0.0),      # API returns 'High_Price'
        "Low": r.get("Low_Price", 0.0),        # API returns 'Low_Price'
        "Prev Close": r.get("OldPrice", 0.0),   # API returns 'OldPrice'
        "Change": r.get("Pricediff", 0.0),     # API returns 'Pricediff'
        "Pct Change": r.get("change", 0.0),    # API returns 'change'
        "Volume": r.get("Volume", 0),
        "52W High": r.get("HI_52_WK", 0.0),
        "52W Low": r.get("LO_52_WK", 0.0),
        "Last Update": r.get("Upd_Time", "N/A")
    }

    # Format the response for the LLM
    header = f"### Stock Price: {metrics['Company']} [{ex}]"
    lines = [header, "---"]
    
    for label, value in metrics.items():
        if label == "Company":
            continue
            
        if isinstance(value, (int, float)) and label != "Volume":
            formatted_val = f"₹{value:,.2f}"
        elif label == "Volume":
            formatted_val = f"{int(value):,}"
        else:
            formatted_val = str(value)
            
        lines.append(f"* **{label}:** {formatted_val}")

    return "\n".join(lines)

@mcp.tool(description=(
    "Retrieves current (during market hours) stock prices for NSE/BSE. "
    "Includes current price, open, high, low, and volume. "
    "Use this for real-time price checks during trading hours. "
    "REQUIRES co_code — call resolve_nse_symbol first. exchange: 'NSE' or 'BSE'."
))
def get_delayed_stock_price(co_code: int, exchange: str = "NSE") -> str:

    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
        
    try:
        ex = exchange.upper() if exchange.upper() in ("NSE", "BSE") else "NSE"
    except Exception:
        ex = "NSE"
        
    # Endpoint remains same as per your configuration
    url = EP["nse_bse_current_stock_price"].format(ex=ex)
    data, err = _get(url, f"DelayedPrice[{ex}]")
    
    if err or not data.get("success"):
        return f"Error fetching delayed prices for {ex}."
        
    rows = data.get("data", [])
    
    # FILTER: Match the specific co_code from the list
    target_row = next((r for r in rows if int(float(r.get("co_code", 0))) == val), None)
    
    if not target_row:
        return f"Company code {val} not found in the current {ex} delayed price feed."
    
    # Mapping keys to match your actual API response
    metrics = {
        "Company": target_row.get("CO_NAME", "N/A"),
        "Symbol": target_row.get("SYMBOL", "N/A"),
        "Current Price": target_row.get("price", 0.0), # API returns 'price'
        "Open": target_row.get("Open", 0.0),           # API returns 'Open'
        "High": target_row.get("High", 0.0),           # API returns 'High'
        "Low": target_row.get("Low", 0.0),             # API returns 'Low'
        "Volume": target_row.get("Volume", 0),         # API returns 'Volume'
        "Trade Date": target_row.get("Tr_Date", "N/A") # API returns 'Tr_Date'
    }

    # Format the response for the LLM
    header = f"### Delayed Stock Price: {metrics['Company']} ({metrics['Symbol']}) [{ex}]"
    lines = [header, "---"]
    
    for label, value in metrics.items():
        if label in ["Company", "Symbol"]:
            continue
            
        if isinstance(value, (int, float)) and label != "Volume":
            formatted_val = f"₹{value:,.2f}"
        elif label == "Volume":
            # Handles cases where Volume is returned as a float (e.g., 443.0)
            formatted_val = f"{int(float(value)):,}"
        else:
            formatted_val = str(value)
            
        lines.append(f"* **{label}:** {formatted_val}")

    return "\n".join(lines)

# @mcp.tool(description="Get live index values (NIFTY 50, SENSEX, BANK NIFTY, etc.) with LTP, change and % change.")
# def get_market_indices(exchange: str = "NSE") -> str:
#     data, err = _get(EP["indices"], "Indices")
#     if err:
#         return err
#     records = _rows(data)
#     filtered = []
#     for item in records:
#         ex_val = (item.get("EXCHANGE") or item.get("exchange") or "").upper()
#         if exchange and ex_val not in ("", exchange.upper()):
#             continue
#         filtered.append({
#             "symbol": item.get("SYMBOL") or item.get("symbol") or item.get("IndexName"),
#             "ltp":    item.get("LTP")    or item.get("ltp")    or item.get("Close"),
#             "change": item.get("CHANGE") or item.get("change") or item.get("NetChange"),
#             "pct":    item.get("PER_CHANGE") or item.get("pchange") or item.get("PercentChange"),
#             "open":   item.get("OPEN")   or item.get("open"),
#             "high":   item.get("HIGH")   or item.get("high"),
#             "low":    item.get("LOW")    or item.get("low"),
#             "prev":   item.get("PREV_CLOSE") or item.get("prevclose") or item.get("PreviousClose"),
#         })
#     filtered = [{k: v for k, v in idx.items() if v is not None} for idx in filtered]
#     return json.dumps(filtered)
@mcp.tool(description=(
    "Retrieves real-time market index data for the NSE and BSE. "
    "Use this to check the current performance of major benchmarks like NIFTY 50, SENSEX, "
    "and sectoral indices (Bank, IT, etc.). Returns Last Traded Price (LTP), "
    "absolute change, and percentage change. Specify 'NSE' or 'BSE' as the exchange."
))
def get_market_indices(exchange: str = "NSE") -> str:
    try:
        ex_limit = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)

    # Endpoint: indices
    data, err = _get(EP["indices"], "Indices")
    if err:
        return err
        
    rows = _rows(data)
    if not rows:
        return "No index data found."

    # Filter for major indices to provide a concise summary
    major_indices = {
        "NSE": ["NIFTY 50", "NIFTY BANK", "NIFTY IT", "NIFTY NEXT 50", "NIFTY MIDCAP 100"],
        "BSE": ["SENSEX", "BSE BANKEX", "BSE IT", "BSE 100", "BSE MIDCAP"]
    }

    target_list = major_indices.get(ex_limit, [])
    lines = [f"### Market Indices Summary — {ex_limit}"]
    lines.append("---")
    
    count = 0
    for row in rows:
        # Match names based on actual API return keys
        name = (row.get("IndexName") or row.get("SYMBOL") or row.get("symbol") or "").strip()
        ex_val = (row.get("exchange") or row.get("EXCHANGE") or "").upper()

        # Filtering logic
        if ex_limit and ex_val and ex_val != ex_limit:
            continue
        if target_list and name.upper() not in [n.upper() for n in target_list]:
            continue

        # Map values to keys identified in the latest API samples
        ltp = row.get("price") or row.get("LTP") or row.get("Close") or 0.0
        pct = row.get("pchange") or row.get("PER_CHANGE") or row.get("PercentChange") or 0.0
        chg = row.get("Pricediff") or row.get("CHANGE") or row.get("NetChange") or 0.0

        # Visual indicator for trend
        try:
            val = float(chg)
            indicator = "🟢" if val > 0 else "🔴" if val < 0 else "⚪"
        except (ValueError, TypeError):
            indicator = "⚪"

        # Formatting for readability
        formatted_ltp = f"{float(ltp):,.2f}"
        formatted_pct = f"{float(pct):.2f}%"
        
        lines.append(f"* **{name}**: {formatted_ltp} ({indicator} {formatted_pct})")
        count += 1

    if count == 0:
        return f"No major indices found for {ex_limit}."

    return "\n".join(lines)


@mcp.tool(description="Get exchange trading holidays. exchange: 'NSE' (default) or 'BSE'.")
def get_exchange_holidays(exchange: str = "NSE") -> str:
    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)
    url = EP["exchange_holidays"].format(ex=ex)
    data, err = _get(url, f"Holidays[{ex}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return f"No holiday data for {ex}."
    lines = [f"Exchange Holidays — {ex}:"]
    for row in rows:
        p = _pick(row, ["holidaydate", "purpose", "day"])
        lines.append(f"  {str(p.get('holidaydate',''))[:10]}  ({p.get('day','')})  — {p.get('purpose','N/A')}")
    return "\n".join(lines)


@mcp.tool(description=(
    "Retrieves the top active stocks by value or volume for a specific market group or index "
    "(e.g., 'NIFTY50', 'BSE_SENSEX'). Useful for identifying stocks with the highest liquidity "
    "and trading interest during the session. REQUIRES exchange and group name."
))
def get_active_performers(exchange: str = "NSE", group: str = "NIFTY50", record_count: int = 5) -> str:
    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)
        
    resolved = _resolve_group(group, exchange=ex) or group.strip().upper()
    
    # Endpoint derived from your logs: MostActiveToppers/{ex}/{group}/value/{record_count}
    url = EP["active_performer"].format(ex=ex.lower(), group=resolved, record_count=record_count)
    data, err = _get(url, f"ActivePerformer[{ex}/{resolved}]")
    
    if err:
        return err
        
    rows = _rows(data)
    if not rows:
        return f"No active performer data found for group '{resolved}' on {ex}."

    header = f"### Top Active Performers: {resolved} ({ex})"
    lines = [header, "---"]
    
    for i, row in enumerate(rows, 1):
        # Extracting based on the actual keys seen in your API response
        name = row.get("lname") or row.get("co_name") or "N/A"
        symbol = row.get("symbol", "N/A")
        ltp = row.get("close_price", 0.0)
        net_chg = row.get("netchg", 0.0)
        pct_chg = row.get("perchg", 0.0)
        volume = row.get("vol_traded", 0)
        value_traded = row.get("val_traded", 0.0) # In Crores based on typical CMOTS format

        # Determine trend indicator
        indicator = "🟢" if net_chg > 0 else "🔴" if net_chg < 0 else "⚪"

        # Format the entry
        stock_line = (
            f"{i}. **{name}** ({symbol})\n"
            f"   * **LTP:** ₹{float(ltp):,.2f} ({indicator} {float(pct_chg):.2f}%)\n"
            f"   * **Volume:** {int(float(volume)):,}\n"
            f"   * **Value Traded:** ₹{float(value_traded):,.2f} Cr"
        )
        lines.append(stock_line)

    return "\n".join(lines)


@mcp.tool(description=(
    "Retrieves the top gaining stocks for a specific market group or index (e.g., 'NIFTY50', 'BSE_SENSEX'). "
    "Returns stocks with the highest percentage price increase. Useful for identifying bullish momentum. "
    "Requires exchange and group name."
))
def get_top_gainers(exchange: str = "NSE", group: str = "NIFTY50", record_count: int = 5) -> str:
    try:
        # Use lowercase exchange for the URL path as per your request log
        ex_path = exchange.lower()
        ex_display = exchange.upper()
    except Exception:
        ex_path = "nse"
        ex_display = "NSE"
    
    resolved = _resolve_group(group, exchange=ex_display) or group.strip().upper()
    
    # Endpoint derived from your logs: Gainers/{ex}/{group}/{record_count}
    url = EP["gainers"].format(ex=ex_path, group=resolved, record_count=record_count)
    data, err = _get(url, f"Gainers[{ex_display}/{resolved}]")
    
    if err:
        return err
        
    rows = _rows(data)
    if not rows:
        return f"No gainers found for group '{resolved}' on {ex_display}."
    
    header = f"### Top Gainers: {resolved} ({ex_display})"
    lines = [header, "---"]
    
    for i, row in enumerate(rows, 1):
        # Extracting based on the actual keys seen in your API response
        name = row.get("lname") or row.get("co_name") or "N/A"
        symbol = row.get("symbol", "N/A")
        ltp = row.get("close_price") or row.get("price") or 0.0
        net_chg = row.get("netchg") or 0.0
        pct_chg = row.get("perchg") or 0.0
        volume = row.get("vol_traded") or 0
        
        # Formatting the entry for a clean chat response
        stock_line = (
            f"{i}. **{name}** ({symbol})\n"
            f"   * **LTP:** ₹{float(ltp):,.2f} (🟢 +{float(pct_chg):.2f}%)\n"
            f"   * **Net Change:** +₹{float(net_chg):,.2f}\n"
            f"   * **Volume:** {int(float(volume)):,}"
        )
        lines.append(stock_line)
        
    return "\n".join(lines)

@mcp.tool(description=(
    "Retrieves the top losing stocks for a specific market group or index (e.g., 'NIFTY50', 'BSE_SENSEX'). "
    "Returns stocks with the largest percentage price decrease. Useful for identifying bearish trends. "
    "Requires exchange and group name."
))
def get_top_losers(exchange: str = "NSE", group: str = "NIFTY50", record_count: int = 5) -> str:
    """
    Args:
        exchange: 'NSE' or 'BSE'.
        group: The index or sector group code (e.g., 'NIFTY50', 'BSE_SENSEX').
        record_count: Number of records to return (default 5).
    """
    try:
        # Lowercase for URL path, Uppercase for display context
        ex_path = exchange.lower()
        ex_display = exchange.upper()
    except Exception:
        ex_path = "nse"
        ex_display = "NSE"
    
    resolved = _resolve_group(group, exchange=ex_display) or group.strip().upper()
    
    # Endpoint derived from your logs: losers/{ex}/{group}/{record_count}
    url = EP["losers"].format(ex=ex_path, group=resolved, record_count=record_count)
    data, err = _get(url, f"Losers[{ex_display}/{resolved}]")
    
    if err:
        return err
        
    rows = _rows(data)
    if not rows:
        return f"No losers found for group '{resolved}' on {ex_display}."
    
    header = f"### Top Losers: {resolved} ({ex_display})"
    lines = [header, "---"]
    
    for i, row in enumerate(rows, 1):
        # Extracting based on the exact keys from your API response
        name = row.get("lname") or row.get("co_name") or "N/A"
        symbol = row.get("symbol", "N/A")
        ltp = row.get("close_price") or row.get("price") or 0.0
        net_chg = row.get("netchg") or 0.0
        pct_chg = row.get("perchg") or 0.0
        volume = row.get("vol_traded") or 0
        
        # Formatting for a clean and professional chat output
        stock_line = (
            f"{i}. **{name}** ({symbol})\n"
            f"   * **LTP:** ₹{float(ltp):,.2f} (🔴 {float(pct_chg):.2f}%)\n"
            f"   * **Net Change:** -₹{abs(float(net_chg)):,.2f}\n"
            f"   * **Volume:** {int(float(volume)):,}"
        )
        lines.append(stock_line)
        
    return "\n".join(lines)


@mcp.tool(description=(
    "Retrieves stocks that are outperforming or underperforming relative to a benchmark index "
    "(e.g., 'BSE_SENSEX', 'NIFTY50'). 'Out' shows stocks beating the index, while 'under' shows "
    "those lagging behind. Useful for relative strength analysis. "
    "Requires exchange, group, and performer type ('out' or 'under')."
))
def get_out_under_performers(
    exchange: str = "NSE", group: str = "NIFTY50",
    performer: str = "out", record_count: int = 5
) -> str:
    try:
        ex_path = exchange.lower()
        ex_display = exchange.upper()
    except Exception:
        ex_path = "nse"
        ex_display = "NSE"
        
    p_type = performer.lower()
    if p_type not in ("out", "under"):
        return "performer must be 'out' or 'under'."
        
    resolved = _resolve_group(group, exchange=ex_display) or group.strip().upper()
    
    # Endpoint derived from your logs: OutUnderPerformers/{ex}/{group}/{performer}/{record_count}
    url = EP["out_under_performers"].format(ex=ex_path, group=resolved, performer=p_type, record_count=record_count)
    data, err = _get(url, f"OutUnder[{ex_display}/{resolved}/{p_type}]")
    
    if err:
        return err
        
    rows = _rows(data)
    if not rows:
        return f"No {p_type}performer data found for {resolved} on {ex_display}."
        
    label = "Outperformers" if p_type == "out" else "Underperformers"
    header = f"### {label} vs Index: {resolved} ({ex_display})"
    lines = [header, "---"]
    
    for i, row in enumerate(rows, 1):
        # Extracting based on actual keys in the JSON response
        name = row.get("lname") or row.get("co_name") or "N/A"
        current_close = row.get("close1") or 0.0  # close1 is the latest date price in your log
        prev_close = row.get("close") or 0.0     # close is the starting comparison price
        diff = row.get("diff") or 0.0
        pct_chg = row.get("perchg") or 0.0
        rel_val = row.get("opval") or 0.0        # Relative performance value vs index
        
        # Determine visual indicator
        indicator = "📈" if p_type == "out" else "📉"
        
        # Formatting for a detailed technical response
        stock_line = (
            f"{i}. **{name}**\n"
            f"   * **Current Price:** ₹{float(current_close):,.2f} ({indicator} {float(pct_chg):.2f}%)\n"
            f"   * **Price Change:** ₹{float(diff):,.2f}\n"
            f"   * **Relative Alpha:** {float(rel_val):.2f}%"
        )
        lines.append(stock_line)
        
    return "\n".join(lines)

@mcp.tool(description=(
    "Retrieves market breadth statistics (Advances vs. Declines) for a specific exchange. "
    "This data shows the number of stocks that have gained (Advances), lost (Declines), "
    "or remained unchanged, along with the A/D ratio for various indices and market groups. "
    "Use this to gauge overall market sentiment and strength."
))
def get_advance_decline(exchange: str = "NSE") -> str:
    """
    Args:
        exchange: 'NSE' or 'BSE' (defaults to 'NSE').
    """
    try:
        ex_display = exchange.upper()
        # Ensure the exchange is normalized for the API call
        ex_path = "BSE" if ex_display == "BSE" else "NSE"
    except Exception:
        ex_path = "NSE"
        ex_display = "NSE"
        
    url = EP["advance_decline"].format(ex=ex_path)
    data, err = _get(url, f"AdvanceDecline[{ex_display}]")
    
    if err:
        return err
        
    rows = _rows(data)
    if not rows:
        return f"No advance-decline data found for {ex_display}."
        
    header = f"### Market Breadth (Advance/Decline): {ex_display}"
    lines = [header, "---"]
    
    # We'll pick a few key indices to show to avoid overwhelming the output, 
    # but still show the general market trend.
    for row in rows:
        # Extract based on actual API keys: indexlongname, adv, dec, noc, ad
        name = row.get("indexlongname") or row.get("indexname") or "N/A"
        adv = row.get("adv", 0)
        dec = row.get("dec", 0)
        unch = row.get("noc", 0)
        ratio = row.get("ad", 0.0)
        
        # Determine sentiment indicator based on A/D ratio
        sentiment = "🐂" if float(ratio) > 1.2 else "🐻" if float(ratio) < 0.8 else "⚖️"
        
        # We only display major groups or the exchange total to keep it scannable
        # You can adjust this filter based on which groups you find most useful
        lines.append(
            f"**{name}**\n"
            f"   * {sentiment} **A/D Ratio:** {float(ratio):.2f}\n"
            f"   * **Advances:** {adv} | **Declines:** {dec} | **Unchanged:** {unch}"
        )
        
    return "\n".join(lines)

@mcp.tool(description=(
    "Retrieves a list of stocks that have recently hit or are trading near their 52-week high. "
    "Use this to identify stocks with strong upward momentum or those breaking out of long-term ranges. "
    "Requires exchange ('NSE' or 'BSE') and group (use '-' for all stocks)."
))
def get_52week_highs(exchange: str = "NSE", group: str = "-", record_count: int = 10) -> str:
    """
    Args:
        exchange: 'NSE' or 'BSE'.
        group: The index/sector group (e.g., 'NIFTY50') or '-' for all stocks.
        record_count: Number of records to return (default 10).
    """
    try:
        ex_display = exchange.upper()
        ex_path = "NSE" if ex_display == "NSE" else "BSE"
    except Exception:
        ex_path = "NSE"
        ex_display = "NSE"
    
    resolved = (_resolve_group(group, exchange=ex_display) if group != "-" else None) or "-"
    
    # Endpoint derived from logs: FiftyTwoWeekHighEOD/{ex}/{group}/{record_count}
    url = EP["52w_high"].format(ex=ex_path, group=resolved, record_count=record_count)
    data, err = _get(url, f"52WkHigh[{ex_display}]")
    
    if err:
        return err
        
    rows = _rows(data)
    if not rows:
        return f"No 52-week high data found for group '{resolved}' on {ex_display}."
    
    # API uses 'n' prefix for NSE and 'b' for BSE specific historical data
    high_key = "n52high" if ex_path == "NSE" else "b52high"
    date_key = "n52hdate" if ex_path == "NSE" else "b52hdate"

    header = f"### 52-Week High Summary: {ex_display} ({resolved})"
    lines = [header, "---"]

    for i, row in enumerate(rows, 1):
        # Extract based on actual API response keys
        name = row.get("lname") or row.get("co_name") or "N/A"
        symbol = row.get("symbol", "N/A")
        ltp = row.get("price", 0.0)
        high_52 = row.get(high_key, 0.0)
        h_date_raw = row.get(date_key, "N/A")
        pct_chg = row.get("pchange", 0.0)
        
        # Clean up the date string (removing timestamp if present)
        h_date = h_date_raw.split("T")[0] if "T" in h_date_raw else h_date_raw
        
        # Formatting for clear financial reporting
        stock_line = (
            f"{i}. **{name}** ({symbol})\n"
            f"   * **Current Price:** ₹{float(ltp):,.2f} ({float(pct_chg):.2f}%)\n"
            f"   * **52W High:** ₹{float(high_52):,.2f} (Reached: {h_date})"
        )
        lines.append(stock_line)
        
    return "\n".join(lines)


@mcp.tool(description=(
    "Retrieves a list of stocks that have recently hit or are trading near their 52-week low. "
    "Use this to identify stocks with significant downward momentum or potential 'bottom-fishing' "
    "opportunities. Requires exchange ('NSE' or 'BSE') and group (use '-' for all stocks)."
))
def get_52week_lows(exchange: str = "NSE", group: str = "-", record_count: int = 10) -> str:
    try:
        ex_display = exchange.upper()
        # API path usually expects lowercase or normalized casing
        ex_path = "NSE" if ex_display == "NSE" else "BSE"
    except Exception:
        ex_path = "NSE"
        ex_display = "NSE"
    
    resolved = (_resolve_group(group, exchange=ex_display) if group != "-" else None) or "-"
    
    # Endpoint derived from logs: FiftyTwoWeekLowEOD/{ex}/{group}/{record_count}
    url = EP["52w_low"].format(ex=ex_path, group=resolved, record_count=record_count)
    data, err = _get(url, f"52WkLow[{ex_display}]")
    
    if err:
        return err
        
    rows = _rows(data)
    if not rows:
        return f"No 52-week low data found for group '{resolved}' on {ex_display}."

    # API uses 'n' prefix for NSE and 'b' for BSE specific historical data
    low_key = "n52low" if ex_path == "NSE" else "b52low"
    date_key = "n52ldate" if ex_path == "NSE" else "b52ldate"

    header = f"### 52-Week Low Summary: {ex_display} ({resolved})"
    lines = [header, "---"]

    for i, row in enumerate(rows, 1):
        # Extracting based on actual API response keys
        name = row.get("lname") or row.get("co_name") or "N/A"
        symbol = row.get("symbol", "N/A")
        ltp = row.get("price", 0.0)
        low_52 = row.get(low_key, 0.0)
        l_date_raw = row.get(date_key, "N/A")
        pct_chg = row.get("pchange", 0.0)
        
        # Clean up the date string (removing timestamp if present)
        l_date = l_date_raw.split("T")[0] if "T" in l_date_raw else l_date_raw
        
        # Formatting for a clean and professional response
        stock_line = (
            f"{i}. **{name}** ({symbol})\n"
            f"   * **Current Price:** ₹{float(ltp):,.2f} ({float(pct_chg):.2f}%)\n"
            f"   * **52W Low:** ₹{float(low_52):,.2f} (Reached: {l_date})"
        )
        lines.append(stock_line)
        
    return "\n".join(lines)


@mcp.tool(description=(
    "Retrieves stocks reaching new price highs or lows over a specified period "
    "(e.g., 'year', 'month', 'week') for a given index group (e.g., 'CNXMIDCAP'). "
    "Use this to identify momentum breakouts or breakdown trends in specific market segments."
))
def get_new_highs_lows(
    group: str = "CNXMIDCAP", high_or_low: str = "high",
    period: str = "year", record_count: int = 10
) -> str:
    """
    Args:
        group: The index group (e.g., 'CNXMIDCAP', 'NIFTY50').
        high_or_low: 'high' for new highs, 'low' for new lows.
        period: Timeframe to check ('year', 'month', or 'week').
        record_count: Number of records to return (default 10).
    """
    hl_type = high_or_low.lower()
    if hl_type not in ("high", "low"):
        return "high_or_low must be 'high' or 'low'."
    
    # Endpoint derived from logs: NewHigh-NewLowEOD/{group}/{high_or_low}/{period}/{record_count}
    # Note: Exchange is implicitly handled by the group/index name in this endpoint
    url = EP["new_high_low"].format(
        group=group.strip().upper(), 
        high_or_low=hl_type,
        period=period.lower(), 
        record_count=record_count
    )
    
    data, err = _get(url, f"NewHighLow[{group}/{hl_type}/{period}]")
    if err:
        return err
        
    rows = _rows(data)
    if not rows:
        return f"No stocks found reaching new {hl_type}s in {group} for the '{period}' period."

    header = f"### New {hl_type.title()}s ({period.title()}): {group.upper()}"
    lines = [header, "---"]

    for i, row in enumerate(rows, 1):
        # Extracting based on actual API response keys
        name = row.get("lname") or row.get("co_name") or "N/A"
        symbol = row.get("symbol", "N/A")
        ltp = row.get("price", 0.0)
        pct_chg = row.get("pchange", 0.0)
        volume = row.get("volume", 0)
        
        # Trend and status keys
        high_52 = row.get("n52high") or row.get("b52high") or "N/A"
        low_52 = row.get("n52low") or row.get("b52low") or "N/A"
        
        # Visual indicators
        indicator = "🟢" if hl_type == "high" else "🔴"
        
        # Detailed entry format
        stock_line = (
            f"{i}. **{name}** ({symbol})\n"
            f"   * **Current Price:** ₹{float(ltp):,.2f} ({indicator} {float(pct_chg):.2f}%)\n"
            f"   * **Volume:** {int(float(volume)):,}\n"
            f"   * **52W Range:** ₹{low_52} - ₹{high_52}"
        )
        lines.append(stock_line)

    return "\n".join(lines)

@mcp.tool(description="Get companies in a specific market index. REQUIRES index_code — call _resolve_index_code first.")
def get_index_companies(index_code: int) -> str:
    val, err = _require_int(index_code, "index_code", "_resolve_index_code")
    if err:
        return err
    url = EP["index_wise_comp"].format(index_code=val)
    data, err = _get(url, f"IndexComp[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No companies found for this index."
    lines = [f"Companies in index {val} ({len(rows)} total):"]
    for i, row in enumerate(rows[:30], 1):
        p = _pick(row, ["CompanyLongName", "NSESymbol", "BSECode", "SectorName"])
        lines.append(
            f"  {i:>3}. {p.get('CompanyLongName','N/A')}"
            f"  NSE: {p.get('NSESymbol','N/A')}"
            f"  Sector: {p.get('SectorName','N/A')}"
        )
    return "\n".join(lines)


@mcp.tool(description="Get all companies in a sector by sector code.")
def get_sector_companies(sector_code: str) -> str:
    url = EP["sector_wise_comp"].format(sector_code=sector_code)
    data, err = _get(url, f"SectorComp[{sector_code}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No companies found for this sector code."
    lines = [f"Companies in sector '{sector_code}' ({len(rows)} total):"]
    for i, row in enumerate(rows[:30], 1):
        p = _pick(row, ["co_name", "symbol", "sc_code"])
        lines.append(
            f"  {i:>3}. {p.get('co_name','N/A')}"
            f"  NSE: {p.get('symbol','N/A')}"
            f"  BSE: {p.get('sc_code','N/A')}"
        )
    return "\n".join(lines)


@mcp.tool(description="Get latest BSE corporate announcements.")
def get_bse_announcements() -> str:
    data, err = _get(EP["bse_announcement"], "BSEAnnouncement")
    if err:
        return err
    
    rows = _rows(data)
    if not rows:
        return "No BSE announcements found."
        
    lines = ["Latest BSE Corporate Announcements:"]
    lines.append("-" * 60)

    # Limiting to top 15 as in your original logic
    for i, row in enumerate(rows[:15], 1):
        # Mapping to the keys in your image: lname, caption, date, etc.
        p = _pick(row, [
            "lname", 
            "symbol", 
            "caption", 
            "date", 
            "memo", 
            "fileurl", 
            "typeofannouncement"
        ])
        
        # Format the date for readability (assuming YYYY-MM-DD format)
        raw_date = str(p.get("date", ""))[:10]
        name = p.get("lname", "N/A")
        ticker = p.get("symbol", "")
        # Use caption as headline, fallback to the announcement type
        headline = p.get("caption") or p.get("typeofannouncement", "N/A")
        link = p.get("fileurl", "")

        lines.append(f"  {i:>2}. [{raw_date}] {name} ({ticker})")
        lines.append(f"      Topic: {headline}")
        if link:
            lines.append(f"      Link: {link}")
        lines.append("") # Spacer for readability

    return "\n".join(lines)


@mcp.tool(description="Get latest NSE corporate announcements.")
def get_nse_announcements() -> str:
    data, err = _get(EP["nse_announcement"], "NSEAnnouncement")
    if err:
        return err
    
    rows = _rows(data)
    if not rows:
        return "No NSE announcements found."
        
    lines = ["Latest NSE Corporate Announcements:"]
    lines.append("-" * 65)

    # Limiting to top 15 for concise context
    for i, row in enumerate(rows[:15], 1):
        # Mapped to the keys in your provided image
        p = _pick(row, [
            "lname", 
            "symbol", 
            "caption", 
            "date", 
            "memo", 
            "fileurl", 
            "Subject"
        ])
        
        raw_date = str(p.get("date", ""))[:10]
        name = p.get("lname", "N/A")
        ticker = p.get("symbol", "N/A")
        # Use Subject primarily, fallback to caption if Subject is missing
        headline = p.get("Subject") or p.get("caption", "No subject provided")
        link = p.get("fileurl", "")

        lines.append(f"  {i:>2}. [{raw_date}] {name} ({ticker})")
        lines.append(f"      Subject: {headline}")
        if link:
            lines.append(f"      Link: {link}")
        lines.append("") # Spacer for readability

    return "\n".join(lines)


@mcp.tool(description="Get latest corporate news headlines. count: number of articles (default 10).")
def get_corporate_news(count: int = 10) -> str:
    # Ensure count is passed to the API as per your schema 'recordcount'
    url = EP["corporate_news"].format(n=count)
    data, err = _get(url, "CorporateNews")
    
    if err:
        return err
    
    rows = _rows(data)
    if not rows:
        return "No corporate news available."
        
    lines = [f"Latest Corporate News (Top {count}):"]
    lines.append("-" * 60)

    for row in rows:
        # Mapping to the keys in your provided image: date, heading, caption, Arttext
        p = _pick(row, [
            "sno",
            "section_name",
            "date",
            "heading",
            "caption",
            "Arttext"
        ])
        
        # Format the date (assuming datetime format from your schema)
        raw_date = str(p.get("date", ""))[:10]
        title = p.get("heading", "No Title")
        summary = p.get("caption", "")
        # Arttext is the full body; we can preview it if caption is missing
        body = p.get("Arttext", "")
        
        lines.append(f"\n  [{raw_date}] {title}")
        
        # Prioritize the caption, fallback to a snippet of Arttext
        if summary:
            lines.append(f"  Summary: {summary}")
        elif body:
            # Provide a snippet of the main article text
            lines.append(f"  Snippet: {str(body)[:150]}...")
            
    return "\n".join(lines)

@mcp.tool(description="Get companies that declared results today.")
def get_results_today() -> str:
    data, err = _get(EP["results_today"], "ResultsToday")
    if err:
        return err
    
    rows = _rows(data)
    if not rows:
        return "No results declared today."
        
    lines = [f"Companies Reporting Today ({len(rows)}):"]
    lines.append("-" * 50)

    for i, row in enumerate(rows, 1):
        # Picking from your schema: co_name and resultdate
        # Including heading/caption in case the API returns result summaries
        p = _pick(row, [
            "co_name", 
            "symbol", 
            "resultdate", 
            "heading", 
            "caption",
            "time"
        ])
        
        name = p.get("co_name") or p.get("heading") or "N/A"
        symbol = p.get("symbol", "")
        # Handle cases where resultdate might be under the 'date' key from your image
        date_val = p.get("resultdate") or row.get("date", "N/A")
        raw_date = str(date_val)[:10]
        
        # Adding 'time' from your schema as it's useful for "Results Today"
        time_val = p.get("time", "")
        time_str = f" at {time_val}" if time_val else ""

        lines.append(
            f"  {i:>2}. {name:<30} | {symbol:<10} | {raw_date}{time_str}"
        )
        
        # If there's a brief caption (like 'Board meeting concluded'), include it
        if p.get("caption"):
            lines.append(f"      Status: {p['caption']}")

    return "\n".join(lines)

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — STOCK FUNDAMENTAL RATIOS
# ═══════════════════════════════════════════════════════════════════════════════

# ──────────────────────────────────────────────────────────────────────────────
# KEY FINANCIAL RATIOS
# EOD fields  : COLUMNNAME, StandaloneConsolidated  (pivot table — dynamic year cols)
# TTM/Daily fields: CO_CODE, COLUMNNAME, MCAP, EPS, ROE_TTM, ROCE_TTM, ROA_TTM,
#                   EBIT_TTM, EBITDA_TTM, EV, EV_EBITDA, NetIncomeMargin,
#                   GrossIncomeMargin, AssetTurnover_TTM, Sales_TotalAssets_TTM,
#                   NetDebt_EBITDA_TTM, EBITDA_Margin_TTM, TotalShareHolderEq,
#                   ShortternDebt, EPSDiluted, TotalAssets_TTM,
#                   StandaloneConsolidated
# ──────────────────────────────────────────────────────────────────────────────
@mcp.tool(description=(
    "Retrieves historical financial ratios (last 5 years) such as Debt-Equity, ROCE, RONW, "
    "Current Ratio, and Profit Margins. Useful for fundamental analysis of a company's "
    "financial health and operational efficiency."
    "REQUIRES co_code. report_type: 's' (standalone) or 'c' (consolidated)." 
))
def get_key_financial_ratios(co_code: int, report_type: str = "s") -> str:

    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err

    # API expects uppercase 'S' or 'C' in the URL path based on your log
    t_path = report_type.upper() if report_type.lower() in ("s", "c") else "S"
    
    url = EP["key_ratios"].format(co_code=val, t=t_path)
    data, err = _get(url, f"KeyRatios[{val}]")
    
    if err:
        return err

    rows = _rows(data)
    if not rows:
        return f"No key ratio data found for company code {val}."

    # Robust detection for Y<YYYYMM> columns (e.g., Y202503)
    # We collect all year-based keys and sort them descending to show the latest first
    all_keys = set()
    for row in rows:
        all_keys.update(row.keys())
        
    year_cols = sorted(
        [k for k in all_keys if k.startswith("Y") and k[1:].isdigit()],
        reverse=True
    )[:5]

    if not year_cols:
        return "Financial ratios are currently unavailable for this company."

    def fmt_year(yc):
        # Maps Y202503 -> Mar 2025
        months = {"03": "Mar", "06": "Jun", "09": "Sep", "12": "Dec"}
        year = yc[1:5]
        month_code = yc[5:]
        return f"{months.get(month_code, month_code)} {year}"

    # Formatting the output for a clean, readable financial report
    header_label = "Consolidated" if t_path == "C" else "Standalone"
    lines = [f"### Historical Key Ratios ({header_label})", "---"]

    for row in rows:
        metric = row.get("COLUMNNAME", "").strip()
        # Skip the 'Year End' row as it's redundant with our column headers
        if not metric or metric == "Year End":
            continue
            
        metric_line = f"**{metric}**"
        yearly_data = []
        
        for yc in year_cols:
            val_raw = row.get(yc)
            if val_raw is None or val_raw == "":
                formatted_val = "N/A"
            else:
                try:
                    formatted_val = f"{float(val_raw):.2f}"
                except (ValueError, TypeError):
                    formatted_val = str(val_raw)
            
            yearly_data.append(f"{fmt_year(yc)}: `{formatted_val}`")
        
        lines.append(f"* {metric_line} — {' | '.join(yearly_data)}")

    return "\n".join(lines)

# ──────────────────────────────────────────────────────────────────────────────
# DAILY / TTM RATIOS  (live market data)
# Fields: CO_CODE, MCAP, EPS, EV, PBV, DivYield, ROA_TTM, ROCE_TTM, ROE_TTM,
#         EBIT_TTM, EBITDA_TTM, EV_EBITDA, BookValue, NetIncomeMargin,
#         GrossIncomeMargin, AssetTurnover_TTM, Sales_TotalAssets_TTM,
#         NetDebt_EBITDA_TTM, EBITDA_Margin_TTM, TotalShareHolderEq,
#         ShortternDebt, EPSDiluted, TotalAssets_TTM, StandaloneConsolidated
# ──────────────────────────────────────────────────────────────────────────────

@mcp.tool(description=(
    "Fetches comprehensive live/daily Trailing Twelve Months (TTM) financial ratios and market valuation metrics. "
    "Provides critical investment data including Market Cap, P/E Ratio, PEG Ratio (Growth), P/B Value, Dividend Yield, "
    "and Enterprise Value (EV). It also returns operational performance margins (Net Income, EBITDA), "
    "efficiency ratios (ROE, ROCE, ROA), and leverage metrics like Net Debt/EBITDA. "
    "REQUIRES co_code — call resolve_nse_symbol first."
))
def get_daily_ratios(co_code: int, report_type: str = "S") -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    
    # Force uppercase and default to 'S' (Standalone) if input is invalid
    t = report_type.upper() if report_type.upper() in ("S", "C") else "S"
    
    # The URL line remains as requested
    url = EP["daily_ratios"].format(co_code=val, t=t)
    
    data, err = _get(url, f"DailyRatios[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return f"No daily ratio data found for co_code {val}."
    
    r = rows[0]
    
    # Fields matched to your API JSON response keys
    FIELDS = [
        "MCAP", "EPS", "PE", "PEGRatio_TTM", "PBV", "DIVYIELD", "EV", 
        "EV_EBITDA_TTM", "BookValue", "ROE_TTM", "ROCE_TTM", "ROA_TTM", 
        "NetIncomeMargin_TTM", "EBITDA_Margin_TTM", "NetDebt_EBITDA_TTM"
    ]
    
    p = _pick(r, FIELDS)
    
    # UI Header logic
    lines = [f"### Daily & TTM Ratios ({'Standalone' if t == 'S' else 'Consolidated'})"]
    lines.append("-" * 45)
    
    units = {
        "MCAP": "Cr", "EV": "Cr", "EPS": "Rs", "BookValue": "Rs",
        "ROE_TTM": "%", "ROCE_TTM": "%", "ROA_TTM": "%", 
        "NetIncomeMargin_TTM": "%", "EBITDA_Margin_TTM": "%", "DIVYIELD": "%"
    }

    for k in FIELDS:
        v = p.get(k, "N/A")
        unit = units.get(k, "")
        if isinstance(v, (int, float)):
            v = f"{v:.2f}"
        
        display_key = k.replace("_TTM", "")
        lines.append(f"  {display_key:<20}: {v} {unit}".strip())
        
    return "\n".join(lines)

# ──────────────────────────────────────────────────────────────────────────────
# MARGIN RATIOS
# EOD fields : co_code, Type, PBDTIM, EBTIM, PATIM, OPM, CPM
# TTM fields : GrossIncomeMargin, AssetTurnover_TTM, Sales_TotalAssets_TTM,
#              NetDebt_EBITDA_TTM, EBITDA_Margin_TTM, TotalShareHolderEq,
#              ShortternDebt, EPSDiluted (all surfaced via get_daily_ratios too)
# ──────────────────────────────────────────────────────────────────────────────
@mcp.tool(description=(
    "Retrieves historical annual margin ratios to analyze profitability trends over multiple years. "
    "Provides key margins including PBIDTIM (Operating Profit Margin before Interest/Depreciation), "
    "EBITM (Operating Margin), PreTaxMargin (EBT), PATM (Net Profit Margin), and CPM (Cash Profit Margin). "
    "This tool is essential for assessing a company's operational efficiency and bottom-line growth. "
    "REQUIRES co_code — call resolve_nse_symbol first."
))
def get_margin_ratios(co_code: int, report_type: str = "S") -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    
    # Standardizing to capital letters for API consistency
    t = report_type.upper() if report_type.upper() in ("S", "C") else "S"
    
    # URL line kept as per original structure
    url = EP["margin_ratios"].format(co_code=val, t=t)
    
    data, err = _get(url, f"MarginRatios[{val}]")
    if err:
        return err
    
    rows = _rows(data)
    if not rows:
        return f"No margin ratio data found for co_code {val}."

    # Updated FIELDS to match the API response:
    # PBIDTIM, EBITM, PreTaxMargin, PATM, CPM
    FIELDS = ["YRC", "PBIDTIM", "EBITM", "PreTaxMargin", "PATM", "CPM"]
    
    header = "Standalone" if t == 'S' else "Consolidated"
    lines = [f"### Historical Margin Ratios ({header})"]
    lines.append(f"{'Year':<8} | {'OPM(%)':<8} | {'EBITM(%)':<8} | {'PBT(%)':<8} | {'PAT(%)':<8} | {'CPM(%)':<8}")
    lines.append("-" * 65)

    for row in rows[:5]:
        p = _pick(row, FIELDS)
        
        # Formatting the Year (YRC) from YYYYMM to YYYY
        raw_yr = str(int(p.get("YRC", 0))) if p.get("YRC") else "N/A"
        yr = raw_yr[:4] if len(raw_yr) >= 4 else raw_yr
        
        # Extracting values and formatting decimals
        opm = f"{p.get('PBIDTIM', 0):.2f}"
        ebitm = f"{p.get('EBITM', 0):.2f}"
        pbt = f"{p.get('PreTaxMargin', 0):.2f}"
        pat = f"{p.get('PATM', 0):.2f}"
        cpm = f"{p.get('CPM', 0):.2f}"
        
        lines.append(f"{yr:<8} | {opm:<8} | {ebitm:<8} | {pbt:<8} | {pat:<8} | {cpm:<8}")

    return "\n".join(lines)

# ──────────────────────────────────────────────────────────────────────────────
# PERFORMANCE RATIOS  ← NEW (was missing from original code)
# EOD fields : co_code, Type, ROA, ROE, ROCE
# ──────────────────────────────────────────────────────────────────────────────
@mcp.tool(description=(
    "Get historical performance ratios across years: ROA, ROE, ROCE. "
    "REQUIRES co_code — call resolve_nse_symbol first. "
    "report_type: 's' = standalone (default), 'c' = consolidated."
))
def get_performance_ratios(co_code: int, report_type: str = "s") -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    t = report_type if report_type in ("s", "c") else "s"
    url = EP["performance_ratios"].format(co_code=val, t=t)
    data, err = _get(url, f"PerformanceRatios[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No performance ratio data found."
    FIELDS = ["Year", "YRC", "co_code", "Type", "ROA", "ROE", "ROCE"]
    lines = [f"Performance Ratios [{'Standalone' if t == 's' else 'Consolidated'}]:"]
    for row in rows[:5]:
        p = _pick(row, FIELDS)
        yr = p.get("Year") or p.get("YRC", "N/A")
        lines.append(
            f"  {yr}  |  ROE: {p.get('ROE', 'N/A')}%"
            f"  ROCE: {p.get('ROCE', 'N/A')}%"
            f"  ROA: {p.get('ROA', 'N/A')}%"
        )
    return "\n".join(lines)


# ──────────────────────────────────────────────────────────────────────────────
# EFFICIENCY RATIOS
# EOD fields : co_code, Type, FixedCapitals, SalesReceivablesDays,
#              InventoryDays, CreditorDays, OPM  (NOT AssetTurnover etc.)
# ──────────────────────────────────────────────────────────────────────────────
def get_efficiency_ratios(co_code: int, report_type: str = "S") -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    
    # Ensure uppercase 'S' or 'C' as required by the API
    t = report_type.upper() if report_type.upper() in ("S", "C") else "S"
    
    url = EP["efficiency_ratios"].format(co_code=val, t=t)
    data, err = _get(url, f"EfficiencyRatios[{val}]")
    if err:
        return err
    
    rows = _rows(data)
    if not rows:
        return "No efficiency ratio data found."

    # Updated to match the actual API response keys
    FIELDS = ["Year", "YRC", "co_code", "FixedCapitals_Sales", 
              "ReceivableDays", "InventoryDays", "PayableDays", "OPM"]

    lines = [f"Efficiency Ratios [{'Standalone' if t == 'S' else 'Consolidated'}]:"]
    for row in rows[:5]:
        p = _pick(row, FIELDS)
        yr = p.get("Year") or str(int(p.get("YRC", 0))) or "N/A"
        
        lines.append(
            f"  {yr}  |  Fixed Cap T/O: {p.get('FixedCapitals_Sales', 'N/A'):.2f}"
            f"  Debtor Days: {p.get('ReceivableDays', 'N/A'):.2f}"
            f"  Inv Days: {p.get('InventoryDays', 'N/A'):.2f}"
            f"  Creditor Days: {p.get('PayableDays', 'N/A'):.2f}"
            f"  OPM: {p.get('OPM', 'N/A')}%"
        )
    return "\n".join(lines)


# ──────────────────────────────────────────────────────────────────────────────
# FINANCIAL STABILITY RATIOS  ← NEW (was missing from original code)
# EOD fields : co_code, Type, TotalDebt_Equity, LongTermDebt_Equity,
#              QuickRatio, InterestCover, TotalDebt_MCap
# ──────────────────────────────────────────────────────────────────────────────
def get_financial_stability_ratios(co_code: int, report_type: str = "S") -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    
    # API strictly requires uppercase 'S' or 'C'
    t = report_type.upper() if report_type.upper() in ("S", "C") else "S"
    
    url = EP["financial_stability_ratios"].format(co_code=val, t=t)
    data, err = _get(url, f"FinancialStabilityRatios[{val}]")
    if err:
        return err
    
    rows = _rows(data)
    if not rows:
        return "No financial stability ratio data found."
    
    # Fields based on your successful API response
    FIELDS = ["Year", "YRC", "co_code", "TotalDebt_Equity", 
              "LongTermDebt_Equity", "QuickRatio", "InterestCover", "TotalDebt_MCap"]
    
    lines = [f"Financial Stability Ratios [{'Standalone' if t == 'S' else 'Consolidated'}]:"]
    
    for row in rows[:5]:
        p = _pick(row, FIELDS)
        
        # Format YRC (e.g., 202503.0) to a cleaner string if Year is missing
        yr_raw = p.get("Year") or p.get("YRC")
        yr = str(int(yr_raw)) if yr_raw else "N/A"
        
        # Helper to format floats to 2 decimal places if they exist
        def fmt(val):
            return f"{val:.2f}" if isinstance(val, (int, float)) else "N/A"

        lines.append(
            f"  {yr}  |  D/E: {fmt(p.get('TotalDebt_Equity'))}x"
            f"  LT D/E: {fmt(p.get('LongTermDebt_Equity'))}x"
            f"  Quick: {fmt(p.get('QuickRatio'))}x"
            f"  Int Cover: {fmt(p.get('InterestCover'))}x"
            f"  Debt/MCap: {fmt(p.get('TotalDebt_MCap'))}"
        )
        
    return "\n".join(lines)


# ──────────────────────────────────────────────────────────────────────────────
# VALUATION RATIOS
# EOD fields : co_code, Type, Price_BookValue, PE, DividendField, EV, EBITDA,
#              EV_EBITDA  (NOT PriceSales, DivYield as named in old code)
# ──────────────────────────────────────────────────────────────────────────────
def get_valuation_ratios(co_code: int, report_type: str = "S") -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    
    # Standardize to uppercase for API consistency
    t = report_type.upper() if report_type.upper() in ("S", "C") else "S"
    
    url = EP["valuation_ratios"].format(co_code=val, t=t)
    data, err = _get(url, f"ValuationRatios[{val}]")
    if err:
        return err
    
    rows = _rows(data)
    if not rows:
        return "No valuation ratio data found."

    # Updated FIELDS to match actual API response keys
    # Note: Using DividendYield instead of DividendField
    FIELDS = ["Year", "YRC", "co_code", "PE", "Price_BookValue", 
              "DividendYield", "EV_EBITDA", "Mcap_Sales"]
    
    lines = [f"Valuation Ratios [{'Standalone' if t == 'S' else 'Consolidated'}]:"]
    
    for row in rows[:5]:
        p = _pick(row, FIELDS)
        
        # Clean up the year formatting from YRC (e.g., 202503 -> 2025)
        yr_raw = p.get("Year") or p.get("YRC")
        yr = str(int(yr_raw))[:4] if yr_raw else "N/A"
        
        # Helper for clean float formatting
        def f(k):
            v = p.get(k)
            return f"{v:.2f}" if isinstance(v, (int, float)) else "N/A"

        lines.append(
            f"  {yr}  |  PE: {f('PE')}x"
            f"  PB: {f('Price_BookValue')}x"
            f"  EV/EBITDA: {f('EV_EBITDA')}x"
            f"  Div Yield: {f('DividendYield')}%"
            f"  Mcap/Sales: {f('Mcap_Sales')}x"
        )
        
    return "\n".join(lines)


# ──────────────────────────────────────────────────────────────────────────────
# CASH FLOW RATIOS
# EOD fields : co_code, Type, CashFlowPerShare, PriceCashFlowRatio,
#              PriceFreeCashFlow, SalestoCashFlow, SalestoCashFlowRatio  (corrected)
# ──────────────────────────────────────────────────────────────────────────────
@mcp.tool(description=(
    "Get historical cash flow ratios across years: CashFlowPerShare, PriceCashFlowRatio, "
    "PriceFreeCashFlow, SalestoCashFlow, SalestoCashFlowRatio. "
    "REQUIRES co_code — call resolve_nse_symbol first. "
    "report_type: 's' = standalone (default), 'c' = consolidated."
))
def get_cashflow_ratios(co_code: int, report_type: str = "S") -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    
    # API requires uppercase 'S' or 'C'
    t = report_type.upper() if report_type.upper() in ("S", "C") else "S"
    
    url = EP["cashflow_ratios"].format(co_code=val, t=t)
    data, err = _get(url, f"CashflowRatios[{val}]")
    if err:
        return err
    
    rows = _rows(data)
    if not rows:
        return "No cash flow ratio data found."

    # Mapped to the actual keys returned in your JSON debug log
    FIELDS = [
        "Year", "YRC", "co_code", 
        "CashFlowPerShare", "PricetoCashFlowRatio", 
        "FreeCashFlowperShare", "PricetoFreeCashFlow", "FreeCashFlowYield"
    ]
    
    lines = [f"Cash Flow Ratios [{'Standalone' if t == 'S' else 'Consolidated'}]:"]
    
    for row in rows[:5]:
        p = _pick(row, FIELDS)
        
        # Format year from YRC (202503.0 -> 2025)
        yr_raw = p.get("Year") or p.get("YRC")
        yr = str(int(yr_raw))[:4] if yr_raw else "N/A"
        
        # Helper for 2-decimal float formatting
        def f(k):
            v = p.get(k)
            return f"{v:.2f}" if isinstance(v, (int, float)) else "N/A"

        lines.append(
            f"  {yr}  |  CFPS: {f('CashFlowPerShare')}"
            f"  P/CF: {f('PricetoCashFlowRatio')}x"
            f"  FCFPS: {f('FreeCashFlowperShare')}"
            f"  P/FCF: {f('PricetoFreeCashFlow')}x"
            f"  FCF Yield: {f('FreeCashFlowYield')}%"
        )
        
    return "\n".join(lines)


# ──────────────────────────────────────────────────────────────────────────────
# GROWTH RATIOS
# Fields (Yearly/C): NetSalesGrowth, PATGrowth, EBITDAGrowth, PBTGrowth, EPSGrowth
# ──────────────────────────────────────────────────────────────────────────────
@mcp.tool(description="Provides Year-over-Year (YoY) growth percentages for Net Sales, "
          "EBITDA, EBIT, PAT, and EPS. Use this when the user asks for historical performance " 
          "trends or wants to see how fast the company is scaling."
          "REQUIRES co_code — call resolve_nse_symbol first. "
           "report_type: 's' = standalone (default), 'c' = consolidated.")
def get_growth_ratios(co_code: int, report_type: str = "s") -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err: return err
    t = report_type if report_type in ("s", "c") else "s"
    url = EP["growth_ratio"].format(co_code=val, t=t)
    data, err = _get(url, f"GrowthRatios[{val}]")
    if err: return err
    
    rows = _rows(data)
    if not rows: return "No growth data found."
    
    FIELDS = ["Year", "NetSalesGrowth", "PATGrowth", "EBITDAGrowth", "PBTGrowth", "EPSGrowth"]
    lines = [f"### Annual Growth Trends ({'Standalone' if t == 's' else 'Consolidated'})"]
    lines.append(f"{'Year':<10} | {'Sales':>8} | {'EBITDA':>8} | {'PAT':>8} | {'EPS':>8}")
    lines.append("-" * 60)

    for row in rows[:5]:
        p = _pick(row, FIELDS)
        yr = p.get("Year") or "N/A"
        lines.append(
            f"{yr:<10} | {p.get('NetSalesGrowth','0'):>7}% | {p.get('EBITDAGrowth','0'):>7}% | "
            f"{p.get('PATGrowth','0'):>7}% | {p.get('EPSGrowth','0'):>7}%"
        )
    return "\n".join(lines)

# ──────────────────────────────────────────────────────────────────────────────
# LIQUIDITY RATIOS
# EOD fields : co_code, Type, Loans_to_Deposits, Cash_vs_Deposits,
#              Incl_an_to_Deposit, Cmdr_to_Deposits,
#              InterestExpended_to_Ts, InterestExpended_to_Tl, CAR
# ──────────────────────────────────────────────────────────────────────────────
@mcp.tool(description=(
    "Get historical liquidity ratios across years: Loans_to_Deposits, Cash_vs_Deposits, "
    "Incl_an_to_Deposit, Cmdr_to_Deposits, InterestExpended_to_Ts, "
    "InterestExpended_to_Tl, CAR (Capital Adequacy Ratio). "
    "REQUIRES co_code — call resolve_nse_symbol first. "
    "report_type: 's' = standalone (default), 'c' = consolidated."
))
def get_liquidity_ratios(co_code: int, report_type: str = "S") -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    
    # API requires uppercase 'S' or 'C'
    t = report_type.upper() if report_type.upper() in ("S", "C") else "S"
    
    url = EP["liquidity_ratios"].format(co_code=val, t=t)
    data, err = _get(url, f"LiquidityRatios[{val}]")
    if err:
        return err
    
    rows = _rows(data)
    if not rows:
        return "No liquidity ratio data found."

    # Updated to match the actual API keys from your JSON response
    FIELDS = [
        "YRC", "Loans_to_Deposits", "Cash_to_Deposits", 
        "Investment_toDeposits", "IncLoan_to_Deposit", "CASA"
    ]
    
    lines = [f"Liquidity Ratios [{'Standalone' if t == 'S' else 'Consolidated'}]:"]
    
    for row in rows[:5]:
        p = _pick(row, FIELDS)
        
        # Format year from YRC (202503.0 -> 2025)
        yr_raw = p.get("YRC")
        yr = str(int(yr_raw))[:4] if yr_raw else "N/A"
        
        # Helper for 2-decimal float formatting
        def f(k):
            v = p.get(k)
            return f"{v:.2f}" if isinstance(v, (int, float)) else "N/A"

        lines.append(
            f"  {yr}  |  Loans/Dep: {f('Loans_to_Deposits')}"
            f"  Cash/Dep: {f('Cash_to_Deposits')}"
            f"  Inv/Dep: {f('Investment_toDeposits')}"
            f"  Inc. L/D: {f('IncLoan_to_Deposit')}"
            f"  CASA: {f('CASA')}%"
        )
        
    return "\n".join(lines)

# ──────────────────────────────────────────────────────────────────────────────
# SOLVENCY / LEVERAGE RATIOS  (kept from original; field names verified)
# Fields: Year/YRC, DERatio, InterestCoverage, DebtEBITDA,
#         TotalDebtEquity, LTDebtEquity
# ──────────────────────────────────────────────────────────────────────────────
@mcp.tool(description=(
    "Get historical solvency/leverage ratios across years: DERatio, InterestCoverage, "
    "DebtEBITDA, TotalDebtEquity, LTDebtEquity. "
    "REQUIRES co_code — call resolve_nse_symbol first. "
    "report_type: 's' = standalone (default), 'c' = consolidated."
))
def get_solvency_ratios(co_code: int, report_type: str = "S") -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    
    # Enforce uppercase for API consistency
    t = report_type.upper() if report_type.upper() in ("S", "C") else "S"
    
    url = EP["solvency_ratios"].format(co_code=val, t=t)
    data, err = _get(url, f"SolvencyRatios[{val}]")
    if err:
        return err
    
    rows = _rows(data)
    if not rows:
        return "No solvency ratio data found."

    # Updated to match the specific prefixed keys in the JSON response
    # Note: Using lowercase 'yrc' as per your debug log
    FIELDS = [
        "yrc", 
        "solvency_totaldebttoequityratio", 
        "solvency_interestcoverageratio", 
        "solvency_currentratio"
    ]
    
    lines = [f"Solvency Ratios [{'Standalone' if t == 'S' else 'Consolidated'}]:"]
    
    for row in rows[:5]:
        p = _pick(row, FIELDS)
        
        # API returned lowercase 'yrc' for this specific endpoint
        yr_raw = p.get("yrc")
        yr = str(int(yr_raw))[:4] if yr_raw else "N/A"
        
        # Helper for clean float formatting
        def f(k):
            v = p.get(k)
            return f"{v:.2f}" if isinstance(v, (int, float)) else "N/A"

        lines.append(
            f"  {yr}  |  D/E: {f('solvency_totaldebttoequityratio')}x"
            f"  Int Coverage: {f('solvency_interestcoverageratio')}x"
            f"  Current Ratio: {f('solvency_currentratio')}x"
        )
        
    return "\n".join(lines)

# ──────────────────────────────────────────────────────────────────────────────
# RETURN RATIOS  (renamed from old get_return_ratios; field names verified)
# Fields: Year/YRC, ROE, ROCE, ROA, ROIC
# NOTE: The Excel TTM section shows these as ROE_TTM / ROCE_TTM / ROA_TTM
#       which are served by get_daily_ratios. This tool covers yearly historical.
# ──────────────────────────────────────────────────────────────────────────────
@mcp.tool(description=(
    "Get historical return ratios across years: ROE, ROCE, ROA, ROIC. "
    "For TTM (trailing twelve months) versions use get_daily_ratios. "
    "REQUIRES co_code — call resolve_nse_symbol first. "
    "report_type: 's' = standalone (default), 'c' = consolidated."
))
def get_return_ratios(co_code: int, report_type: str = "S") -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err: return err
    t = report_type.upper() if report_type.upper() in ("S", "C") else "S"
    
    url = EP["return_ratios"].format(co_code=val, t=t)
    data, err = _get(url, f"ReturnRatios[{val}]")
    if err: return err
    
    rows = _rows(data)
    if not rows: return "No return data found."

    FIELDS = [
        "YRC", "Return_ROE", "Return_ROE_NetProfit", "Return_ROE_Networth",
        "Return_ROCE", "Return_ROCE_EBIT", "Return_ROCE_CapitalEmployed",
        "Return_ReturnOnAssets"
    ]
    
    lines = [f"### Profitability & Return Analysis ({'Standalone' if t == 'S' else 'Consolidated'})"]
    for row in rows[:5]:
        p = _pick(row, FIELDS)
        yr = str(int(p.get("YRC", 0)))[:4] if p.get("YRC") else "N/A"
        
        lines.append(f"**Year: {yr}**")
        lines.append(f"  - **ROE:** {p.get('Return_ROE', 0):.2f}% "
                     f"(PAT: {p.get('Return_ROE_NetProfit'):,.0f} / NW: {p.get('Return_ROE_Networth'):,.0f})")
        lines.append(f"  - **ROCE:** {p.get('Return_ROCE', 0):.2f}% "
                     f"(EBIT: {p.get('Return_ROCE_EBIT'):,.0f} / Cap. Emp: {p.get('Return_ROCE_CapitalEmployed'):,.0f})")
        lines.append(f"  - **ROA:** {p.get('Return_ReturnOnAssets', 0):.2f}%")
        lines.append("---")
        
    return "\n".join(lines)


# ──────────────────────────────────────────────────────────────────────────────
# ALL BASIC RATIOS  ← NEW (was missing from original code)
# TTM/Daily fields: CO_CODE, EPS, EPS_Yearly, PE, PE_Yearly, PBV,
#                   QuickRatio_Yearly, DivYield_TTM (DivField_TTM),
#                   BookValue, ROE, ROE_Yearly, ROCE, ROCE_Yearly,
#                   EBITDA_Yearly, EV, EV_EBITDA, NetIncomeMargin,
#                   PEGRatio, Growth, Qr
# ──────────────────────────────────────────────────────────────────────────────
@mcp.tool(description=(
    "Get all basic combined ratios (TTM + yearly blended): EPS, EPS_Yearly, PE, PE_Yearly, "
    "PBV, QuickRatio_Yearly, DivYield_TTM, BookValue, ROE, ROE_Yearly, ROCE, ROCE_Yearly, "
    "EBITDA_Yearly, EV, EV_EBITDA, NetIncomeMargin, PEGRatio, Growth, Qr. "
    "REQUIRES co_code — call resolve_nse_symbol first. "
    "report_type: 's' = standalone (default), 'c' = consolidated."
))
def get_all_basic_ratios(co_code: int, report_type: str = "S") -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    
    # Enforce uppercase 'S' or 'C'
    t = report_type.upper() if report_type.upper() in ("S", "C") else "S"
    
    url = EP["all_basic_ratios"].format(co_code=val, t=t)
    data, err = _get(url, f"AllBasicRatios[{val}]")
    if err:
        return err
    
    rows = _rows(data)
    if not rows:
        return "No basic ratio data found."
    
    # Mapped to the specific keys found in the Reliance Industries (476) debug log
    # Includes TTM, Yearly, and Quarterly variants
    FIELDS = [
        "lname", "EPS_Qtr", "EPS_Yearly", "EPS_TTM", "PE_Yearly", "PE_TTM",
        "DivYield_Yearly", "DivYield_TTM", "CurrentRatio_Yearly", 
        "QuickRatio_Yearly", "DebtEquityRatio_Yearly", "EBITDA_Qtr", 
        "EBITDA_Yearly", "Pat_Growth_Qtr", "NetSales_Growth_Qtr",
        "Pat_Growth_Yearly", "NetSales_Growth_Yearly", "Pat_Growth_TTM", 
        "NetSales_Growth_TTM"
    ]
    
    r = rows[0]
    p = _pick(r, FIELDS)
    
    lines = [f"### All Basic Ratios: {p.get('lname', 'Unknown Company')} (Code: {val})"]
    lines.append(f"**Reporting Type:** {'Standalone' if t == 'S' else 'Consolidated'}\n")

    # Helper for clean formatting
    def fmt(val, is_pct=False):
        if isinstance(val, (int, float)):
            return f"{val:,.2f}{'%' if is_pct else ''}"
        return "N/A"

    # Category: Valuation & Earnings
    lines.append("#### 📊 Valuation & Earnings")
    lines.append(f"  - **EPS:** TTM: {p.get('EPS_TTM')} | Yearly: {p.get('EPS_Yearly')} | Qtr: {p.get('EPS_Qtr')}")
    lines.append(f"  - **P/E Ratio:** TTM: {fmt(p.get('PE_TTM'))}x | Yearly: {fmt(p.get('PE_Yearly'))}x")
    lines.append(f"  - **Div Yield:** TTM: {fmt(p.get('DivYield_TTM'), True)} | Yearly: {fmt(p.get('DivYield_Yearly'), True)}")

    # Category: Liquidity & Solvency
    lines.append("\n#### 🛡️ Liquidity & Solvency")
    lines.append(f"  - **Current Ratio:** {fmt(p.get('CurrentRatio_Yearly'))}x")
    lines.append(f"  - **Quick Ratio:** {fmt(p.get('QuickRatio_Yearly'))}x")
    lines.append(f"  - **Debt/Equity:** {fmt(p.get('DebtEquityRatio_Yearly'))}x")

    # Category: Growth (Sales & PAT)
    lines.append("\n#### 📈 Growth Performance")
    lines.append(f"  - **Sales Growth:** TTM: {fmt(p.get('NetSales_Growth_TTM'), True)} | Yearly: {fmt(p.get('NetSales_Growth_Yearly'), True)} | Qtr: {fmt(p.get('NetSales_Growth_Qtr'), True)}")
    lines.append(f"  - **PAT Growth:**   TTM: {fmt(p.get('Pat_Growth_TTM'), True)} | Yearly: {fmt(p.get('Pat_Growth_Yearly'), True)} | Qtr: {fmt(p.get('Pat_Growth_Qtr'), True)}")
    
    # Category: Operational
    lines.append("\n#### ⚙️ Operational")
    lines.append(f"  - **EBITDA:** Yearly: {fmt(p.get('EBITDA_Yearly'))} | Qtr: {fmt(p.get('EBITDA_Qtr'))}")

    return "\n".join(lines)


# ──────────────────────────────────────────────────────────────────────────────
# QUARTERLY RESULTS BASED RATIOS  ← NEW (was missing from original code)
# Fields (Quarterly / C): CO_CODE, EV, EPS, PBV, FBV, BookValue, EBT,
#                         EBITDA, GrossIncomeMargin, EBITDAMargin, COGS,
#                         PE, PEGRatio, NetSales, IndNetprofit,
#                         DividendPayout_TTM, ROCE_TTM
# ──────────────────────────────────────────────────────────────────────────────
@mcp.tool(description=(
    "Get ratios derived from quarterly results: EV, EPS, PBV, FBV, BookValue, EBT, "
    "EBITDA, GrossIncomeMargin, EBITDAMargin, COGS, PE, PEGRatio, NetSales, "
    "IndNetprofit, DividendPayout_TTM, ROCE_TTM. "
    "REQUIRES co_code — call resolve_nse_symbol first. "
    "report_type: 's' = standalone (default), 'c' = consolidated."
))
def get_quarterly_ratios(co_code: int, report_type: str = "S") -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    
    t = report_type.upper() if report_type.upper() in ("S", "C") else "S"
    url = EP["quarterly_ratios"].format(co_code=val, t=t)
    data, err = _get(url, f"QuarterlyRatios[{val}]")
    if err:
        return err
    
    rows = _rows(data)
    if not rows:
        return "No quarterly ratio data found."

    # Mapped to the actual lowercase keys in the live JSON response
    FIELDS = [
        "qtrend", "netsales", "ebitdamargin", "netincomemargin", 
        "eps", "pe", "pbv", "netprofit", "ebitda"
    ]
    
    lines = [f"### Quarterly Performance Trends ({'Standalone' if t == 'S' else 'Consolidated'})"]
    lines.append(f"{'Quarter':<10} | {'Sales':>10} | {'EBITDA%':>9} | {'PAT%':>8} | {'EPS':>7} | {'P/E':>7}")
    lines.append("-" * 75)

    for row in rows[:5]:  # Show last 5 quarters for trend analysis
        p = _pick(row, FIELDS)
        
        # Format qtrend (e.g., 202603 -> Mar-26)
        q_raw = str(int(p.get("qtrend", 0)))
        q_label = f"{q_raw[4:]}/{q_raw[2:4]}" if len(q_raw) == 6 else q_raw
        
        def f(k, suffix="", is_pct=False):
            v = p.get(k)
            if isinstance(v, (int, float)):
                return f"{v:,.2f}{'%' if is_pct else suffix}"
            return "N/A"

        lines.append(
            f"{q_label:<10} | "
            f"{f('netsales'):>10} | "
            f"{f('ebitdamargin', is_pct=True):>9} | "
            f"{f('netincomemargin', is_pct=True):>8} | "
            f"{f('eps'):>7} | "
            f"{f('pe'):>7}"
        )
        
    return "\n".join(lines)

# ──────────────────────────────────────────────────────────────────────────────
# YEARLY RESULTS BASED RATIOS  ← NEW (was missing from original code)
# Fields (Yearly / C): CO_CODE, EV, PE, PBV, DivYield_LO, DivField_TTM,
#                      EPS, BookValue, ROE, ROA, ROCE, ROIC, EBITDA,
#                      NetIncomeMargin, GrossIncomeMargin, EBITDAMargin,
#                      AssetTurnover, FCF_Margin, NetDebt_FCF, CurrentRatio,
#                      NetDebt, LongTermDebt, TotalShareHolderEq, ShortternDebt,
#                      EBITDA_Yearly, TotalAssets, NetSales, AnnualDividend,
#                      IndNetprofit, TotalShareHolderEarnings, SectorPE
# ──────────────────────────────────────────────────────────────────────────────
@mcp.tool(description=(
    "Get ratios derived from yearly results: EV, PE, PBV, DivYield, EPS, BookValue, "
    "ROE, ROA, ROCE, ROIC, EBITDA, NetIncomeMargin, GrossIncomeMargin, EBITDAMargin, "
    "AssetTurnover, FCF_Margin, NetDebt_FCF, CurrentRatio, NetDebt, LongTermDebt, "
    "TotalShareHolderEq, ShortternDebt, TotalAssets, NetSales, AnnualDividend, "
    "IndNetprofit, TotalShareHolderEarnings, SectorPE. "
    "REQUIRES co_code — call resolve_nse_symbol first. "
    "report_type: 's' = standalone (default), 'c' = consolidated."
))
def get_yearly_ratios(co_code: int, report_type: str = "S") -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    
    # Standardize to uppercase 'S' or 'C'
    t = report_type.upper() if report_type.upper() in ("S", "C") else "S"
    
    url = EP["yearly_ratios"].format(co_code=val, t=t)
    data, err = _get(url, f"YearlyRatios[{val}]")
    if err:
        return err
    
    rows = _rows(data)
    if not rows:
        return "No yearly ratio data found."

    # Mapped to the specific lowercase/mixed keys from the live API response
    FIELDS = [
        "yearend", "netsales", "netprofit", "eps", "pe", "SectorPE", 
        "pbv", "roe", "roce", "roa", "ebitda_margin", "netincomemargin",
        "debt_equity", "netdebt_ebitda", "fcf_margin", "currentratio",
        "totalshareholdersequity", "longtermdebt", "annualdividend"
    ]
    
    lines = [f"### Yearly Financial Summary ({'Standalone' if t == 'S' else 'Consolidated'})"]
    lines.append(f"{'Year':<10} | {'Sales':>10} | {'ROE%':>7} | {'P/E':>7} | {'Sec. PE':>7} | {'D/E':>6}")
    lines.append("-" * 65)

    for row in rows[:5]:
        p = _pick(row, FIELDS)
        
        # Format Year (202503.0 -> 2025)
        yr_raw = str(int(p.get("yearend", 0)))
        yr = yr_raw[:4] if len(yr_raw) >= 4 else "N/A"
        
        def f(k, suffix="", is_pct=False, comma=False):
            v = p.get(k)
            if isinstance(v, (int, float)):
                fmt_str = f"{v:,.2f}" if comma else f"{v:.2f}"
                return f"{fmt_str}{'%' if is_pct else suffix}"
            return "N/A"

        # Table Row
        lines.append(
            f"{yr:<10} | "
            f"{f('netsales', comma=True):>10} | "
            f"{f('roe', is_pct=True):>7} | "
            f"{f('pe'):>7} | "
            f"{f('SectorPE'):>7} | "
            f"{f('debt_equity'):>6}"
        )
        
        # Add detailed margins and debt info for the most recent year only
        if row == rows[0]:
            lines.append(f"\n**Detailed Metrics (FY{yr}):**")
            lines.append(f"  - **Margins:** EBITDA: {f('ebitda_margin', is_pct=True)} | Net: {f('netincomemargin', is_pct=True)}")
            lines.append(f"  - **Liquidity:** Current Ratio: {f('currentratio')}x | FCF Margin: {f('fcf_margin', is_pct=True)}")
            lines.append(f"  - **Solvency:** Net Debt/EBITDA: {f('netdebt_ebitda')}x | Net Worth: {f('totalshareholdersequity', comma=True)}")
            lines.append(f"  - **Returns:** ROCE: {f('roce', is_pct=True)} | ROA: {f('roa', is_pct=True)}")
            lines.append("-" * 65)

    return "\n".join(lines)



# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — STOCK FINANCIAL STATEMENTS
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.tool(description=(
    "Fetches quarterly P&L results from equifizapis.cmots.com. "
    "Handles pivoted data where COLUMNNAME is the metric and Y-prefixed keys are quarters."
    "REQUIRES co_code — call resolve_nse_symbol first. "
    "report_type: 's' = standalone (default), 'c' = consolidated."
))
def get_quarterly_results(co_code: int, report_type: str = "S") -> str:
    """
    Fetches and formats quarterly financial results.
    Args:
        co_code: CMOTS Company Code.
        report_type: 'S' for Standalone, 'C' for Consolidated.
    """
    val, err = _require_int(co_code, "co_code", "get_quarterly_results")
    if err:
        return err
    
    t = report_type.upper() if report_type.upper() in ("S", "C") else "S"
    url = EP["quarterly_results"].format(co_code=val, t=t)
    
    response_data, err = _get(url, f"QuarterlyResults[{val}]")
    
    if err or not response_data.get("success"):
        return f"Error fetching data for code {val}. Message: {response_data.get('message', 'Unknown error')}"

    data = response_data.get("data", [])
    if not data:
        return "No quarterly data rows found."

    # 1. Identify Period Columns dynamically from the first row
    # Sorted reverse to get the latest dates first
    periods = sorted(
        [k for k in data[0].keys() if k.startswith("Y") and k[1:].isdigit()],
        reverse=True
    )[:5] # Showing 5 quarters is usually the sweet spot for comparison

    # 2. Extract specific metrics with strict matching
    report = {p: {} for p in periods}
    
    for row in data:
        raw_name = str(row.get("COLUMNNAME", "")).strip().upper()
        
        # Mapping logic based on your specific JSON response keys
        target_label = None
        if raw_name == "NET SALES/INCOME FROM OPERATIONS":
            target_label = "Net Sales"
        elif raw_name == "TOTAL EXPENSES":
            target_label = "Total Exp"
        elif raw_name == "NET PROFIT AFTER TAX FOR THE PERIOD":
            target_label = "PAT"
        elif "EPS AFTER" in raw_name and "BASIC" in raw_name:
            target_label = "EPS"

        if target_label:
            for p in periods:
                report[p][target_label] = row.get(p, 0.0)

    # 3. Build the Markdown Response
    lines = [f"## Quarterly Results: {t == 'S' and 'Standalone' or 'Consolidated'}", "---"]
    
    for p in periods:
        # Format Y202603 -> Mar 2026
        yr, mo = p[1:5], p[5:]
        mo_label = {"03": "Mar", "06": "Jun", "09": "Sep", "12": "Dec"}.get(mo, mo)
        
        m = report[p]
        sales = m.get('Net Sales', 0)
        pat = m.get('PAT', 0)
        eps = m.get('EPS', 0)
        
        # Calculate Margin
        margin = (pat / sales * 100) if sales > 0 else 0
        
        lines.append(
            f"### {mo_label} {yr}\n"
            f"*   **Net Sales:** ₹{sales:,.2f} Cr\n"
            f"*   **Total Exp:** ₹{m.get('Total Exp', 0):,.2f} Cr\n"
            f"*   **PAT:** ₹{pat:,.2f} Cr (Margin: **{margin:.2f}%**)\n"
            f"*   **EPS:** {eps:.2f}"
        )

    return "\n".join(lines)


@mcp.tool(description=(
    "Get annual Profit & Loss: Revenue, Operating Profit, PAT, and EPS for past years. "
    "Handles pivoted data from the CMOTS ProftandLoss API."
))
def get_profit_loss(co_code: int, report_type: str = "S") -> str:
    """
    Fetches and formats multi-year annual financial performance.
    """
    val, err = _require_int(co_code, "co_code", "get_profit_loss")
    if err: return err
    
    t = report_type.upper() if report_type.upper() in ("S", "C") else "S"
    url = EP["profit_loss"].format(co_code=val, t=t)
    
    response_data, err = _get(url, f"P&L[{val}]")
    if err or not response_data.get("success"):
        return f"Error fetching P&L data for code {val}."

    data = response_data.get("data", [])
    if not data: return "No annual data found."

    # Filter for March year-ends (03) to ensure full fiscal year comparison
    periods = sorted(
        [k for k in data[0].keys() if k.startswith("Y") and k.endswith("03")],
        reverse=True
    )[:5]

    report = {p: {} for p in periods}
    for row in data:
        name = str(row.get("COLUMNNAME", "")).strip().upper()
        
        if name == "REVENUE FROM OPERATIONS - NET":
            label = "Sales"
        elif name == "OPERATION PROFIT BEFORE DEPRECIATION":
            label = "EBITDA"
        elif name == "PROFIT AFTER TAX":
            label = "PAT"
        elif name == "EARNING PER SHARE - BASIC":
            label = "EPS"
        else:
            continue

        for p in periods:
            report[p][label] = row.get(p, 0.0)

    # Output Formatting
    lines = [f"## Annual Profit & Loss ({'Standalone' if t=='S' else 'Consolidated'})", "---"]
    for p in periods:
        m = report[p]
        sales, ebitda = m.get('Sales', 0), m.get('EBITDA', 0)
        margin = (ebitda / sales * 100) if sales > 0 else 0
        
        lines.append(
            f"### FY {p[1:5]}\n"
            f"* **Net Sales:** ₹{sales:,.2f} Cr\n"
            f"* **EBITDA:** ₹{ebitda:,.2f} Cr (Margin: **{margin:.2f}%**)\n"
            f"* **PAT:** ₹{m.get('PAT', 0):,.2f} Cr | **EPS:** {m.get('EPS', 0):.2f}"
        )

    return "\n".join(lines)



@mcp.tool(description=(
    "Get annual Balance Sheet: Total Assets, Equity, Debt, and Cash for past years. "
    "Handles pivoted data from the CMOTS BalanceSheet API."
))
def get_balance_sheet(co_code: int, report_type: str = "S") -> str:
    """
    Fetches and formats the annual Balance Sheet.
    Args:
        co_code: CMOTS Company Code.
        report_type: 'S' for Standalone, 'C' for Consolidated.
    """
    val, err = _require_int(co_code, "co_code", "get_balance_sheet")
    if err: return err
    
    t = report_type.upper() if report_type.upper() in ("S", "C") else "S"
    url = EP["balance_sheet"].format(co_code=val, t=t)
    
    response_data, err = _get(url, f"BalanceSheet[{val}]")
    if err or not response_data.get("success"):
        return f"Error fetching Balance Sheet for code {val}."

    data = response_data.get("data", [])
    if not data: return "No balance sheet data found."

    # 1. Identify Fiscal Year (March) Columns
    periods = sorted(
        [k for k in data[0].keys() if k.startswith("Y") and k.endswith("03")],
        reverse=True
    )[:5]

    # 2. Extract specific metrics using strict matching to avoid double-counting
    report = {p: {} for p in periods}
    for row in data:
        name = str(row.get("COLUMNNAME", "")).strip().upper()
        
        target_label = None
        if name == "TOTAL ASSETS":
            target_label = "Assets"
        elif name == "TOTAL SHAREHOLDER'S FUND":
            target_label = "Equity"
        elif name == "LONG TERM BORROWINGS":
            target_label = "LT Debt"
        elif name == "SHORT TERM BORROWINGS":
            target_label = "ST Debt"
        # We use RID 26 specifically for the total Cash sub-header
        elif name == "CASH AND CASH EQUIVALENTS" and row.get("RID") == 26:
            target_label = "Cash"
        
        if target_label:
            for p in periods:
                report[p][target_label] = row.get(p, 0.0)

    # 3. Build Response
    lines = [f"## Balance Sheet ({'Standalone' if t=='S' else 'Consolidated'})", "---"]
    
    for p in periods:
        m = report[p]
        assets = m.get("Assets", 0)
        equity = m.get("Equity", 0)
        lt_debt = m.get("LT Debt", 0)
        st_debt = m.get("ST Debt", 0)
        cash = m.get("Cash", 0)
        
        # Financial Health Ratios
        debt_to_equity = (lt_debt + st_debt) / equity if equity > 0 else 0
        
        lines.append(
            f"### FY {p[1:5]}\n"
            f"*   **Total Assets:** ₹{assets:,.2f} Cr\n"
            f"*   **Shareholders' Fund:** ₹{equity:,.2f} Cr\n"
            f"*   **Cash & Bank:** ₹{cash:,.2f} Cr\n"
            f"*   **Debt-to-Equity:** {debt_to_equity:.2f}"
        )
        
        if (lt_debt + st_debt) > 0:
            lines.append(f"    *(Total Debt: ₹{lt_debt + st_debt:,.2f} Cr)*")
        else:
            lines.append("    *Status: **Debt Free***")

    return "\n".join(lines)


@mcp.tool(description=(
    "Get annual Cash Flow Statement: Operating, Investing, and Financing cash flows. "
    "Calculates Free Cash Flow (FCF) from raw data."
))
def get_cash_flow(co_code: int, report_type: str = "S") -> str:
    """
    Fetches and summarizes the annual Cash Flow statement.
    """
    val, err = _require_int(co_code, "co_code", "get_cash_flow")
    if err: return err
    
    t = report_type.upper() if report_type.upper() in ("S", "C") else "S"
    url = EP["cash_flow"].format(co_code=val, t=t)
    
    response_data, err = _get(url, f"CashFlow[{val}]")
    if err or not response_data.get("success"):
        return f"Error fetching Cash Flow for code {val}."

    data = response_data.get("data", [])
    if not data: return "No cash flow data found."

    # 1. Identify Fiscal Year (March) Columns
    periods = sorted(
        [k for k in data[0].keys() if k.startswith("Y") and k.endswith("03")],
        reverse=True
    )[:5]

    # 2. Extract specific summary metrics
    report = {p: {} for p in periods}
    for row in data:
        name = str(row.get("COLUMNNAME", "")).strip().upper()
        
        # Mapping to the specific summary rows in the CMOTS JSON
        target_label = None
        if name == "NET CASH GENERATED FROM (USED IN) OPERATIONS":
            target_label = "Operating"
        elif name == "NET CASH PROVIDED BY (USED IN) INVESTING ACTIVITIES":
            target_label = "Investing"
        elif name == "CASH PROVIDED BY (USED IN) FINANCING ACTIVITIES":
            target_label = "Financing"
        elif name == "PURCHASE OF FIXED ASSETS":
            target_label = "Capex"
        elif name == "CASH AND CASH EQUIVALENTS AT END":
            target_label = "ClosingCash"
        
        if target_label:
            for p in periods:
                report[p][target_label] = row.get(p, 0.0)

    # 3. Format Response
    lines = [f"## Cash Flow Summary ({'Standalone' if t=='S' else 'Consolidated'})", "---"]
    
    for p in periods:
        m = report[p]
        op_cash = m.get("Operating", 0)
        inv_cash = m.get("Investing", 0)
        fin_cash = m.get("Financing", 0)
        capex = m.get("Capex", 0)
        
        # FCF is a key indicator of a company's ability to pay dividends/debt
        fcf = op_cash + capex 
        
        lines.append(
            f"### FY {p[1:5]}\n"
            f"*   **Cash from Operations:** ₹{op_cash:,.2f} Cr\n"
            f"*   **Cash from Investing:** ₹{inv_cash:,.2f} Cr\n"
            f"*   **Cash from Financing:** ₹{fin_cash:,.2f} Cr\n"
            f"*   **Free Cash Flow:** ₹{fcf:,.2f} Cr\n"
            f"*   **Closing Cash Balance:** ₹{m.get('ClosingCash', 0):,.2f} Cr"
        )

    return "\n".join(lines)


@mcp.tool(description=(
    "Fetches half-yearly financial results (Revenue, Profit, Tax, EPS) from equifizapis.cmots.com. "
    "Handles pivoted data where COLUMNNAME is the metric and Y-prefixed keys (e.g., Y202509) are periods."
))
def get_half_yearly_results(co_code: int, report_type: str = "S") -> str:
    val, err = _require_int(co_code, "co_code", "get_half_yearly_results")
    if err: return err
    
    t = report_type.upper() if report_type.upper() in ("S", "C") else "S"
    url = EP["half_yearly_results"].format(co_code=val, t=t)
    
    response_data, err = _get(url, f"HalfYearly[{val}]")
    if err or not response_data.get("success"):
        return f"Error fetching half-yearly data for code {val}."

    data = response_data.get("data", [])
    if not data: return "No half-yearly results found."

    # 1. Identify Period Columns
    periods = sorted(
        [k for k in data[0].keys() if k.startswith("Y") and k[1:].isdigit()],
        reverse=True
    )[:4] # Keep it concise

    # 2. Detect if this is a Bank (Look for 'Interest Earned')
    is_bank = any("INTEREST EARNED" in str(row.get("COLUMNNAME", "")).upper() for row in data)

    # 3. Define Metric Map based on Sector
    if is_bank:
        metric_map = {
            "INTEREST EARNED": "Interest Income",
            "OPERATING PROFIT": "Op Profit",
            "NET PROFIT AFTER TAX": "PAT",
            "EPS AFTER": "EPS",
            "(%) GROSS NON PERFORMING ASSETS": "GNPA %",
            "NET INTEREST MARGIN": "NIM %"
        }
    else:
        metric_map = {
            "NET SALES": "Sales",
            "TOTAL EXPENSES": "Expenses",
            "NET PROFIT AFTER TAX": "PAT",
            "EPS AFTER": "EPS"
        }

    report = {p: {} for p in periods}
    for row in data:
        raw_name = str(row.get("COLUMNNAME", "")).strip().upper()
        for key, label in metric_map.items():
            if key in raw_name:
                # For EPS, specifically look for 'BASIC' to avoid diluted overlap
                if label == "EPS" and "BASIC" not in raw_name:
                    continue
                for p in periods:
                    report[p][label] = row.get(p, 0.0)

    # 4. Format Output
    sector_label = "Banking" if is_bank else "Corporate"
    lines = [f"## Half-Yearly Results ({sector_label} | {'Standalone' if t=='S' else 'Consolidated'})", "---"]
    
    for p in periods:
        m = report[p]
        year = p[1:5]
        
        if is_bank:
            # Banking Specific Formatting
            lines.append(
                f"### H1 Ending Sep {year}\n"
                f"*   **Interest Earned:** ₹{m.get('Interest Income', 0):,.2f} Cr\n"
                f"*   **Op Profit:** ₹{m.get('Op Profit', 0):,.2f} Cr | **PAT:** ₹{m.get('PAT', 0):,.2f} Cr\n"
                f"*   **Asset Quality:** GNPA: **{m.get('GNPA %', 0):.2f}%** | NIM: **{m.get('NIM %', 0):.2f}%**\n"
                f"*   **EPS:** {m.get('EPS', 0):.2f}"
            )
        else:
            # Manufacturing/Service Formatting
            lines.append(
                f"### H1 Ending {year}\n"
                f"*   **Net Sales:** ₹{m.get('Sales', 0):,.2f} Cr\n"
                f"*   **PAT:** ₹{m.get('PAT', 0):,.2f} Cr\n"
                f"*   **EPS:** {m.get('EPS', 0):.2f}"
            )

    return "\n".join(lines)



@mcp.tool(description=(
    "Fetches nine-month cumulative financial results (Revenue, Profit, Tax, EPS) from equifizapis.cmots.com. "
    "Handles pivoted data where COLUMNNAME is the metric and Y-prefixed keys are periods."
))
def get_nine_months_results(co_code: int, report_type: str = "S") -> str:
    """
    Fetches cumulative nine-month financial results (Apr-Dec).
    """
    val, err = _require_int(co_code, "co_code", "get_nine_months_results")
    if err: return err
    
    t = report_type.upper() if report_type.upper() in ("S", "C") else "S"
    url = EP["nine_months_results"].format(co_code=val, t=t)
    
    response_data, err = _get(url, f"NineMonth[{val}]")
    if err or not response_data.get("success"):
        return f"Error fetching nine-month data for code {val}."

    data = response_data.get("data", [])
    if not data: return "No nine-month results found."

    # 1. Identify Period Columns
    periods = sorted(
        [k for k in data[0].keys() if k.startswith("Y") and k[1:].isdigit()],
        reverse=True
    )[:4]

    # 2. Map Metrics
    report = {p: {} for p in periods}
    for row in data:
        name = str(row.get("COLUMNNAME", "")).strip().upper()
        
        # Mapping based on corporate/manufacturing structure in trace
        if name == "NET SALES/INCOME FROM OPERATIONS":
            label = "Sales"
        elif name == "TOTAL EXPENSES":
            label = "Expenses"
        elif name == "TOTAL TAX":
            label = "Tax"
        elif name == "NET PROFIT AFTER TAX FOR THE PERIOD":
            label = "PAT"
        elif "EPS AFTER" in name and "BASIC" in name:
            label = "EPS"
        else:
            continue

        for p in periods:
            report[p][label] = row.get(p, 0.0)

    # 3. Format Output
    lines = [f"## 9-Month Cumulative Results ({'Standalone' if t=='S' else 'Consolidated'})", "---"]
    
    for p in periods:
        m = report[p]
        year = p[1:5]
        sales = m.get("Sales", 0)
        pat = m.get("PAT", 0)
        tax = m.get("Tax", 0)
        
        # Handle Tax Credits for clearer UX
        tax_str = f"₹{abs(tax):,.2f} Cr {'(Credit)' if tax < 0 else '(Paid)'}"
        margin = (pat / sales * 100) if sales > 0 else 0
        
        lines.append(
            f"### 9M Period Ending Dec {year}\n"
            f"*   **Cumulative Sales:** ₹{sales:,.2f} Cr\n"
            f"*   **Net Profit (PAT):** ₹{pat:,.2f} Cr (Margin: **{margin:.2f}%**)\n"
            f"*   **Tax Provision:** {tax_str}\n"
            f"*   **EPS:** {m.get('EPS', 0):.2f}"
        )

    return "\n".join(lines)




@mcp.tool(description=(
    "Fetches quarterly revenue and margin trends. "
    "Useful for identifying seasonality and efficiency shifts over time."
    "Get quarterly revenue, EBITDA, and PAT trends. "
    "REQUIRES co_code — call resolve_nse_symbol first."
))

def get_quarterly_trends(co_code: int) -> str:
    """
    Args:
        co_code: CMOTS Company Code.
    """
    val, err = _require_int(co_code, "co_code", "get_quarterly_trends")
    if err:
        return err
        
    url = EP["q_trend_revenue"].format(co_code=val)
    response_data, err = _get(url, f"QTrend[{val}]")
    
    if err or not response_data.get("success"):
        return f"Error fetching trends for code {val}."

    data = response_data.get("data", [])
    if not data:
        return "No quarterly trend data found."

    # 1. Map the keys returned by the specific Trends API
    lines = [f"## Quarterly Revenue & Margin Trends (Code: {val})", "---"]
    
    # Take the last 8 quarters to show a 2-year trend
    for row in data[:8]:
        yrc = str(row.get("yrc", ""))
        revenue = row.get("NetRevenue", 0.0)
        margin = row.get("GrossProfitmargin", 0.0)
        
        # Format YYYYMM -> e.g., Mar 2026
        if len(yrc) == 6:
            year, month = yrc[:4], yrc[4:]
            mo_label = {"03": "Mar", "06": "Jun", "09": "Sep", "12": "Dec"}.get(month, month)
            period_label = f"{mo_label} {year}"
        else:
            period_label = yrc

        lines.append(
            f"**{period_label}**\n"
            f"*   **Net Revenue:** ₹{revenue:,.2f} Cr\n"
            f"*   **Gross Margin:** {margin:.2f}%"
        )

    return "\n".join(lines)



@mcp.tool(description=(
    "Get summary shareholding pattern: Promoter, FII, DII, and Public % for recent quarters. "
    "REQUIRES co_code."
))
def get_shareholding_pattern(co_code: int) -> str:
    """
    Fetches and summarizes the shareholding structure over the last 5 quarters.
    """
    val, err = _require_int(co_code, "co_code", "get_shareholding_pattern")
    if err: return err
    
    url = EP["shareholding_detailed"].format(co_code=val)
    response_data, err = _get(url, f"Shareholding[{val}]")
    
    if err or not response_data.get("success"):
        return f"Error fetching shareholding data for code {val}."

    data = response_data.get("data", [])
    if not data: return "No shareholding data found."

    lines = [f"## Shareholding Pattern (Code: {val})", "---"]
    
    # Analyze the most recent 5 quarters
    for row in data[:5]:
        # Date formatting: 202603 -> Mar 2026
        yrc = str(int(row.get("YRC", 0)))
        year, month = yrc[:4], yrc[4:]
        mo_label = {"03": "Mar", "06": "Jun", "09": "Sep", "12": "Dec"}.get(month, month)
        
        # Primary ownership categories from JSON keys
        promoter = row.get("TotalPromoter_PerShares", 0.0)
        pledge = row.get("TotalPromoter_PerPledgeShares", 0.0)
        fii = row.get("PPIFII", 0.0)
        mf = row.get("PPIMF", 0.0)
        insurance = row.get("PPIINS", 0.0)
        public = row.get("PPSUBTOT", 0.0)
        
        # DII is typically the sum of Mutual Funds and Insurance/Banks
        dii = mf + insurance 
        
        pledge_str = f" (**{pledge:.2f}% Pledged**)" if pledge > 0 else " (No Pledge)"
        
        lines.append(
            f"### {mo_label} {year}\n"
            f"*   **Promoter:** {promoter:.2f}%{pledge_str}\n"
            f"*   **FII (Foreign Inst):** {fii:.2f}%\n"
            f"*   **DII (Domestic Inst):** {dii:.2f}% (MF: {mf:.2f}%, Ins: {insurance:.2f}%)\n"
            f"*   **Public & Others:** {public:.2f}%"
        )
        
    return "\n".join(lines)


@mcp.tool(description=(
    "Get list of major shareholders owning a significant stake (usually >1%). "
    "REQUIRES co_code."
))
def get_major_shareholders(co_code: int) -> str:
    """
    Fetches individuals/entities holding more than 1% (or significant stakes).
    """
    val, err = _require_int(co_code, "co_code", "get_major_shareholders")
    if err: return err
    
    url = EP["shareholding_1pct"].format(co_code=val)
    response_data, err = _get(url, f"MajorShareholders[{val}]")
    
    if err or not response_data.get("success"):
        return f"Error fetching major shareholders for code {val}."

    data = response_data.get("data", [])
    if not data: return "No major shareholder data found."

    # 1. Process data: Filter non-zero stakes and sort by highest ownership
    holders = [d for d in data if d.get("perstake", 0) > 0]
    holders.sort(key=lambda x: x.get("perstake", 0), reverse=True)

    if not holders:
        return "No major shareholders found for this company."

    # 2. Build Markdown
    filing_date = holders[0].get("date", "")[:10]
    lines = [f"## Major Shareholders (As of {filing_date})", "---"]
    
    # 3. Categorize for better UX (Promoter vs Public Institutions)
    for row in holders:
        name = str(row.get("Name", "Unknown")).title().strip()
        stake = row.get("perstake", 0.0)
        h_type = "Promoter Group" if "Promoter" in row.get("Type", "") else "Public/Institutional"
        
        # We use a simple bulleted list for clean scannability
        lines.append(
            f"*   **{name}**\n"
            f"    *   Stake: **{stake:.2f}%** ({int(row.get('NOOFshares', 0)):,} shares)\n"
            f"    *   Category: {h_type}"
        )

    return "\n".join(lines)


@mcp.tool(description=(
    "Fetches annual financial results (Revenue, PBT, PAT, EPS) from equifizapis.cmots.com. "
    "Handles pivoted data where metrics are rows and Y-prefixed keys (e.g., Y202603) are years."
))
def get_yearly_results(co_code: int, report_type: str = "S") -> str:
    """
    Fetches and formats audited annual financial results.
    """
    val, err = _require_int(co_code, "co_code", "get_yearly_results")
    if err: return err
    
    t = report_type.upper() if report_type.upper() in ("S", "C") else "S"
    url = EP["get_yearly_results"].format(co_code=val, t=t)
    
    response_data, err = _get(url, f"YearlyResults[{val}]")
    if err or not response_data.get("success"):
        return f"Error fetching annual results for code {val}."

    data = response_data.get("data", [])
    if not data: return "No annual results found."

    # 1. Identify Year Columns (FY ends in March '03')
    periods = sorted(
        [k for k in data[0].keys() if k.startswith("Y") and k.endswith("03")],
        reverse=True
    )[:5] # Last 5 years for trend analysis

    # 2. Map Metrics with strict normalization
    report = {p: {} for p in periods}
    for row in data:
        name = str(row.get("COLUMNNAME", "")).strip().upper()
        
        if name == "NET SALES/INCOME FROM OPERATIONS":
            label = "Sales"
        elif name == "PROFIT FROM ORDINARY ACTIVITIES BEFORE TAX":
            label = "PBT"
        elif name == "NET PROFIT AFTER TAX FOR THE PERIOD":
            label = "PAT"
        elif "EPS AFTER" in name and "BASIC" in name:
            label = "EPS"
        elif name == "DIVIDEND PER SHARE(RS.)":
            label = "Dividend"
        else:
            continue

        for p in periods:
            report[p][label] = row.get(p, 0.0)

    # 3. Format Output
    lines = [f"## Audited Annual Results ({'Standalone' if t=='S' else 'Consolidated'})", "---"]
    
    for p in periods:
        m = report[p]
        year = p[1:5]
        sales = m.get("Sales", 0)
        pat = m.get("PAT", 0)
        
        # Calculate annual margin
        margin = (pat / sales * 100) if sales > 0 else 0
        
        lines.append(
            f"### FY {year}\n"
            f"*   **Total Revenue:** ₹{sales:,.2f} Cr\n"
            f"*   **Profit Before Tax:** ₹{m.get('PBT', 0):,.2f} Cr\n"
            f"*   **Net Profit (PAT):** ₹{pat:,.2f} Cr (Margin: **{margin:.2f}%**)\n"
            f"*   **EPS:** {m.get('EPS', 0):.2f} | **Dividend:** ₹{m.get('Dividend', 0):.2f} per share"
        )

    return "\n".join(lines)


@mcp.tool(description=(
    "Retrieves the most recent quarterly balance sheet data (Assets, Liabilities, Share Capital, Debt). "
    "Use this for high-frequency tracking of a company's financial position, liquidity (Cash & Bank), "
    "and leverage (Loan Funds) between annual reports. "
    "REQUIRES co_code. report_type: 'S' for Standalone, 'C' for Consolidated."
))
def get_quarterly_balance_sheet(co_code: int, report_type: str = "S") -> str:
    """
    Fetches the quarterly balance sheet snapshot.
    """
    val, err = _require_int(co_code, "co_code", "get_quarterly_balance_sheet")
    if err: return err
    
    t = report_type.upper() if report_type.upper() in ("S", "C") else "S"
    url = EP["get_quarterly_balance_sheet"].format(co_code=val, t=t)
    
    response_data, err = _get(url, f"QuarterlyBS[{val}]")
    if err or not response_data.get("success"):
        return f"Error fetching quarterly balance sheet for code {val}."

    data = response_data.get("data", [])
    if not data: return "No data found."

    # 1. Identify Period Columns (Latest 5 quarters)
    periods = sorted(
        [k for k in data[0].keys() if k.startswith("Y") and k[1:].isdigit()],
        reverse=True
    )[:5]

    # 2. Map Metrics with strict normalization
    report = {p: {} for p in periods}
    for row in data:
        name = str(row.get("COLUMNNAME", "")).strip().upper()
        
        if name == "TOTAL ASSETS":
            label = "Assets"
        elif name == "SHARE CAPITAL":
            label = "Capital"
        elif name == "RESERVES & SURPLUS":
            label = "Reserves"
        elif name == "LOAN FUNDS":
            label = "Debt"
        elif name == "CASH & BANK BALANCE":
            label = "Cash"
        elif name == "NET CURRENT ASSETS":
            label = "WorkingCap"
        else:
            continue

        for p in periods:
            report[p][label] = row.get(p, 0.0)

    # 3. Build Response
    lines = [f"## Quarterly Balance Sheet ({'Standalone' if t=='S' else 'Consolidated'})", "---"]
    
    for p in periods:
        yr, mo = p[1:5], p[5:]
        mo_label = {"03": "Mar", "06": "Jun", "09": "Sep", "12": "Dec"}.get(mo, mo)
        m = report[p]
        
        net_worth = m.get("Capital", 0) + m.get("Reserves", 0)
        debt = m.get("Debt", 0)
        d_e_ratio = (debt / net_worth) if net_worth > 0 else 0
        
        lines.append(
            f"### {mo_label} {yr}\n"
            f"*   **Total Assets:** ₹{m.get('Assets', 0):,.2f} Cr\n"
            f"*   **Net Worth:** ₹{net_worth:,.2f} Cr\n"
            f"*   **Debt-to-Equity:** {d_e_ratio:.2f} (Debt: ₹{debt:,.2f} Cr)\n"
            f"*   **Cash & Bank:** ₹{m.get('Cash', 0):,.2f} Cr\n"
            f"*   **Working Capital:** ₹{m.get('WorkingCap', 0):,.2f} Cr"
        )

    return "\n".join(lines)

@mcp.tool(description=(
    "Retrieves the half-yearly balance sheet (Assets, Liabilities, Share Capital, Loans). "
    "Crucial for evaluating mid-year liquidity, asset growth, and capital structure changes "
    "that occur between full annual reports. "
    "REQUIRES co_code. report_type: 'S' for Standalone, 'C' for Consolidated."
))
def get_half_yearly_balance_sheet(co_code: int, report_type: str = "S") -> str:
    """
    Fetches the half-yearly (H1) balance sheet snapshot.
    """
    val, err = _require_int(co_code, "co_code", "get_half_yearly_balance_sheet")
    if err: return err
    
    t = report_type.upper() if report_type.upper() in ("S", "C") else "S"
    url = EP["get_half_yearly_balance_sheet"].format(co_code=val, t=t)
    
    response_data, err = _get(url, f"HalfYearlyBS[{val}]")
    if err or not response_data.get("success"):
        return f"Error fetching half-yearly balance sheet for code {val}."

    data = response_data.get("data", [])
    if not data: return "No data found."

    # 1. Identify Period Columns (Latest 5 H1/FY periods)
    periods = sorted(
        [k for k in data[0].keys() if k.startswith("Y") and k[1:].isdigit()],
        reverse=True
    )[:5]

    # 2. Map Metrics with strict normalization
    report = {p: {} for p in periods}
    for row in data:
        name = str(row.get("COLUMNNAME", "")).strip().upper()
        
        # Mapping to the 'Sources' and 'Application' structure of the API
        if name == "TOTAL ASSETS":
            label = "Assets"
        elif name == "SHARE CAPITAL":
            label = "Capital"
        elif name == "RESERVES & SURPLUS":
            label = "Reserves"
        elif name == "LOAN FUNDS":
            label = "Debt"
        elif name == "CASH & BANK BALANCE":
            label = "Cash"
        elif name == "FIXED ASSETS":
            label = "FixedAssets"
        else:
            continue

        for p in periods:
            report[p][label] = row.get(p, 0.0)

    # 3. Build Response
    lines = [f"## Half-Yearly Balance Sheet ({'Standalone' if t=='S' else 'Consolidated'})", "---"]
    
    for p in periods:
        yr, mo = p[1:5], p[5:]
        period_label = "H1 (Sep)" if mo == "09" else "FY End (Mar)"
        m = report[p]
        
        net_worth = m.get("Capital", 0) + m.get("Reserves", 0)
        debt = m.get("Debt", 0)
        
        lines.append(
            f"### {period_label} {yr}\n"
            f"*   **Total Assets:** ₹{m.get('Assets', 0):,.2f} Cr\n"
            f"*   **Net Worth (Equity):** ₹{net_worth:,.2f} Cr\n"
            f"*   **Total Borrowings:** ₹{debt:,.2f} Cr\n"
            f"*   **Fixed Assets (Net):** ₹{m.get('FixedAssets', 0):,.2f} Cr\n"
            f"*   **Cash Position:** ₹{m.get('Cash', 0):,.2f} Cr"
        )

    return "\n".join(lines)

@mcp.tool(description=(
    "Retrieves the annual balance sheet for multiple fiscal years. "
    "Includes critical solvency and structural data such as Share Capital, Reserves, "
    "Loan Funds (Debt), Fixed Assets, and Total Assets. "
    "Use this for long-term trend analysis of a company's net worth, leverage, and asset growth. "
    "REQUIRES co_code. report_type: 'S' for Standalone, 'C' for Consolidated."
))
def get_annual_balance_sheet(co_code: int, report_type: str = "S") -> str:
    """
    Fetches and formats audited multi-year Balance Sheets.
    """
    val, err = _require_int(co_code, "co_code", "get_annual_balance_sheet")
    if err: return err
    
    t = report_type.upper() if report_type.upper() in ("S", "C") else "S"
    url = EP["get_annual_balance_sheet"].format(co_code=val, t=t)
    
    response_data, err = _get(url, f"AnnualBS[{val}]")
    if err or not response_data.get("success"):
        return f"Error fetching annual balance sheet for code {val}."

    data = response_data.get("data", [])
    if not data: return "No annual balance sheet data found."

    # 1. Identify Fiscal Year Columns (Ending in March '03')
    periods = sorted(
        [k for k in data[0].keys() if k.startswith("Y") and k.endswith("03")],
        reverse=True
    )[:5]

    # 2. Map Metrics with strict normalization
    report = {p: {} for p in periods}
    for row in data:
        name = str(row.get("COLUMNNAME", "")).strip().upper()
        
        if name == "TOTAL ASSETS":
            label = "Assets"
        elif name == "SHARE CAPITAL":
            label = "Capital"
        elif name == "RESERVES & SURPLUS":
            label = "Reserves"
        elif name == "LOAN FUNDS":
            label = "Debt"
        elif name == "FIXED ASSETS":
            label = "FixedAssets"
        elif name == "NET CURRENT ASSETS":
            label = "WorkingCap"
        else:
            continue

        for p in periods:
            report[p][label] = row.get(p, 0.0)

    # 3. Format Output
    lines = [f"## Audited Annual Balance Sheet ({'Standalone' if t=='S' else 'Consolidated'})", "---"]
    
    for p in periods:
        m = report[p]
        year = p[1:5]
        net_worth = m.get("Capital", 0) + m.get("Reserves", 0)
        debt = m.get("Debt", 0)
        
        # Calculate Solvency (Debt-to-Equity)
        d_e_ratio = (debt / net_worth) if net_worth > 0 else 0
        
        lines.append(
            f"### FY {year}\n"
            f"*   **Net Worth (Equity):** ₹{net_worth:,.2f} Cr\n"
            f"*   **Total Debt:** ₹{debt:,.2f} Cr (D/E Ratio: **{d_e_ratio:.2f}**)\n"
            f"*   **Fixed Assets:** ₹{m.get('FixedAssets', 0):,.2f} Cr\n"
            f"*   **Net Working Capital:** ₹{m.get('WorkingCap', 0):,.2f} Cr\n"
            f"*   **Total Assets:** ₹{m.get('Assets', 0):,.2f} Cr"
        )

    return "\n".join(lines)


@mcp.tool(description=(
    "Retrieves the Trailing Twelve Months (TTM) growth trends for PAT (Profit After Tax) and Net Sales. "
    "Essential for evaluating a company's current valuation and performance by smoothing out seasonality. "
    "REQUIRES co_code. report_type: 'S' (Standalone) or 'C' (Consolidated)."
))
def get_ttm_growth_trends(co_code: int, report_type: str = "S") -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err: return err
    t = report_type.upper() if report_type.upper() in ("S", "C") else "S"
    url = EP["get_ttm_growth_trends"].format(co_code=val, t=t)
    
    response_data, err = _get(url, f"TTMGrowth[{val}]")
    if err or not response_data.get("success"):
        return f"Error fetching TTM growth data for code {val}."

    data = response_data.get("data", [])
    if not data: return "No TTM data records found."

    lines = [f"### TTM Performance Trends [{'Standalone' if t=='S' else 'Consolidated'}]", "---"]
    for row in data[:4]:
        # Mapping: Pat_Growth_TTM, NetSales_Growth_TTM
        yrc = str(int(row.get("yrc", 0)))
        lines.append(
            f"**As of {yrc}**\n"
            f"* **TTM PAT Growth:** {row.get('Pat_Growth_TTM', 0):,.2f}%\n"
            f"* **TTM Sales Growth:** {row.get('NetSales_Growth_TTM', 0):,.2f}%"
        )
    return "\n".join(lines)

@mcp.tool(description=(
    "Analyzes quarterly revenue trends and gross profit margins. "
    "Use this tool when a user asks about sales growth, top-line performance, "
    "or gross margin expansion over time. REQUIRES co_code."
))
def get_quarterly_revenue_trends(co_code: int) -> str:
    """
    Args:
        co_code: CMOTS Company Code.
    """
    val, err = _require_int(co_code, "co_code", "get_quarterly_revenue_trends")
    if err: return err
    
    # URL matches the trace: QuarterlyTrendsrevenue
    url = EP["get_quarterly_revenue_trends"].format(co_code=val)
    
    response_data, err = _get(url, f"RevTrend[{val}]")
    if err or not response_data.get("success"):
        return f"Error fetching revenue trends for code {val}."

    data = response_data.get("data", [])
    if not data:
        return "No quarterly trend data found."

    lines = [f"## Quarterly Revenue & Gross Margin Trends (Code: {val})", "---"]
    
    # Analyze the most recent 8 quarters to provide a 2-year comparative view
    for row in data[:8]:
        # Format the YRC (202603 -> Mar 2026)
        yrc_raw = str(int(row.get("yrc", 0)))
        if len(yrc_raw) == 6:
            year, month = yrc_raw[:4], yrc_raw[4:]
            mo_label = {"03": "Mar", "06": "Jun", "09": "Sep", "12": "Dec"}.get(month, month)
            period = f"{mo_label} {year}"
        else:
            period = yrc_raw

        # Correcting the keys to match the API's CamelCase response
        revenue = row.get("NetRevenue", 0.0)
        margin = row.get("GrossProfitmargin", 0.0)
        
        lines.append(
            f"**{period}**\n"
            f"*   **Net Revenue:** ₹{revenue:,.2f} Cr\n"
            f"*   **Gross Profit Margin:** {margin:.2f}%"
        )

    return "\n".join(lines)


@mcp.tool(description=(
    "Retrieves quarterly trends for EBITDA and EBITDA margins. "
    "This is the primary tool for assessing a company's operational efficiency, "
    "cash-level profitability, and margin stability. REQUIRES co_code."
))
def get_quarterly_ebitda_trends(co_code: int) -> str:
    """
    Args:
        co_code: CMOTS Company Code.
    """
    val, err = _require_int(co_code, "co_code", "get_quarterly_ebitda_trends")
    if err: return err
    
    # URL matches the trace: QuarterlyTrendEBITDA
    url = EP["get_quarterly_ebitda_trends"].format(co_code=val)
    
    response_data, err = _get(url, f"EBITDATrend[{val}]")
    if err or not response_data.get("success"):
        return f"Error fetching EBITDA trends for code {val}."

    data = response_data.get("data", [])
    if not data:
        return "No EBITDA trend data found."

    lines = [f"## Quarterly EBITDA & Margin Trends (Code: {val})", "---"]
    
    # Analyze the most recent 8 quarters for a 2-year operational view
    for row in data[:8]:
        # Format the YRC (e.g., 202603 -> Mar 2026)
        yrc_raw = str(int(row.get("yrc", 0)))
        if len(yrc_raw) == 6:
            year, month = yrc_raw[:4], yrc_raw[4:]
            mo_label = {"03": "Mar", "06": "Jun", "09": "Sep", "12": "Dec"}.get(month, month)
            period = f"{mo_label} {year}"
        else:
            period = yrc_raw

        # Correcting the keys to match the API's CamelCase response
        ebitda = row.get("EBITDA", 0.0)
        margin = row.get("EBITDAMargin", 0.0)
        
        lines.append(
            f"**{period}**\n"
            f"*   **EBITDA:** ₹{ebitda:,.2f} Cr\n"
            f"*   **EBITDA Margin:** {margin:.2f}%"
        )

    return "\n".join(lines)

@mcp.tool(description=(
    "Fetches quarterly trends for EBIT (Operating Profit) and EBIT margins. "
    "Use this for queries regarding operating leverage, depreciation impact, "
    "and core business profitability. REQUIRES co_code."
))
def get_quarterly_ebit_trends(co_code: int) -> str:
    """
    Args:
        co_code: CMOTS Company Code.
    """
    val, err = _require_int(co_code, "co_code", "get_quarterly_ebit_trends")
    if err: return err
    
    # URL matches the trace: QuarterlyTrendEBIT
    url = EP["get_quarterly_ebit_trends"].format(co_code=val)
    
    response_data, err = _get(url, f"EBITTrend[{val}]")
    if err or not response_data.get("success"):
        return f"Error fetching EBIT trends for code {val}."

    data = response_data.get("data", [])
    if not data:
        return "No EBIT trend data found."

    lines = [f"## Quarterly EBIT & Operating Margin Trends (Code: {val})", "---"]
    
    # Analyze the most recent 8 quarters for a 2-year comparative view
    for row in data[:8]:
        # Format the YRC (e.g., 202603 -> Mar 2026)
        yrc_raw = str(int(row.get("yrc", 0)))
        if len(yrc_raw) == 6:
            year, month = yrc_raw[:4], yrc_raw[4:]
            mo_label = {"03": "Mar", "06": "Jun", "09": "Sep", "12": "Dec"}.get(month, month)
            period = f"{mo_label} {year}"
        else:
            period = yrc_raw

        # Correcting the keys to match the API's CamelCase response
        ebit = row.get("EBIT", 0.0)
        margin = row.get("EBITMargin", 0.0)
        
        lines.append(
            f"**{period}**\n"
            f"*   **EBIT (Op. Profit):** ₹{ebit:,.2f} Cr\n"
            f"*   **EBIT Margin:** {margin:.2f}%"
        )

    return "\n".join(lines)


@mcp.tool(description=(
    "Retrieves quarterly Year-over-Year (YoY) growth percentages for Net Sales and PAT. "
    "Use this for analyzing short-term growth momentum and identifying performance spikes. "
    "REQUIRES co_code. report_type: 'S' for Standalone, 'C' for Consolidated."
))
def get_growth_data_quarterly(co_code: int, report_type: str = "S") -> str:
    """
    Args:
        co_code: CMOTS Company Code.
        report_type: 'S' for Standalone, 'C' for Consolidated.
    """
    val, err = _require_int(co_code, "co_code", "get_growth_data_quarterly")
    if err: return err
    
    t = report_type.upper() if report_type.upper() in ("S", "C") else "S"
    url = EP["get_growth_data_quarterly"].format(co_code=val, t=t)
    
    response_data, err = _get(url, f"QtrGrowth[{val}]")
    if err or not response_data.get("success"):
        return f"Error fetching quarterly growth data for code {val}."

    data = response_data.get("data", [])
    if not data: return "No quarterly growth records found."

    lines = [f"## Quarterly Growth Trends (YoY) ({'Standalone' if t=='S' else 'Consolidated'})", "---"]
    
    # Show last 6 quarters for a clear trend line
    for row in data[:6]:
        yrc_raw = str(int(row.get("yrc", 0)))
        if len(yrc_raw) == 6:
            year, month = yrc_raw[:4], yrc_raw[4:]
            mo_label = {"03": "Mar", "06": "Jun", "09": "Sep", "12": "Dec"}.get(month, month)
            period = f"{mo_label} {year}"
        else:
            period = yrc_raw

        pat_g = row.get("Pat_Growth_Qtr", 0.0)
        sales_g = row.get("NetSales_Growth_Qtr", 0.0)
        
        # Using :+.2f to automatically show + or - signs for growth
        lines.append(
            f"### {period}\n"
            f"*   **Net Sales Growth:** {sales_g:+.2f}%\n"
            f"*   **Net Profit (PAT) Growth:** {pat_g:+.2f}%"
        )
        
    return "\n".join(lines)

@mcp.tool(description=(
    "Retrieves annual growth percentages for Net Sales and PAT (Profit After Tax). "
    "Best for evaluating long-term compounding growth and identifying multi-year trends. "
    "REQUIRES co_code. report_type: 'S' for Standalone, 'C' for Consolidated."
))
def get_growth_data_yearly(co_code: int, report_type: str = "S") -> str:
    """
    Args:
        co_code: CMOTS Company Code.
        report_type: 'S' for Standalone, 'C' for Consolidated.
    """
    val, err = _require_int(co_code, "co_code", "get_growth_data_yearly")
    if err: return err
    
    t = report_type.upper() if report_type.upper() in ("S", "C") else "S"
    url = EP["get_growth_data_yearly"].format(co_code=val, t=t)
    
    response_data, err = _get(url, f"YearlyGrowth[{val}]")
    if err or not response_data.get("success"):
        return f"Error fetching yearly growth data for code {val}."

    data = response_data.get("data", [])
    if not data: return "No yearly growth records found."

    lines = [f"## Annual Growth Trends (YoY) ({'Standalone' if t=='S' else 'Consolidated'})", "---"]
    
    # Analyze the last 5 fiscal years
    for row in data[:5]:
        yrc_raw = str(int(row.get("yrc", 0)))
        year = yrc_raw[:4]
        
        pat_g = row.get("Pat_Growth_Yearly", 0.0)
        sales_g = row.get("NetSales_Growth_Yearly", 0.0)
        
        # Using :+.2f to clearly distinguish growth from contraction
        lines.append(
            f"### FY {year}\n"
            f"*   **Net Sales Growth:** {sales_g:+.2f}%\n"
            f"*   **Net Profit (PAT) Growth:** {pat_g:+.2f}%"
        )
        
    return "\n".join(lines)

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — COMPANY PROFILE & GOVERNANCE
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.tool(description=(
    "Retrieves the comprehensive company profile including incorporation date, "
    "industry category, chairman, auditor, face value, and corporate contact details. "
    "Use this to provide background context on a company's corporate identity. "
    "REQUIRES co_code — call resolve_nse_symbol first."
))
def get_company_profile(co_code: int) -> str:
    """
    Args:
        co_code: CMOTS Company Code.
    """
    val, err = _require_int(co_code, "co_code", "get_company_profile")
    if err: return err
    
    url = EP["company_profile"].format(co_code=val)
    response_data, err = _get(url, f"CompanyProfile[{val}]")
    
    if err or not response_data.get("success"):
        return f"Error fetching company profile for code {val}."
        
    data = response_data.get("data", [])
    if not data:
        return "No company profile data found."
    
    r = data[0]
    
    # Mapping based on UPPERCASE keys found in your terminal trace
    # Handling Address logic: Use HO address if available, fallback to REG address
    city = r.get("ho_city") or r.get("REGDIST") or ""
    state = r.get("ho_statename") or r.get("REGSTATE") or ""
    pin = r.get("ho_pin") or r.get("REGPIN") or ""
    addr_line = f"{r.get('REGADD1', '')} {r.get('REGADD2', '')}".strip()

    p = {
        "Full Name": r.get("LNAME"),
        "Industry": r.get("ind_l_name"),
        "Group": r.get("HSE_S_NAME"),
        "ISIN": r.get("ISIN"),
        "Incorporated": r.get("INC_DT"),
        "Chairman": r.get("CHAIRMAN"),
        "Auditor": r.get("AUDITOR"),
        "Face Value": f"₹{r.get('FV')}",
        "Address": f"{addr_line}, {city}, {state} - {pin}".strip(", -"),
        "Website": r.get("INTERNET"),
        "Email": r.get("EMAIL")
    }

    # Formatting for the LLM
    lines = [f"## Company Profile: {p['Full Name']}", "---"]
    
    for label, value in p.items():
        if label != "Full Name" and value and str(value).strip() not in ("", "None"):
            lines.append(f"* **{label}:** {value}")

    return "\n".join(lines)


@mcp.tool(description=(
    "Fetches the business background and sector classification of a company. "
    "Use this for queries about what a company does or its history. "
    "REQUIRES co_code — call resolve_nse_symbol first."
))
def get_company_background(co_code: int) -> str:
    """
    Args:
        co_code: CMOTS Company Code.
    """
    val, err = _require_int(co_code, "co_code", "get_company_background")
    if err: return err
    
    url = EP["comp_background"].format(co_code=val)
    response_data, err = _get(url, f"CompBackground[{val}]")
    
    if err or not response_data.get("success"):
        return f"Error fetching background for code {val}."

    data = response_data.get("data", [])
    if not data:
        return "No company background data found."
    
    r = data[0]

    # Map using UPPERCASE keys as seen in the terminal trace
    # HSE_S_NAME provides the parent group identity (e.g., ITC, Tata, Reliance)
    # ind_l_name provides the specific sector (e.g., Cigarettes, FMCG)
    name = r.get("LNAME", "N/A")
    group = r.get("HSE_S_NAME", "N/A")
    sector = r.get("ind_l_name", "N/A")
    inc_year = r.get("INC_DT", "N/A")
    
    # Memo/Background fields are often used for long-form descriptions
    description = r.get("memo") or r.get("background") or "Detailed description currently unavailable in the database."

    lines = [
        f"## Business Background: {name}",
        "---",
        f"* **Sector:** {sector}",
        f"* **Group:** {group}",
        f"* **Incorporated:** {inc_year}",
        "",
        "### Overview",
        description
    ]

    return "\n".join(lines)

@mcp.tool(description=(
    "Get board of directors with names and designations. "
    "REQUIRES co_code — call resolve_nse_symbol first."
))
def get_board_of_directors(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    url = EP["board_of_directors"].format(co_code=val)
    data, err = _get(url, f"BoardOfDirectors[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No board of directors data found."
    lines = ["Board of Directors:"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, ["dir_name", "dir_desg"])
        lines.append(f"  {i:>3}. {p.get('dir_name','N/A'):<40} — {p.get('dir_desg','N/A')}")
    return "\n".join(lines)


@mcp.tool(description=(
    "Get company's banking partners. "
    "REQUIRES co_code — call resolve_nse_symbol first."
))
def get_company_bankers(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    url = EP["bankers"].format(co_code=val)
    data, err = _get(url, f"Bankers[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No banker data found."
    lname = rows[0].get("lname", "N/A")
    banks = [r.get("bnk_name", "") for r in rows if r.get("bnk_name")]
    return f"Bankers for {lname}:\n" + "\n".join(f"  • {b}" for b in banks)


@mcp.tool(description=(
    "Get management team names, designations, and brief professional backgrounds. "
    "Note: Detailed biographies are summarized to maintain context efficiency."
))
def get_management_biodata(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
        
    url = EP["biodata"].format(co_code=val)
    data, err = _get(url, f"Biodata[{val}]")
    if err:
        return err
        
    rows = _rows(data)
    if not rows:
        return "No management biodata found."
    
    company_name = rows[0].get("lname", "N/A")
    lines = [f"### Management Team — {company_name}:"]
    
    for i, row in enumerate(rows[:12], 1):  # Limit to top 12 executives to prevent massive responses
        desc = row.get('ShortDescription', 'N/A').strip()
        memo = row.get('memo', "").strip()
        
        # --- INTELLIGENT BIODATA TRUNCATION ---
        # If there is a long biography, extract only the first 2-3 sentences.
        # This usually covers their total experience and education.
        if memo:
            # Simple regex to grab the first two sentences
            sentences = re.findall(r'[^.!?]+[.!?]', memo)
            summary = " ".join(sentences[:2]) if sentences else memo[:200]
            
            lines.append(f"{i}. **{desc}**")
            lines.append(f"   *Background:* {summary}...")
        else:
            lines.append(f"{i}. **{desc}** (No detailed biography available)")

    final_output = "\n".join(lines)
    
    # Final safeguard for token window
    if len(final_output) > 4000:
        return final_output[:3800] + "\n\n[... List truncated for brevity ...]"
        
    return final_output


@mcp.tool(description=(
    "Get subsidiaries, joint ventures, and collaborations. "
    "REQUIRES co_code — call resolve_nse_symbol first."
))
def get_subsidiaries_jvs(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    url = EP["subsidiaries"].format(co_code=val)
    data, err = _get(url, f"Subsidiaries[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No subsidiary / JV data found."
    lname = rows[0].get("lname", "N/A")
    lines = [f"Subsidiaries & JVs — {lname}:"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, ["coll_name", "coll_ctry", "PERC_SH"])
        lines.append(
            f"  {i:>3}. {p.get('coll_name','N/A')}"
            f"  [{p.get('coll_ctry','')}]"
            f"  Stake: {p.get('PERC_SH','N/A')}%"
        )
    return "\n".join(lines)

@mcp.tool(description=(
    "Get Related Party Transactions (RAG). "
    "Summarizes transactions with subsidiaries, JVs, and KMPs for the most recent financial year."
))
def get_related_party_transactions(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
        
    url = EP["related_party_transactions"].format(co_code=val)
    data, err = _get(url, f"RelatedPartyTransactions[{val}]")
    if err:
        return err
        
    rows = _rows(data)
    if not rows:
        return "No related party transaction data found."

    # 1. Get the most recent year available in the data
    latest_yrc = max(row.get('YRC', 0) for row in rows)
    latest_rows = [r for r in rows if r.get('YRC') == latest_yrc]
    
    lines = [f"### Related Party Transactions (FY {str(int(latest_yrc))[:4]}):"]
    
    # 2. Group significant transactions
    # We ignore 'Grand Total' and 'Total...' rows to show the actual line items
    # and only show rows where TOTAL > 0
    categories = {
        "Revenue/Income": [],
        "Expenses": [],
        "Assets/Investments/Loans": []
    }

    for row in latest_rows:
        name = row.get('NAT_TRANS', 'Unknown')
        total = row.get('TOTAL', 0)
        trans_type = row.get('TYPE', '')
        subtype = row.get('SUBTYPE', '')

        # Skip summary/total rows to avoid double counting in the LLM's head
        if "Total" in name or total == 0:
            continue

        entry = (f"- **{name}**: {total} Cr "
                 f"(Subsi: {row.get('SUBSI', 0)}, JV: {row.get('JV', 0)}, KMP: {row.get('KMP', 0)})")

        if trans_type == "Profit & Loss":
            if subtype == "Income":
                categories["Revenue/Income"].append(entry)
            else:
                categories["Expenses"].append(entry)
        elif trans_type == "Balance Sheet" or not trans_type:
            categories["Assets/Investments/Loans"].append(entry)

    # 3. Build the response
    for cat, items in categories.items():
        if items:
            lines.append(f"\n**{cat}:**")
            lines.extend(items)

    final_output = "\n".join(lines)
    
    # Final check to ensure we aren't flooding
    if len(final_output) > 5000:
        return final_output[:4800] + "\n\n[... Data truncated for brevity ...]"
        
    return final_output

@mcp.tool(description=(
    "Get employee count: total, male, female, contract workers. "
    "REQUIRES co_code — call resolve_nse_symbol first."
))
def get_employee_count(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    url = EP["employee_count"].format(co_code=val)
    data, err = _get(url, f"EmployeeCount[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No employee count data found."
    lines = ["Employee Count:"]
    for row in rows[:5]:
        p = _pick(row, ["yrc", "totalempoyee", "totalempoyee_male",
                        "totalempoyee_female", "totalemployee_contractbasis"])
        lines.append(
            f"  {p.get('yrc','N/A')}"
            f"  Total: {p.get('totalempoyee','N/A')}"
            f"  Male: {p.get('totalempoyee_male','N/A')}"
            f"  Female: {p.get('totalempoyee_female','N/A')}"
            f"  Contract: {p.get('totalemployee_contractbasis','N/A')}"
            f" permanent: {p.get('permenantdisabledemployee','NA')}"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get capital structure history: authorised, issued, paid-up equity capital over years. "
    "REQUIRES co_code — call resolve_nse_symbol first."
))
def get_capital_structure(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    url = EP["capital_structure"].format(co_code=val)
    data, err = _get(url, f"CapitalStructure[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No capital structure data found."
    lines = ["Capital Structure:"]
    for row in rows[:6]:
        p = _pick(row, ["YRC", "EquityAuthorised", "EquityIssued", "EquityPaidUp", "FaceValue"])
        lines.append(
            f"  {p.get('YRC','N/A')}"
            f"  Auth: {p.get('EquityAuthorised','N/A')} Cr"
            f"  Issued: {p.get('EquityIssued','N/A')} Cr"
            f"  Paid-up: {p.get('EquityPaidUp','N/A')} Cr"
            f"  FV: {p.get('FaceValue','N/A')}"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get promoter pledge share details: pledged quantity and % of total holding. "
    "REQUIRES co_code — call resolve_nse_symbol first."
))
def get_pledge_share_details(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    url = EP["pledge_shares"].format(co_code=val)
    data, err = _get(url, f"PledgeShares[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No pledge share data found."
    lines = ["Pledge Shares:"]
    for row in rows[:8]:
        p = _pick(row, ["date", "type", "name", "totalpledgeshares", "perc_totalsharesheld"])
        lines.append(
            f"  {str(p.get('date',''))[:10]}"
            f"  {p.get('type',''):<15}"
            f"  {p.get('name','N/A')}"
            f"  Pledged: {p.get('totalpledgeshares','N/A')}"
            f"  ({p.get('perc_totalsharesheld','N/A')}%)"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get chronological equity history: equity changes and remarks by year. "
    "REQUIRES co_code — call resolve_nse_symbol first."
))
def get_chronological_history(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    url = EP["chrono_history"].format(co_code=val)
    data, err = _get(url, f"ChronoHistory[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No chronological history found."
    lines = ["Chronological Equity History:"]
    for row in rows[:10]:
        p = _pick(row, ["yrc", "eqtyason", "eqty", "remarks"])
        lines.append(
            f"  {p.get('yrc','N/A')}"
            f"  Equity: {p.get('eqty','N/A')} Cr"
            f"  | {p.get('remarks','')}"
        )
    return "\n".join(lines)

import re

@mcp.tool(description=(
    "Get the historical background and key milestones of a company. "
    "Note: For large companies, this returns a summarized version to prevent context flooding."
))
def get_company_history(co_code: int) -> str:
    # Validate and ensure integer co_code
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
        
    url = EP["company_history"].format(co_code=val)
    data, err = _get(url, f"CompanyHistory[{val}]")
    
    if err:
        return err
    
    rows = _rows(data)
    if not rows or 'memo' not in rows[0]:
        return "No historical data found for this company."
        
    # Extract keys based on the provided documentation image
    full_memo = rows[0].get('memo', "").strip()
    company_name = rows[0].get('lname', 'the company')

    if not full_memo:
        return f"Historical records for {company_name} are currently empty."

    # --- INDUSTRIAL GRADE SUMMARY LOGIC ---
    
    # 1. Capture the Overview (First paragraph)
    paragraphs = [p.strip() for p in full_memo.split('\r\n') if p.strip()]
    overview = paragraphs[0] if paragraphs else ""

    # 2. Extract Year-based Milestones using Regex 
    # (Captures 'In 2024...', 'During FY2023...', etc.)
    milestone_pattern = r'(?:In|During|On)\s+(?:the\s+)?(?:year\s+)?(?:FY|fiscal\s+)?\d{4}[^.]*\.'
    all_milestones = re.findall(milestone_pattern, full_memo)

    # 3. Build the LLM-friendly response
    response_parts = [f"### Company Overview: {company_name}", overview]
    
    if all_milestones:
        response_parts.append("\n### Key Historical Milestones (Recent):")
        # Take the last 10 milestones to focus on current relevance
        recent_milestones = all_milestones[-10:]
        for m in recent_milestones:
            response_parts.append(f"- {m}")
    
    final_output = "\n".join(response_parts)

    # Final token safeguard: limit to ~4000 characters
    if len(final_output) > 4000:
        return final_output[:3800] + "\n\n[... Remaining history truncated for efficiency ...]"
        
    return final_output


@mcp.tool(description=(
    "Get substantial acquisition / insider trading disclosures. "
    "REQUIRES co_code — call resolve_nse_symbol first."
))
def get_substantial_acquisitions(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    url = EP["substantial_acq"].format(co_code=val)
    data, err = _get(url, f"SubstantialAcq[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No substantial acquisition data found."
    lines = ["Substantial Acquisitions / Insider Trades:"]
    for i, row in enumerate(rows[:10], 1):
        p = _pick(row, ["NameofAcquirer_Seller", "Acq_Sale", "TransactedQuantity",
                        "TransactedQuantity_PerChange", "TransactionPeriod"])
        lines.append(
            f"  {i:>3}. {p.get('NameofAcquirer_Seller','N/A')}"
            f"  [{p.get('Acq_Sale','N/A')}]"
            f"  Qty: {p.get('TransactedQuantity','N/A')}"
            f"  ({p.get('TransactedQuantity_PerChange','N/A')}%)"
            f"  on {str(p.get('TransactionPeriod',''))[:10]}"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get segment-wise revenue and EBIT (geography or product breakdown). "
    "segment_type: 'geography' (default) or 'product'. "
    "REQUIRES co_code. report_type: 's' or 'c'."
))
def get_segment_data(co_code: int, segment_type: str = "geography", report_type: str = "s") -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    t = report_type.lower()
    ep = "segment_geography" if segment_type.lower() == "geography" else "segment_product"
    url = EP[ep].format(co_code=val, report_type=t)
    data, err = _get(url, f"Segment[{val}/{segment_type}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No segment data found."
    lines = [f"Segment Data ({segment_type.title()}-wise):"]
    for row in rows[:10]:
        p = _pick(row, ["SegmentName", "yrc", "RevenuefromOperations",
                        "ProfitLossBeforeInterestTax", "SegmentAssets"])
        lines.append(
            f"  [{p.get('yrc','N/A')}] {p.get('SegmentName','N/A')}"
            f"  Rev: {p.get('RevenuefromOperations','N/A')} Cr"
            f"  EBIT: {p.get('ProfitLossBeforeInterestTax','N/A')} Cr"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get R&D expenditure: capital and recurring R&D spend by year. "
    "REQUIRES co_code — call resolve_nse_symbol first."
))
def get_r_and_d_expenditure(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    url = EP["r_and_d"].format(co_code=val)
    data, err = _get(url, f"R&D[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No R&D expenditure data found."
    lines = ["R&D Expenditure:"]
    for row in rows[:6]:
        p = _pick(row, ["yrc", "capital", "recurring", "percentage"])
        lines.append(
            f"  {p.get('yrc','N/A')}"
            f"  Capital: {p.get('capital','N/A')} Cr"
            f"  Recurring: {p.get('recurring','N/A')} Cr"
            f"  % of Sales: {p.get('percentage','N/A')}%"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get finished products / key product list for a manufacturing company. "
    "REQUIRES co_code — call resolve_nse_symbol first."
))
def get_finished_products(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    url = EP["finished_products"].format(co_code=val)
    data, err = _get(url, f"FinishedProducts[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No finished product data found."
    
    lines = ["Finished Products:"]
    for i, row in enumerate(rows[:15], 1):
        # Mapped keys based on the provided image schema
        p = _pick(row, [
            "prname",   # Product Name
            "uom",      # Unit of Measurement
            "inst",     # Installed Capacity
            "prodn",    # Production
            "saleqty",  # Sales Quantity
            "saleval"   # Sales Value
        ])
        
        lines.append(
            f"  {i:>3}. {p.get('prname','N/A')}"
            f"  ({p.get('uom','')})"
            f"  Capacity: {p.get('inst','N/A')}"
            f"  Production: {p.get('prodn','N/A')}"
            f"  Sales Qty: {p.get('saleqty','N/A')}"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get raw materials consumption data. "
    "REQUIRES co_code — call resolve_nse_symbol first."
))
def get_raw_materials(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    url = EP["raw_materials"].format(co_code=val)
    data, err = _get(url, f"RawMaterials[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No raw material data found."
    
    lines = ["Raw Materials:"]
    for i, row in enumerate(rows[:15], 1):
        # Mapped keys based on the provided image schema
        p = _pick(row, [
            "prname",  # Raw material name
            "uom",     # Unit of measurement
            "qty",     # Consumption quantity
            "value"    # Value in Cr
        ])
        
        lines.append(
            f"  {i:>3}. {p.get('prname','N/A')}"
            f"  ({p.get('uom','')})"
            f"  Consumption: {p.get('qty','N/A')}"
            f"  Value: {p.get('value','N/A')} Cr"
        )
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — IPO TOOLS
# ═══════════════════════════════════════════════════════════════════════════════
@mcp.tool(description="Get upcoming IPOs (announced, not yet open). exchange: 'NSE' or 'BSE'.")
def get_forthcoming_ipos(exchange: str = "NSE", count: int = 10) -> str:
    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)
    url = EP["forthcoming_ipo"].format(ex=ex, n=count)
    data, err = _get(url, f"ForthcomingIPO[{ex}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No forthcoming IPOs found."

    lines = [f"Forthcoming IPOs — {ex} ({len(rows)}):"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, [
            "lname",              # company name
            "issuetype",          # IPO / SME
            "issueprice",         # upper price band
            "issuepri2",          # lower price band
            "opendate",           # subscription open date
            "closdate",           # subscription close date
            "ListDate",           # expected listing date
            "daysleft",           # days until subscription opens
            "minqty",             # minimum lot quantity
            "maxretinv",          # max retail investment
            "issuesize",          # issue size (₹ cr)
        ])

        # price band string
        lo = p.get("issuepri2") or ""
        hi = p.get("issueprice") or ""
        if lo and hi and lo != hi:
            price_str = f"₹{lo} – ₹{hi}"
        elif hi:
            price_str = f"₹{hi}"
        else:
            price_str = "TBA"

        days = p.get("daysleft")
        days_str = f"  Opens in: {days} days" if days not in (None, "", "0", 0) else ""

        lines.append(
            f"\n  {i:>2}. {p.get('lname', 'N/A')}  [{p.get('issuetype', '')}]"
            f"\n      Price Band : {price_str}{days_str}"
            f"\n      Open: {str(p.get('opendate', ''))[:10]}  "
            f"Close: {str(p.get('closdate', ''))[:10]}  "
            f"List: {str(p.get('ListDate', ''))[:10]}"
            f"\n      Min Qty: {p.get('minqty', 'N/A')}  "
            f"Max Retail Investment: ₹{p.get('maxretinv', 'N/A')}  "
            f"Issue Size: {p.get('issuesize', 'N/A')}"
        )
    return "\n".join(lines)

@mcp.tool(description="Get IPOs currently open for subscription. exchange: 'NSE' or 'BSE'.")
def get_open_ipos(exchange: str = "NSE", count: int = 10) -> str:
    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)
    url = EP["open_ipo"].format(ex=ex, n=count)
    data, err = _get(url, f"OpenIPO[{ex}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No currently open IPOs."

    lines = [f"Open IPOs — {ex} ({len(rows)}):"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, [
            "Lname",             # company name
            "IssueType",         # IPO / SME
            "ISSUEPRICE",        # upper price band
            "ISSUEPRI2",         # lower price band
            "OPENDATE",          # subscription open
            "CLOSDATE",          # subscription close
            "LISTDATE",          # expected listing date
            "BBCLOSDATE",        # anchor investor close (if applicable)
            "MinQty",            # minimum lot quantity
            "mininvestment",     # minimum investment amount
            "IssueSize",         # issue size (₹ cr)
            "Nsesymbol",         # NSE ticker
            "Bsesymbol",         # BSE ticker
        ])

        # price band string
        lo = p.get("ISSUEPRI2") or ""
        hi = p.get("ISSUEPRICE") or ""
        if lo and hi and lo != hi:
            price_str = f"₹{lo} – ₹{hi}"
        elif hi:
            price_str = f"₹{hi}"
        else:
            price_str = "N/A"

        symbol = p.get("Nsesymbol") or p.get("Bsesymbol") or "N/A"

        lines.append(
            f"\n  {i:>2}. {p.get('Lname', 'N/A')}  [{p.get('IssueType', '')}]  ({symbol})"
            f"\n      Price Band : {price_str}"
            f"\n      Open: {str(p.get('OPENDATE', ''))[:10]}  "
            f"Close: {str(p.get('CLOSDATE', ''))[:10]}  "
            f"List: {str(p.get('LISTDATE', ''))[:10]}"
            f"\n      Min Qty: {p.get('MinQty', 'N/A')}  "
            f"Min Investment: ₹{p.get('mininvestment', 'N/A')}  "
            f"Issue Size: {p.get('IssueSize', 'N/A')}"
        )
    return "\n".join(lines)


@mcp.tool(description="Get recently closed IPOs (bidding ended). exchange: 'NSE' or 'BSE'.")
def get_closed_ipos(exchange: str = "NSE", count: int = 10) -> str:
    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)
    url = EP["closed_ipo"].format(ex=ex, n=count)
    data, err = _get(url, f"ClosedIPO[{ex}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No recently closed IPOs."

    lines = [f"Closed IPOs — {ex} ({len(rows)}):"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, [
            "lname",          # company name
            "issuetype",      # IPO / SME
            "opendate",       # bidding open
            "closdate",       # bidding close
            "listdate",       # listing date
            "issueprice",     # upper price band / final price
            "listprice",      # listing price
            "allotmentprice", # allotment price
            "issuesize",      # issue size
            "Nsesymbol",      # NSE ticker
            "Bsesymbol",      # BSE ticker
        ])

        # listing gain/loss if both prices available
        gain_str = ""
        try:
            lp = float(p.get("listprice") or 0)
            ip = float(p.get("issueprice") or 0)
            if lp and ip:
                gain = ((lp - ip) / ip) * 100
                sign = "+" if gain >= 0 else ""
                gain_str = f"  Gain: {sign}{gain:.1f}%"
        except (TypeError, ValueError):
            pass

        symbol = p.get("Nsesymbol") or p.get("Bsesymbol") or "N/A"
        lines.append(
            f"\n  {i:>2}. {p.get('lname', 'N/A')}  [{p.get('issuetype','')}]  ({symbol})"
            f"\n      Open: {str(p.get('opendate',''))[:10]}  "
            f"Close: {str(p.get('closdate',''))[:10]}  "
            f"List: {str(p.get('listdate',''))[:10]}"
            f"\n      Issue Price: {p.get('issueprice','N/A')}  "
            f"List Price: {p.get('listprice','N/A')}  "
            f"Allotment: {p.get('allotmentprice','N/A')}"
            f"{gain_str}"
            f"\n      Issue Size: {p.get('issuesize','N/A')}"
        )
    return "\n".join(lines)


@mcp.tool(description="Get recently listed IPOs with listing price, offer price, and performance. exchange: 'NSE' or 'BSE'. type: 'SME' or 'IPO'.")
def get_new_ipo_listings(exchange: str = "NSE", count: int = 10, type: str = "IPO") -> str:
    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)
    url = EP["new_listing"].format(ex=ex, n=count)
    data, err = _get(url, f"NewListing[{ex}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No recent IPO listings found."
    lines = [f"New IPO Listings — {ex} ({len(rows)}):"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, [
            "co_name",
            "lname",
            "listdate",
            "listprice",
            "listvol",
            "offerprice",
            "IssuePrice",
            "issuesize",
            "high",
            "low",
            "close",
            "perchange",
            "volume",
            "lasttr_date",
            "type",
        ])
        # Listing gain = ((listprice - offerprice) / offerprice) * 100
        try:
            lp = float(p.get("listprice") or 0)
            op = float(p.get("offerprice") or p.get("IssuePrice") or 0)
            listing_gain = round(((lp - op) / op) * 100, 2) if op else "N/A"
        except (TypeError, ZeroDivisionError):
            listing_gain = "N/A"

        company = p.get("co_name") or p.get("lname") or "N/A"
        lines.append(
            f"  {i:>3}. {company}"
            f"  |  Type: {p.get('type', 'N/A')}"
            f"  |  Listed: {str(p.get('listdate', ''))[:10]}"
            f"  |  Offer Price: {p.get('offerprice') or p.get('IssuePrice', 'N/A')}"
            f"  |  List Price: {p.get('listprice', 'N/A')}"
            f"  |  Listing Gain: {listing_gain}%"
            f"  |  Close: {p.get('close', 'N/A')}"
            f"  |  % Change: {p.get('perchange', 'N/A')}%"
            f"  |  Issue Size: {p.get('issuesize', 'N/A')}"
            f"  |  Last Traded: {str(p.get('lasttr_date', ''))[:10]}"
        )
    return "\n".join(lines)


@mcp.tool(description="Get top-performing IPOs ranked by listing gains. Returns company name, "
        "offer price, listing price, close price, listing gain %, percent change, "
        "and list date. Use for: best IPO performers, IPO listing gains, "
        "top IPOs, IPO returns, BSE/NSE IPO performance.")
def get_best_ipo_performers(exchange: str = "NSE", count: int = 10) -> str:
    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)
    url = EP["best_ipo"].format(ex=ex, n=count)
    data, err = _get(url, f"BestIPO[{ex}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No IPO performer data found."
    lines = [f"Best IPO Performers — {ex}:"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, [
            "co_name",
            "offerprice",
            "listprice",
            "close",
            "pricediff",
            "perchange",
            "per_gain_listing",
            "listdate",
            "isin",
        ])
        lines.append(
            f"  {i:>3}. {p.get('co_name', 'N/A')}"
            f"  |  Offer Price: {p.get('offerprice', 'N/A')}"
            f"  |  List Price: {p.get('listprice', 'N/A')}"
            f"  |  Close: {p.get('close', 'N/A')}"
            f"  |  Listing Gain: {p.get('per_gain_listing', 'N/A')}%"
            f"  |  % Change: {p.get('perchange', 'N/A')}%"
            f"  |  Price Diff: {p.get('pricediff', 'N/A')}"
            f"  |  Listed: {p.get('listdate', 'N/A')}"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get detailed IPO info: price band, lot size, issue size, listing date. "
    "REQUIRES co_code from resolve_nse_symbol"
))
def get_ipo_details(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "get_forthcoming_ipos / get_open_ipos")
    if err:
        return err
    url = EP["ipo_details"].format(co_code=val)
    data, err = _get(url, f"IPODetails[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No IPO detail data found."
    r = rows[0]
    p = _pick(r, [
        "companyname",
        "sectorname",
        "ipostartdate",
        "ippenddate",
        "listdate",
        "facevalue",
        "priceband1",
        "priceband2",
        "lotsize",
        "listingat",
        "employeediscount",
        "minlot",
        "mininvestment",
        "maxlot",
        "maxinvestment",
        "totalissuesize",
        "freshissue",
        "Offerforsale",
        "retaildiscount",
        "issuetype",
    ])
    lines = ["IPO Details:"]
    for k, v in p.items():
        lines.append(f"  {k:<25}: {v}")
    return "\n".join(lines)




@mcp.tool(description=(
    "Get IPO subscription status: QIB, NII, Retail subscription times. "
    "REQUIRES co_code from resolve_nse_symbol"
))
def get_ipo_subscription_status(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    url = EP["subscription_status"].format(co_code=val)
    data, err = _get(url, f"SubscriptionStatus[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No subscription data found."
    r = rows[0]

    p = _pick(r, [
        "co_code",
        "companyname",
        "QualifiedInstitutionalBuyers_QIB_Shareoffered",
        "QualifiedInstitutionalBuyers_QIB_ShareBid",
        "QualifiedInstitutionalBuyers_QIB_Subscribed",
        "NonInstitutionalInvestors_NII_Shareoffered",
        "NonInstitutionalInvestors_NII_ShareBid",
        "NonInstitutionalInvestors_NII_Subscribed",
        "RetailIndividualInvestors_RII_Shareoffered",
        "RetailIndividualInvestors_RII_ShareBid",
        "RetailIndividualInvestors_RII_Subscribed",
        "ReservationPortionShareholder_ExistingRetailShareholders_Shareoffered",
        "ReservationPortionShareholder_ExistingRetailShareholders_ShareBid",
        "ReservationPortionShareholder_ExistingRetailShareholders_Subscribed",
        "EmployeeReservation_Shareoffered",
        "EmployeeReservation_ShareBid",
        "EmployeeReservation_Subscribed",
        "GroupCompanyReservation_Shareoffered",
        "GroupCompanyReservation_ShareBid",
        "GroupCompanyReservation_Subscribed",
        "Total_Shareoffered",
        "Total_ShareBid",
        "Total_Subscribed",
    ])

    def fmt_row(label, offered_key, bid_key, subscribed_key):
        offered   = p.get(offered_key)
        bid       = p.get(bid_key)
        subscribed = p.get(subscribed_key)
        if not offered or str(offered) in ("", "0"):
            return None  # tranche not used in this IPO
        times_str = f"{subscribed}x" if subscribed not in (None, "") else "N/A"
        return f"  {label:<44}: {times_str}  (offered {offered}, bid {bid})"

    categories = [
        ("QIB",
         "QualifiedInstitutionalBuyers_QIB_Shareoffered",
         "QualifiedInstitutionalBuyers_QIB_ShareBid",
         "QualifiedInstitutionalBuyers_QIB_Subscribed"),
        ("NII / HNI",
         "NonInstitutionalInvestors_NII_Shareoffered",
         "NonInstitutionalInvestors_NII_ShareBid",
         "NonInstitutionalInvestors_NII_Subscribed"),
        ("Retail (RII)",
         "RetailIndividualInvestors_RII_Shareoffered",
         "RetailIndividualInvestors_RII_ShareBid",
         "RetailIndividualInvestors_RII_Subscribed"),
        ("Reservation (Existing Retail)",
         "ReservationPortionShareholder_ExistingRetailShareholders_Shareoffered",
         "ReservationPortionShareholder_ExistingRetailShareholders_ShareBid",
         "ReservationPortionShareholder_ExistingRetailShareholders_Subscribed"),
        ("Employee Reservation",
         "EmployeeReservation_Shareoffered",
         "EmployeeReservation_ShareBid",
         "EmployeeReservation_Subscribed"),
        ("Group Company Reservation",
         "GroupCompanyReservation_Shareoffered",
         "GroupCompanyReservation_ShareBid",
         "GroupCompanyReservation_Subscribed"),
    ]

    lines = [f"IPO Subscription Status — {p.get('companyname', 'N/A')}", "-" * 70]
    for label, o, b, s in categories:
        row = fmt_row(label, o, b, s)
        if row:
            lines.append(row)

    lines.append("-" * 70)
    lines.append(
        f"  {'TOTAL':<44}: {p.get('Total_Subscribed', 'N/A')}x  "
        f"(offered {p.get('Total_Shareoffered', 'N/A')}, "
        f"bid {p.get('Total_ShareBid', 'N/A')})"
    )
    return "\n".join(lines)

@mcp.tool(description="Get forthcoming DRHP (Draft Red Herring Prospectus) filings. exchange: 'NSE' or 'BSE'.")
def get_forthcoming_drh_filings(exchange: str = "NSE", count: int = 10) -> str:
    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)
    url = EP["forthcoming_drh"].format(ex=ex, n=count)
    data, err = _get(url, f"ForthcomingDRH[{ex}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No forthcoming DRHP filings found."

    lines = [f"Forthcoming DRHP Filings — {ex} ({len(rows)}):"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, [
            "LNAME",           # company name
            "IssueType",       # IPO / SME
            "SebiFiledDate",   # when DRHP was filed with SEBI — key date
            "OPENDATE",        # expected subscription open (may be blank)
            "CLOSDATE",        # expected subscription close (may be blank)
            "ISSUEPRICE",      # upper price band (often TBA at DRHP stage)
            "ISSUEPRI2",       # lower price band
            "IssueSize",       # issue size (₹ cr)
            "Lotsize",         # lot size
        ])

        # price band — often TBA at DRHP stage
        lo = p.get("ISSUEPRI2") or ""
        hi = p.get("ISSUEPRICE") or ""
        if lo and hi and lo != hi:
            price_str = f"₹{lo} – ₹{hi}"
        elif hi:
            price_str = f"₹{hi}"
        else:
            price_str = "TBA"

        open_str  = str(p.get("OPENDATE",  "") or "")[:10] or "TBA"
        close_str = str(p.get("CLOSDATE",  "") or "")[:10] or "TBA"

        lines.append(
            f"\n  {i:>2}. {p.get('LNAME', 'N/A')}  [{p.get('IssueType', '')}]"
            f"\n      SEBI Filed : {str(p.get('SebiFiledDate', '') or '')[:10]}"
            f"\n      Price Band : {price_str}  |  "
            f"Issue Size: {p.get('IssueSize', 'N/A')}  |  "
            f"Lot Size: {p.get('Lotsize', 'N/A')}"
            f"\n      Open: {open_str}  Close: {close_str}"
        )
    return "\n".join(lines)



# @mcp.tool(description=(
#     "Get the IPO master list: all IPOs with co_code, ISIN, company name, issue type, "
#     "open/close dates, and minimum investment. Useful for looking up co_code values "
#     "required by other IPO tools."
# ))
# def get_ipo_master() -> str:
#     data, err = _get(EP["ipo_master"], "IPOMaster")
#     if err:
#         return err
#     rows = _rows(data)
#     if not rows:
#         return "No IPO master data found."
 
#     lines = [f"IPO Master ({len(rows)} records):"]
#     for i, row in enumerate(rows, 1):
#         p = _pick(row, [
#             "co_code",
#             "isin",
#             "companyshortname",
#             "companyname",
#             "issue",
#             "issuetype",
#             "opendate",
#             "closedate",
#             "type",
#             "ipotype",
#             "freshissue",
#             "mininvestment",
#         ])
#         lines.append(
#             f"\n  {i:>3}. [{p.get('co_code', 'N/A')}] {p.get('companyname') or p.get('companyshortname', 'N/A')}"
#             f"  |  ISIN: {p.get('isin', 'N/A')}"
#             f"  |  Type: {p.get('issuetype') or p.get('ipotype', 'N/A')}"
#             f"  |  Open: {str(p.get('opendate', ''))[:10]}  Close: {str(p.get('closedate', ''))[:10]}"
#             f"  |  Min Investment: ₹{p.get('mininvestment', 'N/A')}"
#         )
#     return "\n".join(lines)
 
 
@mcp.tool(description=(
    "Get IPO synopsis: company address, objects of issue, price details, "
    "application money tranches. REQUIRES co_code from get_ipo_master / resolve_nse_symbol."
))
def get_ipo_synopsis(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "get_ipo_master")
    if err:
        return err
    url = EP["ipo_synopsis"].format(co_code=val)
    data, err = _get(url, f"IPOSynopsis[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No synopsis data found."
    r = rows[0]
    p = _pick(r, [
        "lname",
        "regadd1", "regadd2", "regdist", "regstate", "regpin",
        "tel1", "fax1", "email", "internet",
        "co_code",
        "object",
        "opendate", "closdate",
        "appnmoney1", "appnmoney2",
        "alotmoney1", "alotmoney2",
        "multiples", "min_appln",
        "projcost", "publiss1",
        "tot_eqty", "issueprice",
    ])
    address = ", ".join(filter(None, [
        p.get("regadd1"), p.get("regadd2"),
        p.get("regdist"), p.get("regstate"), p.get("regpin"),
    ]))
    lines = [
        f"IPO Synopsis — {p.get('lname', 'N/A')}",
        f"  Address      : {address}",
        f"  Tel          : {p.get('tel1', 'N/A')}  |  Fax: {p.get('fax1', 'N/A')}",
        f"  Email        : {p.get('email', 'N/A')}  |  Web: {p.get('internet', 'N/A')}",
        f"  Open         : {str(p.get('opendate', ''))[:10]}  |  Close: {str(p.get('closdate', ''))[:10]}",
        f"  Issue Price  : ₹{p.get('issueprice', 'N/A')}",
        f"  Min Appln    : {p.get('min_appln', 'N/A')}  |  Multiples: {p.get('multiples', 'N/A')}",
        f"  Project Cost : {p.get('projcost', 'N/A')}  |  Public Issue: {p.get('publiss1', 'N/A')}",
        f"  Total Equity : {p.get('tot_eqty', 'N/A')}",
        f"  Appln Money  : On Application ₹{p.get('appnmoney1', 'N/A')} / On Allotment ₹{p.get('alotmoney1', 'N/A')}",
        f"  Objects      : {p.get('object', 'N/A')}",
    ]
    return "\n".join(lines)
 
 
@mcp.tool(description=(
    "Get IPO key timeline dates: subscription open/close, allotment date, "
    "refund date, demat credit date, and listing date. "
    "REQUIRES co_code from get_ipo_master / resolve_nse_symbol."
))
def get_ipo_timeline(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "get_ipo_master")
    if err:
        return err
    url = EP["ipo_timeline"].format(co_code=val)
    data, err = _get(url, f"IPOTimeline[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No timeline data found."
    r = rows[0]
    p = _pick(r, [
        "co_code",
        "ipostartdate",
        "ippenddate",
        "AllotmentDate",
        "RefundDate",
        "CreditofsharestoDemataccountDate",
        "ListingDate",
    ])
    lines = [
        f"IPO Timeline — co_code {p.get('co_code', val)}",
        f"  Subscription Open  : {str(p.get('ipostartdate', 'N/A'))[:10]}",
        f"  Subscription Close : {str(p.get('ippenddate', 'N/A'))[:10]}",
        f"  Allotment Date     : {str(p.get('AllotmentDate', 'N/A'))[:10]}",
        f"  Refund Date        : {str(p.get('RefundDate', 'N/A'))[:10]}",
        f"  Demat Credit Date  : {str(p.get('CreditofsharestoDemataccountDate', 'N/A'))[:10]}",
        f"  Listing Date       : {str(p.get('ListingDate', 'N/A'))[:10]}",
    ]
    return "\n".join(lines)
 
 
@mcp.tool(description=(
    "Get IPO promoter details: promoter names, pre- and post-issue shareholding "
    "shares and percentages. REQUIRES co_code from get_ipo_master / resolve_nse_symbol."
))
def get_ipo_promoter_details(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "get_ipo_master")
    if err:
        return err
    url = EP["ipo_promoter_details"].format(co_code=val)
    data, err = _get(url, f"IPOPromoterDetails[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No promoter details found."
 
    company = rows[0].get("CompanyName") or rows[0].get("companyname", "N/A")
    lines = [f"IPO Promoter Details — {company}"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, [
            "CompanyCode",
            "CompanyName",
            "issuetype",
            "sno",
            "PromotersName",
            "PreIssueShares",
            "PreIssuePercentage",
            "PostIssueShares",
            "PostIssuePercentage",
        ])
        lines.append(
            f"\n  {i:>2}. {p.get('PromotersName', 'N/A')}"
            f"\n      Pre-Issue : {p.get('PreIssueShares', 'N/A')} shares "
            f"({p.get('PreIssuePercentage', 'N/A')}%)"
            f"\n      Post-Issue: {p.get('PostIssueShares', 'N/A')} shares "
            f"({p.get('PostIssuePercentage', 'N/A')}%)"
        )
    return "\n".join(lines)
 
 
@mcp.tool(description=(
    "Get IPO listing info: listing date, BSE code, NSE symbol, ISIN, and final issue price. "
    "REQUIRES co_code from get_ipo_master / resolve_nse_symbol."
))
def get_ipo_listing_info(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "get_ipo_master")
    if err:
        return err
    url = EP["ipo_listing_info"].format(co_code=val)
    data, err = _get(url, f"IPOListingInfo[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No listing info found."
    r = rows[0]
    p = _pick(r, [
        "co_code",
        "companyname",
        "listdate",
        "bsecode",
        "nsesymbol",
        "isin",
        "FinalIssuePrice",
    ])
    lines = [
        f"IPO Listing Info — {p.get('companyname', 'N/A')}",
        f"  Listing Date     : {str(p.get('listdate', 'N/A'))[:10]}",
        f"  NSE Symbol       : {p.get('nsesymbol', 'N/A')}",
        f"  BSE Code         : {p.get('bsecode', 'N/A')}",
        f"  ISIN             : {p.get('isin', 'N/A')}",
        f"  Final Issue Price: ₹{p.get('FinalIssuePrice', 'N/A')}",
    ]
    return "\n".join(lines)
 
 
@mcp.tool(description=(
    "Get IPO registrar details: registrar name, phone number, email, and website. "
    "REQUIRES co_code from get_ipo_master / resolve_nse_symbol."
))
def get_ipo_registrar(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "get_ipo_master")
    if err:
        return err
    url = EP["ipo_registrar"].format(co_code=val)
    data, err = _get(url, f"IPORegistrar[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No registrar data found."
    r = rows[0]
    p = _pick(r, [
        "co_code",
        "companyname",
        "RegistrarName",
        "RegistrarPhoneNumber",
        "RegistrarEmailid",
        "RegistrarWebsite",
    ])
    lines = [
        f"IPO Registrar — {p.get('companyname', 'N/A')}",
        f"  Registrar : {p.get('RegistrarName', 'N/A')}",
        f"  Phone     : {p.get('RegistrarPhoneNumber', 'N/A')}",
        f"  Email     : {p.get('RegistrarEmailid', 'N/A')}",
        f"  Website   : {p.get('RegistrarWebsite', 'N/A')}",
    ]
    return "\n".join(lines)
 
 
@mcp.tool(description=(
    "Get IPO lead managers (book running lead managers / BRLMs). "
    "REQUIRES co_code from get_ipo_master / resolve_nse_symbol."
))
def get_ipo_lead_managers(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "get_ipo_master")
    if err:
        return err
    url = EP["ipo_lead_manager"].format(co_code=val)
    data, err = _get(url, f"IPOLeadManager[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No lead manager data found."
 
    company = rows[0].get("companyname", "N/A")
    lines = [f"IPO Lead Managers — {company}"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, ["co_code", "companyname", "LeadManager"])
        lines.append(f"  {i:>2}. {p.get('LeadManager', 'N/A')}")
    return "\n".join(lines)
 
 
@mcp.tool(description=(
    "Get IPO prospectus list filed with SEBI. Optionally filter by company name (partial match) "
    "or co_code. If neither is provided, returns the first 20 records. "
    "Fields: co_code, lname, opendate, closdate, dpdate, dp, dpclear, VOLYR, VOLSRNO, ClosDate."
))
def get_ipo_prospectus(company_name: str = "", co_code: int = 0) -> str:
    url = EP["ipo_prospectus"]
    data, err = _get(url, "IPOProspectus")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No prospectus data found."

    # ── Filter ───────────────────────────────────────────────────────────────
    if co_code and int(co_code) > 0:
        rows = [r for r in rows if str(r.get("co_code", "")) == str(co_code)]
    elif company_name.strip():
        keyword = company_name.strip().lower()
        rows = [r for r in rows if keyword in str(r.get("lname", "")).lower()]

    if not rows:
        return f"No prospectus records found matching your query."

    # ── If no filter given, cap at 20 to avoid huge output ──────────────────
    if not co_code and not company_name.strip():
        rows = rows[:20]

    lines = [f"IPO Prospectus (SEBI filings) — {len(rows)} record(s):"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, [
            "co_code",
            "lname",
            "opendate",
            "closdate",
            "ClosDate",    # alternate casing seen in some responses
            "dpdate",
            "dp",
            "dpclear",
            "VOLYR",
            "VOLSRNO",
        ])
        close = str(p.get("closdate") or p.get("ClosDate") or "")[:10]
        lines.append(
            f"\n  {i:>3}. [{p.get('co_code', 'N/A')}] {p.get('lname', 'N/A')}"
            f"\n       Open   : {str(p.get('opendate', ''))[:10]}  |  Close: {close}"
            f"\n       DP Date: {str(p.get('dpdate', ''))[:10]}  |  DP: {p.get('dp', 'N/A')}"
            f"\n       DP Clear: {p.get('dpclear', 'N/A')}  |  "
            f"Vol/Yr: {p.get('VOLYR', 'N/A')}  |  Vol SrNo: {p.get('VOLSRNO', 'N/A')}"
        )
    return "\n".join(lines)
 
 
@mcp.tool(description=(
    "Get detailed IPO allocation breakdown: QIB / NII / Retail share percentages, "
    "fresh issue vs OFS split, price range, issue size range, anchor investor portion, "
    "business summary, industry summary, promoter pre/post shareholding. "
    "REQUIRES co_code from get_ipo_master / resolve_nse_symbol."
))
def get_ipo_allocation_details(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "get_ipo_master")
    if err:
        return err
    url = EP["ipo_allocation_details"].format(co_code=val)
    data, err = _get(url, f"IPOAllocationDetails[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No allocation detail data found."
    r = rows[0]
    p = _pick(r, [
        "companycode", "isin", "companyname", "issuetype",
        "freshissue_noofshares", "freshissue_amountrscr",
        "offerforsale_noofshares", "offerforsale_amountrscr",
        "total_noofshares", "total_amountrscr",
        "pricefixed", "pricerangefrom", "pricerangeto",
        "issuesizefrom", "issuesizeto",
        "sharesofferedtoexistingshareholders",
        "sharesofferedtoemployees",
        "sharesofferedtomarketmakers",
        "totalsharesofferedtoqib",
        "ofwhichanchorinvestors",
        "balanceavailableforallocationtoqibsotherthananchorinvestors",
        "availableforallocationtomutualfundsonly",
        "balanceofqibportionforallqibsincludingmutualfunds",
        "sharesofferedtonon_institutionalportion",
        "sharesofferedtoretailportion",
        "sharesofferedtononretail",
        "qib_per", "non_institutionalportion_per", "retailportion_per",
        "finalisationofbasisofallotmentwiththedesignatedstockexchange",
        "initiationofrefunds",
        "creditofequitysharestodemataccountsofallottees",
        "commencementoftradingoftheequitysharesonthestockexchanges",
        "creditratingby", "creditrating",
        "businesssummary", "industrysummary", "companyhistory",
        "promoterspreshareholdingnoofshares",
        "promoterspreshareholdingpercentage",
        "promoterspostshareholdingnoofshares",
        "comments",
    ])
 
    # price band
    if p.get("pricerangefrom") and p.get("pricerangeto"):
        price_str = f"₹{p['pricerangefrom']} – ₹{p['pricerangeto']}"
    elif p.get("pricefixed"):
        price_str = f"₹{p['pricefixed']} (fixed)"
    else:
        price_str = "N/A"
 
    lines = [
        f"IPO Allocation Details — {p.get('companyname', 'N/A')}  [{p.get('issuetype', '')}]",
        f"  ISIN          : {p.get('isin', 'N/A')}",
        f"  Price Band    : {price_str}",
        f"  Issue Size    : ₹{p.get('issuesizefrom', 'N/A')} – ₹{p.get('issuesizeto', 'N/A')} cr",
        "",
        "  ── Issue Structure ──",
        f"  Fresh Issue   : {p.get('freshissue_noofshares', 'N/A')} shares  "
            f"(₹{p.get('freshissue_amountrscr', 'N/A')} cr)",
        f"  OFS           : {p.get('offerforsale_noofshares', 'N/A')} shares  "
            f"(₹{p.get('offerforsale_amountrscr', 'N/A')} cr)",
        f"  Total         : {p.get('total_noofshares', 'N/A')} shares  "
            f"(₹{p.get('total_amountrscr', 'N/A')} cr)",
        "",
        "  ── Allocation Split ──",
        f"  QIB           : {p.get('totalsharesofferedtoqib', 'N/A')} shares  "
            f"({p.get('qib_per', 'N/A')}%)",
        f"    of which Anchor Investors : {p.get('ofwhichanchorinvestors', 'N/A')}",
        f"    Balance for other QIBs    : {p.get('balanceavailableforallocationtoqibsotherthananchorinvestors', 'N/A')}",
        f"    of which MF only          : {p.get('availableforallocationtomutualfundsonly', 'N/A')}",
        f"    All QIBs incl MF (balance): {p.get('balanceofqibportionforallqibsincludingmutualfunds', 'N/A')}",
        f"  NII           : {p.get('sharesofferedtonon_institutionalportion', 'N/A')} shares  "
            f"({p.get('non_institutionalportion_per', 'N/A')}%)",
        f"  Retail        : {p.get('sharesofferedtoretailportion', 'N/A')} shares  "
            f"({p.get('retailportion_per', 'N/A')}%)",
        f"  Non-Retail    : {p.get('sharesofferedtononretail', 'N/A')} shares",
        f"  Employee Rsv  : {p.get('sharesofferedtoemployees', 'N/A')} shares",
        f"  Existing SH   : {p.get('sharesofferedtoexistingshareholders', 'N/A')} shares",
        f"  Market Maker  : {p.get('sharesofferedtomarketmakers', 'N/A')} shares",
        "",
        "  ── Key Dates ──",
        f"  Allotment Finalisation : {str(p.get('finalisationofbasisofallotmentwiththedesignatedstockexchange', ''))[:10]}",
        f"  Refund Initiation      : {str(p.get('initiationofrefunds', ''))[:10]}",
        f"  Demat Credit           : {str(p.get('creditofequitysharestodemataccountsofallottees', ''))[:10]}",
        f"  Trading Commencement   : {str(p.get('commencementoftradingoftheequitysharesonthestockexchanges', ''))[:10]}",
        "",
        "  ── Credit Rating ──",
        f"  Rated by: {p.get('creditratingby', 'N/A')}  |  Rating: {p.get('creditrating', 'N/A')}",
        "",
        "  ── Promoter Holding ──",
        f"  Pre-Issue : {p.get('promoterspreshareholdingnoofshares', 'N/A')} shares  "
            f"({p.get('promoterspreshareholdingpercentage', 'N/A')}%)",
        f"  Post-Issue: {p.get('promoterspostshareholdingnoofshares', 'N/A')} shares",
        "",
        f"  Business Summary : {p.get('businesssummary', 'N/A')}",
        f"  Industry Summary : {p.get('industrysummary', 'N/A')}",
        f"  Comments         : {p.get('comments', 'N/A')}",
    ]
    return "\n".join(lines)
 
 
@mcp.tool(description=(
    "Get IPO selling shareholder details: who is selling in the OFS, "
    "category, number of shares offered, and pre/post holding percentages. "
    "REQUIRES co_code from get_ipo_master / resolve_nse_symbol."
))
def get_ipo_selling_shareholders(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "get_ipo_master")
    if err:
        return err
    url = EP["ipo_selling_shareholders"].format(co_code=val)
    data, err = _get(url, f"IPOSellingShareholders[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No selling shareholder data found."
 
    company = rows[0].get("companyname", "N/A")
    lines = [f"IPO Selling Shareholders (OFS) — {company}"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, [
            "companycode", "companyname", "issuetype", "sno",
            "sellingshareholders", "category",
            "noofsharesoffered",
            "preholdingshares", "preholding_per",
            "postholdingshares", "postholding_per",
        ])
        lines.append(
            f"\n  {i:>2}. {p.get('sellingshareholders', 'N/A')}  [{p.get('category', 'N/A')}]"
            f"\n      Shares Offered: {p.get('noofsharesoffered', 'N/A')}"
            f"\n      Pre-Holding   : {p.get('preholdingshares', 'N/A')} shares "
            f"({p.get('preholding_per', 'N/A')}%)"
            f"\n      Post-Holding  : {p.get('postholdingshares', 'N/A')} shares "
            f"({p.get('postholding_per', 'N/A')}%)"
        )
    return "\n".join(lines)
 
 
@mcp.tool(description=(
    "Get IPO industry peer comparison: peer companies' EPS (basic/diluted), NAV per share, "
    "P/E ratio, RoNW, face value, and total income — useful for valuation benchmarking. "
    "REQUIRES co_code from get_ipo_master / resolve_nse_symbol."
))
def get_ipo_industry_peers(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "get_ipo_master")
    if err:
        return err
    url = EP["ipo_industry_peers"].format(co_code=val)
    data, err = _get(url, f"IPOIndustryPeers[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No industry peer data found."
 
    company = rows[0].get("companyname", "N/A")
    lines = [f"IPO Industry Peer Comparison — {company}"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, [
            "companycode", "companyname", "issuetype", "sno",
            "companyname_peercompany",
            "facevalue",
            "standalone_consolidated",
            "yrc",
            "totalincome",
            "epsbasis", "epsdiluted",
            "navpershare",
            "pebasiceps", "pedilutedeps",
            "ronw_per",
            "latestnavperiod", "latestnav",
            "comment",
        ])
        lines.append(
            f"\n  {i:>2}. {p.get('companyname_peercompany', 'N/A')}  "
            f"[{p.get('standalone_consolidated', '')}]  FY: {p.get('yrc', 'N/A')}"
            f"\n      FV: ₹{p.get('facevalue', 'N/A')}  |  Total Income: {p.get('totalincome', 'N/A')}"
            f"\n      EPS Basic: {p.get('epsbasis', 'N/A')}  |  EPS Diluted: {p.get('epsdiluted', 'N/A')}"
            f"\n      NAV/Share: {p.get('navpershare', 'N/A')}  |  Latest NAV ({p.get('latestnavperiod', '')}): "
            f"{p.get('latestnav', 'N/A')}"
            f"\n      P/E Basic: {p.get('pebasiceps', 'N/A')}  |  P/E Diluted: {p.get('pedilutedeps', 'N/A')}"
            f"\n      RoNW: {p.get('ronw_per', 'N/A')}%"
        )
    return "\n".join(lines)
 
 
@mcp.tool(description=(
    "Get IPO risk factors listed in the prospectus. "
    "REQUIRES co_code from get_ipo_master / resolve_nse_symbol."
))
def get_ipo_risk_details(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "get_ipo_master")
    if err:
        return err
    url = EP["ipo_risk_details"].format(co_code=val)
    data, err = _get(url, f"IPORiskDetails[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No risk detail data found."
 
    company = rows[0].get("companyname", "N/A")
    lines = [f"IPO Risk Factors — {company}"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, [
            "companycode", "companyname", "issuetype",
            "sno", "risktype", "riskdetails",
        ])
        lines.append(
            f"\n  {i:>2}. [{p.get('risktype', 'N/A')}]\n      {p.get('riskdetails', 'N/A')}"
        )
    return "\n".join(lines)
 
 
@mcp.tool(description=(
    "Get IPO business strategies listed in the prospectus. "
    "REQUIRES co_code from get_ipo_master / resolve_nse_symbol."
))
def get_ipo_strategy_details(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "get_ipo_master")
    if err:
        return err
    url = EP["ipo_strategy_details"].format(co_code=val)
    data, err = _get(url, f"IPOStrategyDetails[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No strategy detail data found."
 
    company = rows[0].get("companyname", "N/A")
    lines = [f"IPO Business Strategies — {company}"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, [
            "companycode", "companyname", "issuetype",
            "sno", "strategydetails",
        ])
        lines.append(f"\n  {i:>2}. {p.get('strategydetails', 'N/A')}")
    return "\n".join(lines)
 
 
@mcp.tool(description=(
    "Get IPO competitive strengths listed in the prospectus. "
    "REQUIRES co_code from get_ipo_master / resolve_nse_symbol."
))
def get_ipo_strength_details(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "get_ipo_master")
    if err:
        return err
    url = EP["ipo_strength_details"].format(co_code=val)
    data, err = _get(url, f"IPOStrengthDetails[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No strength detail data found."
 
    company = rows[0].get("companyname", "N/A")
    lines = [f"IPO Competitive Strengths — {company}"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, [
            "companycode", "companyname", "issuetype",
            "sno", "strengthdetails",
        ])
        lines.append(f"\n  {i:>2}. {p.get('strengthdetails', 'N/A')}")
    return "\n".join(lines)
 
 
@mcp.tool(description=(
    "Get IPO product and service details from the prospectus. "
    "REQUIRES co_code from get_ipo_master / resolve_nse_symbol."
))
def get_ipo_product_services(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "get_ipo_master")
    if err:
        return err
    url = EP["ipo_product_services"].format(co_code=val)
    data, err = _get(url, f"IPOProductServices[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No product/service data found."
 
    company = rows[0].get("CompanyName", "N/A")
    lines = [f"IPO Products & Services — {company}"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, [
            "CompanyCode", "CompanyName", "issuetype",
            "sno", "Product_Services_Details",
        ])
        lines.append(f"\n  {i:>2}. {p.get('Product_Services_Details', 'N/A')}")
    return "\n".join(lines)
 
 
@mcp.tool(description=(
    "Get IPO customer details from the prospectus. "
    "REQUIRES co_code from get_ipo_master / resolve_nse_symbol."
))
def get_ipo_customer_details(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "get_ipo_master")
    if err:
        return err
    url = EP["ipo_customer_details"].format(co_code=val)
    data, err = _get(url, f"IPOCustomerDetails[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No customer data found."
 
    company = rows[0].get("CompanyName", "N/A")
    lines = [f"IPO Customer Details — {company}"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, [
            "CompanyCode", "CompanyName", "issuetype",
            "sno", "CustomerDetails",
        ])
        lines.append(f"\n  {i:>2}. {p.get('CustomerDetails', 'N/A')}")
    return "\n".join(lines)
 
 
@mcp.tool(description=(
    "Get IPO financial statements: total assets, total revenue, profit, total liabilities, "
    "total expenditure, EBITDA, share capital. "
    "report_type: 'S' for Standalone, 'C' for Consolidated. "
    "REQUIRES co_code from get_ipo_master / resolve_nse_symbol."
))
def get_ipo_financials(co_code: int, report_type: str = "S") -> str:
    val, err = _require_int(co_code, "co_code", "get_ipo_master")
    if err:
        return err
    rt = report_type.upper()
    if rt not in ("S", "C"):
        return "report_type must be 'S' (Standalone) or 'C' (Consolidated)."
    url = EP["ipo_financials"].format(co_code=val, report_type=rt)
    label = "Standalone" if rt == "S" else "Consolidated"
    data, err = _get(url, f"IPOFinancials[{val}/{rt}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No IPO financial data found."
 
    lines = [f"IPO Financials ({label}) — co_code {val}"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, [
            "co_code", "yrc", "sect_name",
            "totalassets", "totalrevenue", "profit",
            "TotalLiabilities", "TotalExpenditure",
            "ebitda", "sharecapital",
        ])
        lines.append(
            f"\n  {i:>2}. FY: {p.get('yrc', 'N/A')}  |  Sector: {p.get('sect_name', 'N/A')}"
            f"\n      Total Assets     : {p.get('totalassets', 'N/A')}"
            f"\n      Total Revenue    : {p.get('totalrevenue', 'N/A')}"
            f"\n      Profit           : {p.get('profit', 'N/A')}"
            f"\n      Total Liabilities: {p.get('TotalLiabilities', 'N/A')}"
            f"\n      Total Expenditure: {p.get('TotalExpenditure', 'N/A')}"
            f"\n      EBITDA           : {p.get('ebitda', 'N/A')}"
            f"\n      Share Capital    : {p.get('sharecapital', 'N/A')}"
        )
    return "\n".join(lines)
 
 
@mcp.tool(description=(
    "Get IPO anchor investor details: bid date, shares offered to anchors, "
    "anchor portion size (₹ cr), and lock-in period breakdown (30-day and 90-day). "
    "REQUIRES co_code from get_ipo_master / resolve_nse_symbol."
))
def get_ipo_anchor_investor_details(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "get_ipo_master")
    if err:
        return err
    url = EP["anchor_investor_details"].format(co_code=val)
    data, err = _get(url, f"AnchorInvestorDetails[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No anchor investor data found."
    r = rows[0]
    p = _pick(r, [
        "co_code",
        "biddate",
        "sharesoffered",
        "Anchor_PortionSize_Cr",
        "Anchor_lockin_50perc_shares_30Days",
        "Anchor_lockin_remaining_shares_90Days",
    ])
    lines = [
        f"IPO Anchor Investor Details — co_code {p.get('co_code', val)}",
        f"  Bid Date               : {str(p.get('biddate', 'N/A'))[:10]}",
        f"  Shares Offered         : {p.get('sharesoffered', 'N/A')}",
        f"  Anchor Portion Size    : ₹{p.get('Anchor_PortionSize_Cr', 'N/A')} cr",
        f"  Lock-in 30 Days (50%)  : {p.get('Anchor_lockin_50perc_shares_30Days', 'N/A')} shares",
        f"  Lock-in 90 Days (rest) : {p.get('Anchor_lockin_remaining_shares_90Days', 'N/A')} shares",
    ]
    return "\n".join(lines)
 
 
@mcp.tool(description=(
    "Get objects/use of proceeds of an IPO from the prospectus. "
    "REQUIRES co_code from get_ipo_master / resolve_nse_symbol."
))
def get_ipo_objects_of_issue(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "get_ipo_master")
    if err:
        return err
    url = EP["objects_of_issue"].format(co_code=val)
    data, err = _get(url, f"ObjectsOfIssue[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No objects of issue data found."
 
    lines = [f"Objects of the Issue — co_code {val}"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, ["project"])
        lines.append(f"  {i:>2}. {p.get('project', 'N/A')}")
    return "\n".join(lines)
 
 
@mcp.tool(description=(
    "Get IPOs where basis of allotment has been finalised. "
    "Returns company name, issue type, close date, and allotment reference. "
    "count: number of records to fetch (default 10)."
))
def get_basis_of_allotment(count: int = 10) -> str:
    url = EP["basis_of_allotment"].format(n=count)
    data, err = _get(url, "BasisOfAllotment")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No basis of allotment data found."
 
    lines = [f"Basis of Allotment — {len(rows)} records:"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, [
            "co_code",
            "IssueType",
            "ClosDate",
            "VOLYR",
            "VOLSRNO",
            "ba",
            "lname",
        ])
        lines.append(
            f"\n  {i:>2}. [{p.get('co_code', 'N/A')}] {p.get('lname', 'N/A')}  "
            f"[{p.get('IssueType', 'N/A')}]"
            f"\n      Close Date: {str(p.get('ClosDate', ''))[:10]}  |  "
            f"Vol/Yr: {p.get('VOLYR', 'N/A')}  |  Vol SrNo: {p.get('VOLSRNO', 'N/A')}"
            f"\n      BA: {p.get('ba', 'N/A')}"
        )
    return "\n".join(lines)
 
 
@mcp.tool(description=(
    "Get IPO company logos list with company name, issue type, open/close/list dates, "
    "and logo image filename. Useful for building IPO dashboards or UI."
))
def get_ipo_logos() -> str:
    url = EP["ipo_logo"]
    data, err = _get(url, "IPOCompanyLogo")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No IPO logo data found."
 
    lines = [f"IPO Company Logos — {len(rows)} records:"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, [
            "co_code",
            "companyname",
            "issuetype",
            "opendate",
            "closedate",
            "listdate",
            "logoimagename",
        ])
        lines.append(
            f"  {i:>3}. [{p.get('co_code', 'N/A')}] {p.get('companyname', 'N/A')}  "
            f"[{p.get('issuetype', 'N/A')}]"
            f"  |  Open: {str(p.get('opendate', ''))[:10]}"
            f"  |  Close: {str(p.get('closedate', ''))[:10]}"
            f"  |  List: {str(p.get('listdate', ''))[:10]}"
            f"  |  Logo: {p.get('logoimagename', 'N/A')}"
        )
    return "\n".join(lines)
 



# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 7 — MF RESOLVERS
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.tool(description=(
    "Resolve an AMC / fund-house name to its internal mf_cocode. "
    "Call this BEFORE any AMC-level MF tool. Accepts brand names and aliases. "
    "The returned mf_cocode is used only to call other tools — do NOT show it to the user."
))
def resolve_mf_fund(query: str) -> str:
    cleaned   = _clean_mf_query(query)
    canonical = AMC_ALIAS_MAP.get(cleaned.lower().strip(), cleaned)
    hits = _fuzzy_mf_amc(canonical, limit=1, threshold=FUZZY_THRESHOLD)
    if not hits:
        hits = _fuzzy_mf_amc(canonical, limit=1, threshold=65)
    if hits:
        h = hits[0]
        return (
            f"RESOLVED\n"
            f"AMC Name  : {h.get('lname')}\n"
            f"mf_cocode : {h.get('mf_cocode')}"
        )
    return (
        f"NOT FOUND: '{canonical}'. "
        "Try the full AMC name (e.g. 'HDFC Mutual Fund')."
    )


@mcp.tool(description=(
    "Resolve a mutual fund scheme name into mf_schcode. "
    "CALL THIS FIRST before any tool that requires mf_schcode. "
    "Read the returned mf_schcode and pass it exactly to the next tool. "
    "Never skip this step or guess a code."
))
def resolve_mf_scheme(query: str, mf_cocode: Optional[int] = None) -> str:
    cleaned   = _clean_mf_query(query)
    canonical = SCHEME_ALIAS_MAP.get(cleaned.lower().strip(), cleaned)
    hits = _fuzzy_mf_scheme(canonical, mf_cocode=mf_cocode, limit=1, threshold=FUZZY_THRESHOLD)
    if not hits:
        hits = _fuzzy_mf_scheme(canonical, mf_cocode=None, limit=1, threshold=65)
    if hits:
        h = hits[0]
        return (
            f"RESOLVED\n"
            f"Scheme    : {h.get('sch_name')}\n"
            f"Category  : {h.get('category')}\n"
            f"mf_schcode: {h.get('mf_schcode')}"
        )
    return (
        f"NOT FOUND: '{canonical}'. "
        "Try the full scheme name (e.g. 'Mirae Asset Emerging Bluechip Fund - Direct Plan (Growth)')."
    )


@mcp.tool(description=(
    "Search mutual fund schemes by partial name, category, or keyword. "
    "Returns scheme names for the user to pick from."
))
def search_mf_schemes(query: str, limit: int = 5) -> str:
    hits = _fuzzy_mf_scheme(query, limit=min(max(limit, 1), 10), threshold=55)
    if not hits:
        return f"No schemes found matching '{query}'."
    lines = [f"Schemes matching '{query}':"]
    for i, s in enumerate(hits, 1):
        lines.append(f"  {i}. {s.get('sch_name')}  ({s.get('category')})")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 8 — MF AMC-LEVEL TOOLS
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.tool(description="List all AMC / fund houses with scheme counts and AUM summary.")
def get_all_fund_houses() -> str:
    data, err = _get(EP["fund_house"], "FundHouses")
    if err:
        return err
    rows = _as_list(data)[:50]
    lines = [f"Fund Houses ({len(rows)} AMCs):"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, ["lname", "fund_type", "osch", "csch", "sumoftotnav", "dateas"])
        lines.append(
            f"  {i:>3}. {p.get('lname','N/A')}"
            f"  [{p.get('fund_type','')}]"
            f"  Open: {p.get('osch','?')}  Closed: {p.get('csch','?')}"
            f"  AUM: {p.get('sumoftotnav','N/A')} Cr"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "List fund categories (All/Equity/Debt/Hybrid) for an AMC. "
    "REQUIRES mf_cocode — call resolve_mf_fund first."
))
def get_fund_categories(mf_cocode: int, category: str = "All") -> str:
    val, err = _require_int(mf_cocode, "mf_cocode", "resolve_mf_fund")
    if err:
        return err
    url = EP["fund_category_amc"].format(mf_cocode=val, category=category)
    data, err = _get(url, f"FundCategory[{val}/{category}]")
    if err:
        return err
    rows = _as_list(data)[:30]
    if not rows:
        return f"No categories found for this AMC under '{category}'."
    lines = [f"Categories ({category}):"]
    for row in rows:
        p = _pick(row, ["maincategory", "vclass"])
        lines.append(f"  • {p.get('maincategory','')} — {p.get('vclass','')}")
    return "\n".join(lines)


@mcp.tool(description=(
    "List all scheme names offered by an AMC with current NAV. "
    "REQUIRES mf_cocode — call resolve_mf_fund first."
))
def get_schemes_by_amc(mf_cocode: int) -> str:
    val, err = _require_int(mf_cocode, "mf_cocode", "resolve_mf_fund")
    if err:
        return err
    url = EP["scheme_master"].format(mf_cocode=val)
    data, err = _get(url, f"SchemeMaster[{val}]")
    if err:
        return err
    rows = _as_list(data)
    if not rows:
        return "No schemes found for this AMC."
    lines = [f"Schemes ({len(rows)} total):"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, ["sch_name", "category", "navrs"])
        lines.append(
            f"  {i:>3}. {p.get('sch_name','N/A')}"
            f"  [{p.get('category','')}]"
            f"  NAV: {p.get('navrs','N/A')}"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get fund profile / available scheme classes for an AMC. "
    "REQUIRES mf_cocode — call resolve_mf_fund first."
))
def get_fund_profile(mf_cocode: int) -> str:
    val, err = _require_int(mf_cocode, "mf_cocode", "resolve_mf_fund")
    if err:
        return err
    url = EP["fund_profile"].format(mf_cocode=val)
    data, err = _get(url, f"FundProfile[{val}]")
    if err:
        return err
    rows = _as_list(data)[:20]
    if not rows:
        return "No profile data found."
    lines = ["Fund Profile — available scheme classes:"]
    for row in rows:
        p = _pick(row, ["VCLASS"])
        if p.get("VCLASS"):
            lines.append(f"  • {p['VCLASS']}")
    return "\n".join(lines) if len(lines) > 1 else "No class data found."


@mcp.tool(description="List all fund managers across AMCs with scheme assignments and tenure.")
def get_fund_managers() -> str:
    data, err = _get(EP["fund_manager"], "FundManagers")
    if err:
        return err
    rows = _as_list(data)[:30]
    if not rows:
        return "No fund manager data available."
    sch_lookup = {s["mf_schcode"]: s["sch_name"] for s in _load_mf_scheme_cache()}
    lines = [f"Fund Managers ({len(rows)} records):"]
    for row in rows:
        mgr   = row.get("fund_mgr") or row.get("FundMgr") or "N/A"
        since = row.get("SinceDate") or row.get("sincedate") or ""
        scode = row.get("mf_schcode") or row.get("MF_SCHCODE")
        sname = sch_lookup.get(scode, "N/A") if scode else "N/A"
        lines.append(f"  • {mgr}  |  Scheme: {sname}  |  Since: {since}")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 9 — MF SCHEME-LEVEL TOOLS
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.tool(description=(
    "Retrieves the latest Net Asset Value (NAV) and daily price performance. "
    "Provides current price, change amount, and percentage growth. "
    "REQUIRES a valid mf_schcode (call resolve_mf_scheme first)."
))
def get_scheme_nav(mf_schcode: int) -> str:
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    data, err = _get(EP["daily_nav"], "DailyNAV")
    if err:
        return err
    records = data.get("data", []) if isinstance(data, dict) else []
    match = next(
        (r for r in records if int(float(r.get("mf_schcode", -1))) == val), None
    )
    if not match:
        return f"NAV data not found for mf_schcode={val} today."
    return (
        f"Today's NAV — {match.get('mf_schname','N/A')}\n"
        f"  Date       : {match.get('navdate','N/A')}\n"
        f"  NAV        : {match.get('nav','N/A')}\n"
        f"  Prev NAV   : {match.get('prevnav','N/A')}\n"
        f"  Change     : {match.get('navchng','N/A')}  ({match.get('navperchng','N/A')}%)"
    )

@mcp.tool(description=(
    "Retrieves the fund's investment profile: includes minimum entry amounts, "
    "strategic objectives, and tax classification. Use this to explain fund "
    "rules and suitability. REQUIRES mf_schcode (call resolve_mf_scheme first)."
))

def get_investment_details(mf_schcode: int) -> str:
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    url = EP["investment_details"].format(mf_schcode=val)
    data, err = _get(url, f"InvestmentDetails[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No investment detail data found."
    p = _pick(rows[0], ["inc_invest", "mininvt", "Objective", "taxbname", "TAXB"])
    return (
        f"Investment Details:\n"
        f"  Min Investment      : {p.get('mininvt','N/A')}\n"
        f"  Inception Investment: {p.get('inc_invest','N/A')}\n"
        f"  Tax Treatment       : {p.get('taxbname') or p.get('TAXB','N/A')}\n"
        f"  Objective           : {p.get('Objective','N/A')}"
    )

@mcp.tool(description=(
    "Provides a detailed breakdown of a fund's costs: the annual expense ratio "
    "and any exit load charges for early withdrawals. Essential for evaluating "
    "long-term investment efficiency. REQUIRES mf_schcode (call resolve_mf_scheme first)."
))
def get_expense_ratio(mf_schcode: int) -> str:
    # ... your implementation
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    data, err = _get(EP["expense_ratio"], f"ExpenseRatio[{val}]")
    if err:
        return err
    rows = _rows(data)
    match = next((r for r in rows if int(float(r.get("mf_schcode", -1))) == val), None)
    if not match:
        return "Expense ratio data not found for this scheme."
    p = _pick(match, ["EXPRATIO", "entry", "exit", "mininvt", "SIP_MinInv"])
    return (
        f"Expense & Load Details:\n"
        f"  Expense Ratio  : {p.get('EXPRATIO','N/A')}%\n"
        f"  Entry Load     : {p.get('entry','N/A')}\n"
        f"  Exit Load      : {p.get('exit','N/A')}\n"
        f"  Min Investment : {p.get('mininvt','N/A')}\n"
        f"  SIP Min        : {p.get('SIP_MinInv','N/A')}"
    )


@mcp.tool(description=(
    "Retrieves debt-specific metrics: Yield to Maturity (YTM), Modified Duration, "
    "and Average Maturity. Essential for analyzing a fund's interest rate "
    "sensitivity and projected yield. REQUIRES a valid mf_schcode "
    "(call resolve_mf_scheme first)."
))
def get_avg_maturity(mf_schcode: int) -> str:
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    data, err = _get(EP["avg_maturity"], f"AvgMaturity[{val}]")
    if err:
        return err
    rows = _rows(data)
    match = next((r for r in rows if int(float(r.get("mf_schcode", -1))) == val), None)
    if not match:
        return "Average maturity data not found for this scheme."
    p = _pick(match, ["avg_maturity", "ModDuration", "MacaulayDuration", "YTM", "AvgMaturityDate"])
    return (
        f"Debt Fund Metrics:\n"
        f"  Avg Maturity      : {p.get('avg_maturity','N/A')}\n"
        f"  Modified Duration : {p.get('ModDuration','N/A')}\n"
        f"  Macaulay Duration : {p.get('MacaulayDuration','N/A')}\n"
        f"  YTM               : {p.get('YTM','N/A')}%\n"
        f"  As of Date        : {p.get('AvgMaturityDate','N/A')}"
    )


@mcp.tool(description=(
    "Retrieves the historical Assets Under Management (AUM) for a scheme. "
    "Used to analyze the fund's growth, scale, and investor participation "
    "trends over time. REQUIRES a valid mf_schcode (call resolve_mf_scheme first)."
))
def get_scheme_aum(mf_schcode: int) -> str:
    
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    data, err = _get(EP["scheme_aum"], f"SchemeAUM[{val}]")
    if err:
        return err
    rows = _rows(data)
    matched = next((r for r in rows if int(float(r.get("mf_schcode", -1))) == val), None)
    if not matched:
        return "AUM data not found for this scheme."
    aum_rows = matched if isinstance(matched, list) else [matched]
    lines = ["Historical AUM:"]
    for row in aum_rows[:24]:
        p = _pick(row, ["AUMDate", "AUM"])
        lines.append(f"  {p.get('AUMDate','N/A')}: {p.get('AUM','N/A')} Cr")
    return "\n".join(lines)

@mcp.tool(description=(
    "Retrieves historical NAV data for performance benchmarking. "
    "Accepts 'M' (Months) or 'Y' (Years) as 'period' and an integer 'periodval'. "
    "Use this to evaluate historical returns and price volatility. "
    "REQUIRES mf_schcode (call resolve_mf_scheme first)."
))
def get_nav_historical(mf_schcode: int, period: str = "Y", periodval: int = 1) -> str:
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    p  = _normalise_period(period)
    pv = max(1, int(periodval) if periodval else 1)
    url = EP["nav_historical"].format(mf_schcode=val, period=p, periodval=pv)
    data, err = _get(url, f"NAVHistorical[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No historical NAV data found."
    lines = [f"Historical NAV (last {pv}{'Y' if p=='Y' else 'M'}):"]
    for row in rows:
        pr = _pick(row, ["NavDate", "NAVRS", "adjnavrs"])
        lines.append(f"  {pr.get('NavDate','N/A')}: {pr.get('NAVRS','N/A')} : {pr.get('adjnavrs','N/A')}")
    return "\n".join(lines)


@mcp.tool(description=(
    "Fetches trailing returns (1W, 1M, 3M, 6M, 1Y, 3Y, 5Y, and Inception) "
    "alongside benchmark data for comparison. Use this to evaluate if the "
    "fund is beating its index over various periods. "
    "REQUIRES a valid mf_schcode (call resolve_mf_scheme first)."
))
def get_scheme_returns(mf_schcode: int) -> str:
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    url = EP["scheme_returns"].format(mf_schcode=val)
    data, err = _get(url, f"SchemeReturns[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No returns data found."
    row = rows[0]
    p = _pick(row, ["sch_name", "Date", "1week", "1Month", "3Month", "6Month",
                    "1Year", "3Year", "5Year", "Inception",
                    "Category_1YRet", "Category_3YRet", "Category_5YRet"])
    return (
        f"Returns — {p.get('sch_name','N/A')}  (as of {p.get('Date','N/A')})\n"
        f"  1 Week   : {p.get('1week','N/A')}%\n"
        f"  1 Month  : {p.get('1Month','N/A')}%\n"
        f"  3 Months : {p.get('3Month','N/A')}%\n"
        f"  6 Months : {p.get('6Month','N/A')}%\n"
        f"  1 Year   : {p.get('1Year','N/A')}%  (Category avg: {p.get('Category_1YRet','N/A')}%)\n"
        f"  3 Years  : {p.get('3Year','N/A')}%  (Category avg: {p.get('Category_3YRet','N/A')}%)\n"
        f"  5 Years  : {p.get('5Year','N/A')}%  (Category avg: {p.get('Category_5YRet','N/A')}%)\n"
        f"  Inception: {p.get('Inception','N/A')}%"
    )


@mcp.tool(description=(
    "Calculates the absolute value and percentage returns for a lumpsum investment "
    "across standard intervals (1W, 1M, 3M, 6M, 1Y, 3Y, 5Y, 10Y, and Inception). "
    "Use this to demonstrate long-term capital appreciation. "
    "REQUIRES a valid mf_schcode (call resolve_mf_scheme first)."
))
def get_lumpsum_returns(mf_schcode: int) -> str:
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    url = EP["lumpsum_return"].format(mf_schcode=val)
    data, err = _get(url, f"LumpsumReturn[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No lumpsum return data found."
    r = rows[0]
    def _rv(key): return r.get(key, "N/A")
    return (
        f"Lumpsum Returns (Investment: {_rv('InvAmount')}):\n"
        f"  1 Week    : {_rv('W1LatestValue')}  ({_rv('W1Return_Abs')}% abs)\n"
        f"  1 Month   : {_rv('M1LatestValue')}  ({_rv('M1Return_Ann')}% ann)\n"
        f"  3 Months  : {_rv('M3LatestValue')}  ({_rv('M3Return_Ann')}% ann)\n"
        f"  6 Months  : {_rv('M6LatestValue')}  ({_rv('M6Return_Ann')}% ann)\n"
        f"  1 Year    : {_rv('Y1LatestValue')}  ({_rv('Y1Return_Ann')}% ann)\n"
        f"  3 Years   : {_rv('Y3LatestValue')}  ({_rv('Y3Return_Ann')}% ann)\n"
        f"  5 Years   : {_rv('Y5LatestValue')}  ({_rv('Y5Return_Ann')}% ann)\n"
        f"  10 Years  : {_rv('Y10LatestValue')}  ({_rv('Y10Return_Ann')}% ann)\n"
        f"  Inception : {_rv('InceptionLatestValue')}  ({_rv('InceptionReturn_Ann')}% ann)"
    )


@mcp.tool(description=(
    "Compares multiple schemes side-by-side across NAV, AUM, returns, fund management, "
    "and exit loads. Use this for 'Versus' queries or to evaluate a shortlist of funds. "
    "REQUIRES a comma-separated string of mf_schcodes (e.g., '123,456'). "
    "Ensure each code is first validated via resolve_mf_scheme."
))
def compare_schemes(mf_schcodes: str) -> str:
    if not mf_schcodes or not mf_schcodes.strip():
        return "Provide a comma-separated list of scheme codes (e.g. '19283,204,17152')."
    url = EP["scheme_comparison"].format(schcodes=mf_schcodes.strip())
    data, err = _get(url, f"SchemeComparison[{mf_schcodes}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No comparison data found."
    FIELDS = ["Sch_Name", "VClass", "NAVRS", "NavDate", "SchemeAssets",
              "FundManager", "1WEEK", "1MONTH", "3MONTH", "6MONTH",
              "1YEAR", "3YEAR", "5YEAR", "INCEPTION", "ExitLoad"]
    lines = [f"Scheme Comparison ({len(rows)} schemes):"]
    for row in rows:
        p = _pick(row, FIELDS)
        lines.append(f"\n  ── {p.get('Sch_Name','N/A')} [{p.get('VClass','')}] ──")
        lines.append(f"     NAV        : {p.get('NAVRS','N/A')}  (as of {p.get('NavDate','N/A')})")
        lines.append(f"     AUM        : {p.get('SchemeAssets','N/A')} Cr")
        lines.append(f"     Manager    : {p.get('FundManager','N/A')}")
        lines.append(
            f"     Returns    : 1W {p.get('1WEEK','N/A')}% | 1M {p.get('1MONTH','N/A')}% | "
            f"1Y {p.get('1YEAR','N/A')}% | 3Y {p.get('3YEAR','N/A')}% | "
            f"5Y {p.get('5YEAR','N/A')}% | Inception {p.get('INCEPTION','N/A')}%"
        )
        lines.append(f"     Exit Load  : {p.get('ExitLoad','N/A')}")
    return "\n".join(lines)

@mcp.tool(description=(
    "Identifies top-performing funds by ranking returns. "
    "Requires 'fund_type' (Equity, Debt, or Hybrid) and 'category' (e.g., 'Large Cap', 'Mid Cap', or 'all'). "
    "Set 'top' to the number of funds requested (default 10). "
    "Use this to answer discovery queries like 'Which are the best performing Hybrid funds?'"
))
def get_fund_performance(top: int = 10, fund_type: str = "Equity", category: str = "all") -> str:
    ft = fund_type.strip().title()
    if ft not in {"Equity", "Debt", "Hybrid"}:
        return f"fund_type must be Equity, Debt, or Hybrid — got '{fund_type}'."
    url = EP["fund_performance"].format(top=top, type=ft, category=category)
    data, err = _get(url, f"FundPerformance[{ft}/{category}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No performance data found."
    lines = [f"Top {len(rows)} {ft} Funds ({category}):"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, ["schemename", "Scheme", "TypeName", "NavRs", "1YEAR", "3Year", "5Year", "AUM_Cr"])
        name = p.get("schemename") or p.get("Scheme", "N/A")
        lines.append(
            f"  {i:>3}. {name}  [{p.get('TypeName','')}]"
            f"  NAV: {p.get('NavRs','N/A')}"
            f"  1Y: {p.get('1YEAR','N/A')}%"
            f"  3Y: {p.get('3Year','N/A')}%"
            f"  AUM: {p.get('AUM_Cr','N/A')} Cr"
        )
    return "\n".join(lines)

@mcp.tool(description=(
    "Fetches average trailing returns for broad fund categories (e.g., Mid Cap, Liquid). "
    "Use this to answer 'How is the Equity market doing?' or to provide a "
    "peer-group average for benchmarking specific schemes. "
    "Requires 'fund_type' (Equity, Debt, or Hybrid) and optional 'top' count."
))
def get_category_performance(fund_type: str = "Equity", top: int = 20) -> str:
    ft = fund_type.strip().title()
    url = EP["category_performance"].format(type=ft, top=top)
    data, err = _get(url, f"CategoryPerformance[{ft}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No category performance data found."
    lines = [f"Category Returns — {ft}:"]
    for row in rows:
        p = _pick(row, ["typename", "ret1y", "ret3y", "ret5y", "ret10y", "ret1m"])
        lines.append(
            f"  {p.get('typename','N/A'):<35}"
            f"  1Y: {p.get('ret1y','N/A')}%"
            f"  3Y: {p.get('ret3y','N/A')}%"
            f"  5Y: {p.get('ret5y','N/A')}%"
        )
    return "\n".join(lines)

@mcp.tool(description=(
    "Retrieves scheduling rules for SIP (Investment), SWP (Withdrawal), and STP (Transfer). "
    "Provides available monthly transaction dates and minimum amount requirements. "
    "Use this to guide users through the setup of automated financial plans. "
))
def get_sip_dates(plan: str = "SIP") -> str:
    p = plan.strip().upper()
    if p not in ("SIP", "SWP", "STP"):
        return f"plan must be 'SIP', 'SWP', or 'STP' — got '{plan}'."
    url = EP["sip_dates"].format(plan=p)
    data, err = _get(url, f"SIPDates[{p}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return f"No {p} date data found."
    lines = [f"{p} Available Dates:"]
    for row in rows[:10]:
        pr = _pick(row, ["frequency", "d1", "d2", "d3"])
        dates = ", ".join(str(pr[d]) for d in ["d1", "d2", "d3"] if pr.get(d))
        lines.append(f"  {pr.get('frequency','N/A')}: {dates}")
    return "\n".join(lines)


@mcp.tool(description=(
    "Retrieves scheduling rules and minimums for SIP (Systematic Investment) "
    "and SWP (Systematic Withdrawal) plans. Provides valid monthly dates "
    "and entry thresholds. Use this to guide the setup of automated "
    "transaction strategies. REQUIRES mf_schcode (call resolve_mf_scheme first)."
))
def get_scheme_sip_details(mf_schcode: int) -> str:
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    url = EP["scheme_sip_swp"].format(mf_schcode=val)
    data, err = _get(url, f"SchemeSIPSWP[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No SIP/SWP data found for this scheme."
    lines = ["SIP / SWP Details:"]
    for row in rows:
        p = _pick(row, ["SIPDates", "minamt", "multamt", "avail_period"])
        lines.append(
            f"  Dates: {p.get('SIPDates','N/A')}"
            f"  |  Min: {p.get('minamt','N/A')}"
            f"  |  Multiples: {p.get('multamt','N/A')}"
            f"  |  Period: {p.get('avail_period','N/A')}"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Fetches the fund's top portfolio holdings, including company names, "
    "sector allocation, and percentage of total assets. Use this to "
    "evaluate sector concentration and underlying stock exposure. "
    "REQUIRES a valid mf_schcode (call resolve_mf_scheme first)."
))
def get_mf_holdings(mf_schcode: int) -> str:
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    url = EP["mf_holding"].format(mf_schcode=val)
    data, err = _get(url, f"MFHolding[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No holdings data found."
    lines = [f"Top Holdings ({len(rows)} stocks):"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, ["co_name", "perc_hold", "mktvalue", "iind_name", "type", "rating"])
        lines.append(
            f"  {i:>3}. {p.get('co_name','N/A')}"
            f"  [{p.get('iind_name', p.get('type',''))}]"
            f"  {p.get('perc_hold','N/A')}%"
            f"  {p.get('mktvalue','N/A')} Cr"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Finds mutual funds that hold a specific company in their portfolio. "
    "Ranks results by the percentage weightage of the stock within each fund. "
    "Use this for 'Which funds hold X?' queries or for stock-specific exposure analysis. "
    "REQUIRES a co_code (call resolve_nse_symbol first)."
))
def get_funds_holding_company(co_code: int, top: int = 10) -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    url = EP["company_mf_holding"].format(co_code=val, top=top)
    data, err = _get(url, f"CompanyWiseMFHolding[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No mutual funds found holding this stock."
    lines = [f"Funds holding this stock ({len(rows)}):"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, ["sch_name", "invdate", "mktvalue", "perc_hold"])
        lines.append(
            f"  {i:>3}. {p.get('sch_name','N/A')}"
            f"  |  {p.get('perc_hold','N/A')}% of portfolio"
            f"  |  {p.get('mktvalue','N/A')} Cr"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Fetches the percentage allocation of the fund across various industry sectors. "
    "Use this for 'Sector breakdown' queries or to evaluate thematic concentration "
    "risk (e.g., exposure to Banking or IT). REQUIRES a valid mf_schcode "
    "(call resolve_mf_scheme first)."
))
def get_sector_allocation(mf_schcode: int) -> str:
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    url = EP["sector_allocation"].format(mf_schcode=val)
    data, err = _get(url, f"SectorAllocation[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No sector allocation data found."
    as_of = rows[0].get("secdate", "N/A")
    lines = [f"Sector Allocation (as of {as_of}):"]
    for row in rows:
        p = _pick(row, ["sector", "perc_hold"])
        lines.append(f"  {p.get('sector','N/A'):<35}: {p.get('perc_hold','N/A')}%")
    return "\n".join(lines)


@mcp.tool(description=(
    "Fetches the broad asset allocation of a fund (Equity vs. Debt vs. Cash %). "
    "Use this to evaluate the fund's risk-reward balance and asset-level "
    "diversification. REQUIRES a valid mf_schcode (call resolve_mf_scheme first)."
))

def get_asset_allocation(mf_schcode: int) -> str:
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    url = EP["asset_allocation"].format(mf_schcode=val)
    data, err = _get(url, f"AssetAllocation[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No asset allocation data found."
    as_of = rows[0].get("currentmonth", "N/A")
    lines = [f"Asset Allocation (as of {as_of}):"]
    for row in rows:
        p = _pick(row, ["assetname", "holding_currentmonth", "holding_prevmonth"])
        lines.append(
            f"  {p.get('assetname','N/A'):<20}"
            f": {p.get('holding_currentmonth','N/A')}%"
            f"  (prev: {p.get('holding_prevmonth','N/A')}%)"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Identifies recent portfolio changes: new stock additions ('in'), "
    "complete exits ('out'), or unchanged positions ('un'). "
    "Use this to evaluate the fund manager's latest buying and selling activity. "
    "REQUIRES a valid mf_schcode (call resolve_mf_scheme first) and 'move_type'."
))
def get_portfolio_changes(mf_schcode: int, move_type: str = "in") -> str:
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    t = move_type.lower()
    if t not in ("in", "out", "un"):
        return "move_type must be 'in', 'out', or 'un'."
    url = EP["whats_in_out"].format(type=t, mf_schcode=val)
    data, err = _get(url, f"WhatsInOut[{val}/{t}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        labels = {"in": "new buys", "out": "sold holdings", "un": "unchanged positions"}
        return f"No {labels[t]} found for this fund."
    label = {"in": "New Buys", "out": "Sold Positions", "un": "Unchanged"}[t]
    lines = [f"{label} ({len(rows)} stocks):"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, ["CO_NAME", "Perc_Hold", "mktvalue", "AssetType"])
        lines.append(
            f"  {i:>3}. {p.get('CO_NAME','N/A')}"
            f"  [{p.get('AssetType','')}]"
            f"  {p.get('Perc_Hold','N/A')}%"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Fetches the market-cap allocation (Large, Mid, and Small-cap %) of a scheme. "
    "Use this to evaluate the fund's risk profile and to ensure it aligns "
    "with its stated investment objective. REQUIRES a valid mf_schcode "
    "(call resolve_mf_scheme first)."
))
def get_mcap_allocation(mf_schcode: int) -> str:
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    url = EP["mcap_equity"].format(mf_schcode=val)
    data, err = _get(url, f"MCAPAllocation[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No market cap allocation data found."
    as_of = rows[0].get("CurrPFDate", "N/A")
    lines = [f"Market Cap Allocation (as of {as_of}):"]
    for row in rows:
        p = _pick(row, ["mcaptype", "perc_hold"])
        lines.append(f"  {p.get('mcaptype','N/A'):<15}: {p.get('perc_hold','N/A')}%")
    return "\n".join(lines)


@mcp.tool(description=(
    "Fetches the most significant stock purchases and sales made by the fund manager "
    "this month. Use this to evaluate active management decisions and "
    "high-conviction tactical moves. REQUIRES a valid mf_schcode "
    "(call resolve_mf_scheme first)."
))
def get_most_bought_sold(mf_schcode: int) -> str:
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    url = EP["most_sold_bought"].format(mf_schcode=val)
    data, err = _get(url, f"MostBoughtSold[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No buy/sell activity data found."
    lines = ["Most Bought / Sold This Month:"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, ["CO_NAME", "diff", "chng"])
        chng = float(p.get("chng", 0) or 0)
        direction = "▲ BUY" if chng >= 0 else "▼ SELL"
        lines.append(
            f"  {i:>3}. {direction}  {p.get('CO_NAME','N/A')}"
            f"  |  Change: {p.get('diff','N/A')} Cr ({chng:+.2f}%)"
        )
    return "\n".join(lines)

@mcp.tool(description=(
    "Lists all active New Fund Offers (NFOs) available for subscription. "
    "Includes the fund's objective, minimum investment requirements, and "
    "closing dates. Use this to explore new market themes and innovative "
    "investment strategies before they go live."
))
def get_new_fund_offers() -> str:
    data, err = _get(EP["new_fund_offer"], "NewFundOffer")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No open NFOs at this time."
    lines = [f"Open NFOs ({len(rows)}):"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, ["sch_name", "lname", "launc_date", "cldate", "mininvt", "schemetype"])
        lines.append(
            f"  {i:>3}. {p.get('sch_name','N/A')}"
            f"  by {p.get('lname','N/A')}"
            f"  [{p.get('schemetype','')}]"
            f"  Opens: {p.get('launc_date','N/A')}"
            f"  Closes: {p.get('cldate','N/A')}"
            f"  Min: {p.get('mininvt','N/A')}"
        )
    return "\n".join(lines)

@mcp.tool(description=(
    "Retrieves aggregate MF industry activity, including gross purchases, sales, "
    "and net flows for Equity and Debt markets. Use this to assess broad "
    "institutional sentiment and market liquidity trends. This provides "
    "macro-context rather than scheme-specific data."
))
def get_mf_market_activity() -> str:
    data, err = _get(EP["mf_activities"], "MFActivities")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No market activity data available."
    FIELDS = ["TRANS_DATE", "EQ_GR_PURC", "EQ_GR_SALE", "EQ_G_NE_PS", "DE_GR_PURC", "DE_GR_SALE"]
    lines = ["MF Market Activity (Cr):"]
    lines.append(f"  {'Date':<12}  {'Eq.Buy':>12}  {'Eq.Sell':>12}  {'Eq.Net':>12}  {'Debt Buy':>12}")
    for row in rows:
        p = _pick(row, FIELDS)
        lines.append(
            f"  {str(p.get('TRANS_DATE',''))[:10]:<12}"
            f"  {str(p.get('EQ_GR_PURC','N/A')):>12}"
            f"  {str(p.get('EQ_GR_SALE','N/A')):>12}"
            f"  {str(p.get('EQ_G_NE_PS','N/A')):>12}"
            f"  {str(p.get('DE_GR_PURC','N/A')):>12}"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Retrieves risk-adjusted metrics: Alpha (outperformance), Beta (volatility vs market), "
    "Sharpe Ratio (reward-to-risk), and Standard Deviation. Use this to evaluate "
    "fund quality beyond raw returns. REQUIRES a valid mf_schcode "
    "(call resolve_mf_scheme first)."
))
def get_scheme_ratios(mf_schcode: int) -> str:
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    data, err = _get(EP["scheme_ratios"], f"SchemeRatios[{val}]")
    if err:
        return err
    rows = _rows(data)
    match = next(
        (r for r in rows if str(r.get("MF_SCHCODE") or r.get("Mf_SchCode") or "") == str(val)),
        None,
    )
    if not match:
        return "Risk ratio data not found for this scheme."
    p = _pick(match, ["Scheme_Nam", "DATE", "BETA", "SD", "TREYNOR", "ALPHA", "SHARPE"])
    return (
        f"Risk Ratios — {p.get('Scheme_Nam','N/A')}  (as of {p.get('DATE','N/A')})\n"
        f"  Beta    : {p.get('BETA','N/A')}\n"
        f"  Std Dev : {p.get('SD','N/A')}\n"
        f"  Treynor : {p.get('TREYNOR','N/A')}\n"
        f"  Alpha   : {p.get('ALPHA','N/A')}\n"
        f"  Sharpe  : {p.get('SHARPE','N/A')}"
    )


@mcp.tool(description=(
    "Retrieves recent and historical dividend announcements, including payout "
    "amounts and record dates. Use this to evaluate the income-generation "
    "consistency of a scheme. REQUIRES a valid mf_schcode "
    "(call resolve_mf_scheme first)."
))
def get_dividend_details(mf_schcode: int) -> str:
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    url = EP["dividend_details"].format(mf_schcode=val)
    data, err = _get(url, f"DividendDetails[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No dividend announcements found for this scheme."
    lines = ["Dividend History:"]
    for row in rows:
        p = _pick(row, ["DivAmount", "Divtype", "DivDate", "RecordDate"])
        lines.append(
            f"  {p.get('DivDate','N/A')}"
            f"  {p.get('DivAmount','N/A')} per unit"
            f"  [{p.get('Divtype','')}]"
            f"  Record: {p.get('RecordDate','N/A')}"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Retrieves the latest mutual fund industry news and announcements. "
    "Use '-' for a summary of recent headlines or a specific serial number "
    "to fetch full article details. Essential for keeping users informed "
    "on market trends and regulatory shifts."
))
def get_mf_news(sno: str = "-") -> str:
    url = EP["mf_news"].format(sno=sno.strip() or "-")
    data, err = _get(url, f"MFNews[{sno}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No MF news available."
    lines = ["MF News:"]
    for row in rows:
        p = _pick(row, ["date", "heading", "caption"])
        lines.append(f"\n  [{p.get('date','N/A')}]  {p.get('heading','N/A')}")
        if p.get("caption"):
            lines.append(f"  {p['caption']}")
    return "\n".join(lines)


@mcp.tool(description=(
    "Retrieves BSE StAR MF platform transaction rules: includes settlement "
    "cycles, Demat/Physical mode compatibility, and SIP/SWP eligibility flags. "
    "Use this to confirm trade execution logistics for a specific scheme. "
    "REQUIRES a valid mf_schcode (call resolve_mf_scheme first)."
))
def get_bse_star_scheme(mf_schcode: int) -> str:
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    url = EP["bse_star_scheme"].format(mf_schcode=val)
    data, err = _get(url, f"BSEStarScheme[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No BSE Star data found for this scheme."
    p = _pick(rows[0], ["SCH_NAME", "Scheme_Type", "SIP_FLAG", "STP_FLAG", "SWP_FLAG",
                        "Settlement_Type", "ISIN", "Purchase_Allowed", "Redemption_Allowed"])
    return (
        f"BSE Star Details — {p.get('SCH_NAME','N/A')}\n"
        f"  Scheme Type         : {p.get('Scheme_Type','N/A')}\n"
        f"  SIP Available       : {'Yes' if p.get('SIP_FLAG') == 1 else 'No'}\n"
        f"  STP Available       : {'Yes' if p.get('STP_FLAG') == 1 else 'No'}\n"
        f"  SWP Available       : {'Yes' if p.get('SWP_FLAG') == 1 else 'No'}\n"
        f"  Purchase Allowed    : {'Yes' if p.get('Purchase_Allowed') == 1 else 'No'}\n"
        f"  Redemption Allowed  : {'Yes' if p.get('Redemption_Allowed') == 1 else 'No'}\n"
        f"  Settlement Type     : {p.get('Settlement_Type','N/A')}\n"
        f"  ISIN                : {p.get('ISIN','N/A')}"
    )

@mcp.tool(description=(
    "Retrieves institutional identifiers (AMFI Code and ISIN) for mutual fund schemes. "
    "Use this for cross-referencing with external broker data or verifying a scheme's "
    "unique industry identity. Optionally accepts mf_schcode to filter results."
))
def get_amfi_master(mf_schcode: Optional[int] = None) -> str:
    data, err = _get(EP["amfi_master"], "AMFIMaster")
    if err:
        return err
    rows = _rows(data)
    if mf_schcode:
        rows = [r for r in rows if str(r.get("mf_schcode") or "") == str(mf_schcode)]
    rows = rows[:20]
    if not rows:
        return "No AMFI data found."
    lines = ["AMFI Mapping:"]
    for row in rows:
        p = _pick(row, ["amficode", "growth_payoutisin", "reinvestmentisin"])
        lines.append(
            f"  AMFI Code : {p.get('amficode','N/A')}"
            f"  |  Growth ISIN: {p.get('growth_payoutisin','N/A')}"
            f"  |  Reinvest ISIN: {p.get('reinvestmentisin','N/A')}"
        )
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 10 — ETF LEVEL TOOLS
# ═══════════════════════════════════════════════════════════════════════════════
@mcp.tool(description=(
    "Get current ETF price and trading data: open, high, low, current price, previous price, "
    "price diff, volume, and 52-week high/low. "
    "REQUIRES isin — call resolve_etf_isin first. exchange: 'NSE' (default) or 'BSE'."
))
def get_etf_quotes(isin: str, exchange: str = "NSE") -> str:
    """
    Args:
        isin: ETF ISIN code. Call resolve_etf_isin first.
        exchange: 'NSE' or 'BSE'.
    """
    if not isin or not isin.strip():
        return "'isin' is required. Call resolve_etf_isin first to get it."

    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)

    # Updated URL construction to match your successful test: {exchange}/{isin}
    url = EP["get_etf_quotes"].format(isin=isin, ex=ex)
    
    data, err = _get(url, f"ETFGetQuotes[{isin}/{ex}]")
    if err:
        return err

    # Based on your test output, 'data' is the key containing the list
    rows = data.get("data") if isinstance(data, dict) else None
    if not rows or len(rows) == 0:
        return f"No ETF quote data found for ISIN '{isin}' on {ex}."

    r = rows[0]
    
    # Mapping keys exactly as they appear in your JSON output
    metrics = {
        "ISIN":          isin,
        "Exchange":      r.get("Exchange", ex),
        "Trade Date":    r.get("TradeDate", "N/A"),
        "Open":          r.get("DayOpen", 0.0),
        "High":          r.get("DayHigh", 0.0),
        "Low":           r.get("DayLow", 0.0),
        "Current Price": r.get("CurrentPrice", 0.0),
        "Prev Price":    r.get("PreviousPrice", 0.0),
        "Price Diff":    r.get("Pricediff", 0.0),
        "Pct Change":    r.get("PricePerChange", 0.0),
        "Volume":        r.get("Volume", 0),
        "52W High":      r.get("HI_52_WK", 0.0),
        "52W Low":       r.get("LO_52_WK", 0.0),
        "52W High Date": r.get("H52DATE", "N/A"),
        "52W Low Date":  r.get("L52DATE", "N/A"),
    }

    # Formatting the output for the user
    header = f"### ETF Quote: {metrics['ISIN']} [{metrics['Exchange']}]"
    lines = [header, "---"]

    for label, value in metrics.items():
        if label in {"ISIN", "Exchange"}:
            continue
        
        # Format Volume with commas
        if label == "Volume":
            formatted = f"{int(value):,}"
        # Format currency fields (Current Price, High, Low, etc.)
        elif isinstance(value, (float, int)) and label != "Pct Change":
            formatted = f"₹{value:,.2f}"
        # Format percentage
        elif label == "Pct Change":
            formatted = f"{value:.2f}%"
        # Clean up ISO timestamps for readability
        elif "Date" in label and value != "N/A":
            formatted = value.split('T')[0]
        else:
            formatted = str(value)
            
        lines.append(f"* **{label}:** {formatted}")

    return "\n".join(lines)

@mcp.tool(description=(
    "Get ETF historical returns (1y, 3y, 5y, inception) and category benchmarks. "
    "REQUIRES isin — call resolve_etf_isin first."
))
def get_etf_returns(isin: str) -> str:
    """
    Args:
        isin: ETF ISIN code. Call resolve_etf_isin first.
    """
    if not isin or not isin.strip():
        return "'isin' is required. Call resolve_etf_isin first to get it."

    # Endpoint as per your successful test: https://equifizapis.cmots.com/api/ETFReturns/{isin}
    url = EP["get_etf_returns"].format(isin=isin)
    
    data, err = _get(url, f"ETFReturns[{isin}]")
    if err:
        return err

    # Extracting from the 'data' list in the response
    rows = data.get("data") if isinstance(data, dict) else None
    if not rows:
        return f"No return data found for ISIN '{isin}'."

    r = rows[0]
    
    def fmt_pct(val):
        try:
            return f"{float(val):.2f}%" if val is not None else "N/A"
        except (ValueError, TypeError):
            return "N/A"

    header = f"### ETF Performance: {isin}"
    
    # Building the response lines with correct PascalCase keys
    lines = [
        header,
        "---",
        f"* **Category:** {r.get('ETFCategory', 'N/A')}",
        f"* **Returns Since Inception:** {fmt_pct(r.get('RetInc'))}",
        "#### 1-Year Performance",
        f"* **Return:** {fmt_pct(r.get('Ret1Y'))} (Category Avg: {fmt_pct(r.get('CategoryAvg1Y'))})",
        f"* **Rank:** {r.get('rank1Y', 'N/A')} of {r.get('count1Y', 'N/A')}",
        "#### 3-Year Performance",
        f"* **Return:** {fmt_pct(r.get('Ret3Y'))} (Category Avg: {fmt_pct(r.get('CategoryAvg3Y'))})",
        f"* **Rank:** {r.get('rank3Y', 'N/A')} of {r.get('count3Y', 'N/A')}",
        "#### 5-Year Performance",
        f"* **Return:** {fmt_pct(r.get('Ret5Y'))} (Category Avg: {fmt_pct(r.get('CategoryAvg5Y'))})",
        f"* **Rank:** {r.get('rank5Y', 'N/A')} of {r.get('count5Y', 'N/A')}"
    ]

    return "\n".join(lines)

@mcp.tool(description=(
    "Get ETF fundamental data: expense ratio, AUM, P/E, P/B ratios, and risk profile. "
    "REQUIRES isin — call resolve_etf_isin first."
))
def get_etf_fundamentals(isin: str) -> str:
    """
    Args:
        isin: ETF ISIN code. Call resolve_etf_isin first.
    """
    if not isin or not isin.strip():
        return "'isin' is required. Call resolve_etf_isin first to get it."

    # Endpoint based on your successful test: https://equifizapis.cmots.com/api/ETFFundamentals/{isin}
    url = EP["get_etf_fundamentals"].format(isin=isin)
    
    data, err = _get(url, f"ETFFundamentals[{isin}]")
    if err:
        return err

    # Extracting from the 'data' list in the response
    rows = data.get("data") if isinstance(data, dict) else None
    if not rows:
        return f"No fundamental data found for ISIN '{isin}'."

    r = rows[0]
    
    # Helper to clean up ISO dates (2026-04-30T00:00:00 -> 2026-04-30)
    def fmt_date(d_str):
        return d_str.split('T')[0] if d_str and 'T' in d_str else d_str

    header = f"### ETF Fundamentals: {isin}"
    lines = [header, "---"]

    # Description (using PascalCase key from your logs)
    desc = r.get("Description")
    if desc:
        lines.append(f"{desc}\n")
    
    # Basic Info
    lines.append(f"* **Category:** {r.get('ETFCategory', 'N/A')}")
    lines.append(f"* **Inception Date:** {fmt_date(r.get('InceptionDate', 'N/A'))}")

    # Cost & Risk
    er = r.get("ExpenseRatio", 0.0)
    er_date = fmt_date(r.get("ExpenseRatioDate", "N/A"))
    lines.append(f"* **Expense Ratio:** {er}% (as of {er_date})")
    lines.append(f"* **Riskometer:** {r.get('Riskometer', 'N/A')}")

    # Size & Portfolio
    aum = r.get("AUM", 0.0)
    aum_date = fmt_date(r.get("AUMDate", "N/A"))
    lines.append(f"* **AUM (Assets Under Management):** ₹{aum:,.2f} Cr (as of {aum_date})")
    lines.append(f"* **Stock Count:** {r.get('StockCount', 0)} holdings")

    # Valuation Metrics (Showing N/A if 0.0, common for commodity/debt ETFs)
    pe = r.get("PortfolioPE")
    pb = r.get("PortfolioPB")
    lines.append(f"* **Portfolio P/E:** {pe if pe != 0.0 else 'N/A'}")
    lines.append(f"* **Portfolio P/B:** {pb if pb != 0.0 else 'N/A'}")

    return "\n".join(lines)

@mcp.tool(description=(
    "Get general information about an ETF: description, launch date, fund managers, and ETF code. "
    "REQUIRES isin — call resolve_etf_isin first."
))
def get_etf_about(isin: str) -> str:
    """
    Args:
        isin: ETF ISIN code. Call resolve_etf_isin first.
    """
    if not isin or not isin.strip():
        return "'isin' is required. Call resolve_etf_isin first to get it."

    # Updated URL construction to match your test: https://equifizapis.cmots.com/api/ETFAboutus/{isin}
    url = EP["get_etf_about"].format(isin=isin)
    
    data, err = _get(url, f"ETFAboutus[{isin}]")
    if err:
        return err

    # Extracting from the 'data' list in the response
    rows = data.get("data") if isinstance(data, dict) else None
    if not rows:
        return f"No background information found for ISIN '{isin}'."

    r = rows[0]
    
    # Use ETFCode for header if available, otherwise fallback to ISIN
    etf_code = r.get("ETFCode", isin)
    header = f"### About ETF: {etf_code}"
    lines = [header, "---"]

    # Fund Description (PascalCase)
    desc = r.get("Description")
    if desc:
        lines.append(f"**Overview:**\n{desc}\n")

    # Key Facts
    # Formatting the date to remove the timestamp
    raw_date = r.get("FoundedDate", "N/A")
    founded_date = raw_date.split('T')[0] if 'T' in raw_date else raw_date

    # Cleaning up Fund Manager names (removing potential leading/trailing whitespace)
    managers = r.get("FundManagers", "N/A").strip()

    lines.append(f"* **ISIN:** {r.get('ISIN', isin)}")
    lines.append(f"* **ETF Code:** {etf_code}")
    lines.append(f"* **Founded Date:** {founded_date}")
    lines.append(f"* **Fund Manager(s):** {managers}")

    return "\n".join(lines)



@mcp.tool(description=(
    "Get detailed equity holdings for an ETF, including stock names, sectors, "
    "and percentage weightage. REQUIRES isin — call resolve_etf_isin first."
))
def get_etf_equity_holdings(isin: str) -> str:
    """
    Args:
        isin: ETF ISIN code. Call resolve_etf_isin first.
    """
    if not isin or not isin.strip():
        return "'isin' is required. Call resolve_etf_isin first to get it."

    # Updated URL construction based on your test: https://equifizapis.cmots.com/api/ETFShareHoldingEquity/{isin}
    url = EP["etf_equity_holdings"].format(isin=isin)
    
    data, err = _get(url, f"ETFShareHoldingEquity[{isin}]")
    if err:
        return err

    # Extracting from the 'data' list in the response
    rows = data.get("data") if isinstance(data, dict) else None
    if not rows:
        return f"No equity holding data found for ISIN '{isin}'."

    # Clean up the portfolio date from the first entry
    raw_date = rows[0].get("PortfolioDate", "N/A")
    portfolio_date = raw_date.split('T')[0] if 'T' in raw_date else raw_date
    
    header = f"### ETF Equity Holdings: {isin}"
    lines = [header, f"**Portfolio Date:** {portfolio_date}", "---"]

    # Table Header for better scannability
    lines.append("| Scrip Name | Sector | Weight (%) | Price | Change |")
    lines.append("|:---|:---|:---|:---|:---|")

    for r in rows:
        scrip_name = r.get("ScripName", "Unknown")
        sector = r.get("Sector", "N/A")
        weight = r.get("HoldingPercentage", 0.0)
        price = r.get("ScripPrice", 0.0)
        change = r.get("ScripPerChange", 0.0)

        # Formatting each row into the table
        lines.append(
            f"| **{scrip_name}** | {sector} | {weight:.2f}% | ₹{price:,.2f} | {change:+.2f}% |"
        )

    return "\n".join(lines)


@mcp.tool(description=(
    "Get the complete monthly portfolio holdings for an ETF. "
    "Includes security names, market values, and percentage weights. "
    "REQUIRES isin — call resolve_etf_isin first."
))
def get_etf_monthly_portfolio(isin: str) -> str:
    """
    Args:
        isin: ETF ISIN code. Call resolve_etf_isin first.
    """
    if not isin or not isin.strip():
        return "'isin' is required. Call resolve_etf_isin first to get it."

    # Updated URL based on test: https://equifizapis.cmots.com/api/ETFMonthlyPortfolioAllHoldings/{isin}
    url = EP["get_etf_monthly_portfolio"].format(isin=isin)
    
    data, err = _get(url, f"ETFMonthlyPortfolio[{isin}]")
    if err:
        return err

    # Extracting from the 'data' list in the response
    rows = data.get("data") if isinstance(data, dict) else None
    if not rows:
        return f"No monthly portfolio data found for ISIN '{isin}'."

    # Clean up the reporting date
    raw_date = rows[0].get("PortfolioDate", "N/A")
    report_date = raw_date.split('T')[0] if 'T' in raw_date else raw_date
    
    header = f"### Monthly Portfolio Disclosure: {isin}"
    lines = [header, f"**Reporting Date:** {report_date}", "---"]

    # Table Header
    lines.append("| Security | Asset Type | Sector | Weight (%) | Market Value |")
    lines.append("|:---|:---|:---|:---|:---|")

    for r in rows:
        security = r.get("HoldingSecurityName", "Unknown")
        asset_type = r.get("AssetName", "N/A")
        # Note the full key name from your logs: SectorName_EquityInvestment
        sector = r.get("SectorName_EquityInvestment") or "-"
        weight = r.get("HoldingPercentage", 0.0)
        mkt_val = r.get("MarketValue", 0.0)
        
        # Add to table
        lines.append(
            f"| {security} | {asset_type} | {sector} | {weight:.2f}% | ₹{mkt_val:,.2f} Cr |"
        )

    return "\n".join(lines)


@mcp.tool(description=(
    "Get the sector-wise allocation of an ETF. "
    "Provides percentage weights for various industries. "
    "REQUIRES isin — call resolve_etf_isin first."
))
def get_etf_sector_allocation(isin: str) -> str:
    """
    Args:
        isin: ETF ISIN code. Call resolve_etf_isin first.
    """
    if not isin or not isin.strip():
        return "'isin' is required. Call resolve_etf_isin first to get it."

    # Updated URL construction based on test: https://equifizapis.cmots.com/api/ETFSectorAllocation/{isin}
    url = EP["get_etf_sector_allocation"].format(isin=isin)
    
    data, err = _get(url, f"ETFSectorAllocation[{isin}]")
    if err:
        return err

    # Extracting from the 'data' list in the response
    rows = data.get("data") if isinstance(data, dict) else None
    if not rows:
        return f"No sector allocation data found for ISIN '{isin}'."

    # First row defines the date of the portfolio snapshot
    raw_date = rows[0].get("PortfolioDate", "N/A")
    portfolio_date = raw_date.split('T')[0] if 'T' in raw_date else raw_date
    
    header = f"### Sector Allocation: {isin}"
    lines = [header, f"**Snapshot Date:** {portfolio_date}", "---"]

    # Sorting by PercentageHolding (PascalCase) descending
    sorted_rows = sorted(
        rows, 
        key=lambda x: x.get("PercentageHolding", 0.0), 
        reverse=True
    )

    for r in sorted_rows:
        sector = r.get("SectorName", "Other/Unknown")
        weight = r.get("PercentageHolding", 0.0)
        shares = r.get("TotalShares", 0)

        # Format based on the weight
        # Using :.2f for weight as sector allocations can often have decimals
        lines.append(f"* **{sector}:** {weight:.2f}% (Total Shares: {int(shares):,})")

    return "\n".join(lines)
    

@mcp.tool(description=(
    "Get the high-level asset allocation of an ETF (e.g., Equity vs. Cash). "
    "REQUIRES isin — call resolve_etf_isin first."
))
def get_etf_asset_allocation(isin: str) -> str:
    """
    Args:
        isin: ETF ISIN code. Call resolve_etf_isin first.
    """
    if not isin or not isin.strip():
        return "'isin' is required. Call resolve_etf_isin first to get it."

    # Updated URL construction to match successful test pattern
    url = EP["get_etf_asset_allocation"].format(isin=isin)
    
    data, err = _get(url, f"ETFAssetAllocation[{isin}]")
    if err:
        return err

    # Extracting from the 'data' list in the response
    rows = data.get("data") if isinstance(data, dict) else None
    if not rows:
        return f"No asset allocation data found for ISIN '{isin}'."

    # Clean up the portfolio date from the first entry
    raw_date = rows[0].get("PortfolioDate", "N/A")
    portfolio_date = raw_date.split('T')[0] if 'T' in raw_date else raw_date
    
    header = f"### Asset Allocation: {isin}"
    lines = [header, f"**Portfolio Date:** {portfolio_date}", "---"]

    # Sort by weight to show the primary asset class first
    sorted_rows = sorted(
        rows, 
        key=lambda x: x.get("PercentageHolding", 0.0), 
        reverse=True
    )

    for r in sorted_rows:
        # Mapping to the exact PascalCase keys from your API response
        asset_name = r.get("AssetName", "Other")
        weight = r.get("PercentageHolding", 0.0)
        
        # Displaying weight with 2 decimal places for accuracy
        lines.append(f"* **{asset_name}:** {weight:.2f}%")

    return "\n".join(lines)

# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    mcp.run()
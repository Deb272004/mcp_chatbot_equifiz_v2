"""
graph.py – EQUIFIZ Financial AI Agent (v5)

Key changes from v4
───────────────────
1. ASYNCIO FIX — nest_asyncio.apply() is called at module load so that
   uvicorn's running event loop does not conflict with asyncio.run() calls
   inside thread pools.  node_mcp_tool_call now uses
   loop.run_until_complete() on a freshly-created event loop that is
   executed in a single background thread, completely avoiding the
   "unhandled errors in a TaskGroup" crash.

2. EARLY-EXIT on "not found" — if the very first (and only) data-tool
   result contains a "not found" / "unavailable" signal, the MCP synthesis
   path returns a clean user-facing message instead of re-entering the LLM
   loop and hanging.

3. TIMEOUT hardening — the background thread used for the MCP call has a
   120-second hard timeout.  A TimeoutError is caught and returned as a
   clean error string rather than propagating an unhandled exception.

4. All v4 design decisions (mcp_pre_resolve, mcp_multi_pre_resolve,
   code injection, resolver guard in mcp_client.py) are unchanged.

5. HIGH-PRECISION PARAMETER RESOLUTION (v5.1) — node_mcp_pre_resolve now
   uses PARAM_TO_TABLE_MAP + _targeted_db_lookup for deterministic,
   table-aware fuzzy resolution of mf_schcode, mf_cocode, and co_code.

Pipeline design (unchanged from v4)
────────────────────────────────────
Node A  : classify + extract
           Detects query_type, entity, intent, relevant_apis (2-3).
           Also sets is_broad=True if the query is a "full analysis" request.
           Detects mcp_needed=True for real-time stock/MF queries.

           ├─ "greeting"    → greeting_handler → END
           ├─ "general"     → general_handler → synthesis → END
           │
           ├─ "company"
           │    ├─ mcp_needed=True  → symbol_resolution → mcp_pre_resolve
           │    │                     → mcp_tool_call → mcp_synthesis → END
           │    ├─ is_broad=False   → symbol_resolution → direct_api_fetch
           │    │                     → summarise_apis → synthesis → END
           │    └─ is_broad=True    → symbol_resolution → full_ingest (ChromaDB)
           │                          → vector_retrieval → summarise_apis
           │                          → synthesis → END
           │
           ├─ "comparison"
           │    ├─ mcp_needed=True  → mcp_multi_pre_resolve → mcp_tool_call
           │    │                     → mcp_synthesis → END
           │    ├─ is_broad=False   → multi_direct_fetch → per_co_summarise
           │    │                     → comparison_synthesis → END
           │    └─ is_broad=True    → multi_full_ingest → per_co_summarise
           │                          → comparison_synthesis → END
           │
           └─ "investment"  → multi_direct_fetch → per_co_summarise
                              → investment_advisor → END
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import hashlib
import json
import logging
import re
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Any, Optional, TypedDict

import psycopg2
import psycopg2.extras
from rapidfuzz import fuzz

from langchain_core.messages import HumanMessage
from langchain_ollama import ChatOllama
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langgraph.graph import END, StateGraph

import db
import equifiz_client as api
import vector_store as vs
from config.config import OLLAMA_BASE_URL, OLLAMA_MODEL
from rich.console import Console

console = Console()

logger = logging.getLogger(__name__)

TODAY = str(date.today())


# ── LLM ───────────────────────────────────────────────────────────────────────

llm = ChatOllama(
    base_url=OLLAMA_BASE_URL,
    model=OLLAMA_MODEL,
)

splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=50)


# ── Conversation turn helper ──────────────────────────────────────────────────

class Turn(TypedDict):
    role: str       # "user" | "assistant"
    content: str


# ── Intent → API mapping ──────────────────────────────────────────────────────

INTENT_API_MAP: dict[str, list[tuple[str, Optional[str]]]] = {
    "pe_ratio":          [("daily_ratios", None), ("key_ratios", "s")],
    "eps":               [("daily_ratios", None), ("key_ratios", "s"), ("quarterly_results", "s")],
    "mcap":              [("daily_ratios", None)],
    "quarterly_results": [("quarterly_results", "s"), ("quarterly_results", "c")],
    "profit_and_loss":   [("profit_and_loss", "s"), ("profit_and_loss", "c")],
    "balance_sheet":     [("balance_sheet", "s"), ("balance_sheet", "c")],
    "key_ratios":        [("key_ratios", "s"), ("key_ratios", "c"), ("daily_ratios", None)],
    "daily_ratios":      [("daily_ratios", None), ("key_ratios", "s")],
    "general":           [("daily_ratios", None), ("key_ratios", "s")],
}

_DEFAULT_APIS: list[tuple[str, Optional[str]]] = [
    ("daily_ratios", None),
    ("key_ratios", "s"),
]

_ALL_FINANCIAL_APIS: list[tuple[str, Optional[str]]] = [
    ("daily_ratios",      None),
    ("quarterly_results", "s"),
    ("quarterly_results", "c"),
    ("profit_and_loss",   "s"),
    ("profit_and_loss",   "c"),
    ("balance_sheet",     "s"),
    ("balance_sheet",     "c"),
    ("key_ratios",        "s"),
    ("key_ratios",        "c"),
]

_BROAD_KEYWORDS = {
    "full analysis", "complete analysis", "full report", "complete report",
    "deep dive", "detailed analysis", "everything about", "all financials",
    "comprehensive", "full picture",
}

_MCP_KEYWORDS = {
    # Real-time stock data
    "live price", "current price", "stock price", "intraday", "today price",
    "ltp", "last traded price", "real time", "real-time",
    # MF identity / listing queries
    "mutual fund", "mutualfund", "mf", "scheme", "schemes",
    "amc", "fund house", "nfo", "new fund offer",
    # MF transaction / structure
    "sip", "swp", "stp", "nav", "net asset value",
    "fund manager", "expense ratio", "exit load", "entry load",
    # MF analytics
    "holdings", "sector allocation", "asset allocation",
    "portfolio changes", "whats in", "whats out",
    "mcap allocation", "most bought", "most sold",
    "scheme returns", "fund returns", "lumpsum return",
    "dividend details", "scheme ratios", "bse star",
    "amfi", "fund performance", "category performance",
    "scheme comparison", "compare funds", "compare schemes",
    # Common phrasings
    "what schemes", "list schemes", "show schemes",
    "what funds", "list funds", "show funds",
    "provides", "offers", "available schemes", "available funds",
}

_MF_SCHCODE_TOOLS = {
    "get_scheme_nav", "get_investment_details", "get_expense_ratio",
    "get_avg_maturity", "get_scheme_aum", "get_nav_historical",
    "get_scheme_returns", "get_lumpsum_returns", "get_scheme_sip_details",
    "get_mf_holdings", "get_sector_allocation", "get_asset_allocation",
    "get_portfolio_changes", "get_mcap_allocation", "get_most_bought_sold",
    "get_scheme_ratios", "get_dividend_details", "get_bse_star_scheme",
}

_MF_COCODE_TOOLS = {
    "get_fund_categories", "get_schemes_by_amc", "get_fund_profile",
}

_CO_CODE_TOOLS = {
    "get_funds_holding_company",
}

# Phrases that indicate the MCP data tool returned nothing useful
_MCP_NOT_FOUND_SIGNALS = (
    "not found",
    "no data",
    "unavailable",
    "not available",
    "no nav data",
    "no returns data",
    "no holdings data",
    "no sector",
    "no asset",
    "data not found",
)


# ── DB config (shared by MCP pre-resolution helpers) ─────────────────────────

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "equifiz",
    "user":     "postgres",
    "password": "1234",
}


# ── Parameter → DB table mapping ─────────────────────────────────────────────
#
# Each entry maps an MCP tool parameter name to the DB table and columns
# used to resolve it.  _targeted_db_lookup() uses this config to perform
# a generic, table-aware fuzzy search without duplicating SQL per table.

PARAM_TO_TABLE_MAP: dict[str, dict[str, str]] = {
    "mf_schcode": {
        "table":    "scheme_master",
        "column":   "sch_name",
        "id_field": "mf_schcode",
    },
    "mf_cocode": {
        "table":    "fund_house",
        "column":   "lname",
        "id_field": "mf_cocode",
    },
    "co_code": {
        "table":    "company_master",
        "column":   "companyname",
        "id_field": "co_code",
    },
}


# ── Agent State ───────────────────────────────────────────────────────────────

class AgentState(TypedDict, total=False):
    # Input
    user_query: str
    conversation_history: list[Turn]
    force_refresh: bool

    # Classification
    query_type: str
    extracted_entity: str
    report_type: str
    intent: str
    is_broad: bool
    mcp_needed: bool
    mcp_tool_hint: str
    relevant_apis: list[tuple[str, Optional[str]]]

    # Symbol resolution (single-company path)
    nse_symbol: Optional[str]
    co_code: Optional[int]
    company_info: Optional[dict]

    # MCP-specific resolved codes
    mcp_resolved_codes: dict
    mcp_scheme_name: Optional[str]
    mcp_amc_name: Optional[str]

    # Multi-company (comparison / investment)
    companies: list[dict]

    # Direct-fetch & summarise path
    raw_api_data: dict[str, Any]
    api_summaries: dict[str, str]

    # ChromaDB path (broad queries only)
    ingested_apis: list[str]
    failed_apis: list[str]
    vector_context: list[dict]

    # MCP path output
    mcp_raw_result: str
    mcp_tool_calls_made: list[str]

    # Output
    final_answer: str
    error: Optional[str]


# ── LLM helpers ──────────────────────────────────────────────────────────────

def _llm_json(prompt: str) -> dict:
    resp = llm.invoke([HumanMessage(content=prompt)])
    text = resp.content.strip()
    text = re.sub(r"^```(?:json)?|```$", "", text, flags=re.MULTILINE).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            return json.loads(match.group())
        raise ValueError(f"Could not parse JSON from LLM: {text[:300]}")


def _llm_text(prompt: str) -> str:
    resp = llm.invoke([HumanMessage(content=prompt)])
    return resp.content.strip()


def _format_history(state: AgentState, n: int = 8) -> str:
    history = state.get("conversation_history") or []
    return "\n".join(
        f"{t['role'].upper()}: {t['content']}" for t in history[-n:]
    ) or "(no prior conversation)"


# ── Markdown scrubber ─────────────────────────────────────────────────────────

def _strip_markdown(text: str) -> str:
    lines = text.splitlines()
    lines = [l for l in lines if not re.match(r"^\s*\|", l)]
    text = "\n".join(lines)
    text = text.replace("₹", "Rs ")
    text = re.sub(r"\*{1,3}(.+?)\*{1,3}", r"\1", text, flags=re.DOTALL)
    text = re.sub(r"_{1,3}(.+?)_{1,3}", r"\1", text, flags=re.DOTALL)
    text = re.sub(r"(?m)^[ \t]*(?:[-*#]+|[0-9]+\.)[ \t]+", "", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


# ── Node A: Classify + Extract + API selection ────────────────────────────────

CLASSIFY_AND_EXTRACT_PROMPT = """\
You are a financial query analyzer for an Indian stock-market chatbot.

## Conversation history (last 5 turns)
{history}

## Current user query
{query}

Return ONLY a valid JSON object with NO extra text, preamble, or markdown:
{{
  "query_type": "<greeting|general|company|comparison|investment>",
  "is_broad": <true|false>,
  "mcp_needed": <true|false>,
  "mcp_tool_hint": "<tool_name_or_empty>",
  "primary": {{
    "company_name": "<canonical name or null>",
    "nse_symbol":   "<NSE ticker or null>",
    "scheme_name":  "<MF scheme name or null>",
    "amc_name":     "<AMC / fund house name or null>",
    "intent":       "<pe_ratio|eps|mcap|quarterly_results|profit_and_loss|balance_sheet|key_ratios|daily_ratios|nav|mf_returns|mf_holdings|mf_sector|mf_expense|general>",
    "report_type":  "<s or c>",
    "relevant_apis": ["<api_key_1>", "<api_key_2>"]
  }},
  "additional_companies": [
    {{"company_name": "...", "nse_symbol": "..."}}
  ]
}}

Classification rules:
- "greeting"    : Hello, hi, thanks, bye, what can you do, small talk
- "general"     : Generic financial concept, no specific company
- "company"     : Question about ONE specific company OR mutual fund scheme
- "comparison"  : Explicit/implicit comparison between TWO OR MORE companies/schemes
- "investment"  : Buy recommendation, which stock/fund to invest in

mcp_needed — set TRUE when the user asks for:
  - Live/current/today stock price, LTP, intraday data
  - NAV (net asset value) of any mutual fund scheme
  - Any MF-specific data: holdings, sector allocation, asset allocation,
    expense ratio, exit load, SIP details, scheme returns, fund manager,
    dividend, NFO, fund performance, category performance, AUM,
    scheme comparison, portfolio changes, risk ratios (Alpha/Beta/Sharpe),
    lumpsum return calculator
  - Anything requiring the MF MCP server tools
  - Listing schemes / funds offered by an AMC (e.g. "what schemes does SBI MF provide?")
  - Any question about what an AMC offers, its fund categories, or scheme list
  Set FALSE for historical fundamental data (P&L, balance sheet, key ratios, PE).

mcp_tool_hint — when mcp_needed=true, suggest the best MCP tool:
  nav query          → "get_scheme_nav"
  returns query      → "get_scheme_returns"
  holdings           → "get_mf_holdings"
  sector allocation  → "get_sector_allocation"
  asset allocation   → "get_asset_allocation"
  expense/load       → "get_expense_ratio"
  SIP details        → "get_scheme_sip_details"
  risk ratios        → "get_scheme_ratios"
  dividend           → "get_dividend_details"
  fund performance   → "get_fund_performance"
  category returns   → "get_category_performance"
  scheme comparison  → "compare_schemes"
  portfolio changes  → "get_portfolio_changes"
  fund manager list  → "get_fund_managers"
  all AMC schemes    → "get_schemes_by_amc"
  NFO                → "get_new_fund_offers"
  market activity    → "get_mf_market_activity"
  lumpsum calculator → "get_lumpsum_returns"
  scheme listing / AMC schemes → "get_schemes_by_amc"
  AMC fund categories          → "get_fund_categories"
  (leave empty if unclear)

is_broad — set true ONLY when the user explicitly asks for:
  "full analysis", "complete report", "deep dive", "everything about X",
  "all financials", "comprehensive report", "detailed analysis".
  Default is FALSE. mcp_needed queries are NOT broad by default.

Entity rules:
- Resolve brand names: "Maggi" → "Nestle India", "Jio" → "Reliance Industries"
- Use conversation history to resolve pronouns ("it", "the company", "them")
- "additional_companies" is non-empty only for comparison/investment

API selection (relevant_apis — pick 2-3, ignored when is_broad=true or mcp_needed=true):
  pe_ratio / valuation  → ["daily_ratios", "key_ratios"]
  eps / earnings        → ["daily_ratios", "key_ratios", "quarterly_results"]
  revenue / profit      → ["quarterly_results", "profit_and_loss"]
  balance sheet / debt  → ["balance_sheet", "key_ratios"]
  comparison / invest   → ["daily_ratios", "key_ratios", "quarterly_results"]
  general / unclear     → ["daily_ratios", "key_ratios"]
"""

def node_classify_and_extract(state: AgentState) -> AgentState:
    console.print("[Node A] Classify + extract + API selection")
    history = state.get("conversation_history") or []
    history_text = "\n".join(
        f"{t['role'].upper()}: {t['content']}" for t in history[-10:]
    ) or "(none)"

    query_lower   = state.get("user_query", "").lower()
    keyword_broad = any(kw in query_lower for kw in _BROAD_KEYWORDS)
    keyword_mcp   = any(kw in query_lower for kw in _MCP_KEYWORDS)

    # Additional pattern: any query mentioning an AMC name + "scheme/fund/provide/offer"
    _AMC_NAMES = {"sbi", "hdfc", "icici", "axis", "nippon", "mirae", "kotak",
                "uti", "ppfas", "parag parikh", "quant", "tata", "dsp",
                "franklin", "aditya birla", "absl", "edelweiss", "bandhan",
                "motilal", "canara", "navi", "pgim", "whiteoak", "360 one"}

    _LISTING_WORDS = {"scheme", "schemes", "fund", "funds", "provide", "provides",
                    "offer", "offers", "available", "list", "show", "what"}
    if not keyword_mcp:
        words = set(query_lower.split())
        has_amc = any(amc in query_lower for amc in _AMC_NAMES)
        has_listing = bool(words & _LISTING_WORDS)
        if has_amc and has_listing:
            keyword_mcp = True
    try:
        result = _llm_json(
            CLASSIFY_AND_EXTRACT_PROMPT.format(
                history=history_text,
                query=state["user_query"],
            )
        )
        state["query_type"]    = result.get("query_type", "general")
        state["is_broad"]      = result.get("is_broad", False) or keyword_broad
        state["mcp_needed"]    = result.get("mcp_needed", False) or keyword_mcp
        state["mcp_tool_hint"] = result.get("mcp_tool_hint", "")

        primary = result.get("primary") or {}
        state["extracted_entity"] = primary.get("company_name") or state["user_query"]
        state["nse_symbol"]       = primary.get("nse_symbol")
        state["intent"]           = primary.get("intent", "daily_ratios")
        state["report_type"]      = primary.get("report_type", "s")
        state["mcp_scheme_name"]  = primary.get("scheme_name")
        state["mcp_amc_name"]     = primary.get("amc_name")

        raw_apis = primary.get("relevant_apis") or []
        if raw_apis and not state["is_broad"] and not state["mcp_needed"]:
            rt = state["report_type"]
            state["relevant_apis"] = [
                (k, None if k == "daily_ratios" else rt)
                for k in raw_apis[:3]
            ]
        else:
            state["relevant_apis"] = INTENT_API_MAP.get(state["intent"], _DEFAULT_APIS)

        additional = result.get("additional_companies") or []
        if additional:
            state["companies"] = [
                {"name": c.get("company_name", ""), "nse_symbol": c.get("nse_symbol")}
                for c in additional
            ]

        console.print(
            f"  → type={state['query_type']} broad={state['is_broad']} "
            f"mcp={state['mcp_needed']} entity={state['extracted_entity']} "
            f"tool_hint={state['mcp_tool_hint']}"
        )
    except Exception as e:
        console.print(f"Classify+extract failed: {e} — defaulting to general")
        state["query_type"]       = "general"
        state["is_broad"]         = keyword_broad
        state["mcp_needed"]       = keyword_mcp
        state["mcp_tool_hint"]    = ""
        state["extracted_entity"] = state["user_query"]
        state["intent"]           = "daily_ratios"
        state["report_type"]      = "s"
        state["relevant_apis"]    = _DEFAULT_APIS

    return state


# ── Node B: Greeting handler ──────────────────────────────────────────────────

GREETING_PROMPT = """\
You are EQUIFIZ — a friendly Indian stock-market AI assistant.
Respond warmly and briefly to the user's greeting or small talk.
Mention 1-2 things you can help with (PE ratios, quarterly results,
investment recommendations, company comparisons, live NAV, MF returns,
sector allocation, etc.).

User: {query}
"""

def node_greeting_handler(state: AgentState) -> AgentState:
    console.print("[Node B] Greeting handler")
    state["final_answer"] = _llm_text(GREETING_PROMPT.format(query=state["user_query"]))
    return state


# ── Node C: General handler ───────────────────────────────────────────────────

def node_general_handler(state: AgentState) -> AgentState:
    console.print("[Node C] General handler — vector search only")
    try:
        state["vector_context"] = vs.query(state["user_query"], n_results=5)
    except Exception as e:
        console.print(f"  General vector search failed: {e}")
        state["vector_context"] = []
    return state


# ── Symbol resolution ─────────────────────────────────────────────────────────

def _resolve_single(name: str, nse_symbol: Optional[str]) -> dict:
    if nse_symbol:
        row = db.lookup_by_nse_symbol(nse_symbol)
        if row:
            return {"co_code": row["co_code"], "company_info": row, "nse_symbol": nse_symbol}
    matches = db.fuzzy_search_company(name, limit=1)
    if matches:
        best = matches[0]
        return {
            "co_code":      best["co_code"],
            "company_info": best,
            "nse_symbol":   best.get("nsesymbol"),
        }
    return {}


def node_symbol_resolution(state: AgentState) -> AgentState:
    console.print("[Node 3] Symbol resolution")
    resolved = _resolve_single(
        state.get("extracted_entity", ""),
        state.get("nse_symbol"),
    )
    state["co_code"]      = resolved.get("co_code")
    state["company_info"] = resolved.get("company_info")
    state["nse_symbol"]   = resolved.get("nse_symbol")
    if resolved:
        console.print(f"  → co_code={state['co_code']}")
    else:
        console.print(f"  → No match for '{state.get('extracted_entity')}'")
    return state


# ═══════════════════════════════════════════════════════════════════════════════
# MCP Pre-Resolution Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _targeted_db_lookup(
    entity: str,
    table: str,
    name_col: str,
    id_col: str,
) -> Optional[dict]:
    """
    Generic fuzzy lookup for any table/column combination defined in
    PARAM_TO_TABLE_MAP.

    For scheme_master, the full row is returned (including mf_cocode) so
    that the caller can also populate the parent AMC code in a single query.

    Returns the best-matching row dict, or None if no match exceeds the 70
    confidence threshold.
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        console.print(f"🔍 [DB LOOKUP] Searching table '{table}' for entity: '{entity}'")
        # scheme_master gets a wider SELECT so mf_cocode is available to
        # the caller without a second round-trip.
        if table == "scheme_master":
            cur.execute("SELECT mf_schcode, mf_cocode, sch_name, category FROM scheme_master")
        else:
            cur.execute(f"SELECT {id_col}, {name_col} FROM {table}")

        rows = [dict(r) for r in cur.fetchall()]
        cur.close()
        conn.close()

        best_match:  Optional[dict] = None
        best_score:  int            = 0
        entity_lower = entity.lower()

        for row in rows:
            candidate = (row.get(name_col) or "").lower()
            score = max(
                fuzz.token_set_ratio(entity_lower, candidate),
                fuzz.partial_ratio(entity_lower, candidate),
            )
            if score > best_score:
                best_score, best_match = score, row

        if best_match and best_score >= 60:
            console.print(
                f"✅ [MATCH FOUND] Table: '{table}' | ID: {best_match[id_col]} | "
                f"Matched Name: '{best_match[name_col]}' | Score: {best_score}"
            )
            return best_match

        console.print(
            f"  [_targeted_db_lookup] No confident match in {table} for '{entity}' "
            f"(best score={best_score})"
        )
        return None

    except Exception as e:
        console.print(f"  [_targeted_db_lookup] DB error on {table}: {e}")
        return None


def _resolve_mf_codes_from_db(
    scheme_name: Optional[str],
    amc_name: Optional[str],
    entity_name: Optional[str],
) -> dict:
    """
    Resolve mf_cocode and mf_schcode from the local DB cache.
    Falls back through: scheme_name → amc_name → entity_name.

    Kept for use by node_mcp_multi_pre_resolve which still needs a
    self-contained resolution path per entity.
    """
    resolved: dict = {}

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        search_scheme = scheme_name or entity_name or ""
        if search_scheme:
            cur.execute("SELECT mf_schcode, mf_cocode, sch_name, category FROM scheme_master")
            schemes = [dict(r) for r in cur.fetchall()]
            best_score, best_scheme = 0, None
            for s in schemes:
                name_lower  = (s.get("sch_name") or "").lower()
                query_lower = search_scheme.lower()
                score = max(
                    fuzz.token_set_ratio(query_lower, name_lower),
                    fuzz.partial_ratio(query_lower, name_lower),
                )
                if score > best_score:
                    best_score, best_scheme = score, s
            if best_scheme and best_score >= 70:
                resolved["mf_schcode"]      = int(best_scheme["mf_schcode"])
                resolved["mf_cocode"]       = int(best_scheme["mf_cocode"])
                resolved["scheme_name"]     = best_scheme["sch_name"]
                resolved["scheme_category"] = best_scheme.get("category")
                console.print(
                    f"  [_resolve_mf_codes_from_db] Scheme resolved: "
                    f"{best_scheme['sch_name']} "
                    f"(schcode={resolved['mf_schcode']}, score={best_score})"
                )

        if not resolved.get("mf_cocode") and (amc_name or entity_name):
            search_amc = amc_name or entity_name or ""
            cur.execute("SELECT mf_cocode, lname, nameamc FROM fund_house")
            houses = [dict(r) for r in cur.fetchall()]
            best_score, best_house = 0, None
            for h in houses:
                nl = (h.get("lname") or "").lower()
                ns = (h.get("nameamc") or "").lower()
                ql = search_amc.lower()
                score = max(
                    fuzz.token_set_ratio(ql, nl), fuzz.partial_ratio(ql, nl),
                    fuzz.token_set_ratio(ql, ns), fuzz.partial_ratio(ql, ns),
                )
                if score > best_score:
                    best_score, best_house = score, h
            if best_house and best_score >= 70:
                resolved["mf_cocode"] = int(best_house["mf_cocode"])
                resolved["amc_name"]  = best_house["lname"]
                console.print(
                    f"  [_resolve_mf_codes_from_db] AMC resolved: "
                    f"{best_house['lname']} "
                    f"(cocode={resolved['mf_cocode']}, score={best_score})"
                )

        cur.close()
        conn.close()

    except Exception as e:
       console.print(f"  [_resolve_mf_codes_from_db] DB resolution failed: {e}")

    return resolved


# ═══════════════════════════════════════════════════════════════════════════════
# MCP Pre-Resolution Nodes
# ═══════════════════════════════════════════════════════════════════════════════

# def node_mcp_pre_resolve(state: AgentState) -> AgentState:
#     """
#     High-Precision Parameter Resolution (v5.1)

#     Strategy
#     ────────
#     1. Query the equifiz_tools vector collection to find the single best-
#        matching MCP tool for the user's query.
#     2. Read the tool's required_parameters from its metadata.
#     3. For each required parameter that appears in PARAM_TO_TABLE_MAP, call
#        _targeted_db_lookup() to resolve the numeric code from the appropriate
#        DB table using fuzzy matching on the extracted entity name.
#     4. For scheme lookups, the full row also carries mf_cocode so both codes
#        are resolved in one DB round-trip.
#     5. Fall back gracefully: if the tool vector collection is unavailable,
#        or the tool metadata is missing required_parameters, resolve using the
#        legacy _resolve_mf_codes_from_db() path to ensure backward compat.
#     """
#     logger.info("[Node MPR] High-Precision Parameter Resolution")

#     entity     = state.get("extracted_entity", "")
#     user_query = state.get("user_query", "")
#     resolved_codes: dict = {}

#     # ── Carry forward co_code from symbol_resolution if already present ───
#     if state.get("co_code"):
#         resolved_codes["co_code"] = state["co_code"]
#         logger.info(f"  co_code={state['co_code']} (carried from symbol_resolution)")

#     # ── Step 1: find the best-matching tool in the vector store ───────────
#     req_params: list[str] = []
#     try:
#         tool_results = vs.query_collection(
#             collection_name="equifiz_tools",
#             query=user_query,
#             n_results=1,
#         )
#         if tool_results:
#             meta       = tool_results[0].get("metadata", {})
#             tool_name  = meta.get("name", "(unknown)")
#             raw_params = meta.get("required_parameters", "")
#             req_params = [p.strip() for p in raw_params.split(",") if p.strip()]
#             logger.info(f"  Best tool: '{tool_name}' | required params: {req_params}")
#         else:
#             logger.warning("  No tool matched in equifiz_tools collection — falling back")
#     except Exception as e:
#         logger.warning(f"  Tool vector query failed: {e} — falling back to legacy resolution")

#     # ── Step 2: resolve each required parameter using PARAM_TO_TABLE_MAP ─
#     if req_params:
#         for param in req_params:
#             if param in resolved_codes:
#                 # Already resolved (e.g. co_code from symbol_resolution)
#                 continue
#             if param not in PARAM_TO_TABLE_MAP:
#                 logger.debug(f"  Param '{param}' not in PARAM_TO_TABLE_MAP — skipping")
#                 continue

#             cfg   = PARAM_TO_TABLE_MAP[param]
#             match = _targeted_db_lookup(
#                 entity   = entity,
#                 table    = cfg["table"],
#                 name_col = cfg["column"],
#                 id_col   = cfg["id_field"],
#             )

#             if match:
#                 resolved_codes[param] = int(match[cfg["id_field"]])

#                 # scheme_master rows also carry mf_cocode — grab it free of charge
#                 if param == "mf_schcode" and "mf_cocode" in match:
#                     resolved_codes.setdefault("mf_cocode", int(match["mf_cocode"]))
#                     logger.info(
#                         f"  mf_schcode={resolved_codes['mf_schcode']}, "
#                         f"mf_cocode={resolved_codes.get('mf_cocode')} "
#                         f"(from scheme_master row)"
#                     )
#                 else:
#                     logger.info(f"  {param}={resolved_codes[param]}")
#             else:
#                 logger.warning(f"  Could not resolve '{param}' for entity '{entity}'")

#     # ── Step 3: fallback — if nothing resolved yet, use legacy helper ─────
#     if not resolved_codes or (
#         "mf_schcode" not in resolved_codes and "mf_cocode" not in resolved_codes
#         and "co_code" not in resolved_codes
#     ):
#         logger.info("  Falling back to _resolve_mf_codes_from_db()")
#         mf_resolved = _resolve_mf_codes_from_db(
#             scheme_name = state.get("mcp_scheme_name"),
#             amc_name    = state.get("mcp_amc_name"),
#             entity_name = entity,
#         )
#         resolved_codes.update(mf_resolved)

#         # Sync friendly names back onto state
#         if mf_resolved.get("scheme_name"):
#             state["mcp_scheme_name"] = mf_resolved["scheme_name"]
#         if mf_resolved.get("amc_name"):
#             state["mcp_amc_name"] = mf_resolved["amc_name"]

#     # ── Commit ────────────────────────────────────────────────────────────
#     state["mcp_resolved_codes"] = resolved_codes

#     if resolved_codes:
#         logger.info(f"  Final resolved codes: {resolved_codes}")
#     else:
#         logger.warning(
#             f"  No codes resolved for entity='{entity}' / "
#             f"scheme='{state.get('mcp_scheme_name')}' / "
#             f"amc='{state.get('mcp_amc_name')}'"
#         )

#     return state
def node_mcp_pre_resolve(state: AgentState) -> AgentState:
    """
    High-Precision Parameter Resolution (v5.1)

    Strategy
    ────────
    1. Query the equifiz_tools vector collection to find the single best-
       matching MCP tool for the user's query.
    2. Read the tool's required_parameters from its metadata.
    3. For each required parameter that appears in PARAM_TO_TABLE_MAP, call
       _targeted_db_lookup() to resolve the numeric code from the appropriate
       DB table using fuzzy matching on the extracted entity name.
    4. For scheme lookups, the full row also carries mf_cocode so both codes
       are resolved in one DB round-trip.
    5. Fall back gracefully: if the tool vector collection is unavailable,
       or the tool metadata is missing required_parameters, resolve using the
       legacy _resolve_mf_codes_from_db() path to ensure backward compat.
    """
    console.print("[Node MPR] High-Precision Parameter Resolution")
    
    entity     = state.get("extracted_entity", "")
    user_query = state.get("user_query", "")
    resolved_codes: dict = {}
    # entity = (
    #     state.get("mcp_scheme_name")
    #     or state.get("mcp_amc_name")
    #     or state.get("extracted_entity", "")
    # )
    # user_query = state.get("user_query", "")
    # resolved_codes: dict = {}

    # ── Carry forward co_code from symbol_resolution if already present ───
    if state.get("co_code"):
        resolved_codes["co_code"] = state["co_code"]
        console.print(f"  co_code={state['co_code']} (carried from symbol_resolution)")

    # ── Step 1: find the best-matching tool in the vector store ───────────
    req_params: list[str] = []
    try:
        # --- HOTFIX: Direct ChromaDB connection to bypass missing attribute error ---
        import chromadb
        client = chromadb.PersistentClient(path="./chroma_db")
        collection = client.get_collection(name="equifiz_tools")
        
        # Query the collection directly
        res = collection.query(query_texts=[user_query], n_results=1)
        
        if res['ids'] and res['ids'][0]:
            meta       = res['metadatas'][0][0]
            tool_name  = res['ids'][0][0]
            raw_params = meta.get("required_parameters", "")
            req_params = [p.strip() for p in raw_params.split(",") if p.strip()]
            
            # LOGGER: Show which tool was matched semantically
            console.print(f"🎯 [CHROMA HIT] Tool: '{tool_name}' | Metadata requires: {req_params}")
        else:
            console.print("  No tool matched in equifiz_tools collection — falling back")
            
    except Exception as e:
        console.print(f"  Tool vector query failed: {e} — falling back to legacy resolution")

    # ── Step 2: resolve each required parameter using PARAM_TO_TABLE_MAP ─
    if req_params:
        for param in req_params:
            if param in resolved_codes:
                # Already resolved (e.g. co_code from symbol_resolution)
                continue
            if param not in PARAM_TO_TABLE_MAP:
                console.print(f"  Param '{param}' not in PARAM_TO_TABLE_MAP — skipping")
                continue

            cfg   = PARAM_TO_TABLE_MAP[param]
            
            # LOGGER: Show exactly which table is being called for which param
            console.print(f"📂 [ROUTING] Parameter '{param}' triggers search in table '{cfg['table']}'")
            
            match = _targeted_db_lookup(
                entity   = entity,
                table    = cfg["table"],
                name_col = cfg["column"],
                id_col   = cfg["id_field"],
            )

            if match:
                code_val = int(match[cfg["id_field"]])
                resolved_codes[param] = code_val
                
                # LOGGER: Success message with fetched code
                console.print(f"💎 [SUCCESS] Fetched {param}={code_val} from {cfg['table']}")

                # scheme_master rows also carry mf_cocode — grab it free of charge
                if param == "mf_schcode" and "mf_cocode" in match:
                    resolved_codes.setdefault("mf_cocode", int(match["mf_cocode"]))
                    console.print(
                        f"🔗 [LINKED] Also pulled mf_cocode={resolved_codes['mf_cocode']} "
                        f"(from scheme_master row)"
                    )
            else:
                console.print(f"  Could not resolve '{param}' for entity '{entity}'")

    # ── Step 3: fallback — if nothing resolved yet, use legacy helper ─────
    if not resolved_codes or (
        "mf_schcode" not in resolved_codes and "mf_cocode" not in resolved_codes
        and "co_code" not in resolved_codes
    ):
        console.print("🔄 [FALLBACK] Falling back to legacy _resolve_mf_codes_from_db()")
        mf_resolved = _resolve_mf_codes_from_db(
            scheme_name = state.get("mcp_scheme_name"),
            amc_name    = state.get("mcp_amc_name"),
            entity_name = entity,
        )
        resolved_codes.update(mf_resolved)

        # Sync friendly names back onto state
        if mf_resolved.get("scheme_name"):
            state["mcp_scheme_name"] = mf_resolved["scheme_name"]
        if mf_resolved.get("amc_name"):
            state["mcp_amc_name"] = mf_resolved["amc_name"]

    # ── Commit ────────────────────────────────────────────────────────────
    state["mcp_resolved_codes"] = resolved_codes

    if resolved_codes:
        console.print(f"🏁 [FINAL] Resolved codes: {resolved_codes}")
    else:
        console.print(
            f"  No codes resolved for entity='{entity}' / "
            f"scheme='{state.get('mcp_scheme_name')}' / "
            f"amc='{state.get('mcp_amc_name')}'"
        )

    return state

def node_mcp_multi_pre_resolve(state: AgentState) -> AgentState:
    """
    Pre-resolve codes for ALL companies in a multi-company MCP query.
    """
    console.print("[Node MMPR] MCP multi-company pre-resolution")

    to_resolve: list[tuple[str, Optional[str]]] = []
    if state.get("extracted_entity"):
        to_resolve.append((state["extracted_entity"], state.get("nse_symbol")))
    for c in (state.get("companies") or []):
        name = c.get("name") or c.get("company_name", "")
        if name:
            to_resolve.append((name, c.get("nse_symbol")))

    def _resolve_one(name: str, nse_sym: Optional[str]) -> dict:
        result: dict = {"name": name}
        stock = _resolve_single(name, nse_sym)
        if stock:
            result["co_code"]      = stock["co_code"]
            result["company_info"] = stock["company_info"]
            result["nse_symbol"]   = stock["nse_symbol"]
        mf = _resolve_mf_codes_from_db(
            scheme_name = name,
            amc_name    = name,
            entity_name = name,
        )
        result.update(mf)
        result["mcp_resolved_codes"] = {
            k: result[k]
            for k in ("co_code", "mf_cocode", "mf_schcode")
            if k in result
        }
        return result

    with ThreadPoolExecutor(max_workers=min(4, len(to_resolve))) as executor:
        futures = {
            executor.submit(_resolve_one, name, sym): name
            for name, sym in to_resolve
        }
        results_map: dict[str, dict] = {}
        for future in as_completed(futures):
            name = futures[future]
            try:
                results_map[name] = future.result()
            except Exception as e:
                results_map[name] = {"name": name, "error": str(e)}

    state["companies"] = [results_map.get(name, {"name": name}) for name, _ in to_resolve]

    if state["companies"]:
        first = state["companies"][0]
        state["mcp_resolved_codes"] = first.get("mcp_resolved_codes", {})
        if first.get("co_code"):
            state["co_code"] = first["co_code"]

    console.print(
        f"  Resolved {len(state['companies'])} entities: "
        + str([{c.get("name"): c.get("mcp_resolved_codes", {})} for c in state["companies"]])
    )
    return state


# ═══════════════════════════════════════════════════════════════════════════════
# MCP Tool Call Node  (v5 — asyncio-safe)
# ═══════════════════════════════════════════════════════════════════════════════

def _run_mcp_in_new_loop(enriched_query: str) -> str:
    from mcp_client import run_mcp_query

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(run_mcp_query(enriched_query))
    except ExceptionGroup as eg:
        sub_errors = [str(e) for e in eg.exceptions]
        console.print(f"MCP TaskGroup Error: {', '.join(sub_errors)}")
        return f"MCP Error: {sub_errors[0]}"
    except Exception as e:
        console.print(f"Internal MCP Task Error: {e}")
        return f"Error: {str(e)}"
    finally:
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
            for task in asyncio.all_tasks(loop):
                task.cancel()
        except Exception:
            pass
        loop.close()


# def node_mcp_tool_call(state: AgentState) -> AgentState:
#     """
#     Execute the MCP client query with pre-resolved codes.

#     v5 changes
#     ──────────
#     • Runs the async MCP query in a FRESH event loop on a dedicated thread
#       (via _run_mcp_in_new_loop) — this eliminates the TaskGroup crash that
#       occurred when asyncio.run() was called inside uvicorn's loop.
#     • Hard 600-second timeout via concurrent.futures.
#     • Catches TimeoutError and returns a clean error sentinel.
#     """
#     logger.info("[Node MTC] MCP tool call")

#     try:
#         from mcp_client import run_mcp_query  # noqa: F401 — validate import early
#     except ImportError:
#         logger.error("MCP client module not found. Ensure mcp_client.py is in the path.")
#         state["mcp_raw_result"] = ""
#         state["error"]          = "mcp_client_not_found"
#         return state

#     resolved_codes = state.get("mcp_resolved_codes") or {}
#     user_query     = state.get("user_query", "")
#     tool_hint      = state.get("mcp_tool_hint", "")
#     scheme_name    = state.get("mcp_scheme_name", "")
#     amc_name       = state.get("mcp_amc_name", "")

#     # ── Build enriched query with pre-resolved codes ───────────────────────
#     code_lines = []
#     if resolved_codes.get("mf_schcode"):
#         code_lines.append(
#             f"mf_schcode={resolved_codes['mf_schcode']}"
#             + (f" (scheme: {scheme_name})" if scheme_name else "")
#         )
#     if resolved_codes.get("mf_cocode"):
#         code_lines.append(
#             f"mf_cocode={resolved_codes['mf_cocode']}"
#             + (f" (AMC: {amc_name})" if amc_name else "")
#         )
#     if resolved_codes.get("co_code"):
#         code_lines.append(f"co_code={resolved_codes['co_code']}")

#     enriched_query = user_query
#     if code_lines:
#         enriched_query = (
#             f"{user_query}\n\n"
#             f"[PRE-RESOLVED CODES — use these directly in tool calls, "
#             f"do NOT call resolver tools again]\n"
#             + "\n".join(code_lines)
#         )
#     if tool_hint:
#         enriched_query += f"\n[SUGGESTED TOOL: {tool_hint}]"

#     logger.info(f"  Enriched query (first 200 chars): {enriched_query[:200]}")

#     # ── Execute on a dedicated thread with a fresh event loop ─────────────
#     try:
#         with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
#             future = executor.submit(_run_mcp_in_new_loop, enriched_query)
#             result = future.result(timeout=600)

#         state["mcp_raw_result"]      = result
#         state["mcp_tool_calls_made"] = []
#         state["error"]               = None
#         logger.info(f"  MCP result length: {len(result)} chars")

#     except concurrent.futures.TimeoutError:
#         logger.error("  MCP tool call timed out after 600 seconds")
#         state["mcp_raw_result"] = ""
#         state["error"]          = "mcp_call_timeout"

#     except Exception as e:
#         logger.error(f"  MCP tool call failed: {e}")
#         state["mcp_raw_result"] = ""
#         state["error"]          = f"mcp_call_failed: {e}"

#     return state


def node_mcp_tool_call(state: AgentState) -> AgentState:
    """
    Execute the MCP client query with high-authority prompt injection.
    
    Updated to include strict System Instructions to force the internal LLM 
    to use pre-resolved ID values for tool arguments.
    """
    console.print("[Node MTC] MCP tool call")

    try:
        from mcp_client import run_mcp_query
    except ImportError:
        console.print("MCP client module not found.")
        state["mcp_raw_result"] = ""
        state["error"] = "mcp_client_not_found"
        return state

    resolved_codes = state.get("mcp_resolved_codes") or {}
    user_query     = state.get("user_query", "")
    tool_hint      = state.get("mcp_tool_hint", "")
    scheme_name    = state.get("mcp_scheme_name", "")
    amc_name       = state.get("mcp_amc_name", "")

    # ── Build high-authority injection block ───────────────────────────────
   # ── Minimal, Clear Injection ───────────────────────────────────────────
    # Inside graph.py -> node_mcp_tool_call

    code_lines = []
    if resolved_codes.get("mf_schcode"):
        code_lines.append(f"mf_schcode={resolved_codes['mf_schcode']}")
    if resolved_codes.get("mf_cocode"):
        code_lines.append(f"mf_cocode={resolved_codes['mf_cocode']}")

    if code_lines:
        # Wrap IDs in a specific tag for easy stripping
        injection_block = "\n".join(code_lines)
        enriched_query = (
            f"<PRE_RESOLVED>\n{injection_block}\n</PRE_RESOLVED>\n"
            f"User Query: {user_query}"
        )
    else:
        enriched_query = user_query

    if tool_hint:
        enriched_query += f"\n\n[RECOMMENDED TOOL: {tool_hint}]"

    console.print(f"📤 [MCP SEND] Enriched Query: {enriched_query[:300]}...")

    # ── Execute logic (isolated thread/loop) ─────────────────────────────
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_run_mcp_in_new_loop, enriched_query)
            # Keeping your 600s timeout
            result = future.result(timeout=600)

        if not result:
            console.print("📥 [MCP RECEIVE] Empty result from server")
            state["mcp_raw_result"] = "No data returned from server."
        else:
            console.print(f"📥 [MCP RECEIVE] Result length: {len(result)} chars")
            state["mcp_raw_result"] = result
            
        state["mcp_tool_calls_made"] = []
        state["error"] = None

    except concurrent.futures.TimeoutError:
        console.print("🚨 [TIMEOUT] MCP call timed out")
        state["mcp_raw_result"] = ""
        state["error"] = "mcp_call_timeout"

    except Exception as e:
        console.print(f"💥 [CRASH] MCP tool call failed: {e}")
        state["mcp_raw_result"] = ""
        state["error"] = f"mcp_call_failed: {e}"

    return state


# ── MCP Synthesis Node ────────────────────────────────────────────────────────

MCP_SYNTHESIS_PROMPT = """\
You are a knowledgeable Indian equity and mutual fund analyst assistant.

## Conversation history (last 4 turns)
{history}

## User Question
{user_query}

## Live Data from MCP Server
{mcp_result}

## Instructions
1. Answer the user's question using the live data above.
2. Write in plain prose paragraphs only. Do NOT use markdown tables, bullet
   points, hyphens as list markers, asterisks, or any special formatting
   characters. Do NOT use the rupee symbol — write "Rs" instead.
3. Present key numbers naturally woven into sentences.
4. Be honest if specific data fields are missing or unavailable.
5. Use Indian number formatting (Rs Cr for large numbers).
6. Keep under 250 words unless detail is explicitly requested.
7. End with: This is not financial advice.
"""

# Error sentinels and their user-facing messages
_MCP_ERROR_MESSAGES = {
    "mcp_client_not_found": (
        "Sorry, the live data service is currently unavailable. "
        "Please try again shortly."
    ),
    "mcp_call_timeout": (
        "The live data request timed out. The server may be busy — "
        "please try again in a moment."
    ),
}

def node_mcp_synthesis(state: AgentState) -> AgentState:
    console.print("[Node MS] MCP synthesis")
    history_text = _format_history(state)
    mcp_result   = state.get("mcp_raw_result", "")
    error        = state.get("error", "")

    # ── Hard error cases ───────────────────────────────────────────────────
    if error in _MCP_ERROR_MESSAGES:
        state["final_answer"] = _MCP_ERROR_MESSAGES[error]
        return state

    if error and error.startswith("mcp_call_failed"):
        state["final_answer"] = (
            "The live data query encountered an error. "
            "Please try again or rephrase your question."
        )
        return state

    if not mcp_result:
        state["final_answer"] = (
            "No data was returned from the live server. "
            "Please try again shortly."
        )
        return state

    # ── Early-exit: data tool returned "not found" on its first (and only) round ──
    result_lower = mcp_result.lower()
    if any(sig in result_lower for sig in _MCP_NOT_FOUND_SIGNALS):
        console.print("  MCP result contains 'not found' signal — synthesising from raw text")

    # ── Normal synthesis ───────────────────────────────────────────────────
    prompt = MCP_SYNTHESIS_PROMPT.format(
        history=history_text,
        user_query=state.get("user_query", ""),
        mcp_result=mcp_result[:3000],
    )
    try:
        state["final_answer"] = _llm_text(prompt)
    except Exception as e:
        console.print(f"MCP synthesis LLM call failed: {e}")
        state["final_answer"] = mcp_result
    return state


# ── Shared API helpers ────────────────────────────────────────────────────────

_API_FETCHERS = {
    "daily_ratios":      lambda co, rt: api.fetch_daily_ratios(co, rt),
    "quarterly_results": lambda co, rt: api.fetch_quarterly_results(co, rt),
    "profit_and_loss":   lambda co, rt: api.fetch_profit_and_loss(co, rt),
    "balance_sheet":     lambda co, rt: api.fetch_balance_sheet(co, rt),
    "key_ratios":        lambda co, rt: api.fetch_key_financial_ratios(co, rt),
}


def _safe_code(raw) -> str:
    try:
        return str(int(float(raw)))
    except (TypeError, ValueError):
        return str(raw).strip()


def _records_to_text(records: Any, api_name: str) -> str:
    lines = [f"API: {api_name}  |  Date: {TODAY}"]
    if isinstance(records, dict):
        inner   = records.get("data", records)
        records = inner if isinstance(inner, list) else [inner]
    if isinstance(records, list):
        for rec in records:
            if not isinstance(rec, dict):
                continue
            line = "  |  ".join(
                f"{k}: {v}" for k, v in rec.items() if v not in (None, "", "null")
            )
            if line:
                lines.append(line)
    return "\n".join(lines)


def _fetch_one_raw(co_code: int, api_key: str, fin_type: Optional[str]) -> tuple[str, Any]:
    label   = f"{api_key}_{fin_type}" if fin_type else api_key
    fetcher = _API_FETCHERS.get(api_key)
    if not fetcher:
        return label, None
    try:
        return label, fetcher(co_code, fin_type or "s")
    except Exception as e:
        console.print(f"  Fetch failed [{label}]: {e}")
        return label, None


# ═══════════════════════════════════════════════════════════════════════════════
# FAST PATH — direct fetch, no ChromaDB
# ═══════════════════════════════════════════════════════════════════════════════

def node_direct_api_fetch(state: AgentState) -> AgentState:
    console.print("[Node D] Direct API fetch (no ChromaDB)")
    co_code = state.get("co_code")
    if not co_code:
        state["raw_api_data"] = {}
        state["error"]        = f"co_code not resolved for '{state.get('extracted_entity')}'"
        return state

    apis     = state.get("relevant_apis") or _DEFAULT_APIS
    raw_data: dict[str, Any] = {}

    with ThreadPoolExecutor(max_workers=min(4, len(apis))) as executor:
        futures = {
            executor.submit(_fetch_one_raw, co_code, api_key, fin_type): (api_key, fin_type)
            for api_key, fin_type in apis
        }
        for future in as_completed(futures):
            try:
                label, records = future.result()
                if records:
                    raw_data[label] = records
            except Exception as e:
                api_key, fin_type = futures[future]
                console.print(f"  Executor error [{api_key}]: {e}")

    state["raw_api_data"] = raw_data
    state["error"]        = "all_apis_failed" if not raw_data else None
    console.print(f"  → {len(raw_data)}/{len(apis)} APIs fetched")
    return state


# ── Multi-company direct fetch ────────────────────────────────────────────────

def _resolve_and_direct_fetch(
    name: str,
    nse_sym: Optional[str],
    apis: list[tuple[str, Optional[str]]],
) -> dict:
    resolved = _resolve_single(name, nse_sym)
    if not resolved:
        return {"name": name, "error": f"Could not resolve '{name}'"}

    co_code  = resolved["co_code"]
    raw_data: dict[str, Any] = {}

    with ThreadPoolExecutor(max_workers=min(4, len(apis))) as executor:
        futures = {
            executor.submit(_fetch_one_raw, co_code, api_key, fin_type): (api_key, fin_type)
            for api_key, fin_type in apis
        }
        for future in as_completed(futures):
            try:
                label, records = future.result()
                if records:
                    raw_data[label] = records
            except Exception as e:
                api_key, fin_type = futures[future]
                console.print(f"  [{name}] executor error [{api_key}]: {e}")

    return {
        "name":            name,
        "co_code":         co_code,
        "company_info":    resolved["company_info"],
        "nse_symbol":      resolved["nse_symbol"],
        "raw_api_data":    raw_data,
        "all_apis_failed": not raw_data,
    }


def node_multi_direct_fetch(state: AgentState) -> AgentState:
    console.print("[Node MD] Multi-company direct fetch")
    apis = state.get("relevant_apis") or _DEFAULT_APIS

    to_resolve: list[tuple[str, Optional[str]]] = []
    if state.get("extracted_entity"):
        to_resolve.append((state["extracted_entity"], state.get("nse_symbol")))
    for c in (state.get("companies") or []):
        name = c.get("name") or c.get("company_name", "")
        if name:
            to_resolve.append((name, c.get("nse_symbol")))

    with ThreadPoolExecutor(max_workers=min(4, len(to_resolve))) as executor:
        futures = {
            executor.submit(_resolve_and_direct_fetch, name, sym, apis): name
            for name, sym in to_resolve
        }
        results_map: dict[str, dict] = {}
        for future in as_completed(futures):
            name = futures[future]
            try:
                results_map[name] = future.result()
            except Exception as e:
                results_map[name] = {"name": name, "error": str(e)}
                console.print(f"  Failed for {name}: {e}")

    state["companies"] = [results_map.get(name, {"name": name}) for name, _ in to_resolve]
    return state


# ═══════════════════════════════════════════════════════════════════════════════
# DEEP PATH — full ChromaDB ingestion (is_broad=True only)
# ═══════════════════════════════════════════════════════════════════════════════

def _chunks_are_fresh(co_code_str: str, api_name: str, fin_type: str) -> bool:
    try:
        col = vs._get_collection()
        where_clauses = [
            {"api_name":     {"$eq": api_name}},
            {"co_code":      {"$eq": co_code_str}},
            {"refreshed_at": {"$eq": TODAY}},
        ]
        if fin_type:
            where_clauses.append({"fin_type": {"$eq": fin_type}})
        existing = col.get(where={"$and": where_clauses}, include=["metadatas"])
        return len(existing.get("ids", [])) > 0
    except Exception:
        return False


def _ingest_chunks(text: str, api_name: str, co_code: str, fin_type: str = "") -> int:
    if not text.strip():
        return 0
    col = vs._get_collection()
    where_clauses = [{"api_name": {"$eq": api_name}}, {"co_code": {"$eq": co_code}}]
    if fin_type:
        where_clauses.append({"fin_type": {"$eq": fin_type}})
    try:
        stale = col.get(where={"$and": where_clauses}, include=["metadatas"])
        if stale["ids"]:
            col.delete(ids=stale["ids"])
    except Exception as e:
        console.print(f"    Stale delete failed [{api_name}]: {e}")
    chunks     = splitter.split_text(text)
    if not chunks:
        return 0
    embeddings = vs.embed(chunks)
    ids        = [str(uuid.uuid4()) for _ in chunks]
    metadatas  = [
        {
            "source_type":  "live",
            "api_name":     api_name,
            "co_code":      co_code,
            "fin_type":     fin_type,
            "refreshed_at": TODAY,
            "content_hash": hashlib.md5(text.encode()).hexdigest(),
            "chunk_id":     str(i),
        }
        for i, _ in enumerate(chunks)
    ]
    col.add(ids=ids, documents=chunks, embeddings=embeddings, metadatas=metadatas)
    return len(chunks)


def _fetch_and_ingest_one(
    co_code: int, co_code_str: str,
    api_key: str, fin_type: Optional[str],
    force_refresh: bool,
) -> tuple[str, str]:
    label   = f"{api_key}_{fin_type}" if fin_type else api_key
    fetcher = _API_FETCHERS.get(api_key)
    if not fetcher:
        return label, "failed"
    if not force_refresh and _chunks_are_fresh(co_code_str, label, fin_type or ""):
        return label, "fresh"
    try:
        raw = fetcher(co_code, fin_type or "s")
    except Exception as e:
        console.print(f"  Fetch failed [{label}]: {e}")
        return label, "failed"
    if not raw:
        return label, "failed"
    n = _ingest_chunks(
        _records_to_text(raw, label),
        api_name=label,
        co_code=co_code_str,
        fin_type=fin_type or "",
    )
    console.print(f"  [{label}] → {n} chunks ingested")
    return label, "ok"


def _ingest_co_code(co_code: int, force_refresh: bool = False) -> tuple[list[str], list[str]]:
    co_code_str = _safe_code(co_code)
    if not force_refresh:
        all_fresh = all(
            _chunks_are_fresh(co_code_str, f"{k}_{t}" if t else k, t or "")
            for k, t in _ALL_FINANCIAL_APIS
        )
        if all_fresh:
            console.print(f"  All fresh today for {co_code_str} — skipping ingest")
            return [f"{k}_{t}" if t else k for k, t in _ALL_FINANCIAL_APIS], []

    ingested: list[str] = []
    failed:   list[str] = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(
                _fetch_and_ingest_one,
                co_code, co_code_str, api_key, fin_type, force_refresh
            ): (api_key, fin_type)
            for api_key, fin_type in _ALL_FINANCIAL_APIS
        }
        for future in as_completed(futures):
            try:
                label, status = future.result()
                (ingested if status in ("ok", "fresh") else failed).append(label)
            except Exception as e:
                api_key, fin_type = futures[future]
                failed.append(f"{api_key}_{fin_type}" if fin_type else api_key)
                console.print(f"  Executor error: {e}")
    return ingested, failed


def node_full_ingest(state: AgentState) -> AgentState:
    console.print("[Node FI] Full ChromaDB ingest")
    co_code = state.get("co_code")
    if not co_code:
        state["ingested_apis"] = []
        state["failed_apis"]   = []
        state["error"]         = f"co_code not resolved for '{state.get('extracted_entity')}'"
        return state

    ingested, failed = _ingest_co_code(co_code, force_refresh=state.get("force_refresh", False))
    state["ingested_apis"] = ingested
    state["failed_apis"]   = failed
    state["error"]         = "all_apis_failed" if not ingested else None
    return state


def node_vector_retrieval(state: AgentState) -> AgentState:
    console.print("[Node VR] Vector retrieval")
    query_parts = [state.get("user_query", "")]
    company     = state.get("company_info") or {}
    if company.get("sectorname"):
        query_parts.append(company["sectorname"])
    if state.get("intent"):
        query_parts.append(state["intent"])

    co_code_str  = _safe_code(state["co_code"]) if state.get("co_code") else ""
    search_query = " | ".join(query_parts)
    where_filter = {"co_code": {"$eq": co_code_str}} if co_code_str else None

    try:
        results = vs.query(search_query, n_results=8, where=where_filter)
    except Exception as e:
        console.print(f"  Filtered retrieval failed ({e}), retrying without filter")
        try:
            results = vs.query(search_query, n_results=8)
        except Exception as e2:
            console.print(f"  Retrieval failed: {e2}")
            results = []

    state["vector_context"] = results

    chunks_by_api: dict[str, list[str]] = {}
    for r in results:
        key = r["metadata"].get("api_name", "retrieved")
        chunks_by_api.setdefault(key, []).append(r["document"])
    state["raw_api_data"] = {k: "\n".join(v) for k, v in chunks_by_api.items()}

    console.print(f"  → {len(results)} chunks retrieved across {len(chunks_by_api)} APIs")
    return state


def _resolve_and_ingest(name: str, nse_sym: Optional[str], force: bool) -> dict:
    resolved = _resolve_single(name, nse_sym)
    if not resolved:
        return {"name": name, "error": f"Could not resolve '{name}'"}
    co_code          = resolved["co_code"]
    ingested, failed = _ingest_co_code(co_code, force_refresh=force)
    co_code_str      = _safe_code(co_code)
    try:
        results = vs.query(name, n_results=6, where={"co_code": {"$eq": co_code_str}})
    except Exception:
        try:
            results = vs.query(name, n_results=6)
        except Exception:
            results = []
    chunks_by_api: dict[str, list[str]] = {}
    for r in results:
        key = r["metadata"].get("api_name", "retrieved")
        chunks_by_api.setdefault(key, []).append(r["document"])
    return {
        "name":             name,
        "co_code":          co_code,
        "company_info":     resolved["company_info"],
        "nse_symbol":       resolved["nse_symbol"],
        "raw_api_data":     {k: "\n".join(v) for k, v in chunks_by_api.items()},
        "all_apis_failed":  not ingested,
        "has_partial_data": bool(failed),
    }


def node_multi_full_ingest(state: AgentState) -> AgentState:
    console.print("[Node MFI] Multi-company full ingest (broad)")
    force        = state.get("force_refresh", False)
    to_resolve: list[tuple[str, Optional[str]]] = []
    if state.get("extracted_entity"):
        to_resolve.append((state["extracted_entity"], state.get("nse_symbol")))
    for c in (state.get("companies") or []):
        name = c.get("name") or c.get("company_name", "")
        if name:
            to_resolve.append((name, c.get("nse_symbol")))

    with ThreadPoolExecutor(max_workers=min(4, len(to_resolve))) as executor:
        futures = {
            executor.submit(_resolve_and_ingest, name, sym, force): name
            for name, sym in to_resolve
        }
        results_map: dict[str, dict] = {}
        for future in as_completed(futures):
            name = futures[future]
            try:
                results_map[name] = future.result()
            except Exception as e:
                results_map[name] = {"name": name, "error": str(e)}

    state["companies"] = [results_map.get(name, {"name": name}) for name, _ in to_resolve]
    return state


# ═══════════════════════════════════════════════════════════════════════════════
# Summarisation nodes
# ═══════════════════════════════════════════════════════════════════════════════

PER_API_SUMMARY_PROMPT = """\
You are a financial data summariser for an Indian stock market assistant.

API: {api_label}
Company: {company_name}
User intent: {intent}

Raw data (truncated to 3000 chars):
{raw_text}

Extract only the figures directly relevant to the user's intent.
Return 3-6 bullet points, each under 20 words.
Use Indian number formatting (Rs Cr). Factual only — no commentary.
Output ONLY the bullet points, nothing else.
"""

def node_summarise_api_responses(state: AgentState) -> AgentState:
    console.print("[Node E] Per-API summarisation")
    raw_data     = state.get("raw_api_data") or {}
    company_info = state.get("company_info") or {}
    company_name = company_info.get("companyname") or state.get("extracted_entity", "the company")
    intent       = state.get("intent", "general")

    def _summarise_one(label: str, records: Any) -> tuple[str, str]:
        if isinstance(records, str):
            raw_text = records[:3000]
        else:
            raw_text = _records_to_text(records, label)[:3000]
        prompt = PER_API_SUMMARY_PROMPT.format(
            api_label=label,
            company_name=company_name,
            intent=intent,
            raw_text=raw_text,
        )
        try:
            return label, _llm_text(prompt)
        except Exception as e:
            console.print(f"  Summarise failed [{label}]: {e}")
            return label, raw_text[:400]

    summaries: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=min(4, len(raw_data))) as executor:
        futures = {
            executor.submit(_summarise_one, lbl, recs): lbl
            for lbl, recs in raw_data.items()
        }
        for future in as_completed(futures):
            try:
                label, summary = future.result()
                summaries[label] = summary
            except Exception as e:
                lbl = futures[future]
                summaries[lbl] = "(summary unavailable)"
                console.print(f"  Executor error [{lbl}]: {e}")

    state["api_summaries"] = summaries
    return state


PER_COMPANY_SUMMARY_PROMPT = """\
You are summarising financial data for one company.

Company: {company_name} (NSE: {nse_symbol})
User question: {user_query}

Financial data:
{data_text}

Extract the most relevant figures: PE, EPS, Revenue, Profit, ROE,
Debt/Equity, MCAP, Dividend Yield — wherever available.
Return 5-8 bullet points, each under 20 words.
Use Indian number formatting (Rs Cr). Factual only.
Output ONLY the bullet points, nothing else.
"""

def node_per_company_summarise(state: AgentState) -> AgentState:
    console.print("[Node PCS] Per-company summarisation")
    companies  = state.get("companies") or []
    user_query = state.get("user_query", "")

    def _summarise(c: dict) -> dict:
        if c.get("error") or c.get("all_apis_failed"):
            c["summary"] = "(no data available)"
            return c
        raw_api_data = c.get("raw_api_data") or {}
        data_text = "\n\n".join(
            f"### {label}\n"
            + (_records_to_text(v, label) if not isinstance(v, str) else v)
            for label, v in raw_api_data.items()
        )[:4000]
        prompt = PER_COMPANY_SUMMARY_PROMPT.format(
            company_name=c.get("name", "Unknown"),
            nse_symbol=c.get("nse_symbol", "N/A"),
            user_query=user_query,
            data_text=data_text or "(no data retrieved)",
        )
        try:
            c["summary"] = _llm_text(prompt)
        except Exception as e:
            console.print(f"  Per-company summarise failed [{c.get('name')}]: {e}")
            c["summary"] = data_text[:400]
        return c

    with ThreadPoolExecutor(max_workers=min(4, len(companies))) as executor:
        futures = {executor.submit(_summarise, c): i for i, c in enumerate(companies)}
        updated = [None] * len(companies)
        for future in as_completed(futures):
            idx = futures[future]
            try:
                updated[idx] = future.result()
            except Exception as e:
                updated[idx] = companies[idx]
                console.print(f"  Summarise executor error at idx {idx}: {e}")

    state["companies"] = [c for c in updated if c is not None]
    return state


# ═══════════════════════════════════════════════════════════════════════════════
# Synthesis nodes
# ═══════════════════════════════════════════════════════════════════════════════

def _networks_busy_reply(user_query: str, entity: str) -> str:
    BUSY = (
        "You are EQUIFIZ. The user asked: \"{q}\". "
        "All live APIs failed for \"{e}\". "
        "Respond politely in under 60 words: apologise, note networks are busy, suggest retry."
    )
    try:
        return _llm_text(BUSY.format(q=user_query, e=entity))
    except Exception:
        return f"Sorry, data networks are currently busy for {entity}. Please try again shortly."


SYNTHESIS_PROMPT = """\
You are a knowledgeable Indian equity analyst assistant.

## Conversation history (last 4 turns)
{history}

## User Question
{user_query}

## Company Info
{company_info}

## Financial Data Summaries
{summaries}

{partial_note}

## Instructions
1. Answer the user's question concisely using the summarised data above.
2. Write in plain prose paragraphs only. Do NOT use markdown tables, bullet
   points, hyphens as list markers, asterisks, or any special formatting
   characters. Do NOT use the rupee symbol — write "Rs" instead.
3. Provide sector comparison where relevant, woven naturally into sentences.
4. Highlight if valuation looks expensive or cheap vs sector.
5. Use Indian number formatting (Rs Cr for large numbers — spell out "Rs").
6. Be honest if data is unavailable.
7. Keep under 250 words unless detail is explicitly requested.
"""

GENERAL_SYNTHESIS_PROMPT = """\
You are a knowledgeable Indian equity analyst assistant.

## Conversation history (last 4 turns)
{history}

## User Question
{user_query}

## Knowledge Base Context
{vector_context}

## Instructions
1. Answer the conceptual/general question clearly and concisely.
2. Write in plain prose paragraphs only. Do NOT use markdown tables, bullet
   points, hyphens as list markers, asterisks, or any special formatting
   characters. Do NOT use the rupee symbol — write "Rs" instead.
3. Use simple language; give Indian market examples where useful.
4. If context docs don't cover the topic, answer from general knowledge.
5. Keep under 200 words.
"""

def node_synthesis(state: AgentState) -> AgentState:
    console.print("[Node S] Synthesis")
    history_text = _format_history(state)
    query_type   = state.get("query_type", "company")
    error        = state.get("error")

    if query_type == "general":
        vector_context = state.get("vector_context") or []
        context_text   = "\n".join(
            f"- [{r['metadata'].get('api_name', '')}] {r['document']}"
            for r in vector_context
        ) or "No relevant context found."
        prompt = GENERAL_SYNTHESIS_PROMPT.format(
            history=history_text,
            user_query=state["user_query"],
            vector_context=context_text,
        )
        try:
            state["final_answer"] = _llm_text(prompt)
        except Exception as e:
            state["final_answer"] = f"Synthesis failed: {e}"
        return state

    api_summaries = state.get("api_summaries") or {}

    if error == "all_apis_failed" and not api_summaries:
        state["final_answer"] = _networks_busy_reply(
            state.get("user_query", ""),
            state.get("extracted_entity", "the requested company"),
        )
        return state

    if error and not api_summaries:
        state["final_answer"] = (
            f"Sorry, I couldn't find data for '{state.get('extracted_entity')}'. "
            "Please check the company name or NSE symbol and try again."
        )
        return state

    summaries_text = "\n\n".join(
        f"### {label}\n{summary}" for label, summary in api_summaries.items()
    ) or "No financial data available."

    raw_data      = state.get("raw_api_data") or {}
    relevant_apis = state.get("relevant_apis") or []
    partial_note  = ""
    if len(raw_data) < len(relevant_apis):
        missing = len(relevant_apis) - len(raw_data)
        partial_note = (
            f"Data note: {missing} of {len(relevant_apis)} data sources "
            f"temporarily unavailable."
        )

    prompt = SYNTHESIS_PROMPT.format(
        history=history_text,
        user_query=state["user_query"],
        company_info=json.dumps(state.get("company_info") or {}, default=str),
        summaries=summaries_text,
        partial_note=partial_note,
    )
    try:
        state["final_answer"] = _llm_text(prompt)
    except Exception as e:
        console.print(f"Synthesis failed: {e}")
        state["final_answer"] = f"Synthesis failed: {e}\nSummary: {summaries_text[:400]}"
    return state


COMPARISON_PROMPT = """\
You are a senior Indian equity analyst.

## Conversation history
{history}

## User Question
{user_query}

## Company Summaries
{company_blocks}

{partial_note}

## Instructions
1. Write in flowing prose paragraphs only. Do NOT use markdown tables, bullet
   points, hyphens as list markers, asterisks, headers, or any special
   formatting characters. Do NOT use the rupee symbol — write "Rs" instead.
2. Structure your response as follows: one opening paragraph naming both
   companies and the comparison context, then one paragraph each covering
   valuation, profitability, growth, and safety/debt, then a closing verdict
   paragraph naming the stronger pick with reasoning.
3. Weave all numbers into sentences.
4. Highlight sector benchmarks where relevant.
5. Use Indian number formatting (Rs Cr — spell out "Rs").
6. Be honest if specific data is missing.
7. Keep under 400 words unless detail is explicitly requested.
8. End with a single sentence stating this is not financial advice.
"""

def node_comparison_synthesis(state: AgentState) -> AgentState:
    console.print("[Node CS] Comparison synthesis")
    companies    = state.get("companies") or []
    history_text = _format_history(state)

    if all(c.get("all_apis_failed") for c in companies if not c.get("error")):
        state["final_answer"] = _networks_busy_reply(
            state.get("user_query", ""),
            " and ".join(c.get("name", "Unknown") for c in companies),
        )
        return state

    blocks      = []
    partial_cos = []
    for c in companies:
        if c.get("all_apis_failed"):
            partial_cos.append(f"{c.get('name')} (no data)")
        elif c.get("has_partial_data"):
            partial_cos.append(f"{c.get('name')} (partial data)")
        blocks.append(
            f"### {c.get('name', 'Unknown')} (NSE: {c.get('nse_symbol', 'N/A')})\n"
            f"{c.get('summary', '(no data available)')}"
        )

    partial_note = (
        "Data note: some sources temporarily unavailable for "
        + ", ".join(partial_cos) + "."
    ) if partial_cos else ""

    prompt = COMPARISON_PROMPT.format(
        history=history_text,
        user_query=state["user_query"],
        company_blocks="\n\n".join(blocks) or "No company data available.",
        partial_note=partial_note,
    )
    try:
        state["final_answer"] = _llm_text(prompt)
    except Exception as e:
        state["final_answer"] = f"Comparison synthesis failed: {e}"
    return state


INVESTMENT_PROMPT = """\
You are a SEBI-registered research analyst providing FACTUAL analysis.

## Conversation history
{history}

## User Question
{user_query}

## Company Summaries
{company_blocks}

{partial_note}

## Analysis Instructions
1. Write in flowing prose paragraphs only. Do NOT use markdown tables, bullet
   points, hyphens as list markers, asterisks, headers, or any special
   formatting characters. Do NOT use the rupee symbol — write "Rs" instead.
2. Write one paragraph per company covering PE, PB, ROE, EPS, revenue trend,
   debt/equity, and MCAP — all woven naturally into sentences.
3. Follow with one paragraph ranking the companies from most to least
   attractive for investment, with clear qualitative reasoning.
4. State clearly in prose what data was unavailable for any company.
5. Use Indian number formatting (Rs Cr — spell out "Rs").
6. Keep under 500 words.
7. End with the sentence: This is not personal financial advice.
"""

def node_investment_advisor(state: AgentState) -> AgentState:
    console.print("[Node IA] Investment advisor")
    companies    = state.get("companies") or []
    history_text = _format_history(state)

    if all(c.get("all_apis_failed") for c in companies if not c.get("error")):
        state["final_answer"] = _networks_busy_reply(
            state.get("user_query", ""),
            " and ".join(c.get("name", "Unknown") for c in companies),
        )
        return state

    blocks      = []
    partial_cos = []
    for c in companies:
        if c.get("all_apis_failed"):
            partial_cos.append(f"{c.get('name')} (no data)")
        blocks.append(
            f"### {c.get('name', 'Unknown')} (NSE: {c.get('nse_symbol', 'N/A')})\n"
            f"{c.get('summary', '(no data available)')}"
        )

    partial_note = (
        "Data note: live data unavailable for " + ", ".join(partial_cos)
        + ". Analysis may be incomplete."
    ) if partial_cos else ""

    prompt = INVESTMENT_PROMPT.format(
        history=history_text,
        user_query=state["user_query"],
        company_blocks="\n\n".join(blocks) or "No company data available.",
        partial_note=partial_note,
    )
    try:
        state["final_answer"] = _llm_text(prompt)
    except Exception as e:
        state["final_answer"] = f"Investment analysis failed: {e}"
    return state


# ═══════════════════════════════════════════════════════════════════════════════
# Routers
# ═══════════════════════════════════════════════════════════════════════════════

def route_after_classify(state: AgentState) -> str:
    qt       = state.get("query_type", "general")
    is_broad = state.get("is_broad", False)
    mcp      = state.get("mcp_needed", False)

    if qt == "greeting":
        return "greeting"
    if qt == "general":
        return "general"
    if qt == "company":
        return "company"
    if qt in ("comparison", "investment"):
        if mcp:
            return "multi_mcp"
        if is_broad and qt == "comparison":
            return "multi_broad"
        return "multi_fast"
    return "company"


def route_after_symbol(state: AgentState) -> str:
    if state.get("mcp_needed"):
        return "mcp_pre_resolve"
    if state.get("is_broad"):
        return "full_ingest"
    return "direct_api_fetch"


def route_after_multi(state: AgentState) -> str:
    return "investment" if state.get("query_type") == "investment" else "comparison"


# ═══════════════════════════════════════════════════════════════════════════════
# Build graph
# ═══════════════════════════════════════════════════════════════════════════════

def build_graph() -> Any:
    g = StateGraph(AgentState)

    # ── Nodes ──────────────────────────────────────────────────────────────
    g.add_node("classify_and_extract",    node_classify_and_extract)
    g.add_node("greeting_handler",        node_greeting_handler)
    g.add_node("general_handler",         node_general_handler)
    g.add_node("symbol_resolution",       node_symbol_resolution)

    # MCP path nodes
    g.add_node("mcp_pre_resolve",         node_mcp_pre_resolve)
    g.add_node("mcp_multi_pre_resolve",   node_mcp_multi_pre_resolve)
    g.add_node("mcp_tool_call",           node_mcp_tool_call)
    g.add_node("mcp_synthesis",           node_mcp_synthesis)

    # Fast path
    g.add_node("direct_api_fetch",        node_direct_api_fetch)
    g.add_node("multi_direct_fetch",      node_multi_direct_fetch)

    # Deep path (broad only)
    g.add_node("full_ingest",             node_full_ingest)
    g.add_node("vector_retrieval",        node_vector_retrieval)
    g.add_node("multi_full_ingest",       node_multi_full_ingest)

    # Shared summarise + synthesis
    g.add_node("summarise_api_responses", node_summarise_api_responses)
    g.add_node("per_company_summarise",   node_per_company_summarise)
    g.add_node("synthesis",               node_synthesis)
    g.add_node("comparison_synthesis",    node_comparison_synthesis)
    g.add_node("investment_advisor",      node_investment_advisor)

    g.set_entry_point("classify_and_extract")

    # ── Top-level routing ──────────────────────────────────────────────────
    g.add_conditional_edges(
        "classify_and_extract",
        route_after_classify,
        {
            "greeting":    "greeting_handler",
            "general":     "general_handler",
            "company":     "symbol_resolution",
            "multi_fast":  "multi_direct_fetch",
            "multi_broad": "multi_full_ingest",
            "multi_mcp":   "mcp_multi_pre_resolve",
        },
    )

    g.add_edge("greeting_handler", END)
    g.add_edge("general_handler",  "synthesis")

    # ── Single-company: symbol_resolution → branch ─────────────────────────
    g.add_conditional_edges(
        "symbol_resolution",
        route_after_symbol,
        {
            "mcp_pre_resolve":  "mcp_pre_resolve",
            "direct_api_fetch": "direct_api_fetch",
            "full_ingest":      "full_ingest",
        },
    )

    # ── MCP single-company path ─────────────────────────────────────────────
    g.add_edge("mcp_pre_resolve",       "mcp_tool_call")
    g.add_edge("mcp_tool_call",         "mcp_synthesis")
    g.add_edge("mcp_synthesis",         END)

    # ── MCP multi-company path ──────────────────────────────────────────────
    g.add_edge("mcp_multi_pre_resolve", "mcp_tool_call")

    # ── Fast path: direct_api_fetch → summarise → synthesis → END ──────────
    g.add_edge("direct_api_fetch",        "summarise_api_responses")
    g.add_edge("summarise_api_responses", "synthesis")
    g.add_edge("synthesis",               END)

    # ── Deep path: full_ingest → vector_retrieval → summarise → synthesis ──
    g.add_edge("full_ingest",      "vector_retrieval")
    g.add_edge("vector_retrieval", "summarise_api_responses")

    # ── Multi fast: direct → per-company summarise → comparison/investment ─
    g.add_edge("multi_direct_fetch", "per_company_summarise")

    # ── Multi deep (broad comparison): full ingest → per-company summarise ─
    g.add_edge("multi_full_ingest", "per_company_summarise")

    g.add_conditional_edges(
        "per_company_summarise",
        route_after_multi,
        {
            "comparison": "comparison_synthesis",
            "investment": "investment_advisor",
        },
    )
    g.add_edge("comparison_synthesis", END)
    g.add_edge("investment_advisor",   END)

    return g.compile()


# ── Singleton ─────────────────────────────────────────────────────────────────

_graph = None

def get_graph():
    global _graph
    if _graph is None:
        _graph = build_graph()
    return _graph


# ── Public entrypoint ─────────────────────────────────────────────────────────

def run_query(
    user_query: str,
    conversation_history: Optional[list[Turn]] = None,
    force_refresh: bool = False,
) -> tuple[str, list[Turn]]:
    history = conversation_history or []
    result  = get_graph().invoke(
        {
            "user_query":           user_query,
            "conversation_history": history,
            "force_refresh":        force_refresh,
        }
    )
    answer = _strip_markdown(result.get("final_answer", "No answer generated."))
    return answer, history + [
        {"role": "user",      "content": user_query},
        {"role": "assistant", "content": answer},
    ]
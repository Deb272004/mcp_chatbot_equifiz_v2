

"""
graph.py – EQUIFIZ Financial AI Agent (v6.0)

Key changes from v5.3
───────────────────────
1. FULLY DYNAMIC TOOL REGISTRY — No more hardcoded _MCP_KEYWORDS,
   TOOL_REQUIRED_PARAMS, _MF_SCHCODE_TOOLS, _MF_COCODE_TOOLS, or
   _CO_CODE_TOOLS sets. All tool discovery, mcp_needed detection, and
   required-parameter resolution now query the equifiz_tools Chroma
   collection at runtime.

2. DYNAMIC mcp_needed DETECTION — _query_tool_registry() searches Chroma
   with the user query. If ANY tool scores above TOOL_SCORE_THRESHOLD,
   mcp_needed is set True automatically — no keyword lists needed.

3. DYNAMIC PROMPT INJECTION — Node A receives a live snapshot of the top-k
   matching tools (name + description) so the LLM picks mcp_tool_hint from
   real metadata, not a frozen prompt section.

4. DYNAMIC PARAMETER RESOLUTION — node_mcp_pre_resolve reads
   required_parameters from Chroma metadata for the hinted tool. The
   PARAM_TO_TABLE_MAP (DB routing) stays because it reflects your actual
   Postgres schema, not tool business logic.

5. compare_schemes MULTI-CODE SUPPORT — compare_schemes requires multiple
   mf_schcodes (comma-separated). The resolver now collects one code per
   entity and builds the composite value automatically.

Pipeline design (v7.0)
────────────────────────────────────
Node A  : classify + extract
           Queries Chroma for top matching tools → injects into LLM prompt.
           LLM sets mcp_needed + mcp_tool_hint from live tool metadata.
           Chroma score gate overrides LLM if score exceeds threshold.

           ├─ "greeting"    → greeting_handler → END
           ├─ "general"     → general_handler → synthesis → END
           │
           ├─ "company"     → mcp_pre_resolve → mcp_tool_call
           │                  → mcp_synthesis → END
           │
           ├─ "comparison"  → mcp_multi_pre_resolve → mcp_tool_call
           │                  → mcp_synthesis → END
           │
           └─ "investment"  → mcp_multi_pre_resolve → mcp_tool_call
                              → mcp_synthesis → END
"""
from __future__ import annotations

import asyncio
import concurrent.futures
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
from langgraph.graph import END, StateGraph

import db
import vector_store as vs
from config.config import OLLAMA_BASE_URL, OLLAMA_MODEL
from rich.console import Console

console = Console()
logger  = logging.getLogger(__name__)
TODAY   = str(date.today())


# ── LLM ───────────────────────────────────────────────────────────────────────

llm = ChatOllama(base_url=OLLAMA_BASE_URL, model=OLLAMA_MODEL)


# ── Conversation turn helper ──────────────────────────────────────────────────

class Turn(TypedDict):
    role: str       # "user" | "assistant"
    content: str


# ── DB routing: param name → Postgres table (schema-driven, not tool-driven) ─
# This is the ONLY hardcoded map and it belongs here: it reflects your DB
# schema, not any tool's business logic.

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
    "isin": {
        "table":    "etf_master",
        "column":   "etfname",
        "id_field": "isin",
    },
    "index_code" : {
        "table" : "group_master",
        "column": "group_name",
        "id_field" : "indexcode",
    },
 }

# ── DB config ─────────────────────────────────────────────────────────────────

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "equifiz",
    "user":     "postgres",
    "password": "1234",
}

# Cosine-distance threshold for Chroma tool matching.
# Change this line (currently 0.40)
TOOL_SCORE_THRESHOLD = 0.55


# ═══════════════════════════════════════════════════════════════════════════════
# Chroma Tool Registry  — single source of truth for tool discovery
# ═══════════════════════════════════════════════════════════════════════════════

def _query_tool_registry(
    query: str,
    n_results: int = 6,
    score_threshold: float = TOOL_SCORE_THRESHOLD,
) -> list[dict]:
    """
    Query the equifiz_tools Chroma collection with a natural-language query.

    Returns a list of matching tool dicts sorted best-first:
        {tool_name, description, required_parameters, parameters, score}

    required_parameters is a list[str] parsed from the comma-separated
    metadata string (empty list for no-param tools).

    Returns [] on any error so callers degrade gracefully.
    """
    try:
        import chromadb
        client     = chromadb.PersistentClient(path="./chroma_db")
        collection = client.get_collection(name="equifiz_tools")
        count      = collection.count()
        if count == 0:
            return []

        res = collection.query(
            query_texts=[query],
            n_results=min(n_results, count),
            include=["metadatas", "distances"],
        )

        if not (res["ids"] and res["ids"][0]):
            return []

        tools: list[dict] = []
        for tool_id, meta, dist in zip(
            res["ids"][0],
            res["metadatas"][0],
            res["distances"][0],
        ):
            if dist > score_threshold:
                continue

            raw_req = meta.get("required_parameters", "") or ""
            req_params = [p.strip() for p in raw_req.split(",") if p.strip()]

            tools.append({
                "tool_name":           meta.get("name", tool_id),
                "description":         meta.get("description", ""),
                "required_parameters": req_params,
                "parameters":          meta.get("parameters", ""),
                "score":               round(1 - dist, 3),
            })

        console.print(
            f"  [ToolRegistry] '{query[:60]}' → {len(tools)} tools matched "
            f"(threshold={score_threshold})"
        )
        return tools

    except Exception as e:
        console.print(f"  [ToolRegistry] Chroma query failed: {e}")
        return []


def _get_tool_meta(tool_name: str) -> Optional[dict]:
    """
    Fetch metadata for a single known tool by name from Chroma.
    Returns the same dict format as _query_tool_registry entries, or None.
    """
    try:
        import chromadb
        client     = chromadb.PersistentClient(path="./chroma_db")
        collection = client.get_collection(name="equifiz_tools")
        res        = collection.get(ids=[tool_name], include=["metadatas"])
        if res["ids"]:
            meta       = res["metadatas"][0]
            raw_req    = meta.get("required_parameters", "") or ""
            req_params = [p.strip() for p in raw_req.split(",") if p.strip()]
            return {
                "tool_name":           meta.get("name", tool_name),
                "description":         meta.get("description", ""),
                "required_parameters": req_params,
                "parameters":          meta.get("parameters", ""),
            }
    except Exception as e:
        console.print(f"  [ToolRegistry] get by id '{tool_name}' failed: {e}")
    return None


def _build_tool_candidates_block(tools: list[dict]) -> str:
    """
    Format matched tools into a readable block for the LLM prompt.
    """
    if not tools:
        return "(no tools matched for this query)"
    lines = []
    for t in tools:
        req = ", ".join(t["required_parameters"]) if t["required_parameters"] else "none"
        lines.append(
            f"- {t['tool_name']} (score={t['score']}): {t['description'][:120]}"
            f"\n    requires: {req}"
        )
    return "\n".join(lines)


# ── Agent State ───────────────────────────────────────────────────────────────

class AgentState(TypedDict, total=False):
    # Input
    user_query:           str
    conversation_history: list[Turn]
    force_refresh:        bool

    # Classification
    query_type:       str
    extracted_entity: str
    report_type:      str
    intent:           str
    is_broad:         bool
    mcp_needed:       bool
    mcp_tool_hint:    str

    # Matched tool candidates (set in Node A, consumed by pre-resolve)
    matched_tools: list[dict]

    # Symbol resolution (kept for general_handler / vector path only)
    nse_symbol:   Optional[str]
    co_code:      Optional[int]
    company_info: Optional[dict]

    # MCP-specific resolved codes (primary entity)
    mcp_resolved_codes: dict
    mcp_scheme_name:    Optional[str]
    mcp_amc_name:       Optional[str]

    # Multi-company entity list
    companies: list[dict]

    # ChromaDB path (general handler only)
    vector_context: list[dict]

    # MCP path output
    mcp_raw_result:      str
    mcp_tool_calls_made: list[str]

    # Output
    final_answer: str
    error:        Optional[str]


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


def _strip_markdown(text: str) -> str:
    lines = text.splitlines()
    lines = [l for l in lines if not re.match(r"^\s*\|", l)]
    text  = "\n".join(lines)
    text  = text.replace("₹", "Rs ")
    text  = re.sub(r"\*{1,3}(.+?)\*{1,3}", r"\1", text, flags=re.DOTALL)
    text  = re.sub(r"_{1,3}(.+?)_{1,3}", r"\1", text, flags=re.DOTALL)
    text  = re.sub(r"(?m)^[ \t]*(?:[-*#]+|[0-9]+\.)[ \t]+", "", text)
    text  = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


# ═══════════════════════════════════════════════════════════════════════════════
# Node A: Classify + Extract  (fully dynamic)
# ═══════════════════════════════════════════════════════════════════════════════

_BROAD_KEYWORDS = {
    "full analysis", "complete analysis", "full report", "complete report",
    "deep dive", "detailed analysis", "everything about", "all financials",
    "comprehensive", "full picture",
}

CLASSIFY_AND_EXTRACT_PROMPT = """\
You are a financial query analyzer for an Indian stock-market chatbot.

## Conversation history (last 5 turns)
{history}

## Current user query
{query}

## Available MCP tools retrieved for this query (ranked by relevance)
These are REAL tools from the live registry. Pick mcp_tool_hint from this list only.
{tool_candidates}

Return ONLY a valid JSON object with NO extra text, preamble, or markdown:
{{
  "query_type": "<greeting|general|company|comparison|investment>",
  "is_broad": <true|false>,
  "mcp_needed": <true|false>,
  "mcp_tool_hint": "<exact tool_name from the list above, or empty string>",
  "primary": {{
    "company_name": "<canonical name or null>",
    "nse_symbol":   "<NSE ticker or null>",
    "scheme_name":  "<MF scheme full name or null>",
    "amc_name":     "<AMC / fund house full name or null>",
    "report_type":  "<s or c>"
  }},
  "additional_companies": [
    {{
      "company_name": "<canonical name or null>",
      "nse_symbol":   "<NSE ticker or null>",
      "scheme_name":  "<MF scheme full name or null>",
      "amc_name":     "<AMC / fund house full name or null>"
    }}
  ]
}}

Classification rules:
- "greeting"    : Hello, hi, thanks, bye, what can you do, small talk
- "general"     : Generic financial concept, no specific company/fund
- "company"     : Question about ONE specific company OR mutual fund scheme/AMC
- "comparison"  : Explicit/implicit comparison of TWO OR MORE companies/schemes
- "investment"  : Buy/invest recommendation query

mcp_needed:
  Set TRUE if ANY of the top tool candidates above match the query well
  (the score is already filtered — if tools appear in the list, they matched).
  Also set TRUE for: live/real-time prices, NAV, any MF data, IPO data,
  market indices, gainers/losers, advance-decline, 52-week highs/lows,
  AND for any fundamental data query (PE, EPS, balance sheet, P&L,
  key ratios, quarterly results) — all data now comes via MCP.
  Set FALSE only for greetings and pure general concept questions.

mcp_tool_hint:
  Pick the SINGLE best tool_name from the list above.
  If multiple tools match, pick the most specific one for the query.
  Leave empty only if mcp_needed is false.

is_broad:
  Set true ONLY for: "full analysis", "complete report", "deep dive",
  "everything about X", "all financials", "comprehensive". Default FALSE.

Entity extraction:
  - Resolve brand names: "Maggi" → "Nestle India", "Jio" → "Reliance Industries"
  - Use conversation history to resolve pronouns
  - For MF queries: populate BOTH scheme_name AND amc_name wherever possible
  - For comparison/investment: primary = first entity,
    additional_companies = every subsequent entity (one object each)
    For MF entities ALWAYS populate scheme_name AND amc_name.
  - If query is about an AMC's schemes, set amc_name and leave company_name null
"""


def node_classify_and_extract(state: AgentState) -> AgentState:
    console.print("[Node A] Classify + extract (dynamic tool registry)")

    history      = state.get("conversation_history") or []
    history_text = "\n".join(
        f"{t['role'].upper()}: {t['content']}" for t in history[-10:]
    ) or "(none)"
    user_query  = state.get("user_query", "")
    query_lower = user_query.lower()
    keyword_broad = any(kw in query_lower for kw in _BROAD_KEYWORDS)

    # ── Step 1: Query Chroma for matching tools ────────────────────────────
    matched_tools = _query_tool_registry(user_query, n_results=6)
    state["matched_tools"] = matched_tools

    chroma_mcp = len(matched_tools) > 0
    if chroma_mcp:
        console.print(
            f"  [Chroma gate] {len(matched_tools)} tools matched → mcp_needed=True"
        )

    for i in matched_tools:
        console.print(i)

    tool_candidates_block = _build_tool_candidates_block(matched_tools)

    # ── Step 2: LLM classification ─────────────────────────────────────────
    try:
        result = _llm_json(
            CLASSIFY_AND_EXTRACT_PROMPT.format(
                history         = history_text,
                query           = user_query,
                tool_candidates = tool_candidates_block,
            )
        )

        state["query_type"]    = result.get("query_type", "general")
        state["is_broad"]      = result.get("is_broad", False) or keyword_broad
        state["mcp_needed"]    = result.get("mcp_needed", False) or chroma_mcp
        state["mcp_tool_hint"] = result.get("mcp_tool_hint", "")

        # If LLM didn't pick a hint but Chroma matched, use the top Chroma result
        if state["mcp_needed"] and not state["mcp_tool_hint"] and matched_tools:
            state["mcp_tool_hint"] = matched_tools[0]["tool_name"]
            console.print(
                f"  [Chroma fallback hint] Using top match: {state['mcp_tool_hint']}"
            )

        primary = result.get("primary") or {}

        if state["mcp_needed"]:
            mcp_scheme = primary.get("scheme_name")
            mcp_amc    = primary.get("amc_name")
            state["extracted_entity"] = (
                mcp_scheme or mcp_amc
                or primary.get("company_name") or user_query
            )
        else:
            state["extracted_entity"] = primary.get("company_name") or user_query

        state["nse_symbol"]      = primary.get("nse_symbol")
        state["intent"]          = primary.get("intent", "general")
        state["report_type"]     = primary.get("report_type", "s")
        state["mcp_scheme_name"] = primary.get("scheme_name")
        state["mcp_amc_name"]    = primary.get("amc_name")

        # Build companies list
        additional = result.get("additional_companies") or []
        if additional:
            state["companies"] = [
                {
                    "name":        c.get("scheme_name") or c.get("company_name") or "",
                    "nse_symbol":  c.get("nse_symbol"),
                    "scheme_name": c.get("scheme_name"),
                    "amc_name":    c.get("amc_name"),
                }
                for c in additional
            ]

        console.print(
            f"  → type={state['query_type']} broad={state['is_broad']} "
            f"mcp={state['mcp_needed']} hint={state['mcp_tool_hint']} "
            f"entity={state['extracted_entity']} "
            f"additional={len(state.get('companies', []))}"
        )

    except Exception as e:
        console.print(f"  Classify+extract failed: {e} — defaulting to general")
        state["query_type"]       = "general"
        state["is_broad"]         = keyword_broad
        state["mcp_needed"]       = chroma_mcp
        state["mcp_tool_hint"]    = matched_tools[0]["tool_name"] if matched_tools else ""
        state["extracted_entity"] = user_query
        state["intent"]           = "general"
        state["report_type"]      = "s"

    return state


# ── Node B: Greeting handler ──────────────────────────────────────────────────

GREETING_PROMPT = """\
You are EQUIFIZ — a friendly Indian stock-market AI assistant.
Respond warmly and briefly to the user's greeting or small talk.
Mention 1-2 things you can help with (PE ratios, quarterly results,
investment recommendations, live NAV, MF returns, IPO listings, etc.).

User: {query}
"""

def node_greeting_handler(state: AgentState) -> AgentState:
    console.print("[Node B] Greeting handler")
    state["final_answer"] = _llm_text(
        GREETING_PROMPT.format(query=state["user_query"])
    )
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


# ── Symbol resolution (kept for general handler / vector path) ────────────────

def _resolve_single(name: str, nse_symbol: Optional[str]) -> dict:
    if nse_symbol:
        row = db.lookup_by_nse_symbol(nse_symbol)
        if row:
            return {
                "co_code":      row["co_code"],
                "company_info": row,
                "nse_symbol":   nse_symbol,
            }
    matches = db.fuzzy_search_company(name, limit=1)
    if matches:
        best = matches[0]
        return {
            "co_code":      best["co_code"],
            "company_info": best,
            "nse_symbol":   best.get("nsesymbol"),
        }
    return {}


# ═══════════════════════════════════════════════════════════════════════════════
# DB Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _targeted_db_lookup(
    entity:   str,
    table:    str,
    name_col: str,
    id_col:   str,
) -> Optional[dict]:
    """
    Generic fuzzy lookup for any table/column in PARAM_TO_TABLE_MAP.
    For scheme_master returns full row (including mf_cocode) in one query.
    Returns best-matching row or None if best score < 60.
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        console.print(f"  🔍 DB lookup: table='{table}' entity='{entity}'")

        if table == "scheme_master":
            cur.execute(
                "SELECT mf_schcode, mf_cocode, sch_name, category FROM scheme_master"
            )
        else:
            cur.execute(f"SELECT {id_col}, {name_col} FROM {table}")

        rows = [dict(r) for r in cur.fetchall()]
        cur.close()
        conn.close()

        best_match: Optional[dict] = None
        best_score: int            = 0
        el = entity.lower()

        for row in rows:
            candidate = (row.get(name_col) or "").lower()
            score = max(
                fuzz.token_set_ratio(el, candidate),
                fuzz.partial_ratio(el, candidate),
            )
            if score > best_score:
                best_score, best_match = score, row

        if best_match and best_score >= 60:
            console.print(
                f"  ✅ match='{best_match.get(name_col)}' "
                f"id={best_match.get(id_col)} score={best_score}"
            )
            return best_match

        console.print(f"  No confident match in {table} (best={best_score})")
        return None

    except Exception as e:
        console.print(f"  [_targeted_db_lookup] DB error on {table}: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# Core per-entity resolver  (dynamic — reads required_params from Chroma)
# ═══════════════════════════════════════════════════════════════════════════════

def _get_required_params_for_tool(tool_hint: str, matched_tools: list[dict]) -> list[str]:
    """
    Determine which DB parameters a tool needs.

    Resolution order:
      1. matched_tools list from Node A (already in memory, free)
      2. Direct Chroma get by tool name
      3. Empty list (treat as no-param tool)

    Returns a list of param names that appear in PARAM_TO_TABLE_MAP.
    """
    if not tool_hint:
        return []

    for t in matched_tools:
        if t["tool_name"] == tool_hint:
            db_params = [
                p for p in t["required_parameters"]
                if p in PARAM_TO_TABLE_MAP
            ]
            console.print(
                f"  📋 Tool '{tool_hint}' DB params (from cache): {db_params}"
            )
            return db_params

    meta = _get_tool_meta(tool_hint)
    if meta:
        db_params = [
            p for p in meta["required_parameters"]
            if p in PARAM_TO_TABLE_MAP
        ]
        console.print(
            f"  📋 Tool '{tool_hint}' DB params (from Chroma): {db_params}"
        )
        return db_params

    console.print(f"  Tool '{tool_hint}' not found in registry — treating as no-param")
    return []


# def _resolve_entity_codes(
#     name:            str,
#     scheme_name:     Optional[str],
#     amc_name:        Optional[str],
#     nse_symbol:      Optional[str],
#     required_params: list[str],
# ) -> dict:
#     """
#     Resolve all DB codes needed by the target tool for a single entity.
#     """
#     codes:  dict = {}
#     result: dict = {"name": name}

#     needs_schcode  = "mf_schcode"  in required_params
#     needs_cocode   = "mf_cocode"   in required_params
#     needs_co_code  = "co_code"     in required_params
#     needs_schcodes = "mf_schcodes" in required_params

#     # ── mf_schcode / mf_schcodes ──────────────────────────────────────────
#     if needs_schcode or needs_schcodes:
#         search = scheme_name or name
#         cfg    = PARAM_TO_TABLE_MAP["mf_schcode"]
#         match  = _targeted_db_lookup(
#             entity=search, table=cfg["table"],
#             name_col=cfg["column"], id_col=cfg["id_field"],
#         )
#         if match:
#             param_key = "mf_schcodes" if needs_schcodes else "mf_schcode"
#             codes[param_key] = int(match["mf_schcode"])
#             if "mf_cocode" in match:
#                 codes.setdefault("mf_cocode", int(match["mf_cocode"]))
#             result["resolved_scheme_name"] = match.get("sch_name", search)
#             console.print(
#                 f"  💎 [{name}] mf_schcode={codes[param_key]} "
#                 f"mf_cocode={codes.get('mf_cocode')}"
#             )

#     # ── mf_cocode (only if not already populated from scheme_master) ──────
#     if needs_cocode and not codes.get("mf_cocode"):
#         search = amc_name or name
#         cfg    = PARAM_TO_TABLE_MAP["mf_cocode"]
#         match  = _targeted_db_lookup(
#             entity=search, table=cfg["table"],
#             name_col=cfg["column"], id_col=cfg["id_field"],
#         )
#         if match:
#             codes["mf_cocode"] = int(match["mf_cocode"])
#             console.print(f"  💎 [{name}] mf_cocode={codes['mf_cocode']}")

#     # ── co_code ───────────────────────────────────────────────────────────
#     if needs_co_code:
#         stock = _resolve_single(name, nse_symbol)
#         if stock:
#             codes["co_code"] = stock["co_code"]
#             result.update({
#                 "co_code":      stock["co_code"],
#                 "company_info": stock["company_info"],
#                 "nse_symbol":   stock["nse_symbol"],
#             })
#             console.print(f"  💎 [{name}] co_code={codes['co_code']}")

#     result["mcp_resolved_codes"] = codes
#     return result

def _resolve_entity_codes(
    name:            str,
    scheme_name:     Optional[str],
    amc_name:        Optional[str],
    nse_symbol:      Optional[str],
    required_params: list[str],
) -> dict:
    """
    Resolve all DB codes needed by the target tool for a single entity.
    """
    codes:  dict = {}
    result: dict = {"name": name}

    needs_schcode   = "mf_schcode"  in required_params
    needs_cocode    = "mf_cocode"   in required_params
    needs_co_code   = "co_code"     in required_params
    needs_schcodes  = "mf_schcodes" in required_params
    needs_isin      = "isin"        in required_params   # ← NEW
    needs_indexcode = "index_code"  in required_params   # ← NEW

    # ── mf_schcode / mf_schcodes ──────────────────────────────────────────
    if needs_schcode or needs_schcodes:
        search = scheme_name or name
        cfg    = PARAM_TO_TABLE_MAP["mf_schcode"]
        match  = _targeted_db_lookup(
            entity=search, table=cfg["table"],
            name_col=cfg["column"], id_col=cfg["id_field"],
        )
        if match:
            param_key = "mf_schcodes" if needs_schcodes else "mf_schcode"
            codes[param_key] = int(match["mf_schcode"])
            if "mf_cocode" in match:
                codes.setdefault("mf_cocode", int(match["mf_cocode"]))
            result["resolved_scheme_name"] = match.get("sch_name", search)
            console.print(
                f"  💎 [{name}] mf_schcode={codes[param_key]} "
                f"mf_cocode={codes.get('mf_cocode')}"
            )

    # ── mf_cocode (only if not already populated from scheme_master) ──────
    if needs_cocode and not codes.get("mf_cocode"):
        search = amc_name or name
        cfg    = PARAM_TO_TABLE_MAP["mf_cocode"]
        match  = _targeted_db_lookup(
            entity=search, table=cfg["table"],
            name_col=cfg["column"], id_col=cfg["id_field"],
        )
        if match:
            codes["mf_cocode"] = int(match["mf_cocode"])
            console.print(f"  💎 [{name}] mf_cocode={codes['mf_cocode']}")

    # ── co_code ───────────────────────────────────────────────────────────
    if needs_co_code:
        stock = _resolve_single(name, nse_symbol)
        if stock:
            codes["co_code"] = stock["co_code"]
            result.update({
                "co_code":      stock["co_code"],
                "company_info": stock["company_info"],
                "nse_symbol":   stock["nse_symbol"],
            })
            console.print(f"  💎 [{name}] co_code={codes['co_code']}")

    # ── isin ──────────────────────────────────────────────────────────────
    if needs_isin:                                                  # ← NEW
        search = scheme_name or name
        cfg    = PARAM_TO_TABLE_MAP["isin"]
        match  = _targeted_db_lookup(
            entity=search, table=cfg["table"],
            name_col=cfg["column"], id_col=cfg["id_field"],
        )
        if match:
            codes["isin"] = match["isin"]          # string — no int cast
            result["resolved_etf_name"] = match.get("etf_name", search)
            console.print(f"  💎 [{name}] isin={codes['isin']}")

    # ── index_code ────────────────────────────────────────────────────────
    if needs_indexcode:                                             # ← NEW
        cfg   = PARAM_TO_TABLE_MAP["index_code"]
        match = _targeted_db_lookup(
            entity=name, table=cfg["table"],
            name_col=cfg["column"], id_col=cfg["id_field"],
        )
        if match:
            codes["index_code"] = int(match["indexcode"])   # note: id_field is "indexcode"
            result["resolved_index_name"] = match.get("group_name", name)
            console.print(f"  💎 [{name}] index_code={codes['index_code']}")

    result["mcp_resolved_codes"] = codes
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# MCP Pre-Resolution — single entity
# ═══════════════════════════════════════════════════════════════════════════════

def node_mcp_pre_resolve(state: AgentState) -> AgentState:
    """
    Resolve DB codes for the primary entity using the dynamic tool registry.
    """
    console.print("[Node MPR] Single-entity MCP pre-resolve (v7.0)")

    entity        = (
        state.get("mcp_scheme_name")
        or state.get("mcp_amc_name")
        or state.get("extracted_entity", "")
    )
    tool_hint     = state.get("mcp_tool_hint", "").strip()
    matched_tools = state.get("matched_tools") or []

    required_params = _get_required_params_for_tool(tool_hint, matched_tools)

    if not required_params:
        console.print(
            f"  ⚡ Tool '{tool_hint}' needs no DB codes — MCP server resolves from query"
        )
        state["mcp_resolved_codes"] = {}
        return state

    resolved = _resolve_entity_codes(
        name            = entity,
        scheme_name     = state.get("mcp_scheme_name"),
        amc_name        = state.get("mcp_amc_name"),
        nse_symbol      = state.get("nse_symbol"),
        required_params = required_params,
    )

    state["mcp_resolved_codes"] = resolved.get("mcp_resolved_codes", {})
    if resolved.get("resolved_scheme_name"):
        state["mcp_scheme_name"] = resolved["resolved_scheme_name"]

    console.print(f"  🏁 Resolved codes: {state['mcp_resolved_codes']}")
    return state


# ═══════════════════════════════════════════════════════════════════════════════
# MCP Pre-Resolution — multi-entity
# ═══════════════════════════════════════════════════════════════════════════════

def node_mcp_multi_pre_resolve(state: AgentState) -> AgentState:
    """
    Resolve DB codes for ALL entities in a multi-entity MCP query.
    """
    console.print("[Node MMPR] Multi-entity MCP pre-resolve (v7.0)")

    tool_hint       = state.get("mcp_tool_hint", "")
    matched_tools   = state.get("matched_tools") or []
    required_params = _get_required_params_for_tool(tool_hint, matched_tools)

    entities: list[dict] = []
    primary_name = (
        state.get("mcp_scheme_name")
        or state.get("mcp_amc_name")
        or state.get("extracted_entity", "")
    )
    if primary_name:
        entities.append({
            "name":        primary_name,
            "scheme_name": state.get("mcp_scheme_name"),
            "amc_name":    state.get("mcp_amc_name"),
            "nse_symbol":  state.get("nse_symbol"),
        })
    for c in (state.get("companies") or []):
        name = c.get("scheme_name") or c.get("name") or c.get("company_name", "")
        if name:
            entities.append({
                "name":        name,
                "scheme_name": c.get("scheme_name"),
                "amc_name":    c.get("amc_name"),
                "nse_symbol":  c.get("nse_symbol"),
            })

    console.print(
        f"  Entities ({len(entities)}): {[e['name'] for e in entities]}"
    )

    def _resolve_one(entry: dict) -> dict:
        return _resolve_entity_codes(
            name            = entry["name"],
            scheme_name     = entry.get("scheme_name"),
            amc_name        = entry.get("amc_name"),
            nse_symbol      = entry.get("nse_symbol"),
            required_params = required_params,
        )

    name_to_result: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=min(6, max(len(entities), 1))) as executor:
        futures = {executor.submit(_resolve_one, e): e["name"] for e in entities}
        for future in as_completed(futures):
            name = futures[future]
            try:
                name_to_result[name] = future.result()
            except Exception as exc:
                console.print(f"  Resolution failed for '{name}': {exc}")
                name_to_result[name] = {"name": name, "mcp_resolved_codes": {}}

    resolved_entities = [
        name_to_result.get(e["name"], {"name": e["name"], "mcp_resolved_codes": {}})
        for e in entities
    ]

    # ── compare_schemes: merge individual mf_schcode → mf_schcodes ────────
    is_compare = "mf_schcodes" in required_params or tool_hint == "compare_schemes"
    if is_compare:
        scheme_codes = []
        for r in resolved_entities:
            codes = r.get("mcp_resolved_codes", {})
            code  = codes.get("mf_schcodes") or codes.get("mf_schcode")
            if code:
                scheme_codes.append(str(code))
        if scheme_codes:
            merged_codes = {"mf_schcodes": ",".join(scheme_codes)}
            state["mcp_resolved_codes"] = merged_codes
            console.print(f"  🔗 compare_schemes mf_schcodes={merged_codes['mf_schcodes']}")
    else:
        if resolved_entities:
            primary = resolved_entities[0]
            state["mcp_resolved_codes"] = primary.get("mcp_resolved_codes", {})
            if primary.get("co_code"):
                state["co_code"] = primary["co_code"]

    state["companies"] = resolved_entities
    console.print(
        "  Final resolved:\n"
        + "\n".join(
            f"    [{r['name']}] → {r.get('mcp_resolved_codes', {})}"
            for r in resolved_entities
        )
    )
    return state


# ═══════════════════════════════════════════════════════════════════════════════
# Injection block builder
# ═══════════════════════════════════════════════════════════════════════════════

# def _build_injection_block(state: AgentState) -> str:
#     """
#     Build the <PRE_RESOLVED> XML block prepended to the enriched MCP query.
#     """
#     companies = [
#         c for c in (state.get("companies") or [])
#         if c.get("mcp_resolved_codes")
#     ]

#     if len(companies) >= 2:
#         primary_codes = state.get("mcp_resolved_codes") or {}
#         if "mf_schcodes" in primary_codes:
#             lines = [f"mf_schcodes={primary_codes['mf_schcodes']}"]
#             return "<PRE_RESOLVED>\n" + "\n".join(lines) + "\n</PRE_RESOLVED>"

#         lines: list[str] = []
#         for i, c in enumerate(companies, start=1):
#             codes = c["mcp_resolved_codes"]
#             label = (
#                 c.get("resolved_scheme_name")
#                 or c.get("scheme_name")
#                 or c.get("name", f"Entity {i}")
#             )
#             lines.append(f"Entity {i}: {label}")
#             for k, v in codes.items():
#                 lines.append(f"  {k}={v}")
#         if lines:
#             return "<PRE_RESOLVED>\n" + "\n".join(lines) + "\n</PRE_RESOLVED>"
#         return ""

#     resolved_codes = state.get("mcp_resolved_codes") or {}
#     scheme_name    = state.get("mcp_scheme_name", "")
#     amc_name       = state.get("mcp_amc_name", "")

#     code_lines: list[str] = []
#     for k, v in resolved_codes.items():
#         label = ""
#         if k in ("mf_schcode", "mf_schcodes") and scheme_name:
#             label = f" (scheme: {scheme_name})"
#         elif k == "mf_cocode" and amc_name:
#             label = f" (AMC: {amc_name})"
#         code_lines.append(f"{k}={v}{label}")

#     if code_lines:
#         return "<PRE_RESOLVED>\n" + "\n".join(code_lines) + "\n</PRE_RESOLVED>"
#     return ""

# def _build_injection_block(state: AgentState) -> str:
#     """
#     Build the <PRE_RESOLVED> XML block prepended to the enriched MCP query.
#     Always includes human-readable names so the MCP server can resolve
#     even when numeric codes are missing.
#     """
#     companies = [
#         c for c in (state.get("companies") or [])
#         if c.get("mcp_resolved_codes") or c.get("name") or c.get("scheme_name")
#     ]

#     # ── Multi-entity path ────────────────────────────────────────────────────
#     if len(companies) >= 2:
#         primary_codes = state.get("mcp_resolved_codes") or {}
#         if "mf_schcodes" in primary_codes:
#             lines = [f"mf_schcodes={primary_codes['mf_schcodes']}"]
#             return "<PRE_RESOLVED>\n" + "\n".join(lines) + "\n</PRE_RESOLVED>"

#         lines: list[str] = []
#         for i, c in enumerate(companies, start=1):
#             codes = c.get("mcp_resolved_codes") or {}
#             label = (
#                 c.get("resolved_scheme_name")
#                 or c.get("scheme_name")
#                 or c.get("name", f"Entity {i}")
#             )
#             amc   = c.get("amc_name") or c.get("mf_coname") or c.get("company_name") or ""

#             lines.append(f"Entity {i}: {label}")
#             if amc:
#                 lines.append(f"  amc_name={amc}")             # ← NEW: always emit name
#             for k, v in codes.items():
#                 lines.append(f"  {k}={v}")

#         if lines:
#             return "<PRE_RESOLVED>\n" + "\n".join(lines) + "\n</PRE_RESOLVED>"
#         return ""

#     # ── Single-entity path ───────────────────────────────────────────────────
#     resolved_codes = state.get("mcp_resolved_codes") or {}
#     scheme_name    = state.get("mcp_scheme_name", "") or state.get("mcp_entity", "")
#     amc_name       = state.get("mcp_amc_name", "")

#     lines: list[str] = []

#     # Always emit names first — MCP server can use these even without codes
#     if scheme_name:
#         lines.append(f"scheme_name={scheme_name}")             # ← NEW
#     if amc_name:
#         lines.append(f"amc_name={amc_name}")                   # ← NEW

#     # Then emit codes (with inline labels for readability)
#     for k, v in resolved_codes.items():
#         label = ""
#         if k in ("mf_schcode", "mf_schcodes") and scheme_name:
#             label = f" (scheme: {scheme_name})"
#         elif k == "mf_cocode" and amc_name:
#             label = f" (AMC: {amc_name})"
#         lines.append(f"{k}={v}{label}")

#     if lines:
#         return "<PRE_RESOLVED>\n" + "\n".join(lines) + "\n</PRE_RESOLVED>"
#     return ""

def _build_injection_block(state: AgentState) -> str:
    """
    Build the <PRE_RESOLVED> XML block prepended to the enriched MCP query.
    Codes are emitted first (MCP server prefers direct lookup).
    Names follow as fallback for when codes are absent or resolution fails.
    """
    companies = [
        c for c in (state.get("companies") or [])
        if c.get("mcp_resolved_codes") or c.get("name") or c.get("scheme_name")
    ]

    # ── Multi-entity path ────────────────────────────────────────────────────
    if len(companies) >= 2:
        primary_codes = state.get("mcp_resolved_codes") or {}
        if "mf_schcodes" in primary_codes:
            lines = [f"mf_schcodes={primary_codes['mf_schcodes']}"]
            return "<PRE_RESOLVED>\n" + "\n".join(lines) + "\n</PRE_RESOLVED>"

        lines: list[str] = []
        for i, c in enumerate(companies, start=1):
            codes = c.get("mcp_resolved_codes") or {}
            label = (
                c.get("resolved_scheme_name")
                or c.get("scheme_name")
                or c.get("name", f"Entity {i}")
            )
            amc = c.get("amc_name") or c.get("mf_coname") or c.get("company_name") or ""

            lines.append(f"Entity {i}: {label}")
            # Codes first — direct lookup, no resolution needed
            for k, v in codes.items():
                lines.append(f"  {k}={v}")
            # Names as fallback
            if amc:
                lines.append(f"  amc_name={amc}")

        if lines:
            return "<PRE_RESOLVED>\n" + "\n".join(lines) + "\n</PRE_RESOLVED>"
        return ""

    # ── Single-entity path ───────────────────────────────────────────────────
    resolved_codes = state.get("mcp_resolved_codes") or {}
    scheme_name    = state.get("mcp_scheme_name", "") or state.get("mcp_entity", "")
    amc_name       = state.get("mcp_amc_name", "")

    lines: list[str] = []

    # Codes first — MCP server uses these directly, no resolve_mf_scheme call needed
    for k, v in resolved_codes.items():
        label = ""
        if k in ("mf_schcode", "mf_schcodes") and scheme_name:
            label = f" (scheme: {scheme_name})"
        elif k == "mf_cocode" and amc_name:
            label = f" (AMC: {amc_name})"
        lines.append(f"{k}={v}{label}")

    # Names as fallback — used only when codes are missing or resolution fails
    if scheme_name:
        lines.append(f"scheme_name={scheme_name}")
    if amc_name:
        lines.append(f"amc_name={amc_name}")

    if lines:
        return "<PRE_RESOLVED>\n" + "\n".join(lines) + "\n</PRE_RESOLVED>"
    return ""

# ═════════════════════
# ══════════════════════════════════════════════════════════
# MCP Tool Call Node
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


def node_mcp_tool_call(state: AgentState) -> AgentState:
    console.print("[Node MTC] MCP tool call (v7.0)")

    try:
        from mcp_client import run_mcp_query  # noqa: F401
    except ImportError:
        console.print("MCP client module not found.")
        state["mcp_raw_result"] = ""
        state["error"] = "mcp_client_not_found"
        return state

    user_query      = state.get("user_query", "")
    tool_hint       = state.get("mcp_tool_hint", "")
    injection_block = _build_injection_block(state)

    if injection_block:
        enriched_query = f"{injection_block}\nUser Query: {user_query}"
    else:
        enriched_query = user_query

    if tool_hint:
        enriched_query += f"\n\n[RECOMMENDED TOOL: {tool_hint}]"

    console.print(f"  📤 Sending ({len(enriched_query)} chars):\n{enriched_query[:500]}")

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_run_mcp_in_new_loop, enriched_query)
            result = future.result(timeout=600)

        state["mcp_raw_result"]      = result or "No data returned from server."
        state["mcp_tool_calls_made"] = []
        state["error"]               = None
        console.print(f"  📥 Received {len(state['mcp_raw_result'])} chars")

    except concurrent.futures.TimeoutError:
        console.print("  🚨 MCP call timed out")
        state["mcp_raw_result"] = ""
        state["error"]          = "mcp_call_timeout"
    except Exception as e:
        console.print(f"  💥 MCP call failed: {e}")
        state["mcp_raw_result"] = ""
        state["error"]          = f"mcp_call_failed: {e}"

    return state


# ── MCP Synthesis ─────────────────────────────────────────────────────────────

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
2. Write in plain prose paragraphs only. No markdown, no bullet points,
   no asterisks, no tables. Do NOT use the rupee symbol — write "Rs" instead.
3. Present key numbers naturally woven into sentences.
4. Be honest if specific data fields are missing or unavailable.
5. Use Indian number formatting (Rs Cr for large numbers).
6. Keep under 250 words unless detail is explicitly requested.
7. End with: This is not financial advice.
"""

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
    error      = state.get("error", "")
    mcp_result = state.get("mcp_raw_result", "")

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
            "No data was returned from the live server. Please try again shortly."
        )
        return state

    prompt = MCP_SYNTHESIS_PROMPT.format(
        history    = _format_history(state),
        user_query = state.get("user_query", ""),
        mcp_result = mcp_result[:3000],
    )
    try:
        state["final_answer"] = _llm_text(prompt)
    except Exception as e:
        console.print(f"  MCP synthesis failed: {e}")
        state["final_answer"] = mcp_result
    return state


# ── General synthesis (for general/concept queries) ───────────────────────────

GENERAL_SYNTHESIS_PROMPT = """\
You are a knowledgeable Indian equity analyst assistant.

## Conversation history (last 4 turns)
{history}

## User Question
{user_query}

## Knowledge Base Context
{vector_context}

## Instructions
1. Answer clearly and concisely.
2. Plain prose only — no markdown. Do NOT use the rupee symbol — write "Rs".
3. Simple language; Indian market examples where useful.
4. Answer from general knowledge if context doesn't cover the topic.
5. Under 200 words.
"""

def node_synthesis(state: AgentState) -> AgentState:
    console.print("[Node S] General synthesis")
    vector_context = state.get("vector_context") or []
    context_text   = "\n".join(
        f"- [{r['metadata'].get('api_name', '')}] {r['document']}"
        for r in vector_context
    ) or "No relevant context found."
    prompt = GENERAL_SYNTHESIS_PROMPT.format(
        history        = _format_history(state),
        user_query     = state["user_query"],
        vector_context = context_text,
    )
    try:
        state["final_answer"] = _llm_text(prompt)
    except Exception as e:
        state["final_answer"] = f"Synthesis failed: {e}"
    return state


# ═══════════════════════════════════════════════════════════════════════════════
# Routers
# ═══════════════════════════════════════════════════════════════════════════════

def route_after_classify(state: AgentState) -> str:
    qt  = state.get("query_type", "general")
    mcp = state.get("mcp_needed", False)

    if qt == "greeting":
        return "greeting"

    if qt == "general" and not mcp:
        return "general"

    # Everything else — company, comparison, investment, or general with a
    # matched tool — goes through MCP. Multi-entity types get the multi resolver.
    if qt in ("comparison", "investment"):
        return "multi_mcp"

    return "mcp_direct"


# ═══════════════════════════════════════════════════════════════════════════════
# Build graph
# ═══════════════════════════════════════════════════════════════════════════════

def build_graph() -> Any:
    g = StateGraph(AgentState)

    g.add_node("classify_and_extract",  node_classify_and_extract)
    g.add_node("greeting_handler",      node_greeting_handler)
    g.add_node("general_handler",       node_general_handler)
    g.add_node("mcp_pre_resolve",       node_mcp_pre_resolve)
    g.add_node("mcp_multi_pre_resolve", node_mcp_multi_pre_resolve)
    g.add_node("mcp_tool_call",         node_mcp_tool_call)
    g.add_node("mcp_synthesis",         node_mcp_synthesis)
    g.add_node("synthesis",             node_synthesis)

    g.set_entry_point("classify_and_extract")

    g.add_conditional_edges(
        "classify_and_extract",
        route_after_classify,
        {
            "greeting":   "greeting_handler",
            "general":    "general_handler",
            "mcp_direct": "mcp_pre_resolve",
            "multi_mcp":  "mcp_multi_pre_resolve",
        },
    )

    g.add_edge("greeting_handler",      END)
    g.add_edge("general_handler",       "synthesis")
    g.add_edge("synthesis",             END)

    g.add_edge("mcp_pre_resolve",       "mcp_tool_call")
    g.add_edge("mcp_multi_pre_resolve", "mcp_tool_call")
    g.add_edge("mcp_tool_call",         "mcp_synthesis")
    g.add_edge("mcp_synthesis",         END)

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
    user_query:           str,
    conversation_history: Optional[list[Turn]] = None,
    force_refresh:        bool = False,
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
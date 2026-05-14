"""
graph.py — EQUIFIZ Financial AI Agent (v10.0)

Key changes from v9.0
─────────────────────────────────────────────────────────────────────
Multi-Intent Decomposition — solves the problem of a single query
containing sub-questions with different entity families / intents.

1. NODE IQ — node_decompose_intents (NEW)
   Runs AFTER classify_and_extract (entities are known) but BEFORE
   any tool vector search.  The LLM splits the raw query into a list
   of IntentItem dicts, one per logical sub-question.
   Each IntentItem carries: entity, entity_type, intent_description,
   tool_hint, and resolved_codes (populated later).

2. NODE TVS — node_per_intent_tool_search (NEW)
   Iterates over the IntentItem list and runs one Chroma query per
   intent_description.  Attaches matched_tools to each IntentItem.
   This replaces the single upfront _query_tool_registry() call that
   was previously done inside node_classify_and_extract.

3. NODE MTCI — node_mcp_tool_call_intents (NEW)
   Replaces node_mcp_tool_call.  For each IntentItem:
     a) Uses the per-intent matched_tools to pick the best tool.
     b) Builds a per-intent injection block (entity codes already
        resolved by mcp_pre_resolve / mcp_multi_pre_resolve).
     c) Calls the MCP client with the focused enriched query.
     d) Stores the result in IntentItem["mcp_result"].
   All MCP calls run in parallel (ThreadPoolExecutor).

4. node_classify_and_extract — tool vector search REMOVED.
   It now only classifies query_type, extracts entities and sets
   primary/additional entity metadata.  matched_tools on AgentState
   is set to [] here; TVS fills it per intent instead.

5. node_mcp_synthesis — receives state["intent_results"] (list of
   {intent_description, entity, mcp_result}) and synthesises a
   single coherent answer across all intents.

6. AgentState — added "intents" (list[IntentItem]) and
   "intent_results" (list[dict]) fields.

All v9.0 logic retained where unchanged.
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


# ── Intent item ───────────────────────────────────────────────────────────────

class IntentItem(TypedDict, total=False):
    """One logical sub-question extracted from the user query."""
    entity:             str          # canonical name of the entity
    entity_type:        str          # stock | mf_scheme | mf_amc | etf | index | general
    intent_description: str          # natural-language description for vector search
    tool_hint:          str          # suggested tool name (may be empty)
    scheme_name:        Optional[str]
    amc_name:           Optional[str]
    nse_symbol:         Optional[str]
    matched_tools:      list[dict]   # filled by node_per_intent_tool_search
    resolved_codes:     dict         # filled by resolve step
    mcp_result:         str          # filled by node_mcp_tool_call_intents


# ── Tool family classification ────────────────────────────────────────────────

_TOOL_FAMILY_HINTS: dict[str, str] = {
    "get_company_stock":     "stock",
    "get_delayed_stock":     "stock",
    "get_market_indices":    "index",
    "get_active_performer":  "stock",
    "get_top_gainers":       "stock",
    "get_top_losers":        "stock",
    "get_out_under":         "stock",
    "get_advance_decline":   "stock",
    "get_52week":            "stock",
    "get_new_highs":         "stock",
    "get_index_companies":   "index",
    "get_sector_companies":  "stock",
    "get_quarterly_results": "stock",
    "get_profit_loss":       "stock",
    "get_balance_sheet":     "stock",
    "get_cash_flow":         "stock",
    "get_half_yearly":       "stock",
    "get_nine_months":       "stock",
    "get_yearly_results":    "stock",
    "get_quarterly_balance": "stock",
    "get_annual_balance":    "stock",
    "get_ttm_growth":        "stock",
    "get_quarterly_revenue": "stock",
    "get_quarterly_ebitda":  "stock",
    "get_quarterly_ebit":    "stock",
    "get_growth_data":       "stock",
    "get_key_financial":     "stock",
    "get_daily_ratios":      "stock",
    "get_margin_ratios":     "stock",
    "get_performance_ratio": "stock",
    "get_cashflow_ratios":   "stock",
    "get_growth_ratios":     "stock",
    "get_liquidity_ratios":  "stock",
    "get_solvency_ratios":   "stock",
    "get_return_ratios":     "stock",
    "get_all_basic_ratios":  "stock",
    "get_quarterly_ratios":  "stock",
    "get_yearly_ratios":     "stock",
    "get_valuation_ratios":  "stock",
    "get_shareholding":      "stock",
    "get_major_sharehold":   "stock",
    "get_company_profile":   "stock",
    "get_company_backgroun": "stock",
    "get_board_of":          "stock",
    "get_company_bankers":   "stock",
    "get_management":        "stock",
    "get_subsidiaries":      "stock",
    "get_related_party":     "stock",
    "get_employee_count":    "stock",
    "get_capital_structure": "stock",
    "get_pledge_share":      "stock",
    "get_chronological":     "stock",
    "get_company_history":   "stock",
    "get_substantial":       "stock",
    "get_segment_data":      "stock",
    "get_r_and_d":           "stock",
    "get_finished_products": "stock",
    "get_raw_materials":     "stock",
    "get_forthcoming_ipo":   "stock",
    "get_open_ipos":         "stock",
    "get_closed_ipos":       "stock",
    "get_new_ipo":           "stock",
    "get_best_ipo":          "stock",
    "get_ipo_":              "stock",
    "get_anchor_investor":   "stock",
    "get_basis_of":          "stock",
    "get_scheme_nav":        "mf_scheme",
    "get_investment_detail": "mf_scheme",
    "get_expense_ratio":     "mf_scheme",
    "get_avg_maturity":      "mf_scheme",
    "get_scheme_aum":        "mf_scheme",
    "get_nav_historical":    "mf_scheme",
    "get_scheme_returns":    "mf_scheme",
    "get_lumpsum_returns":   "mf_scheme",
    "get_scheme_sip":        "mf_scheme",
    "get_mf_holdings":       "mf_scheme",
    "get_sector_allocation": "mf_scheme",
    "get_asset_allocation":  "mf_scheme",
    "get_portfolio_changes": "mf_scheme",
    "get_mcap_allocation":   "mf_scheme",
    "get_most_bought":       "mf_scheme",
    "get_scheme_ratios":     "mf_scheme",
    "get_dividend_details":  "mf_scheme",
    "get_bse_star_scheme":   "mf_scheme",
    "compare_schemes":       "mf_scheme",
    "get_whats_in_out":      "mf_scheme",
    "get_fund_categories":   "mf_amc",
    "get_schemes_by_amc":    "mf_amc",
    "get_fund_profile":      "mf_amc",
    "get_etf_":              "etf",
    "get_index_":            "index",
}


def _infer_entity_type_from_tool(tool_hint: str) -> str:
    if not tool_hint:
        return "general"
    tl = tool_hint.lower()
    for prefix, family in _TOOL_FAMILY_HINTS.items():
        if tl.startswith(prefix):
            return family
    return "general"


# ── DB routing ────────────────────────────────────────────────────────────────

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
        "table":    "companies",
        "column":   "companyname",
        "id_field": "co_code",
    },
    "isin": {
        "table":    "etf_master",
        "column":   "etfname",
        "id_field": "isin",
    },
    "index_code": {
        "table":    "group_master",
        "column":   "group_name",
        "id_field": "indexcode",
    },
}

ENTITY_TYPE_PARAM_MAP: dict[str, list[str]] = {
    "stock":     ["co_code"],
    "mf_scheme": ["mf_schcode"],
    "mf_amc":    ["mf_cocode"],
    "etf":       ["isin"],
    "index":     ["index_code"],
    "general":   [],
}

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "equifiz",
    "user":     "postgres",
    "password": "1234",
}

TOOL_SCORE_THRESHOLD = 0.55


# ══════════════════════════════════════════════════════════════════════════════
# Chroma Tool Registry
# ══════════════════════════════════════════════════════════════════════════════

def _query_tool_registry(
    query: str,
    n_results: int = 8,
    score_threshold: float = TOOL_SCORE_THRESHOLD,
) -> list[dict]:
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
            res["ids"][0], res["metadatas"][0], res["distances"][0]
        ):
            if dist > score_threshold:
                continue
            raw_req    = meta.get("required_parameters", "") or ""
            req_params = [p.strip() for p in raw_req.split(",") if p.strip()]
            tools.append({
                "tool_name":           meta.get("name", tool_id),
                "description":         meta.get("description", ""),
                "required_parameters": req_params,
                "parameters":          meta.get("parameters", ""),
                "score":               round(1 - dist, 3),
            })

        console.print(
            f"  [ToolRegistry] '{query[:60]}' → {len(tools)} tools "
            f"(threshold={score_threshold})"
        )
        return tools

    except Exception as e:
        console.print(f"  [ToolRegistry] Chroma query failed: {e}")
        return []


def _get_tool_meta(tool_name: str) -> Optional[dict]:
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
    query_type:            str
    extracted_entity:      str
    report_type:           str
    intent:                str
    is_broad:              bool
    mcp_needed:            bool
    mcp_tool_hint:         str

    # Primary entity metadata
    primary_entity_type:  str
    primary_tool_hint:    str
    primary_query_intent: str

    # Matched tool candidates (kept for compatibility; TVS fills per-intent instead)
    matched_tools: list[dict]

    # ── NEW v10.0 ────────────────────────────────────────────────────────────
    # Decomposed intents — one entry per logical sub-question
    intents: list[IntentItem]
    # Final per-intent results fed into synthesis
    intent_results: list[dict]   # [{entity, intent_description, mcp_result}, ...]

    # Symbol resolution (general / vector path)
    nse_symbol:   Optional[str]
    co_code:      Optional[int]
    company_info: Optional[dict]

    # MCP-specific resolved codes (primary entity)
    mcp_resolved_codes: dict
    mcp_scheme_name:    Optional[str]
    mcp_amc_name:       Optional[str]

    # Multi-entity list (all entities after multi_pre_resolve)
    companies: list[dict]

    # ChromaDB path
    vector_context: list[dict]

    # MCP path output (legacy single-result kept for compatibility)
    mcp_raw_result:      str
    mcp_tool_calls_made: list[str]

    # Output
    final_answer: str
    error:        Optional[str]


# ── LLM helpers ───────────────────────────────────────────────────────────────

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


# ══════════════════════════════════════════════════════════════════════════════
# Node A: Classify + Extract (v10.0 — entity extraction only, no tool search)
# ══════════════════════════════════════════════════════════════════════════════

CLASSIFY_AND_EXTRACT_PROMPT = """\
You are a financial query analyzer for an Indian stock-market chatbot.

## Conversation history
{history}

## Current user query
{query}

Return ONLY a valid JSON object with this exact structure:
{{
  "query_type": "<greeting|general|company|comparison|investment>",
  "is_broad": <true|false>,
  "mcp_needed": <true|false>,
  "primary": {{
    "entity_type": "<stock|mf_scheme|mf_amc|etf|index|general>",
    "company_name": "<canonical name or empty>",
    "nse_symbol": "<symbol or empty>",
    "scheme_name": "<scheme name or empty>",
    "amc_name": "<amc name or empty>",
    "query_intent": "<what data is needed for this entity>"
  }},
  "additional_companies": [
    {{
      "entity_type": "<stock|mf_scheme|mf_amc|etf|index|general>",
      "company_name": "<name or empty>",
      "scheme_name": "<name or empty>",
      "amc_name": "<name or empty>",
      "nse_symbol": "<symbol or empty>",
      "query_intent": "<what data is needed for this entity>"
    }}
  ]
}}

ENTITY IDENTIFICATION RULES:
1. "Reliance", "TCS", "HDFC Bank" → always stock.
2. "SBI Mutual Fund", "HDFC AMC" → always mf_amc.
3. "Parag Parikh Flexi Cap", "Axis Bluechip" → always mf_scheme.
4. Extract ALL named entities from the query.
5. First named entity → "primary". Rest → "additional_companies".
6. If only one entity → "additional_companies" is [].
7. Do NOT include tool_hint here — tools are resolved later per intent.
"""


def node_classify_and_extract(state: AgentState) -> AgentState:
    """
    v10.0: Entity extraction only.
    Tool vector search has been moved to node_per_intent_tool_search.
    """
    console.print("[Node A] Classify + extract (v10.0 — entity extraction only)")

    history_text = _format_history(state, n=10)
    user_query   = state.get("user_query", "")

    # Clear matched_tools — will be filled per-intent by TVS
    state["matched_tools"] = []

    try:
        result = _llm_json(
            CLASSIFY_AND_EXTRACT_PROMPT.format(
                history = history_text,
                query   = user_query,
            )
        )

        state["query_type"] = result.get("query_type", "general")
        state["mcp_needed"] = result.get("mcp_needed", False)

        primary = result.get("primary") or {}

        raw_entity_type = primary.get("entity_type", "general")
        state["primary_entity_type"] = raw_entity_type
        state["primary_tool_hint"]   = ""       # filled later by TVS
        state["primary_query_intent"] = primary.get("query_intent", "")
        state["mcp_tool_hint"]        = ""

        state["extracted_entity"] = (
            primary.get("company_name")
            or primary.get("scheme_name")
            or user_query
        )
        state["nse_symbol"]      = primary.get("nse_symbol") or None
        state["mcp_scheme_name"] = primary.get("scheme_name") or None
        state["mcp_amc_name"]    = primary.get("amc_name") or None

        additional = result.get("additional_companies") or []
        companies: list[dict] = []

        for c in additional:
            companies.append({
                "name":         (
                    c.get("company_name")
                    or c.get("scheme_name")
                    or c.get("amc_name")
                    or ""
                ),
                "entity_type":  c.get("entity_type", "general"),
                "tool_hint":    "",         # filled later by TVS
                "query_intent": c.get("query_intent", ""),
                "scheme_name":  c.get("scheme_name"),
                "amc_name":     c.get("amc_name"),
                "nse_symbol":   c.get("nse_symbol"),
            })

        state["companies"] = companies

        console.print(
            f"  → type={state['query_type']} "
            f"primary_entity_type={raw_entity_type} "
            f"additional={len(companies)}"
        )

    except Exception as e:
        console.print(f"  [ERROR] Node A failed: {e}")
        state["query_type"]           = "general"
        state["mcp_needed"]           = False
        state["primary_entity_type"]  = "general"
        state["primary_tool_hint"]    = ""
        state["primary_query_intent"] = ""
        state["companies"]            = []

    return state


# ══════════════════════════════════════════════════════════════════════════════
# Node IQ: Decompose intents (NEW v10.0)
#
# Splits the user query into a list of IntentItems BEFORE any tool search.
# One IntentItem per logical sub-question.  For a simple single-entity query
# this produces exactly one item.  For mixed queries like
# "Reliance financials and SBI MF scheme ratios" it produces two items,
# each with the correct entity_type so TVS can fetch the right tool family.
# ══════════════════════════════════════════════════════════════════════════════

DECOMPOSE_INTENTS_PROMPT = """\
You are analyzing a financial query to break it into independent sub-questions.

## Conversation history (last 4 turns)
{history}

## User query
{query}

## Already extracted entities
Primary entity: {primary_entity} (type={primary_type}, intent={primary_intent})
Additional entities: {additional_entities}

Return ONLY a valid JSON object:
{{
  "intents": [
    {{
      "entity":             "<canonical entity name>",
      "entity_type":        "<stock|mf_scheme|mf_amc|etf|index|general>",
      "intent_description": "<specific data needed — used as vector search query>",
      "scheme_name":        "<if mf_scheme, the scheme name, else empty>",
      "amc_name":           "<if mf_amc, the AMC name, else empty>",
      "nse_symbol":         "<if stock and NSE symbol is known, else empty>"
    }}
  ]
}}

RULES:
1. Each intent must map to exactly ONE entity and ONE data need.
2. "intent_description" should be a precise phrase that will retrieve the
   correct MCP tool via semantic search.  Examples:
     - "quarterly financial results for Reliance Industries"
     - "scheme ratios for Parag Parikh Flexi Cap fund"
     - "key financial ratios for TCS stock"
     - "SBI Mutual Fund AMC profile and fund categories"
3. If the query has only one entity and one intent → return a list of length 1.
4. Never merge two different entity families into one intent item.
5. If the query has a broad comparison (e.g. "compare X and Y on metric Z"),
   create one intent per entity.
6. entity_type rules: Reliance/TCS/HDFC Bank → stock; SBI MF/HDFC AMC → mf_amc;
   Parag Parikh Flexi Cap/Axis Bluechip → mf_scheme.
"""


def node_decompose_intents(state: AgentState) -> AgentState:
    """
    NEW v10.0: Splits the user query into a list of IntentItems.
    Runs after classify_and_extract so entity names are already known.
    """
    console.print("[Node IQ] Decompose intents (v10.0)")

    user_query    = state.get("user_query", "")
    primary_name  = (
        state.get("mcp_scheme_name")
        or state.get("mcp_amc_name")
        or state.get("extracted_entity", "")
    )
    primary_type  = state.get("primary_entity_type", "general")
    primary_intent = state.get("primary_query_intent", "")

    additional_entities_str = "; ".join(
        f"{c.get('name', '')} (type={c.get('entity_type', 'general')}, "
        f"intent={c.get('query_intent', '')})"
        for c in (state.get("companies") or [])
    ) or "none"

    try:
        result = _llm_json(
            DECOMPOSE_INTENTS_PROMPT.format(
                history            = _format_history(state, n=4),
                query              = user_query,
                primary_entity     = primary_name,
                primary_type       = primary_type,
                primary_intent     = primary_intent,
                additional_entities = additional_entities_str,
            )
        )

        raw_intents = result.get("intents") or []
        intents: list[IntentItem] = []

        for item in raw_intents:
            et = item.get("entity_type", "general")
            intents.append(IntentItem(
                entity             = item.get("entity", ""),
                entity_type        = et,
                intent_description = item.get("intent_description", ""),
                scheme_name        = item.get("scheme_name") or None,
                amc_name           = item.get("amc_name") or None,
                nse_symbol         = item.get("nse_symbol") or None,
                tool_hint          = "",
                matched_tools      = [],
                resolved_codes     = {},
                mcp_result         = "",
            ))

        # Fallback: if LLM returned empty, synthesise one intent from primary
        if not intents:
            console.print("  ⚠ LLM returned no intents — using primary entity fallback")
            intents.append(IntentItem(
                entity             = primary_name,
                entity_type        = primary_type,
                intent_description = primary_intent or user_query,
                scheme_name        = state.get("mcp_scheme_name"),
                amc_name           = state.get("mcp_amc_name"),
                nse_symbol         = state.get("nse_symbol"),
                tool_hint          = "",
                matched_tools      = [],
                resolved_codes     = {},
                mcp_result         = "",
            ))

        state["intents"] = intents
        console.print(
            f"  → {len(intents)} intent(s): "
            + "; ".join(
                f"[{i['entity']} / {i['entity_type']}] {i['intent_description'][:60]}"
                for i in intents
            )
        )

    except Exception as e:
        console.print(f"  [ERROR] Node IQ failed: {e}")
        # Fallback to single intent
        state["intents"] = [IntentItem(
            entity             = primary_name,
            entity_type        = primary_type,
            intent_description = user_query,
            scheme_name        = state.get("mcp_scheme_name"),
            amc_name           = state.get("mcp_amc_name"),
            nse_symbol         = state.get("nse_symbol"),
            tool_hint          = "",
            matched_tools      = [],
            resolved_codes     = {},
            mcp_result         = "",
        )]

    return state


# ══════════════════════════════════════════════════════════════════════════════
# Node TVS: Per-intent tool vector search (NEW v10.0)
#
# Runs one Chroma semantic search per IntentItem using intent_description as
# the query.  Stores the top matched tools into IntentItem["matched_tools"].
# Also picks the best tool_hint per intent and updates it.
#
# Because each intent_description is entity-type-specific
# (e.g. "scheme ratios for Parag Parikh Flexi Cap"), the semantic search
# naturally returns mf_scheme tools — never stock tools — avoiding the
# cross-family confusion that plagued v9.0.
# ══════════════════════════════════════════════════════════════════════════════

def node_per_intent_tool_search(state: AgentState) -> AgentState:
    """
    NEW v10.0: Run one Chroma vector search per intent using
    intent_description as the query string.
    """
    console.print("[Node TVS] Per-intent tool vector search (v10.0)")

    intents = state.get("intents") or []
    if not intents:
        console.print("  ⚠ No intents to search for")
        return state

    def _search_for_intent(intent: IntentItem) -> IntentItem:
        search_query = (
            f"{intent['entity_type']} {intent['intent_description']}"
            if intent.get("entity_type") not in ("general", "")
            else intent["intent_description"]
        )
        tools = _query_tool_registry(search_query, n_results=8)

        # Filter to tools that match the intent's entity family
        et = intent.get("entity_type", "general")
        if et != "general" and tools:
            family_tools = [
                t for t in tools
                if _infer_entity_type_from_tool(t["tool_name"]) == et
            ]
            # Only apply filter if it doesn't empty the list
            if family_tools:
                tools = family_tools

        intent["matched_tools"] = tools

        # Pick best tool_hint from the top result
        if tools:
            intent["tool_hint"] = tools[0]["tool_name"]
            console.print(
                f"  [{intent['entity']}] intent='{intent['intent_description'][:50]}' "
                f"→ top tool={intent['tool_hint']} ({len(tools)} matched)"
            )
        else:
            intent["tool_hint"] = ""
            console.print(
                f"  [{intent['entity']}] intent='{intent['intent_description'][:50]}' "
                f"→ no tools matched"
            )

        return intent

    with ThreadPoolExecutor(max_workers=min(6, len(intents))) as ex:
        futures = {ex.submit(_search_for_intent, intent): idx
                   for idx, intent in enumerate(intents)}
        updated: dict[int, IntentItem] = {}
        for future in as_completed(futures):
            idx = futures[future]
            try:
                updated[idx] = future.result()
            except Exception as exc:
                console.print(f"  [red]TVS failed for intent #{idx}: {exc}[/red]")
                updated[idx] = intents[idx]

    state["intents"] = [updated[i] for i in sorted(updated)]

    # Backfill legacy state fields from the first intent for compatibility
    if state["intents"]:
        first = state["intents"][0]
        state["matched_tools"]       = first.get("matched_tools") or []
        state["mcp_tool_hint"]       = first.get("tool_hint") or ""
        state["primary_tool_hint"]   = first.get("tool_hint") or ""

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
    console.print("[Node C] General handler")
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


# ══════════════════════════════════════════════════════════════════════════════
# DB helpers
# ══════════════════════════════════════════════════════════════════════════════

def _targeted_db_lookup(
    entity:   str,
    table:    str,
    name_col: str,
    id_col:   str,
) -> Optional[dict]:
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

        console.print(f"  ⚠ No confident match in {table} (best={best_score})")
        return None

    except Exception as e:
        console.print(f"  [_targeted_db_lookup] DB error on {table}: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
# Required params resolver
# ══════════════════════════════════════════════════════════════════════════════

def _get_required_params_for_entity(
    tool_hint:     str,
    entity_type:   str,
    matched_tools: list[dict],
) -> list[str]:
    if entity_type and entity_type != "general":
        params = ENTITY_TYPE_PARAM_MAP.get(entity_type, [])
        console.print(
            f"  📋 entity_type='{entity_type}' → authoritative params: {params}"
        )
        return params

    if tool_hint:
        for t in matched_tools:
            if t["tool_name"] == tool_hint:
                db_params = [p for p in t["required_parameters"] if p in PARAM_TO_TABLE_MAP]
                if db_params:
                    return db_params
                break

        meta = _get_tool_meta(tool_hint)
        if meta:
            db_params = [p for p in meta["required_parameters"] if p in PARAM_TO_TABLE_MAP]
            if db_params:
                return db_params

    console.print(f"  📋 No DB params resolved for entity_type='{entity_type}'")
    return []


# ══════════════════════════════════════════════════════════════════════════════
# Core per-entity DB resolver
# ══════════════════════════════════════════════════════════════════════════════

def _resolve_entity_codes(
    name:            str,
    scheme_name:     Optional[str],
    amc_name:        Optional[str],
    nse_symbol:      Optional[str],
    required_params: list[str],
) -> dict:
    codes:  dict = {}
    result: dict = {"name": name}

    needs_schcode   = "mf_schcode"  in required_params
    needs_cocode    = "mf_cocode"   in required_params
    needs_co_code   = "co_code"     in required_params
    needs_isin      = "isin"        in required_params
    needs_indexcode = "index_code"  in required_params

    if needs_schcode:
        search = scheme_name or name
        cfg    = PARAM_TO_TABLE_MAP["mf_schcode"]
        match  = _targeted_db_lookup(
            entity=search, table=cfg["table"],
            name_col=cfg["column"], id_col=cfg["id_field"],
        )
        if match:
            codes["mf_schcode"] = int(match["mf_schcode"])
            if "mf_cocode" in match:
                codes.setdefault("mf_cocode", int(match["mf_cocode"]))
            result["resolved_scheme_name"] = match.get("sch_name", search)

    if needs_cocode and not codes.get("mf_cocode"):
        search = amc_name or name
        cfg    = PARAM_TO_TABLE_MAP["mf_cocode"]
        match  = _targeted_db_lookup(
            entity=search, table=cfg["table"],
            name_col=cfg["column"], id_col=cfg["id_field"],
        )
        if match:
            codes["mf_cocode"] = int(match["mf_cocode"])
            result["resolved_amc_name"] = match.get("lname", search)

    if needs_co_code:
        stock = _resolve_single(name, nse_symbol)
        if stock:
            codes["co_code"] = stock["co_code"]
            result.update({
                "co_code":      stock["co_code"],
                "company_info": stock["company_info"],
                "nse_symbol":   stock["nse_symbol"],
            })

    if needs_isin:
        search = scheme_name or name
        cfg    = PARAM_TO_TABLE_MAP["isin"]
        match  = _targeted_db_lookup(
            entity=search, table=cfg["table"],
            name_col=cfg["column"], id_col=cfg["id_field"],
        )
        if match:
            codes["isin"] = match["isin"]
            result["resolved_etf_name"] = match.get("etfname", search)

    if needs_indexcode:
        cfg   = PARAM_TO_TABLE_MAP["index_code"]
        match = _targeted_db_lookup(
            entity=name, table=cfg["table"],
            name_col=cfg["column"], id_col=cfg["id_field"],
        )
        if match:
            codes["index_code"] = int(match["indexcode"])
            result["resolved_index_name"] = match.get("group_name", name)

    result["mcp_resolved_codes"] = codes
    return result


# ══════════════════════════════════════════════════════════════════════════════
# Resolve codes for all intents (v10.0 helper)
#
# After TVS has filled intent["tool_hint"] and intent["matched_tools"],
# this function populates intent["resolved_codes"] for every IntentItem
# in parallel.  Called from both mcp_pre_resolve (single) and
# mcp_multi_pre_resolve (multi).
# ══════════════════════════════════════════════════════════════════════════════

def _resolve_intents_codes(intents: list[IntentItem]) -> list[IntentItem]:
    """Resolve DB codes for each IntentItem in parallel."""

    def _resolve_one(intent: IntentItem) -> IntentItem:
        et         = intent.get("entity_type", "general")
        tool_hint  = intent.get("tool_hint", "")
        matched    = intent.get("matched_tools") or []

        if et == "general" and tool_hint:
            et = _infer_entity_type_from_tool(tool_hint)
            intent["entity_type"] = et

        req_params = _get_required_params_for_entity(tool_hint, et, matched)

        resolved = _resolve_entity_codes(
            name            = intent.get("entity", ""),
            scheme_name     = intent.get("scheme_name"),
            amc_name        = intent.get("amc_name"),
            nse_symbol      = intent.get("nse_symbol"),
            required_params = req_params,
        )
        intent["resolved_codes"] = resolved.get("mcp_resolved_codes", {})
        console.print(
            f"  💎 [{intent['entity']}] resolved_codes={intent['resolved_codes']}"
        )
        return intent

    result_intents: dict[int, IntentItem] = {}
    with ThreadPoolExecutor(max_workers=min(6, len(intents) or 1)) as ex:
        futures = {ex.submit(_resolve_one, intent): idx
                   for idx, intent in enumerate(intents)}
        for future in as_completed(futures):
            idx = futures[future]
            try:
                result_intents[idx] = future.result()
            except Exception as exc:
                console.print(f"  [red]Code resolution failed for intent #{idx}: {exc}[/red]")
                result_intents[idx] = intents[idx]

    return [result_intents[i] for i in sorted(result_intents)]


# ══════════════════════════════════════════════════════════════════════════════
# MCP Pre-Resolution — single entity (v10.0)
# ══════════════════════════════════════════════════════════════════════════════

def node_mcp_pre_resolve(state: AgentState) -> AgentState:
    console.print("[Node MPR] Single-entity MCP pre-resolve (v10.0)")

    intents = state.get("intents") or []
    if not intents:
        console.print("  ⚠ No intents; skipping resolve")
        state["mcp_resolved_codes"] = {}
        return state

    resolved_intents = _resolve_intents_codes(intents)
    state["intents"] = resolved_intents

    # Back-fill legacy primary codes from the first intent
    if resolved_intents:
        state["mcp_resolved_codes"] = resolved_intents[0].get("resolved_codes", {})

    console.print(f"  🏁 Primary resolved codes: {state['mcp_resolved_codes']}")
    return state


# ══════════════════════════════════════════════════════════════════════════════
# MCP Pre-Resolution — multi-entity (v10.0)
# ══════════════════════════════════════════════════════════════════════════════

def node_mcp_multi_pre_resolve(state: AgentState) -> AgentState:
    console.print("[Node MMPR] Multi-entity MCP pre-resolve (v10.0)")

    intents = state.get("intents") or []
    if not intents:
        console.print("  ⚠ No intents; skipping resolve")
        return state

    resolved_intents = _resolve_intents_codes(intents)
    state["intents"] = resolved_intents

    if resolved_intents:
        state["mcp_resolved_codes"] = resolved_intents[0].get("resolved_codes", {})

    console.print(
        f"  🏁 Resolved {len(resolved_intents)} intents. "
        f"Primary codes: {state['mcp_resolved_codes']}"
    )
    return state


# ══════════════════════════════════════════════════════════════════════════════
# Injection block builder (v10.0)
#
# Builds per-intent PRE_RESOLVED blocks from IntentItem["resolved_codes"].
# When called with a single intent the output matches the v9.0 single-entity
# format exactly, keeping mcp_client.py fully compatible.
# ══════════════════════════════════════════════════════════════════════════════

def _build_injection_block_for_intent(intent: IntentItem) -> str:
    """Build a single-intent PRE_RESOLVED block."""
    codes        = intent.get("resolved_codes") or {}
    entity_type  = intent.get("entity_type", "")
    tool_hint    = intent.get("tool_hint", "")
    scheme_name  = intent.get("scheme_name", "") or ""
    amc_name     = intent.get("amc_name", "") or ""
    intent_desc  = intent.get("intent_description", "")

    lines: list[str] = []
    if entity_type:
        lines.append(f"entity_type={entity_type}")
    if intent_desc:
        lines.append(f"query_intent={intent_desc}")
    if tool_hint:
        lines.append(f"recommended_tool={tool_hint}")

    _FAMILY_COMMENTS = {
        "co_code":    "stock/equity tools only",
        "mf_schcode": "MF scheme tools only",
        "mf_cocode":  "AMC/fund-house tools only",
        "isin":       "ETF tools only",
        "index_code": "index tools only",
    }
    for param, val in codes.items():
        comment = f"  # {_FAMILY_COMMENTS[param]}" if param in _FAMILY_COMMENTS else ""
        lines.append(f"{param}={val}{comment}")

    if scheme_name:
        lines.append(f"scheme_name={scheme_name}")
    if amc_name:
        lines.append(f"amc_name={amc_name}")

    if lines:
        return "<PRE_RESOLVED>\n" + "\n".join(lines) + "\n</PRE_RESOLVED>"
    return ""


def _build_injection_block(state: AgentState) -> str:
    """
    Compatibility wrapper used by node_mcp_tool_call (legacy single call).
    Delegates to the first intent's block.
    """
    intents = state.get("intents") or []
    if intents:
        return _build_injection_block_for_intent(intents[0])

    # Hard fallback: v9.0 path
    resolved_codes = state.get("mcp_resolved_codes") or {}
    entity_type    = state.get("primary_entity_type", "")
    tool_hint      = state.get("mcp_tool_hint", "")
    lines: list[str] = []
    if entity_type:
        lines.append(f"entity_type={entity_type}")
    if tool_hint:
        lines.append(f"recommended_tool={tool_hint}")
    for param, val in resolved_codes.items():
        lines.append(f"{param}={val}")
    if lines:
        return "<PRE_RESOLVED>\n" + "\n".join(lines) + "\n</PRE_RESOLVED>"
    return ""


# ══════════════════════════════════════════════════════════════════════════════
# MCP runner helper (unchanged from v9.0)
# ══════════════════════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════════════════════
# Node MTCI: MCP Tool Call — per Intent (NEW v10.0)
#
# Replaces node_mcp_tool_call.
#
# For each IntentItem:
#   1. Build a focused injection block from intent["resolved_codes"].
#   2. Append the intent_description as the effective sub-query.
#   3. Append [RECOMMENDED TOOL: <tool_hint>] so mcp_client routes correctly.
#   4. Call MCP.  Store result in intent["mcp_result"].
#
# All MCP calls run in parallel (ThreadPoolExecutor).
# Results are aggregated into state["intent_results"] for synthesis.
# state["mcp_raw_result"] is set to the concatenation for compatibility.
# ══════════════════════════════════════════════════════════════════════════════

def node_mcp_tool_call_intents(state: AgentState) -> AgentState:
    """
    NEW v10.0: Per-intent MCP tool call with focused injection blocks.
    """
    console.print("[Node MTCI] Per-intent MCP tool call (v10.0)")

    try:
        from mcp_client import run_mcp_query  # noqa: F401
    except ImportError:
        console.print("MCP client module not found.")
        state["mcp_raw_result"]  = ""
        state["intent_results"]  = []
        state["error"]           = "mcp_client_not_found"
        return state

    intents    = state.get("intents") or []
    user_query = state.get("user_query", "")

    if not intents:
        console.print("  ⚠ No intents; falling back to raw user query")
        intents = [IntentItem(
            entity             = "",
            entity_type        = "general",
            intent_description = user_query,
            tool_hint          = state.get("mcp_tool_hint", ""),
            matched_tools      = [],
            resolved_codes     = state.get("mcp_resolved_codes") or {},
            mcp_result         = "",
        )]

    def _call_intent(intent: IntentItem) -> IntentItem:
        injection = _build_injection_block_for_intent(intent)
        sub_query = intent.get("intent_description") or user_query

        if injection:
            enriched = f"{injection}\nUser Query: {sub_query}"
        else:
            enriched = sub_query

        tool_hint = intent.get("tool_hint", "")
        if tool_hint:
            enriched += f"\n\n[RECOMMENDED TOOL: {tool_hint}]"

        console.print(
            f"  📤 Intent [{intent.get('entity', '?')}] "
            f"tool={tool_hint} query='{sub_query[:60]}'"
        )

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                future = ex.submit(_run_mcp_in_new_loop, enriched)
                result = future.result(timeout=600)
            intent["mcp_result"] = result or "No data returned from server."
        except concurrent.futures.TimeoutError:
            intent["mcp_result"] = "MCP call timed out for this intent."
        except Exception as exc:
            intent["mcp_result"] = f"MCP call failed: {exc}"

        console.print(
            f"  📥 Intent [{intent.get('entity', '?')}] "
            f"→ {len(intent['mcp_result'])} chars"
        )
        return intent

    # Parallel MCP calls — one per intent
    result_map: dict[int, IntentItem] = {}
    with ThreadPoolExecutor(max_workers=min(4, len(intents))) as ex:
        futures = {ex.submit(_call_intent, intent): idx
                   for idx, intent in enumerate(intents)}
        for future in as_completed(futures):
            idx = futures[future]
            try:
                result_map[idx] = future.result()
            except Exception as exc:
                console.print(f"  [red]Intent #{idx} MCP call failed: {exc}[/red]")
                result_map[idx] = intents[idx]

    state["intents"] = [result_map[i] for i in sorted(result_map)]

    # Build intent_results for synthesis
    state["intent_results"] = [
        {
            "entity":             intent.get("entity", ""),
            "entity_type":        intent.get("entity_type", ""),
            "intent_description": intent.get("intent_description", ""),
            "mcp_result":         intent.get("mcp_result", ""),
        }
        for intent in state["intents"]
    ]

    # Legacy field — concatenation for mcp_synthesis compatibility
    state["mcp_raw_result"] = "\n\n---\n\n".join(
        f"[{r['entity']} / {r['intent_description']}]\n{r['mcp_result']}"
        for r in state["intent_results"]
    )
    state["mcp_tool_calls_made"] = []
    state["error"]               = None

    return state


# ══════════════════════════════════════════════════════════════════════════════
# MCP Synthesis (v10.0 — multi-intent aware)
# ══════════════════════════════════════════════════════════════════════════════

MCP_SYNTHESIS_PROMPT = """\
You are a knowledgeable Indian equity and mutual fund analyst assistant.

## Conversation history (last 4 turns)
{history}

## User Question
{user_query}

## Live Data from MCP Server (per intent)
{mcp_result}

## Instructions
1. Answer EACH sub-question in the user's query using the relevant live data block above.
2. Write in plain prose paragraphs only. No markdown, no bullet points,
   no asterisks, no tables. Do NOT use the rupee symbol — write "Rs" instead.
3. Present key numbers naturally woven into sentences.
4. Be honest if specific data fields are missing or unavailable.
5. Use Indian number formatting (Rs Cr for large numbers).
6. Keep under 300 words unless detail is explicitly requested.
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
    console.print("[Node MS] MCP synthesis (v10.0)")
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
        mcp_result = mcp_result[:4000],
    )
    try:
        state["final_answer"] = _llm_text(prompt)
    except Exception as e:
        console.print(f"  MCP synthesis failed: {e}")
        state["final_answer"] = mcp_result
    return state


# ── General synthesis ─────────────────────────────────────────────────────────

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


# ══════════════════════════════════════════════════════════════════════════════
# Router (v10.0 — unchanged logic, just routes to MTCI instead of MTC)
# ══════════════════════════════════════════════════════════════════════════════

def route_after_classify(state: AgentState) -> str:
    qt  = state.get("query_type", "general")
    mcp = state.get("mcp_needed", False)

    intents      = state.get("intents") or []
    additional   = state.get("companies") or []
    has_many     = len(additional) > 0 or len(intents) > 1

    primary_type    = state.get("primary_entity_type", "general")
    secondary_types = {c.get("entity_type", "general") for c in additional}
    is_heterogeneous = bool(
        secondary_types - {"general"} - {primary_type}
    )

    if qt == "greeting":
        return "greeting"

    if qt == "general" and not mcp:
        return "general"

    if has_many or qt == "comparison" or is_heterogeneous:
        console.print(
            f"  → multi_mcp "
            f"(has_many={has_many}, comparison={qt=='comparison'}, "
            f"heterogeneous={is_heterogeneous})"
        )
        return "multi_mcp"

    console.print(f"  → mcp_direct (single entity, type={primary_type})")
    return "mcp_direct"


# ══════════════════════════════════════════════════════════════════════════════
# Build graph (v10.0)
# ══════════════════════════════════════════════════════════════════════════════

def build_graph() -> Any:
    g = StateGraph(AgentState)

    # Existing nodes
    g.add_node("classify_and_extract",  node_classify_and_extract)
    g.add_node("greeting_handler",      node_greeting_handler)
    g.add_node("general_handler",       node_general_handler)
    g.add_node("synthesis",             node_synthesis)
    g.add_node("mcp_synthesis",         node_mcp_synthesis)

    # NEW v10.0 nodes
    g.add_node("decompose_intents",         node_decompose_intents)
    g.add_node("per_intent_tool_search",    node_per_intent_tool_search)
    g.add_node("mcp_pre_resolve",           node_mcp_pre_resolve)
    g.add_node("mcp_multi_pre_resolve",     node_mcp_multi_pre_resolve)
    g.add_node("mcp_tool_call_intents",     node_mcp_tool_call_intents)

    # ── Entry point ────────────────────────────────────────────────────────
    g.set_entry_point("classify_and_extract")

    # classify → decompose_intents (always, before routing)
    g.add_edge("classify_and_extract", "decompose_intents")

    # decompose → per_intent_tool_search
    g.add_edge("decompose_intents", "per_intent_tool_search")

    # per_intent_tool_search → router
    g.add_conditional_edges(
        "per_intent_tool_search",
        route_after_classify,
        {
            "greeting":   "greeting_handler",
            "general":    "general_handler",
            "mcp_direct": "mcp_pre_resolve",
            "multi_mcp":  "mcp_multi_pre_resolve",
        },
    )

    # Terminal paths
    g.add_edge("greeting_handler",          END)
    g.add_edge("general_handler",           "synthesis")
    g.add_edge("synthesis",                 END)

    # MCP paths — both resolve nodes feed into the single MTCI node
    g.add_edge("mcp_pre_resolve",           "mcp_tool_call_intents")
    g.add_edge("mcp_multi_pre_resolve",     "mcp_tool_call_intents")
    g.add_edge("mcp_tool_call_intents",     "mcp_synthesis")
    g.add_edge("mcp_synthesis",             END)

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


# """
# graph.py – EQUIFIZ Financial AI Agent (v6.0)

# Key changes from v5.3
# ───────────────────────
# 1. FULLY DYNAMIC TOOL REGISTRY — No more hardcoded _MCP_KEYWORDS,
#    TOOL_REQUIRED_PARAMS, _MF_SCHCODE_TOOLS, _MF_COCODE_TOOLS, or
#    _CO_CODE_TOOLS sets. All tool discovery, mcp_needed detection, and
#    required-parameter resolution now query the equifiz_tools Chroma
#    collection at runtime.

# 2. DYNAMIC mcp_needed DETECTION — _query_tool_registry() searches Chroma
#    with the user query. If ANY tool scores above TOOL_SCORE_THRESHOLD,
#    mcp_needed is set True automatically — no keyword lists needed.

# 3. DYNAMIC PROMPT INJECTION — Node A receives a live snapshot of the top-k
#    matching tools (name + description) so the LLM picks mcp_tool_hint from
#    real metadata, not a frozen prompt section.

# 4. DYNAMIC PARAMETER RESOLUTION — node_mcp_pre_resolve reads
#    required_parameters from Chroma metadata for the hinted tool. The
#    PARAM_TO_TABLE_MAP (DB routing) stays because it reflects your actual
#    Postgres schema, not tool business logic.

# 5. compare_schemes MULTI-CODE SUPPORT — compare_schemes requires multiple
#    mf_schcodes (comma-separated). The resolver now collects one code per
#    entity and builds the composite value automatically.

# Pipeline design (v7.0)
# ────────────────────────────────────
# Node A  : classify + extract
#            Queries Chroma for top matching tools → injects into LLM prompt.
#            LLM sets mcp_needed + mcp_tool_hint from live tool metadata.
#            Chroma score gate overrides LLM if score exceeds threshold.

#            ├─ "greeting"    → greeting_handler → END
#            ├─ "general"     → general_handler → synthesis → END
#            │
#            ├─ "company"     → mcp_pre_resolve → mcp_tool_call
#            │                  → mcp_synthesis → END
#            │
#            ├─ "comparison"  → mcp_multi_pre_resolve → mcp_tool_call
#            │                  → mcp_synthesis → END
#            │
#            └─ "investment"  → mcp_multi_pre_resolve → mcp_tool_call
#                               → mcp_synthesis → END
# """
# from __future__ import annotations

# import asyncio
# import concurrent.futures
# import json
# import logging
# import re
# import uuid
# from concurrent.futures import ThreadPoolExecutor, as_completed
# from datetime import date
# from typing import Any, Optional, TypedDict

# import psycopg2
# import psycopg2.extras
# from rapidfuzz import fuzz

# from langchain_core.messages import HumanMessage
# from langchain_ollama import ChatOllama
# from langgraph.graph import END, StateGraph

# import db
# import vector_store as vs
# from config.config import OLLAMA_BASE_URL, OLLAMA_MODEL
# from rich.console import Console

# console = Console()
# logger  = logging.getLogger(__name__)
# TODAY   = str(date.today())


# # ── LLM ───────────────────────────────────────────────────────────────────────

# llm = ChatOllama(base_url=OLLAMA_BASE_URL, model=OLLAMA_MODEL)


# # ── Conversation turn helper ──────────────────────────────────────────────────

# class Turn(TypedDict):
#     role: str       # "user" | "assistant"
#     content: str


# # ── DB routing: param name → Postgres table (schema-driven, not tool-driven) ─
# # This is the ONLY hardcoded map and it belongs here: it reflects your DB
# # schema, not any tool's business logic.

# PARAM_TO_TABLE_MAP: dict[str, dict[str, str]] = {
#     "mf_schcode": {
#         "table":    "scheme_master",
#         "column":   "sch_name",
#         "id_field": "mf_schcode",
#     },
#     "mf_cocode": {
#         "table":    "fund_house",
#         "column":   "lname",
#         "id_field": "mf_cocode",
#     },
#     "co_code": {
#         "table":    "company_master",
#         "column":   "companyname",
#         "id_field": "co_code",
#     },
#     "isin": {
#         "table":    "etf_master",
#         "column":   "etfname",
#         "id_field": "isin",
#     },
#     "index_code" : {
#         "table" : "group_master",
#         "column": "group_name",
#         "id_field" : "indexcode",
#     },
#  }

# # ── DB config ─────────────────────────────────────────────────────────────────

# DB_CONFIG = {
#     "host":     "localhost",
#     "port":     5432,
#     "dbname":   "equifiz",
#     "user":     "postgres",
#     "password": "1234",
# }

# # Cosine-distance threshold for Chroma tool matching.
# # Change this line (currently 0.40)
# TOOL_SCORE_THRESHOLD = 0.55


# # ═══════════════════════════════════════════════════════════════════════════════
# # Chroma Tool Registry  — single source of truth for tool discovery
# # ═══════════════════════════════════════════════════════════════════════════════

# def _query_tool_registry(
#     query: str,
#     n_results: int = 6,
#     score_threshold: float = TOOL_SCORE_THRESHOLD,
# ) -> list[dict]:
#     """
#     Query the equifiz_tools Chroma collection with a natural-language query.

#     Returns a list of matching tool dicts sorted best-first:
#         {tool_name, description, required_parameters, parameters, score}

#     required_parameters is a list[str] parsed from the comma-separated
#     metadata string (empty list for no-param tools).

#     Returns [] on any error so callers degrade gracefully.
#     """
#     try:
#         import chromadb
#         client     = chromadb.PersistentClient(path="./chroma_db")
#         collection = client.get_collection(name="equifiz_tools")
#         count      = collection.count()
#         if count == 0:
#             return []

#         res = collection.query(
#             query_texts=[query],
#             n_results=min(n_results, count),
#             include=["metadatas", "distances"],
#         )

#         if not (res["ids"] and res["ids"][0]):
#             return []

#         tools: list[dict] = []
#         for tool_id, meta, dist in zip(
#             res["ids"][0],
#             res["metadatas"][0],
#             res["distances"][0],
#         ):
#             if dist > score_threshold:
#                 continue

#             raw_req = meta.get("required_parameters", "") or ""
#             req_params = [p.strip() for p in raw_req.split(",") if p.strip()]

#             tools.append({
#                 "tool_name":           meta.get("name", tool_id),
#                 "description":         meta.get("description", ""),
#                 "required_parameters": req_params,
#                 "parameters":          meta.get("parameters", ""),
#                 "score":               round(1 - dist, 3),
#             })

#         console.print(
#             f"  [ToolRegistry] '{query[:60]}' → {len(tools)} tools matched "
#             f"(threshold={score_threshold})"
#         )
#         return tools

#     except Exception as e:
#         console.print(f"  [ToolRegistry] Chroma query failed: {e}")
#         return []


# def _get_tool_meta(tool_name: str) -> Optional[dict]:
#     """
#     Fetch metadata for a single known tool by name from Chroma.
#     Returns the same dict format as _query_tool_registry entries, or None.
#     """
#     try:
#         import chromadb
#         client     = chromadb.PersistentClient(path="./chroma_db")
#         collection = client.get_collection(name="equifiz_tools")
#         res        = collection.get(ids=[tool_name], include=["metadatas"])
#         if res["ids"]:
#             meta       = res["metadatas"][0]
#             raw_req    = meta.get("required_parameters", "") or ""
#             req_params = [p.strip() for p in raw_req.split(",") if p.strip()]
#             return {
#                 "tool_name":           meta.get("name", tool_name),
#                 "description":         meta.get("description", ""),
#                 "required_parameters": req_params,
#                 "parameters":          meta.get("parameters", ""),
#             }
#     except Exception as e:
#         console.print(f"  [ToolRegistry] get by id '{tool_name}' failed: {e}")
#     return None


# def _build_tool_candidates_block(tools: list[dict]) -> str:
#     """
#     Format matched tools into a readable block for the LLM prompt.
#     """
#     if not tools:
#         return "(no tools matched for this query)"
#     lines = []
#     for t in tools:
#         req = ", ".join(t["required_parameters"]) if t["required_parameters"] else "none"
#         lines.append(
#             f"- {t['tool_name']} (score={t['score']}): {t['description'][:120]}"
#             f"\n    requires: {req}"
#         )
#     return "\n".join(lines)


# # ── Agent State ───────────────────────────────────────────────────────────────

# class AgentState(TypedDict, total=False):
#     # Input
#     user_query:           str
#     conversation_history: list[Turn]
#     force_refresh:        bool

#     # Classification
#     query_type:       str
#     extracted_entity: str
#     report_type:      str
#     intent:           str
#     is_broad:         bool
#     mcp_needed:       bool
#     mcp_tool_hint:    str

#     # Matched tool candidates (set in Node A, consumed by pre-resolve)
#     matched_tools: list[dict]

#     # Symbol resolution (kept for general_handler / vector path only)
#     nse_symbol:   Optional[str]
#     co_code:      Optional[int]
#     company_info: Optional[dict]

#     # MCP-specific resolved codes (primary entity)
#     mcp_resolved_codes: dict
#     mcp_scheme_name:    Optional[str]
#     mcp_amc_name:       Optional[str]

#     # Multi-company entity list
#     companies: list[dict]

#     # ChromaDB path (general handler only)
#     vector_context: list[dict]

#     # MCP path output
#     mcp_raw_result:      str
#     mcp_tool_calls_made: list[str]

#     # Output
#     final_answer: str
#     error:        Optional[str]


# # ── LLM helpers ──────────────────────────────────────────────────────────────

# def _llm_json(prompt: str) -> dict:
#     resp = llm.invoke([HumanMessage(content=prompt)])
#     text = resp.content.strip()
#     text = re.sub(r"^```(?:json)?|```$", "", text, flags=re.MULTILINE).strip()
#     try:
#         return json.loads(text)
#     except json.JSONDecodeError:
#         match = re.search(r"\{.*\}", text, re.DOTALL)
#         if match:
#             return json.loads(match.group())
#         raise ValueError(f"Could not parse JSON from LLM: {text[:300]}")


# def _llm_text(prompt: str) -> str:
#     resp = llm.invoke([HumanMessage(content=prompt)])
#     return resp.content.strip()


# def _format_history(state: AgentState, n: int = 8) -> str:
#     history = state.get("conversation_history") or []
#     return "\n".join(
#         f"{t['role'].upper()}: {t['content']}" for t in history[-n:]
#     ) or "(no prior conversation)"


# def _strip_markdown(text: str) -> str:
#     lines = text.splitlines()
#     lines = [l for l in lines if not re.match(r"^\s*\|", l)]
#     text  = "\n".join(lines)
#     text  = text.replace("₹", "Rs ")
#     text  = re.sub(r"\*{1,3}(.+?)\*{1,3}", r"\1", text, flags=re.DOTALL)
#     text  = re.sub(r"_{1,3}(.+?)_{1,3}", r"\1", text, flags=re.DOTALL)
#     text  = re.sub(r"(?m)^[ \t]*(?:[-*#]+|[0-9]+\.)[ \t]+", "", text)
#     text  = re.sub(r"\n{3,}", "\n\n", text)
#     return text.strip()


# # ═══════════════════════════════════════════════════════════════════════════════
# # Node A: Classify + Extract  (fully dynamic)
# # ═══════════════════════════════════════════════════════════════════════════════

# _BROAD_KEYWORDS = {
#     "full analysis", "complete analysis", "full report", "complete report",
#     "deep dive", "detailed analysis", "everything about", "all financials",
#     "comprehensive", "full picture",
# }

# CLASSIFY_AND_EXTRACT_PROMPT = """\
# You are a financial query analyzer for an Indian stock-market chatbot.

# ## Conversation history (last 5 turns)
# {history}

# ## Current user query
# {query}

# ## Available MCP tools retrieved for this query (ranked by relevance)
# These are REAL tools from the live registry. Pick mcp_tool_hint from this list only.
# {tool_candidates}

# Return ONLY a valid JSON object with NO extra text, preamble, or markdown:
# {{
#   "query_type": "<greeting|general|company|comparison|investment>",
#   "is_broad": <true|false>,
#   "mcp_needed": <true|false>,
#   "mcp_tool_hint": "<exact tool_name from the list above, or empty string>",
#   "primary": {{
#     "company_name": "<canonical name or null>",
#     "nse_symbol":   "<NSE ticker or null>",
#     "scheme_name":  "<MF scheme full name or null>",
#     "amc_name":     "<AMC / fund house full name or null>",
#     "report_type":  "<s or c>"
#   }},
#   "additional_companies": [
#     {{
#       "company_name": "<canonical name or null>",
#       "nse_symbol":   "<NSE ticker or null>",
#       "scheme_name":  "<MF scheme full name or null>",
#       "amc_name":     "<AMC / fund house full name or null>"
#     }}
#   ]
# }}

# Classification rules:
# - "greeting"    : Hello, hi, thanks, bye, what can you do, small talk
# - "general"     : Generic financial concept, no specific company/fund
# - "company"     : Question about ONE specific company OR mutual fund scheme/AMC
# - "comparison"  : Explicit/implicit comparison of TWO OR MORE companies/schemes
# - "investment"  : Buy/invest recommendation query

# mcp_needed:
#   Set TRUE if ANY of the top tool candidates above match the query well
#   (the score is already filtered — if tools appear in the list, they matched).
#   Also set TRUE for: live/real-time prices, NAV, any MF data, IPO data,
#   market indices, gainers/losers, advance-decline, 52-week highs/lows,
#   AND for any fundamental data query (PE, EPS, balance sheet, P&L,
#   key ratios, quarterly results) — all data now comes via MCP.
#   Set FALSE only for greetings and pure general concept questions.

# mcp_tool_hint:
#   Pick the SINGLE best tool_name from the list above.
#   If multiple tools match, pick the most specific one for the query.
#   Leave empty only if mcp_needed is false.

# is_broad:
#   Set true ONLY for: "full analysis", "complete report", "deep dive",
#   "everything about X", "all financials", "comprehensive". Default FALSE.

# Entity extraction:
#   - Resolve brand names: "Maggi" → "Nestle India", "Jio" → "Reliance Industries"
#   - Use conversation history to resolve pronouns
#   - For MF queries: populate BOTH scheme_name AND amc_name wherever possible
#   - For comparison/investment: primary = first entity,
#     additional_companies = every subsequent entity (one object each)
#     For MF entities ALWAYS populate scheme_name AND amc_name.
#   - If query is about an AMC's schemes, set amc_name and leave company_name null
# """


# def node_classify_and_extract(state: AgentState) -> AgentState:
#     console.print("[Node A] Classify + extract (dynamic tool registry)")

#     history      = state.get("conversation_history") or []
#     history_text = "\n".join(
#         f"{t['role'].upper()}: {t['content']}" for t in history[-10:]
#     ) or "(none)"
#     user_query  = state.get("user_query", "")
#     query_lower = user_query.lower()
#     keyword_broad = any(kw in query_lower for kw in _BROAD_KEYWORDS)

#     # ── Step 1: Query Chroma for matching tools ────────────────────────────
#     matched_tools = _query_tool_registry(user_query, n_results=6)
#     state["matched_tools"] = matched_tools

#     chroma_mcp = len(matched_tools) > 0
#     if chroma_mcp:
#         console.print(
#             f"  [Chroma gate] {len(matched_tools)} tools matched → mcp_needed=True"
#         )

#     for i in matched_tools:
#         console.print(i)

#     tool_candidates_block = _build_tool_candidates_block(matched_tools)

#     # ── Step 2: LLM classification ─────────────────────────────────────────
#     try:
#         result = _llm_json(
#             CLASSIFY_AND_EXTRACT_PROMPT.format(
#                 history         = history_text,
#                 query           = user_query,
#                 tool_candidates = tool_candidates_block,
#             )
#         )

#         state["query_type"]    = result.get("query_type", "general")
#         state["is_broad"]      = result.get("is_broad", False) or keyword_broad
#         state["mcp_needed"]    = result.get("mcp_needed", False) or chroma_mcp
#         state["mcp_tool_hint"] = result.get("mcp_tool_hint", "")

#         # If LLM didn't pick a hint but Chroma matched, use the top Chroma result
#         if state["mcp_needed"] and not state["mcp_tool_hint"] and matched_tools:
#             state["mcp_tool_hint"] = matched_tools[0]["tool_name"]
#             console.print(
#                 f"  [Chroma fallback hint] Using top match: {state['mcp_tool_hint']}"
#             )

#         primary = result.get("primary") or {}

#         if state["mcp_needed"]:
#             mcp_scheme = primary.get("scheme_name")
#             mcp_amc    = primary.get("amc_name")
#             state["extracted_entity"] = (
#                 mcp_scheme or mcp_amc
#                 or primary.get("company_name") or user_query
#             )
#         else:
#             state["extracted_entity"] = primary.get("company_name") or user_query

#         state["nse_symbol"]      = primary.get("nse_symbol")
#         state["intent"]          = primary.get("intent", "general")
#         state["report_type"]     = primary.get("report_type", "s")
#         state["mcp_scheme_name"] = primary.get("scheme_name")
#         state["mcp_amc_name"]    = primary.get("amc_name")

#         # Build companies list
#         additional = result.get("additional_companies") or []
#         if additional:
#             state["companies"] = [
#                 {
#                     "name":        c.get("scheme_name") or c.get("company_name") or "",
#                     "nse_symbol":  c.get("nse_symbol"),
#                     "scheme_name": c.get("scheme_name"),
#                     "amc_name":    c.get("amc_name"),
#                 }
#                 for c in additional
#             ]

#         console.print(
#             f"  → type={state['query_type']} broad={state['is_broad']} "
#             f"mcp={state['mcp_needed']} hint={state['mcp_tool_hint']} "
#             f"entity={state['extracted_entity']} "
#             f"additional={len(state.get('companies', []))}"
#         )

#     except Exception as e:
#         console.print(f"  Classify+extract failed: {e} — defaulting to general")
#         state["query_type"]       = "general"
#         state["is_broad"]         = keyword_broad
#         state["mcp_needed"]       = chroma_mcp
#         state["mcp_tool_hint"]    = matched_tools[0]["tool_name"] if matched_tools else ""
#         state["extracted_entity"] = user_query
#         state["intent"]           = "general"
#         state["report_type"]      = "s"

#     return state


# # ── Node B: Greeting handler ──────────────────────────────────────────────────

# GREETING_PROMPT = """\
# You are EQUIFIZ — a friendly Indian stock-market AI assistant.
# Respond warmly and briefly to the user's greeting or small talk.
# Mention 1-2 things you can help with (PE ratios, quarterly results,
# investment recommendations, live NAV, MF returns, IPO listings, etc.).

# User: {query}
# """

# def node_greeting_handler(state: AgentState) -> AgentState:
#     console.print("[Node B] Greeting handler")
#     state["final_answer"] = _llm_text(
#         GREETING_PROMPT.format(query=state["user_query"])
#     )
#     return state


# # ── Node C: General handler ───────────────────────────────────────────────────

# def node_general_handler(state: AgentState) -> AgentState:
#     console.print("[Node C] General handler — vector search only")
#     try:
#         state["vector_context"] = vs.query(state["user_query"], n_results=5)
#     except Exception as e:
#         console.print(f"  General vector search failed: {e}")
#         state["vector_context"] = []
#     return state


# # ── Symbol resolution (kept for general handler / vector path) ────────────────

# def _resolve_single(name: str, nse_symbol: Optional[str]) -> dict:
#     if nse_symbol:
#         row = db.lookup_by_nse_symbol(nse_symbol)
#         if row:
#             return {
#                 "co_code":      row["co_code"],
#                 "company_info": row,
#                 "nse_symbol":   nse_symbol,
#             }
#     matches = db.fuzzy_search_company(name, limit=1)
#     if matches:
#         best = matches[0]
#         return {
#             "co_code":      best["co_code"],
#             "company_info": best,
#             "nse_symbol":   best.get("nsesymbol"),
#         }
#     return {}


# # ═══════════════════════════════════════════════════════════════════════════════
# # DB Helpers
# # ═══════════════════════════════════════════════════════════════════════════════

# def _targeted_db_lookup(
#     entity:   str,
#     table:    str,
#     name_col: str,
#     id_col:   str,
# ) -> Optional[dict]:
#     """
#     Generic fuzzy lookup for any table/column in PARAM_TO_TABLE_MAP.
#     For scheme_master returns full row (including mf_cocode) in one query.
#     Returns best-matching row or None if best score < 60.
#     """
#     try:
#         conn = psycopg2.connect(**DB_CONFIG)
#         cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
#         console.print(f"  🔍 DB lookup: table='{table}' entity='{entity}'")

#         if table == "scheme_master":
#             cur.execute(
#                 "SELECT mf_schcode, mf_cocode, sch_name, category FROM scheme_master"
#             )
#         else:
#             cur.execute(f"SELECT {id_col}, {name_col} FROM {table}")

#         rows = [dict(r) for r in cur.fetchall()]
#         cur.close()
#         conn.close()

#         best_match: Optional[dict] = None
#         best_score: int            = 0
#         el = entity.lower()

#         for row in rows:
#             candidate = (row.get(name_col) or "").lower()
#             score = max(
#                 fuzz.token_set_ratio(el, candidate),
#                 fuzz.partial_ratio(el, candidate),
#             )
#             if score > best_score:
#                 best_score, best_match = score, row

#         if best_match and best_score >= 60:
#             console.print(
#                 f"  ✅ match='{best_match.get(name_col)}' "
#                 f"id={best_match.get(id_col)} score={best_score}"
#             )
#             return best_match

#         console.print(f"  No confident match in {table} (best={best_score})")
#         return None

#     except Exception as e:
#         console.print(f"  [_targeted_db_lookup] DB error on {table}: {e}")
#         return None


# # ═══════════════════════════════════════════════════════════════════════════════
# # Core per-entity resolver  (dynamic — reads required_params from Chroma)
# # ═══════════════════════════════════════════════════════════════════════════════

# def _get_required_params_for_tool(tool_hint: str, matched_tools: list[dict]) -> list[str]:
#     """
#     Determine which DB parameters a tool needs.

#     Resolution order:
#       1. matched_tools list from Node A (already in memory, free)
#       2. Direct Chroma get by tool name
#       3. Empty list (treat as no-param tool)

#     Returns a list of param names that appear in PARAM_TO_TABLE_MAP.
#     """
#     if not tool_hint:
#         return []

#     for t in matched_tools:
#         if t["tool_name"] == tool_hint:
#             db_params = [
#                 p for p in t["required_parameters"]
#                 if p in PARAM_TO_TABLE_MAP
#             ]
#             console.print(
#                 f"  📋 Tool '{tool_hint}' DB params (from cache): {db_params}"
#             )
#             return db_params

#     meta = _get_tool_meta(tool_hint)
#     if meta:
#         db_params = [
#             p for p in meta["required_parameters"]
#             if p in PARAM_TO_TABLE_MAP
#         ]
#         console.print(
#             f"  📋 Tool '{tool_hint}' DB params (from Chroma): {db_params}"
#         )
#         return db_params

#     console.print(f"  Tool '{tool_hint}' not found in registry — treating as no-param")
#     return []


# # def _resolve_entity_codes(
# #     name:            str,
# #     scheme_name:     Optional[str],
# #     amc_name:        Optional[str],
# #     nse_symbol:      Optional[str],
# #     required_params: list[str],
# # ) -> dict:
# #     """
# #     Resolve all DB codes needed by the target tool for a single entity.
# #     """
# #     codes:  dict = {}
# #     result: dict = {"name": name}

# #     needs_schcode  = "mf_schcode"  in required_params
# #     needs_cocode   = "mf_cocode"   in required_params
# #     needs_co_code  = "co_code"     in required_params
# #     needs_schcodes = "mf_schcodes" in required_params

# #     # ── mf_schcode / mf_schcodes ──────────────────────────────────────────
# #     if needs_schcode or needs_schcodes:
# #         search = scheme_name or name
# #         cfg    = PARAM_TO_TABLE_MAP["mf_schcode"]
# #         match  = _targeted_db_lookup(
# #             entity=search, table=cfg["table"],
# #             name_col=cfg["column"], id_col=cfg["id_field"],
# #         )
# #         if match:
# #             param_key = "mf_schcodes" if needs_schcodes else "mf_schcode"
# #             codes[param_key] = int(match["mf_schcode"])
# #             if "mf_cocode" in match:
# #                 codes.setdefault("mf_cocode", int(match["mf_cocode"]))
# #             result["resolved_scheme_name"] = match.get("sch_name", search)
# #             console.print(
# #                 f"  💎 [{name}] mf_schcode={codes[param_key]} "
# #                 f"mf_cocode={codes.get('mf_cocode')}"
# #             )

# #     # ── mf_cocode (only if not already populated from scheme_master) ──────
# #     if needs_cocode and not codes.get("mf_cocode"):
# #         search = amc_name or name
# #         cfg    = PARAM_TO_TABLE_MAP["mf_cocode"]
# #         match  = _targeted_db_lookup(
# #             entity=search, table=cfg["table"],
# #             name_col=cfg["column"], id_col=cfg["id_field"],
# #         )
# #         if match:
# #             codes["mf_cocode"] = int(match["mf_cocode"])
# #             console.print(f"  💎 [{name}] mf_cocode={codes['mf_cocode']}")

# #     # ── co_code ───────────────────────────────────────────────────────────
# #     if needs_co_code:
# #         stock = _resolve_single(name, nse_symbol)
# #         if stock:
# #             codes["co_code"] = stock["co_code"]
# #             result.update({
# #                 "co_code":      stock["co_code"],
# #                 "company_info": stock["company_info"],
# #                 "nse_symbol":   stock["nse_symbol"],
# #             })
# #             console.print(f"  💎 [{name}] co_code={codes['co_code']}")

# #     result["mcp_resolved_codes"] = codes
# #     return result

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

#     needs_schcode   = "mf_schcode"  in required_params
#     needs_cocode    = "mf_cocode"   in required_params
#     needs_co_code   = "co_code"     in required_params
#     needs_schcodes  = "mf_schcodes" in required_params
#     needs_isin      = "isin"        in required_params   # ← NEW
#     needs_indexcode = "index_code"  in required_params   # ← NEW

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

#     # ── isin ──────────────────────────────────────────────────────────────
#     if needs_isin:                                                  # ← NEW
#         search = scheme_name or name
#         cfg    = PARAM_TO_TABLE_MAP["isin"]
#         match  = _targeted_db_lookup(
#             entity=search, table=cfg["table"],
#             name_col=cfg["column"], id_col=cfg["id_field"],
#         )
#         if match:
#             codes["isin"] = match["isin"]          # string — no int cast
#             result["resolved_etf_name"] = match.get("etf_name", search)
#             console.print(f"  💎 [{name}] isin={codes['isin']}")

#     # ── index_code ────────────────────────────────────────────────────────
#     if needs_indexcode:                                             # ← NEW
#         cfg   = PARAM_TO_TABLE_MAP["index_code"]
#         match = _targeted_db_lookup(
#             entity=name, table=cfg["table"],
#             name_col=cfg["column"], id_col=cfg["id_field"],
#         )
#         if match:
#             codes["index_code"] = int(match["indexcode"])   # note: id_field is "indexcode"
#             result["resolved_index_name"] = match.get("group_name", name)
#             console.print(f"  💎 [{name}] index_code={codes['index_code']}")

#     result["mcp_resolved_codes"] = codes
#     return result


# # ═══════════════════════════════════════════════════════════════════════════════
# # MCP Pre-Resolution — single entity
# # ═══════════════════════════════════════════════════════════════════════════════

# def node_mcp_pre_resolve(state: AgentState) -> AgentState:
#     """
#     Resolve DB codes for the primary entity using the dynamic tool registry.
#     """
#     console.print("[Node MPR] Single-entity MCP pre-resolve (v7.0)")

#     entity        = (
#         state.get("mcp_scheme_name")
#         or state.get("mcp_amc_name")
#         or state.get("extracted_entity", "")
#     )
#     tool_hint     = state.get("mcp_tool_hint", "").strip()
#     matched_tools = state.get("matched_tools") or []

#     required_params = _get_required_params_for_tool(tool_hint, matched_tools)

#     if not required_params:
#         console.print(
#             f"  ⚡ Tool '{tool_hint}' needs no DB codes — MCP server resolves from query"
#         )
#         state["mcp_resolved_codes"] = {}
#         return state

#     resolved = _resolve_entity_codes(
#         name            = entity,
#         scheme_name     = state.get("mcp_scheme_name"),
#         amc_name        = state.get("mcp_amc_name"),
#         nse_symbol      = state.get("nse_symbol"),
#         required_params = required_params,
#     )

#     state["mcp_resolved_codes"] = resolved.get("mcp_resolved_codes", {})
#     if resolved.get("resolved_scheme_name"):
#         state["mcp_scheme_name"] = resolved["resolved_scheme_name"]

#     console.print(f"  🏁 Resolved codes: {state['mcp_resolved_codes']}")
#     return state


# # ═══════════════════════════════════════════════════════════════════════════════
# # MCP Pre-Resolution — multi-entity
# # ═══════════════════════════════════════════════════════════════════════════════

# def node_mcp_multi_pre_resolve(state: AgentState) -> AgentState:
#     """
#     Resolve DB codes for ALL entities in a multi-entity MCP query.
#     """
#     console.print("[Node MMPR] Multi-entity MCP pre-resolve (v7.0)")

#     tool_hint       = state.get("mcp_tool_hint", "")
#     matched_tools   = state.get("matched_tools") or []
#     required_params = _get_required_params_for_tool(tool_hint, matched_tools)

#     entities: list[dict] = []
#     primary_name = (
#         state.get("mcp_scheme_name")
#         or state.get("mcp_amc_name")
#         or state.get("extracted_entity", "")
#     )
#     if primary_name:
#         entities.append({
#             "name":        primary_name,
#             "scheme_name": state.get("mcp_scheme_name"),
#             "amc_name":    state.get("mcp_amc_name"),
#             "nse_symbol":  state.get("nse_symbol"),
#         })
#     for c in (state.get("companies") or []):
#         name = c.get("scheme_name") or c.get("name") or c.get("company_name", "")
#         if name:
#             entities.append({
#                 "name":        name,
#                 "scheme_name": c.get("scheme_name"),
#                 "amc_name":    c.get("amc_name"),
#                 "nse_symbol":  c.get("nse_symbol"),
#             })

#     console.print(
#         f"  Entities ({len(entities)}): {[e['name'] for e in entities]}"
#     )

#     def _resolve_one(entry: dict) -> dict:
#         return _resolve_entity_codes(
#             name            = entry["name"],
#             scheme_name     = entry.get("scheme_name"),
#             amc_name        = entry.get("amc_name"),
#             nse_symbol      = entry.get("nse_symbol"),
#             required_params = required_params,
#         )

#     name_to_result: dict[str, dict] = {}
#     with ThreadPoolExecutor(max_workers=min(6, max(len(entities), 1))) as executor:
#         futures = {executor.submit(_resolve_one, e): e["name"] for e in entities}
#         for future in as_completed(futures):
#             name = futures[future]
#             try:
#                 name_to_result[name] = future.result()
#             except Exception as exc:
#                 console.print(f"  Resolution failed for '{name}': {exc}")
#                 name_to_result[name] = {"name": name, "mcp_resolved_codes": {}}

#     resolved_entities = [
#         name_to_result.get(e["name"], {"name": e["name"], "mcp_resolved_codes": {}})
#         for e in entities
#     ]

#     # ── compare_schemes: merge individual mf_schcode → mf_schcodes ────────
#     is_compare = "mf_schcodes" in required_params or tool_hint == "compare_schemes"
#     if is_compare:
#         scheme_codes = []
#         for r in resolved_entities:
#             codes = r.get("mcp_resolved_codes", {})
#             code  = codes.get("mf_schcodes") or codes.get("mf_schcode")
#             if code:
#                 scheme_codes.append(str(code))
#         if scheme_codes:
#             merged_codes = {"mf_schcodes": ",".join(scheme_codes)}
#             state["mcp_resolved_codes"] = merged_codes
#             console.print(f"  🔗 compare_schemes mf_schcodes={merged_codes['mf_schcodes']}")
#     else:
#         if resolved_entities:
#             primary = resolved_entities[0]
#             state["mcp_resolved_codes"] = primary.get("mcp_resolved_codes", {})
#             if primary.get("co_code"):
#                 state["co_code"] = primary["co_code"]

#     state["companies"] = resolved_entities
#     console.print(
#         "  Final resolved:\n"
#         + "\n".join(
#             f"    [{r['name']}] → {r.get('mcp_resolved_codes', {})}"
#             for r in resolved_entities
#         )
#     )
#     return state


# # ═══════════════════════════════════════════════════════════════════════════════
# # Injection block builder
# # ═══════════════════════════════════════════════════════════════════════════════

# # def _build_injection_block(state: AgentState) -> str:
# #     """
# #     Build the <PRE_RESOLVED> XML block prepended to the enriched MCP query.
# #     """
# #     companies = [
# #         c for c in (state.get("companies") or [])
# #         if c.get("mcp_resolved_codes")
# #     ]

# #     if len(companies) >= 2:
# #         primary_codes = state.get("mcp_resolved_codes") or {}
# #         if "mf_schcodes" in primary_codes:
# #             lines = [f"mf_schcodes={primary_codes['mf_schcodes']}"]
# #             return "<PRE_RESOLVED>\n" + "\n".join(lines) + "\n</PRE_RESOLVED>"

# #         lines: list[str] = []
# #         for i, c in enumerate(companies, start=1):
# #             codes = c["mcp_resolved_codes"]
# #             label = (
# #                 c.get("resolved_scheme_name")
# #                 or c.get("scheme_name")
# #                 or c.get("name", f"Entity {i}")
# #             )
# #             lines.append(f"Entity {i}: {label}")
# #             for k, v in codes.items():
# #                 lines.append(f"  {k}={v}")
# #         if lines:
# #             return "<PRE_RESOLVED>\n" + "\n".join(lines) + "\n</PRE_RESOLVED>"
# #         return ""

# #     resolved_codes = state.get("mcp_resolved_codes") or {}
# #     scheme_name    = state.get("mcp_scheme_name", "")
# #     amc_name       = state.get("mcp_amc_name", "")

# #     code_lines: list[str] = []
# #     for k, v in resolved_codes.items():
# #         label = ""
# #         if k in ("mf_schcode", "mf_schcodes") and scheme_name:
# #             label = f" (scheme: {scheme_name})"
# #         elif k == "mf_cocode" and amc_name:
# #             label = f" (AMC: {amc_name})"
# #         code_lines.append(f"{k}={v}{label}")

# #     if code_lines:
# #         return "<PRE_RESOLVED>\n" + "\n".join(code_lines) + "\n</PRE_RESOLVED>"
# #     return ""

# # def _build_injection_block(state: AgentState) -> str:
# #     """
# #     Build the <PRE_RESOLVED> XML block prepended to the enriched MCP query.
# #     Always includes human-readable names so the MCP server can resolve
# #     even when numeric codes are missing.
# #     """
# #     companies = [
# #         c for c in (state.get("companies") or [])
# #         if c.get("mcp_resolved_codes") or c.get("name") or c.get("scheme_name")
# #     ]

# #     # ── Multi-entity path ────────────────────────────────────────────────────
# #     if len(companies) >= 2:
# #         primary_codes = state.get("mcp_resolved_codes") or {}
# #         if "mf_schcodes" in primary_codes:
# #             lines = [f"mf_schcodes={primary_codes['mf_schcodes']}"]
# #             return "<PRE_RESOLVED>\n" + "\n".join(lines) + "\n</PRE_RESOLVED>"

# #         lines: list[str] = []
# #         for i, c in enumerate(companies, start=1):
# #             codes = c.get("mcp_resolved_codes") or {}
# #             label = (
# #                 c.get("resolved_scheme_name")
# #                 or c.get("scheme_name")
# #                 or c.get("name", f"Entity {i}")
# #             )
# #             amc   = c.get("amc_name") or c.get("mf_coname") or c.get("company_name") or ""

# #             lines.append(f"Entity {i}: {label}")
# #             if amc:
# #                 lines.append(f"  amc_name={amc}")             # ← NEW: always emit name
# #             for k, v in codes.items():
# #                 lines.append(f"  {k}={v}")

# #         if lines:
# #             return "<PRE_RESOLVED>\n" + "\n".join(lines) + "\n</PRE_RESOLVED>"
# #         return ""

# #     # ── Single-entity path ───────────────────────────────────────────────────
# #     resolved_codes = state.get("mcp_resolved_codes") or {}
# #     scheme_name    = state.get("mcp_scheme_name", "") or state.get("mcp_entity", "")
# #     amc_name       = state.get("mcp_amc_name", "")

# #     lines: list[str] = []

# #     # Always emit names first — MCP server can use these even without codes
# #     if scheme_name:
# #         lines.append(f"scheme_name={scheme_name}")             # ← NEW
# #     if amc_name:
# #         lines.append(f"amc_name={amc_name}")                   # ← NEW

# #     # Then emit codes (with inline labels for readability)
# #     for k, v in resolved_codes.items():
# #         label = ""
# #         if k in ("mf_schcode", "mf_schcodes") and scheme_name:
# #             label = f" (scheme: {scheme_name})"
# #         elif k == "mf_cocode" and amc_name:
# #             label = f" (AMC: {amc_name})"
# #         lines.append(f"{k}={v}{label}")

# #     if lines:
# #         return "<PRE_RESOLVED>\n" + "\n".join(lines) + "\n</PRE_RESOLVED>"
# #     return ""

# def _build_injection_block(state: AgentState) -> str:
#     """
#     Build the <PRE_RESOLVED> XML block prepended to the enriched MCP query.
#     Codes are emitted first (MCP server prefers direct lookup).
#     Names follow as fallback for when codes are absent or resolution fails.
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
#             amc = c.get("amc_name") or c.get("mf_coname") or c.get("company_name") or ""

#             lines.append(f"Entity {i}: {label}")
#             # Codes first — direct lookup, no resolution needed
#             for k, v in codes.items():
#                 lines.append(f"  {k}={v}")
#             # Names as fallback
#             if amc:
#                 lines.append(f"  amc_name={amc}")

#         if lines:
#             return "<PRE_RESOLVED>\n" + "\n".join(lines) + "\n</PRE_RESOLVED>"
#         return ""

#     # ── Single-entity path ───────────────────────────────────────────────────
#     resolved_codes = state.get("mcp_resolved_codes") or {}
#     scheme_name    = state.get("mcp_scheme_name", "") or state.get("mcp_entity", "")
#     amc_name       = state.get("mcp_amc_name", "")

#     lines: list[str] = []

#     # Codes first — MCP server uses these directly, no resolve_mf_scheme call needed
#     for k, v in resolved_codes.items():
#         label = ""
#         if k in ("mf_schcode", "mf_schcodes") and scheme_name:
#             label = f" (scheme: {scheme_name})"
#         elif k == "mf_cocode" and amc_name:
#             label = f" (AMC: {amc_name})"
#         lines.append(f"{k}={v}{label}")

#     # Names as fallback — used only when codes are missing or resolution fails
#     if scheme_name:
#         lines.append(f"scheme_name={scheme_name}")
#     if amc_name:
#         lines.append(f"amc_name={amc_name}")

#     if lines:
#         return "<PRE_RESOLVED>\n" + "\n".join(lines) + "\n</PRE_RESOLVED>"
#     return ""

# # ═════════════════════
# # ══════════════════════════════════════════════════════════
# # MCP Tool Call Node
# # ═══════════════════════════════════════════════════════════════════════════════

# def _run_mcp_in_new_loop(enriched_query: str) -> str:
#     from mcp_client import run_mcp_query

#     loop = asyncio.new_event_loop()
#     asyncio.set_event_loop(loop)
#     try:
#         return loop.run_until_complete(run_mcp_query(enriched_query))
#     except ExceptionGroup as eg:
#         sub_errors = [str(e) for e in eg.exceptions]
#         console.print(f"MCP TaskGroup Error: {', '.join(sub_errors)}")
#         return f"MCP Error: {sub_errors[0]}"
#     except Exception as e:
#         console.print(f"Internal MCP Task Error: {e}")
#         return f"Error: {str(e)}"
#     finally:
#         try:
#             loop.run_until_complete(loop.shutdown_asyncgens())
#             for task in asyncio.all_tasks(loop):
#                 task.cancel()
#         except Exception:
#             pass
#         loop.close()


# def node_mcp_tool_call(state: AgentState) -> AgentState:
#     console.print("[Node MTC] MCP tool call (v7.0)")

#     try:
#         from mcp_client import run_mcp_query  # noqa: F401
#     except ImportError:
#         console.print("MCP client module not found.")
#         state["mcp_raw_result"] = ""
#         state["error"] = "mcp_client_not_found"
#         return state

#     user_query      = state.get("user_query", "")
#     tool_hint       = state.get("mcp_tool_hint", "")
#     injection_block = _build_injection_block(state)

#     if injection_block:
#         enriched_query = f"{injection_block}\nUser Query: {user_query}"
#     else:
#         enriched_query = user_query

#     if tool_hint:
#         enriched_query += f"\n\n[RECOMMENDED TOOL: {tool_hint}]"

#     console.print(f"  📤 Sending ({len(enriched_query)} chars):\n{enriched_query[:500]}")

#     try:
#         with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
#             future = executor.submit(_run_mcp_in_new_loop, enriched_query)
#             result = future.result(timeout=600)

#         state["mcp_raw_result"]      = result or "No data returned from server."
#         state["mcp_tool_calls_made"] = []
#         state["error"]               = None
#         console.print(f"  📥 Received {len(state['mcp_raw_result'])} chars")

#     except concurrent.futures.TimeoutError:
#         console.print("  🚨 MCP call timed out")
#         state["mcp_raw_result"] = ""
#         state["error"]          = "mcp_call_timeout"
#     except Exception as e:
#         console.print(f"  💥 MCP call failed: {e}")
#         state["mcp_raw_result"] = ""
#         state["error"]          = f"mcp_call_failed: {e}"

#     return state


# # ── MCP Synthesis ─────────────────────────────────────────────────────────────

# MCP_SYNTHESIS_PROMPT = """\
# You are a knowledgeable Indian equity and mutual fund analyst assistant.

# ## Conversation history (last 4 turns)
# {history}

# ## User Question
# {user_query}

# ## Live Data from MCP Server
# {mcp_result}

# ## Instructions
# 1. Answer the user's question using the live data above.
# 2. Write in plain prose paragraphs only. No markdown, no bullet points,
#    no asterisks, no tables. Do NOT use the rupee symbol — write "Rs" instead.
# 3. Present key numbers naturally woven into sentences.
# 4. Be honest if specific data fields are missing or unavailable.
# 5. Use Indian number formatting (Rs Cr for large numbers).
# 6. Keep under 250 words unless detail is explicitly requested.
# 7. End with: This is not financial advice.
# """

# _MCP_ERROR_MESSAGES = {
#     "mcp_client_not_found": (
#         "Sorry, the live data service is currently unavailable. "
#         "Please try again shortly."
#     ),
#     "mcp_call_timeout": (
#         "The live data request timed out. The server may be busy — "
#         "please try again in a moment."
#     ),
# }

# def node_mcp_synthesis(state: AgentState) -> AgentState:
#     console.print("[Node MS] MCP synthesis")
#     error      = state.get("error", "")
#     mcp_result = state.get("mcp_raw_result", "")

#     if error in _MCP_ERROR_MESSAGES:
#         state["final_answer"] = _MCP_ERROR_MESSAGES[error]
#         return state
#     if error and error.startswith("mcp_call_failed"):
#         state["final_answer"] = (
#             "The live data query encountered an error. "
#             "Please try again or rephrase your question."
#         )
#         return state
#     if not mcp_result:
#         state["final_answer"] = (
#             "No data was returned from the live server. Please try again shortly."
#         )
#         return state

#     prompt = MCP_SYNTHESIS_PROMPT.format(
#         history    = _format_history(state),
#         user_query = state.get("user_query", ""),
#         mcp_result = mcp_result[:3000],
#     )
#     try:
#         state["final_answer"] = _llm_text(prompt)
#     except Exception as e:
#         console.print(f"  MCP synthesis failed: {e}")
#         state["final_answer"] = mcp_result
#     return state


# # ── General synthesis (for general/concept queries) ───────────────────────────

# GENERAL_SYNTHESIS_PROMPT = """\
# You are a knowledgeable Indian equity analyst assistant.

# ## Conversation history (last 4 turns)
# {history}

# ## User Question
# {user_query}

# ## Knowledge Base Context
# {vector_context}

# ## Instructions
# 1. Answer clearly and concisely.
# 2. Plain prose only — no markdown. Do NOT use the rupee symbol — write "Rs".
# 3. Simple language; Indian market examples where useful.
# 4. Answer from general knowledge if context doesn't cover the topic.
# 5. Under 200 words.
# """

# def node_synthesis(state: AgentState) -> AgentState:
#     console.print("[Node S] General synthesis")
#     vector_context = state.get("vector_context") or []
#     context_text   = "\n".join(
#         f"- [{r['metadata'].get('api_name', '')}] {r['document']}"
#         for r in vector_context
#     ) or "No relevant context found."
#     prompt = GENERAL_SYNTHESIS_PROMPT.format(
#         history        = _format_history(state),
#         user_query     = state["user_query"],
#         vector_context = context_text,
#     )
#     try:
#         state["final_answer"] = _llm_text(prompt)
#     except Exception as e:
#         state["final_answer"] = f"Synthesis failed: {e}"
#     return state


# # ═══════════════════════════════════════════════════════════════════════════════
# # Routers
# # ═══════════════════════════════════════════════════════════════════════════════

# def route_after_classify(state: AgentState) -> str:
#     qt  = state.get("query_type", "general")
#     mcp = state.get("mcp_needed", False)

#     if qt == "greeting":
#         return "greeting"

#     if qt == "general" and not mcp:
#         return "general"

#     # Everything else — company, comparison, investment, or general with a
#     # matched tool — goes through MCP. Multi-entity types get the multi resolver.
#     if qt in ("comparison", "investment"):
#         return "multi_mcp"

#     return "mcp_direct"


# # ═══════════════════════════════════════════════════════════════════════════════
# # Build graph
# # ═══════════════════════════════════════════════════════════════════════════════

# def build_graph() -> Any:
#     g = StateGraph(AgentState)

#     g.add_node("classify_and_extract",  node_classify_and_extract)
#     g.add_node("greeting_handler",      node_greeting_handler)
#     g.add_node("general_handler",       node_general_handler)
#     g.add_node("mcp_pre_resolve",       node_mcp_pre_resolve)
#     g.add_node("mcp_multi_pre_resolve", node_mcp_multi_pre_resolve)
#     g.add_node("mcp_tool_call",         node_mcp_tool_call)
#     g.add_node("mcp_synthesis",         node_mcp_synthesis)
#     g.add_node("synthesis",             node_synthesis)

#     g.set_entry_point("classify_and_extract")

#     g.add_conditional_edges(
#         "classify_and_extract",
#         route_after_classify,
#         {
#             "greeting":   "greeting_handler",
#             "general":    "general_handler",
#             "mcp_direct": "mcp_pre_resolve",
#             "multi_mcp":  "mcp_multi_pre_resolve",
#         },
#     )

#     g.add_edge("greeting_handler",      END)
#     g.add_edge("general_handler",       "synthesis")
#     g.add_edge("synthesis",             END)

#     g.add_edge("mcp_pre_resolve",       "mcp_tool_call")
#     g.add_edge("mcp_multi_pre_resolve", "mcp_tool_call")
#     g.add_edge("mcp_tool_call",         "mcp_synthesis")
#     g.add_edge("mcp_synthesis",         END)

#     return g.compile()


# # ── Singleton ─────────────────────────────────────────────────────────────────

# _graph = None

# def get_graph():
#     global _graph
#     if _graph is None:
#         _graph = build_graph()
#     return _graph


# # ── Public entrypoint ─────────────────────────────────────────────────────────

# def run_query(
#     user_query:           str,
#     conversation_history: Optional[list[Turn]] = None,
#     force_refresh:        bool = False,
# ) -> tuple[str, list[Turn]]:
#     history = conversation_history or []
#     result  = get_graph().invoke(
#         {
#             "user_query":           user_query,
#             "conversation_history": history,
#             "force_refresh":        force_refresh,
#         }
#     )
#     answer = _strip_markdown(result.get("final_answer", "No answer generated."))
#     return answer, history + [
#         {"role": "user",      "content": user_query},
#         {"role": "assistant", "content": answer},
#     ]





# """
# graph.py – EQUIFIZ Financial AI Agent (v8.2)

# Key changes from v8.1
# ───────────────────────
# 1. FIX 4 — route_after_classify now checks has_many (len(companies) > 0)
#    before checking query_type. This ensures that any query with multiple
#    entities — even if the LLM classifies it as "investment" or "company"
#    instead of "comparison" — still routes to multi_mcp. Previously a
#    mixed stock+MF query classified as "investment" would route to
#    mcp_direct (single-entity path), silently dropping all additional
#    entities.

# All other pipeline logic is identical to v8.1.
# """
# from __future__ import annotations

# import asyncio
# import concurrent.futures
# import json
# import logging
# import re
# import uuid
# from concurrent.futures import ThreadPoolExecutor, as_completed
# from datetime import date
# from typing import Any, Optional, TypedDict

# import psycopg2
# import psycopg2.extras
# from rapidfuzz import fuzz

# from langchain_core.messages import HumanMessage
# from langchain_ollama import ChatOllama
# from langgraph.graph import END, StateGraph

# import db
# import vector_store as vs
# from config.config import OLLAMA_BASE_URL, OLLAMA_MODEL
# from rich.console import Console

# console = Console()
# logger  = logging.getLogger(__name__)
# TODAY   = str(date.today())


# # ── LLM ───────────────────────────────────────────────────────────────────────

# llm = ChatOllama(base_url=OLLAMA_BASE_URL, model=OLLAMA_MODEL)


# # ── Conversation turn helper ──────────────────────────────────────────────────

# class Turn(TypedDict):
#     role: str       # "user" | "assistant"
#     content: str


# # ── DB routing: param name → Postgres table ───────────────────────────────────

# PARAM_TO_TABLE_MAP: dict[str, dict[str, str]] = {
#     "mf_schcode": {
#         "table":    "scheme_master",
#         "column":   "sch_name",
#         "id_field": "mf_schcode",
#     },
#     "mf_cocode": {
#         "table":    "fund_house",
#         "column":   "lname",
#         "id_field": "mf_cocode",
#     },
#     "co_code": {
#         "table":    "companies",
#         "column":   "companyname",
#         "id_field": "co_code",
#     },
#     "isin": {
#         "table":    "etf_master",
#         "column":   "etfname",
#         "id_field": "isin",
#     },
#     "index_code": {
#         "table":    "group_master",
#         "column":   "group_name",
#         "id_field": "indexcode",
#     },
# }

# # ── entity_type → guaranteed required DB params ───────────────────────────────

# ENTITY_TYPE_PARAM_MAP: dict[str, list[str]] = {
#     "stock":     ["co_code"],
#     "mf_scheme": ["mf_schcode"],
#     "mf_amc":    ["mf_cocode"],
#     "etf":       ["isin"],
#     "index":     ["index_code"],
#     "general":   [],
# }

# # ── DB config ─────────────────────────────────────────────────────────────────

# DB_CONFIG = {
#     "host":     "localhost",
#     "port":     5432,
#     "dbname":   "equifiz",
#     "user":     "postgres",
#     "password": "1234",
# }

# TOOL_SCORE_THRESHOLD = 0.55


# # ═══════════════════════════════════════════════════════════════════════════════
# # Chroma Tool Registry
# # ═══════════════════════════════════════════════════════════════════════════════

# def _query_tool_registry(
#     query: str,
#     n_results: int = 6,
#     score_threshold: float = TOOL_SCORE_THRESHOLD,
# ) -> list[dict]:
#     try:
#         import chromadb
#         client     = chromadb.PersistentClient(path="./chroma_db")
#         collection = client.get_collection(name="equifiz_tools")
#         count      = collection.count()
#         if count == 0:
#             return []

#         res = collection.query(
#             query_texts=[query],
#             n_results=min(n_results, count),
#             include=["metadatas", "distances"],
#         )

#         if not (res["ids"] and res["ids"][0]):
#             return []

#         tools: list[dict] = []
#         for tool_id, meta, dist in zip(
#             res["ids"][0], res["metadatas"][0], res["distances"][0]
#         ):
#             if dist > score_threshold:
#                 continue
#             raw_req = meta.get("required_parameters", "") or ""
#             req_params = [p.strip() for p in raw_req.split(",") if p.strip()]
#             tools.append({
#                 "tool_name":           meta.get("name", tool_id),
#                 "description":         meta.get("description", ""),
#                 "required_parameters": req_params,
#                 "parameters":          meta.get("parameters", ""),
#                 "score":               round(1 - dist, 3),
#             })

#         console.print(
#             f"  [ToolRegistry] '{query[:60]}' → {len(tools)} tools matched "
#             f"(threshold={score_threshold})"
#         )
#         return tools

#     except Exception as e:
#         console.print(f"  [ToolRegistry] Chroma query failed: {e}")
#         return []


# def _get_tool_meta(tool_name: str) -> Optional[dict]:
#     try:
#         import chromadb
#         client     = chromadb.PersistentClient(path="./chroma_db")
#         collection = client.get_collection(name="equifiz_tools")
#         res        = collection.get(ids=[tool_name], include=["metadatas"])
#         if res["ids"]:
#             meta       = res["metadatas"][0]
#             raw_req    = meta.get("required_parameters", "") or ""
#             req_params = [p.strip() for p in raw_req.split(",") if p.strip()]
#             return {
#                 "tool_name":           meta.get("name", tool_name),
#                 "description":         meta.get("description", ""),
#                 "required_parameters": req_params,
#                 "parameters":          meta.get("parameters", ""),
#             }
#     except Exception as e:
#         console.print(f"  [ToolRegistry] get by id '{tool_name}' failed: {e}")
#     return None


# def _build_tool_candidates_block(tools: list[dict]) -> str:
#     if not tools:
#         return "(no tools matched for this query)"
#     lines = []
#     for t in tools:
#         req = ", ".join(t["required_parameters"]) if t["required_parameters"] else "none"
#         lines.append(
#             f"- {t['tool_name']} (score={t['score']}): {t['description'][:120]}"
#             f"\n    requires: {req}"
#         )
#     return "\n".join(lines)


# # ── Agent State ───────────────────────────────────────────────────────────────

# class AgentState(TypedDict, total=False):
#     # Input
#     user_query:           str
#     conversation_history: list[Turn]
#     force_refresh:        bool

#     # Classification
#     query_type:       str
#     extracted_entity: str
#     report_type:      str
#     intent:           str
#     is_broad:         bool
#     mcp_needed:       bool
#     mcp_tool_hint:    str

#     # Matched tool candidates (set in Node A)
#     matched_tools: list[dict]

#     # Symbol resolution (kept for general_handler / vector path only)
#     nse_symbol:   Optional[str]
#     co_code:      Optional[int]
#     company_info: Optional[dict]

#     # MCP-specific resolved codes (primary entity)
#     mcp_resolved_codes: dict
#     mcp_scheme_name:    Optional[str]
#     mcp_amc_name:       Optional[str]

#     # Multi-entity list
#     companies: list[dict]

#     # ChromaDB path (general handler only)
#     vector_context: list[dict]

#     # MCP path output
#     mcp_raw_result:      str
#     mcp_tool_calls_made: list[str]

#     # Output
#     final_answer: str
#     error:        Optional[str]


# # ── LLM helpers ───────────────────────────────────────────────────────────────

# def _llm_json(prompt: str) -> dict:
#     resp = llm.invoke([HumanMessage(content=prompt)])
#     text = resp.content.strip()
#     text = re.sub(r"^```(?:json)?|```$", "", text, flags=re.MULTILINE).strip()
#     try:
#         return json.loads(text)
#     except json.JSONDecodeError:
#         match = re.search(r"\{.*\}", text, re.DOTALL)
#         if match:
#             return json.loads(match.group())
#         raise ValueError(f"Could not parse JSON from LLM: {text[:300]}")


# def _llm_text(prompt: str) -> str:
#     resp = llm.invoke([HumanMessage(content=prompt)])
#     return resp.content.strip()


# def _format_history(state: AgentState, n: int = 8) -> str:
#     history = state.get("conversation_history") or []
#     return "\n".join(
#         f"{t['role'].upper()}: {t['content']}" for t in history[-n:]
#     ) or "(no prior conversation)"


# def _strip_markdown(text: str) -> str:
#     lines = text.splitlines()
#     lines = [l for l in lines if not re.match(r"^\s*\|", l)]
#     text  = "\n".join(lines)
#     text  = text.replace("₹", "Rs ")
#     text  = re.sub(r"\*{1,3}(.+?)\*{1,3}", r"\1", text, flags=re.DOTALL)
#     text  = re.sub(r"_{1,3}(.+?)_{1,3}", r"\1", text, flags=re.DOTALL)
#     text  = re.sub(r"(?m)^[ \t]*(?:[-*#]+|[0-9]+\.)[ \t]+", "", text)
#     text  = re.sub(r"\n{3,}", "\n\n", text)
#     return text.strip()


# # ═══════════════════════════════════════════════════════════════════════════════
# # Node A: Classify + Extract
# # ═══════════════════════════════════════════════════════════════════════════════

# _BROAD_KEYWORDS = {
#     "full analysis", "complete analysis", "full report", "complete report",
#     "deep dive", "detailed analysis", "everything about", "all financials",
#     "comprehensive", "full picture",
# }
# # ── Updated Node A Logic ──────────────────────────────────────────────────────

# CLASSIFY_AND_EXTRACT_PROMPT = """\
# You are a financial query analyzer for an Indian stock-market chatbot.

# ## Conversation history
# {history}

# ## Current user query
# {query}

# ## Available MCP tools
# {tool_candidates}

# Return ONLY a valid JSON object:
# {{
#   "query_type": "<greeting|general|company|comparison|investment>",
#   "is_broad": <true|false>,
#   "mcp_needed": <true|false>,
#   "mcp_tool_hint": "<tool for primary entity>",
#   "primary": {{
#     "entity_type": "<stock|mf_scheme|mf_amc|etf|index>",
#     "company_name": "<canonical name>",
#     "nse_symbol": "<symbol>",
#     "scheme_name": "<scheme name>",
#     "amc_name": "<amc name>",
#     "tool_hint": "<specific tool for THIS entity>",
#     "query_intent": "<intent>"
#   }},
#   "additional_companies": [
#     {{
#       "entity_type": "<stock|mf_scheme|mf_amc|etf|index>",
#       "company_name": "<name>",
#       "tool_hint": "<specific tool for THIS entity>",
#       "query_intent": "<intent>"
#     }}
#   ]
# }}

# STRICT ENTITY ALIGNMENT RULES:
# 1. RELIANCE IS A STOCK: "Reliance" or "Reliance Industries" is ALWAYS entity_type="stock". 
#    - NEVER assign tools like 'get_scheme_ratios' or 'get_expense_ratio' to it.
#    - Use 'get_company_ratios' or 'get_stock_price' if available.
# 2. SBI MUTUAL FUND: "SBI Mutual Fund" or "schemes under SBI" is ALWAYS entity_type="mf_amc".
# 3. MIXED ENTITIES: If the query contains both a stock and a mutual fund, query_type MUST be "comparison".
# 4. PARAMETER MATCHING: 
#    - entity_type="stock" requires tools that take 'co_code'.
#    - entity_type="mf_scheme" requires tools that take 'mf_schcode'.
#    - entity_type="mf_amc" requires tools that take 'mf_cocode'.
# 5. NO HALLUCINATION: If a valid tool for the entity_type is not in the 'Available MCP tools' list, set tool_hint to "".
# """

# def node_classify_and_extract(state: AgentState) -> AgentState:
#     console.print("[Node A] Classify + extract (v8.2.1 - Strict Alignment)")

#     history_text = _format_history(state, n=10)
#     user_query = state.get("user_query", "")
    
#     # 1. Semantic Search for tools
#     matched_tools = _query_tool_registry(user_query, n_results=8)
#     state["matched_tools"] = matched_tools
#     tool_candidates_block = _build_tool_candidates_block(matched_tools)

#     try:
#         # 2. LLM Classification
#         result = _llm_json(
#             CLASSIFY_AND_EXTRACT_PROMPT.format(
#                 history=history_text,
#                 query=user_query,
#                 tool_candidates=tool_candidates_block,
#             )
#         )

#         state["query_type"] = result.get("query_type", "general")
#         state["mcp_needed"] = result.get("mcp_needed", False) or (len(matched_tools) > 0)
        
#         # 3. Process Primary Entity
#         primary = result.get("primary", {})
#         state["primary_entity_type"] = primary.get("entity_type", "general")
#         state["primary_tool_hint"] = primary.get("tool_hint", "")
#         state["extracted_entity"] = primary.get("company_name") or primary.get("scheme_name") or user_query
#         state["nse_symbol"] = primary.get("nse_symbol")
#         state["mcp_scheme_name"] = primary.get("scheme_name")
#         state["mcp_amc_name"] = primary.get("amc_name")

#         # 4. Process Additional Entities (The "Reliance and SBI" fix)
#         additional = result.get("additional_companies") or []
#         state["companies"] = []
#         for c in additional:
#             state["companies"].append({
#                 "name": c.get("company_name") or c.get("scheme_name") or "",
#                 "entity_type": c.get("entity_type", "general"),
#                 "tool_hint": c.get("tool_hint", ""),
#                 "query_intent": c.get("query_intent", ""),
#                 "scheme_name": c.get("scheme_name"),
#                 "amc_name": c.get("amc_name")
#             })

#         # 5. Global Hint Logic
#         state["mcp_tool_hint"] = state["primary_tool_hint"] or (matched_tools[0]["tool_name"] if matched_tools else "")

#         console.print(f"  → Routing: type={state['query_type']} | entities={1 + len(state['companies'])}")

#     except Exception as e:
#         console.print(f"  [ERROR] Node A failed: {e}")
#         state["query_type"] = "general"
#         state["mcp_needed"] = False

#     return state


# # ── Node B: Greeting handler ──────────────────────────────────────────────────

# GREETING_PROMPT = """\
# You are EQUIFIZ — a friendly Indian stock-market AI assistant.
# Respond warmly and briefly to the user's greeting or small talk.
# Mention 1-2 things you can help with (PE ratios, quarterly results,
# investment recommendations, live NAV, MF returns, IPO listings, etc.).

# User: {query}
# """

# def node_greeting_handler(state: AgentState) -> AgentState:
#     console.print("[Node B] Greeting handler")
#     state["final_answer"] = _llm_text(
#         GREETING_PROMPT.format(query=state["user_query"])
#     )
#     return state


# # ── Node C: General handler ───────────────────────────────────────────────────

# def node_general_handler(state: AgentState) -> AgentState:
#     console.print("[Node C] General handler — vector search only")
#     try:
#         state["vector_context"] = vs.query(state["user_query"], n_results=5)
#     except Exception as e:
#         console.print(f"  General vector search failed: {e}")
#         state["vector_context"] = []
#     return state


# # ── Symbol resolution ─────────────────────────────────────────────────────────

# def _resolve_single(name: str, nse_symbol: Optional[str]) -> dict:
#     if nse_symbol:
#         row = db.lookup_by_nse_symbol(nse_symbol)
#         if row:
#             return {
#                 "co_code":      row["co_code"],
#                 "company_info": row,
#                 "nse_symbol":   nse_symbol,
#             }
#     matches = db.fuzzy_search_company(name, limit=1)
#     if matches:
#         best = matches[0]
#         return {
#             "co_code":      best["co_code"],
#             "company_info": best,
#             "nse_symbol":   best.get("nsesymbol"),
#         }
#     return {}


# # ═══════════════════════════════════════════════════════════════════════════════
# # DB Helpers
# # ═══════════════════════════════════════════════════════════════════════════════

# def _targeted_db_lookup(
#     entity:   str,
#     table:    str,
#     name_col: str,
#     id_col:   str,
# ) -> Optional[dict]:
#     try:
#         conn = psycopg2.connect(**DB_CONFIG)
#         cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
#         console.print(f"  🔍 DB lookup: table='{table}' entity='{entity}'")

#         if table == "scheme_master":
#             cur.execute(
#                 "SELECT mf_schcode, mf_cocode, sch_name, category FROM scheme_master"
#             )
#         else:
#             cur.execute(f"SELECT {id_col}, {name_col} FROM {table}")

#         rows = [dict(r) for r in cur.fetchall()]
#         cur.close()
#         conn.close()

#         best_match: Optional[dict] = None
#         best_score: int            = 0
#         el = entity.lower()

#         for row in rows:
#             candidate = (row.get(name_col) or "").lower()
#             score = max(
#                 fuzz.token_set_ratio(el, candidate),
#                 fuzz.partial_ratio(el, candidate),
#             )
#             if score > best_score:
#                 best_score, best_match = score, row

#         if best_match and best_score >= 60:
#             console.print(
#                 f"  ✅ match='{best_match.get(name_col)}' "
#                 f"id={best_match.get(id_col)} score={best_score}"
#             )
#             return best_match

#         console.print(f"  No confident match in {table} (best={best_score})")
#         return None

#     except Exception as e:
#         console.print(f"  [_targeted_db_lookup] DB error on {table}: {e}")
#         return None


# # ═══════════════════════════════════════════════════════════════════════════════
# # Required params resolver — entity_type is AUTHORITATIVE
# # ═══════════════════════════════════════════════════════════════════════════════

# def _get_required_params_for_entity(
#     tool_hint:     str,
#     entity_type:   str,
#     matched_tools: list[dict],
# ) -> list[str]:
#     # Step 1: known entity_type → authoritative params
#     if entity_type and entity_type != "general":
#         params = ENTITY_TYPE_PARAM_MAP.get(entity_type, [])
#         console.print(
#             f"  📋 entity_type='{entity_type}' → authoritative params: {params}"
#         )
#         return params

#     # Step 2: entity_type unknown → use tool_hint
#     if tool_hint:
#         for t in matched_tools:
#             if t["tool_name"] == tool_hint:
#                 db_params = [p for p in t["required_parameters"] if p in PARAM_TO_TABLE_MAP]
#                 if db_params:
#                     console.print(
#                         f"  📋 Tool '{tool_hint}' DB params (cache): {db_params}"
#                     )
#                     return db_params
#                 break

#         meta = _get_tool_meta(tool_hint)
#         if meta:
#             db_params = [p for p in meta["required_parameters"] if p in PARAM_TO_TABLE_MAP]
#             if db_params:
#                 console.print(
#                     f"  📋 Tool '{tool_hint}' DB params (Chroma): {db_params}"
#                 )
#                 return db_params

#     console.print(f"  📋 entity_type='{entity_type}' — no params resolved, skipping DB lookup")
#     return []


# # ═══════════════════════════════════════════════════════════════════════════════
# # Core per-entity resolver
# # ═══════════════════════════════════════════════════════════════════════════════

# def _resolve_entity_codes(
#     name:            str,
#     scheme_name:     Optional[str],
#     amc_name:        Optional[str],
#     nse_symbol:      Optional[str],
#     required_params: list[str],
# ) -> dict:
#     codes:  dict = {}
#     result: dict = {"name": name}

#     needs_schcode   = "mf_schcode"  in required_params
#     needs_cocode    = "mf_cocode"   in required_params
#     needs_co_code   = "co_code"     in required_params
#     needs_schcodes  = "mf_schcodes" in required_params
#     needs_isin      = "isin"        in required_params
#     needs_indexcode = "index_code"  in required_params

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

#     if needs_cocode and not codes.get("mf_cocode"):
#         search = amc_name or name
#         cfg    = PARAM_TO_TABLE_MAP["mf_cocode"]
#         match  = _targeted_db_lookup(
#             entity=search, table=cfg["table"],
#             name_col=cfg["column"], id_col=cfg["id_field"],
#         )
#         if match:
#             codes["mf_cocode"] = int(match["mf_cocode"])
#             result["resolved_amc_name"] = match.get("lname", search)
#             console.print(f"  💎 [{name}] mf_cocode={codes['mf_cocode']}")

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

#     if needs_isin:
#         search = scheme_name or name
#         cfg    = PARAM_TO_TABLE_MAP["isin"]
#         match  = _targeted_db_lookup(
#             entity=search, table=cfg["table"],
#             name_col=cfg["column"], id_col=cfg["id_field"],
#         )
#         if match:
#             codes["isin"] = match["isin"]
#             result["resolved_etf_name"] = match.get("etfname", search)
#             console.print(f"  💎 [{name}] isin={codes['isin']}")

#     if needs_indexcode:
#         cfg   = PARAM_TO_TABLE_MAP["index_code"]
#         match = _targeted_db_lookup(
#             entity=name, table=cfg["table"],
#             name_col=cfg["column"], id_col=cfg["id_field"],
#         )
#         if match:
#             codes["index_code"] = int(match["indexcode"])
#             result["resolved_index_name"] = match.get("group_name", name)
#             console.print(f"  💎 [{name}] index_code={codes['index_code']}")

#     result["mcp_resolved_codes"] = codes
#     return result


# # ═══════════════════════════════════════════════════════════════════════════════
# # MCP Pre-Resolution — single entity
# # ═══════════════════════════════════════════════════════════════════════════════

# def node_mcp_pre_resolve(state: AgentState) -> AgentState:
#     console.print("[Node MPR] Single-entity MCP pre-resolve (v8.2)")

#     entity       = (
#         state.get("mcp_scheme_name")
#         or state.get("mcp_amc_name")
#         or state.get("extracted_entity", "")
#     )
#     tool_hint    = state.get("primary_tool_hint") or state.get("mcp_tool_hint", "")
#     entity_type  = state.get("primary_entity_type", "general")
#     matched_tools = state.get("matched_tools") or []

#     required_params = _get_required_params_for_entity(tool_hint, entity_type, matched_tools)

#     if not required_params:
#         console.print(f"  ⚡ entity_type='{entity_type}' needs no DB codes")
#         state["mcp_resolved_codes"] = {}
#         return state

#     resolved = _resolve_entity_codes(
#         name            = entity,
#         scheme_name     = state.get("mcp_scheme_name"),
#         amc_name        = state.get("mcp_amc_name"),
#         nse_symbol      = state.get("nse_symbol"),
#         required_params = required_params,
#     )

#     state["mcp_resolved_codes"] = resolved.get("mcp_resolved_codes", {})
#     if resolved.get("resolved_scheme_name"):
#         state["mcp_scheme_name"] = resolved["resolved_scheme_name"]

#     console.print(f"  🏁 Resolved codes: {state['mcp_resolved_codes']}")
#     return state


# # ═══════════════════════════════════════════════════════════════════════════════
# # MCP Pre-Resolution — multi-entity
# # ═══════════════════════════════════════════════════════════════════════════════
# def node_mcp_multi_pre_resolve(state: AgentState) -> AgentState:
#     console.print("[Node MMPR] Multi-entity MCP pre-resolve (v8.2.1 - Heterogeneous)")

#     matched_tools = state.get("matched_tools") or []
#     entities: list[dict] = []

#     # 1. Collect all entities
#     primary_name = (
#         state.get("mcp_scheme_name")
#         or state.get("mcp_amc_name")
#         or state.get("extracted_entity", "")
#     )
#     if primary_name:
#         entities.append({
#             "name":         primary_name,
#             "scheme_name":  state.get("mcp_scheme_name"),
#             "amc_name":     state.get("mcp_amc_name"),
#             "nse_symbol":   state.get("nse_symbol"),
#             "entity_type":  state.get("primary_entity_type", "general"),
#             "tool_hint":    state.get("primary_tool_hint") or state.get("mcp_tool_hint", ""),
#             "query_intent": state.get("primary_query_intent", ""),
#         })

#     for c in (state.get("companies") or []):
#         name = c.get("scheme_name") or c.get("name") or c.get("company_name", "")
#         if name:
#             entities.append({
#                 "name":         name,
#                 "scheme_name":  c.get("scheme_name"),
#                 "amc_name":     c.get("amc_name"),
#                 "nse_symbol":   c.get("nse_symbol"),
#                 "entity_type":  c.get("entity_type", "general"),
#                 "tool_hint":    c.get("tool_hint", ""),
#                 "query_intent": c.get("query_intent", ""),
#             })

#     # 2. Parallel Resolution Logic
#     def _resolve_one(entry: dict) -> dict:
#         # Override 'general' type if the tool hint clearly belongs to a specific category
#         e_type = entry["entity_type"]
#         hint = entry["tool_hint"].lower()
        
#         if e_type == "general":
#             if "scheme" in hint or "mf" in hint:
#                 e_type = "mf_scheme"
#             elif "company" in hint or "stock" in hint:
#                 e_type = "stock"

#         req_params = _get_required_params_for_entity(
#             tool_hint=entry["tool_hint"],
#             entity_type=e_type,
#             matched_tools=matched_tools,
#         )
        
#         resolved = _resolve_entity_codes(
#             name=entry["name"],
#             scheme_name=entry.get("scheme_name"),
#             amc_name=entry.get("amc_name"),
#             nse_symbol=entry.get("nse_symbol"),
#             required_params=req_params,
#         )
#         resolved["entity_type"]  = e_type
#         resolved["tool_hint"]    = entry["tool_hint"]
#         resolved["query_intent"] = entry["query_intent"]
#         return resolved

#     # 3. Execution
#     name_to_result: dict[str, dict] = {}
#     with ThreadPoolExecutor(max_workers=min(6, len(entities) or 1)) as executor:
#         futures = {executor.submit(_resolve_one, e): e["name"] for e in entities}
#         for future in as_completed(futures):
#             name = futures[future]
#             name_to_result[name] = future.result()

#     resolved_entities = [name_to_result.get(e["name"]) for e in entities if e["name"] in name_to_result]
#     state["companies"] = resolved_entities

#     # Update primary codes from the first successful resolution
#     if resolved_entities:
#         for r in resolved_entities:
#             if r.get("mcp_resolved_codes"):
#                 state["mcp_resolved_codes"] = r["mcp_resolved_codes"]
#                 if r.get("co_code"):
#                     state["co_code"] = r["co_code"]
#                 break

#     return state


# # ═══════════════════════════════════════════════════════════════════════════════
# # Injection block builder
# # ═══════════════════════════════════════════════════════════════════════════════

# def _build_injection_block(state: AgentState) -> str:
#     companies = [
#         c for c in (state.get("companies") or [])
#         if c.get("mcp_resolved_codes") or c.get("name") or c.get("scheme_name")
#     ]

#     if len(companies) >= 2:
#         primary_codes = state.get("mcp_resolved_codes") or {}
#         if "mf_schcodes" in primary_codes:
#             lines = [f"mf_schcodes={primary_codes['mf_schcodes']}"]
#             return "<PRE_RESOLVED>\n" + "\n".join(lines) + "\n</PRE_RESOLVED>"

#         lines: list[str] = []
#         for i, c in enumerate(companies, start=1):
#             codes        = c.get("mcp_resolved_codes") or {}
#             entity_type  = c.get("entity_type", "general")
#             query_intent = c.get("query_intent", "")
#             tool_hint    = c.get("tool_hint", "")
#             raw_name     = c.get("name", f"Entity {i}")
#             resolved_label = (
#                 c.get("resolved_scheme_name")
#                 or c.get("resolved_amc_name")
#                 or c.get("scheme_name")
#                 or raw_name
#             )
#             amc = c.get("amc_name") or ""

#             lines.append(f"Entity {i}: {resolved_label}")
#             lines.append(f"  entity_type={entity_type}")
#             if query_intent:
#                 lines.append(f"  query_intent={query_intent}")
#             if tool_hint:
#                 lines.append(f"  recommended_tool={tool_hint}")

#             for k, v in codes.items():
#                 lines.append(f"  {k}={v}")

#             if entity_type == "stock" and "co_code" not in codes:
#                 lines.append(f"  company_name={raw_name}")
#             elif entity_type == "mf_amc" and amc and "mf_cocode" not in codes:
#                 lines.append(f"  amc_name={amc}")
#             elif entity_type == "mf_amc" and amc:
#                 lines.append(f"  amc_name={amc}")

#         if lines:
#             return "<PRE_RESOLVED>\n" + "\n".join(lines) + "\n</PRE_RESOLVED>"
#         return ""

#     resolved_codes = state.get("mcp_resolved_codes") or {}
#     scheme_name    = state.get("mcp_scheme_name", "") or state.get("mcp_entity", "")
#     amc_name       = state.get("mcp_amc_name", "")
#     entity_type    = state.get("primary_entity_type", "")
#     query_intent   = state.get("primary_query_intent", "")

#     lines: list[str] = []
#     if entity_type:
#         lines.append(f"entity_type={entity_type}")
#     if query_intent:
#         lines.append(f"query_intent={query_intent}")

#     for k, v in resolved_codes.items():
#         label = ""
#         if k in ("mf_schcode", "mf_schcodes") and scheme_name:
#             label = f" (scheme: {scheme_name})"
#         elif k == "mf_cocode" and amc_name:
#             label = f" (AMC: {amc_name})"
#         lines.append(f"{k}={v}{label}")

#     if scheme_name:
#         lines.append(f"scheme_name={scheme_name}")
#     if amc_name:
#         lines.append(f"amc_name={amc_name}")

#     if lines:
#         return "<PRE_RESOLVED>\n" + "\n".join(lines) + "\n</PRE_RESOLVED>"
#     return ""


# # ═══════════════════════════════════════════════════════════════════════════════
# # MCP Tool Call Node
# # ═══════════════════════════════════════════════════════════════════════════════

# def _run_mcp_in_new_loop(enriched_query: str) -> str:
#     from mcp_client import run_mcp_query

#     loop = asyncio.new_event_loop()
#     asyncio.set_event_loop(loop)
#     try:
#         return loop.run_until_complete(run_mcp_query(enriched_query))
#     except ExceptionGroup as eg:
#         sub_errors = [str(e) for e in eg.exceptions]
#         console.print(f"MCP TaskGroup Error: {', '.join(sub_errors)}")
#         return f"MCP Error: {sub_errors[0]}"
#     except Exception as e:
#         console.print(f"Internal MCP Task Error: {e}")
#         return f"Error: {str(e)}"
#     finally:
#         try:
#             loop.run_until_complete(loop.shutdown_asyncgens())
#             for task in asyncio.all_tasks(loop):
#                 task.cancel()
#         except Exception:
#             pass
#         loop.close()


# def node_mcp_tool_call(state: AgentState) -> AgentState:
#     console.print("[Node MTC] MCP tool call (v8.2)")

#     try:
#         from mcp_client import run_mcp_query  # noqa: F401
#     except ImportError:
#         console.print("MCP client module not found.")
#         state["mcp_raw_result"] = ""
#         state["error"] = "mcp_client_not_found"
#         return state

#     user_query      = state.get("user_query", "")
#     tool_hint       = state.get("mcp_tool_hint", "")
#     injection_block = _build_injection_block(state)

#     if injection_block:
#         enriched_query = f"{injection_block}\nUser Query: {user_query}"
#     else:
#         enriched_query = user_query

#     if tool_hint:
#         enriched_query += f"\n\n[RECOMMENDED TOOL: {tool_hint}]"

#     console.print(f"  📤 Sending ({len(enriched_query)} chars):\n{enriched_query[:600]}")

#     try:
#         with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
#             future = executor.submit(_run_mcp_in_new_loop, enriched_query)
#             result = future.result(timeout=600)

#         state["mcp_raw_result"]      = result or "No data returned from server."
#         state["mcp_tool_calls_made"] = []
#         state["error"]               = None
#         console.print(f"  📥 Received {len(state['mcp_raw_result'])} chars")

#     except concurrent.futures.TimeoutError:
#         console.print("  🚨 MCP call timed out")
#         state["mcp_raw_result"] = ""
#         state["error"]          = "mcp_call_timeout"
#     except Exception as e:
#         console.print(f"  💥 MCP call failed: {e}")
#         state["mcp_raw_result"] = ""
#         state["error"]          = f"mcp_call_failed: {e}"

#     return state


# # ── MCP Synthesis ─────────────────────────────────────────────────────────────

# MCP_SYNTHESIS_PROMPT = """\
# You are a knowledgeable Indian equity and mutual fund analyst assistant.

# ## Conversation history (last 4 turns)
# {history}

# ## User Question
# {user_query}

# ## Live Data from MCP Server
# {mcp_result}

# ## Instructions
# 1. Answer the user's question using the live data above.
# 2. Write in plain prose paragraphs only. No markdown, no bullet points,
#    no asterisks, no tables. Do NOT use the rupee symbol — write "Rs" instead.
# 3. Present key numbers naturally woven into sentences.
# 4. Be honest if specific data fields are missing or unavailable.
# 5. Use Indian number formatting (Rs Cr for large numbers).
# 6. Keep under 250 words unless detail is explicitly requested.
# 7. End with: This is not financial advice.
# """

# _MCP_ERROR_MESSAGES = {
#     "mcp_client_not_found": (
#         "Sorry, the live data service is currently unavailable. "
#         "Please try again shortly."
#     ),
#     "mcp_call_timeout": (
#         "The live data request timed out. The server may be busy — "
#         "please try again in a moment."
#     ),
# }

# def node_mcp_synthesis(state: AgentState) -> AgentState:
#     console.print("[Node MS] MCP synthesis")
#     error      = state.get("error", "")
#     mcp_result = state.get("mcp_raw_result", "")

#     if error in _MCP_ERROR_MESSAGES:
#         state["final_answer"] = _MCP_ERROR_MESSAGES[error]
#         return state
#     if error and error.startswith("mcp_call_failed"):
#         state["final_answer"] = (
#             "The live data query encountered an error. "
#             "Please try again or rephrase your question."
#         )
#         return state
#     if not mcp_result:
#         state["final_answer"] = (
#             "No data was returned from the live server. Please try again shortly."
#         )
#         return state

#     prompt = MCP_SYNTHESIS_PROMPT.format(
#         history    = _format_history(state),
#         user_query = state.get("user_query", ""),
#         mcp_result = mcp_result[:3000],
#     )
#     try:
#         state["final_answer"] = _llm_text(prompt)
#     except Exception as e:
#         console.print(f"  MCP synthesis failed: {e}")
#         state["final_answer"] = mcp_result
#     return state


# # ── General synthesis ─────────────────────────────────────────────────────────

# GENERAL_SYNTHESIS_PROMPT = """\
# You are a knowledgeable Indian equity analyst assistant.

# ## Conversation history (last 4 turns)
# {history}

# ## User Question
# {user_query}

# ## Knowledge Base Context
# {vector_context}

# ## Instructions
# 1. Answer clearly and concisely.
# 2. Plain prose only — no markdown. Do NOT use the rupee symbol — write "Rs".
# 3. Simple language; Indian market examples where useful.
# 4. Answer from general knowledge if context doesn't cover the topic.
# 5. Under 200 words.
# """

# def node_synthesis(state: AgentState) -> AgentState:
#     console.print("[Node S] General synthesis")
#     vector_context = state.get("vector_context") or []
#     context_text   = "\n".join(
#         f"- [{r['metadata'].get('api_name', '')}] {r['document']}"
#         for r in vector_context
#     ) or "No relevant context found."
#     prompt = GENERAL_SYNTHESIS_PROMPT.format(
#         history        = _format_history(state),
#         user_query     = state["user_query"],
#         vector_context = context_text,
#     )
#     try:
#         state["final_answer"] = _llm_text(prompt)
#     except Exception as e:
#         state["final_answer"] = f"Synthesis failed: {e}"
#     return state


# # ═══════════════════════════════════════════════════════════════════════════════
# # Router — v8.2: has_many check prevents investment/company queries with
# # multiple entities from being silently routed to the single-entity path
# # ═══════════════════════════════════════════════════════════════════════════════

# def route_after_classify(state: AgentState) -> str:
#     qt       = state.get("query_type", "general")
#     mcp      = state.get("mcp_needed", False)
#     # FIX 4: check for additional entities regardless of query_type label
#     has_many = len(state.get("companies") or []) > 0

#     if qt == "greeting":
#         return "greeting"

#     if qt == "general" and not mcp:
#         return "general"

#     # Route to multi_mcp whenever there are additional entities — even if
#     # the LLM classified the query as "investment" or "company" instead of
#     # "comparison". The LLM sometimes mis-labels mixed queries.
#     if has_many or qt == "comparison":
#         return "multi_mcp"

#     # Single entity with MCP tool matched
#     return "mcp_direct"


# # ═══════════════════════════════════════════════════════════════════════════════
# # Build graph
# # ═══════════════════════════════════════════════════════════════════════════════

# def build_graph() -> Any:
#     g = StateGraph(AgentState)

#     g.add_node("classify_and_extract",  node_classify_and_extract)
#     g.add_node("greeting_handler",      node_greeting_handler)
#     g.add_node("general_handler",       node_general_handler)
#     g.add_node("mcp_pre_resolve",       node_mcp_pre_resolve)
#     g.add_node("mcp_multi_pre_resolve", node_mcp_multi_pre_resolve)
#     g.add_node("mcp_tool_call",         node_mcp_tool_call)
#     g.add_node("mcp_synthesis",         node_mcp_synthesis)
#     g.add_node("synthesis",             node_synthesis)

#     g.set_entry_point("classify_and_extract")

#     g.add_conditional_edges(
#         "classify_and_extract",
#         route_after_classify,
#         {
#             "greeting":   "greeting_handler",
#             "general":    "general_handler",
#             "mcp_direct": "mcp_pre_resolve",
#             "multi_mcp":  "mcp_multi_pre_resolve",
#         },
#     )

#     g.add_edge("greeting_handler",      END)
#     g.add_edge("general_handler",       "synthesis")
#     g.add_edge("synthesis",             END)

#     g.add_edge("mcp_pre_resolve",       "mcp_tool_call")
#     g.add_edge("mcp_multi_pre_resolve", "mcp_tool_call")
#     g.add_edge("mcp_tool_call",         "mcp_synthesis")
#     g.add_edge("mcp_synthesis",         END)

#     return g.compile()


# # ── Singleton ─────────────────────────────────────────────────────────────────

# _graph = None

# def get_graph():
#     global _graph
#     if _graph is None:
#         _graph = build_graph()
#     return _graph


# # ── Public entrypoint ─────────────────────────────────────────────────────────

# def run_query(
#     user_query:           str,
#     conversation_history: Optional[list[Turn]] = None,
#     force_refresh:        bool = False,
# ) -> tuple[str, list[Turn]]:
#     history = conversation_history or []
#     result  = get_graph().invoke(
#         {
#             "user_query":           user_query,
#             "conversation_history": history,
#             "force_refresh":        force_refresh,
#         }
#     )
#     answer = _strip_markdown(result.get("final_answer", "No answer generated."))
#     return answer, history + [
#         {"role": "user",      "content": user_query},
#         {"role": "assistant", "content": answer},
#     ]



"""
graph.py — EQUIFIZ Financial AI Agent (v9.0)

Key changes from v8.2
─────────────────────────────────────────────────────────────────────
Aligned with mcp_client.py v4.0 (dynamic param discovery).

1. CLASSIFY PROMPT — strict entity-type/tool-family alignment enforced
   in the LLM prompt. Stock entities must get stock tools, MF scheme
   entities must get MF scheme tools. The prompt now lists explicit
   allowed-tool families per entity_type so the LLM cannot mix them.

2. ENTITY TYPE INFERENCE — node_classify_and_extract infers entity_type
   from the resolved tool_hint when the LLM returns "general". Uses
   _TOOL_FAMILY_HINTS dict (tool_name prefix → entity_type) so no
   hardcoded tool name lists are needed.

3. HETEROGENEOUS DETECTION — route_after_classify detects when
   primary + additional entities are DIFFERENT families (stock vs
   mf_scheme etc.) and forces "multi_mcp" routing even when the LLM
   labels the query_type as "investment" or "company".

4. _resolve_one IMPROVED — in node_mcp_multi_pre_resolve, entity_type
   inference now uses _infer_entity_type_from_tool() which checks the
   tool name against family-prefix maps. Falls back to fuzzy DB search
   across both company and scheme caches.

5. _build_injection_block IMPROVED — always emits "recommended_tool"
   and entity-family comment lines so mcp_client's find_entity_for_tool
   can use label_hint AND recommended_tool for precise code binding.

6. PRIMARY ENTITY added to companies list for multi-entity resolution
   so node_mcp_multi_pre_resolve processes ALL entities (primary +
   additional) in one parallel pass, then splits them back.

7. AgentState — added "primary_query_intent" field (was referenced in
   v8.x but never declared).

All v8.2 logic retained where unchanged.
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


# ── Tool family classification ────────────────────────────────────────────────
# Maps tool name prefixes/patterns → entity family.
# Used to infer entity_type when LLM returns "general".
# Extend this dict as new tools are added to the server.

_TOOL_FAMILY_HINTS: dict[str, str] = {
    # Stock / equity tools
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
    # IPO tools (resolve to stock/co_code)
    "get_forthcoming_ipo":   "stock",
    "get_open_ipos":         "stock",
    "get_closed_ipos":       "stock",
    "get_new_ipo":           "stock",
    "get_best_ipo":          "stock",
    "get_ipo_":              "stock",
    "get_anchor_investor":   "stock",
    "get_basis_of":          "stock",
    # MF scheme tools
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
    # MF AMC tools
    "get_fund_categories":   "mf_amc",
    "get_schemes_by_amc":    "mf_amc",
    "get_fund_profile":      "mf_amc",
    # ETF tools
    "get_etf_":              "etf",
    # Index tools
    "get_index_":            "index",
}


def _infer_entity_type_from_tool(tool_hint: str) -> str:
    """
    Return the entity family for a given tool name using prefix matching.
    Returns "general" if no match found.
    """
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

# entity_type → DB params it requires
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
        console.print(tools)
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
    primary_query_intent: str   # ← was missing in v8.x

    # Matched tool candidates
    matched_tools: list[dict]

    # Symbol resolution (general / vector path)
    nse_symbol:   Optional[str]
    co_code:      Optional[int]
    company_info: Optional[dict]

    # MCP-specific resolved codes (primary entity)
    mcp_resolved_codes: dict
    mcp_scheme_name:    Optional[str]
    mcp_amc_name:       Optional[str]

    # Multi-entity list (includes ALL entities after multi_pre_resolve)
    companies: list[dict]

    # ChromaDB path
    vector_context: list[dict]

    # MCP path output
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
# Node A: Classify + Extract (v9.0)
# ══════════════════════════════════════════════════════════════════════════════

CLASSIFY_AND_EXTRACT_PROMPT = """\
You are a financial query analyzer for an Indian stock-market chatbot.

## Conversation history
{history}

## Current user query
{query}

## Available MCP tools (with required params)
{tool_candidates}

Return ONLY a valid JSON object with this exact structure:
{{
  "query_type": "<greeting|general|company|comparison|investment>",
  "is_broad": <true|false>,
  "mcp_needed": <true|false>,
  "mcp_tool_hint": "<tool for primary entity>",
  "primary": {{
    "entity_type": "<stock|mf_scheme|mf_amc|etf|index>",
    "company_name": "<canonical name or empty>",
    "nse_symbol": "<symbol or empty>",
    "scheme_name": "<scheme name or empty>",
    "amc_name": "<amc name or empty>",
    "tool_hint": "<specific tool for THIS entity>",
    "query_intent": "<what data is needed>"
  }},
  "additional_companies": [
    {{
      "entity_type": "<stock|mf_scheme|mf_amc|etf|index>",
      "company_name": "<name or empty>",
      "scheme_name": "<name or empty>",
      "amc_name": "<name or empty>",
      "nse_symbol": "<symbol or empty>",
      "tool_hint": "<specific tool for THIS entity>",
      "query_intent": "<what data is needed>"
    }}
  ]
}}

STRICT ENTITY-TO-TOOL ALIGNMENT RULES (MUST follow):
1. entity_type="stock"     → tool MUST require co_code    (e.g. get_daily_ratios, get_quarterly_results)
2. entity_type="mf_scheme" → tool MUST require mf_schcode (e.g. get_scheme_returns, get_mf_holdings)
3. entity_type="mf_amc"    → tool MUST require mf_cocode  (e.g. get_schemes_by_amc, get_fund_categories)
4. entity_type="etf"       → tool MUST require isin        (e.g. get_etf_quotes, get_etf_returns)
5. entity_type="index"     → tool MUST require index_code  (e.g. get_index_companies)
6. NEVER assign a tool from one family to an entity of a different family.
7. "Reliance", "TCS", "HDFC Bank" → always stock. NEVER mf_scheme or mf_amc.
8. "SBI Mutual Fund", "HDFC AMC" → always mf_amc.
9. "Parag Parikh Flexi Cap", "Axis Bluechip" → always mf_scheme.
10. If query has entities from DIFFERENT families → query_type MUST be "comparison".
11. If tool_hint is not in the available tools list above → set tool_hint to "".

ENTITY IDENTIFICATION:
- Extract ALL named companies, funds, schemes from the query.
- First named entity → "primary". Rest → "additional_companies".
- If only one entity → "additional_companies" is empty list [].
"""


def node_classify_and_extract(state: AgentState) -> AgentState:
    console.print("[Node A] Classify + extract (v9.0)")

    history_text = _format_history(state, n=10)
    user_query   = state.get("user_query", "")

    # Semantic search for relevant tools
    matched_tools = _query_tool_registry(user_query, n_results=10)
    state["matched_tools"] = matched_tools
    tool_candidates_block  = _build_tool_candidates_block(matched_tools)

    try:
        result = _llm_json(
            CLASSIFY_AND_EXTRACT_PROMPT.format(
                history        = history_text,
                query          = user_query,
                tool_candidates = tool_candidates_block,
            )
        )

        state["query_type"] = result.get("query_type", "general")
        state["mcp_needed"] = result.get("mcp_needed", False) or bool(matched_tools)

        # ── Primary entity ─────────────────────────────────────────────────
        primary = result.get("primary") or {}

        raw_entity_type = primary.get("entity_type", "general")
        raw_tool_hint   = primary.get("tool_hint", "")

        # If LLM returned "general", try to infer from tool_hint
        if raw_entity_type == "general" and raw_tool_hint:
            raw_entity_type = _infer_entity_type_from_tool(raw_tool_hint)

        state["primary_entity_type"] = raw_entity_type
        state["primary_tool_hint"]   = raw_tool_hint
        state["primary_query_intent"] = primary.get("query_intent", "")
        state["mcp_tool_hint"]        = (
            raw_tool_hint
            or (matched_tools[0]["tool_name"] if matched_tools else "")
        )

        # Entity name fields
        state["extracted_entity"] = (
            primary.get("company_name")
            or primary.get("scheme_name")
            or user_query
        )
        state["nse_symbol"]     = primary.get("nse_symbol") or None
        state["mcp_scheme_name"] = primary.get("scheme_name") or None
        state["mcp_amc_name"]    = primary.get("amc_name") or None

        # ── Additional entities ────────────────────────────────────────────
        additional = result.get("additional_companies") or []
        companies: list[dict] = []

        for c in additional:
            raw_et   = c.get("entity_type", "general")
            raw_hint = c.get("tool_hint", "")

            # Infer entity_type from tool_hint if still general
            if raw_et == "general" and raw_hint:
                raw_et = _infer_entity_type_from_tool(raw_hint)

            companies.append({
                "name":         (
                    c.get("company_name")
                    or c.get("scheme_name")
                    or c.get("amc_name")
                    or ""
                ),
                "entity_type":  raw_et,
                "tool_hint":    raw_hint,
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
        state["query_type"]          = "general"
        state["mcp_needed"]          = False
        state["primary_entity_type"] = "general"
        state["primary_tool_hint"]   = ""
        state["primary_query_intent"] = ""
        state["companies"]           = []

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
    """
    Returns the DB params needed to call this entity's tool.
    entity_type is authoritative when not "general".
    Falls back to tool_hint schema inspection.
    """
    if entity_type and entity_type != "general":
        params = ENTITY_TYPE_PARAM_MAP.get(entity_type, [])
        console.print(
            f"  📋 entity_type='{entity_type}' → authoritative params: {params}"
        )
        return params

    # entity_type unknown — inspect tool schema
    if tool_hint:
        for t in matched_tools:
            if t["tool_name"] == tool_hint:
                db_params = [p for p in t["required_parameters"] if p in PARAM_TO_TABLE_MAP]
                if db_params:
                    console.print(
                        f"  📋 tool_hint='{tool_hint}' DB params (cache): {db_params}"
                    )
                    return db_params
                break

        meta = _get_tool_meta(tool_hint)
        if meta:
            db_params = [p for p in meta["required_parameters"] if p in PARAM_TO_TABLE_MAP]
            if db_params:
                console.print(
                    f"  📋 tool_hint='{tool_hint}' DB params (Chroma): {db_params}"
                )
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
    """
    Looks up DB IDs for a single entity based on required_params.
    Returns a dict including mcp_resolved_codes.
    """
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
            console.print(
                f"  💎 [{name}] mf_schcode={codes['mf_schcode']} "
                f"mf_cocode={codes.get('mf_cocode')}"
            )

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
            console.print(f"  💎 [{name}] mf_cocode={codes['mf_cocode']}")

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
            console.print(f"  💎 [{name}] isin={codes['isin']}")

    if needs_indexcode:
        cfg   = PARAM_TO_TABLE_MAP["index_code"]
        match = _targeted_db_lookup(
            entity=name, table=cfg["table"],
            name_col=cfg["column"], id_col=cfg["id_field"],
        )
        if match:
            codes["index_code"] = int(match["indexcode"])
            result["resolved_index_name"] = match.get("group_name", name)
            console.print(f"  💎 [{name}] index_code={codes['index_code']}")

    result["mcp_resolved_codes"] = codes
    return result


# ══════════════════════════════════════════════════════════════════════════════
# MCP Pre-Resolution — single entity
# ══════════════════════════════════════════════════════════════════════════════

def node_mcp_pre_resolve(state: AgentState) -> AgentState:
    console.print("[Node MPR] Single-entity MCP pre-resolve (v9.0)")

    entity      = (
        state.get("mcp_scheme_name")
        or state.get("mcp_amc_name")
        or state.get("extracted_entity", "")
    )
    tool_hint   = state.get("primary_tool_hint") or state.get("mcp_tool_hint", "")
    entity_type = state.get("primary_entity_type", "general")

    # Strengthen entity_type from tool_hint if still general
    if entity_type == "general" and tool_hint:
        entity_type = _infer_entity_type_from_tool(tool_hint)
        state["primary_entity_type"] = entity_type

    matched_tools   = state.get("matched_tools") or []
    required_params = _get_required_params_for_entity(tool_hint, entity_type, matched_tools)

    if not required_params:
        console.print(f"  ⚡ entity_type='{entity_type}' needs no DB codes")
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


# ══════════════════════════════════════════════════════════════════════════════
# MCP Pre-Resolution — multi-entity (v9.0)
#
# Key change: builds a unified list of ALL entities (primary + additional)
# so they are all resolved in one parallel pass. After resolution, splits
# them back: first entry → primary codes, rest → state["companies"].
# ══════════════════════════════════════════════════════════════════════════════

def node_mcp_multi_pre_resolve(state: AgentState) -> AgentState:
    console.print("[Node MMPR] Multi-entity MCP pre-resolve (v9.0)")

    matched_tools = state.get("matched_tools") or []

    # ── Build unified entity list (primary first) ──────────────────────────
    all_entities: list[dict] = []

    primary_name = (
        state.get("mcp_scheme_name")
        or state.get("mcp_amc_name")
        or state.get("extracted_entity", "")
    )
    primary_type = state.get("primary_entity_type", "general")
    primary_hint = state.get("primary_tool_hint") or state.get("mcp_tool_hint", "")

    # Strengthen primary entity_type from tool_hint
    if primary_type == "general" and primary_hint:
        primary_type = _infer_entity_type_from_tool(primary_hint)
        state["primary_entity_type"] = primary_type

    if primary_name:
        all_entities.append({
            "name":         primary_name,
            "scheme_name":  state.get("mcp_scheme_name"),
            "amc_name":     state.get("mcp_amc_name"),
            "nse_symbol":   state.get("nse_symbol"),
            "entity_type":  primary_type,
            "tool_hint":    primary_hint,
            "query_intent": state.get("primary_query_intent", ""),
            "_is_primary":  True,
        })

    for c in (state.get("companies") or []):
        name = (
            c.get("scheme_name")
            or c.get("name")
            or c.get("company_name", "")
        )
        if not name:
            continue

        raw_type = c.get("entity_type", "general")
        raw_hint = c.get("tool_hint", "")

        # Strengthen from tool_hint
        if raw_type == "general" and raw_hint:
            raw_type = _infer_entity_type_from_tool(raw_hint)

        all_entities.append({
            "name":         name,
            "scheme_name":  c.get("scheme_name"),
            "amc_name":     c.get("amc_name"),
            "nse_symbol":   c.get("nse_symbol"),
            "entity_type":  raw_type,
            "tool_hint":    raw_hint,
            "query_intent": c.get("query_intent", ""),
            "_is_primary":  False,
        })

    console.print(
        f"  Resolving {len(all_entities)} entities: "
        + ", ".join(f"{e['name']}({e['entity_type']})" for e in all_entities)
    )

    # ── Per-entity resolver ────────────────────────────────────────────────
    def _resolve_one(entry: dict) -> dict:
        e_type    = entry["entity_type"]
        tool_hint = entry.get("tool_hint", "")

        # Final inference attempt using fuzzy DB search when still "general"
        if e_type == "general":
            e_type = _infer_entity_type_from_tool(tool_hint)

        if e_type == "general":
            # Last resort: check both company and scheme caches
            name = entry["name"]
            try:
                conn = psycopg2.connect(**DB_CONFIG)
                cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute(
                    "SELECT co_code, companyname FROM companies "
                    "WHERE LOWER(companyname) LIKE %s LIMIT 1",
                    (f"%{name.lower()[:20]}%",)
                )
                row = cur.fetchone()
                if row:
                    e_type = "stock"
                else:
                    cur.execute(
                        "SELECT mf_schcode FROM scheme_master "
                        "WHERE LOWER(sch_name) LIKE %s LIMIT 1",
                        (f"%{name.lower()[:20]}%",)
                    )
                    row = cur.fetchone()
                    if row:
                        e_type = "mf_scheme"
                cur.close()
                conn.close()
            except Exception:
                pass

        req_params = _get_required_params_for_entity(tool_hint, e_type, matched_tools)

        resolved = _resolve_entity_codes(
            name        = entry["name"],
            scheme_name = entry.get("scheme_name"),
            amc_name    = entry.get("amc_name"),
            nse_symbol  = entry.get("nse_symbol"),
            required_params = req_params,
        )
        resolved["entity_type"]  = e_type
        resolved["tool_hint"]    = tool_hint
        resolved["query_intent"] = entry.get("query_intent", "")
        resolved["_is_primary"]  = entry.get("_is_primary", False)
        resolved["scheme_name"]  = entry.get("scheme_name")
        resolved["amc_name"]     = entry.get("amc_name")
        return resolved

    # ── Parallel resolution ────────────────────────────────────────────────
    name_to_result: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=min(6, len(all_entities) or 1)) as executor:
        futures = {
            executor.submit(_resolve_one, e): e["name"]
            for e in all_entities
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                name_to_result[name] = future.result()
            except Exception as ex:
                console.print(f"  [red]Resolution failed for '{name}': {ex}[/red]")

    resolved_all = [
        name_to_result[e["name"]]
        for e in all_entities
        if e["name"] in name_to_result
    ]

    # ── Split primary vs additional ────────────────────────────────────────
    primary_result    = None
    additional_results: list[dict] = []

    for r in resolved_all:
        if r.get("_is_primary"):
            primary_result = r
        else:
            additional_results.append(r)

    # Update primary state
    if primary_result:
        state["mcp_resolved_codes"] = primary_result.get("mcp_resolved_codes", {})
        if primary_result.get("co_code"):
            state["co_code"] = primary_result["co_code"]
        if primary_result.get("resolved_scheme_name"):
            state["mcp_scheme_name"] = primary_result["resolved_scheme_name"]

    # Store ALL resolved entities in companies (including primary)
    # mcp_client needs all of them to build the injection block
    state["companies"] = resolved_all

    console.print(
        f"  🏁 Resolved all {len(resolved_all)} entities. "
        f"Primary codes: {primary_result.get('mcp_resolved_codes') if primary_result else {}}"
    )
    return state


# ══════════════════════════════════════════════════════════════════════════════
# Injection block builder (v9.0)
#
# Always emits:
#   - "Entity N: <label>" for each entity
#   - entity_type, query_intent, recommended_tool
#   - The resolved ID param(s)
#   - A comment line binding the ID to its family
#
# mcp_client v4.0 uses "recommended_tool" for find_entity_for_tool Priority 2.
# The comment lines help during debugging but are stripped by the parser.
# ══════════════════════════════════════════════════════════════════════════════

def _build_injection_block(state: AgentState) -> str:
    companies = state.get("companies") or []

    # ── Multi-entity block ─────────────────────────────────────────────────
    if len(companies) >= 2:
        lines: list[str] = []

        for i, c in enumerate(companies, start=1):
            codes        = c.get("mcp_resolved_codes") or {}
            entity_type  = c.get("entity_type", "general")
            query_intent = c.get("query_intent", "")
            tool_hint    = c.get("tool_hint", "")
            raw_name     = c.get("name", f"Entity {i}")

            # Pick the best display label
            resolved_label = (
                c.get("resolved_scheme_name")
                or c.get("resolved_amc_name")
                or c.get("resolved_etf_name")
                or c.get("scheme_name")
                or raw_name
            )

            lines.append(f"Entity {i}: {resolved_label}")
            lines.append(f"  entity_type={entity_type}")
            if query_intent:
                lines.append(f"  query_intent={query_intent}")
            if tool_hint:
                lines.append(f"  recommended_tool={tool_hint}")

            # Emit each resolved code with a family binding comment
            for param, val in codes.items():
                family_comment = {
                    "co_code":    "stock/equity tools only",
                    "mf_schcode": "MF scheme tools only",
                    "mf_cocode":  "AMC/fund-house tools only",
                    "isin":       "ETF tools only",
                    "index_code": "index tools only",
                }.get(param, "")
                comment = f"  # {family_comment}" if family_comment else ""
                lines.append(f"  {param}={val}{comment}")

            # Fallback name hints for auto-resolver
            amc = c.get("amc_name") or ""
            if entity_type == "stock" and "co_code" not in codes:
                lines.append(f"  company_name={raw_name}")
            elif entity_type in ("mf_amc",) and amc:
                lines.append(f"  amc_name={amc}")
            elif entity_type == "mf_scheme" and "mf_schcode" not in codes:
                sname = c.get("scheme_name") or raw_name
                lines.append(f"  scheme_name={sname}")

        if lines:
            return "<PRE_RESOLVED>\n" + "\n".join(lines) + "\n</PRE_RESOLVED>"
        return ""

    # ── Single-entity block ────────────────────────────────────────────────
    resolved_codes = state.get("mcp_resolved_codes") or {}
    scheme_name    = state.get("mcp_scheme_name", "")
    amc_name       = state.get("mcp_amc_name", "")
    entity_type    = state.get("primary_entity_type", "")
    query_intent   = state.get("primary_query_intent", "")
    tool_hint      = state.get("primary_tool_hint") or state.get("mcp_tool_hint", "")

    lines: list[str] = []
    if entity_type:
        lines.append(f"entity_type={entity_type}")
    if query_intent:
        lines.append(f"query_intent={query_intent}")
    if tool_hint:
        lines.append(f"recommended_tool={tool_hint}")

    for param, val in resolved_codes.items():
        family_comment = {
            "co_code":    "stock/equity tools only",
            "mf_schcode": "MF scheme tools only",
            "mf_cocode":  "AMC/fund-house tools only",
            "isin":       "ETF tools only",
            "index_code": "index tools only",
        }.get(param, "")
        comment = f"  # {family_comment}" if family_comment else ""
        lines.append(f"{param}={val}{comment}")

    if scheme_name:
        lines.append(f"scheme_name={scheme_name}")
    if amc_name:
        lines.append(f"amc_name={amc_name}")

    if lines:
        return "<PRE_RESOLVED>\n" + "\n".join(lines) + "\n</PRE_RESOLVED>"
    return ""


# ══════════════════════════════════════════════════════════════════════════════
# MCP Tool Call Node
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


def node_mcp_tool_call(state: AgentState) -> AgentState:
    console.print("[Node MTC] MCP tool call (v9.0)")

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

    console.print(
        f"  📤 Sending to mcp_client ({len(enriched_query)} chars):\n"
        f"{enriched_query[:600]}"
    )

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
# Router (v9.0)
#
# Forces multi_mcp when:
#   a) there are additional entities (has_many)
#   b) query_type == "comparison"
#   c) primary and at least one additional entity are from DIFFERENT families
#      (is_heterogeneous) — this is the core heterogeneous-query fix
# ══════════════════════════════════════════════════════════════════════════════

def route_after_classify(state: AgentState) -> str:
    qt  = state.get("query_type", "general")
    mcp = state.get("mcp_needed", False)

    additional   = state.get("companies") or []
    has_many     = len(additional) > 0

    primary_type     = state.get("primary_entity_type", "general")
    secondary_types  = {c.get("entity_type", "general") for c in additional}
    # Heterogeneous = secondary entities exist that are a DIFFERENT family
    # than the primary (ignoring "general" which is ambiguous)
    is_heterogeneous = bool(
        secondary_types - {"general"} - {primary_type}
    )

    if qt == "greeting":
        return "greeting"

    if qt == "general" and not mcp:
        return "general"

    # Route to multi_mcp for any of these conditions
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
# Build graph
# ══════════════════════════════════════════════════════════════════════════════

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
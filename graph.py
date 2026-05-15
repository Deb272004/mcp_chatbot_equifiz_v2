

# """
# graph.py — EQUIFIZ Financial AI Agent (v10.2)

# Key changes from v10.1
# ─────────────────────────────────────────────────────────────────────
# 1. LATENCY: Node A (classify+extract) and Node IQ (decompose intents)
#    MERGED into a single LLM call — Node AIQ.
#    - One prompt, one LLM round-trip instead of two.
#    - Entity extraction and intent decomposition happen together.
#    - The old `node_classify_and_extract` and `node_decompose_intents`
#      are replaced by `node_classify_extract_and_decompose`.

# 2. OVER-DECOMPOSITION FIX:
#    - The merged prompt explicitly counts named entities BEFORE generating
#      intents. len(intents) must equal len(named_entities).
#    - A "single entity → single intent" fast-path is enforced via prompt
#      rule: if only ONE entity is found, return exactly ONE intent item.
#    - Negative examples added to the prompt to stop hallucination of
#      extra intents from a single-entity query.

# 3. STATE COMPAT: All downstream nodes (TVS, MMPR, MTCI, MS) unchanged.
#    `state["intents"]` is populated by the merged node exactly as before.

# 4. `node_classify_and_extract` and `node_decompose_intents` are kept as
#    stubs (no-ops that log a deprecation warning) so any external callers
#    don't crash — but the graph no longer routes through them.

# All v10.1 logic retained where unchanged.
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
# from pathlib import Path
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


# # ── Intent item ───────────────────────────────────────────────────────────────

# class IntentItem(TypedDict, total=False):
#     entity:             str
#     entity_type:        str
#     intent_description: str
#     tool_hint:          str
#     scheme_name:        Optional[str]
#     amc_name:           Optional[str]
#     nse_symbol:         Optional[str]
#     matched_tools:      list[dict]
#     resolved_codes:     dict
#     mcp_result:         str


# # ── Tool family classification ────────────────────────────────────────────────

# _TOOL_FAMILY_HINTS: dict[str, str] = {
#     "get_company_stock":     "stock",
#     "get_delayed_stock":     "stock",
#     "get_market_indices":    "index",
#     "get_active_performer":  "stock",
#     "get_top_gainers":       "stock",
#     "get_top_losers":        "stock",
#     "get_out_under":         "stock",
#     "get_advance_decline":   "stock",
#     "get_52week":            "stock",
#     "get_new_highs":         "stock",
#     "get_index_companies":   "index",
#     "get_sector_companies":  "stock",
#     "get_quarterly_results": "stock",
#     "get_profit_loss":       "stock",
#     "get_balance_sheet":     "stock",
#     "get_cash_flow":         "stock",
#     "get_half_yearly":       "stock",
#     "get_nine_months":       "stock",
#     "get_yearly_results":    "stock",
#     "get_quarterly_balance": "stock",
#     "get_annual_balance":    "stock",
#     "get_ttm_growth":        "stock",
#     "get_quarterly_revenue": "stock",
#     "get_quarterly_ebitda":  "stock",
#     "get_quarterly_ebit":    "stock",
#     "get_growth_data":       "stock",
#     "get_key_financial":     "stock",
#     "get_daily_ratios":      "stock",
#     "get_margin_ratios":     "stock",
#     "get_performance_ratio": "stock",
#     "get_cashflow_ratios":   "stock",
#     "get_growth_ratios":     "stock",
#     "get_liquidity_ratios":  "stock",
#     "get_solvency_ratios":   "stock",
#     "get_return_ratios":     "stock",
#     "get_all_basic_ratios":  "stock",
#     "get_quarterly_ratios":  "stock",
#     "get_yearly_ratios":     "stock",
#     "get_valuation_ratios":  "stock",
#     "get_shareholding":      "stock",
#     "get_major_sharehold":   "stock",
#     "get_company_profile":   "stock",
#     "get_company_backgroun": "stock",
#     "get_board_of":          "stock",
#     "get_company_bankers":   "stock",
#     "get_management":        "stock",
#     "get_subsidiaries":      "stock",
#     "get_related_party":     "stock",
#     "get_employee_count":    "stock",
#     "get_capital_structure": "stock",
#     "get_pledge_share":      "stock",
#     "get_chronological":     "stock",
#     "get_company_history":   "stock",
#     "get_substantial":       "stock",
#     "get_segment_data":      "stock",
#     "get_r_and_d":           "stock",
#     "get_finished_products": "stock",
#     "get_raw_materials":     "stock",
#     "get_forthcoming_ipo":   "stock",
#     "get_open_ipos":         "stock",
#     "get_closed_ipos":       "stock",
#     "get_new_ipo":           "stock",
#     "get_best_ipo":          "stock",
#     "get_ipo_":              "stock",
#     "get_anchor_investor":   "stock",
#     "get_basis_of":          "stock",
#     "get_scheme_nav":        "mf_scheme",
#     "get_investment_detail": "mf_scheme",
#     "get_expense_ratio":     "mf_scheme",
#     "get_avg_maturity":      "mf_scheme",
#     "get_scheme_aum":        "mf_scheme",
#     "get_nav_historical":    "mf_scheme",
#     "get_scheme_returns":    "mf_scheme",
#     "get_lumpsum_returns":   "mf_scheme",
#     "get_scheme_sip":        "mf_scheme",
#     "get_mf_holdings":       "mf_scheme",
#     "get_sector_allocation": "mf_scheme",
#     "get_asset_allocation":  "mf_scheme",
#     "get_portfolio_changes": "mf_scheme",
#     "get_mcap_allocation":   "mf_scheme",
#     "get_most_bought":       "mf_scheme",
#     "get_scheme_ratios":     "mf_scheme",
#     "get_dividend_details":  "mf_scheme",
#     "get_bse_star_scheme":   "mf_scheme",
#     "compare_schemes":       "mf_scheme",
#     "get_whats_in_out":      "mf_scheme",
#     "get_fund_categories":   "mf_amc",
#     "get_schemes_by_amc":    "mf_amc",
#     "get_fund_profile":      "mf_amc",
#     "get_etf_":              "etf",
#     "get_index_":            "index",
# }


# def _infer_entity_type_from_tool(tool_hint: str) -> str:
#     if not tool_hint:
#         return "general"
#     tl = tool_hint.lower()
#     for prefix, family in _TOOL_FAMILY_HINTS.items():
#         if tl.startswith(prefix):
#             return family
#     return "general"


# # ── DB routing ────────────────────────────────────────────────────────────────

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

# ENTITY_TYPE_PARAM_MAP: dict[str, list[str]] = {
#     "stock":     ["co_code"],
#     "mf_scheme": ["mf_schcode"],
#     "mf_amc":    ["mf_cocode"],
#     "etf":       ["isin"],
#     "index":     ["index_code"],
#     "general":   [],
# }

# DB_CONFIG = {
#     "host":     "localhost",
#     "port":     5432,
#     "dbname":   "equifiz",
#     "user":     "postgres",
#     "password": "1234",
# }

# TOOL_SCORE_THRESHOLD = 0.55

# # ── Chroma singleton ──────────────────────────────────────────────────────────
# from chroma_singleton import get_chroma_collection
# try:
#     _tool_collection = get_chroma_collection()
#     console.print(f"  [ToolRegistry] Chroma ready — {_tool_collection.count()} tools")
# except Exception as e:
#     console.print(f"  [ToolRegistry] Chroma init failed: {e}")
#     _tool_collection = None


# # ── Agent State ───────────────────────────────────────────────────────────────

# class AgentState(TypedDict, total=False):
#     user_query:           str
#     conversation_history: list[Turn]
#     force_refresh:        bool

#     query_type:            str
#     extracted_entity:      str
#     report_type:           str
#     intent:                str
#     is_broad:              bool
#     mcp_needed:            bool
#     mcp_tool_hint:         str

#     primary_entity_type:  str
#     primary_tool_hint:    str
#     primary_query_intent: str

#     matched_tools: list[dict]
#     intents: list[IntentItem]
#     intent_results: list[dict]

#     nse_symbol:   Optional[str]
#     co_code:      Optional[int]
#     company_info: Optional[dict]

#     mcp_resolved_codes: dict
#     mcp_scheme_name:    Optional[str]
#     mcp_amc_name:       Optional[str]

#     companies: list[dict]

#     vector_context: list[dict]

#     mcp_raw_result:      str
#     mcp_tool_calls_made: list[str]

#     final_answer: str
#     error:        Optional[str]


# # ── LLM helpers ───────────────────────────────────────────────────────────────

# def _llm_json(prompt: str) -> dict:
#     """
#     Robust JSON extractor for LLM responses. 
#     Handles markdown blocks, trailing commas, and single-quote issues.
#     """
#     resp = llm.invoke([HumanMessage(content=prompt)])
#     text = resp.content.strip()

#     # 1. Strip Markdown code blocks if they exist
#     clean_text = re.sub(r"^```(?:json)?|```$", "", text, flags=re.MULTILINE).strip()

#     # 2. Try standard parsing immediately
#     try:
#         return json.loads(clean_text)
#     except json.JSONDecodeError:
#         pass 

#     # 3. Greedy Extraction: Find the outermost curly braces
#     # This ignores preamble like "Here is your JSON:"
#     match = re.search(r"(\{.*\})", clean_text, re.DOTALL)
#     if match:
#         greedy_text = match.group(1)
#         try:
#             return json.loads(greedy_text)
#         except json.JSONDecodeError:
#             # 4. Final Fallbacks for common LLM syntax errors
#             try:
#                 # Replace single quotes with double quotes (common in small models)
#                 # Fix trailing commas before closing braces/brackets
#                 fixed_text = greedy_text.replace("'", '"')
#                 fixed_text = re.sub(r",\s*([\]}])", r"\1", fixed_text) 
#                 return json.loads(fixed_text)
#             except:
#                 pass

#     # If all fails, log the raw text for debugging and raise
#     logger.error(f"[JSON FAIL] Raw LLM Output: {text}")
#     raise ValueError(f"Could not parse JSON from LLM response. Check logs.")


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


# # ══════════════════════════════════════════════════════════════════════════════
# # Node AIQ: Classify + Extract + Decompose  (MERGED — v10.2)
# #
# # Replaces both node_classify_and_extract (Node A) and
# # node_decompose_intents (Node IQ) with a single LLM call.
# #
# # Design rules baked into the prompt:
# #   • Count named entities first → intents array must match that count.
# #   • Single entity  → exactly 1 intent. No splitting by data-type.
# #   • Multiple entities → exactly N intents (one per entity).
# #   • A single entity asking for multiple data points (e.g. "PE and revenue
# #     of Reliance") → still 1 intent; downstream tool-selection handles it.
# # ══════════════════════════════════════════════════════════════════════════════

# # CLASSIFY_EXTRACT_DECOMPOSE_PROMPT = """\
# # You are a financial query analyzer for an Indian stock-market assistant.

# # ## Conversation history (last 4 turns)
# # {history}

# # ## Today's date
# # {today}

# # ## User query
# # "{query}"

# # ────────────────────────────────────────────────────────────
# # STEP 1 — COUNT NAMED ENTITIES
# # List every distinct financial entity named in the query.
# # Do NOT invent entities. Do NOT split one entity into many intents.

# # Examples:
# #   "stock price of Reliance"              → 1 entity: Reliance
# #   "NAV of Parag Parikh and Axis Bluechip"→ 2 entities: Parag Parikh Flexi Cap, Axis Bluechip
# #   "key ratios of Reliance, NAV of SBI Multicap and Axis Bluechip" → 3 entities
# #   "what is PE ratio"                     → 0 entities (general question)

# # STEP 2 — PRODUCE OUTPUT JSON (one intent per entity found in STEP 1)

# # Return ONLY valid JSON:
# # {{
# #   "query_type": "<greeting|general|stock|mf_scheme|mf_amc|comparison|investment>",
# #   "is_broad":   <true|false>,
# #   "mcp_needed": <true|false>,
# #   "intents": [
# #     {{
# #       "entity":             "<name exactly as written in query>",
# #       "entity_type":        "<stock|mf_scheme|mf_amc|etf|index|general>",
# #       "intent_description": "<specific data needed, e.g. 'stock price of Reliance'>",
# #       "scheme_name":        "<if mf_scheme, else empty string>",
# #       "amc_name":           "<if mf_amc, else empty string>",
# #       "nse_symbol":         "<NSE ticker if known, else empty string>"
# #     }}
# #   ]
# # }}

# # ────────────────────────────────────────────────────────────
# # ENTITY TYPE RULES (apply in order, stop at first match):
# # 1. "SBI Mutual Fund", "HDFC AMC", "Mirae Asset", "Nippon AMC", "Kotak MF",
# #    "Axis AMC", "ICICI Prudential AMC", "Franklin Templeton", "DSP"
# #    → entity_type = mf_amc.  NEVER etf.  NEVER mf_scheme.
# # 2. Name ending in "Fund", "Scheme", "Plan", or known scheme names like
# #    "Parag Parikh Flexi Cap", "Axis Bluechip", "SBI Small Cap Fund"
# #    → entity_type = mf_scheme.
# # 3. entity_type = etf ONLY when the word "ETF" appears explicitly in the query.
# # 4. "Reliance", "TCS", "HDFC Bank", "Infosys", "Wipro" → entity_type = stock.
# # 5. "Nifty 50", "Sensex", "Bank Nifty" → entity_type = index.
# # 6. General knowledge questions with no entity → entity_type = general.

# # INTENT COUNT RULES (CRITICAL — violations cause downstream errors):
# # • len(intents) MUST equal the number of distinct entities you found in STEP 1.
# # • If 0 entities → intents = [] and query_type = "general" or "greeting".
# # • If 1 entity  → intents has exactly 1 item. NEVER split into 2.
# # • If N entities → intents has exactly N items, one per entity.
# # • A query asking for multiple data points on ONE entity (e.g. "PE and EPS
# #   of Reliance") → still 1 intent. The tool layer will fetch both fields.
# # • NEVER create an intent for an entity not present in the query.

# # mcp_needed RULES:
# # • true  when any entity is a stock, mf_scheme, mf_amc, or etf.
# # • false for greetings and pure general/educational questions.

# # NEGATIVE EXAMPLES (do NOT do these):
# #   ✗ "stock price of Reliance" → 2 intents (wrong: only 1 entity)
# #   ✗ Inventing "SBI Bluechip ETF" when user said "SBI Mutual Fund"
# #   ✗ Splitting "PE ratio and revenue of TCS" into 2 intents
# # ────────────────────────────────────────────────────────────
# # """


# CLASSIFY_EXTRACT_DECOMPOSE_PROMPT = """\
# You are a Lead Financial Systems Analyst for an Indian stock-market assistant. Your task is to decompose user queries into executable data intents.

# ## Context
# - History: {history}
# - Today's Date: {today}
# - Query: "{query}"

# ────────────────────────────────────────────────────────────
# STEP 1: SUBJECT IDENTIFICATION
# Identify every distinct subject the user is asking about. A subject is either:
# 1. A Specific Entity: (e.g., Reliance Industries, HDFC AMC, Nifty 50, SBI Small Cap Fund).
# 2. A Market Category/Concept: (e.g., "upcoming IPOs", "top gainers", "market news", "bulk deals").

# Example Decomposition:
# - "Price of Reliance and upcoming IPOs" 
#   -> Subject 1: Reliance (Entity)
#   -> Subject 2: Upcoming IPOs (Concept)

# STEP 2: JSON GENERATION
# Generate exactly one intent item for every subject identified in Step 1.

# Return ONLY valid JSON:
# {{
#   "query_type": "<greeting|general|stock|mf_scheme|mf_amc|comparison|investment>",
#   "is_broad": <true|false>,
#   "mcp_needed": <true|false>,
#   "intents": [
#     {{
#       "entity": "<entity name OR market concept name>",
#       "entity_type": "<stock|mf_scheme|mf_amc|etf|index|general>",
#       "intent_description": "<specific data point needed for this subject>",
#       "scheme_name": "<if mf_scheme, else empty>",
#       "amc_name": "<if mf_amc, else empty>",
#       "nse_symbol": "<NSE ticker if known, else empty>"
#     }}
#   ]
# }}

# ────────────────────────────────────────────────────────────
# DECOMPOSITION RULES (CRITICAL):
# 1. THE 1:1 MAPPING: len(intents) MUST equal the number of subjects identified in Step 1. 
# 2. NO BUNDLING: If a query asks for two different subjects (e.g., a stock price and a market trend), you MUST return two separate intent objects.
# 3. ENTITY TYPE ASSIGNMENT:
#    - "Reliance", "TCS", "HDFC Bank" -> entity_type = stock
#    - "SBI Mutual Fund", "Nippon AMC" -> entity_type = mf_amc
#    - "Parag Parikh Flexi Cap", "Axis Bluechip" -> entity_type = mf_scheme
#    - "Nifty 50", "Sensex" -> entity_type = index
#    - Concepts like "IPOs", "Gainers", or "Educational definitions" -> entity_type = general
# 4. DATA POINT MERGING: If a user asks for multiple metrics on the SAME entity (e.g., "PE, EPS, and Revenue of Reliance"), return ONE intent for Reliance. The intent_description should list all metrics.

# MCP_NEEDED LOGIC:
# - Set to true if ANY intent requires fetching data from the live market, database, or external tools (Prices, IPO lists, NAVs, etc.).

# NEGATIVE EXAMPLES (DO NOT DO THESE):
# ✗ User: "Price of Reliance and IPOs" -> 1 intent (Wrong: Market concepts are separate subjects)
# ✗ User: "How is the market today?" -> entity_type = stock (Wrong: This is an index or general query)
# ✗ User: "PE of TCS and EPS of Wipro" -> 1 intent (Wrong: These are two distinct entities)
# ────────────────────────────────────────────────────────────
# """



# def node_classify_extract_and_decompose(state: AgentState) -> AgentState:
#     """
#     Merged Node AIQ (v10.2).
#     Replaces the sequential Node A → Node IQ with a single LLM call.
#     Populates: query_type, mcp_needed, primary_entity_type, intents,
#                extracted_entity, companies (for downstream compat).
#     """
#     console.print("[Node AIQ] Classify + extract + decompose (v10.2 — merged, 1 LLM call)")

#     user_query   = state.get("user_query", "")
#     history_text = _format_history(state, n=4)

#     # ── Defaults ──────────────────────────────────────────────────────────
#     state["matched_tools"] = []
#     state["intents"]       = []
#     state["companies"]     = []

#     try:
#         result = _llm_json(
#             CLASSIFY_EXTRACT_DECOMPOSE_PROMPT.format(
#                 history = history_text,
#                 today   = TODAY,
#                 query   = user_query,
#             )
#         )

#         qt        = result.get("query_type", "general")
#         mcp       = bool(result.get("mcp_needed", False))
#         raw_intents = result.get("intents") or []

#         # ── Build IntentItem list ─────────────────────────────────────────
#         intents: list[IntentItem] = []
#         for item in raw_intents:
#             et = item.get("entity_type", "general")
#             intents.append(IntentItem(
#                 entity             = item.get("entity", ""),
#                 entity_type        = et,
#                 intent_description = item.get("intent_description", ""),
#                 scheme_name        = item.get("scheme_name") or None,
#                 amc_name           = item.get("amc_name") or None,
#                 nse_symbol         = item.get("nse_symbol") or None,
#                 tool_hint          = "",
#                 matched_tools      = [],
#                 resolved_codes     = {},
#                 mcp_result         = "",
#             ))

#         # ── Fallback: if LLM returned no intents for a non-general query ──
#         if not intents and qt not in ("greeting", "general"):
#             console.print("  ⚠ LLM returned no intents for non-general query — fallback")
#             intents.append(IntentItem(
#                 entity             = user_query,
#                 entity_type        = "general",
#                 intent_description = user_query,
#                 scheme_name        = None,
#                 amc_name           = None,
#                 nse_symbol         = None,
#                 tool_hint          = "",
#                 matched_tools      = [],
#                 resolved_codes     = {},
#                 mcp_result         = "",
#             ))

#         state["query_type"]  = qt
#         state["mcp_needed"]  = mcp
#         state["intents"]     = intents

#         # ── Populate legacy fields from first intent ───────────────────────
#         if intents:
#             first = intents[0]
#             state["primary_entity_type"]  = first.get("entity_type", "general")
#             state["primary_query_intent"] = first.get("intent_description", "")
#             state["extracted_entity"]     = first.get("entity", user_query)
#             state["mcp_scheme_name"]      = first.get("scheme_name")
#             state["mcp_amc_name"]         = first.get("amc_name")
#             state["nse_symbol"]           = first.get("nse_symbol")
#         else:
#             state["primary_entity_type"]  = "general"
#             state["primary_query_intent"] = user_query
#             state["extracted_entity"]     = user_query
#             state["mcp_scheme_name"]      = None
#             state["mcp_amc_name"]         = None
#             state["nse_symbol"]           = None

#         # ── Populate companies list (downstream compat) ───────────────────
#         state["companies"] = [
#             {
#                 "name":         i.get("entity", ""),
#                 "entity_type":  i.get("entity_type", "general"),
#                 "tool_hint":    "",
#                 "query_intent": i.get("intent_description", ""),
#                 "scheme_name":  i.get("scheme_name"),
#                 "amc_name":     i.get("amc_name"),
#                 "nse_symbol":   i.get("nse_symbol"),
#             }
#             for i in intents[1:]   # all but primary
#         ]

#         console.print(
#             f"  → type={qt} | mcp={mcp} | {len(intents)} intent(s): "
#             + "; ".join(
#                 f"[{i['entity']} / {i['entity_type']}] {i['intent_description'][:55]}"
#                 for i in intents
#             )
#         )

#     except Exception as e:
#         console.print(f"  [ERROR] Node AIQ failed: {e}")
#         state["query_type"]           = "general"
#         state["mcp_needed"]           = False
#         state["primary_entity_type"]  = "general"
#         state["primary_query_intent"] = user_query
#         state["extracted_entity"]     = user_query
#         state["companies"]            = []
#         state["intents"]              = []

#     return state


# # ── DEPRECATED STUBS (kept for external callers only) ────────────────────────

# def node_classify_and_extract(state: AgentState) -> AgentState:
#     console.print(
#         "[DEPRECATED] node_classify_and_extract called — "
#         "this node is no longer in the graph (merged into Node AIQ v10.2). "
#         "Passing state through unchanged."
#     )
#     return state


# def node_decompose_intents(state: AgentState) -> AgentState:
#     console.print(
#         "[DEPRECATED] node_decompose_intents called — "
#         "this node is no longer in the graph (merged into Node AIQ v10.2). "
#         "Passing state through unchanged."
#     )
#     return state


# # ══════════════════════════════════════════════════════════════════════════════
# # Chroma tool registry helpers (unchanged from v10.1)
# # ══════════════════════════════════════════════════════════════════════════════

# def _query_tool_registry(
#     query: str,
#     n_results: int = 8,
#     score_threshold: float = TOOL_SCORE_THRESHOLD,
# ) -> list[dict]:
#     if _tool_collection is None:
#         console.print("  [ToolRegistry] Collection not available — skipping query")
#         return []
#     try:
#         count = _tool_collection.count()
#         if count == 0:
#             return []

#         res = _tool_collection.query(
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
#             raw_req    = meta.get("required_parameters", "") or ""
#             req_params = [p.strip() for p in raw_req.split(",") if p.strip()]
#             tools.append({
#                 "tool_name":           meta.get("name", tool_id),
#                 "description":         meta.get("description", ""),
#                 "required_parameters": req_params,
#                 "parameters":          meta.get("parameters", ""),
#                 "score":               round(1 - dist, 3),
#             })

#         console.print(
#             f"  [ToolRegistry] '{query[:60]}' → {len(tools)} tools "
#             f"(threshold={score_threshold})"
#         )
#         return tools

#     except Exception as e:
#         console.print(f"  [ToolRegistry] Chroma query failed: {e}")
#         return []


# def _get_tool_meta(tool_name: str) -> Optional[dict]:
#     if _tool_collection is None:
#         return None
#     try:
#         res = _tool_collection.get(ids=[tool_name], include=["metadatas"])
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


# # ══════════════════════════════════════════════════════════════════════════════
# # Node TVS: Per-intent tool vector search (unchanged from v10.1)
# # ══════════════════════════════════════════════════════════════════════════════

# def node_per_intent_tool_search(state: AgentState) -> AgentState:
#     console.print("[Node TVS] Per-intent tool vector search (v10.1)")

#     intents = state.get("intents") or []
#     if not intents:
#         console.print("  ⚠ No intents to search for")
#         return state

#     def _search_for_intent(intent: IntentItem) -> IntentItem:
#         et = intent.get("entity_type", "general")
#         search_query = (
#             f"{et} {intent['intent_description']}"
#             if et not in ("general", "")
#             else intent["intent_description"]
#         )

#         tools = _query_tool_registry(search_query, n_results=8)

#         if et != "general" and tools:
#             family_tools = [
#                 t for t in tools
#                 if _infer_entity_type_from_tool(t["tool_name"]) == et
#             ]
#             if family_tools:
#                 tools = family_tools

#         tool_hint = tools[0]["tool_name"] if tools else ""

#         if tool_hint:
#             console.print(
#                 f"  [{intent['entity']}] "
#                 f"intent='{intent['intent_description'][:50]}' "
#                 f"→ top tool={tool_hint} ({len(tools)} matched)"
#             )
#         else:
#             console.print(
#                 f"  [{intent['entity']}] "
#                 f"intent='{intent['intent_description'][:50]}' "
#                 f"→ no tools matched"
#             )

#         return IntentItem(
#             entity             = intent.get("entity", ""),
#             entity_type        = et,
#             intent_description = intent.get("intent_description", ""),
#             scheme_name        = intent.get("scheme_name"),
#             amc_name           = intent.get("amc_name"),
#             nse_symbol         = intent.get("nse_symbol"),
#             tool_hint          = tool_hint,
#             matched_tools      = tools,
#             resolved_codes     = {},
#             mcp_result         = "",
#         )

#     updated: dict[int, IntentItem] = {}

#     with ThreadPoolExecutor(max_workers=min(6, len(intents))) as ex:
#         futures = {
#             ex.submit(_search_for_intent, intent): idx
#             for idx, intent in enumerate(intents)
#         }
#         for future in as_completed(futures):
#             idx = futures[future]
#             try:
#                 updated[idx] = future.result()
#             except Exception as exc:
#                 console.print(f"  [red]TVS failed for intent #{idx}: {exc}[/red]")
#                 original = intents[idx]
#                 updated[idx] = IntentItem(
#                     entity             = original.get("entity", ""),
#                     entity_type        = original.get("entity_type", "general"),
#                     intent_description = original.get("intent_description", ""),
#                     scheme_name        = original.get("scheme_name"),
#                     amc_name           = original.get("amc_name"),
#                     nse_symbol         = original.get("nse_symbol"),
#                     tool_hint          = "",
#                     matched_tools      = [],
#                     resolved_codes     = {},
#                     mcp_result         = "",
#                 )

#     state["intents"] = [updated[i] for i in sorted(updated)]

#     if state["intents"]:
#         first = state["intents"][0]
#         state["matched_tools"]     = first.get("matched_tools") or []
#         state["mcp_tool_hint"]     = first.get("tool_hint") or ""
#         state["primary_tool_hint"] = first.get("tool_hint") or ""

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
#     console.print("[Node C] General handler")
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


# # ══════════════════════════════════════════════════════════════════════════════
# # DB helpers (unchanged from v10.1)
# # ══════════════════════════════════════════════════════════════════════════════

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

#         console.print(f"  ⚠ No confident match in {table} (best={best_score})")
#         return None

#     except Exception as e:
#         console.print(f"  [_targeted_db_lookup] DB error on {table}: {e}")
#         return None


# # ══════════════════════════════════════════════════════════════════════════════
# # Required params resolver (unchanged from v10.1)
# # ══════════════════════════════════════════════════════════════════════════════

# def _get_required_params_for_entity(
#     tool_hint:     str,
#     entity_type:   str,
#     matched_tools: list[dict],
# ) -> list[str]:
#     if entity_type and entity_type != "general":
#         params = ENTITY_TYPE_PARAM_MAP.get(entity_type, [])
#         console.print(
#             f"  📋 entity_type='{entity_type}' → authoritative params: {params}"
#         )
#         return params

#     if tool_hint:
#         for t in matched_tools:
#             if t["tool_name"] == tool_hint:
#                 db_params = [p for p in t["required_parameters"] if p in PARAM_TO_TABLE_MAP]
#                 if db_params:
#                     return db_params
#                 break

#         meta = _get_tool_meta(tool_hint)
#         if meta:
#             db_params = [p for p in meta["required_parameters"] if p in PARAM_TO_TABLE_MAP]
#             if db_params:
#                 return db_params

#     console.print(f"  📋 No DB params resolved for entity_type='{entity_type}'")
#     return []


# # ══════════════════════════════════════════════════════════════════════════════
# # Core per-entity DB resolver (unchanged from v10.1)
# # ══════════════════════════════════════════════════════════════════════════════

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
#     needs_isin      = "isin"        in required_params
#     needs_indexcode = "index_code"  in required_params

#     if needs_schcode:
#         search = scheme_name or name
#         cfg    = PARAM_TO_TABLE_MAP["mf_schcode"]
#         match  = _targeted_db_lookup(
#             entity=search, table=cfg["table"],
#             name_col=cfg["column"], id_col=cfg["id_field"],
#         )
#         if match:
#             codes["mf_schcode"] = int(match["mf_schcode"])
#             if "mf_cocode" in match:
#                 codes.setdefault("mf_cocode", int(match["mf_cocode"]))
#             result["resolved_scheme_name"] = match.get("sch_name", search)

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

#     if needs_co_code:
#         stock = _resolve_single(name, nse_symbol)
#         if stock:
#             codes["co_code"] = stock["co_code"]
#             result.update({
#                 "co_code":      stock["co_code"],
#                 "company_info": stock["company_info"],
#                 "nse_symbol":   stock["nse_symbol"],
#             })

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

#     if needs_indexcode:
#         cfg   = PARAM_TO_TABLE_MAP["index_code"]
#         match = _targeted_db_lookup(
#             entity=name, table=cfg["table"],
#             name_col=cfg["column"], id_col=cfg["id_field"],
#         )
#         if match:
#             codes["index_code"] = int(match["indexcode"])
#             result["resolved_index_name"] = match.get("group_name", name)

#     result["mcp_resolved_codes"] = codes
#     return result


# # ══════════════════════════════════════════════════════════════════════════════
# # Resolve codes for all intents (unchanged from v10.1)
# # ══════════════════════════════════════════════════════════════════════════════

# def _resolve_intents_codes(intents: list[IntentItem]) -> list[IntentItem]:
#     def _resolve_one(intent: IntentItem) -> IntentItem:
#         et        = intent.get("entity_type", "general")
#         tool_hint = intent.get("tool_hint", "")
#         matched   = intent.get("matched_tools") or []

#         if et == "general" and tool_hint:
#             et = _infer_entity_type_from_tool(tool_hint)
#             intent["entity_type"] = et

#         req_params = _get_required_params_for_entity(tool_hint, et, matched)

#         resolved = _resolve_entity_codes(
#             name            = intent.get("entity", ""),
#             scheme_name     = intent.get("scheme_name"),
#             amc_name        = intent.get("amc_name"),
#             nse_symbol      = intent.get("nse_symbol"),
#             required_params = req_params,
#         )
#         intent["resolved_codes"] = resolved.get("mcp_resolved_codes", {})
#         console.print(
#             f"  💎 [{intent['entity']}] resolved_codes={intent['resolved_codes']}"
#         )
#         return intent

#     result_intents: dict[int, IntentItem] = {}
#     with ThreadPoolExecutor(max_workers=min(6, len(intents) or 1)) as ex:
#         futures = {
#             ex.submit(_resolve_one, intent): idx
#             for idx, intent in enumerate(intents)
#         }
#         for future in as_completed(futures):
#             idx = futures[future]
#             try:
#                 result_intents[idx] = future.result()
#             except Exception as exc:
#                 console.print(f"  [red]Code resolution failed for intent #{idx}: {exc}[/red]")
#                 result_intents[idx] = intents[idx]

#     return [result_intents[i] for i in sorted(result_intents)]


# # ══════════════════════════════════════════════════════════════════════════════
# # MCP Pre-Resolution nodes (unchanged from v10.1)
# # ══════════════════════════════════════════════════════════════════════════════

# def node_mcp_pre_resolve(state: AgentState) -> AgentState:
#     console.print("[Node MPR] Single-entity MCP pre-resolve (v10.1)")

#     intents = state.get("intents") or []
#     if not intents:
#         console.print("  ⚠ No intents; skipping resolve")
#         state["mcp_resolved_codes"] = {}
#         return state

#     resolved_intents = _resolve_intents_codes(intents)
#     state["intents"] = resolved_intents

#     if resolved_intents:
#         state["mcp_resolved_codes"] = resolved_intents[0].get("resolved_codes", {})

#     console.print(f"  🏁 Primary resolved codes: {state['mcp_resolved_codes']}")
#     return state


# def node_mcp_multi_pre_resolve(state: AgentState) -> AgentState:
#     console.print("[Node MMPR] Multi-entity MCP pre-resolve (v10.1)")

#     intents = state.get("intents") or []
#     if not intents:
#         console.print("  ⚠ No intents; skipping resolve")
#         return state

#     resolved_intents = _resolve_intents_codes(intents)
#     state["intents"] = resolved_intents

#     if resolved_intents:
#         state["mcp_resolved_codes"] = resolved_intents[0].get("resolved_codes", {})

#     console.print(
#         f"  🏁 Resolved {len(resolved_intents)} intents. "
#         f"Primary codes: {state['mcp_resolved_codes']}"
#     )
#     return state


# # ══════════════════════════════════════════════════════════════════════════════
# # Injection block builders (unchanged from v10.1)
# # ══════════════════════════════════════════════════════════════════════════════

# def _build_injection_block_for_intent(intent: IntentItem) -> str:
#     codes       = intent.get("resolved_codes") or {}
#     entity_type = intent.get("entity_type", "")
#     tool_hint   = intent.get("tool_hint", "")
#     scheme_name = intent.get("scheme_name", "") or ""
#     amc_name    = intent.get("amc_name", "") or ""
#     intent_desc = intent.get("intent_description", "")

#     lines: list[str] = []
#     if entity_type:
#         lines.append(f"entity_type={entity_type}")
#     if intent_desc:
#         lines.append(f"query_intent={intent_desc}")
#     if tool_hint:
#         lines.append(f"recommended_tool={tool_hint}")

#     _FAMILY_COMMENTS = {
#         "co_code":    "stock/equity tools only",
#         "mf_schcode": "MF scheme tools only",
#         "mf_cocode":  "AMC/fund-house tools only",
#         "isin":       "ETF tools only",
#         "index_code": "index tools only",
#     }
#     for param, val in codes.items():
#         comment = f"  # {_FAMILY_COMMENTS[param]}" if param in _FAMILY_COMMENTS else ""
#         lines.append(f"{param}={val}{comment}")

#     if scheme_name:
#         lines.append(f"scheme_name={scheme_name}")
#     if amc_name:
#         lines.append(f"amc_name={amc_name}")

#     if lines:
#         return "<PRE_RESOLVED>\n" + "\n".join(lines) + "\n</PRE_RESOLVED>"
#     return ""


# def _build_injection_block(state: AgentState) -> str:
#     intents = state.get("intents") or []
#     if intents:
#         return _build_injection_block_for_intent(intents[0])

#     resolved_codes = state.get("mcp_resolved_codes") or {}
#     entity_type    = state.get("primary_entity_type", "")
#     tool_hint      = state.get("mcp_tool_hint", "")
#     lines: list[str] = []
#     if entity_type:
#         lines.append(f"entity_type={entity_type}")
#     if tool_hint:
#         lines.append(f"recommended_tool={tool_hint}")
#     for param, val in resolved_codes.items():
#         lines.append(f"{param}={val}")
#     if lines:
#         return "<PRE_RESOLVED>\n" + "\n".join(lines) + "\n</PRE_RESOLVED>"
#     return ""


# # ══════════════════════════════════════════════════════════════════════════════
# # MCP runner helper (unchanged from v10.1)
# # ══════════════════════════════════════════════════════════════════════════════

# # def _run_mcp_in_new_loop(enriched_query: str) -> str:
# #     from mcp_client import run_mcp_query

# #     loop = asyncio.new_event_loop()
# #     asyncio.set_event_loop(loop)
# #     try:
# #         return loop.run_until_complete(run_mcp_query(enriched_query))
# #     except ExceptionGroup as eg:
# #         sub_errors = [str(e) for e in eg.exceptions]
# #         console.print(f"MCP TaskGroup Error: {', '.join(sub_errors)}")
# #         return f"MCP Error: {sub_errors[0]}"
# #     except Exception as e:
# #         console.print(f"Internal MCP Task Error: {e}")
# #         return f"Error: {str(e)}"
# #     finally:
# #         try:
# #             loop.run_until_complete(loop.shutdown_asyncgens())
# #             for task in asyncio.all_tasks(loop):
# #                 task.cancel()
# #         except Exception:
# #             pass
# #         loop.close()


# def _run_mcp_in_new_loop(enriched_query: str) -> str:
#     from mcp_client import run_mcp_query
#     import asyncio

#     # Create a fresh loop for this thread
#     loop = asyncio.new_event_loop()
#     asyncio.set_event_loop(loop)
    
#     try:
#         # Core execution
#         return loop.run_until_complete(run_mcp_query(enriched_query))
    
#     except ExceptionGroup as eg:
#         # Standard MCP/TaskGroup error handling
#         msg = "; ".join(str(e) for e in eg.exceptions)
#         console.print(f"[bold red]MCP TaskGroup Error:[/bold red] {msg}")
#         return f"MCP Error: {msg}"
    
#     except Exception as e:
#         console.print(f"[bold red]Internal MCP Error:[/bold red] {e}")
#         return f"Error: {str(e)}"
    
#     finally:
#         try:
#             # 1. Allow async generators to finish (e.g., transport streams)
#             loop.run_until_complete(loop.shutdown_asyncgens())
            
#             # 2. Identify remaining tasks (MCP background readers)
#             pending = asyncio.all_tasks(loop)
#             if pending:
#                 # Give them a tiny window to cancel gracefully
#                 for task in pending:
#                     task.cancel()
#                 # Use run_until_complete to wait for the cancellations to propagate
#                 loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
#         except Exception:
#             pass
#         finally:
#             loop.close()


# # ══════════════════════════════════════════════════════════════════════════════
# # Node MTCI: MCP Tool Call — per Intent (unchanged from v10.1)
# # ══════════════════════════════════════════════════════════════════════════════

# # def node_mcp_tool_call_intents(state: AgentState) -> AgentState:
# #     console.print("[Node MTCI] Per-intent MCP tool call (v10.1)")

# #     try:
# #         from mcp_client import run_mcp_query  # noqa: F401
# #     except ImportError:
# #         console.print("MCP client module not found.")
# #         state["mcp_raw_result"] = ""
# #         state["intent_results"] = []
# #         state["error"]          = "mcp_client_not_found"
# #         return state

# #     intents    = state.get("intents") or []
# #     user_query = state.get("user_query", "")

# #     if not intents:
# #         console.print("  ⚠ No intents; falling back to raw user query")
# #         intents = [IntentItem(
# #             entity             = "",
# #             entity_type        = "general",
# #             intent_description = user_query,
# #             tool_hint          = state.get("mcp_tool_hint", ""),
# #             matched_tools      = [],
# #             resolved_codes     = state.get("mcp_resolved_codes") or {},
# #             mcp_result         = "",
# #         )]

# #     def _call_intent(intent: IntentItem) -> IntentItem:
# #         injection = _build_injection_block_for_intent(intent)
# #         sub_query = intent.get("intent_description") or user_query

# #         enriched  = f"{injection}\nUser Query: {sub_query}" if injection else sub_query
# #         tool_hint = intent.get("tool_hint", "")
# #         if tool_hint:
# #             enriched += f"\n\n[RECOMMENDED TOOL: {tool_hint}]"

# #         console.print(
# #             f"  📤 Intent [{intent.get('entity', '?')}] "
# #             f"tool={tool_hint} query='{sub_query[:60]}'"
# #         )

# #         try:
# #             with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
# #                 future = ex.submit(_run_mcp_in_new_loop, enriched)
# #                 result = future.result(timeout=600)
# #             intent["mcp_result"] = result or "No data returned from server."
# #         except concurrent.futures.TimeoutError:
# #             intent["mcp_result"] = "MCP call timed out for this intent."
# #         except Exception as exc:
# #             intent["mcp_result"] = f"MCP call failed: {exc}"

# #         console.print(
# #             f"  📥 Intent [{intent.get('entity', '?')}] "
# #             f"→ {len(intent['mcp_result'])} chars"
# #             f"{intent['mcp_result'][:100]}"
# #         )
# #         return intent

# #     result_map: dict[int, IntentItem] = {}
# #     with ThreadPoolExecutor(max_workers=min(4, len(intents))) as ex:
# #         futures = {
# #             ex.submit(_call_intent, intent): idx
# #             for idx, intent in enumerate(intents)
# #         }
# #         for future in as_completed(futures):
# #             idx = futures[future]
# #             try:
# #                 result_map[idx] = future.result()
# #             except Exception as exc:
# #                 console.print(f"  [red]Intent #{idx} MCP call failed: {exc}[/red]")
# #                 result_map[idx] = intents[idx]

# #     state["intents"] = [result_map[i] for i in sorted(result_map)]

# #     state["intent_results"] = [
# #         {
# #             "entity":             intent.get("entity", ""),
# #             "entity_type":        intent.get("entity_type", ""),
# #             "intent_description": intent.get("intent_description", ""),
# #             "mcp_result":         intent.get("mcp_result", ""),
# #         }
# #         for intent in state["intents"]
# #     ]

# #     state["mcp_raw_result"] = "\n\n---\n\n".join(
# #         f"[{r['entity']} / {r['intent_description']}]\n{r['mcp_result']}"
# #         for r in state["intent_results"]
# #     )
# #     state["mcp_tool_calls_made"] = []
# #     state["error"]               = None

# #     return state


# def node_mcp_tool_call_intents(state: AgentState) -> AgentState:
#     console.print("[Node MTCI] Single-session multi-intent MCP call (v10.3)")

#     try:
#         from mcp_client import run_mcp_query_multi
#     except ImportError:
#         console.print("MCP client module not found.")
#         state["mcp_raw_result"] = ""
#         state["intent_results"] = []
#         state["error"]          = "mcp_client_not_found"
#         return state

#     intents    = state.get("intents") or []
#     user_query = state.get("user_query", "")

#     # Fallback: no intents → synthesise from raw query (unchanged behaviour)
#     if not intents:
#         console.print("  ⚠ No intents; falling back to single raw query")
#         from mcp_client import run_mcp_query
#         try:
#             loop = asyncio.new_event_loop()
#             asyncio.set_event_loop(loop)
#             raw = loop.run_until_complete(run_mcp_query(user_query))
#             loop.close()
#         except Exception as exc:
#             raw = f"MCP call failed: {exc}"
#         state["mcp_raw_result"]      = raw
#         state["intent_results"]      = []
#         state["mcp_tool_calls_made"] = []
#         state["error"]               = None
#         return state

#     # ── All intents → ONE server session ─────────────────────────────────
#     try:
#         loop = asyncio.new_event_loop()
#         asyncio.set_event_loop(loop)
#         try:
#             filled_intents = loop.run_until_complete(
#                 run_mcp_query_multi(intents)
#             )
#         finally:
#             loop.run_until_complete(loop.shutdown_asyncgens())
#             loop.close()
#     except Exception as exc:
#         console.print(f"  [red]Multi-intent MCP failed: {exc}[/red]")
#         # Mark all intents as errored; synthesis will surface the error message
#         filled_intents = [
#             {**dict(i), "mcp_result": f"MCP call failed: {exc}"}
#             for i in intents
#         ]

#     state["intents"] = filled_intents

#     state["intent_results"] = [
#         {
#             "entity":             i.get("entity", ""),
#             "entity_type":        i.get("entity_type", ""),
#             "intent_description": i.get("intent_description", ""),
#             "mcp_result":         i.get("mcp_result", ""),
#         }
#         for i in filled_intents
#     ]

#     state["mcp_raw_result"] = "\n\n---\n\n".join(
#         f"[{r['entity']} / {r['intent_description']}]\n{r['mcp_result']}"
#         for r in state["intent_results"]
#     )
#     state["mcp_tool_calls_made"] = []
#     state["error"]               = None

#     console.print(
#         f"  ✅ {len(filled_intents)} intent(s) resolved in one session. "
#         f"Total result chars: {len(state['mcp_raw_result'])}"
#     )
#     return state


# # ══════════════════════════════════════════════════════════════════════════════
# # MCP Synthesis (unchanged from v10.1)
# # ══════════════════════════════════════════════════════════════════════════════

# MCP_SYNTHESIS_PROMPT = """\
# You are a knowledgeable Indian equity and mutual fund analyst assistant.

# ## Conversation history (last 4 turns)
# {history}

# ## User Question
# {user_query}

# ## Live Data from MCP Server (per intent)
# {mcp_result}

# ## Instructions
# 1. Answer EACH sub-question in the user's query using the relevant live data block above.
# 2. Write in plain prose paragraphs only. No markdown, no bullet points,
#    no asterisks, no tables. Do NOT use the rupee symbol — write "Rs" instead.
# 3. Present key numbers naturally woven into sentences.
# 4. Be honest if specific data fields are missing or unavailable.
# 5. Use Indian number formatting (Rs Cr for large numbers).
# 6. Keep under 300 words unless detail is explicitly requested.
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
#     console.print("[Node MS] MCP synthesis (v10.1)")
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

#     mcp_snippet = state.get("mcp_raw_result", "")
#     # Hard cap so we never blow the LLM context window
#     if len(mcp_snippet) > 8000:
#         # Keep the first 6000 chars + last 1500 (tail often has final numbers)
#         mcp_snippet = (
#             mcp_snippet[:6000]
#             + "\n\n... [middle trimmed for length] ...\n\n"
#             + mcp_snippet[-1500:]
#         )

#     prompt = MCP_SYNTHESIS_PROMPT.format(
#         history    = _format_history(state),
#         user_query = state.get("user_query", ""),
#         mcp_result = mcp_snippet,
#     )
#     try:
#         state["final_answer"] = _llm_text(prompt)
#     except Exception as e:
#         console.print(f"  MCP synthesis failed: {e}")
#         state["final_answer"] = mcp_result
#     return state


# # ── General synthesis (unchanged from v10.1) ──────────────────────────────────

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


# # ══════════════════════════════════════════════════════════════════════════════
# # Router (v10.2 — unchanged logic, cleaner variable names)
# # ══════════════════════════════════════════════════════════════════════════════

# def route_after_classify(state: AgentState) -> str:
#     qt  = state.get("query_type", "general")
#     mcp = state.get("mcp_needed", False)

#     intents    = state.get("intents") or []
#     additional = state.get("companies") or []

#     for i, intent in enumerate(intents):
#         console.print(
#             f"  [Router] intent[{i}] entity={intent.get('entity')} "
#             f"matched_tools={len(intent.get('matched_tools') or [])} "
#             f"tool_hint={intent.get('tool_hint')}"
#         )

#     # TVS results are authoritative
#     if not mcp and intents:
#         mcp = any(bool(intent.get("matched_tools")) for intent in intents)
#         if mcp:
#             console.print("  → mcp_needed upgraded to True by TVS results")

#     has_many = len(additional) > 0 or len(intents) > 1

#     primary_type    = state.get("primary_entity_type", "general")
#     secondary_types = {c.get("entity_type", "general") for c in additional}
#     is_heterogeneous = bool(secondary_types - {"general"} - {primary_type})

#     if qt == "greeting":
#         return "greeting"

#     if not mcp:
#         return "general"

#     if has_many or qt == "comparison" or is_heterogeneous:
#         console.print(
#             f"  → multi_mcp "
#             f"(has_many={has_many}, comparison={qt == 'comparison'}, "
#             f"heterogeneous={is_heterogeneous})"
#         )
#         return "multi_mcp"

#     console.print(f"  → mcp_direct (single entity, type={primary_type})")
#     return "mcp_direct"


# # ══════════════════════════════════════════════════════════════════════════════
# # Build graph (v10.2 — Node A + IQ replaced by Node AIQ)
# # ══════════════════════════════════════════════════════════════════════════════

# def build_graph() -> Any:
#     g = StateGraph(AgentState)

#     # ── Nodes ─────────────────────────────────────────────────────────────
#     g.add_node("classify_extract_decompose", node_classify_extract_and_decompose)  # NEW merged node
#     g.add_node("greeting_handler",           node_greeting_handler)
#     g.add_node("general_handler",            node_general_handler)
#     g.add_node("synthesis",                  node_synthesis)
#     g.add_node("mcp_synthesis",              node_mcp_synthesis)
#     g.add_node("per_intent_tool_search",     node_per_intent_tool_search)
#     g.add_node("mcp_pre_resolve",            node_mcp_pre_resolve)
#     g.add_node("mcp_multi_pre_resolve",      node_mcp_multi_pre_resolve)
#     g.add_node("mcp_tool_call_intents",      node_mcp_tool_call_intents)

#     # ── Entry point ────────────────────────────────────────────────────────
#     g.set_entry_point("classify_extract_decompose")

#     # ── Edges ──────────────────────────────────────────────────────────────
#     # Single hop: merged node → TVS (was 2 hops: A → IQ → TVS)
#     g.add_edge("classify_extract_decompose", "per_intent_tool_search")

#     g.add_conditional_edges(
#         "per_intent_tool_search",
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

#     g.add_edge("mcp_pre_resolve",       "mcp_tool_call_intents")
#     g.add_edge("mcp_multi_pre_resolve", "mcp_tool_call_intents")
#     g.add_edge("mcp_tool_call_intents", "mcp_synthesis")
#     g.add_edge("mcp_synthesis",         END)

#     return g.compile()


# # ── Singleton ─────────────────────────────────────────────────────────────────

# _graph = None

# def get_graph():
#     global _graph
#     if _graph is None:
#         _graph = build_graph()
#     return _graph


# # ── Public entrypoint (unchanged) ─────────────────────────────────────────────

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
# graph.py — EQUIFIZ Financial AI Agent (v11.0)

# Key changes from v10.2
# ──────────────────────────────────────────────────────────────────────
# 1. MTCI NODE — Sequential per-intent MCP calls (one session, loop)
#    - Calls mcp_client.run_mcp_query_multi() which runs each intent's
#      agentic loop sequentially in one shared server session.
#    - Each intent gets its full MAX_ROUNDS budget independently.
#    - Results are populated per-intent; synthesis receives all of them.

# 2. CODE INJECTION FIXED — tools that need no codes are skipped
#    - _get_required_params_for_entity() now returns [] for "general"
#      entity_type intents (IPOs, top gainers, market news, etc.).
#    - DB lookup is NEVER attempted for these intents.
#    - entity_type="general" intents flow through to MCP with just their
#      intent_description; the MCP client's ToolParamIndex handles the rest.

# 3. TRIM moved to mcp_client.trim_results() — graph just calls it.

# 4. Synthesis prompt improved — clearer instructions, no truncation
#    of first intent result.

# 5. All deprecated node stubs removed (they logged warnings and confused
#    the flow trace).

# All v10.2 logic that's still correct is preserved unchanged.
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
# from pathlib import Path
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

# # ── Types ─────────────────────────────────────────────────────────────────────

# class Turn(TypedDict):
#     role: str       # "user" | "assistant"
#     content: str


# class IntentItem(TypedDict, total=False):
#     entity:             str
#     entity_type:        str   # stock|mf_scheme|mf_amc|etf|index|general
#     intent_description: str
#     tool_hint:          str
#     scheme_name:        Optional[str]
#     amc_name:           Optional[str]
#     nse_symbol:         Optional[str]
#     matched_tools:      list[dict]
#     resolved_codes:     dict
#     mcp_result:         str


# # ── Tool-family hints for entity-type inference ────────────────────────────────

# _TOOL_FAMILY_HINTS: dict[str, str] = {
#     "get_company_stock":     "stock",
#     "get_delayed_stock":     "stock",
#     "get_market_indices":    "index",
#     "get_active_performer":  "stock",
#     "get_top_gainers":       "general",
#     "get_top_losers":        "stock",
#     "get_out_under":         "stock",
#     "get_advance_decline":   "stock",
#     "get_52week":            "stock",
#     "get_new_highs":         "stock",
#     "get_index_companies":   "index",
#     "get_sector_companies":  "stock",
#     "get_quarterly_results": "stock",
#     "get_profit_loss":       "stock",
#     "get_balance_sheet":     "stock",
#     "get_cash_flow":         "stock",
#     "get_half_yearly":       "stock",
#     "get_nine_months":       "stock",
#     "get_yearly_results":    "stock",
#     "get_quarterly_balance": "stock",
#     "get_annual_balance":    "stock",
#     "get_ttm_growth":        "stock",
#     "get_quarterly_revenue": "stock",
#     "get_quarterly_ebitda":  "stock",
#     "get_quarterly_ebit":    "stock",
#     "get_growth_data":       "stock",
#     "get_key_financial":     "stock",
#     "get_daily_ratios":      "stock",
#     "get_margin_ratios":     "stock",
#     "get_performance_ratio": "stock",
#     "get_cashflow_ratios":   "stock",
#     "get_growth_ratios":     "stock",
#     "get_liquidity_ratios":  "stock",
#     "get_solvency_ratios":   "stock",
#     "get_return_ratios":     "stock",
#     "get_all_basic_ratios":  "stock",
#     "get_quarterly_ratios":  "stock",
#     "get_yearly_ratios":     "stock",
#     "get_valuation_ratios":  "stock",
#     "get_shareholding":      "stock",
#     "get_major_sharehold":   "stock",
#     "get_company_profile":   "stock",
#     "get_company_backgroun": "stock",
#     "get_board_of":          "stock",
#     "get_company_bankers":   "stock",
#     "get_management":        "stock",
#     "get_subsidiaries":      "stock",
#     "get_related_party":     "stock",
#     "get_employee_count":    "stock",
#     "get_capital_structure": "stock",
#     "get_pledge_share":      "stock",
#     "get_chronological":     "stock",
#     "get_company_history":   "stock",
#     "get_substantial":       "stock",
#     "get_segment_data":      "stock",
#     "get_r_and_d":           "stock",
#     "get_finished_products": "stock",
#     "get_raw_materials":     "stock",
#     "get_forthcoming_ipo":   "general",   # no entity code needed
#     "get_open_ipos":         "general",   # no entity code needed
#     "get_closed_ipos":       "general",   # no entity code needed
#     "get_new_ipo":           "general",
#     "get_best_ipo":          "general",
#     "get_anchor_investor":   "stock",
#     "get_basis_of":          "stock",
#     "get_scheme_nav":        "mf_scheme",
#     "get_investment_detail": "mf_scheme",
#     "get_expense_ratio":     "mf_scheme",
#     "get_avg_maturity":      "mf_scheme",
#     "get_scheme_aum":        "mf_scheme",
#     "get_nav_historical":    "mf_scheme",
#     "get_scheme_returns":    "mf_scheme",
#     "get_lumpsum_returns":   "mf_scheme",
#     "get_scheme_sip":        "mf_scheme",
#     "get_mf_holdings":       "mf_scheme",
#     "get_sector_allocation": "mf_scheme",
#     "get_asset_allocation":  "mf_scheme",
#     "get_portfolio_changes": "mf_scheme",
#     "get_mcap_allocation":   "mf_scheme",
#     "get_most_bought":       "mf_scheme",
#     "get_scheme_ratios":     "mf_scheme",
#     "get_dividend_details":  "mf_scheme",
#     "get_bse_star_scheme":   "mf_scheme",
#     "compare_schemes":       "mf_scheme",
#     "get_whats_in_out":      "mf_scheme",
#     "get_fund_categories":   "mf_amc",
#     "get_schemes_by_amc":    "mf_amc",
#     "get_fund_profile":      "mf_amc",
#     "get_etf_":              "etf",
#     "get_index_":            "index",
# }


# def _infer_entity_type_from_tool(tool_hint: str) -> str:
#     if not tool_hint:
#         return "general"
#     tl = tool_hint.lower()
#     for prefix, family in _TOOL_FAMILY_HINTS.items():
#         if tl.startswith(prefix):
#             return family
#     return "general"


# # ── DB routing ────────────────────────────────────────────────────────────────

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

# # Maps entity_type → the DB param it needs
# ENTITY_TYPE_PARAM_MAP: dict[str, list[str]] = {
#     "stock":     ["co_code"],
#     "mf_scheme": ["mf_schcode"],
#     "mf_amc":    ["mf_cocode"],
#     "etf":       ["isin"],
#     "index":     ["index_code"],
#     "general":   [],   # ← no code needed; do NOT do DB lookup
# }

# DB_CONFIG = {
#     "host":     "localhost",
#     "port":     5432,
#     "dbname":   "equifiz",
#     "user":     "postgres",
#     "password": "1234",
# }

# TOOL_SCORE_THRESHOLD = 0.55

# # ── Chroma (read-only) ────────────────────────────────────────────────────────
# from chroma_singleton import get_chroma_collection
# try:
#     _tool_collection = get_chroma_collection()
#     console.print(f"  [ToolRegistry] Chroma ready — {_tool_collection.count()} tools")
# except Exception as e:
#     console.print(f"  [ToolRegistry] Chroma init failed: {e}")
#     _tool_collection = None


# # ── Agent State ───────────────────────────────────────────────────────────────

# class AgentState(TypedDict, total=False):
#     user_query:           str
#     conversation_history: list[Turn]
#     force_refresh:        bool

#     query_type:            str
#     extracted_entity:      str
#     report_type:           str
#     intent:                str
#     is_broad:              bool
#     mcp_needed:            bool
#     mcp_tool_hint:         str

#     primary_entity_type:  str
#     primary_tool_hint:    str
#     primary_query_intent: str

#     matched_tools: list[dict]
#     intents: list[IntentItem]
#     intent_results: list[dict]

#     nse_symbol:   Optional[str]
#     co_code:      Optional[int]
#     company_info: Optional[dict]

#     mcp_resolved_codes: dict
#     mcp_scheme_name:    Optional[str]
#     mcp_amc_name:       Optional[str]

#     companies: list[dict]

#     vector_context: list[dict]

#     mcp_raw_result:      str
#     mcp_tool_calls_made: list[str]

#     final_answer: str
#     error:        Optional[str]


# # ── LLM helpers ───────────────────────────────────────────────────────────────

# def _llm_json(prompt: str) -> dict:
#     resp      = llm.invoke([HumanMessage(content=prompt)])
#     text      = resp.content.strip()
#     clean     = re.sub(r"^```(?:json)?|```$", "", text, flags=re.MULTILINE).strip()

#     try:
#         return json.loads(clean)
#     except json.JSONDecodeError:
#         pass

#     match = re.search(r"(\{.*\})", clean, re.DOTALL)
#     if match:
#         greedy = match.group(1)
#         try:
#             return json.loads(greedy)
#         except json.JSONDecodeError:
#             try:
#                 fixed = re.sub(r",\s*([\]}])", r"\1", greedy.replace("'", '"'))
#                 return json.loads(fixed)
#             except Exception:
#                 pass

#     logger.error(f"[JSON FAIL] Raw LLM Output:\n{text}")
#     raise ValueError("Could not parse JSON from LLM response.")


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


# # ══════════════════════════════════════════════════════════════════════════════
# # Node AIQ: Classify + Extract + Decompose  (merged — v10.2 prompt, v11.0 node)
# # ══════════════════════════════════════════════════════════════════════════════

# CLASSIFY_EXTRACT_DECOMPOSE_PROMPT = """\
# You are a Lead Financial Systems Analyst for an Indian stock-market assistant. Your task is to decompose user queries into executable data intents.

# ## Context
# - History: {history}
# - Today's Date: {today}
# - Query: "{query}"

# ────────────────────────────────────────────────────────────
# STEP 1: SUBJECT IDENTIFICATION
# Identify every distinct subject the user is asking about. A subject is either:
# 1. A Specific Entity: (e.g., Reliance Industries, HDFC AMC, Nifty 50, SBI Small Cap Fund).
# 2. A Market Category/Concept: (e.g., "upcoming IPOs", "top gainers", "market news", "bulk deals").

# Example Decomposition:
# - "Price of Reliance and upcoming IPOs"
#   -> Subject 1: Reliance (Entity)
#   -> Subject 2: Upcoming IPOs (Concept)

# STEP 2: JSON GENERATION
# Generate exactly one intent item for every subject identified in Step 1.

# Return ONLY valid JSON:
# {{
#   "query_type": "<greeting|general|stock|mf_scheme|mf_amc|comparison|investment>",
#   "is_broad": <true|false>,
#   "mcp_needed": <true|false>,
#   "intents": [
#     {{
#       "entity": "<entity name OR market concept name>",
#       "entity_type": "<stock|mf_scheme|mf_amc|etf|index|general>",
#       "intent_description": "<specific data point needed for this subject>",
#       "scheme_name": "<if mf_scheme, else empty>",
#       "amc_name": "<if mf_amc, else empty>",
#       "nse_symbol": "<NSE ticker if known, else empty>"
#     }}
#   ]
# }}

# ────────────────────────────────────────────────────────────
# DECOMPOSITION RULES (CRITICAL):
# 1. THE 1:1 MAPPING: len(intents) MUST equal the number of subjects identified in Step 1.
# 2. NO BUNDLING: If a query asks for two different subjects (e.g., a stock price and a market trend), return two separate intent objects.
# 3. ENTITY TYPE ASSIGNMENT:
#    - "Reliance", "TCS", "HDFC Bank" → entity_type = stock
#    - "SBI Mutual Fund", "Nippon AMC" → entity_type = mf_amc
#    - "Parag Parikh Flexi Cap", "Axis Bluechip" → entity_type = mf_scheme
#    - "Nifty 50", "Sensex" → entity_type = index
#    - Concepts like "IPOs", "Gainers", market data, or educational definitions → entity_type = general
# 4. DATA POINT MERGING: If a user asks for multiple metrics on the SAME entity, return ONE intent. List all metrics in intent_description.

# MCP_NEEDED LOGIC:
# - Set to true if ANY intent requires fetching live market data (prices, IPO lists, NAVs, ratios, etc.).
# - Set to false only for pure greetings or purely educational/definitional questions.

# NEGATIVE EXAMPLES (DO NOT DO THESE):
# ✗ User: "Price of Reliance and IPOs" → 1 intent (Wrong: Market concepts are separate subjects)
# ✗ User: "PE of TCS and EPS of Wipro" → 1 intent (Wrong: These are two distinct entities)
# ────────────────────────────────────────────────────────────
# """


# def node_classify_extract_and_decompose(state: AgentState) -> AgentState:
#     console.print("[Node AIQ] Classify + extract + decompose (v11.0)")

#     user_query   = state.get("user_query", "")
#     history_text = _format_history(state, n=4)

#     state["matched_tools"] = []
#     state["intents"]       = []
#     state["companies"]     = []

#     try:
#         result = _llm_json(
#             CLASSIFY_EXTRACT_DECOMPOSE_PROMPT.format(
#                 history = history_text,
#                 today   = TODAY,
#                 query   = user_query,
#             )
#         )

#         qt          = result.get("query_type", "general")
#         mcp         = bool(result.get("mcp_needed", False))
#         raw_intents = result.get("intents") or []

#         intents: list[IntentItem] = []
#         for item in raw_intents:
#             et = item.get("entity_type", "general")
#             intents.append(IntentItem(
#                 entity             = item.get("entity", ""),
#                 entity_type        = et,
#                 intent_description = item.get("intent_description", ""),
#                 scheme_name        = item.get("scheme_name") or None,
#                 amc_name           = item.get("amc_name") or None,
#                 nse_symbol         = item.get("nse_symbol") or None,
#                 tool_hint          = "",
#                 matched_tools      = [],
#                 resolved_codes     = {},
#                 mcp_result         = "",
#             ))

#         if not intents and qt not in ("greeting", "general"):
#             console.print("  ⚠ LLM returned no intents — fallback to single general intent")
#             intents.append(IntentItem(
#                 entity             = user_query,
#                 entity_type        = "general",
#                 intent_description = user_query,
#                 scheme_name        = None,
#                 amc_name           = None,
#                 nse_symbol         = None,
#                 tool_hint          = "",
#                 matched_tools      = [],
#                 resolved_codes     = {},
#                 mcp_result         = "",
#             ))

#         state["query_type"] = qt
#         state["mcp_needed"] = mcp
#         state["intents"]    = intents

#         if intents:
#             first = intents[0]
#             state["primary_entity_type"]  = first.get("entity_type", "general")
#             state["primary_query_intent"] = first.get("intent_description", "")
#             state["extracted_entity"]     = first.get("entity", user_query)
#             state["mcp_scheme_name"]      = first.get("scheme_name")
#             state["mcp_amc_name"]         = first.get("amc_name")
#             state["nse_symbol"]           = first.get("nse_symbol")
#         else:
#             state["primary_entity_type"]  = "general"
#             state["primary_query_intent"] = user_query
#             state["extracted_entity"]     = user_query
#             state["mcp_scheme_name"]      = None
#             state["mcp_amc_name"]         = None
#             state["nse_symbol"]           = None

#         # Legacy companies list (for downstream compat, populated from non-primary intents)
#         state["companies"] = [
#             {
#                 "name":         i.get("entity", ""),
#                 "entity_type":  i.get("entity_type", "general"),
#                 "tool_hint":    "",
#                 "query_intent": i.get("intent_description", ""),
#                 "scheme_name":  i.get("scheme_name"),
#                 "amc_name":     i.get("amc_name"),
#                 "nse_symbol":   i.get("nse_symbol"),
#             }
#             for i in intents[1:]
#         ]

#         console.print(
#             f"  → type={qt} | mcp={mcp} | {len(intents)} intent(s): "
#             + "; ".join(
#                 f"[{i['entity']} / {i['entity_type']}] {i['intent_description'][:55]}"
#                 for i in intents
#             )
#         )

#     except Exception as e:
#         console.print(f"  [ERROR] Node AIQ failed: {e}")
#         state["query_type"]           = "general"
#         state["mcp_needed"]           = False
#         state["primary_entity_type"]  = "general"
#         state["primary_query_intent"] = user_query
#         state["extracted_entity"]     = user_query
#         state["companies"]            = []
#         state["intents"]              = []

#     return state


# # ══════════════════════════════════════════════════════════════════════════════
# # Chroma tool registry helpers (read-only)
# # ══════════════════════════════════════════════════════════════════════════════

# def _query_tool_registry(
#     query: str,
#     n_results: int = 8,
#     score_threshold: float = TOOL_SCORE_THRESHOLD,
# ) -> list[dict]:
#     if _tool_collection is None:
#         return []
#     try:
#         count = _tool_collection.count()
#         if count == 0:
#             return []

#         res = _tool_collection.query(
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
#             raw_req    = meta.get("required_parameters", "") or ""
#             req_params = [p.strip() for p in raw_req.split(",") if p.strip()]
#             tools.append({
#                 "tool_name":           meta.get("name", tool_id),
#                 "description":         meta.get("description", ""),
#                 "required_parameters": req_params,
#                 "parameters":          meta.get("parameters", ""),
#                 "score":               round(1 - dist, 3),
#             })

#         console.print(
#             f"  [ToolRegistry] '{query[:60]}' → {len(tools)} tools "
#             f"(threshold={score_threshold})"
#         )
#         return tools

#     except Exception as e:
#         console.print(f"  [ToolRegistry] Chroma query failed: {e}")
#         return []


# def _get_tool_meta(tool_name: str) -> Optional[dict]:
#     if _tool_collection is None:
#         return None
#     try:
#         res = _tool_collection.get(ids=[tool_name], include=["metadatas"])
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


# # ══════════════════════════════════════════════════════════════════════════════
# # Node TVS: Per-intent tool vector search
# # ══════════════════════════════════════════════════════════════════════════════

# def node_per_intent_tool_search(state: AgentState) -> AgentState:
#     console.print("[Node TVS] Per-intent tool vector search (v11.0)")

#     intents = state.get("intents") or []
#     if not intents:
#         console.print("  ⚠ No intents to search for")
#         return state

#     def _search_for_intent(intent: IntentItem) -> IntentItem:
#         et = intent.get("entity_type", "general")

#         # For general/no-code intents, search purely on intent description
#         if et == "general":
#             search_query = intent["intent_description"]
#         else:
#             search_query = f"{et} {intent['intent_description']}"

#         tools = _query_tool_registry(search_query, n_results=8)

#         # For typed entities, prefer tools that match the family
#         if et not in ("general", "") and tools:
#             family_tools = [
#                 t for t in tools
#                 if _infer_entity_type_from_tool(t["tool_name"]) in (et, "general")
#             ]
#             if family_tools:
#                 tools = family_tools

#         tool_hint = tools[0]["tool_name"] if tools else ""

#         if tool_hint:
#             console.print(
#                 f"  [{intent['entity']}] "
#                 f"intent='{intent['intent_description'][:50]}' "
#                 f"→ top tool={tool_hint} ({len(tools)} matched)"
#             )
#         else:
#             console.print(
#                 f"  [{intent['entity']}] "
#                 f"intent='{intent['intent_description'][:50]}' "
#                 f"→ no tools matched"
#             )

#         return IntentItem(
#             entity             = intent.get("entity", ""),
#             entity_type        = et,
#             intent_description = intent.get("intent_description", ""),
#             scheme_name        = intent.get("scheme_name"),
#             amc_name           = intent.get("amc_name"),
#             nse_symbol         = intent.get("nse_symbol"),
#             tool_hint          = tool_hint,
#             matched_tools      = tools,
#             resolved_codes     = {},
#             mcp_result         = "",
#         )

#     updated: dict[int, IntentItem] = {}
#     with ThreadPoolExecutor(max_workers=min(6, len(intents))) as ex:
#         futures = {
#             ex.submit(_search_for_intent, intent): idx
#             for idx, intent in enumerate(intents)
#         }
#         for future in as_completed(futures):
#             idx = futures[future]
#             try:
#                 updated[idx] = future.result()
#             except Exception as exc:
#                 console.print(f"  [red]TVS failed for intent #{idx}: {exc}[/red]")
#                 original    = intents[idx]
#                 updated[idx] = IntentItem(
#                     entity             = original.get("entity", ""),
#                     entity_type        = original.get("entity_type", "general"),
#                     intent_description = original.get("intent_description", ""),
#                     scheme_name        = original.get("scheme_name"),
#                     amc_name           = original.get("amc_name"),
#                     nse_symbol         = original.get("nse_symbol"),
#                     tool_hint          = "",
#                     matched_tools      = [],
#                     resolved_codes     = {},
#                     mcp_result         = "",
#                 )

#     state["intents"] = [updated[i] for i in sorted(updated)]

#     if state["intents"]:
#         first = state["intents"][0]
#         state["matched_tools"]     = first.get("matched_tools") or []
#         state["mcp_tool_hint"]     = first.get("tool_hint") or ""
#         state["primary_tool_hint"] = first.get("tool_hint") or ""

#     return state


# # ── Greeting handler ──────────────────────────────────────────────────────────

# GREETING_PROMPT = """\
# You are EQUIFIZ — a friendly Indian stock-market AI assistant.
# Respond warmly and briefly to the user's greeting or small talk.
# Mention 1-2 things you can help with (PE ratios, quarterly results,
# investment recommendations, live NAV, MF returns, IPO listings, etc.).

# User: {query}
# """

# def node_greeting_handler(state: AgentState) -> AgentState:
#     console.print("[Node B] Greeting handler")
#     state["final_answer"] = _llm_text(GREETING_PROMPT.format(query=state["user_query"]))
#     return state


# # ── General handler ───────────────────────────────────────────────────────────

# def node_general_handler(state: AgentState) -> AgentState:
#     console.print("[Node C] General handler")
#     try:
#         state["vector_context"] = vs.query(state["user_query"], n_results=5)
#     except Exception as e:
#         console.print(f"  General vector search failed: {e}")
#         state["vector_context"] = []
#     return state


# # ── Symbol resolution helper ──────────────────────────────────────────────────

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


# # ══════════════════════════════════════════════════════════════════════════════
# # DB helpers
# # ══════════════════════════════════════════════════════════════════════════════

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
#             cur.execute("SELECT mf_schcode, mf_cocode, sch_name, category FROM scheme_master")
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

#         console.print(f"  ⚠ No confident match in {table} (best={best_score})")
#         return None

#     except Exception as e:
#         console.print(f"  [_targeted_db_lookup] DB error on {table}: {e}")
#         return None


# # ══════════════════════════════════════════════════════════════════════════════
# # Required params resolver
# # ══════════════════════════════════════════════════════════════════════════════

# def _get_required_params_for_entity(
#     tool_hint:     str,
#     entity_type:   str,
#     matched_tools: list[dict],
# ) -> list[str]:
#     """
#     Returns the list of DB params this entity needs resolved.
#     Returns [] for 'general' entity_type — no DB lookup needed.
#     """
#     # General intents (IPOs, gainers, etc.) need NO codes at all
#     if entity_type == "general":
#         console.print(f"  📋 entity_type='general' → no DB params needed")
#         return []

#     if entity_type and entity_type != "general":
#         params = ENTITY_TYPE_PARAM_MAP.get(entity_type, [])
#         console.print(f"  📋 entity_type='{entity_type}' → authoritative params: {params}")
#         return params

#     if tool_hint:
#         for t in matched_tools:
#             if t["tool_name"] == tool_hint:
#                 db_params = [p for p in t["required_parameters"] if p in PARAM_TO_TABLE_MAP]
#                 if db_params:
#                     return db_params
#                 break

#         meta = _get_tool_meta(tool_hint)
#         if meta:
#             db_params = [p for p in meta["required_parameters"] if p in PARAM_TO_TABLE_MAP]
#             if db_params:
#                 return db_params

#     console.print(f"  📋 No DB params resolved for entity_type='{entity_type}'")
#     return []


# # ══════════════════════════════════════════════════════════════════════════════
# # Per-entity DB code resolver
# # ══════════════════════════════════════════════════════════════════════════════

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
#     needs_isin      = "isin"        in required_params
#     needs_indexcode = "index_code"  in required_params

#     if needs_schcode:
#         search = scheme_name or name
#         cfg    = PARAM_TO_TABLE_MAP["mf_schcode"]
#         match  = _targeted_db_lookup(
#             entity=search, table=cfg["table"],
#             name_col=cfg["column"], id_col=cfg["id_field"],
#         )
#         if match:
#             codes["mf_schcode"] = int(match["mf_schcode"])
#             if "mf_cocode" in match:
#                 codes.setdefault("mf_cocode", int(match["mf_cocode"]))
#             result["resolved_scheme_name"] = match.get("sch_name", search)

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

#     if needs_co_code:
#         stock = _resolve_single(name, nse_symbol)
#         if stock:
#             codes["co_code"] = stock["co_code"]
#             result.update({
#                 "co_code":      stock["co_code"],
#                 "company_info": stock["company_info"],
#                 "nse_symbol":   stock["nse_symbol"],
#             })

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

#     if needs_indexcode:
#         cfg   = PARAM_TO_TABLE_MAP["index_code"]
#         match = _targeted_db_lookup(
#             entity=name, table=cfg["table"],
#             name_col=cfg["column"], id_col=cfg["id_field"],
#         )
#         if match:
#             codes["index_code"] = int(match["indexcode"])
#             result["resolved_index_name"] = match.get("group_name", name)

#     result["mcp_resolved_codes"] = codes
#     return result


# # ── Resolve codes for all intents ─────────────────────────────────────────────

# def _resolve_intents_codes(intents: list[IntentItem]) -> list[IntentItem]:
#     def _resolve_one(intent: IntentItem) -> IntentItem:
#         et        = intent.get("entity_type", "general")
#         tool_hint = intent.get("tool_hint", "")
#         matched   = intent.get("matched_tools") or []

#         # If entity_type is still 'general', try to infer from tool_hint
#         if et == "general" and tool_hint:
#             inferred = _infer_entity_type_from_tool(tool_hint)
#             if inferred != "general":
#                 et = inferred
#                 intent["entity_type"] = et

#         req_params = _get_required_params_for_entity(tool_hint, et, matched)

#         # Skip DB entirely for general/no-code intents
#         if not req_params:
#             console.print(
#                 f"  💎 [{intent['entity']}] entity_type='{et}' "
#                 f"→ no code resolution needed"
#             )
#             intent["resolved_codes"] = {}
#             return intent

#         resolved = _resolve_entity_codes(
#             name            = intent.get("entity", ""),
#             scheme_name     = intent.get("scheme_name"),
#             amc_name        = intent.get("amc_name"),
#             nse_symbol      = intent.get("nse_symbol"),
#             required_params = req_params,
#         )
#         intent["resolved_codes"] = resolved.get("mcp_resolved_codes", {})
#         console.print(
#             f"  💎 [{intent['entity']}] resolved_codes={intent['resolved_codes']}"
#         )
#         return intent

#     result_intents: dict[int, IntentItem] = {}
#     with ThreadPoolExecutor(max_workers=min(6, len(intents) or 1)) as ex:
#         futures = {
#             ex.submit(_resolve_one, intent): idx
#             for idx, intent in enumerate(intents)
#         }
#         for future in as_completed(futures):
#             idx = futures[future]
#             try:
#                 result_intents[idx] = future.result()
#             except Exception as exc:
#                 console.print(f"  [red]Code resolution failed for intent #{idx}: {exc}[/red]")
#                 result_intents[idx] = intents[idx]

#     return [result_intents[i] for i in sorted(result_intents)]


# # ══════════════════════════════════════════════════════════════════════════════
# # MCP Pre-Resolution nodes
# # ══════════════════════════════════════════════════════════════════════════════

# def node_mcp_pre_resolve(state: AgentState) -> AgentState:
#     console.print("[Node MPR] Single-entity MCP pre-resolve (v11.0)")

#     intents = state.get("intents") or []
#     if not intents:
#         console.print("  ⚠ No intents; skipping resolve")
#         state["mcp_resolved_codes"] = {}
#         return state

#     resolved_intents        = _resolve_intents_codes(intents)
#     state["intents"]        = resolved_intents
#     state["mcp_resolved_codes"] = resolved_intents[0].get("resolved_codes", {}) if resolved_intents else {}

#     console.print(f"  🏁 Primary resolved codes: {state['mcp_resolved_codes']}")
#     return state


# def node_mcp_multi_pre_resolve(state: AgentState) -> AgentState:
#     console.print("[Node MMPR] Multi-entity MCP pre-resolve (v11.0)")

#     intents = state.get("intents") or []
#     if not intents:
#         console.print("  ⚠ No intents; skipping resolve")
#         return state

#     resolved_intents            = _resolve_intents_codes(intents)
#     state["intents"]            = resolved_intents
#     state["mcp_resolved_codes"] = resolved_intents[0].get("resolved_codes", {}) if resolved_intents else {}

#     console.print(
#         f"  🏁 Resolved {len(resolved_intents)} intents. "
#         f"Primary codes: {state['mcp_resolved_codes']}"
#     )
#     return state


# # ══════════════════════════════════════════════════════════════════════════════
# # Node MTCI: MCP Tool Call — sequential per-intent in one session (v11.0)
# # ══════════════════════════════════════════════════════════════════════════════

# def _run_mcp_multi_in_thread(intents: list[dict]) -> list[dict]:
#     """
#     Run run_mcp_query_multi in a fresh event loop (called from a thread).
#     """
#     from mcp_client import run_mcp_query_multi, trim_results

#     loop = asyncio.new_event_loop()
#     asyncio.set_event_loop(loop)
#     try:
#         results = loop.run_until_complete(run_mcp_query_multi(intents))
#         return trim_results(results)
#     except ExceptionGroup as eg:
#         msg = "; ".join(str(e) for e in eg.exceptions)
#         console.print(f"[bold red]MCP TaskGroup Error:[/bold red] {msg}")
#         return [{**i, "mcp_result": f"MCP Error: {msg}"} for i in intents]
#     except Exception as e:
#         console.print(f"[bold red]MCP Error:[/bold red] {e}")
#         return [{**i, "mcp_result": f"Error: {e}"} for i in intents]
#     finally:
#         try:
#             loop.run_until_complete(loop.shutdown_asyncgens())
#             pending = asyncio.all_tasks(loop)
#             if pending:
#                 for task in pending:
#                     task.cancel()
#                 loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
#         except Exception:
#             pass
#         loop.close()


# def node_mcp_tool_call_intents(state: AgentState) -> AgentState:
#     console.print("[Node MTCI] Sequential per-intent MCP calls — one session (v11.0)")

#     try:
#         import mcp_client  # noqa: F401
#     except ImportError:
#         console.print("MCP client module not found.")
#         state["mcp_raw_result"] = ""
#         state["intent_results"] = []
#         state["error"]          = "mcp_client_not_found"
#         return state

#     intents    = state.get("intents") or []
#     user_query = state.get("user_query", "")

#     # Fallback: no intents at all
#     if not intents:
#         console.print("  ⚠ No intents; falling back to raw user query")
#         from mcp_client import run_mcp_query
#         try:
#             loop = asyncio.new_event_loop()
#             asyncio.set_event_loop(loop)
#             raw = loop.run_until_complete(run_mcp_query(user_query))
#             loop.close()
#         except Exception as exc:
#             raw = f"MCP call failed: {exc}"
#         state["mcp_raw_result"]      = raw
#         state["intent_results"]      = []
#         state["mcp_tool_calls_made"] = []
#         state["error"]               = None
#         return state

#     # Run all intents sequentially in one shared MCP session
#     # (done in a thread so we can use a fresh event loop)
#     try:
#         with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
#             future       = ex.submit(_run_mcp_multi_in_thread, intents)
#             filled_intents = future.result(timeout=600)
#     except concurrent.futures.TimeoutError:
#         console.print("  [red]Multi-intent MCP timed out[/red]")
#         filled_intents = [
#             {**dict(i), "mcp_result": "MCP call timed out."}
#             for i in intents
#         ]
#     except Exception as exc:
#         console.print(f"  [red]Multi-intent MCP failed: {exc}[/red]")
#         filled_intents = [
#             {**dict(i), "mcp_result": f"MCP call failed: {exc}"}
#             for i in intents
#         ]

#     state["intents"] = filled_intents

#     state["intent_results"] = [
#         {
#             "entity":             i.get("entity", ""),
#             "entity_type":        i.get("entity_type", ""),
#             "intent_description": i.get("intent_description", ""),
#             "mcp_result":         i.get("mcp_result", ""),
#         }
#         for i in filled_intents
#     ]

#     # Full concatenated result for synthesis — all intents, all data
#     state["mcp_raw_result"] = "\n\n---\n\n".join(
#         f"[{r['entity']} / {r['intent_description']}]\n{r['mcp_result']}"
#         for r in state["intent_results"]
#         if r.get("mcp_result")
#     )
#     state["mcp_tool_calls_made"] = []
#     state["error"]               = None

#     total_chars = len(state["mcp_raw_result"])
#     console.print(
#         f"  ✅ {len(filled_intents)} intent(s) resolved. "
#         f"Total chars for synthesis: {total_chars}"
#     )
#     return state


# # ══════════════════════════════════════════════════════════════════════════════
# # MCP Synthesis (v11.0 — improved prompt)
# # ══════════════════════════════════════════════════════════════════════════════

# MCP_SYNTHESIS_PROMPT = """\
# You are a knowledgeable Indian equity and mutual fund analyst assistant.

# ## Conversation history (last 4 turns)
# {history}

# ## User Question
# {user_query}

# ## Live Data Retrieved (one block per sub-question)
# {mcp_result}

# ## Instructions
# 1. Answer EACH sub-question using the data block labelled for it above.
#    If there are multiple data blocks, address each one in turn.
# 2. Write in plain prose paragraphs only. No markdown, no bullet points,
#    no asterisks, no tables. Do NOT use the rupee symbol — write "Rs".
# 3. Present key numbers naturally woven into sentences.
# 4. If a specific data field is missing or the tool returned an error, say so briefly.
# 5. Use Indian number formatting (lakh, crore) for large figures.
# 6. Keep under 300 words unless the user explicitly asked for detail.
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
#     console.print("[Node MS] MCP synthesis (v11.0)")
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

#     # Hard cap to stay within LLM context window
#     mcp_snippet = mcp_result
#     if len(mcp_snippet) > 10000:
#         mcp_snippet = (
#             mcp_snippet[:7500]
#             + "\n\n... [middle trimmed for length] ...\n\n"
#             + mcp_snippet[-2000:]
#         )

#     prompt = MCP_SYNTHESIS_PROMPT.format(
#         history    = _format_history(state),
#         user_query = state.get("user_query", ""),
#         mcp_result = mcp_snippet,
#     )
#     try:
#         state["final_answer"] = _llm_text(prompt)
#     except Exception as e:
#         console.print(f"  MCP synthesis failed: {e}")
#         state["final_answer"] = mcp_result   # fallback to raw data
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


# # ══════════════════════════════════════════════════════════════════════════════
# # Router (v11.0)
# # ══════════════════════════════════════════════════════════════════════════════

# def route_after_classify(state: AgentState) -> str:
#     qt  = state.get("query_type", "general")
#     mcp = state.get("mcp_needed", False)

#     intents    = state.get("intents") or []
#     additional = state.get("companies") or []

#     for i, intent in enumerate(intents):
#         console.print(
#             f"  [Router] intent[{i}] entity={intent.get('entity')} "
#             f"type={intent.get('entity_type')} "
#             f"tools={len(intent.get('matched_tools') or [])} "
#             f"tool_hint={intent.get('tool_hint')}"
#         )

#     # TVS results are authoritative for mcp_needed
#     if not mcp and intents:
#         mcp = any(bool(intent.get("matched_tools")) for intent in intents)
#         if mcp:
#             console.print("  → mcp_needed upgraded to True by TVS results")

#     has_many = len(additional) > 0 or len(intents) > 1

#     primary_type    = state.get("primary_entity_type", "general")
#     secondary_types = {c.get("entity_type", "general") for c in additional}
#     is_heterogeneous = bool(secondary_types - {"general"} - {primary_type})

#     if qt == "greeting":
#         return "greeting"

#     if not mcp:
#         return "general"

#     if has_many or qt == "comparison" or is_heterogeneous:
#         console.print(
#             f"  → multi_mcp "
#             f"(has_many={has_many}, comparison={qt == 'comparison'}, "
#             f"heterogeneous={is_heterogeneous})"
#         )
#         return "multi_mcp"

#     console.print(f"  → mcp_direct (single entity, type={primary_type})")
#     return "mcp_direct"


# # ══════════════════════════════════════════════════════════════════════════════
# # Graph (v11.0)
# # ══════════════════════════════════════════════════════════════════════════════

# def build_graph() -> Any:
#     g = StateGraph(AgentState)

#     g.add_node("classify_extract_decompose", node_classify_extract_and_decompose)
#     g.add_node("per_intent_tool_search",     node_per_intent_tool_search)
#     g.add_node("greeting_handler",           node_greeting_handler)
#     g.add_node("general_handler",            node_general_handler)
#     g.add_node("synthesis",                  node_synthesis)
#     g.add_node("mcp_synthesis",              node_mcp_synthesis)
#     g.add_node("mcp_pre_resolve",            node_mcp_pre_resolve)
#     g.add_node("mcp_multi_pre_resolve",      node_mcp_multi_pre_resolve)
#     g.add_node("mcp_tool_call_intents",      node_mcp_tool_call_intents)

#     g.set_entry_point("classify_extract_decompose")

#     g.add_edge("classify_extract_decompose", "per_intent_tool_search")

#     g.add_conditional_edges(
#         "per_intent_tool_search",
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

#     g.add_edge("mcp_pre_resolve",       "mcp_tool_call_intents")
#     g.add_edge("mcp_multi_pre_resolve", "mcp_tool_call_intents")
#     g.add_edge("mcp_tool_call_intents", "mcp_synthesis")
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
graph.py — EQUIFIZ Financial AI Agent (v11.1)

Key changes from v11.0
──────────────────────────────────────────────────────────────────────
1. _TOOL_FAMILY_HINTS fully corrected — every tool mapped by its actual
   required URL parameter(s), not guessed from name prefix:
     co_code       → "stock"
     mf_schcode    → "mf_scheme"
     mf_cocode     → "mf_amc"
     isin          → "etf"
     index_code    → "index"
     group         → "market"   (resolved from group_master)
     ex only       → "exchange"
     no dyn params → "general"

2. PARAM_TO_TABLE_MAP — added "group" entry pointing to group_master.

3. ENTITY_TYPE_PARAM_MAP — added "market" → ["group"] and
   "exchange" → [] entries.

4. _resolve_entity_codes — added group resolution branch.

5. All v11.0 logic preserved unchanged otherwise.
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
from pathlib import Path
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

# ── Types ─────────────────────────────────────────────────────────────────────

class Turn(TypedDict):
    role: str       # "user" | "assistant"
    content: str


class IntentItem(TypedDict, total=False):
    entity:             str
    entity_type:        str   # stock|mf_scheme|mf_amc|etf|index|market|exchange|general
    intent_description: str
    tool_hint:          str
    scheme_name:        Optional[str]
    amc_name:           Optional[str]
    nse_symbol:         Optional[str]
    matched_tools:      list[dict]
    resolved_codes:     dict
    mcp_result:         str


# ══════════════════════════════════════════════════════════════════════════════
# Tool-family hints — mapped by the actual required parameter each tool uses
# ══════════════════════════════════════════════════════════════════════════════
#
# Legend:
#   stock     — requires co_code         (company / equity)
#   mf_scheme — requires mf_schcode      (mutual-fund scheme)
#   mf_amc    — requires mf_cocode       (mutual-fund AMC / fund-house)
#   etf       — requires isin            (exchange-traded fund)
#   index     — requires index_code      (market index)
#   market    — requires group           (market-wide group / segment, resolved from group_master)
#   exchange  — requires ex only         (exchange-level data, no DB entity)
#   general   — no dynamic entity param  (IPOs, NFO, news, master lists, etc.)
#
# Every entry is keyed on the EXACT tool-name prefix used in Chroma metadata.
# ──────────────────────────────────────────────────────────────────────────────

_TOOL_FAMILY_HINTS: dict[str, str] = {

    # ── Stock: requires co_code ───────────────────────────────────────────────
    "get_company_stock":        "stock",   # GetQuotes/{co_code}/{ex}
    "get_delayed_stock":        "stock",   # BseNseDelayedPrice (co_code variant)
    "get_company_profile":      "stock",   # CompanyProfile/{co_code}
    "get_company_backgroun":    "stock",   # CompBackground/{co_code}
    "get_board_of":             "stock",   # BoardOfDirectors/{co_code}
    "get_company_bankers":      "stock",   # Bankers/{co_code}
    "get_management":           "stock",   # Biodata/{co_code}
    "get_subsidiaries":         "stock",   # Subsidiaries_JVs_Collaborations/{co_code}
    "get_related_party":        "stock",   # Related_Party_transaction/{co_code}
    "get_employee_count":       "stock",   # EmployeeCount/{co_code}
    "get_capital_structure":    "stock",   # capital-structure/{co_code}
    "get_pledge_share":         "stock",   # Pledgesharesdetails/{co_code}
    "get_substantial":          "stock",   # SubstantialAcquisition/{co_code}
    "get_segment_data":         "stock",   # SegmentGeographyWiseNew/{co_code}/{report_type}
    "get_r_and_d":              "stock",   # R_and_D/{co_code}
    "get_finished_products":    "stock",   # FinishedProducts/{co_code}
    "get_raw_materials":        "stock",   # RawMaterials/{co_code}
    "get_chronological":        "stock",   # ChronologicalHistory/{co_code}
    "get_company_history":      "stock",   # CompanyHistory/{co_code}
    # Financials
    "get_quarterly_results":    "stock",   # QuarterlyResults/{co_code}/{t}
    "get_profit_loss":          "stock",   # ProftandLoss/{co_code}/{t}
    "get_balance_sheet":        "stock",   # BalanceSheet/{co_code}/{t}
    "get_cash_flow":            "stock",   # CashFlow/{co_code}/{t}
    "get_half_yearly":          "stock",   # Half-Yearly-Results/{co_code}/{t}
    "get_nine_months":          "stock",   # Nine-Month-Result/{co_code}/{t}
    "get_yearly_results":       "stock",   # Yearly-Results/{co_code}/{t}
    "get_quarterly_balance":    "stock",   # QuarterlyResults-BalanceSheet/{co_code}/{t}
    "get_annual_balance":       "stock",   # Results-BalanceSheet-Yearly/{co_code}/{t}
    "get_ttm_growth":           "stock",   # TTMPATNetSales/{co_code}/{t}
    "get_quarterly_revenue":    "stock",   # QuarterlyTrendsrevenue/{co_code}
    "get_quarterly_ebitda":     "stock",   # QuarterlyTrendEBITDA/{co_code}
    "get_quarterly_ebit":       "stock",   # QuarterlyTrendEBIT/{co_code}
    "get_growth_data":          "stock",   # GrowthDataQuarterly/Yearly/{co_code}/{t}
    # Ratios
    "get_key_financial":        "stock",   # KeyFinancialRatios/{co_code}/{t}
    "get_daily_ratios":         "stock",   # DailyRatios/{co_code}/{t}
    "get_margin_ratios":        "stock",   # MarginRatios/{co_code}/{t}
    "get_valuation_ratios":     "stock",   # ValuationRatios/{co_code}/{t}
    "get_all_basic_ratios":     "stock",   # Allbasicratio/{co_code}/{t}
    "get_return_ratios":        "stock",   # RatiosReturn/{co_code}/{t}
    "get_growth_ratios":        "stock",   # GrowthRatio/{co_code}/{t}
    "get_performance_ratio":    "stock",   # PerformanceRatios/{co_code}/{t}
    "get_cashflow_ratios":      "stock",   # CashFlowRatios/{co_code}/{t}
    "get_liquidity_ratios":     "stock",   # LiquidityRatios/{co_code}/{t}
    "get_solvency_ratios":      "stock",   # RatiosSolvency/{co_code}/{t}
    "get_quarterly_ratios":     "stock",   # QuarterlyRatio/{co_code}/{t}
    "get_yearly_ratios":        "stock",   # YearlyRatio/{co_code}/{t}
    "get_shareholding":         "stock",   # ShareHoldingPatternDetailed/{co_code}
    "get_major_sharehold":      "stock",   # ShareholdingMorethanOnePercent/{co_code}
    # IPO tools that DO need co_code (specific company IPO data)
    "get_anchor_investor":      "stock",   # AnchorInvestorDetails/{co_code}
    "get_basis_of":             "stock",   # BasisOfAllotment/{n}  — uses co_code variant

    # ── Index: requires index_code ────────────────────────────────────────────
    "get_market_indices":       "stock",   # Indices  (no param, but intent is index-level)
    "get_index_companies":       "index",

    # ── Market: requires group (resolved from group_master) ───────────────────
    "get_active_performer":     "market",  # MostActiveToppers/{ex}/{group}/value/{record_count}
    "get_top_gainers":          "market",  # Gainers/{ex}/{group}/{record_count}
    "get_top_losers":           "market",  # losers/{ex}/{group}/{record_count}
    "get_out_under":            "market",  # OutUnderPerformers/{ex}/{group}/{performer}/{record_count}
    "get_52week":               "market",  # FiftyTwoWeekHighEOD/{ex}/{group}/{record_count}
    "get_new_highs":            "market",  # NewHigh-NewLowEOD/{group}/{high_or_low}/{period}/{record_count}
    "get_sector_companies":     "market",  # SectorWiseComp/{sector_code}  — sector maps to group_master

    # ── Exchange: requires ex only — no DB entity resolution needed ───────────
    "get_advance_decline":      "exchange",  # AdvancesDeclines/{ex}
    "get_exchange_holidays":    "exchange",  # ExchangeHolidays/{ex}

    # ── MF Scheme: requires mf_schcode ────────────────────────────────────────
    "get_scheme_nav":           "mf_scheme",  # DailyNAV / SchemeNAVHistorical/{mf_schcode}
    "get_investment_detail":    "mf_scheme",  # InvestmentDetails/{mf_schcode}
    "get_expense_ratio":        "mf_scheme",  # ExpenseRatios  (mf_schcode filter)
    "get_avg_maturity":         "mf_scheme",  # AvgerageMaturityData (mf_schcode filter)
    "get_scheme_aum":           "mf_scheme",  # SchemeAUMHist (mf_schcode filter)
    "get_nav_historical":       "mf_scheme",  # SchemeNAVHistorical/{mf_schcode}/{period}/{periodval}
    "get_scheme_returns":       "mf_scheme",  # SchemeReturns/{mf_schcode}
    "get_lumpsum_returns":      "mf_scheme",  # LumpSumSchemereturnDetails/{mf_schcode}
    "get_scheme_sip":           "mf_scheme",  # SchemeSIPSWPdetails/{mf_schcode}
    "get_mf_holdings":          "mf_scheme",  # MFHolding/{mf_schcode}
    "get_sector_allocation":    "mf_scheme",  # SchemeSectorAllocation/{mf_schcode}
    "get_asset_allocation":     "mf_scheme",  # SchemeAssetAllocation/{mf_schcode}
    "get_portfolio_changes":    "mf_scheme",  # Whats_InOut/{type}/{mf_schcode}
    "get_mcap_allocation":      "mf_scheme",  # MCAP_EquityHolding/{mf_schcode}
    "get_most_bought":          "mf_scheme",  # MostsoldBought/{mf_schcode}
    "get_scheme_ratios":        "mf_scheme",  # Scheme_Ratios (mf_schcode filter)
    "get_dividend_details":     "mf_scheme",  # DividendDetails/{mf_schcode}
    "get_bse_star_scheme":      "mf_scheme",  # BSEStarSchemeMaster/{mf_schcode}
    "compare_schemes":          "mf_scheme",  # SchemeComparsion/{schcodes}
    "get_whats_in_out":         "mf_scheme",  # Whats_InOut/{type}/{mf_schcode}

    # ── MF AMC: requires mf_cocode ────────────────────────────────────────────
    "get_fund_categories":      "mf_amc",   # FundCategoryAMCWise/{mf_cocode}/{category}
    "get_schemes_by_amc":       "mf_amc",   # SchemeMaster/{mf_cocode}
    "get_fund_profile":         "mf_amc",   # fund-profile/{mf_cocode}

    # ── ETF: requires isin ────────────────────────────────────────────────────
    "get_etf_quotes":           "etf",      # ETFGetQuotes/{ex}/{isin}
    "get_etf_returns":          "etf",      # ETFReturns/{isin}
    "get_etf_fundamentals":     "etf",      # ETFFundamentals/{isin}
    "get_etf_about":            "etf",      # ETFAboutus/{isin}
    "get_etf_equity_holdings":  "etf",      # ETFShareHoldingEquity/{isin}
    "get_etf_monthly_portfo":   "etf",      # ETFMonthlyPortfolioAllHoldings/{isin}
    "get_etf_sector_allocatio": "etf",      # ETFSectorAllocation/{isin}
    "get_etf_asset_allocation": "etf",      # ETFAssetAllocation/{isin}
    "get_etf_":                 "etf",      # generic ETF prefix

    # ── General: no dynamic entity param ─────────────────────────────────────
    "get_forthcoming_ipo":      "general",  # forthcomingipo/{ex}/Ipo/{n}
    "get_open_ipos":            "general",  # OpenIssues/{ex}/Ipo/{n}
    "get_closed_ipos":          "general",  # ClosedIssues/{ex}/IPO/{n}
    "get_new_ipo":              "general",  # Newlisting/{ex}/{n}
    "get_best_ipo":             "general",  # BestPerformerIpo/{ex}/{n}
    "get_new_fund_offer":       "general",  # NewFundOffer
    "get_fund_house":           "general",  # Fund_House  (master list)
    "get_amfi_master":          "general",  # AMFIMaster
    "get_fund_manager":         "general",  # FundManager
    "get_index_list":           "general",  # IndexList
    "get_results_today":        "general",  # Today-Results
    "get_result_declarations":  "general",  # ResultDataDeclarations/{date}
    "get_annual_declarations":  "general",  # AnnualDataDeclarations/{date}
    "get_bse_announcement":     "general",  # BSEAnnouncement
    "get_nse_announcement":     "general",  # NSEAnnouncement
    "get_corporate_news":       "general",  # CapitalMarketLiveNews/corporate-news/{n}
    "get_mf_news":              "general",  # MF_News/{sno}
    "get_mf_activities":        "general",  # MFActivities
    "get_ipo_master":           "general",  # ipomaster
    "get_ipo_prospectus":       "general",  # IPOProspectus/sebi
    "get_ipo_logo":             "general",  # IPOCompanyLogo
    "get_fund_performance":     "general",  # FundPerformance/{top}/{type}/{category}
    "get_category_performance": "general",  # CategoryPerformance/{type}/{top}
    "get_sip_dates":            "general",  # SIP_Dates/{plan}
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
    # NEW — market group (gainers, losers, active performers, sector companies, etc.)
    # group_master stores all exchange groups/segments (e.g. "A", "B", "SME", "Nifty 50")
    "group": {
        "table":    "group_master",
        "column":   "group_name",
        "id_field": "group_name",   # the API expects the group_name string itself
    },
}

# Maps entity_type → the DB param(s) it needs
ENTITY_TYPE_PARAM_MAP: dict[str, list[str]] = {
    "stock":     ["co_code"],
    "mf_scheme": ["mf_schcode"],
    "mf_amc":    ["mf_cocode"],
    "etf":       ["isin"],
    "index":     ["index_code"],
    "market":    ["group"],      # NEW — group resolved from group_master
    "exchange":  [],             # ex is a known constant ("NSE"/"BSE"), no DB lookup
    "general":   [],             # no entity code needed; do NOT do DB lookup
}

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "equifiz",
    "user":     "postgres",
    "password": "1234",
}

TOOL_SCORE_THRESHOLD = 0.55

# ── Chroma (read-only) ────────────────────────────────────────────────────────
from chroma_singleton import get_chroma_collection
try:
    _tool_collection = get_chroma_collection()
    console.print(f"  [ToolRegistry] Chroma ready — {_tool_collection.count()} tools")
except Exception as e:
    console.print(f"  [ToolRegistry] Chroma init failed: {e}")
    _tool_collection = None


# ── Agent State ───────────────────────────────────────────────────────────────

class AgentState(TypedDict, total=False):
    user_query:           str
    conversation_history: list[Turn]
    force_refresh:        bool

    query_type:            str
    extracted_entity:      str
    report_type:           str
    intent:                str
    is_broad:              bool
    mcp_needed:            bool
    mcp_tool_hint:         str

    primary_entity_type:  str
    primary_tool_hint:    str
    primary_query_intent: str

    matched_tools: list[dict]
    intents: list[IntentItem]
    intent_results: list[dict]

    nse_symbol:   Optional[str]
    co_code:      Optional[int]
    company_info: Optional[dict]

    mcp_resolved_codes: dict
    mcp_scheme_name:    Optional[str]
    mcp_amc_name:       Optional[str]

    companies: list[dict]

    vector_context: list[dict]

    mcp_raw_result:      str
    mcp_tool_calls_made: list[str]

    final_answer: str
    error:        Optional[str]


# ── LLM helpers ───────────────────────────────────────────────────────────────

def _llm_json(prompt: str) -> dict:
    resp      = llm.invoke([HumanMessage(content=prompt)])
    text      = resp.content.strip()
    clean     = re.sub(r"^```(?:json)?|```$", "", text, flags=re.MULTILINE).strip()

    try:
        return json.loads(clean)
    except json.JSONDecodeError:
        pass

    match = re.search(r"(\{.*\})", clean, re.DOTALL)
    if match:
        greedy = match.group(1)
        try:
            return json.loads(greedy)
        except json.JSONDecodeError:
            try:
                fixed = re.sub(r",\s*([\]}])", r"\1", greedy.replace("'", '"'))
                return json.loads(fixed)
            except Exception:
                pass

    logger.error(f"[JSON FAIL] Raw LLM Output:\n{text}")
    raise ValueError("Could not parse JSON from LLM response.")


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
# Node AIQ: Classify + Extract + Decompose
# ══════════════════════════════════════════════════════════════════════════════

CLASSIFY_EXTRACT_DECOMPOSE_PROMPT = """\
You are a Lead Financial Systems Analyst for an Indian stock-market assistant. Your task is to decompose user queries into executable data intents.

## Context
- History: {history}
- Today's Date: {today}
- Query: "{query}"

────────────────────────────────────────────────────────────
STEP 1: SUBJECT IDENTIFICATION
Identify every distinct subject the user is asking about. A subject is either:
1. A Specific Entity: (e.g., Reliance Industries, HDFC AMC, Nifty 50, SBI Small Cap Fund, Gold BeES ETF).
2. A Market Category/Concept: (e.g., "upcoming IPOs", "top gainers", "market news", "bulk deals", "top losers on NSE").

Example Decomposition:
- "Price of Reliance and upcoming IPOs"
  -> Subject 1: Reliance (Entity)
  -> Subject 2: Upcoming IPOs (Concept)

STEP 2: JSON GENERATION
Generate exactly one intent item for every subject identified in Step 1.

Return ONLY valid JSON:
{{
  "query_type": "<greeting|general|stock|mf_scheme|mf_amc|comparison|investment>",
  "is_broad": <true|false>,
  "mcp_needed": <true|false>,
  "intents": [
    {{
      "entity": "<entity name OR market concept name>",
      "entity_type": "<stock|mf_scheme|mf_amc|etf|index|market|exchange|general>",
      "intent_description": "<specific data point needed for this subject>",
      "scheme_name": "<if mf_scheme, else empty>",
      "amc_name": "<if mf_amc, else empty>",
      "nse_symbol": "<NSE ticker if known, else empty>"
    }}
  ]
}}

────────────────────────────────────────────────────────────
DECOMPOSITION RULES (CRITICAL):
1. THE 1:1 MAPPING: len(intents) MUST equal the number of subjects identified in Step 1.
2. NO BUNDLING: If a query asks for two different subjects (e.g., a stock price and a market trend), return two separate intent objects.
3. ENTITY TYPE ASSIGNMENT:
   - "Reliance", "TCS", "HDFC Bank" → entity_type = stock
   - "SBI Mutual Fund", "Nippon AMC" → entity_type = mf_amc
   - "Parag Parikh Flexi Cap", "Axis Bluechip" → entity_type = mf_scheme
   - "Gold BeES", "Nifty BeES" → entity_type = etf
   - "Nifty 50", "Sensex", index lists → entity_type = index
   - "Top gainers", "Top losers", "Most active", "52-week high", sector performers → entity_type = market
   - "Advances/Declines on NSE", "Exchange holidays" → entity_type = exchange
   - Concepts like "IPOs", "NFO", market news, master lists, educational definitions → entity_type = general
4. DATA POINT MERGING: If a user asks for multiple metrics on the SAME entity, return ONE intent. List all metrics in intent_description.

MCP_NEEDED LOGIC:
- Set to true if ANY intent requires fetching live market data (prices, IPO lists, NAVs, ratios, etc.).
- Set to false only for pure greetings or purely educational/definitional questions.

NEGATIVE EXAMPLES (DO NOT DO THESE):
✗ User: "Price of Reliance and IPOs" → 1 intent (Wrong: Market concepts are separate subjects)
✗ User: "PE of TCS and EPS of Wipro" → 1 intent (Wrong: These are two distinct entities)
────────────────────────────────────────────────────────────
"""


def node_classify_extract_and_decompose(state: AgentState) -> AgentState:
    console.print("[Node AIQ] Classify + extract + decompose (v11.1)")

    user_query   = state.get("user_query", "")
    history_text = _format_history(state, n=4)

    state["matched_tools"] = []
    state["intents"]       = []
    state["companies"]     = []

    try:
        result = _llm_json(
            CLASSIFY_EXTRACT_DECOMPOSE_PROMPT.format(
                history = history_text,
                today   = TODAY,
                query   = user_query,
            )
        )

        qt          = result.get("query_type", "general")
        mcp         = bool(result.get("mcp_needed", False))
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

        if not intents and qt not in ("greeting", "general"):
            console.print("  ⚠ LLM returned no intents — fallback to single general intent")
            intents.append(IntentItem(
                entity             = user_query,
                entity_type        = "general",
                intent_description = user_query,
                scheme_name        = None,
                amc_name           = None,
                nse_symbol         = None,
                tool_hint          = "",
                matched_tools      = [],
                resolved_codes     = {},
                mcp_result         = "",
            ))

        state["query_type"] = qt
        state["mcp_needed"] = mcp
        state["intents"]    = intents

        if intents:
            first = intents[0]
            state["primary_entity_type"]  = first.get("entity_type", "general")
            state["primary_query_intent"] = first.get("intent_description", "")
            state["extracted_entity"]     = first.get("entity", user_query)
            state["mcp_scheme_name"]      = first.get("scheme_name")
            state["mcp_amc_name"]         = first.get("amc_name")
            state["nse_symbol"]           = first.get("nse_symbol")
        else:
            state["primary_entity_type"]  = "general"
            state["primary_query_intent"] = user_query
            state["extracted_entity"]     = user_query
            state["mcp_scheme_name"]      = None
            state["mcp_amc_name"]         = None
            state["nse_symbol"]           = None

        state["companies"] = [
            {
                "name":         i.get("entity", ""),
                "entity_type":  i.get("entity_type", "general"),
                "tool_hint":    "",
                "query_intent": i.get("intent_description", ""),
                "scheme_name":  i.get("scheme_name"),
                "amc_name":     i.get("amc_name"),
                "nse_symbol":   i.get("nse_symbol"),
            }
            for i in intents[1:]
        ]

        console.print(
            f"  → type={qt} | mcp={mcp} | {len(intents)} intent(s): "
            + "; ".join(
                f"[{i['entity']} / {i['entity_type']}] {i['intent_description'][:55]}"
                for i in intents
            )
        )

    except Exception as e:
        console.print(f"  [ERROR] Node AIQ failed: {e}")
        state["query_type"]           = "general"
        state["mcp_needed"]           = False
        state["primary_entity_type"]  = "general"
        state["primary_query_intent"] = user_query
        state["extracted_entity"]     = user_query
        state["companies"]            = []
        state["intents"]              = []

    return state


# ══════════════════════════════════════════════════════════════════════════════
# Chroma tool registry helpers (read-only)
# ══════════════════════════════════════════════════════════════════════════════

def _query_tool_registry(
    query: str,
    n_results: int = 8,
    score_threshold: float = TOOL_SCORE_THRESHOLD,
) -> list[dict]:
    if _tool_collection is None:
        return []
    try:
        count = _tool_collection.count()
        if count == 0:
            return []

        res = _tool_collection.query(
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
    if _tool_collection is None:
        return None
    try:
        res = _tool_collection.get(ids=[tool_name], include=["metadatas"])
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


# ══════════════════════════════════════════════════════════════════════════════
# Node TVS: Per-intent tool vector search
# ══════════════════════════════════════════════════════════════════════════════

def node_per_intent_tool_search(state: AgentState) -> AgentState:
    console.print("[Node TVS] Per-intent tool vector search (v11.1)")

    intents = state.get("intents") or []
    if not intents:
        console.print("  ⚠ No intents to search for")
        return state

    def _search_for_intent(intent: IntentItem) -> IntentItem:
        et = intent.get("entity_type", "general")

        # For general/no-code intents, search purely on intent description
        if et in ("general", "exchange"):
            search_query = intent["intent_description"]
        else:
            search_query = f"{et} {intent['intent_description']}"

        tools = _query_tool_registry(search_query, n_results=8)

        # For typed entities, prefer tools that match the family
        if et not in ("general", "exchange", "") and tools:
            family_tools = [
                t for t in tools
                if _infer_entity_type_from_tool(t["tool_name"]) in (et, "general")
            ]
            if family_tools:
                tools = family_tools

        tool_hint = tools[0]["tool_name"] if tools else ""

        if tool_hint:
            console.print(
                f"  [{intent['entity']}] "
                f"intent='{intent['intent_description'][:50]}' "
                f"→ top tool={tool_hint} ({len(tools)} matched)"
            )
        else:
            console.print(
                f"  [{intent['entity']}] "
                f"intent='{intent['intent_description'][:50]}' "
                f"→ no tools matched"
            )

        return IntentItem(
            entity             = intent.get("entity", ""),
            entity_type        = et,
            intent_description = intent.get("intent_description", ""),
            scheme_name        = intent.get("scheme_name"),
            amc_name           = intent.get("amc_name"),
            nse_symbol         = intent.get("nse_symbol"),
            tool_hint          = tool_hint,
            matched_tools      = tools,
            resolved_codes     = {},
            mcp_result         = "",
        )

    updated: dict[int, IntentItem] = {}
    with ThreadPoolExecutor(max_workers=min(6, len(intents))) as ex:
        futures = {
            ex.submit(_search_for_intent, intent): idx
            for idx, intent in enumerate(intents)
        }
        for future in as_completed(futures):
            idx = futures[future]
            try:
                updated[idx] = future.result()
            except Exception as exc:
                console.print(f"  [red]TVS failed for intent #{idx}: {exc}[/red]")
                original    = intents[idx]
                updated[idx] = IntentItem(
                    entity             = original.get("entity", ""),
                    entity_type        = original.get("entity_type", "general"),
                    intent_description = original.get("intent_description", ""),
                    scheme_name        = original.get("scheme_name"),
                    amc_name           = original.get("amc_name"),
                    nse_symbol         = original.get("nse_symbol"),
                    tool_hint          = "",
                    matched_tools      = [],
                    resolved_codes     = {},
                    mcp_result         = "",
                )

    state["intents"] = [updated[i] for i in sorted(updated)]

    if state["intents"]:
        first = state["intents"][0]
        state["matched_tools"]     = first.get("matched_tools") or []
        state["mcp_tool_hint"]     = first.get("tool_hint") or ""
        state["primary_tool_hint"] = first.get("tool_hint") or ""

    return state


# ── Greeting handler ──────────────────────────────────────────────────────────

GREETING_PROMPT = """\
You are EQUIFIZ — a friendly Indian stock-market AI assistant.
Respond warmly and briefly to the user's greeting or small talk.
Mention 1-2 things you can help with (PE ratios, quarterly results,
investment recommendations, live NAV, MF returns, IPO listings, etc.).

User: {query}
"""

def node_greeting_handler(state: AgentState) -> AgentState:
    console.print("[Node B] Greeting handler")
    state["final_answer"] = _llm_text(GREETING_PROMPT.format(query=state["user_query"]))
    return state


# ── General handler ───────────────────────────────────────────────────────────

def node_general_handler(state: AgentState) -> AgentState:
    console.print("[Node C] General handler")
    try:
        state["vector_context"] = vs.query(state["user_query"], n_results=5)
    except Exception as e:
        console.print(f"  General vector search failed: {e}")
        state["vector_context"] = []
    return state


# ── Symbol resolution helper ──────────────────────────────────────────────────

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
            cur.execute("SELECT mf_schcode, mf_cocode, sch_name, category FROM scheme_master")
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
    Returns the list of DB params this entity needs resolved.
    Returns [] for 'general' and 'exchange' entity types — no DB lookup needed.
    """
    # These types carry no DB-resolvable entity code
    if entity_type in ("general", "exchange"):
        console.print(f"  📋 entity_type='{entity_type}' → no DB params needed")
        return []

    if entity_type and entity_type not in ("general", "exchange"):
        params = ENTITY_TYPE_PARAM_MAP.get(entity_type, [])
        console.print(f"  📋 entity_type='{entity_type}' → authoritative params: {params}")
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
# Per-entity DB code resolver
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
    needs_group     = "group"       in required_params   # NEW

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

    # NEW — resolve market group from group_master
    if needs_group:
        cfg   = PARAM_TO_TABLE_MAP["group"]
        match = _targeted_db_lookup(
            entity=name, table=cfg["table"],
            name_col=cfg["column"], id_col=cfg["id_field"],
        )
        if match:
            # The API expects the group_name string (e.g. "A", "SME", "Nifty 50")
            codes["group"] = match["group_name"]
            result["resolved_group_name"] = match["group_name"]
        else:
            # Fall back to the raw entity name so the MCP call still proceeds
            console.print(f"  ⚠ group not found in group_master for '{name}' — using raw name")
            codes["group"] = name
            result["resolved_group_name"] = name

    result["mcp_resolved_codes"] = codes
    return result


# ── Resolve codes for all intents ─────────────────────────────────────────────

def _resolve_intents_codes(intents: list[IntentItem]) -> list[IntentItem]:
    def _resolve_one(intent: IntentItem) -> IntentItem:
        et        = intent.get("entity_type", "general")
        tool_hint = intent.get("tool_hint", "")
        matched   = intent.get("matched_tools") or []

        # If entity_type is still 'general', try to infer from tool_hint
        if et == "general" and tool_hint:
            inferred = _infer_entity_type_from_tool(tool_hint)
            if inferred != "general":
                et = inferred
                intent["entity_type"] = et

        req_params = _get_required_params_for_entity(tool_hint, et, matched)

        # Skip DB entirely for general/exchange/no-code intents
        if not req_params:
            console.print(
                f"  💎 [{intent['entity']}] entity_type='{et}' "
                f"→ no code resolution needed"
            )
            intent["resolved_codes"] = {}
            return intent

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
        futures = {
            ex.submit(_resolve_one, intent): idx
            for idx, intent in enumerate(intents)
        }
        for future in as_completed(futures):
            idx = futures[future]
            try:
                result_intents[idx] = future.result()
            except Exception as exc:
                console.print(f"  [red]Code resolution failed for intent #{idx}: {exc}[/red]")
                result_intents[idx] = intents[idx]

    return [result_intents[i] for i in sorted(result_intents)]


# ══════════════════════════════════════════════════════════════════════════════
# MCP Pre-Resolution nodes
# ══════════════════════════════════════════════════════════════════════════════

def node_mcp_pre_resolve(state: AgentState) -> AgentState:
    console.print("[Node MPR] Single-entity MCP pre-resolve (v11.1)")

    intents = state.get("intents") or []
    if not intents:
        console.print("  ⚠ No intents; skipping resolve")
        state["mcp_resolved_codes"] = {}
        return state

    resolved_intents        = _resolve_intents_codes(intents)
    state["intents"]        = resolved_intents
    state["mcp_resolved_codes"] = resolved_intents[0].get("resolved_codes", {}) if resolved_intents else {}

    console.print(f"  🏁 Primary resolved codes: {state['mcp_resolved_codes']}")
    return state


def node_mcp_multi_pre_resolve(state: AgentState) -> AgentState:
    console.print("[Node MMPR] Multi-entity MCP pre-resolve (v11.1)")

    intents = state.get("intents") or []
    if not intents:
        console.print("  ⚠ No intents; skipping resolve")
        return state

    resolved_intents            = _resolve_intents_codes(intents)
    state["intents"]            = resolved_intents
    state["mcp_resolved_codes"] = resolved_intents[0].get("resolved_codes", {}) if resolved_intents else {}

    console.print(
        f"  🏁 Resolved {len(resolved_intents)} intents. "
        f"Primary codes: {state['mcp_resolved_codes']}"
    )
    return state


# ══════════════════════════════════════════════════════════════════════════════
# Node MTCI: MCP Tool Call — sequential per-intent in one session (v11.1)
# ══════════════════════════════════════════════════════════════════════════════

def _run_mcp_multi_in_thread(intents: list[dict]) -> list[dict]:
    """
    Run run_mcp_query_multi in a fresh event loop (called from a thread).
    """
    from mcp_client import run_mcp_query_multi, trim_results

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        results = loop.run_until_complete(run_mcp_query_multi(intents))
        return trim_results(results)
    except ExceptionGroup as eg:
        msg = "; ".join(str(e) for e in eg.exceptions)
        console.print(f"[bold red]MCP TaskGroup Error:[/bold red] {msg}")
        return [{**i, "mcp_result": f"MCP Error: {msg}"} for i in intents]
    except Exception as e:
        console.print(f"[bold red]MCP Error:[/bold red] {e}")
        return [{**i, "mcp_result": f"Error: {e}"} for i in intents]
    finally:
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
            pending = asyncio.all_tasks(loop)
            if pending:
                for task in pending:
                    task.cancel()
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        except Exception:
            pass
        loop.close()


def node_mcp_tool_call_intents(state: AgentState) -> AgentState:
    console.print("[Node MTCI] Sequential per-intent MCP calls — one session (v11.1)")

    try:
        import mcp_client  # noqa: F401
    except ImportError:
        console.print("MCP client module not found.")
        state["mcp_raw_result"] = ""
        state["intent_results"] = []
        state["error"]          = "mcp_client_not_found"
        return state

    intents    = state.get("intents") or []
    user_query = state.get("user_query", "")

    # Fallback: no intents at all
    if not intents:
        console.print("  ⚠ No intents; falling back to raw user query")
        from mcp_client import run_mcp_query
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            raw = loop.run_until_complete(run_mcp_query(user_query))
            loop.close()
        except Exception as exc:
            raw = f"MCP call failed: {exc}"
        state["mcp_raw_result"]      = raw
        state["intent_results"]      = []
        state["mcp_tool_calls_made"] = []
        state["error"]               = None
        return state

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            future         = ex.submit(_run_mcp_multi_in_thread, intents)
            filled_intents = future.result(timeout=600)
    except concurrent.futures.TimeoutError:
        console.print("  [red]Multi-intent MCP timed out[/red]")
        filled_intents = [
            {**dict(i), "mcp_result": "MCP call timed out."}
            for i in intents
        ]
    except Exception as exc:
        console.print(f"  [red]Multi-intent MCP failed: {exc}[/red]")
        filled_intents = [
            {**dict(i), "mcp_result": f"MCP call failed: {exc}"}
            for i in intents
        ]

    state["intents"] = filled_intents

    state["intent_results"] = [
        {
            "entity":             i.get("entity", ""),
            "entity_type":        i.get("entity_type", ""),
            "intent_description": i.get("intent_description", ""),
            "mcp_result":         i.get("mcp_result", ""),
        }
        for i in filled_intents
    ]

    state["mcp_raw_result"] = "\n\n---\n\n".join(
        f"[{r['entity']} / {r['intent_description']}]\n{r['mcp_result']}"
        for r in state["intent_results"]
        if r.get("mcp_result")
    )
    state["mcp_tool_calls_made"] = []
    state["error"]               = None

    total_chars = len(state["mcp_raw_result"])
    console.print(
        f"  ✅ {len(filled_intents)} intent(s) resolved. "
        f"Total chars for synthesis: {total_chars}"
    )
    return state


# ══════════════════════════════════════════════════════════════════════════════
# MCP Synthesis (v11.1)
# ══════════════════════════════════════════════════════════════════════════════

MCP_SYNTHESIS_PROMPT = """\
You are a knowledgeable Indian equity and mutual fund analyst assistant.

## Conversation history (last 4 turns)
{history}

## User Question
{user_query}

## Live Data Retrieved (one block per sub-question)
{mcp_result}

## Instructions
1. Answer EACH sub-question using the data block labelled for it above.
   If there are multiple data blocks, address each one in turn.
2. Write in plain prose paragraphs only. No markdown, no bullet points,
   no asterisks, no tables. Do NOT use the rupee symbol — write "Rs".
3. Present key numbers naturally woven into sentences.
4. If a specific data field is missing or the tool returned an error, say so briefly.
5. Use Indian number formatting (lakh, crore) for large figures.
6. Keep under 300 words unless the user explicitly asked for detail.
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
    console.print("[Node MS] MCP synthesis (v11.1)")
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

    mcp_snippet = mcp_result
    if len(mcp_snippet) > 10000:
        mcp_snippet = (
            mcp_snippet[:7500]
            + "\n\n... [middle trimmed for length] ...\n\n"
            + mcp_snippet[-2000:]
        )

    prompt = MCP_SYNTHESIS_PROMPT.format(
        history    = _format_history(state),
        user_query = state.get("user_query", ""),
        mcp_result = mcp_snippet,
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
# Router (v11.1)
# ══════════════════════════════════════════════════════════════════════════════

def route_after_classify(state: AgentState) -> str:
    qt  = state.get("query_type", "general")
    mcp = state.get("mcp_needed", False)

    intents    = state.get("intents") or []
    additional = state.get("companies") or []

    for i, intent in enumerate(intents):
        console.print(
            f"  [Router] intent[{i}] entity={intent.get('entity')} "
            f"type={intent.get('entity_type')} "
            f"tools={len(intent.get('matched_tools') or [])} "
            f"tool_hint={intent.get('tool_hint')}"
        )

    if not mcp and intents:
        mcp = any(bool(intent.get("matched_tools")) for intent in intents)
        if mcp:
            console.print("  → mcp_needed upgraded to True by TVS results")

    has_many = len(additional) > 0 or len(intents) > 1

    primary_type    = state.get("primary_entity_type", "general")
    secondary_types = {c.get("entity_type", "general") for c in additional}
    is_heterogeneous = bool(secondary_types - {"general"} - {primary_type})

    if qt == "greeting":
        return "greeting"

    if not mcp:
        return "general"

    if has_many or qt == "comparison" or is_heterogeneous:
        console.print(
            f"  → multi_mcp "
            f"(has_many={has_many}, comparison={qt == 'comparison'}, "
            f"heterogeneous={is_heterogeneous})"
        )
        return "multi_mcp"

    console.print(f"  → mcp_direct (single entity, type={primary_type})")
    return "mcp_direct"


# ══════════════════════════════════════════════════════════════════════════════
# Graph (v11.1)
# ══════════════════════════════════════════════════════════════════════════════

def build_graph() -> Any:
    g = StateGraph(AgentState)

    g.add_node("classify_extract_decompose", node_classify_extract_and_decompose)
    g.add_node("per_intent_tool_search",     node_per_intent_tool_search)
    g.add_node("greeting_handler",           node_greeting_handler)
    g.add_node("general_handler",            node_general_handler)
    g.add_node("synthesis",                  node_synthesis)
    g.add_node("mcp_synthesis",              node_mcp_synthesis)
    g.add_node("mcp_pre_resolve",            node_mcp_pre_resolve)
    g.add_node("mcp_multi_pre_resolve",      node_mcp_multi_pre_resolve)
    g.add_node("mcp_tool_call_intents",      node_mcp_tool_call_intents)

    g.set_entry_point("classify_extract_decompose")

    g.add_edge("classify_extract_decompose", "per_intent_tool_search")

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

    g.add_edge("greeting_handler",      END)
    g.add_edge("general_handler",       "synthesis")
    g.add_edge("synthesis",             END)

    g.add_edge("mcp_pre_resolve",       "mcp_tool_call_intents")
    g.add_edge("mcp_multi_pre_resolve", "mcp_tool_call_intents")
    g.add_edge("mcp_tool_call_intents", "mcp_synthesis")
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
# ## general redundancy 
# """
# graph.py — EQUIFIZ Financial AI Agent (v11.1)

# Key changes from v11.0
# ──────────────────────────────────────────────────────────────────────
# 1. _TOOL_FAMILY_HINTS fully corrected — every tool mapped by its actual
#    required URL parameter(s), not guessed from name prefix:
#      co_code       → "stock"
#      mf_schcode    → "mf_scheme"
#      mf_cocode     → "mf_amc"
#      isin          → "etf"
#      index_code    → "index"
#      group         → "market"   (resolved from group_master)
#      ex only       → "exchange"
#      no dyn params → "general"

# 2. PARAM_TO_TABLE_MAP — added "group" entry pointing to group_master.

# 3. ENTITY_TYPE_PARAM_MAP — added "market" → ["group"] and
#    "exchange" → [] entries.

# 4. _resolve_entity_codes — added group resolution branch.

# 5. All v11.0 logic preserved unchanged otherwise.
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
#     entity_type:        str   # stock|mf_scheme|mf_amc|etf|index|market|exchange|general
#     intent_description: str
#     tool_hint:          str
#     scheme_name:        Optional[str]
#     amc_name:           Optional[str]
#     nse_symbol:         Optional[str]
#     matched_tools:      list[dict]
#     resolved_codes:     dict
#     mcp_result:         str


# # ══════════════════════════════════════════════════════════════════════════════
# # Tool-family hints — mapped by the actual required parameter each tool uses
# # ══════════════════════════════════════════════════════════════════════════════
# #
# # Legend:
# #   stock     — requires co_code         (company / equity)
# #   mf_scheme — requires mf_schcode      (mutual-fund scheme)
# #   mf_amc    — requires mf_cocode       (mutual-fund AMC / fund-house)
# #   etf       — requires isin            (exchange-traded fund)
# #   index     — requires index_code      (market index)
# #   market    — requires group           (market-wide group / segment, resolved from group_master)
# #   exchange  — requires ex only         (exchange-level data, no DB entity)
# #   general   — no dynamic entity param  (IPOs, NFO, news, master lists, etc.)
# #
# # Every entry is keyed on the EXACT tool-name prefix used in Chroma metadata.
# # ──────────────────────────────────────────────────────────────────────────────

# _TOOL_FAMILY_HINTS: dict[str, str] = {

#     # ── Stock: requires co_code ───────────────────────────────────────────────
#     "get_company_stock":        "stock",   # GetQuotes/{co_code}/{ex}
#     "get_delayed_stock":        "stock",   # BseNseDelayedPrice (co_code variant)
#     "get_company_profile":      "stock",   # CompanyProfile/{co_code}
#     "get_company_backgroun":    "stock",   # CompBackground/{co_code}
#     "get_board_of":             "stock",   # BoardOfDirectors/{co_code}
#     "get_company_bankers":      "stock",   # Bankers/{co_code}
#     "get_management":           "stock",   # Biodata/{co_code}
#     "get_subsidiaries":         "stock",   # Subsidiaries_JVs_Collaborations/{co_code}
#     "get_related_party":        "stock",   # Related_Party_transaction/{co_code}
#     "get_employee_count":       "stock",   # EmployeeCount/{co_code}
#     "get_capital_structure":    "stock",   # capital-structure/{co_code}
#     "get_pledge_share":         "stock",   # Pledgesharesdetails/{co_code}
#     "get_substantial":          "stock",   # SubstantialAcquisition/{co_code}
#     "get_segment_data":         "stock",   # SegmentGeographyWiseNew/{co_code}/{report_type}
#     "get_r_and_d":              "stock",   # R_and_D/{co_code}
#     "get_finished_products":    "stock",   # FinishedProducts/{co_code}
#     "get_raw_materials":        "stock",   # RawMaterials/{co_code}
#     "get_chronological":        "stock",   # ChronologicalHistory/{co_code}
#     "get_company_history":      "stock",   # CompanyHistory/{co_code}
#     # Financials
#     "get_quarterly_results":    "stock",   # QuarterlyResults/{co_code}/{t}
#     "get_profit_loss":          "stock",   # ProftandLoss/{co_code}/{t}
#     "get_balance_sheet":        "stock",   # BalanceSheet/{co_code}/{t}
#     "get_cash_flow":            "stock",   # CashFlow/{co_code}/{t}
#     "get_half_yearly":          "stock",   # Half-Yearly-Results/{co_code}/{t}
#     "get_nine_months":          "stock",   # Nine-Month-Result/{co_code}/{t}
#     "get_yearly_results":       "stock",   # Yearly-Results/{co_code}/{t}
#     "get_quarterly_balance":    "stock",   # QuarterlyResults-BalanceSheet/{co_code}/{t}
#     "get_annual_balance":       "stock",   # Results-BalanceSheet-Yearly/{co_code}/{t}
#     "get_ttm_growth":           "stock",   # TTMPATNetSales/{co_code}/{t}
#     "get_quarterly_revenue":    "stock",   # QuarterlyTrendsrevenue/{co_code}
#     "get_quarterly_ebitda":     "stock",   # QuarterlyTrendEBITDA/{co_code}
#     "get_quarterly_ebit":       "stock",   # QuarterlyTrendEBIT/{co_code}
#     "get_growth_data":          "stock",   # GrowthDataQuarterly/Yearly/{co_code}/{t}
#     # Ratios
#     "get_key_financial":        "stock",   # KeyFinancialRatios/{co_code}/{t}
#     "get_daily_ratios":         "stock",   # DailyRatios/{co_code}/{t}
#     "get_margin_ratios":        "stock",   # MarginRatios/{co_code}/{t}
#     "get_valuation_ratios":     "stock",   # ValuationRatios/{co_code}/{t}
#     "get_all_basic_ratios":     "stock",   # Allbasicratio/{co_code}/{t}
#     "get_return_ratios":        "stock",   # RatiosReturn/{co_code}/{t}
#     "get_growth_ratios":        "stock",   # GrowthRatio/{co_code}/{t}
#     "get_performance_ratio":    "stock",   # PerformanceRatios/{co_code}/{t}
#     "get_cashflow_ratios":      "stock",   # CashFlowRatios/{co_code}/{t}
#     "get_liquidity_ratios":     "stock",   # LiquidityRatios/{co_code}/{t}
#     "get_solvency_ratios":      "stock",   # RatiosSolvency/{co_code}/{t}
#     "get_quarterly_ratios":     "stock",   # QuarterlyRatio/{co_code}/{t}
#     "get_yearly_ratios":        "stock",   # YearlyRatio/{co_code}/{t}
#     "get_shareholding":         "stock",   # ShareHoldingPatternDetailed/{co_code}
#     "get_major_sharehold":      "stock",   # ShareholdingMorethanOnePercent/{co_code}
#     # IPO tools that DO need co_code (specific company IPO data)
#     "get_anchor_investor":      "stock",   # AnchorInvestorDetails/{co_code}
#     "get_basis_of":             "stock",   # BasisOfAllotment/{n}  — uses co_code variant
#     "get_ipo_details":                  "stock",
#     "get_ipo_subscription_status":      "stock",
#     "get_ipo_synopsis":                 "stock",
#     "get_ipo_timeline":                 "stock",
#     "get_ipo_promoter_details":         "stock",
#     "get_ipo_listing_info":             "stock",
#     "get_ipo_objects_of_issue":         "stock",
#     "get_ipo_anchor_investor_details":  "stock",
#     "get_ipo_financials":               "stock",
#     "get_ipo_product_services":         "stock",
#     "get_ipo_strength_details":         "stock",
#     "get_ipo_strategy_details":         "stock",
#     "get_ipo_risk_details":             "stock",
#     "get_ipo_industry_peers":           "stock",
#     "get_ipo_selling_shareholders":     "stock",
#     "get_ipo_allocation_details":       "stock",
#     "get_ipo_prospectus":               "stock",
#     "get_ipo_lead_managers":            "stock",
#     "get_ipo_registrar":                "stock",

#     # ── Index: requires index_code ────────────────────────────────────────────
#     "get_market_indices":       "stock",   # Indices  (no param, but intent is index-level)
#     "get_index_companies":       "index",

#     # ── Market: requires group (resolved from group_master) ───────────────────
#     "get_active_performer":     "market",  # MostActiveToppers/{ex}/{group}/value/{record_count}
#     "get_top_gainers":          "market",  # Gainers/{ex}/{group}/{record_count}
#     "get_top_losers":           "market",  # losers/{ex}/{group}/{record_count}
#     "get_out_under":            "market",  # OutUnderPerformers/{ex}/{group}/{performer}/{record_count}
#     "get_52week":               "market",  # FiftyTwoWeekHighEOD/{ex}/{group}/{record_count}
#     "get_new_highs":            "market",  # NewHigh-NewLowEOD/{group}/{high_or_low}/{period}/{record_count}
#     "get_sector_companies":     "market",  # SectorWiseComp/{sector_code}  — sector maps to group_master

#     # ── Exchange: requires ex only — no DB entity resolution needed ───────────
#     "get_advance_decline":      "exchange",  # AdvancesDeclines/{ex}
#     "get_exchange_holidays":    "exchange",  # ExchangeHolidays/{ex}

#     # ── MF Scheme: requires mf_schcode ────────────────────────────────────────
#     "get_scheme_nav":           "mf_scheme",  # DailyNAV / SchemeNAVHistorical/{mf_schcode}
#     "get_investment_detail":    "mf_scheme",  # InvestmentDetails/{mf_schcode}
#     "get_expense_ratio":        "mf_scheme",  # ExpenseRatios  (mf_schcode filter)
#     "get_avg_maturity":         "mf_scheme",  # AvgerageMaturityData (mf_schcode filter)
#     "get_scheme_aum":           "mf_scheme",  # SchemeAUMHist (mf_schcode filter)
#     "get_nav_historical":       "mf_scheme",  # SchemeNAVHistorical/{mf_schcode}/{period}/{periodval}
#     "get_scheme_returns":       "mf_scheme",  # SchemeReturns/{mf_schcode}
#     "get_lumpsum_returns":      "mf_scheme",  # LumpSumSchemereturnDetails/{mf_schcode}
#     "get_scheme_sip":           "mf_scheme",  # SchemeSIPSWPdetails/{mf_schcode}
#     "get_mf_holdings":          "mf_scheme",  # MFHolding/{mf_schcode}
#     "get_sector_allocation":    "mf_scheme",  # SchemeSectorAllocation/{mf_schcode}
#     "get_asset_allocation":     "mf_scheme",  # SchemeAssetAllocation/{mf_schcode}
#     "get_portfolio_changes":    "mf_scheme",  # Whats_InOut/{type}/{mf_schcode}
#     "get_mcap_allocation":      "mf_scheme",  # MCAP_EquityHolding/{mf_schcode}
#     "get_most_bought":          "mf_scheme",  # MostsoldBought/{mf_schcode}
#     "get_scheme_ratios":        "mf_scheme",  # Scheme_Ratios (mf_schcode filter)
#     "get_dividend_details":     "mf_scheme",  # DividendDetails/{mf_schcode}
#     "get_bse_star_scheme":      "mf_scheme",  # BSEStarSchemeMaster/{mf_schcode}
#     "compare_schemes":          "mf_scheme",  # SchemeComparsion/{schcodes}
#     "get_whats_in_out":         "mf_scheme",  # Whats_InOut/{type}/{mf_schcode}

#     # ── MF AMC: requires mf_cocode ────────────────────────────────────────────
#     "get_fund_categories":      "mf_amc",   # FundCategoryAMCWise/{mf_cocode}/{category}
#     "get_schemes_by_amc":       "mf_amc",   # SchemeMaster/{mf_cocode}
#     "get_fund_profile":         "mf_amc",   # fund-profile/{mf_cocode}

#     # ── ETF: requires isin ────────────────────────────────────────────────────
#     "get_etf_quotes":           "etf",      # ETFGetQuotes/{ex}/{isin}
#     "get_etf_returns":          "etf",      # ETFReturns/{isin}
#     "get_etf_fundamentals":     "etf",      # ETFFundamentals/{isin}
#     "get_etf_about":            "etf",      # ETFAboutus/{isin}
#     "get_etf_equity_holdings":  "etf",      # ETFShareHoldingEquity/{isin}
#     "get_etf_monthly_portfo":   "etf",      # ETFMonthlyPortfolioAllHoldings/{isin}
#     "get_etf_sector_allocatio": "etf",      # ETFSectorAllocation/{isin}
#     "get_etf_asset_allocation": "etf",      # ETFAssetAllocation/{isin}
#     "get_etf_":                 "etf",      # generic ETF prefix

#     # ── General: no dynamic entity param ─────────────────────────────────────
#     "get_forthcoming_ipo":      "general",  # forthcomingipo/{ex}/Ipo/{n}
#     "get_open_ipos":            "general",  # OpenIssues/{ex}/Ipo/{n}
#     "get_closed_ipos":          "general",  # ClosedIssues/{ex}/IPO/{n}
#     "get_new_ipo":              "general",  # Newlisting/{ex}/{n}
#     "get_best_ipo":             "general",  # BestPerformerIpo/{ex}/{n}
#     "get_new_fund_offer":       "general",  # NewFundOffer
#     "get_fund_house":           "general",  # Fund_House  (master list)
#     "get_amfi_master":          "general",  # AMFIMaster
#     "get_fund_manager":         "general",  # FundManager
#     "get_index_list":           "general",  # IndexList
#     "get_results_today":        "general",  # Today-Results
#     "get_result_declarations":  "general",  # ResultDataDeclarations/{date}
#     "get_annual_declarations":  "general",  # AnnualDataDeclarations/{date}
#     "get_bse_announcement":     "general",  # BSEAnnouncement
#     "get_nse_announcement":     "general",  # NSEAnnouncement
#     "get_corporate_news":       "general",  # CapitalMarketLiveNews/corporate-news/{n}
#     "get_mf_news":              "general",  # MF_News/{sno}
#     "get_mf_activities":        "general",  # MFActivities
#     "get_ipo_master":           "general",  # ipomaster
#     "get_ipo_prospectus":       "general",  # IPOProspectus/sebi
#     "get_ipo_logo":             "general",  # IPOCompanyLogo
#     "get_fund_performance":     "general",  # FundPerformance/{top}/{type}/{category}
#     "get_category_performance": "general",  # CategoryPerformance/{type}/{top}
#     "get_sip_dates":            "general",  # SIP_Dates/{plan}
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
#     # NEW — market group (gainers, losers, active performers, sector companies, etc.)
#     # group_master stores all exchange groups/segments (e.g. "A", "B", "SME", "Nifty 50")
#     "group": {
#         "table":    "group_master",
#         "column":   "group_name",
#         "id_field": "group_name",   # the API expects the group_name string itself
#     },
# }

# # Maps entity_type → the DB param(s) it needs
# ENTITY_TYPE_PARAM_MAP: dict[str, list[str]] = {
#     "stock":     ["co_code"],
#     "mf_scheme": ["mf_schcode"],
#     "mf_amc":    ["mf_cocode"],
#     "etf":       ["isin"],
#     "index":     ["index_code"],
#     "market":    ["group"],      # NEW — group resolved from group_master
#     "exchange":  [],             # ex is a known constant ("NSE"/"BSE"), no DB lookup
#     "general":   [],             # no entity code needed; do NOT do DB lookup
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
# # Node AIQ: Classify + Extract + Decompose
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
# 1. A Specific Entity: (e.g., Reliance Industries, HDFC AMC, Nifty 50, SBI Small Cap Fund, Gold BeES ETF).
# 2. A Market Category/Concept: (e.g., "upcoming IPOs", "top gainers", "market news", "bulk deals", "top losers on NSE").

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
#       "entity_type": "<stock|mf_scheme|mf_amc|etf|index|market|exchange|general>",
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
#    - "Gold BeES", "Nifty BeES" → entity_type = etf
#    - "Nifty 50", "Sensex", index lists → entity_type = index
#    - "Top gainers", "Top losers", "Most active", "52-week high", sector performers → entity_type = market
#    - "Advances/Declines on NSE", "Exchange holidays" → entity_type = exchange
#    - Concepts like "IPOs", "NFO", market news, master lists, educational definitions → entity_type = general
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
#     console.print("[Node AIQ] Classify + extract + decompose (v11.1)")

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
#     console.print("[Node TVS] Per-intent tool vector search (v11.1)")

#     intents = state.get("intents") or []
#     if not intents:
#         console.print("  ⚠ No intents to search for")
#         return state

#     def _search_for_intent(intent: IntentItem) -> IntentItem:
#         et = intent.get("entity_type", "general")

#         # For general/no-code intents, search purely on intent description
#         if et in ("general", "exchange"):
#             search_query = intent["intent_description"]
#         else:
#             search_query = f"{et} {intent['intent_description']}"

#         tools = _query_tool_registry(search_query, n_results=8)

#         # For typed entities, prefer tools that match the family
#         if et not in ("general", "exchange", "") and tools:
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
#     Returns [] for 'general' and 'exchange' entity types — no DB lookup needed.
#     """
#     # These types carry no DB-resolvable entity code
#     if entity_type in ("general", "exchange"):
#         console.print(f"  📋 entity_type='{entity_type}' → no DB params needed")
#         return []

#     if entity_type and entity_type not in ("general", "exchange"):
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
#     needs_group     = "group"       in required_params   # NEW

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

#     # NEW — resolve market group from group_master
#     if needs_group:
#         cfg   = PARAM_TO_TABLE_MAP["group"]
#         match = _targeted_db_lookup(
#             entity=name, table=cfg["table"],
#             name_col=cfg["column"], id_col=cfg["id_field"],
#         )
#         if match:
#             # The API expects the group_name string (e.g. "A", "SME", "Nifty 50")
#             codes["group"] = match["group_name"]
#             result["resolved_group_name"] = match["group_name"]
#         else:
#             # Fall back to the raw entity name so the MCP call still proceeds
#             console.print(f"  ⚠ group not found in group_master for '{name}' — using raw name")
#             codes["group"] = name
#             result["resolved_group_name"] = name

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

#         # Skip DB entirely for general/exchange/no-code intents
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
#     console.print("[Node MPR] Single-entity MCP pre-resolve (v11.1)")

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
#     console.print("[Node MMPR] Multi-entity MCP pre-resolve (v11.1)")

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
# # Node MTCI: MCP Tool Call — sequential per-intent in one session (v11.1)
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
#     console.print("[Node MTCI] Sequential per-intent MCP calls — one session (v11.1)")

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

#     try:
#         with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
#             future         = ex.submit(_run_mcp_multi_in_thread, intents)
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
# # MCP Synthesis (v11.1)
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
#     console.print("[Node MS] MCP synthesis (v11.1)")
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


# # ══════════════════════════════════════════════════════════════════════════════
# # Router (v11.1)
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
# # Graph (v11.1)
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


# # general redundancy fixed #################


# """
# graph.py — EQUIFIZ Financial AI Agent (v11.2)

# Key changes from v11.1
# ──────────────────────────────────────────────────────────────────────
# 1. Eliminated entity_type="general" ambiguity:
#    - "general" as entity_type in intents is now reserved ONLY for
#      truly unanswerable-by-MCP questions (greetings, definitions, etc.)
#    - Former "general" tool-backed intent types are now named explicitly:
#        "ipo"          → IPO listings, forthcoming/open/closed IPOs
#        "nfo"          → New Fund Offers
#        "news"         → Corporate news, MF news, announcements
#        "announcement" → BSE/NSE announcements
#        "market_info"  → Fund performance, category performance, SIP dates,
#                         fund manager lists, AMFI master, index lists
#    - These all still resolve through MCP (tools exist for them),
#      just with no DB entity-code lookup needed.

# 2. Router logic updated:
#    - If ANY intent has a matched tool → route to MCP path
#    - Only if NO intents have tools AND query_type != mcp → general_handler
#    - Mixed queries (e.g., "Reliance price + IPO news") correctly route
#      to multi_mcp; the IPO sub-intent gets called without DB lookup,
#      the stock sub-intent gets co_code resolved — both work in one session.

# 3. ENTITY_TYPE_PARAM_MAP extended with new types (all → [] params).

# 4. _TOOL_FAMILY_HINTS updated — "general" entries now carry their real type.

# 5. LLM decomposition prompt updated with new entity_type values.

# 6. All v11.1 logic preserved otherwise.
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
#     entity_type:        str
#     intent_description: str
#     tool_hint:          str
#     scheme_name:        Optional[str]
#     amc_name:           Optional[str]
#     nse_symbol:         Optional[str]
#     matched_tools:      list[dict]
#     resolved_codes:     dict
#     mcp_result:         str


# # ══════════════════════════════════════════════════════════════════════════════
# # Entity type taxonomy
# # ══════════════════════════════════════════════════════════════════════════════
# #
# # Types that require a DB entity-code lookup:
# #   stock     → co_code         (equity / company)
# #   mf_scheme → mf_schcode      (mutual-fund scheme)
# #   mf_amc    → mf_cocode       (AMC / fund-house)
# #   etf       → isin            (exchange-traded fund)
# #   index     → index_code      (market index)
# #   market    → group           (market segment, resolved from group_master)
# #
# # Types that go to MCP but need NO DB lookup (no entity code):
# #   exchange    → ex only (NSE/BSE constant)
# #   ipo         → IPO listings (forthcoming, open, closed, new listing, best performer)
# #   nfo         → New Fund Offers
# #   news        → Corporate news, MF news
# #   announcement→ BSE/NSE announcements
# #   market_info → Fund performance, category performance, SIP dates,
# #                 fund manager lists, AMFI master, index list, result declarations
# #
# # Type that bypasses MCP entirely (vector search fallback):
# #   general → truly unanswerable by live data (definitions, greetings overflow,
# #             purely educational questions with no matching tool)
# #
# # ══════════════════════════════════════════════════════════════════════════════

# # All entity types that require NO DB code resolution but still go to MCP
# _NO_DB_MCP_TYPES = frozenset({
#     "exchange",
#     "ipo",
#     "nfo",
#     "news",
#     "announcement",
#     "market_info",
# })

# # ══════════════════════════════════════════════════════════════════════════════
# # Tool-family hints — mapped by the actual required parameter each tool uses
# # ══════════════════════════════════════════════════════════════════════════════

# _TOOL_FAMILY_HINTS: dict[str, str] = {

#     # ── Stock: requires co_code ───────────────────────────────────────────────
#     "get_company_stock":        "stock",
#     "get_nse_company_announcements": "stock",
#     "get_bse_company_announcements": "stock",
#     "get_company_result_schedule": "stock",
#     "get_delayed_stock":        "stock",
#     "get_company_profile":      "stock",
#     "get_company_backgroun":    "stock",
#     "get_board_of":             "stock",
#     "get_company_bankers":      "stock",
#     "get_management":           "stock",
#     "get_subsidiaries":         "stock",
#     "get_related_party":        "stock",
#     "get_employee_count":       "stock",
#     "get_capital_structure":    "stock",
#     "get_pledge_share":         "stock",
#     "get_substantial":          "stock",
#     "get_segment_data":         "stock",
#     "get_r_and_d":              "stock",
#     "get_finished_products":    "stock",
#     "get_raw_materials":        "stock",
#     "get_chronological":        "stock",
#     "get_company_history":      "stock",
#     # Financials
#     "get_quarterly_results":    "stock",
#     "get_profit_loss":          "stock",
#     "get_balance_sheet":        "stock",
#     "get_cash_flow":            "stock",
#     "get_half_yearly":          "stock",
#     "get_nine_months":          "stock",
#     "get_yearly_results":       "stock",
#     "get_quarterly_balance":    "stock",
#     "get_annual_balance":       "stock",
#     "get_ttm_growth":           "stock",
#     "get_quarterly_revenue":    "stock",
#     "get_quarterly_ebitda":     "stock",
#     "get_quarterly_ebit":       "stock",
#     "get_growth_data":          "stock",
#     # Ratios
#     "get_key_financial":        "stock",
#     "get_daily_ratios":         "stock",
#     "get_margin_ratios":        "stock",
#     "get_valuation_ratios":     "stock",
#     "get_all_basic_ratios":     "stock",
#     "get_return_ratios":        "stock",
#     "get_growth_ratios":        "stock",
#     "get_performance_ratio":    "stock",
#     "get_cashflow_ratios":      "stock",
#     "get_liquidity_ratios":     "stock",
#     "get_solvency_ratios":      "stock",
#     "get_quarterly_ratios":     "stock",
#     "get_yearly_ratios":        "stock",
#     "get_shareholding":         "stock",
#     "get_major_sharehold":      "stock",
#     # IPO tools that need a specific company co_code
#     "get_anchor_investor":      "stock",
#     "get_basis_of":             "stock",
#     "get_ipo_details":                  "stock",
#     "get_ipo_subscription_status":      "stock",
#     "get_ipo_synopsis":                 "stock",
#     "get_ipo_timeline":                 "stock",
#     "get_ipo_promoter_details":         "stock",
#     "get_ipo_listing_info":             "stock",
#     "get_ipo_objects_of_issue":         "stock",
#     "get_ipo_anchor_investor_details":  "stock",
#     "get_ipo_financials":               "stock",
#     "get_ipo_product_services":         "stock",
#     "get_ipo_strength_details":         "stock",
#     "get_ipo_strategy_details":         "stock",
#     "get_ipo_risk_details":             "stock",
#     "get_ipo_industry_peers":           "stock",
#     "get_ipo_selling_shareholders":     "stock",
#     "get_ipo_allocation_details":       "stock",
#     "get_ipo_prospectus":               "stock",
#     "get_ipo_lead_managers":            "stock",
#     "get_ipo_registrar":                "stock",

#     # ── Index: requires index_code ────────────────────────────────────────────
#     "get_market_indices":        "stock",   # Indices endpoint; no param but stock-adjacent
#     "get_index_companies":       "index",

#     # ── Market: requires group (resolved from group_master) ───────────────────
#     "get_active_performer":     "market",
#     "get_top_gainers":          "market",
#     "get_top_losers":           "market",
#     "get_out_under":            "market",
#     "get_52week":               "market",
#     "get_new_highs":            "market",
#     "get_sector_companies":     "market",

#     # ── Exchange: requires ex only ────────────────────────────────────────────
#     "get_advance_decline":      "exchange",
#     "get_exchange_holidays":    "exchange",

#     # ── MF Scheme: requires mf_schcode ────────────────────────────────────────
#     "get_scheme_nav":           "mf_scheme",
#     "get_investment_detail":    "mf_scheme",
#     "get_expense_ratio":        "mf_scheme",
#     "get_avg_maturity":         "mf_scheme",
#     "get_scheme_aum":           "mf_scheme",
#     "get_nav_historical":       "mf_scheme",
#     "get_scheme_returns":       "mf_scheme",
#     "get_lumpsum_returns":      "mf_scheme",
#     "get_scheme_sip":           "mf_scheme",
#     "get_mf_holdings":          "mf_scheme",
#     "get_sector_allocation":    "mf_scheme",
#     "get_asset_allocation":     "mf_scheme",
#     "get_portfolio_changes":    "mf_scheme",
#     "get_mcap_allocation":      "mf_scheme",
#     "get_most_bought":          "mf_scheme",
#     "get_scheme_ratios":        "mf_scheme",
#     "get_dividend_details":     "mf_scheme",
#     "get_bse_star_scheme":      "mf_scheme",
#     "compare_schemes":          "mf_scheme",
#     "get_whats_in_out":         "mf_scheme",

#     # ── MF AMC: requires mf_cocode ────────────────────────────────────────────
#     "get_fund_categories":      "mf_amc",
#     "get_schemes_by_amc":       "mf_amc",
#     "get_fund_profile":         "mf_amc",

#     # ── ETF: requires isin ────────────────────────────────────────────────────
#     "get_etf_quotes":           "etf",
#     "get_etf_returns":          "etf",
#     "get_etf_fundamentals":     "etf",
#     "get_etf_about":            "etf",
#     "get_etf_equity_holdings":  "etf",
#     "get_etf_monthly_portfo":   "etf",
#     "get_etf_sector_allocatio": "etf",
#     "get_etf_asset_allocation": "etf",
#     "get_etf_":                 "etf",

#     # ── IPO: no entity code needed ────────────────────────────────────────────
#     "get_forthcoming_ipo":      "ipo",
#     "get_open_ipos":            "ipo",
#     "get_closed_ipos":          "ipo",
#     "get_new_ipo":              "ipo",
#     "get_best_ipo":             "ipo",
#     "get_ipo_master":           "ipo",
#     "get_ipo_prospectus":       "ipo",
#     "get_ipo_logo":             "ipo",

#     # ── NFO: no entity code needed ────────────────────────────────────────────
#     "get_new_fund_offer":       "nfo",

#     # ── News: no entity code needed ───────────────────────────────────────────
#     "get_corporate_news":       "news",
#     "get_mf_news":              "news",
#     "get_mf_activities":        "news",

#     # ── Announcement: no entity code needed ───────────────────────────────────
#     "get_bse_announcement":     "announcement",
#     "get_nse_announcement":     "announcement",

#     # ── Market Info: no entity code needed ────────────────────────────────────
#     "get_fund_house":           "market_info",
#     "get_amfi_master":          "market_info",
#     "get_fund_manager":         "market_info",
#     "get_index_list":           "market_info",
#     "get_results_today":        "market_info",
#     "get_result_declarations":  "market_info",
#     "get_annual_declarations":  "market_info",
#     "get_fund_performance":     "market_info",
#     "get_category_performance": "market_info",
#     "get_sip_dates":            "market_info",

#     "get_macro_economic_data" :" market_info",
#     "get_forthcoming_bond_ipo"  :" market_info",
#     "get_open_bond_ipo" :" market_info",
#     "get_debt_eod_prices_scripwise" : "bond",
#     "get_debt_top_value" :" market_info",
#     "get_debt_top_volume" :" market_info",
#     "get_debt_market_watch": "market_info",
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
#     "group": {
#         "table":    "group_master",
#         "column":   "group_name",
#         "id_field": "group_name",
#     },
#     "bond_code":{
#         "table":"bond_master",
#         "column":"companyname",
#         "id_field":"code"
#     }
# }

# # Maps entity_type → DB param(s) needed.
# # Types with [] need NO DB lookup — they go straight to MCP.
# ENTITY_TYPE_PARAM_MAP: dict[str, list[str]] = {
#     "stock":        ["co_code"],
#     "mf_scheme":    ["mf_schcode"],
#     "mf_amc":       ["mf_cocode"],
#     "etf":          ["isin"],
#     "index":        ["index_code"],
#     "market":       ["group"],
#     "bond" : ["bond_code"],
#     # No DB params needed:
#     "exchange":     [],
#     "ipo":          [],
#     "nfo":          [],
#     "news":         [],
#     "announcement": [],
#     "market_info":  [],
#     "general":      [],
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
# # Node AIQ: Classify + Extract + Decompose
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
# 1. A Specific Entity: (e.g., Reliance Industries, HDFC AMC, Nifty 50, SBI Small Cap Fund, Gold BeES ETF).
# 2. A Market Category/Concept: (e.g., "upcoming IPOs", "top gainers", "market news", "bulk deals", "top losers on NSE").

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
#       "entity_type": "<see ENTITY TYPE RULES below>",
#       "intent_description": "<specific data point needed for this subject>",
#       "scheme_name": "<if mf_scheme, else empty>",
#       "amc_name": "<if mf_amc, else empty>",
#       "nse_symbol": "<NSE ticker if known, else empty>"
#     }}
#   ]
# }}

# ────────────────────────────────────────────────────────────
# ENTITY TYPE RULES (use exactly one of these values):

#   stock        → A specific listed company equity (Reliance, TCS, HDFC Bank)
#   mf_scheme    → A specific mutual fund scheme (Parag Parikh Flexi Cap, Axis Bluechip)
#   mf_amc       → A mutual fund house / AMC (SBI Mutual Fund, Nippon AMC)
#   etf          → An exchange-traded fund (Gold BeES, Nifty BeES)
#   index        → A market index (Nifty 50, Sensex, Bank Nifty)
#   market       → Market-wide movers needing a group/segment (top gainers, top losers,
#                  52-week highs, most active, sector performers, outperformers)
#   exchange     → Exchange-level data with no specific entity (advances/declines, holidays)
#   ipo          → IPO related data — upcoming, open, closed, new listings, best performers,
#                  IPO master list, prospectus (NOT a specific company's anchor investors)
#   nfo          → New Fund Offers
#   news         → Corporate news, MF news, market activities feed
#   announcement → BSE/NSE corporate announcements
#   market_info  → Fund manager lists, AMFI master, fund performance rankings,
#                  category performance, SIP dates, index list, result declarations
#   general      → ONLY for purely educational/definitional questions that cannot be
#                  answered by any live data tool (e.g., "what is PE ratio?",
#                  "explain SIP", "what is NAV?")

# ────────────────────────────────────────────────────────────
# DECOMPOSITION RULES (CRITICAL):
# 1. THE 1:1 MAPPING: len(intents) MUST equal the number of subjects identified in Step 1.
# 2. NO BUNDLING: Two different subjects → two separate intent objects.
# 3. DATA POINT MERGING: Multiple metrics on the SAME entity → ONE intent.
# 4. MIXED QUERIES FULLY SUPPORTED: "Reliance price and IPO news" → intent[0] type=stock,
#    intent[1] type=ipo. Each will be handled independently in parallel.

# MCP_NEEDED LOGIC:
# - Set to true if ANY intent requires live market data (prices, NAVs, ratios, lists, news).
# - Set to false ONLY for pure greetings or purely educational/definitional questions
#   where every single intent is type=general.
# ────────────────────────────────────────────────────────────
# """


# def node_classify_extract_and_decompose(state: AgentState) -> AgentState:
#     console.print("[Node AIQ] Classify + extract + decompose (v11.2)")

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

#         if not intents and qt not in ("greeting",):
#             console.print("  ⚠ LLM returned no intents — fallback single general intent")
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
#     console.print("[Node TVS] Per-intent tool vector search (v11.2)")

#     intents = state.get("intents") or []
#     if not intents:
#         console.print("  ⚠ No intents to search for")
#         return state

#     def _search_for_intent(intent: IntentItem) -> IntentItem:
#         et = intent.get("entity_type", "general")

#         # Purely educational — skip tool search entirely
#         if et == "general":
#             console.print(
#                 f"  [{intent['entity']}] entity_type=general → skip tool search"
#             )
#             return IntentItem(
#                 entity             = intent.get("entity", ""),
#                 entity_type        = "general",
#                 intent_description = intent.get("intent_description", ""),
#                 scheme_name        = intent.get("scheme_name"),
#                 amc_name           = intent.get("amc_name"),
#                 nse_symbol         = intent.get("nse_symbol"),
#                 tool_hint          = "",
#                 matched_tools      = [],
#                 resolved_codes     = {},
#                 mcp_result         = "",
#             )

#         # Build search query — no-DB types search on intent description alone
#         if et in _NO_DB_MCP_TYPES:
#             search_query = intent["intent_description"]
#         else:
#             search_query = f"{et} {intent['intent_description']}"

#         tools = _query_tool_registry(search_query, n_results=8)

#         # Filter by family when we have a typed entity
#         if et not in _NO_DB_MCP_TYPES and tools:
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


# # ── General handler (vector search fallback) ──────────────────────────────────

# def node_general_handler(state: AgentState) -> AgentState:
#     """
#     Handles purely educational/definitional questions that have no matching
#     MCP tool. Uses vector search over knowledge base.
#     Only reached when ALL intents are type=general OR no tools matched at all.
#     """
#     console.print("[Node C] General handler (vector search fallback)")
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
#     Returns DB params to resolve for this intent.
#     Returns [] for no-DB types (general, exchange, ipo, nfo, news, etc.).
#     """
#     params = ENTITY_TYPE_PARAM_MAP.get(entity_type)
#     if params is not None:
#         if params:
#             console.print(
#                 f"  📋 entity_type='{entity_type}' → authoritative params: {params}"
#             )
#         else:
#             console.print(
#                 f"  📋 entity_type='{entity_type}' → no DB params needed"
#             )
#         return params

#     # Fallback: infer from tool metadata
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
#     needs_group     = "group"       in required_params

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

#     if needs_group:
#         cfg   = PARAM_TO_TABLE_MAP["group"]
#         match = _targeted_db_lookup(
#             entity=name, table=cfg["table"],
#             name_col=cfg["column"], id_col=cfg["id_field"],
#         )
#         if match:
#             codes["group"] = match["group_name"]
#             result["resolved_group_name"] = match["group_name"]
#         else:
#             console.print(
#                 f"  ⚠ group not found in group_master for '{name}' — using raw name"
#             )
#             codes["group"] = name
#             result["resolved_group_name"] = name

#     result["mcp_resolved_codes"] = codes
#     return result


# # ── Resolve codes for all intents ─────────────────────────────────────────────

# def _resolve_intents_codes(intents: list[IntentItem]) -> list[IntentItem]:
#     def _resolve_one(intent: IntentItem) -> IntentItem:
#         et        = intent.get("entity_type", "general")
#         tool_hint = intent.get("tool_hint", "")
#         matched   = intent.get("matched_tools") or []

#         # If entity_type is still 'general' but we got a tool, try to infer type
#         if et == "general" and tool_hint:
#             inferred = _infer_entity_type_from_tool(tool_hint)
#             if inferred != "general":
#                 et = inferred
#                 intent["entity_type"] = et

#         req_params = _get_required_params_for_entity(tool_hint, et, matched)

#         # Skip DB entirely for no-param types
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
#     console.print("[Node MPR] Single-entity MCP pre-resolve (v11.2)")

#     intents = state.get("intents") or []
#     if not intents:
#         console.print("  ⚠ No intents; skipping resolve")
#         state["mcp_resolved_codes"] = {}
#         return state

#     resolved_intents            = _resolve_intents_codes(intents)
#     state["intents"]            = resolved_intents
#     state["mcp_resolved_codes"] = resolved_intents[0].get("resolved_codes", {}) if resolved_intents else {}

#     console.print(f"  🏁 Primary resolved codes: {state['mcp_resolved_codes']}")
#     return state


# def node_mcp_multi_pre_resolve(state: AgentState) -> AgentState:
#     console.print("[Node MMPR] Multi-entity MCP pre-resolve (v11.2)")

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
# # Node MTCI: MCP Tool Call — sequential per-intent in one session (v11.2)
# # ══════════════════════════════════════════════════════════════════════════════

# def _run_mcp_multi_in_thread(intents: list[dict]) -> list[dict]:
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
#     console.print("[Node MTCI] Sequential per-intent MCP calls — one session (v11.2)")

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

#     # Separate intents: MCP-capable vs general (no tool found)
#     mcp_intents     = [i for i in intents if i.get("tool_hint") or i.get("entity_type") not in ("general",)]
#     general_intents = [i for i in intents if not i.get("tool_hint") and i.get("entity_type") == "general"]

#     if general_intents:
#         console.print(
#             f"  ℹ {len(general_intents)} general intent(s) will be answered via synthesis prompt only"
#         )

#     # Fallback: no MCP intents at all
#     if not mcp_intents:
#         console.print("  ⚠ No MCP intents; falling back to raw user query")
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

#     try:
#         with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
#             future         = ex.submit(_run_mcp_multi_in_thread, mcp_intents)
#             filled_intents = future.result(timeout=600)
#     except concurrent.futures.TimeoutError:
#         console.print("  [red]Multi-intent MCP timed out[/red]")
#         filled_intents = [
#             {**dict(i), "mcp_result": "MCP call timed out."}
#             for i in mcp_intents
#         ]
#     except Exception as exc:
#         console.print(f"  [red]Multi-intent MCP failed: {exc}[/red]")
#         filled_intents = [
#             {**dict(i), "mcp_result": f"MCP call failed: {exc}"}
#             for i in mcp_intents
#         ]

#     # Merge back: MCP results + general intents (no result, handled in synthesis)
#     all_intents = filled_intents + [
#         {**dict(i), "mcp_result": "(answered from knowledge base)"}
#         for i in general_intents
#     ]
#     state["intents"] = all_intents

#     state["intent_results"] = [
#         {
#             "entity":             i.get("entity", ""),
#             "entity_type":        i.get("entity_type", ""),
#             "intent_description": i.get("intent_description", ""),
#             "mcp_result":         i.get("mcp_result", ""),
#         }
#         for i in all_intents
#     ]

#     state["mcp_raw_result"] = "\n\n---\n\n".join(
#         f"[{r['entity']} / {r['intent_description']}]\n{r['mcp_result']}"
#         for r in state["intent_results"]
#         if r.get("mcp_result")
#     )
#     state["mcp_tool_calls_made"] = []
#     state["error"]               = None

#     total_chars = len(state["mcp_raw_result"])
#     console.print(
#         f"  ✅ {len(filled_intents)} MCP intent(s) resolved "
#         f"({len(general_intents)} general skipped). "
#         f"Total chars for synthesis: {total_chars}"
#     )
#     return state


# # ══════════════════════════════════════════════════════════════════════════════
# # MCP Synthesis (v11.2)
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
# 2. Sub-questions marked "(answered from knowledge base)" should be answered
#    from your own knowledge directly in the response.
# 3. Write in plain prose paragraphs only. No markdown, no bullet points,
#    no asterisks, no tables. Do NOT use the rupee symbol — write "Rs".
# 4. Present key numbers naturally woven into sentences.
# 5. If a specific data field is missing or the tool returned an error, say so briefly.
# 6. Use Indian number formatting (lakh, crore) for large figures.
# 7. Keep under 300 words unless the user explicitly asked for detail.
# 8. End with: This is not financial advice.
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
#     console.print("[Node MS] MCP synthesis (v11.2)")
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
#         state["final_answer"] = mcp_result
#     return state


# # ── General synthesis (vector search) ────────────────────────────────────────

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
#     console.print("[Node S] General synthesis (vector search)")
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
# # Router (v11.2)
# # ══════════════════════════════════════════════════════════════════════════════

# def route_after_classify(state: AgentState) -> str:
#     qt      = state.get("query_type", "general")
#     intents = state.get("intents") or []

#     if qt == "greeting":
#         return "greeting"

#     # Determine which intents actually have MCP tools matched
#     mcp_intents = [
#         i for i in intents
#         if i.get("tool_hint") or i.get("entity_type") not in ("general",)
#     ]
#     general_only_intents = [
#         i for i in intents
#         if not i.get("tool_hint") and i.get("entity_type") == "general"
#     ]

#     for idx, intent in enumerate(intents):
#         console.print(
#             f"  [Router] intent[{idx}] entity={intent.get('entity')} "
#             f"type={intent.get('entity_type')} "
#             f"tool_hint={intent.get('tool_hint') or '(none)'}"
#         )

#     # All intents are pure general (no tools, all type=general) → vector search
#     if not mcp_intents or (not any(i.get("tool_hint") for i in intents) and
#                             all(i.get("entity_type") == "general" for i in intents)):
#         console.print("  → general (all intents are educational/no tools)")
#         return "general"

#     has_many         = len(intents) > 1
#     additional       = state.get("companies") or []
#     primary_type     = state.get("primary_entity_type", "general")
#     secondary_types  = {c.get("entity_type", "general") for c in additional}
#     is_heterogeneous = bool(secondary_types - {"general"} - {primary_type})

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
# # Graph (v11.2)
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







# ## with bond 

# """
# graph.py — EQUIFIZ Financial AI Agent (v11.3)

# Key changes from v11.2
# ──────────────────────────────────────────────────────────────────────
# 1. Bond entity type added throughout:
#    - entity_type="bond"  → requires bond_code (resolved from bond_master)
#    - PARAM_TO_TABLE_MAP  → "bond_code" entry pointing to bond_master
#    - ENTITY_TYPE_PARAM_MAP → "bond" → ["bond_code"]
#    - _TOOL_FAMILY_HINTS  → bond tools mapped to "bond"
#    - _resolve_entity_codes → needs_bond_code branch added
#    - LLM decomposition prompt updated with bond entity_type rule

# 2. Fixed leading-space bug in _TOOL_FAMILY_HINTS values for
#    get_macro_economic_data, get_forthcoming_bond_ipo, get_open_bond_ipo,
#    get_debt_top_value, get_debt_top_volume, get_debt_market_watch.

# 3. All v11.2 logic preserved otherwise.
# """
# from __future__ import annotations

# import asyncio
# import concurrent.futures
# import json
# import logging
# import re
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

# # ── Types ─────────────────────────────────────────────────────────────────────

# class Turn(TypedDict):
#     role: str       # "user" | "assistant"
#     content: str


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


# # ══════════════════════════════════════════════════════════════════════════════
# # Entity type taxonomy
# # ══════════════════════════════════════════════════════════════════════════════
# #
# # Types that require a DB entity-code lookup:
# #   stock     → co_code         (equity / company)
# #   mf_scheme → mf_schcode      (mutual-fund scheme)
# #   mf_amc    → mf_cocode       (AMC / fund-house)
# #   etf       → isin            (exchange-traded fund)
# #   index     → index_code      (market index)
# #   market    → group           (market segment, resolved from group_master)
# #   bond      → bond_code       (listed bond / debenture / NCD)
# #
# # Types that go to MCP but need NO DB lookup (no entity code):
# #   exchange    → ex only (NSE/BSE constant)
# #   ipo         → IPO listings (forthcoming, open, closed, new listing, best performer)
# #   nfo         → New Fund Offers
# #   news        → Corporate news, MF news
# #   announcement→ BSE/NSE announcements
# #   market_info → Fund performance, category performance, SIP dates,
# #                 fund manager lists, AMFI master, index list, result declarations,
# #                 macro economic data, debt market watch, debt top value/volume
# #
# # Type that bypasses MCP entirely (vector search fallback):
# #   general → truly unanswerable by live data (definitions, greetings overflow,
# #             purely educational questions with no matching tool)
# #
# # ══════════════════════════════════════════════════════════════════════════════

# # All entity types that require NO DB code resolution but still go to MCP
# _NO_DB_MCP_TYPES = frozenset({
#     "exchange",
#     "ipo",
#     "nfo",
#     "news",
#     "announcement",
#     "market_info",
# })

# # ══════════════════════════════════════════════════════════════════════════════
# # Tool-family hints — mapped by the actual required parameter each tool uses
# # ══════════════════════════════════════════════════════════════════════════════

# _TOOL_FAMILY_HINTS: dict[str, str] = {

#     # ── Stock: requires co_code ───────────────────────────────────────────────
#     "get_company_stock":                    "stock",
#     "get_nse_company_announcements":        "stock",
#     "get_bse_company_announcements":        "stock",
#     "get_company_result_schedule":          "stock",
#     "get_delayed_stock":                    "stock",
#     "get_company_profile":                  "stock",
#     "get_company_backgroun":                "stock",
#     "get_board_of":                         "stock",
#     "get_company_bankers":                  "stock",
#     "get_management":                       "stock",
#     "get_subsidiaries":                     "stock",
#     "get_related_party":                    "stock",
#     "get_employee_count":                   "stock",
#     "get_capital_structure":                "stock",
#     "get_pledge_share":                     "stock",
#     "get_substantial":                      "stock",
#     "get_segment_data":                     "stock",
#     "get_r_and_d":                          "stock",
#     "get_finished_products":                "stock",
#     "get_raw_materials":                    "stock",
#     "get_chronological":                    "stock",
#     "get_company_history":                  "stock",
#     # Financials
#     "get_quarterly_results":                "stock",
#     "get_profit_loss":                      "stock",
#     "get_balance_sheet":                    "stock",
#     "get_cash_flow":                        "stock",
#     "get_half_yearly":                      "stock",
#     "get_nine_months":                      "stock",
#     "get_yearly_results":                   "stock",
#     "get_quarterly_balance":                "stock",
#     "get_annual_balance":                   "stock",
#     "get_ttm_growth":                       "stock",
#     "get_quarterly_revenue":                "stock",
#     "get_quarterly_ebitda":                 "stock",
#     "get_quarterly_ebit":                   "stock",
#     "get_growth_data":                      "stock",
#     # Ratios
#     "get_key_financial":                    "stock",
#     "get_daily_ratios":                     "stock",
#     "get_margin_ratios":                    "stock",
#     "get_valuation_ratios":                 "stock",
#     "get_all_basic_ratios":                 "stock",
#     "get_return_ratios":                    "stock",
#     "get_growth_ratios":                    "stock",
#     "get_performance_ratio":                "stock",
#     "get_cashflow_ratios":                  "stock",
#     "get_liquidity_ratios":                 "stock",
#     "get_solvency_ratios":                  "stock",
#     "get_quarterly_ratios":                 "stock",
#     "get_yearly_ratios":                    "stock",
#     "get_shareholding":                     "stock",
#     "get_major_sharehold":                  "stock",
#     # Company-specific IPO data (needs co_code)
#     "get_anchor_investor":                  "stock",
#     "get_basis_of":                         "stock",
#     "get_ipo_details":                      "stock",
#     "get_ipo_subscription_status":          "stock",
#     "get_ipo_synopsis":                     "stock",
#     "get_ipo_timeline":                     "stock",
#     "get_ipo_promoter_details":             "stock",
#     "get_ipo_listing_info":                 "stock",
#     "get_ipo_objects_of_issue":             "stock",
#     "get_ipo_anchor_investor_details":      "stock",
#     "get_ipo_financials":                   "stock",
#     "get_ipo_product_services":             "stock",
#     "get_ipo_strength_details":             "stock",
#     "get_ipo_strategy_details":             "stock",
#     "get_ipo_risk_details":                 "stock",
#     "get_ipo_industry_peers":               "stock",
#     "get_ipo_selling_shareholders":         "stock",
#     "get_ipo_allocation_details":           "stock",
#     "get_ipo_prospectus":                   "stock",
#     "get_ipo_lead_managers":                "stock",
#     "get_ipo_registrar":                    "stock",

#     # ── Index: requires index_code ────────────────────────────────────────────
#     "get_market_indices":                   "stock",   # Indices endpoint; no param but stock-adjacent
#     "get_index_companies":                  "index",

#     # ── Market: requires group (resolved from group_master) ───────────────────
#     "get_active_performer":                 "market",
#     "get_top_gainers":                      "market",
#     "get_top_losers":                       "market",
#     "get_out_under":                        "market",
#     "get_52week":                           "market",
#     "get_new_highs":                        "market",
#     "get_sector_companies":                 "market",

#     # ── Exchange: requires ex only ────────────────────────────────────────────
#     "get_advance_decline":                  "exchange",
#     "get_exchange_holidays":                "exchange",

#     # ── MF Scheme: requires mf_schcode ────────────────────────────────────────
#     "get_scheme_nav":                       "mf_scheme",
#     "get_investment_detail":                "mf_scheme",
#     "get_expense_ratio":                    "mf_scheme",
#     "get_avg_maturity":                     "mf_scheme",
#     "get_scheme_aum":                       "mf_scheme",
#     "get_nav_historical":                   "mf_scheme",
#     "get_scheme_returns":                   "mf_scheme",
#     "get_lumpsum_returns":                  "mf_scheme",
#     "get_scheme_sip":                       "mf_scheme",
#     "get_mf_holdings":                      "mf_scheme",
#     "get_sector_allocation":                "mf_scheme",
#     "get_asset_allocation":                 "mf_scheme",
#     "get_portfolio_changes":                "mf_scheme",
#     "get_mcap_allocation":                  "mf_scheme",
#     "get_most_bought":                      "mf_scheme",
#     "get_scheme_ratios":                    "mf_scheme",
#     "get_dividend_details":                 "mf_scheme",
#     "get_bse_star_scheme":                  "mf_scheme",
#     "compare_schemes":                      "mf_scheme",
#     "get_whats_in_out":                     "mf_scheme",

#     # ── MF AMC: requires mf_cocode ────────────────────────────────────────────
#     "get_fund_categories":                  "mf_amc",
#     "get_schemes_by_amc":                   "mf_amc",
#     "get_fund_profile":                     "mf_amc",

#     # ── ETF: requires isin ────────────────────────────────────────────────────
#     "get_etf_quotes":                       "etf",
#     "get_etf_returns":                      "etf",
#     "get_etf_fundamentals":                 "etf",
#     "get_etf_about":                        "etf",
#     "get_etf_equity_holdings":              "etf",
#     "get_etf_monthly_portfo":               "etf",
#     "get_etf_sector_allocatio":             "etf",
#     "get_etf_asset_allocation":             "etf",
#     "get_etf_":                             "etf",

#     # ── Bond: requires bond_code ──────────────────────────────────────────────
#     "get_debt_eod_prices_scripwise":        "bond",
#     "get_bond_details":                     "bond",
#     "get_bond_price_history":               "bond",
#     "get_bond_cashflow":                    "bond",
#     "get_bond_rating":                      "bond",
#     "get_bond_redemption":                  "bond",
#     "get_bond_interest":                    "bond",
#     "get_bond_":                            "bond",    # generic bond prefix fallback

#     # ── IPO: no entity code needed ────────────────────────────────────────────
#     "get_forthcoming_ipo":                  "ipo",
#     "get_open_ipos":                        "ipo",
#     "get_closed_ipos":                      "ipo",
#     "get_new_ipo":                          "ipo",
#     "get_best_ipo":                         "ipo",
#     "get_ipo_master":                       "ipo",
#     "get_ipo_logo":                         "ipo",

#     # ── NFO: no entity code needed ────────────────────────────────────────────
#     "get_new_fund_offer":                   "nfo",

#     # ── News: no entity code needed ───────────────────────────────────────────
#     "get_corporate_news":                   "news",
#     "get_mf_news":                          "news",
#     "get_mf_activities":                    "news",

#     # ── Announcement: no entity code needed ───────────────────────────────────
#     "get_bse_announcement":                 "announcement",
#     "get_nse_announcement":                 "announcement",

#     # ── Market Info: no entity code needed ────────────────────────────────────
#     "get_fund_house":                       "market_info",
#     "get_amfi_master":                      "market_info",
#     "get_fund_manager":                     "market_info",
#     "get_index_list":                       "market_info",
#     "get_results_today":                    "market_info",
#     "get_result_declarations":              "market_info",
#     "get_annual_declarations":              "market_info",
#     "get_fund_performance":                 "market_info",
#     "get_category_performance":             "market_info",
#     "get_sip_dates":                        "market_info",
#     "get_macro_economic_data":              "market_info",
#     "get_forthcoming_bond_ipo":             "market_info",
#     "get_open_bond_ipo":                    "market_info",
#     "get_debt_top_value":                   "market_info",
#     "get_debt_top_volume":                  "market_info",
#     "get_debt_market_watch":                "market_info",
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
#     "co_code": {
#         "table":    "companies",
#         "column":   "companyname",
#         "id_field": "co_code",
#     },
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
#     "group": {
#         "table":    "group_master",
#         "column":   "group_name",
#         "id_field": "group_name",   # API expects the group_name string itself
#     },
#     "bond_code": {
#         "table":    "bond_master",
#         "column":   "companyname",
#         "id_field": "code",
#     },
# }

# # Maps entity_type → DB param(s) needed.
# # Types with [] need NO DB lookup — they go straight to MCP.
# ENTITY_TYPE_PARAM_MAP: dict[str, list[str]] = {
#     "stock":        ["co_code"],
#     "mf_scheme":    ["mf_schcode"],
#     "mf_amc":       ["mf_cocode"],
#     "etf":          ["isin"],
#     "index":        ["index_code"],
#     "market":       ["group"],
#     "bond":         ["bond_code"],
#     # No DB params needed — go straight to MCP:
#     "exchange":     [],
#     "ipo":          [],
#     "nfo":          [],
#     "news":         [],
#     "announcement": [],
#     "market_info":  [],
#     "general":      [],
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

#     matched_tools:  list[dict]
#     intents:        list[IntentItem]
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
#     resp  = llm.invoke([HumanMessage(content=prompt)])
#     text  = resp.content.strip()
#     clean = re.sub(r"^```(?:json)?|```$", "", text, flags=re.MULTILINE).strip()

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
# # Node AIQ: Classify + Extract + Decompose
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
# 1. A Specific Entity: (e.g., Reliance Industries, HDFC AMC, Nifty 50, SBI Small Cap Fund, Gold BeES ETF, HDFC NCD bond).
# 2. A Market Category/Concept: (e.g., "upcoming IPOs", "top gainers", "market news", "bulk deals", "top losers on NSE", "debt market watch").

# STEP 2: JSON GENERATION
# Generate exactly one intent item for every subject identified in Step 1.

# Return ONLY valid JSON:
# {{
#   "query_type": "<greeting|general|stock|mf_scheme|mf_amc|bond|comparison|investment>",
#   "is_broad": <true|false>,
#   "mcp_needed": <true|false>,
#   "intents": [
#     {{
#       "entity": "<entity name OR market concept name>",
#       "entity_type": "<see ENTITY TYPE RULES below>",
#       "intent_description": "<specific data point needed for this subject>",
#       "scheme_name": "<if mf_scheme, else empty>",
#       "amc_name": "<if mf_amc, else empty>",
#       "nse_symbol": "<NSE ticker if known, else empty>"
#     }}
#   ]
# }}

# ────────────────────────────────────────────────────────────
# ENTITY TYPE RULES (use exactly one of these values):

#   stock        → A specific listed company equity (Reliance, TCS, HDFC Bank)
#   mf_scheme    → A specific mutual fund scheme (Parag Parikh Flexi Cap, Axis Bluechip)
#   mf_amc       → A mutual fund house / AMC (SBI Mutual Fund, Nippon AMC)
#   etf          → An exchange-traded fund (Gold BeES, Nifty BeES)
#   index        → A market index (Nifty 50, Sensex, Bank Nifty)
#   market       → Market-wide movers needing a group/segment (top gainers, top losers,
#                  52-week highs, most active, sector performers, outperformers)
#   bond         → A specific listed bond, debenture, or NCD
#                  (e.g. "HDFC bond", "SBI NCD", "Tata debenture", "REC bond",
#                  "NHAI bond", any named fixed-income instrument listed on BSE/NSE)
#   exchange     → Exchange-level data with no specific entity (advances/declines, holidays)
#   ipo          → IPO related data — upcoming, open, closed, new listings, best performers,
#                  IPO master list, prospectus (NOT a specific company's anchor investors)
#   nfo          → New Fund Offers
#   news         → Corporate news, MF news, market activities feed
#   announcement → BSE/NSE corporate announcements
#   market_info  → Fund manager lists, AMFI master, fund performance rankings,
#                  category performance, SIP dates, index list, result declarations,
#                  macro economic data, debt market watch, debt top value/volume,
#                  forthcoming bond IPOs
#   general      → ONLY for purely educational/definitional questions that cannot be
#                  answered by any live data tool (e.g., "what is PE ratio?",
#                  "explain SIP", "what is a bond?", "what is NAV?")

# ────────────────────────────────────────────────────────────
# DECOMPOSITION RULES (CRITICAL):
# 1. THE 1:1 MAPPING: len(intents) MUST equal the number of subjects identified in Step 1.
# 2. NO BUNDLING: Two different subjects → two separate intent objects.
# 3. DATA POINT MERGING: Multiple metrics on the SAME entity → ONE intent.
# 4. MIXED QUERIES FULLY SUPPORTED: "Reliance price and bond market watch" →
#    intent[0] type=stock, intent[1] type=market_info.
#    "HDFC bond coupon rate and TCS share price" →
#    intent[0] type=bond, intent[1] type=stock.
#    Each intent is handled independently.
# 5. BOND vs STOCK: If a query mentions both a company's equity and its bond/NCD,
#    create SEPARATE intents — one type=stock, one type=bond.

# MCP_NEEDED LOGIC:
# - Set to true if ANY intent requires live market data (prices, NAVs, ratios,
#   lists, news, bond prices, coupon data, etc.).
# - Set to false ONLY for pure greetings or purely educational/definitional
#   questions where every single intent is type=general.
# ────────────────────────────────────────────────────────────
# """


# def node_classify_extract_and_decompose(state: AgentState) -> AgentState:
#     console.print("[Node AIQ] Classify + extract + decompose (v11.3)")

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

#         if not intents and qt not in ("greeting",):
#             console.print("  ⚠ LLM returned no intents — fallback single general intent")
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
#     console.print("[Node TVS] Per-intent tool vector search (v11.3)")

#     intents = state.get("intents") or []
#     if not intents:
#         console.print("  ⚠ No intents to search for")
#         return state

#     def _search_for_intent(intent: IntentItem) -> IntentItem:
#         et = intent.get("entity_type", "general")

#         # Purely educational — skip tool search entirely
#         if et == "general":
#             console.print(
#                 f"  [{intent['entity']}] entity_type=general → skip tool search"
#             )
#             return IntentItem(
#                 entity             = intent.get("entity", ""),
#                 entity_type        = "general",
#                 intent_description = intent.get("intent_description", ""),
#                 scheme_name        = intent.get("scheme_name"),
#                 amc_name           = intent.get("amc_name"),
#                 nse_symbol         = intent.get("nse_symbol"),
#                 tool_hint          = "",
#                 matched_tools      = [],
#                 resolved_codes     = {},
#                 mcp_result         = "",
#             )

#         # No-DB types search on intent description alone; typed entities prefix with type
#         if et in _NO_DB_MCP_TYPES:
#             search_query = intent["intent_description"]
#         else:
#             search_query = f"{et} {intent['intent_description']}"

#         tools = _query_tool_registry(search_query, n_results=8)

#         # Filter by family when we have a typed entity
#         if et not in _NO_DB_MCP_TYPES and tools:
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
#                 original     = intents[idx]
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
# investment recommendations, live NAV, MF returns, IPO listings,
# bond prices, NCD details, etc.).

# User: {query}
# """

# def node_greeting_handler(state: AgentState) -> AgentState:
#     console.print("[Node B] Greeting handler")
#     state["final_answer"] = _llm_text(GREETING_PROMPT.format(query=state["user_query"]))
#     return state


# # ── General handler (vector search fallback) ──────────────────────────────────

# def node_general_handler(state: AgentState) -> AgentState:
#     """
#     Handles purely educational/definitional questions that have no matching
#     MCP tool. Uses vector search over knowledge base.
#     Only reached when ALL intents are type=general OR no tools matched at all.
#     """
#     console.print("[Node C] General handler (vector search fallback)")
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
#             cur.execute(
#                 "SELECT mf_schcode, mf_cocode, sch_name, category FROM scheme_master"
#             )
#         elif table == "bond_master":
#             # Pull extra identifiers to improve fuzzy matching accuracy
#             cur.execute(
#                 "SELECT code, companyname, isin, nsesymbol FROM bond_master"
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
#             # For bonds, also score against isin and nsesymbol
#             if table == "bond_master":
#                 isin_score = fuzz.ratio(
#                     entity.upper(), (row.get("isin") or "").upper()
#                 )
#                 sym_score = fuzz.ratio(
#                     entity.upper(), (row.get("nsesymbol") or "").upper()
#                 )
#                 score = max(score, isin_score, sym_score)

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
#     Returns DB params to resolve for this intent.
#     Returns [] for no-DB types (general, exchange, ipo, nfo, news, etc.).
#     """
#     params = ENTITY_TYPE_PARAM_MAP.get(entity_type)
#     if params is not None:
#         if params:
#             console.print(
#                 f"  📋 entity_type='{entity_type}' → authoritative params: {params}"
#             )
#         else:
#             console.print(
#                 f"  📋 entity_type='{entity_type}' → no DB params needed"
#             )
#         return params

#     # Fallback: infer from tool metadata
#     if tool_hint:
#         for t in matched_tools:
#             if t["tool_name"] == tool_hint:
#                 db_params = [
#                     p for p in t["required_parameters"] if p in PARAM_TO_TABLE_MAP
#                 ]
#                 if db_params:
#                     return db_params
#                 break

#         meta = _get_tool_meta(tool_hint)
#         if meta:
#             db_params = [
#                 p for p in meta["required_parameters"] if p in PARAM_TO_TABLE_MAP
#             ]
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
#     needs_group     = "group"       in required_params
#     needs_bond_code = "bond_code"   in required_params   # ← NEW

#     # ── MF scheme ─────────────────────────────────────────────────────────────
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

#     # ── MF AMC ────────────────────────────────────────────────────────────────
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

#     # ── Equity / company ──────────────────────────────────────────────────────
#     if needs_co_code:
#         stock = _resolve_single(name, nse_symbol)
#         if stock:
#             codes["co_code"] = stock["co_code"]
#             result.update({
#                 "co_code":      stock["co_code"],
#                 "company_info": stock["company_info"],
#                 "nse_symbol":   stock["nse_symbol"],
#             })

#     # ── ETF ───────────────────────────────────────────────────────────────────
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

#     # ── Index ─────────────────────────────────────────────────────────────────
#     if needs_indexcode:
#         cfg   = PARAM_TO_TABLE_MAP["index_code"]
#         match = _targeted_db_lookup(
#             entity=name, table=cfg["table"],
#             name_col=cfg["column"], id_col=cfg["id_field"],
#         )
#         if match:
#             codes["index_code"] = int(match["indexcode"])
#             result["resolved_index_name"] = match.get("group_name", name)

#     # ── Market group ──────────────────────────────────────────────────────────
#     if needs_group:
#         cfg   = PARAM_TO_TABLE_MAP["group"]
#         match = _targeted_db_lookup(
#             entity=name, table=cfg["table"],
#             name_col=cfg["column"], id_col=cfg["id_field"],
#         )
#         if match:
#             codes["group"] = match["group_name"]
#             result["resolved_group_name"] = match["group_name"]
#         else:
#             console.print(
#                 f"  ⚠ group not found in group_master for '{name}' — using raw name"
#             )
#             codes["group"] = name
#             result["resolved_group_name"] = name

#     # ── Bond ──────────────────────────────────────────────────────────────────
#     if needs_bond_code:
#         cfg   = PARAM_TO_TABLE_MAP["bond_code"]
#         match = _targeted_db_lookup(
#             entity=name, table=cfg["table"],
#             name_col=cfg["column"], id_col=cfg["id_field"],
#         )
#         if match:
#             codes["bond_code"] = int(match["code"])
#             result["resolved_bond_name"] = match.get("companyname", name)
#         else:
#             console.print(f"  ⚠ bond_code not found in bond_master for '{name}'")

#     result["mcp_resolved_codes"] = codes
#     return result


# # ── Resolve codes for all intents ─────────────────────────────────────────────

# def _resolve_intents_codes(intents: list[IntentItem]) -> list[IntentItem]:
#     def _resolve_one(intent: IntentItem) -> IntentItem:
#         et        = intent.get("entity_type", "general")
#         tool_hint = intent.get("tool_hint", "")
#         matched   = intent.get("matched_tools") or []

#         # If entity_type is still 'general' but we got a tool, try to infer type
#         if et == "general" and tool_hint:
#             inferred = _infer_entity_type_from_tool(tool_hint)
#             if inferred != "general":
#                 et = inferred
#                 intent["entity_type"] = et

#         req_params = _get_required_params_for_entity(tool_hint, et, matched)

#         # Skip DB entirely for no-param types
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
#                 console.print(
#                     f"  [red]Code resolution failed for intent #{idx}: {exc}[/red]"
#                 )
#                 result_intents[idx] = intents[idx]

#     return [result_intents[i] for i in sorted(result_intents)]


# # ══════════════════════════════════════════════════════════════════════════════
# # MCP Pre-Resolution nodes
# # ══════════════════════════════════════════════════════════════════════════════

# def node_mcp_pre_resolve(state: AgentState) -> AgentState:
#     console.print("[Node MPR] Single-entity MCP pre-resolve (v11.3)")

#     intents = state.get("intents") or []
#     if not intents:
#         console.print("  ⚠ No intents; skipping resolve")
#         state["mcp_resolved_codes"] = {}
#         return state

#     resolved_intents            = _resolve_intents_codes(intents)
#     state["intents"]            = resolved_intents
#     state["mcp_resolved_codes"] = (
#         resolved_intents[0].get("resolved_codes", {}) if resolved_intents else {}
#     )

#     console.print(f"  🏁 Primary resolved codes: {state['mcp_resolved_codes']}")
#     return state


# def node_mcp_multi_pre_resolve(state: AgentState) -> AgentState:
#     console.print("[Node MMPR] Multi-entity MCP pre-resolve (v11.3)")

#     intents = state.get("intents") or []
#     if not intents:
#         console.print("  ⚠ No intents; skipping resolve")
#         return state

#     resolved_intents            = _resolve_intents_codes(intents)
#     state["intents"]            = resolved_intents
#     state["mcp_resolved_codes"] = (
#         resolved_intents[0].get("resolved_codes", {}) if resolved_intents else {}
#     )

#     console.print(
#         f"  🏁 Resolved {len(resolved_intents)} intents. "
#         f"Primary codes: {state['mcp_resolved_codes']}"
#     )
#     return state


# # ══════════════════════════════════════════════════════════════════════════════
# # Node MTCI: MCP Tool Call — sequential per-intent in one session (v11.3)
# # ══════════════════════════════════════════════════════════════════════════════

# def _run_mcp_multi_in_thread(intents: list[dict]) -> list[dict]:
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
#                 loop.run_until_complete(
#                     asyncio.gather(*pending, return_exceptions=True)
#                 )
#         except Exception:
#             pass
#         loop.close()


# def node_mcp_tool_call_intents(state: AgentState) -> AgentState:
#     console.print("[Node MTCI] Sequential per-intent MCP calls — one session (v11.3)")

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

#     # Separate intents: MCP-capable vs general (no tool found)
#     mcp_intents = [
#         i for i in intents
#         if i.get("tool_hint") or i.get("entity_type") not in ("general",)
#     ]
#     general_intents = [
#         i for i in intents
#         if not i.get("tool_hint") and i.get("entity_type") == "general"
#     ]

#     if general_intents:
#         console.print(
#             f"  ℹ {len(general_intents)} general intent(s) answered via synthesis only"
#         )

#     # Fallback: no MCP intents at all
#     if not mcp_intents:
#         console.print("  ⚠ No MCP intents; falling back to raw user query")
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

#     try:
#         with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
#             future         = ex.submit(_run_mcp_multi_in_thread, mcp_intents)
#             filled_intents = future.result(timeout=600)
#     except concurrent.futures.TimeoutError:
#         console.print("  [red]Multi-intent MCP timed out[/red]")
#         filled_intents = [
#             {**dict(i), "mcp_result": "MCP call timed out."}
#             for i in mcp_intents
#         ]
#     except Exception as exc:
#         console.print(f"  [red]Multi-intent MCP failed: {exc}[/red]")
#         filled_intents = [
#             {**dict(i), "mcp_result": f"MCP call failed: {exc}"}
#             for i in mcp_intents
#         ]

#     # Merge MCP results + general intents (handled in synthesis)
#     all_intents = filled_intents + [
#         {**dict(i), "mcp_result": "(answered from knowledge base)"}
#         for i in general_intents
#     ]
#     state["intents"] = all_intents

#     state["intent_results"] = [
#         {
#             "entity":             i.get("entity", ""),
#             "entity_type":        i.get("entity_type", ""),
#             "intent_description": i.get("intent_description", ""),
#             "mcp_result":         i.get("mcp_result", ""),
#         }
#         for i in all_intents
#     ]

#     state["mcp_raw_result"] = "\n\n---\n\n".join(
#         f"[{r['entity']} / {r['intent_description']}]\n{r['mcp_result']}"
#         for r in state["intent_results"]
#         if r.get("mcp_result")
#     )
#     state["mcp_tool_calls_made"] = []
#     state["error"]               = None

#     total_chars = len(state["mcp_raw_result"])
#     console.print(
#         f"  ✅ {len(filled_intents)} MCP intent(s) resolved "
#         f"({len(general_intents)} general skipped). "
#         f"Total chars for synthesis: {total_chars}"
#     )
#     return state


# # ══════════════════════════════════════════════════════════════════════════════
# # MCP Synthesis (v11.3)
# # ══════════════════════════════════════════════════════════════════════════════

# MCP_SYNTHESIS_PROMPT = """\
# You are a knowledgeable Indian equity, mutual fund, and fixed-income analyst assistant.

# ## Conversation history (last 4 turns)
# {history}

# ## User Question
# {user_query}

# ## Live Data Retrieved (one block per sub-question)
# {mcp_result}

# ## Instructions
# 1. Answer EACH sub-question using the data block labelled for it above.
#    If there are multiple data blocks, address each one in turn.
# 2. Sub-questions marked "(answered from knowledge base)" should be answered
#    from your own knowledge directly in the response.
# 3. Write in plain prose paragraphs only. No markdown, no bullet points,
#    no asterisks, no tables. Do NOT use the rupee symbol — write "Rs".
# 4. Present key numbers naturally woven into sentences.
# 5. If a specific data field is missing or the tool returned an error, say so briefly.
# 6. Use Indian number formatting (lakh, crore) for large figures.
# 7. For bond/debt data include coupon rate, maturity date, credit rating, and
#    face value where available.
# 8. Keep under 300 words unless the user explicitly asked for detail.
# 9. End with: This is not financial advice.
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
#     console.print("[Node MS] MCP synthesis (v11.3)")
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
#         state["final_answer"] = mcp_result
#     return state


# # ── General synthesis (vector search) ────────────────────────────────────────

# GENERAL_SYNTHESIS_PROMPT = """\
# You are a knowledgeable Indian equity and fixed-income analyst assistant.

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
#     console.print("[Node S] General synthesis (vector search)")
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
# # Router (v11.3)
# # ══════════════════════════════════════════════════════════════════════════════

# def route_after_classify(state: AgentState) -> str:
#     qt      = state.get("query_type", "general")
#     intents = state.get("intents") or []

#     if qt == "greeting":
#         return "greeting"

#     mcp_intents = [
#         i for i in intents
#         if i.get("tool_hint") or i.get("entity_type") not in ("general",)
#     ]

#     for idx, intent in enumerate(intents):
#         console.print(
#             f"  [Router] intent[{idx}] entity={intent.get('entity')} "
#             f"type={intent.get('entity_type')} "
#             f"tool_hint={intent.get('tool_hint') or '(none)'}"
#         )

#     # All intents are pure general (no tools, all type=general) → vector search
#     if not mcp_intents or (
#         not any(i.get("tool_hint") for i in intents)
#         and all(i.get("entity_type") == "general" for i in intents)
#     ):
#         console.print("  → general (all intents are educational/no tools)")
#         return "general"

#     has_many         = len(intents) > 1
#     additional       = state.get("companies") or []
#     primary_type     = state.get("primary_entity_type", "general")
#     secondary_types  = {c.get("entity_type", "general") for c in additional}
#     is_heterogeneous = bool(secondary_types - {"general"} - {primary_type})

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
# # Graph (v11.3)
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




# ######  stock problem ipo solved ########


# """
# graph.py — EQUIFIZ Financial AI Agent (v11.4)

# Key changes from v11.3
# ──────────────────────────────────────────────────────────────────────
# 1. IPO entity_type split — two sub-cases now handled correctly:

#    ipo_list   → Generic IPO queries: forthcoming, open, closed, new
#                 listings, best performers, IPO master.
#                 Requires NO DB code. Goes straight to MCP.

#    ipo_stock  → Company-specific IPO queries: anchor investors,
#                 subscription status, timeline, financials, prospectus,
#                 listing info, promoter details, etc. for a NAMED company.
#                 Requires co_code (resolved from companies table just
#                 like entity_type='stock').

#    The LLM decomposition prompt now enforces this split explicitly.
#    _TOOL_FAMILY_HINTS, ENTITY_TYPE_PARAM_MAP, and
#    _NO_DB_MCP_TYPES all updated accordingly.

# 2. TVS family filter hardened:
#    - 'ipo_list'  intents only match tools in family 'ipo_list'.
#    - 'ipo_stock' intents only match tools in family 'ipo_stock'.
#    No cross-contamination between the two.

# 3. _resolve_entity_codes: ipo_stock branch added — resolves co_code
#    using the same _resolve_single() path as entity_type='stock'.

# 4. MCP hallucination guard in _get_required_params_for_entity:
#    if resolved_codes is empty for a param-required entity, a warning
#    is logged and the intent is flagged — mcp_client can check this.

# 5. Bond entity handling from v11.3 fully preserved.

# 6. All v11.2 / v11.3 logic preserved otherwise.
# """
# from __future__ import annotations

# import asyncio
# import concurrent.futures
# import json
# import logging
# import re
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

# # ── Types ─────────────────────────────────────────────────────────────────────

# class Turn(TypedDict):
#     role: str       # "user" | "assistant"
#     content: str


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
#     code_missing:       bool   # ← NEW: flag set when DB lookup failed for required code


# # ══════════════════════════════════════════════════════════════════════════════
# # Entity type taxonomy
# # ══════════════════════════════════════════════════════════════════════════════
# #
# # Types that require a DB entity-code lookup:
# #   stock      → co_code         (equity / company)
# #   mf_scheme  → mf_schcode      (mutual-fund scheme)
# #   mf_amc     → mf_cocode       (AMC / fund-house)
# #   etf        → isin            (exchange-traded fund)
# #   index      → index_code      (market index)
# #   market     → group           (market segment, resolved from group_master)
# #   bond       → bond_code       (listed bond / debenture / NCD)
# #   ipo_stock  → co_code         (company-specific IPO data: anchor investors,
# #                                 subscription, timeline, financials, prospectus,
# #                                 promoter details, listing info for a NAMED company)
# #
# # Types that go to MCP but need NO DB lookup (no entity code):
# #   exchange    → ex only (NSE/BSE constant)
# #   ipo_list    → Generic IPO listings: forthcoming, open, closed, new listing,
# #                 best performers, IPO master, IPO logo
# #   nfo         → New Fund Offers
# #   news        → Corporate news, MF news
# #   announcement→ BSE/NSE announcements
# #   market_info → Fund performance, category performance, SIP dates,
# #                 fund manager lists, AMFI master, index list, result declarations,
# #                 macro economic data, debt market watch, debt top value/volume
# #
# # Type that bypasses MCP entirely (vector search fallback):
# #   general → truly unanswerable by live data (definitions, greetings overflow,
# #             purely educational questions with no matching tool)
# #
# # ══════════════════════════════════════════════════════════════════════════════

# # All entity types that require NO DB code resolution but still go to MCP
# _NO_DB_MCP_TYPES = frozenset({
#     "exchange",
#     "ipo_list",
#     "nfo",
#     "news",
#     "announcement",
#     "market_info",
# })

# # ══════════════════════════════════════════════════════════════════════════════
# # Tool-family hints — mapped by the actual required parameter each tool uses
# # ══════════════════════════════════════════════════════════════════════════════

# _TOOL_FAMILY_HINTS: dict[str, str] = {

#     # ── Stock: requires co_code ───────────────────────────────────────────────
#     "get_company_stock_price": "stock",
#     "get_delayed_stock_price": "stock",
#     "get_nse_company_announcements":        "stock",
#     "get_bse_company_announcements":        "stock",
#     "get_company_result_schedule":          "stock",
#     "get_company_profile":                  "stock",
#     "get_company_background":                "stock",
#     "get_board_of_directors":                 "stock",
#     "get_company_bankers":                  "stock",
#     "get_management_biodata":                       "stock",
#     "get_subsidiaries_jvs":                     "stock",
#     "get_related_party_transactions":                    "stock",
#     "get_employee_count":                   "stock",
#     "get_capital_structure":                "stock",
#     "get_pledge_share_details":                     "stock",
#     "get_substantial_acquisitions":                      "stock",
#     "get_segment_data":                     "stock",
#     "get_r_and_d_expenditure":                          "stock",
#     "get_finished_products":                "stock",
#     "get_raw_materials":                    "stock",
#     "get_chronological_history":                    "stock",
#     "get_company_history":                  "stock",
#     "get_funds_holding_company":            "stock",
#     # Financials
#     "get_quarterly_results":                "stock",
#     "get_profit_loss":                      "stock",
#     "get_balance_sheet":                    "stock",
#     "get_cash_flow":                        "stock",
#     "get_half_yearly_results":              "stock",
#     "get_nine_months_results":              "stock",
#     "get_quarterly_trends" :                 "stock",
#     "get_shareholding_pattern":              "stock",
#     "get_major_shareholders":                "stock",
#     "get_quarterly_balance_sheet" :         "stock",
#     "get_yearly_results":                   "stock",
#     "get_annual_balance_sheet":              "stock",
#     "get_half_yearly_balance_sheet":         "stock",
#     "get_ttm_growth_trends":                 "stock",
#     "get_quarterly_revenue_trends":          "stock",
#     "get_quarterly_ebitda_trends":           "stock",
#     "get_quarterly_ebit_trends":             "stock",
#     "get_growth_data_quarterly":             "stock",
#     "get_growth_data_yearly":                "stock",
#     # Ratios
#     "get_key_financial_ratios":             "stock",
#     "get_daily_ratios":                     "stock",
#     "get_margin_ratios":                    "stock",
#     "get_valuation_ratios":                 "stock",
#     "get_all_basic_ratios":                 "stock",
#     "get_return_ratios":                    "stock",
#     "get_growth_ratios":                    "stock",
#     "get_performance_ratios":                "stock",
#     "get_efficiency_ratios":                 "stock",
#     "get_cashflow_ratios":                  "stock",
#     "get_liquidity_ratios":                 "stock",
#     "get_solvency_ratios":                  "stock",
#     "get_quarterly_ratios":                 "stock",
#     "get_yearly_ratios":                    "stock",
#     "get_shareholding":                     "stock",
#     "get_major_sharehold":                  "stock",
#     "get_financial_stability_ratios":       "stock",

#     # ── Company-specific IPO tools: require co_code — family = ipo_stock ─────
#     # These tools need a resolved co_code but are IPO-domain, not equity-domain.
#     # Keeping them separate from 'stock' prevents cross-contamination in TVS
#     # filtering while still triggering co_code DB resolution.
#     "get_anchor_investor":                  "ipo_stock",
#     "get_ipo_details":                      "ipo_stock",
#     "get_ipo_subscription_status":          "ipo_stock",
#     "get_ipo_synopsis":                     "ipo_stock",
#     "get_ipo_timeline":                     "ipo_stock",
#     "get_ipo_promoter_details":             "ipo_stock",
#     "get_ipo_listing_info":                 "ipo_stock",
#     "get_ipo_objects_of_issue":             "ipo_stock",
#     "get_ipo_anchor_investor_details":      "ipo_stock",
#     "get_ipo_industry_peers":                "ipo_stock",
#     "get_ipo_financials":                   "ipo_stock",
#     "get_ipo_product_services":             "ipo_stock",
#     "get_ipo_strength_details":             "ipo_stock",
#     "get_ipo_strategy_details":             "ipo_stock",
#     "get_ipo_risk_details":                 "ipo_stock",
#     "get_ipo_industry_peers":               "ipo_stock",
#     "get_ipo_selling_shareholders":         "ipo_stock",
#     "get_ipo_allocation_details":           "ipo_stock",
#     "get_ipo_prospectus":                   "ipo_stock",
#     "get_ipo_lead_managers":                "ipo_stock",
#     "get_ipo_registrar":                    "ipo_stock",
#     "get_ipo_customer_details":              "ipo_stock",

#     # ── Index: requires index_code ────────────────────────────────────────────
#     "get_market_indices":                   "stock",   # no specific param but stock-adjacent
#     "get_index_companies":                  "index",

#     # ── Market: requires group (resolved from group_master) ───────────────────
#     "get_active_performer":                 "market",
#     "get_top_gainers":                      "market",
#     "get_top_losers":                       "market",
#     "get_out_under_performers":                   "market",
#     "get_52week_highs":                           "market",
#     "get_52week_lows":                      "market",
#     "get_new_highs_lows":                        "market",
#     "get_sector_companies":                 "market",
#     "get_market_indices":                    "market",

#     # ── Exchange: requires ex only ────────────────────────────────────────────
#     "get_advance_decline":                  "exchange",
#     "get_exchange_holidays":                "exchange",
    

#     # ── MF Scheme: requires mf_schcode ────────────────────────────────────────
#     "get_scheme_nav":                       "mf_scheme",
#     "get_investment_details":                "mf_scheme",
#     "get_expense_ratio":                    "mf_scheme",
#     "get_avg_maturity":                     "mf_scheme",
#     "get_scheme_aum":                       "mf_scheme",
#     "get_nav_historical":                   "mf_scheme",
#     "get_scheme_returns":                   "mf_scheme",
#     "get_lumpsum_returns":                  "mf_scheme",
#     "get_scheme_sip":                       "mf_scheme",
#     "get_mf_holdings":                      "mf_scheme",
#     "get_sector_allocation":                "mf_scheme",
#     "get_asset_allocation":                 "mf_scheme",
#     "get_portfolio_changes":                "mf_scheme",
#     "get_mcap_allocation":                  "mf_scheme",
#     "get_most_bought_sold":                  "mf_scheme",
#     "get_scheme_ratios":                    "mf_scheme",
#     "get_dividend_details":                 "mf_scheme",
#     "get_bse_star_scheme":                  "mf_scheme",
#     "compare_schemes":                      "mf_scheme",
#     "get_whats_in_out":                     "mf_scheme",
#     "get_scheme_sip_rules":                 "mf_scheme",
#     "get_scheme_sip_details":               "mf_scheme",

#     # ── MF AMC: requires mf_cocode ────────────────────────────────────────────
#     "get_fund_categories":                  "mf_amc",
#     "get_schemes_by_amc":                   "mf_amc",
#     "get_fund_profile":                     "mf_amc",
#     "get_fund_managers":                     "mf_amc",

#     # ── ETF: requires isin ────────────────────────────────────────────────────
#     "get_etf_quotes":                       "etf",
#     "get_etf_returns":                      "etf",
#     "get_etf_fundamentals":                 "etf",
#     "get_etf_about":                        "etf",
#     "get_etf_equity_holdings":              "etf",
#     "get_etf_monthly_portfolio":               "etf",
#     "get_etf_sector_allocatio":             "etf",
#     "get_etf_asset_allocation":             "etf",
#     "get_etf_":                             "etf",

#     # ── Bond: requires bond_code ──────────────────────────────────────────────
#     "get_debt_eod_prices_scripwise":        "bond",
#     "get_bond_details":                     "bond",
#     "get_bond_price_history":               "bond",
#     "get_bond_cashflow":                    "bond",
#     "get_bond_rating":                      "bond",
#     "get_bond_redemption":                  "bond",
#     "get_bond_interest":                    "bond",
#     "get_bond_":                            "bond",

#     # ── IPO list: NO entity code needed ──────────────────────────────────────
#     "get_forthcoming_ipos":                  "ipo_list",
#     "get_open_ipos":                        "ipo_list",
#     "get_closed_ipos":                      "ipo_list",
#     "get_new_ipo_listings":                          "ipo_list",
#     "get_best_ipo_performers":                         "ipo_list",
#     "get_ipo_master":                       "ipo_list",
#     "get_ipo_logo":                         "ipo_list",
#     "get_forthcoming_drh_filings":           "ipo_list",
#     "get_basis_of_allotment" :               "ipo_list",

#     # bond _ ipo
#     "get_open_bond_ipo":                      "ipo_list",
#     "get_forthcoming_bond_ipo" :               "ipo_list",

#     # ── NFO: no entity code needed ────────────────────────────────────────────
#     "get_new_fund_offer":                   "nfo",

#     # ── News: no entity code needed ───────────────────────────────────────────
#     "get_corporate_news":                   "news",
#     "get_mf_news":                          "news",
#     "get_mf_market_activity":                    "news",

#     # ── Announcement: no entity code needed ───────────────────────────────────
#     "get_bse_announcements":                 "announcement",
#     "get_nse_announcements":                 "announcement",

#     # ── Market Info: no entity code needed ────────────────────────────────────
#     "get_fund_house":                       "market_info",
#     "get_amfi_master":                      "market_info",
#     "get_fund_manager":                     "market_info",
#     "get_index_list":                       "market_info",
#     "get_results_today":                    "market_info",
#     "get_result_declarations":              "market_info",
#     "get_annual_declarations":              "market_info",
#     "get_fund_performance":                 "market_info",
#     "get_category_performance":             "market_info",
#     "get_sip_dates":                        "market_info",
#     "get_macro_economic_data":              "market_info",
#     "get_forthcoming_bond_ipo":             "market_info",
#     "get_open_bond_ipo":                    "market_info",
#     "get_debt_top_value":                   "market_info",
#     "get_debt_top_volume":                  "market_info",
#     "et_new_fund_offers" :                 "market_info",
#     "get_debt_market_watch":                "market_info",
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
#     "co_code": {
#         "table":    "companies",
#         "column":   "companyname",
#         "id_field": "co_code",
#     },
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
#     "group": {
#         "table":    "group_master",
#         "column":   "group_name",
#         "id_field": "group_name",
#     },
#     "bond_code": {
#         "table":    "bond_master",
#         "column":   "companyname",
#         "id_field": "code",
#     },
# }

# # Maps entity_type → DB param(s) needed.
# # Types with [] need NO DB lookup — they go straight to MCP.
# ENTITY_TYPE_PARAM_MAP: dict[str, list[str]] = {
#     "stock":        ["co_code"],
#     "ipo_stock":    ["co_code"],    # ← company-specific IPO; same DB lookup as stock
#     "mf_scheme":    ["mf_schcode"],
#     "mf_amc":       ["mf_cocode"],
#     "etf":          ["isin"],
#     "index":        ["index_code"],
#     "market":       ["group"],
#     "bond":         ["bond_code"],
#     # No DB params needed — go straight to MCP:
#     "exchange":     [],
#     "ipo_list":     [],
#     "nfo":          [],
#     "news":         [],
#     "announcement": [],
#     "market_info":  [],
#     "general":      [],
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

#     matched_tools:  list[dict]
#     intents:        list[IntentItem]
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
#     resp  = llm.invoke([HumanMessage(content=prompt)])
#     text  = resp.content.strip()
#     clean = re.sub(r"^```(?:json)?|```$", "", text, flags=re.MULTILINE).strip()

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
# # Node AIQ: Classify + Extract + Decompose
# # ══════════════════════════════════════════════════════════════════════════════

# # CLASSIFY_EXTRACT_DECOMPOSE_PROMPT = """\
# # You are a Lead Financial Systems Analyst for an Indian stock-market assistant. Your task is to decompose user queries into executable data intents.

# # ## Context
# # - History: {history}
# # - Today's Date: {today}
# # - Query: "{query}"

# # ────────────────────────────────────────────────────────────
# # STEP 1: SUBJECT IDENTIFICATION
# # Identify every distinct subject the user is asking about. A subject is either:
# # 1. A Specific Entity: (e.g., Reliance Industries, HDFC AMC, Nifty 50, SBI Small Cap Fund, Gold BeES ETF, HDFC NCD bond).
# # 2. A Market Category/Concept: (e.g., "upcoming IPOs", "top gainers", "market news", "bulk deals", "top losers on NSE", "debt market watch").

# # STEP 2: JSON GENERATION
# # Generate exactly one intent item for every subject identified in Step 1.

# # Return ONLY valid JSON:
# # {{
# #   "query_type": "<greeting|general|stock|mf_scheme|mf_amc|bond|comparison|investment>",
# #   "is_broad": <true|false>,
# #   "mcp_needed": <true|false>,
# #   "intents": [
# #     {{
# #       "entity": "<entity name OR market concept name>",
# #       "entity_type": "<see ENTITY TYPE RULES below>",
# #       "intent_description": "<specific data point needed for this subject>",
# #       "scheme_name": "<if mf_scheme, else empty>",
# #       "amc_name": "<if mf_amc, else empty>",
# #       "nse_symbol": "<NSE ticker if known, else empty>"
# #     }}
# #   ]
# # }}

# # ────────────────────────────────────────────────────────────
# # ENTITY TYPE RULES (use exactly one of these values):

# #   stock        → A specific listed company's equity data (price, financials, ratios,
# #                  shareholding, profile, management, etc.)
# #                  Examples: Reliance Industries, TCS, HDFC Bank share price

# #   ipo_stock    → Company-SPECIFIC IPO data where you know the company name.
# #                  Use this when the user asks about IPO details, anchor investors,
# #                  subscription status, IPO timeline, IPO financials, IPO prospectus,
# #                  IPO listing info, IPO promoter details, IPO risk factors, IPO objects,
# #                  IPO lead managers, IPO registrar, basis of allotment — for a NAMED company.
# #                  ⚠ CRITICAL: Use ipo_stock (NOT ipo_list) whenever a specific company
# #                  name is mentioned alongside IPO-related data.
# #                  Examples: "Amir Chand IPO details", "TCS anchor investors",
# #                  "HDFC Bank IPO subscription status", "Reliance IPO prospectus"

# #   ipo_list     → Generic/market-wide IPO queries with NO specific company name.
# #                  Use when user wants a LIST or overview — forthcoming IPOs, open IPOs,
# #                  closed IPOs, new listings, best performers, IPO master list, IPO logo.
# #                  ⚠ CRITICAL: Use ipo_list (NOT ipo_stock) when no specific company
# #                  is named, or when the user wants a list of IPOs.
# #                  Examples: "upcoming IPOs", "open IPOs right now",
# #                  "which IPOs are closing this week", "recent IPO listings",
# #                  "best performing IPOs", "IPO master"

# #   mf_scheme    → A specific mutual fund scheme (Parag Parikh Flexi Cap, Axis Bluechip)

# #   mf_amc       → A mutual fund house / AMC (SBI Mutual Fund, Nippon AMC)

# #   etf          → An exchange-traded fund (Gold BeES, Nifty BeES)

# #   index        → A market index (Nifty 50, Sensex, Bank Nifty)

# #   market       → Market-wide movers needing a group/segment (top gainers, top losers,
# #                  52-week highs, most active, sector performers, outperformers)

# #   bond         → A specific listed bond, debenture, or NCD
# #                  (e.g. "HDFC bond", "SBI NCD", "Tata debenture", "REC bond",
# #                  "NHAI bond", any named fixed-income instrument listed on BSE/NSE)

# #   exchange     → Exchange-level data with no specific entity (advances/declines, holidays)

# #   nfo          → New Fund Offers

# #   news         → Corporate news, MF news, market activities feed

# #   announcement → BSE/NSE corporate announcements

# #   market_info  → Fund manager lists, AMFI master, fund performance rankings,
# #                  category performance, SIP dates, index list, result declarations,
# #                  macro economic data, debt market watch, debt top value/volume,
# #                  forthcoming bond IPOs

# #   general      → ONLY for purely educational/definitional questions that cannot be
# #                  answered by any live data tool (e.g., "what is PE ratio?",
# #                  "explain SIP", "what is a bond?", "what is NAV?")

# # ────────────────────────────────────────────────────────────
# # DECOMPOSITION RULES (CRITICAL):
# # 1. THE 1:1 MAPPING: len(intents) MUST equal the number of subjects identified in Step 1.
# # 2. NO BUNDLING: Two different subjects → two separate intent objects.
# # 3. DATA POINT MERGING: Multiple metrics on the SAME entity → ONE intent.
# # 4. MIXED QUERIES FULLY SUPPORTED: "Reliance price and bond market watch" →
# #    intent[0] type=stock, intent[1] type=market_info.
# #    "HDFC bond coupon rate and TCS share price" →
# #    intent[0] type=bond, intent[1] type=stock.
# #    Each intent is handled independently.
# # 5. BOND vs STOCK: If a query mentions both a company's equity and its bond/NCD,
# #    create SEPARATE intents — one type=stock, one type=bond.
# # 6. IPO_STOCK vs IPO_LIST: This is the most important split.
# #    - Named company + IPO context → ipo_stock
# #    - No company name / market-wide IPO query → ipo_list
# #    "Amir Chand IPO details" → ipo_stock (company named)
# #    "What are the upcoming IPOs?" → ipo_list (no company named)
# #    "Reliance IPO prospectus" → ipo_stock (company named)
# #    "Best IPO performers this month" → ipo_list (no company named)

# # MCP_NEEDED LOGIC:
# # - Set to true if ANY intent requires live market data (prices, NAVs, ratios,
# #   lists, news, bond prices, coupon data, etc.).
# # - Set to false ONLY for pure greetings or purely educational/definitional
# #   questions where every single intent is type=general.
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
# 1. A Specific Entity: (e.g., Reliance Industries, HDFC AMC, Nifty 50, SBI Small Cap Fund, Gold BeES ETF, HDFC NCD bond).
# 2. A Market Category/Concept: (e.g., "upcoming IPOs", "top gainers", "market news", "bulk deals", "top losers on NSE", "debt market watch").

# STEP 2: JSON GENERATION
# Generate exactly one intent item for every subject identified in Step 1.

# Return ONLY valid JSON:
# {{
#   "query_type": "<greeting|general|stock|mf_scheme|mf_amc|bond|comparison|investment>",
#   "is_broad": <true|false>,
#   "mcp_needed": <true|false>,
#   "intents": [
#     {{
#       "entity": "<entity name OR market concept name>",
#       "entity_type": "<see ENTITY TYPE RULES below>",
#       "intent_description": "<specific data point needed for this subject>",
#       "scheme_name": "<if mf_scheme, else empty>",
#       "amc_name": "<if mf_amc, else empty>",
#       "nse_symbol": "<NSE ticker if known, else empty>"
#     }}
#   ]
# }}

# ────────────────────────────────────────────────────────────
# ENTITY TYPE RULES (use exactly one of these values):

#   stock        → A specific listed company's EQUITY data. Use when the user asks about
#                  share price, stock quote, OHLC, 52-week high/low, delivery volume,
#                  delayed price, company profile, background, board of directors, bankers,
#                  management team, subsidiaries, related parties, employee count, capital
#                  structure, pledged shares, substantial shareholders, segment data, R&D,
#                  finished products, raw materials, chronological events, company history,
#                  quarterly/annual financial results, P&L, balance sheet, cash flow,
#                  half-yearly/nine-month results, TTM growth, revenue/EBITDA/EBIT trends,
#                  key financial ratios, daily ratios, margin ratios, valuation ratios,
#                  return ratios, growth ratios, performance ratios, cashflow ratios,
#                  liquidity ratios, solvency ratios, shareholding pattern, major shareholders.
#                  ⚠ CRITICAL: Use stock ONLY for equity/share data of a company.
#                  If the query is about the same company's IPO — use ipo_stock instead.
#                  If the query is about the same company's bond/NCD — use bond instead.
#                  Examples: "Reliance share price", "TCS quarterly results",
#                  "HDFC Bank balance sheet", "Infosys management team",
#                  "Wipro shareholding pattern", "ITC PE ratio"

#   ipo_stock    → Company-SPECIFIC IPO data where you know the company name.
#                  Use this when the user asks about IPO details, GMP, allotment status,
#                  anchor investors, anchor investor details, subscription status,
#                  IPO synopsis, IPO timeline, IPO promoter details, IPO listing info,
#                  IPO objects of issue, IPO financials, IPO products/services,
#                  IPO strengths, IPO strategy, IPO risk factors, IPO industry peers,
#                  IPO selling shareholders, IPO allocation details, IPO prospectus,
#                  IPO lead managers, IPO registrar, basis of allotment —
#                  all for a NAMED company.
#                  ⚠ CRITICAL: Use ipo_stock (NOT ipo_list) whenever a specific company
#                  name is mentioned alongside IPO-related data. A co_code DB lookup
#                  will be performed — the company must exist in our database.
#                  Examples: "Amir Chand IPO details", "TCS anchor investors",
#                  "HDFC Bank IPO subscription status", "Reliance IPO prospectus",
#                  "Zomato IPO allotment", "Paytm IPO risk factors",
#                  "LIC IPO lead managers", "Adani IPO timeline"

#   ipo_list     → Generic/market-wide IPO queries with NO specific company name.
#                  Use when the user wants a LIST, overview, or status of IPOs across
#                  the market — forthcoming IPOs, upcoming IPOs, open IPOs, closed IPOs,
#                  new listings, recently listed IPOs, best performing IPOs, IPO master
#                  list, IPO logo, IPO calendar.
#                  ⚠ CRITICAL: Use ipo_list (NOT ipo_stock) when no specific company
#                  is named, or when the user explicitly wants a list/overview of IPOs.
#                  No DB lookup is needed — goes straight to MCP.
#                  Examples: "upcoming IPOs", "open IPOs right now",
#                  "which IPOs are closing this week", "recent IPO listings",
#                  "best performing IPOs this month", "IPO master list",
#                  "forthcoming IPOs on NSE", "new listings today"

#   mf_scheme    → A specific named mutual fund SCHEME. Use when the user asks about
#                  NAV (current or historical), investment details, expense ratio,
#                  average maturity, AUM, scheme returns (1Y/3Y/5Y), lumpsum returns,
#                  SIP calculator, MF holdings/portfolio, sector allocation, asset
#                  allocation, portfolio changes, market-cap allocation, most bought
#                  stocks, scheme ratios, dividend history, BSE STAR platform details,
#                  scheme comparison, what's in/out of portfolio — for a NAMED scheme.
#                  ⚠ CRITICAL: The scheme name must be specific enough to look up in
#                  scheme_master. Provide scheme_name field in JSON.
#                  Examples: "Parag Parikh Flexi Cap NAV", "Axis Bluechip returns",
#                  "SBI Small Cap Fund portfolio", "Mirae Asset Large Cap expense ratio",
#                  "HDFC Mid-Cap Opportunities AUM", "compare Axis vs Mirae bluechip"

#   mf_amc       → A mutual fund HOUSE or AMC (Asset Management Company) — NOT a
#                  specific scheme. Use when the user asks about all schemes offered by
#                  a fund house, fund categories under an AMC, AMC profile/overview,
#                  or fund house details.
#                  ⚠ CRITICAL: Use mf_amc for the fund house itself, not its individual
#                  schemes. Provide amc_name field in JSON.
#                  Examples: "SBI Mutual Fund schemes", "Nippon AMC fund categories",
#                  "HDFC AMC profile", "Axis Mutual Fund all funds",
#                  "Franklin Templeton fund house details", "DSP AMC schemes list"

#   etf          → An exchange-traded fund. Use when the user asks about ETF quotes,
#                  ETF returns, ETF fundamentals, ETF profile/about, ETF equity holdings,
#                  ETF monthly portfolio, ETF sector allocation, ETF asset allocation —
#                  for a NAMED ETF. ETFs trade on exchange like stocks but track an index
#                  or commodity.
#                  ⚠ CRITICAL: Distinguish from regular MF schemes — ETFs have an ISIN
#                  and trade on NSE/BSE intraday. Gold ETF, Index ETF, Sectoral ETF.
#                  Examples: "Gold BeES NAV", "Nifty BeES returns", "SBI ETF Nifty 50",
#                  "Nippon India ETF holdings", "HDFC Sensex ETF fundamentals",
#                  "Bharat Bond ETF portfolio"

#   index        → A market INDEX. Use when the user asks about index constituents,
#                  companies within an index, index composition — for a NAMED index.
#                  ⚠ NOTE: For live index price/level use stock type (get_market_indices
#                  is stock-adjacent). Use index type specifically for index membership
#                  queries (which companies are in Nifty 50, Bank Nifty constituents).
#                  Examples: "Nifty 50 constituents", "Bank Nifty companies list",
#                  "Sensex component stocks", "Nifty Midcap 100 members",
#                  "which stocks are in Nifty IT index"

#   market       → Market-WIDE movers or screeners that require a market GROUP or
#                  SEGMENT (e.g. NSE, BSE, specific sector). Use when the user wants
#                  ranked lists across the market: top gainers, top losers, most active
#                  stocks, 52-week highs/lows, new highs, outperformers, underperformers,
#                  sector-wise top performers, delayed/active performers.
#                  ⚠ CRITICAL: This is NOT about a single company — it's about a
#                  ranked list across a market segment. A group code is resolved from
#                  group_master (e.g. "NSE", "BSE", "NIFTY50").
#                  Examples: "top gainers on NSE today", "most active stocks BSE",
#                  "52-week high stocks", "top losers Nifty", "new highs today",
#                  "which sectors are outperforming", "best performing sector stocks"

#   bond         → A specific named listed BOND, DEBENTURE, or NCD (Non-Convertible
#                  Debenture). Use when the user asks about bond EOD prices, bond details
#                  (coupon rate, face value, maturity), bond price history, bond cash
#                  flows, bond credit rating, bond redemption schedule, bond interest
#                  payment schedule — for a NAMED bond/NCD/debenture issuer.
#                  ⚠ CRITICAL: A bond is a DEBT instrument, not equity. If the same
#                  company has both equity and a bond, create SEPARATE intents —
#                  one type=stock, one type=bond. Resolved via bond_master using bond_code.
#                  Examples: "HDFC NCD coupon rate", "SBI bond price",
#                  "Tata Capital debenture details", "REC bond maturity date",
#                  "NHAI bond rating", "Power Finance bond cash flows",
#                  "Muthoot Finance NCD interest schedule"

#   exchange     → EXCHANGE-LEVEL aggregate data with no specific entity. Use when the
#                  user asks about overall market advance-decline ratio, market breadth,
#                  exchange trading holidays, market open/close schedule — for NSE or BSE
#                  as a whole. No DB lookup needed.
#                  ⚠ CRITICAL: This is about the exchange infrastructure itself, not
#                  individual stocks or sectors.
#                  Examples: "NSE advance decline ratio today", "BSE market holidays 2025",
#                  "how many stocks advanced on NSE", "market breadth today",
#                  "NSE trading holidays", "BSE upcoming holidays"

#   nfo          → New Fund Offers — mutual fund schemes that are currently open for
#                  subscription for the FIRST TIME. Use when the user asks about NFOs,
#                  new MF launches, upcoming fund offers, currently open NFOs.
#                  No DB lookup needed.
#                  ⚠ CRITICAL: NFO is different from IPO (IPO = company equity listing;
#                  NFO = new mutual fund scheme launch). Also different from open IPOs.
#                  Examples: "new fund offers this month", "currently open NFOs",
#                  "upcoming NFOs", "which mutual funds are launching",
#                  "new SBI fund offer", "NFO list today"

#   news         → Corporate or mutual fund NEWS and activity feeds. Use when the user
#                  asks for recent news about a company or the MF industry, press
#                  releases, corporate actions feed, MF industry news, MF activities.
#                  No DB lookup needed — news is fetched by category/keyword.
#                  ⚠ CRITICAL: Distinguish from announcements (which are formal
#                  regulatory filings on BSE/NSE). News is editorial/press content.
#                  Examples: "latest news on Reliance", "corporate news today",
#                  "MF industry news", "market news", "recent developments TCS",
#                  "mutual fund activity updates", "HDFC news today"

#   announcement → Formal regulatory ANNOUNCEMENTS filed on BSE or NSE. Use when the
#                  user asks about corporate announcements, exchange filings, regulatory
#                  disclosures, board meeting notices, AGM notices, results announcements,
#                  dividend declarations, merger/acquisition filings on BSE/NSE.
#                  No DB lookup needed.
#                  ⚠ CRITICAL: These are official exchange filings, not news articles.
#                  Examples: "BSE announcements today", "NSE corporate filings",
#                  "recent board meeting announcements", "dividend announcements BSE",
#                  "merger announcements NSE", "quarterly result announcements",
#                  "AGM notice filings"

#   market_info  → Broad market INFORMATION and analytics that don't belong to a single
#                  entity. Use for: fund manager lists, AMFI master data, fund performance
#                  rankings (category-wise), category-level MF performance, SIP transaction
#                  dates, full index list, result declaration calendars (who declares results
#                  today/this week), annual result declarations, macro-economic data
#                  (GDP, CPI, repo rate, inflation), debt market watch (overall bond market
#                  overview), top value traded bonds, top volume traded bonds,
#                  forthcoming bond IPOs (new bond/NCD issuances coming to market).
#                  No DB lookup needed.
#                  ⚠ CRITICAL: Use this when the query is about market-wide data, not
#                  a single named entity. If a specific company's results are asked,
#                  use stock. If a specific bond is asked, use bond.
#                  Examples: "macro economic indicators India", "repo rate today",
#                  "CPI inflation data", "top fund managers", "AMFI master data",
#                  "best performing MF categories", "SIP dates this month",
#                  "debt market overview", "top bonds by volume today",
#                  "forthcoming bond IPOs", "which companies declare results today",
#                  "result calendar this week", "all index list NSE"

#   general      → ONLY for purely educational or definitional questions that CANNOT
#                  be answered by any live market data tool. Use sparingly — only when
#                  the question is about concepts, definitions, explanations, or
#                  calculations that require no live data at all.
#                  ⚠ CRITICAL: Do NOT use general for anything that has live data
#                  available. "What is the PE ratio of TCS?" → stock (live data exists).
#                  "What is a PE ratio?" → general (definition only, no live data needed).
#                  "What is NAV?" → general. "What is the NAV of HDFC Top 100?" → mf_scheme.
#                  "What is a bond?" → general. "What is the coupon of REC bond?" → bond.
#                  Examples of TRUE general: "explain what SIP means",
#                  "what is the difference between NAV and share price",
#                  "how is expense ratio calculated", "what does EBITDA stand for",
#                  "explain dividend yield", "what is a debenture",
#                  "how does an NFO work", "what is market capitalisation"

# ────────────────────────────────────────────────────────────
# DECOMPOSITION RULES (CRITICAL):
# 1. THE 1:1 MAPPING: len(intents) MUST equal the number of subjects identified in Step 1.
# 2. NO BUNDLING: Two different subjects → two separate intent objects.
# 3. DATA POINT MERGING: Multiple metrics on the SAME entity → ONE intent.
# 4. MIXED QUERIES FULLY SUPPORTED: "Reliance price and bond market watch" →
#    intent[0] type=stock, intent[1] type=market_info.
#    "HDFC bond coupon rate and TCS share price" →
#    intent[0] type=bond, intent[1] type=stock.
#    Each intent is handled independently.
# 5. BOND vs STOCK: If a query mentions both a company's equity and its bond/NCD,
#    create SEPARATE intents — one type=stock, one type=bond.
# 6. IPO_STOCK vs IPO_LIST: This is the most important IPO split.
#    - Named company + IPO context → ipo_stock (co_code will be resolved from DB)
#    - No company name / market-wide IPO query → ipo_list (no DB lookup)
#    "Amir Chand IPO details" → ipo_stock (company named)
#    "What are the upcoming IPOs?" → ipo_list (no company named)
#    "Reliance IPO prospectus" → ipo_stock (company named)
#    "Best IPO performers this month" → ipo_list (no company named)
# 7. GENERAL LAST RESORT: Only assign general when you are certain no live data
#    tool can answer. When in doubt between general and any other type, prefer
#    the other type — live data is almost always available.

# MCP_NEEDED LOGIC:
# - Set to true if ANY intent requires live market data (prices, NAVs, ratios,
#   lists, news, bond prices, coupon data, etc.).
# - Set to false ONLY for pure greetings or purely educational/definitional
#   questions where every single intent is type=general.
# ────────────────────────────────────────────────────────────
# """


# def node_classify_extract_and_decompose(state: AgentState) -> AgentState:
#     console.print("[Node AIQ] Classify + extract + decompose (v11.4)")

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
#                 code_missing       = False,
#             ))

#         if not intents and qt not in ("greeting",):
#             console.print("  ⚠ LLM returned no intents — fallback single general intent")
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
#                 code_missing       = False,
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
# # Node TVS: Per-intent tool vector search  (v11.4)
# # ══════════════════════════════════════════════════════════════════════════════

# # Allowed tool families per entity_type.
# # Only tools whose _TOOL_FAMILY_HINTS family is in this set will be kept
# # after the vector-search step.  An empty set means "no family filter —
# # keep all results" (used for no-DB types where any tool might apply).
# _ALLOWED_FAMILIES: dict[str, frozenset[str]] = {
#     "stock":        frozenset({"stock"}),
#     "ipo_stock":    frozenset({"ipo_stock"}),          # ← strict: only ipo_stock tools
#     "ipo_list":     frozenset({"ipo_list"}),           # ← strict: only ipo_list tools
#     "mf_scheme":    frozenset({"mf_scheme"}),
#     "mf_amc":       frozenset({"mf_amc"}),
#     "etf":          frozenset({"etf"}),
#     "index":        frozenset({"index"}),
#     "market":       frozenset({"market"}),
#     "bond":         frozenset({"bond"}),
#     # No-DB types: no family filter needed — any tool that matches is fine
#     "exchange":     frozenset(),
#     "nfo":          frozenset(),
#     "news":         frozenset(),
#     "announcement": frozenset(),
#     "market_info":  frozenset(),
#     "general":      frozenset(),
# }


# def node_per_intent_tool_search(state: AgentState) -> AgentState:
#     console.print("[Node TVS] Per-intent tool vector search (v11.4)")

#     intents = state.get("intents") or []
#     if not intents:
#         console.print("  ⚠ No intents to search for")
#         return state

#     def _search_for_intent(intent: IntentItem) -> IntentItem:
#         et = intent.get("entity_type", "general")

#         # Purely educational — skip tool search entirely
#         if et == "general":
#             console.print(
#                 f"  [{intent['entity']}] entity_type=general → skip tool search"
#             )
#             return IntentItem(
#                 entity             = intent.get("entity", ""),
#                 entity_type        = "general",
#                 intent_description = intent.get("intent_description", ""),
#                 scheme_name        = intent.get("scheme_name"),
#                 amc_name           = intent.get("amc_name"),
#                 nse_symbol         = intent.get("nse_symbol"),
#                 tool_hint          = "",
#                 matched_tools      = [],
#                 resolved_codes     = {},
#                 mcp_result         = "",
#                 code_missing       = False,
#             )

#         # Build search query
#         if et in _NO_DB_MCP_TYPES:
#             # No-DB types: search on intent description alone
#             search_query = intent["intent_description"]
#         else:
#             # DB-backed types: prefix with entity type to improve relevance
#             search_query = f"{et} {intent['intent_description']}"

#         tools = _query_tool_registry(search_query, n_results=10)

#         # ── Strict family filter ──────────────────────────────────────────────
#         allowed = _ALLOWED_FAMILIES.get(et, frozenset())
#         if allowed:
#             filtered = [
#                 t for t in tools
#                 if _infer_entity_type_from_tool(t["tool_name"]) in allowed
#             ]
#             if filtered:
#                 tools = filtered
#                 console.print(
#                     f"  [TVS] family filter '{et}' → kept {len(tools)} tools"
#                 )
#             else:
#                 # No tools survived the strict filter — keep original set but warn
#                 console.print(
#                     f"  [TVS] ⚠ family filter '{et}' eliminated all tools "
#                     f"— keeping unfiltered results (may be imprecise)"
#                 )

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
#             code_missing       = False,
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
#                 original     = intents[idx]
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
#                     code_missing       = False,
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
# investment recommendations, live NAV, MF returns, IPO listings,
# bond prices, NCD details, etc.).

# User: {query}
# """

# def node_greeting_handler(state: AgentState) -> AgentState:
#     console.print("[Node B] Greeting handler")
#     state["final_answer"] = _llm_text(GREETING_PROMPT.format(query=state["user_query"]))
#     return state


# # ── General handler (vector search fallback) ──────────────────────────────────

# def node_general_handler(state: AgentState) -> AgentState:
#     """
#     Handles purely educational/definitional questions that have no matching
#     MCP tool. Uses vector search over knowledge base.
#     Only reached when ALL intents are type=general OR no tools matched at all.
#     """
#     console.print("[Node C] General handler (vector search fallback)")
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
#             cur.execute(
#                 "SELECT mf_schcode, mf_cocode, sch_name, category FROM scheme_master"
#             )
#         elif table == "bond_master":
#             cur.execute(
#                 "SELECT code, companyname, isin, nsesymbol FROM bond_master"
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
#             if table == "bond_master":
#                 isin_score = fuzz.ratio(
#                     entity.upper(), (row.get("isin") or "").upper()
#                 )
#                 sym_score = fuzz.ratio(
#                     entity.upper(), (row.get("nsesymbol") or "").upper()
#                 )
#                 score = max(score, isin_score, sym_score)

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
#     Returns DB params to resolve for this intent.
#     Returns [] for no-DB types (general, exchange, ipo_list, nfo, news, etc.).
#     """
#     params = ENTITY_TYPE_PARAM_MAP.get(entity_type)
#     if params is not None:
#         if params:
#             console.print(
#                 f"  📋 entity_type='{entity_type}' → authoritative params: {params}"
#             )
#         else:
#             console.print(
#                 f"  📋 entity_type='{entity_type}' → no DB params needed"
#             )
#         return params

#     # Fallback: infer from tool metadata
#     if tool_hint:
#         for t in matched_tools:
#             if t["tool_name"] == tool_hint:
#                 db_params = [
#                     p for p in t["required_parameters"] if p in PARAM_TO_TABLE_MAP
#                 ]
#                 if db_params:
#                     return db_params
#                 break

#         meta = _get_tool_meta(tool_hint)
#         if meta:
#             db_params = [
#                 p for p in meta["required_parameters"] if p in PARAM_TO_TABLE_MAP
#             ]
#             if db_params:
#                 return db_params

#     console.print(f"  📋 No DB params resolved for entity_type='{entity_type}'")
#     return []


# # ══════════════════════════════════════════════════════════════════════════════
# # Per-entity DB code resolver  (v11.4)
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
#     needs_co_code   = "co_code"     in required_params   # stock AND ipo_stock
#     needs_isin      = "isin"        in required_params
#     needs_indexcode = "index_code"  in required_params
#     needs_group     = "group"       in required_params
#     needs_bond_code = "bond_code"   in required_params

#     # ── MF scheme ─────────────────────────────────────────────────────────────
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

#     # ── MF AMC ────────────────────────────────────────────────────────────────
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

#     # ── Equity / company  AND  company-specific IPO (ipo_stock) ───────────────
#     # Both entity_type='stock' and entity_type='ipo_stock' resolve co_code the
#     # same way.  The tool family filter in TVS guarantees that ipo_stock intents
#     # only get ipo_stock tools, so there is no cross-contamination at the MCP
#     # call stage even though the DB resolution path is identical.
#     if needs_co_code:
#         stock = _resolve_single(name, nse_symbol)
#         if stock:
#             codes["co_code"] = stock["co_code"]
#             result.update({
#                 "co_code":      stock["co_code"],
#                 "company_info": stock["company_info"],
#                 "nse_symbol":   stock["nse_symbol"],
#             })
#         else:
#             # Flag that we could not find a co_code — prevents MCP hallucination
#             result["code_missing"] = True
#             console.print(
#                 f"  ⚠ co_code not found for '{name}' — "
#                 f"MCP call will be skipped for this intent"
#             )

#     # ── ETF ───────────────────────────────────────────────────────────────────
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

#     # ── Index ─────────────────────────────────────────────────────────────────
#     if needs_indexcode:
#         cfg   = PARAM_TO_TABLE_MAP["index_code"]
#         match = _targeted_db_lookup(
#             entity=name, table=cfg["table"],
#             name_col=cfg["column"], id_col=cfg["id_field"],
#         )
#         if match:
#             codes["index_code"] = int(match["indexcode"])
#             result["resolved_index_name"] = match.get("group_name", name)

#     # ── Market group ──────────────────────────────────────────────────────────
#     if needs_group:
#         cfg   = PARAM_TO_TABLE_MAP["group"]
#         match = _targeted_db_lookup(
#             entity=name, table=cfg["table"],
#             name_col=cfg["column"], id_col=cfg["id_field"],
#         )
#         if match:
#             codes["group"] = match["group_name"]
#             result["resolved_group_name"] = match["group_name"]
#         else:
#             console.print(
#                 f"  ⚠ group not found in group_master for '{name}' — using raw name"
#             )
#             codes["group"] = name
#             result["resolved_group_name"] = name

#     # ── Bond ──────────────────────────────────────────────────────────────────
#     if needs_bond_code:
#         cfg   = PARAM_TO_TABLE_MAP["bond_code"]
#         match = _targeted_db_lookup(
#             entity=name, table=cfg["table"],
#             name_col=cfg["column"], id_col=cfg["id_field"],
#         )
#         if match:
#             codes["bond_code"] = int(match["code"])
#             result["resolved_bond_name"] = match.get("companyname", name)
#         else:
#             result["code_missing"] = True
#             console.print(f"  ⚠ bond_code not found in bond_master for '{name}'")

#     result["mcp_resolved_codes"] = codes
#     return result


# # ── Resolve codes for all intents ─────────────────────────────────────────────

# def _resolve_intents_codes(intents: list[IntentItem]) -> list[IntentItem]:
#     def _resolve_one(intent: IntentItem) -> IntentItem:
#         et        = intent.get("entity_type", "general")
#         tool_hint = intent.get("tool_hint", "")
#         matched   = intent.get("matched_tools") or []

#         # If entity_type is still 'general' but we got a tool, try to infer type
#         if et == "general" and tool_hint:
#             inferred = _infer_entity_type_from_tool(tool_hint)
#             if inferred != "general":
#                 et = inferred
#                 intent["entity_type"] = et

#         req_params = _get_required_params_for_entity(tool_hint, et, matched)

#         # Skip DB entirely for no-param types
#         if not req_params:
#             console.print(
#                 f"  💎 [{intent['entity']}] entity_type='{et}' "
#                 f"→ no code resolution needed"
#             )
#             intent["resolved_codes"] = {}
#             intent["code_missing"]   = False
#             return intent

#         resolved = _resolve_entity_codes(
#             name            = intent.get("entity", ""),
#             scheme_name     = intent.get("scheme_name"),
#             amc_name        = intent.get("amc_name"),
#             nse_symbol      = intent.get("nse_symbol"),
#             required_params = req_params,
#         )
#         intent["resolved_codes"] = resolved.get("mcp_resolved_codes", {})
#         intent["code_missing"]   = bool(resolved.get("code_missing", False))

#         console.print(
#             f"  💎 [{intent['entity']}] "
#             f"resolved_codes={intent['resolved_codes']} "
#             f"code_missing={intent['code_missing']}"
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
#                 console.print(
#                     f"  [red]Code resolution failed for intent #{idx}: {exc}[/red]"
#                 )
#                 result_intents[idx] = intents[idx]

#     return [result_intents[i] for i in sorted(result_intents)]


# # ══════════════════════════════════════════════════════════════════════════════
# # MCP Pre-Resolution nodes
# # ══════════════════════════════════════════════════════════════════════════════

# def node_mcp_pre_resolve(state: AgentState) -> AgentState:
#     console.print("[Node MPR] Single-entity MCP pre-resolve (v11.4)")

#     intents = state.get("intents") or []
#     if not intents:
#         console.print("  ⚠ No intents; skipping resolve")
#         state["mcp_resolved_codes"] = {}
#         return state

#     resolved_intents            = _resolve_intents_codes(intents)
#     state["intents"]            = resolved_intents
#     state["mcp_resolved_codes"] = (
#         resolved_intents[0].get("resolved_codes", {}) if resolved_intents else {}
#     )

#     console.print(f"  🏁 Primary resolved codes: {state['mcp_resolved_codes']}")
#     return state


# def node_mcp_multi_pre_resolve(state: AgentState) -> AgentState:
#     console.print("[Node MMPR] Multi-entity MCP pre-resolve (v11.4)")

#     intents = state.get("intents") or []
#     if not intents:
#         console.print("  ⚠ No intents; skipping resolve")
#         return state

#     resolved_intents            = _resolve_intents_codes(intents)
#     state["intents"]            = resolved_intents
#     state["mcp_resolved_codes"] = (
#         resolved_intents[0].get("resolved_codes", {}) if resolved_intents else {}
#     )

#     console.print(
#         f"  🏁 Resolved {len(resolved_intents)} intents. "
#         f"Primary codes: {state['mcp_resolved_codes']}"
#     )
#     return state


# # ══════════════════════════════════════════════════════════════════════════════
# # Node MTCI: MCP Tool Call — sequential per-intent in one session (v11.4)
# # ══════════════════════════════════════════════════════════════════════════════

# def _run_mcp_multi_in_thread(intents: list[dict]) -> list[dict]:
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
#                 loop.run_until_complete(
#                     asyncio.gather(*pending, return_exceptions=True)
#                 )
#         except Exception:
#             pass
#         loop.close()


# def node_mcp_tool_call_intents(state: AgentState) -> AgentState:
#     console.print("[Node MTCI] Sequential per-intent MCP calls — one session (v11.4)")

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

#     # Partition intents:
#     #   mcp_intents     → have a tool hint AND resolved codes (or no codes needed)
#     #   blocked_intents → need codes but resolution failed (code_missing=True)
#     #   general_intents → no tool, entity_type=general
#     mcp_intents: list[IntentItem]     = []
#     blocked_intents: list[IntentItem] = []
#     general_intents: list[IntentItem] = []

#     for i in intents:
#         et           = i.get("entity_type", "general")
#         has_tool     = bool(i.get("tool_hint"))
#         code_missing = bool(i.get("code_missing", False))

#         if et == "general" and not has_tool:
#             general_intents.append(i)
#         elif code_missing:
#             # Required DB code could not be resolved — block this intent to
#             # prevent MCP client from hallucinating a code value.
#             blocked_intents.append(i)
#             console.print(
#                 f"  🚫 Blocked intent [{i.get('entity')}] — "
#                 f"required code not found in DB (entity_type={et})"
#             )
#         else:
#             mcp_intents.append(i)

#     if general_intents:
#         console.print(
#             f"  ℹ {len(general_intents)} general intent(s) answered via synthesis only"
#         )
#     if blocked_intents:
#         console.print(
#             f"  ⚠ {len(blocked_intents)} blocked intent(s) — company not found in DB"
#         )

#     # Fallback: no MCP intents at all
#     if not mcp_intents:
#         console.print("  ⚠ No MCP intents; falling back to raw user query")
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

#     try:
#         with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
#             future         = ex.submit(_run_mcp_multi_in_thread, mcp_intents)
#             filled_intents = future.result(timeout=600)
#     except concurrent.futures.TimeoutError:
#         console.print("  [red]Multi-intent MCP timed out[/red]")
#         filled_intents = [
#             {**dict(i), "mcp_result": "MCP call timed out."}
#             for i in mcp_intents
#         ]
#     except Exception as exc:
#         console.print(f"  [red]Multi-intent MCP failed: {exc}[/red]")
#         filled_intents = [
#             {**dict(i), "mcp_result": f"MCP call failed: {exc}"}
#             for i in mcp_intents
#         ]

#     # Assemble all result sets
#     all_intents = (
#         filled_intents
#         + [
#             {**dict(i), "mcp_result": "(answered from knowledge base)"}
#             for i in general_intents
#         ]
#         + [
#             {
#                 **dict(i),
#                 "mcp_result": (
#                     f"Sorry, I could not find '{i.get('entity')}' in our database. "
#                     f"Please check the company name and try again."
#                 ),
#             }
#             for i in blocked_intents
#         ]
#     )
#     state["intents"] = all_intents

#     state["intent_results"] = [
#         {
#             "entity":             i.get("entity", ""),
#             "entity_type":        i.get("entity_type", ""),
#             "intent_description": i.get("intent_description", ""),
#             "mcp_result":         i.get("mcp_result", ""),
#         }
#         for i in all_intents
#     ]

#     state["mcp_raw_result"] = "\n\n---\n\n".join(
#         f"[{r['entity']} / {r['intent_description']}]\n{r['mcp_result']}"
#         for r in state["intent_results"]
#         if r.get("mcp_result")
#     )
#     state["mcp_tool_calls_made"] = []
#     state["error"]               = None

#     total_chars = len(state["mcp_raw_result"])
#     console.print(
#         f"  ✅ {len(filled_intents)} MCP intent(s) resolved "
#         f"({len(general_intents)} general, {len(blocked_intents)} blocked). "
#         f"Total chars for synthesis: {total_chars}"
#     )
#     return state


# # ══════════════════════════════════════════════════════════════════════════════
# # MCP Synthesis (v11.4)
# # ══════════════════════════════════════════════════════════════════════════════

# MCP_SYNTHESIS_PROMPT = """\
# You are a knowledgeable Indian equity, mutual fund, and fixed-income analyst assistant.

# ## Conversation history (last 4 turns)
# {history}

# ## User Question
# {user_query}

# ## Live Data Retrieved (one block per sub-question)
# {mcp_result}

# ## Instructions
# 1. Answer EACH sub-question using the data block labelled for it above.
#    If there are multiple data blocks, address each one in turn.
# 2. Sub-questions marked "(answered from knowledge base)" should be answered
#    from your own knowledge directly in the response.
# 3. Sub-questions where the company was not found should politely inform the
#    user that no record was found and suggest verifying the company name.
# 4. Write in plain prose paragraphs only. No markdown, no bullet points,
#    no asterisks, no tables. Do NOT use the rupee symbol — write "Rs".
# 5. Present key numbers naturally woven into sentences.
# 6. If a specific data field is missing or the tool returned an error, say so briefly.
# 7. Use Indian number formatting (lakh, crore) for large figures.
# 8. For bond/debt data include coupon rate, maturity date, credit rating, and
#    face value where available.
# 9. Keep under 300 words unless the user explicitly asked for detail.
# 10. End with: This is not financial advice.
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
#     console.print("[Node MS] MCP synthesis (v11.4)")
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
#         state["final_answer"] = mcp_result
#     return state


# # ── General synthesis (vector search) ────────────────────────────────────────

# GENERAL_SYNTHESIS_PROMPT = """\
# You are a knowledgeable Indian equity and fixed-income analyst assistant.

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
#     console.print("[Node S] General synthesis (vector search)")
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
# # Router (v11.4)
# # ══════════════════════════════════════════════════════════════════════════════

# def route_after_classify(state: AgentState) -> str:
#     qt      = state.get("query_type", "general")
#     intents = state.get("intents") or []

#     if qt == "greeting":
#         return "greeting"

#     # An intent is "MCP-capable" if it has a tool hint OR its entity_type
#     # is not 'general' (meaning it should go to MCP even if tool hint
#     # is temporarily empty — pre-resolve will handle it).
#     mcp_intents = [
#         i for i in intents
#         if i.get("tool_hint") or i.get("entity_type") not in ("general",)
#     ]

#     for idx, intent in enumerate(intents):
#         console.print(
#             f"  [Router] intent[{idx}] entity={intent.get('entity')} "
#             f"type={intent.get('entity_type')} "
#             f"tool_hint={intent.get('tool_hint') or '(none)'}"
#         )

#     # All intents are pure general (no tools, all type=general) → vector search
#     if not mcp_intents or (
#         not any(i.get("tool_hint") for i in intents)
#         and all(i.get("entity_type") == "general" for i in intents)
#     ):
#         console.print("  → general (all intents are educational/no tools)")
#         return "general"

#     has_many         = len(intents) > 1
#     additional       = state.get("companies") or []
#     primary_type     = state.get("primary_entity_type", "general")
#     secondary_types  = {c.get("entity_type", "general") for c in additional}
#     is_heterogeneous = bool(secondary_types - {"general"} - {primary_type})

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
# # Graph (v11.4)
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



# ### smart compare score

# from __future__ import annotations

# import asyncio
# import concurrent.futures
# import json
# import logging
# import re
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

# # ── Types ─────────────────────────────────────────────────────────────────────

# class Turn(TypedDict):
#     role: str       # "user" | "assistant"
#     content: str


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
#     code_missing:       bool   # flag set when DB lookup failed for required code


# # All entity types that require NO DB code resolution but still go to MCP
# _NO_DB_MCP_TYPES = frozenset({
#     "exchange",
#     "ipo_list",
#     "nfo",
#     "news",
#     "announcement",
#     "market_info",
# })

# # ── Tool Family Mapping Matrix ───────────────────────────────────────────────
# _TOOL_FAMILY_HINTS: dict[str, str] = {

#     # ── Stock: requires co_code ───────────────────────────────────────────────
#     "get_company_stock_price": "stock",
#     "get_delayed_stock_price": "stock",
#     "get_nse_company_announcements":        "stock",
#     "get_bse_company_announcements":        "stock",
#     "get_company_result_schedule":          "stock",
#     "get_company_profile":                  "stock",
#     "get_company_background":               "stock",
#     "get_board_of_directors":                 "stock",
#     "get_company_bankers":                  "stock",
#     "get_management_biodata":                       "stock",
#     "get_subsidiaries_jvs":                     "stock",
#     "get_related_party_transactions":                    "stock",
#     "get_employee_count":                   "stock",
#     "get_capital_structure":                "stock",
#     "get_pledge_share_details":                     "stock",
#     "get_substantial_acquisitions":                      "stock",
#     "get_segment_data":                     "stock",
#     "get_r_and_d_expenditure":                          "stock",
#     "get_finished_products":                "stock",
#     "get_raw_materials":                    "stock",
#     "get_chronological_history":                    "stock",
#     "get_company_history":                  "stock",
#     "get_funds_holding_company":            "stock",
#     # Financials
#     "get_quarterly_results":                "stock",
#     "get_profit_loss":                      "stock",
#     "get_balance_sheet":                    "stock",
#     "get_cash_flow":                        "stock",
#     "get_half_yearly_results":              "stock",
#     "get_nine_months_results":              "stock",
#     "get_quarterly_trends" :                 "stock",
#     "get_shareholding_pattern":              "stock",
#     "get_major_shareholders":                "stock",
#     "get_quarterly_balance_sheet" :         "stock",
#     "get_yearly_results":                   "stock",
#     "get_annual_balance_sheet":              "stock",
#     "get_half_yearly_balance_sheet":         "stock",
#     "get_ttm_growth_trends":                 "stock",
#     "get_quarterly_revenue_trends":          "stock",
#     "get_quarterly_ebitda_trends":           "stock",
#     "get_quarterly_ebit_trends":             "stock",
#     "get_growth_data_quarterly":             "stock",
#     "get_growth_data_yearly":                "stock",
#     # Ratios
#     "get_key_financial_ratios":             "stock",
#     "get_daily_ratios":                     "stock",
#     "get_margin_ratios":                    "stock",
#     "get_valuation_ratios":                 "stock",
#     "get_all_basic_ratios":                 "stock",
#     "get_return_ratios":                    "stock",
#     "get_growth_ratios":                    "stock",
#     "get_performance_ratios":                "stock",
#     "get_efficiency_ratios":                 "stock",
#     "get_cashflow_ratios":                  "stock",
#     "get_liquidity_ratios":                 "stock",
#     "get_solvency_ratios":                  "stock",
#     "get_quarterly_ratios":                 "stock",
#     "get_yearly_ratios":                    "stock",
#     "get_shareholding":                     "stock",
#     "get_major_sharehold":                  "stock",
#     "get_financial_stability_ratios":       "stock",

#     # ── Company-specific IPO tools: require co_code — family = ipo_stock ─────
#     "get_anchor_investor":                  "ipo_stock",
#     "get_ipo_details":                      "ipo_stock",
#     "get_ipo_subscription_status":          "ipo_stock",
#     "get_ipo_synopsis":                     "ipo_stock",
#     "get_ipo_timeline":                     "ipo_stock",
#     "get_ipo_promoter_details":             "ipo_stock",
#     "get_ipo_listing_info":                 "ipo_stock",
#     "get_ipo_objects_of_issue":             "ipo_stock",
#     "get_ipo_anchor_investor_details":      "ipo_stock",
#     "get_ipo_industry_peers":                "ipo_stock",
#     "get_ipo_financials":                   "ipo_stock",
#     "get_ipo_product_services":             "ipo_stock",
#     "get_ipo_strength_details":             "ipo_stock",
#     "get_ipo_strategy_details":             "ipo_stock",
#     "get_ipo_risk_details":                 "ipo_stock",
#     "get_ipo_selling_shareholders":         "ipo_stock",
#     "get_ipo_allocation_details":           "ipo_stock",
#     "get_ipo_prospectus":                   "ipo_stock",
#     "get_ipo_lead_managers":                "ipo_stock",
#     "get_ipo_registrar":                    "ipo_stock",
#     "get_ipo_customer_details":              "ipo_stock",

#     # ── Index: requires index_code ────────────────────────────────────────────
#     "get_market_indices":                   "stock",   # no specific param but stock-adjacent
#     "get_index_companies":                  "index",

#     # ── Market: requires group (resolved from group_master) ───────────────────
#     "get_active_performer":                 "market",
#     "get_top_gainers":                      "market",
#     "get_top_losers":                       "market",
#     "get_out_under_performers":                   "market",
#     "get_52week_highs":                           "market",
#     "get_52week_lows":                      "market",
#     "get_new_highs_lows":                        "market",
#     "get_sector_companies":                 "market",

#     # ── Exchange: requires ex only ────────────────────────────────────────────
#     "get_advance_decline":                  "exchange",
#     "get_exchange_holidays":                "exchange",
    

#     # ── MF Scheme: requires mf_schcode ────────────────────────────────────────
#     "get_scheme_nav":                       "mf_scheme",
#     "get_investment_details":                "mf_scheme",
#     "get_expense_ratio":                    "mf_scheme",
#     "get_avg_maturity":                     "mf_scheme",
#     "get_scheme_aum":                       "mf_scheme",
#     "get_nav_historical":                   "mf_scheme",
#     "get_scheme_returns":                   "mf_scheme",
#     "get_lumpsum_returns":                  "mf_scheme",
#     "get_scheme_sip":                       "mf_scheme",
#     "get_mf_holdings":                      "mf_scheme",
#     "get_sector_allocation":                "mf_scheme",
#     "get_asset_allocation":                 "mf_scheme",
#     "get_portfolio_changes":                "mf_scheme",
#     "get_mcap_allocation":                  "mf_scheme",
#     "get_most_bought_sold":                  "mf_scheme",
#     "get_scheme_ratios":                    "mf_scheme",
#     "get_dividend_details":                 "mf_scheme",
#     "get_bse_star_scheme":                  "mf_scheme",
#     "compare_schemes":                      "mf_scheme",
#     "get_whats_in_out":                     "mf_scheme",
#     "get_scheme_sip_rules":                 "mf_scheme",
#     "get_scheme_sip_details":               "mf_scheme",

#     # ── MF AMC: requires mf_cocode ────────────────────────────────────────────
#     "get_fund_categories":                  "mf_amc",
#     "get_schemes_by_amc":                   "mf_amc",
#     "get_fund_profile":                     "mf_amc",
#     "get_fund_managers":                     "mf_amc",

#     # ── ETF: requires isin ────────────────────────────────────────────────────
#     "get_etf_quotes":                       "etf",
#     "get_etf_returns":                      "etf",
#     "get_etf_fundamentals":                 "etf",
#     "get_etf_about":                        "etf",
#     "get_etf_equity_holdings":              "etf",
#     "get_etf_monthly_portfolio":               "etf",
#     "get_etf_sector_allocatio":             "etf",
#     "get_etf_asset_allocation":             "etf",
#     "get_etf_":                             "etf",

#     # ── Bond: requires bond_code ──────────────────────────────────────────────
#     "get_debt_eod_prices_scripwise":        "bond",
#     "get_bond_details":                     "bond",
#     "get_bond_price_history":               "bond",
#     "get_bond_cashflow":                    "bond",
#     "get_bond_rating":                      "bond",
#     "get_bond_redemption":                  "bond",
#     "get_bond_interest":                    "bond",
#     "get_bond_":                            "bond",

#     # ── IPO list: NO entity code needed ──────────────────────────────────────
#     "get_forthcoming_ipos":                  "ipo_list",
#     "get_open_ipos":                        "ipo_list",
#     "get_closed_ipos":                      "ipo_list",
#     "get_new_ipo_listings":                          "ipo_list",
#     "get_best_ipo_performers":                         "ipo_list",
#     "get_ipo_master":                       "ipo_list",
#     "get_ipo_logo":                         "ipo_list",
#     "get_forthcoming_drh_filings":           "ipo_list",
#     "get_basis_of_allotment" :               "ipo_list",

#     # bond _ ipo
#     "get_open_bond_ipo":                      "ipo_list",
#     "get_forthcoming_bond_ipo" :               "ipo_list",

#     # ── NFO: no entity code needed ────────────────────────────────────────────
#     "get_new_fund_offer":                   "nfo",

#     # ── News: no entity code needed ───────────────────────────────────────────
#     "get_corporate_news":                   "news",
#     "get_mf_news":                          "news",
#     "get_mf_market_activity":                     "news",

#     # ── Announcement: no entity code needed ───────────────────────────────────
#     "get_bse_announcements":                 "announcement",
#     "get_nse_announcements":                 "announcement",

#     # ── Market Info: no entity code needed ────────────────────────────────────
#     "get_fund_house":                       "market_info",
#     "get_amfi_master":                      "market_info",
#     "get_fund_manager":                     "market_info",
#     "get_index_list":                       "market_info",
#     "get_results_today":                    "market_info",
#     "get_result_declarations":              "market_info",
#     "get_annual_declarations":              "market_info",
#     "get_fund_performance":                 "market_info",
#     "get_category_performance":             "market_info",
#     "get_sip_dates":                        "market_info",
#     "get_macro_economic_data":              "market_info",
#     "get_forthcoming_bond_ipo":             "market_info",
#     "get_open_bond_ipo":                    "market_info",
#     "get_debt_top_value":                   "market_info",
#     "get_debt_top_volume":                  "market_info",
#     "et_new_fund_offers" :                  "market_info",
#     "get_debt_market_watch":                "market_info",
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
#     "co_code": {
#         "table":    "companies",
#         "column":   "companyname",
#         "id_field": "co_code",
#     },
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
#     "group": {
#         "table":    "group_master",
#         "column":   "group_name",
#         "id_field": "group_name",
#     },
#     "bond_code": {
#         "table":    "bond_master",
#         "column":   "companyname",
#         "id_field": "code",
#     },
# }

# ENTITY_TYPE_PARAM_MAP: dict[str, list[str]] = {
#     "stock":        ["co_code"],
#     "ipo_stock":    ["co_code"],    # company-specific IPO; same DB lookup as stock
#     "mf_scheme":    ["mf_schcode"],
#     "mf_amc":       ["mf_cocode"],
#     "etf":          ["isin"],
#     "index":        ["index_code"],
#     "market":       ["group"],
#     "bond":         ["bond_code"],
#     "exchange":     [],
#     "ipo_list":     [],
#     "nfo":          [],
#     "news":         [],
#     "announcement": [],
#     "market_info":  [],
#     "general":      [],
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

#     matched_tools:  list[dict]
#     intents:        list[IntentItem]
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


# # ── LLM Prompt Templates ──────────────────────────────────────────────────────

# CLASSIFY_EXTRACT_DECOMPOSE_PROMPT = """\
# You are a Lead Financial Systems Analyst for an Indian stock-market assistant. Your task is to decompose user queries into executable data intents.

# ## Context
# - History: {history}
# - Today's Date: {today}
# - Query: "{query}"

# ────────────────────────────────────────────────────────────
# STEP 1: SUBJECT IDENTIFICATION
# Identify every distinct subject the user is asking about. A subject is either:
# 1. A Specific Entity: (e.g., Reliance Industries, HDFC AMC, Nifty 50, SBI Small Cap Fund, Gold BeES ETF, HDFC NCD bond).
# 2. A Market Category/Concept: (e.g., "upcoming IPOs", "top gainers", "market news", "bulk deals", "top losers on NSE", "debt market watch").

# STEP 2: JSON GENERATION
# Generate exactly one intent item for every subject identified in Step 1.

# Return ONLY valid JSON:
# {{
#   "query_type": "<greeting|general|stock|mf_scheme|mf_amc|bond|comparison|investment>",
#   "is_broad": <true|false>,
#   "mcp_needed": <true|false>,
#   "intents": [
#     {{
#       "entity": "<entity name OR market concept name>",
#       "entity_type": "<see ENTITY TYPE RULES below>",
#       "intent_description": "<specific data point needed for this subject>",
#       "scheme_name": "<if mf_scheme, else empty>",
#       "amc_name": "<if mf_amc, else empty>",
#       "nse_symbol": "<NSE ticker if known, else empty>"
#     }}
#   ]
# }}

# ────────────────────────────────────────────────────────────
# ENTITY TYPE RULES (use exactly one of these values):

#   stock        → A specific listed company's EQUITY data. Use when the user asks about
#                  share price, stock quote, OHLC, 52-week high/low, delivery volume,
#                  delayed price, company profile, background, board of directors, bankers,
#                  management team, subsidiaries, related parties, employee count, capital
#                  structure, pledged shares, substantial shareholders, segment data, R&D,
#                  finished products, raw materials, chronological events, company history,
#                  quarterly/annual financial results, P&L, balance sheet, cash flow,
#                  half-yearly/nine-month results, TTM growth, revenue/EBITDA/EBIT trends,
#                  key financial ratios, daily ratios, margin ratios, valuation ratios,
#                  return ratios, growth ratios, performance ratios, cashflow ratios,
#                  liquidity ratios, solvency ratios, shareholding pattern, major shareholders.
#                  ⚠ CRITICAL: Use stock ONLY for equity/share data of a company.
#                  If the query is about the same company's IPO — use ipo_stock instead.
#                  If the query is about the same company's bond/NCD — use bond instead.
#                  Examples: "Reliance share price", "TCS quarterly results",
#                  "HDFC Bank balance sheet", "Infosys management team",
#                  "Wipro shareholding pattern", "ITC PE ratio"

#   ipo_stock    → Company-SPECIFIC IPO data where you know the company name.
#                  Use this when the user asks about IPO details, GMP, allotment status,
#                  anchor investors, anchor investor details, subscription status,
#                  IPO synopsis, IPO timeline, IPO promoter details, IPO listing info,
#                  IPO objects of issue, IPO financials, IPO products/services,
#                  IPO strengths, IPO strategy, IPO risk factors, IPO industry peers,
#                  IPO selling shareholders, IPO allocation details, IPO prospectus,
#                  IPO lead managers, IPO registrar, basis of allotment —
#                  all for a NAMED company.
#                  ⚠ CRITICAL: Use ipo_stock (NOT ipo_list) whenever a specific company
#                  name is mentioned alongside IPO-related data. A co_code DB lookup
#                  will be performed — the company must exist in our database.
#                  Examples: "Amir Chand IPO details", "TCS anchor investors",
#                  "HDFC Bank IPO subscription status", "Reliance IPO prospectus",
#                  "Zomato IPO allotment", "Paytm IPO risk factors",
#                  "LIC IPO lead managers", "Adani IPO timeline"

#   ipo_list     → Generic/market-wide IPO queries with NO specific company name.
#                  Use when the user wants a LIST, overview, or status of IPOs across
#                  the market — forthcoming IPOs, upcoming IPOs, open IPOs, closed IPOs,
#                  new listings, recently listed IPOs, best performing IPOs, IPO master
#                  list, IPO logo, IPO calendar.
#                  ⚠ CRITICAL: Use ipo_list (NOT ipo_stock) when no specific company
#                  is named, or when the user explicitly wants a list/overview of IPOs.
#                  No DB lookup is needed — goes straight to MCP.
#                  Examples: "upcoming IPOs", "open IPOs right now",
#                  "which IPOs are closing this week", "recent IPO listings",
#                  "best performing IPOs this month", "IPO master list",
#                  "forthcoming IPOs on NSE", "new listings today"

#   mf_scheme    → A specific named mutual fund SCHEME. Use when the user asks about
#                  NAV (current or historical), investment details, expense ratio,
#                  average maturity, AUM, scheme returns (1Y/3Y/5Y), lumpsum returns,
#                  SIP calculator, MF holdings/portfolio, sector allocation, asset
#                  allocation, portfolio changes, market-cap allocation, most bought
#                  stocks, scheme ratios, dividend history, BSE STAR platform details,
#                  scheme comparison, what's in/out of portfolio — for a NAMED scheme.
#                  ⚠ CRITICAL: The scheme name must be specific enough to look up in
#                  scheme_master. Provide scheme_name field in JSON.
#                  Examples: "Parag Parikh Flexi Cap NAV", "Axis Bluechip returns",
#                  "SBI Small Cap Fund portfolio", "Mirae Asset Large Cap expense ratio",
#                  "HDFC Mid-Cap Opportunities AUM", "compare Axis vs Mirae bluechip"

#   mf_amc       → A mutual fund HOUSE or AMC (Asset Management Company) — NOT a
#                  specific scheme. Use when the user asks about all schemes offered by
#                  a fund house, fund categories under an AMC, AMC profile/overview,
#                  or fund house details.
#                  ⚠ CRITICAL: Use mf_amc for the fund house itself, not its individual
#                  schemes. Provide amc_name field in JSON.
#                  Examples: "SBI Mutual Fund schemes", "Nippon AMC fund categories",
#                  "HDFC AMC profile", "Axis Mutual Fund all funds",
#                  "Franklin Templeton fund house details", "DSP AMC schemes list"

#   etf          → An exchange-traded fund. Use when the user asks about ETF quotes,
#                  ETF returns, ETF fundamentals, ETF profile/about, ETF equity holdings,
#                  ETF monthly portfolio, ETF sector allocation, ETF asset allocation —
#                  for a NAMED ETF. ETFs trade on exchange like stocks but track an index
#                  or commodity.
#                  ⚠ CRITICAL: Distinguish from regular MF schemes — ETFs have an ISIN
#                  and trade on NSE/BSE intraday. Gold ETF, Index ETF, Sectoral ETF.
#                  Examples: "Gold BeES NAV", "Nifty BeES returns", "SBI ETF Nifty 50",
#                  "Nippon India ETF holdings", "HDFC Sensex ETF fundamentals",
#                  "Bharat Bond ETF portfolio"

#   index        → A market INDEX. Use when the user asks about index constituents,
#                  companies within an index, index composition — for a NAMED index.
#                  ⚠ NOTE: For live index price/level use stock type (get_market_indices
#                  is stock-adjacent). Use index type specifically for index membership
#                  queries (which companies are in Nifty 50, Bank Nifty constituents).
#                  Examples: "Nifty 50 constituents", "Bank Nifty companies list",
#                  "Sensex component stocks", "Nifty Midcap 100 members",
#                  "which stocks are in Nifty IT index"

#   market       → Market-WIDE movers or screeners that require a market GROUP or
#                  segment (e.g. NSE, BSE, specific sector). Use when the user wants
#                  ranked lists across the market: top gainers, top losers, most active
#                  stocks, 52-week highs/lows, new highs, outperformers, underperformers,
#                  sector-wise top performers, delayed/active performers.
#                  ⚠ CRITICAL: This is NOT about a single company — it's about a
#                  ranked list across a market segment. A group code is resolved from
#                  group_master (e.g. "NSE", "BSE", "NIFTY50").
#                  Examples: "top gainers on NSE today", "most active stocks BSE",
#                  "52-week high stocks", "top losers Nifty", "new highs today",
#                  "which sectors are outperforming", "best performing sector stocks"

#   bond         → A specific named listed BOND, DEBENTURE, or NCD (Non-Convertible
#                  Debenture). Use when the user asks about bond EOD prices, bond details
#                  (coupon rate, face value, maturity), bond price history, bond cash
#                  flows, bond credit rating, bond redemption schedule, bond interest
#                  payment schedule — for a NAMED bond/NCD/debenture issuer.
#                  ⚠ CRITICAL: A bond is a DEBT instrument, not equity. If the same
#                  company has both equity and a bond, create SEPARATE intents —
#                  one type=stock, one type=bond. Resolved via bond_master using bond_code.
#                  Examples: "HDFC NCD coupon rate", "SBI bond price",
#                  "Tata Capital debenture details", "REC bond maturity date",
#                  "NHAI bond rating", "Power Finance bond cash flows",
#                  "Muthoot Finance NCD interest schedule"

#   exchange     → EXCHANGE-LEVEL aggregate data with no specific entity. Use when the
#                  user asks about overall market advance-decline ratio, market breadth,
#                  exchange trading holidays, market open/close schedule — for NSE or BSE
#                  as a whole. No DB lookup needed.
#                  ⚠ CRITICAL: This is about the exchange infrastructure itself, not
#                  individual stocks or sectors.
#                  Examples: "NSE advance decline ratio today", "BSE market holidays 2025",
#                  "how many stocks advanced on NSE", "market breadth today",
#                  "NSE trading holidays", "BSE upcoming holidays"

#   nfo          → New Fund Offers — mutual fund schemes that are currently open for
#                  subscription for the FIRST TIME. Use when the user asks about NFOs,
#                  new MF launches, upcoming fund offers, currently open NFOs.
#                  No DB lookup needed.
#                  ⚠ CRITICAL: NFO is different from IPO (IPO = company equity listing;
#                  NFO = new mutual fund scheme launch). Also different from open IPOs.
#                  Examples: "new fund offers this month", "currently open NFOs",
#                  "upcoming NFOs", "which mutual funds are launching",
#                  "new SBI fund offer", "NFO list today"

#   news         → Corporate or mutual fund NEWS and activity feeds. Use when the user
#                  asks for recent news about a company or the MF industry, press
#                  releases, corporate actions feed, MF industry news, MF activities.
#                  No DB lookup needed — news is fetched by category/keyword.
#                  ⚠ CRITICAL: Distinguish from announcements (which are formal
#                  regulatory filings on BSE/NSE). News is editorial/press content.
#                  Examples: "latest news on Reliance", "corporate news today",
#                  "MF industry news", "market news", "recent developments TCS",
#                  "mutual fund activity updates", "HDFC news today"

#   announcement → Formal regulatory ANNOUNCEMENTS filed on BSE or NSE. Use when the
#                  user asks about corporate announcements, exchange filings, regulatory
#                  disclosures, board meeting notices, AGM notices, results announcements,
#                  dividend declarations, merger/acquisition filings on BSE/NSE.
#                  No DB lookup needed.
#                  ⚠ CRITICAL: These are official exchange filings, not news articles.
#                  Examples: "BSE announcements today", "NSE corporate filings",
#                  "recent board meeting announcements", "dividend announcements BSE",
#                  "merger announcements NSE", "quarterly result announcements",
#                  "AGM notice filings"

#   market_info  → Broad market INFORMATION and analytics that don't belong to a single
#                  entity. Use for: fund manager lists, AMFI master data, fund performance
#                  rankings (category-wise), category-level MF performance, SIP transaction
#                  dates, full index list, result declaration calendars (who declares results
#                  today/this week), annual result declarations, macro-economic data
#                  (GDP, CPI, repo rate, inflation), debt market watch (overall bond market
#                  overview), top value traded bonds, top volume traded bonds,
#                  forthcoming bond IPOs (new bond/NCD issuances coming to market).
#                  No DB lookup needed.
#                  ⚠ CRITICAL: Use this when the query is about market-wide data, not
#                  a single named entity. If a specific company's results are asked,
#                  use stock. If a specific bond is asked, use bond.
#                  Examples: "macro economic indicators India", "repo rate today",
#                  "CPI inflation data", "top fund managers", "AMFI master data",
#                  "best performing MF categories", "SIP dates this month",
#                  "debt market overview", "top bonds by volume today",
#                  "forthcoming bond IPOs", "which companies declare results today",
#                  "result calendar this week", "all index list NSE"

#   general      → ONLY for purely educational or definitional questions that CANNOT
#                  be answered by any live market data tool. Use sparingly — only when
#                  the question is about concepts, definitions, explanations, or
#                  calculations that require no live data at all.
#                  ⚠ CRITICAL: Do NOT use general for anything that has live data
#                  available. "What is the PE ratio of TCS?" → stock (live data exists).
#                  "What is a PE ratio?" → general (definition only, no live data needed).
#                  "What is NAV?" → general. "What is the NAV of HDFC Top 100?" → mf_scheme.
#                  "What is a bond?" → general. "What is the coupon of REC bond?" → bond.
#                  Examples of TRUE general: "explain what SIP means",
#                  "what is the difference between NAV and share price",
#                  "how is expense ratio calculated", "what does EBITDA stand for",
#                  "explain dividend yield", "what is a debenture",
#                  "how does an NFO work", "what is market capitalisation"

# ────────────────────────────────────────────────────────────
# DECOMPOSITION RULES (CRITICAL):
# 1. THE 1:1 MAPPING: len(intents) MUST equal the number of subjects identified in Step 1.
# 2. NO BUNDLING: Two different subjects → two separate intent objects.
# 3. DATA POINT MERGING: Multiple metrics on the SAME entity → ONE intent.
# 4. MIXED QUERIES FULLY SUPPORTED: "Reliance price and bond market watch" →
#    intent[0] type=stock, intent[1] type=market_info.
#    "HDFC bond coupon rate and TCS share price" →
#    intent[0] type=bond, intent[1] type=stock.
#    Each intent is handled independently.
# 5. BOND vs STOCK: If a query mentions both a company's equity and its bond/NCD,
#    create SEPARATE intents — one type=stock, one type=bond.
# 6. IPO_STOCK vs IPO_LIST: This is the most important IPO split.
#    - Named company + IPO context → ipo_stock (co_code will be resolved from DB)
#    - No company name / market-wide IPO query → ipo_list (no DB lookup)
#    - "Amir Chand IPO details" → ipo_stock (company named)
#    - "What are the upcoming IPOs?" → ipo_list (no company named)
#    - "Reliance IPO prospectus" → ipo_stock (company named)
#    - "Best IPO performers this month" → ipo_list (no company named)
# 7. GENERAL LAST RESORT: Only assign general when you are certain no live data
#    tool can answer. When in doubt between general and any other type, prefer
#    the other type — live data is almost always available.

# MCP_NEEDED LOGIC:
# - Set to true if ANY intent requires live market data (prices, NAVs, ratios,
#   lists, news, bond prices, coupon data, etc.).
# - Set to false ONLY for pure greetings or purely educational/definitional
#   questions where every single intent is type=general.
# ────────────────────────────────────────────────────────────
# """

# GREETING_PROMPT = """\
# You are EQUIFIZ — a friendly Indian stock-market AI assistant.
# Respond warmly and briefly to the user's greeting or small talk.
# Mention 1-2 things you can help with (PE ratios, quarterly results,
# investment recommendations, live NAV, MF returns, IPO listings,
# bond prices, NCD details, etc.).

# User: {query}
# """

# MCP_SYNTHESIS_PROMPT = """\
# You are a knowledgeable Indian equity, mutual fund, and fixed-income analyst assistant.

# ## Conversation history (last 4 turns)
# {history}

# ## User Question
# {user_query}

# ## Live Data Retrieved (one block per sub-question)
# {mcp_result}

# ## Instructions
# 1. Answer EACH sub-question using the data block labelled for it above.
#    If there are multiple data blocks, address each one in turn.
# 2. Sub-questions marked "(answered from knowledge base)" should be answered
#    from your own knowledge directly in the response.
# 3. Sub-questions where the company was not found should politely inform the
#    user that no record was found and suggest verifying the company name.
# 4. Write in plain prose paragraphs only. No markdown, no bullet points,
#    no asterisks, no tables. Do NOT use the rupee symbol — write "Rs".
# 5. Present key numbers naturally woven into sentences.
# 6. If a specific data field is missing or the tool returned an error, say so briefly.
# 7. Use Indian number formatting (lakh, crore) for large figures.
# 8. For bond/debt data include coupon rate, maturity date, credit rating, and
#    face value where available.
# 9. Keep under 300 words unless the user explicitly asked for detail.
# 10. End with: This is not financial advice.
# """

# GENERAL_SYNTHESIS_PROMPT = """\
# You are a knowledgeable Indian equity and fixed-income analyst assistant.

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


# # ── LLM helpers ───────────────────────────────────────────────────────────────

# def _llm_json(prompt: str) -> dict:
#     resp  = llm.invoke([HumanMessage(content=prompt)])
#     text  = resp.content.strip()
#     # Using `{3}` instead of sequential literal backticks to secure markdown parser containment
#     clean = re.sub(r"^`{3}(?:json)?|`{3}$", "", text, flags=re.MULTILINE).strip()

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
# # Node AIQ: Classify + Extract + Decompose
# # ══════════════════════════════════════════════════════════════════════════════

# def node_classify_extract_and_decompose(state: AgentState) -> AgentState:
#     console.print("[Node AIQ] Classify + extract + decompose (v11.5)")

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
#                 resolved_codes      = {},
#                 mcp_result         = "",
#                 code_missing       = False,
#             ))

#         if not intents and qt not in ("greeting",):
#             console.print("  ⚠ LLM returned no intents — fallback single general intent")
#             intents.append(IntentItem(
#                 entity             = user_query,
#                 entity_type        = "general",
#                 intent_description = user_query,
#                 scheme_name        = None,
#                 amc_name           = None,
#                 nse_symbol         = None,
#                 tool_hint          = "",
#                 matched_tools      = [],
#                 resolved_codes      = {},
#                 mcp_result         = "",
#                 code_missing       = False,
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
#                 "description":          meta.get("description", ""),
#                 "required_parameters": req_params,
#                 "parameters":           meta.get("parameters", ""),
#                 "score":                round(1 - dist, 3),
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
#                 "description":          meta.get("description", ""),
#                 "required_parameters": req_params,
#                 "parameters":           meta.get("parameters", ""),
#             }
#     except Exception as e:
#         console.print(f"  [ToolRegistry] get by id '{tool_name}' failed: {e}")
#     return None


# # ══════════════════════════════════════════════════════════════════════════════
# # Node TVS: Per-intent tool vector search  (v11.5)
# # ══════════════════════════════════════════════════════════════════════════════

# # Allowed tool families per entity_type.
# _ALLOWED_FAMILIES: dict[str, frozenset[str]] = {
#     "stock":        frozenset({"stock"}),
#     "ipo_stock":    frozenset({"ipo_stock"}),
#     "ipo_list":     frozenset({"ipo_list"}),
#     "mf_scheme":    frozenset({"mf_scheme"}),
#     "mf_amc":       frozenset({"mf_amc"}),
#     "etf":          frozenset({"etf"}),
#     "index":        frozenset({"index"}),
#     "market":       frozenset({"market"}),
#     "bond":         frozenset({"bond"}),
#     "exchange":     frozenset(),
#     "nfo":          frozenset(),
#     "news":         frozenset(),
#     "announcement": frozenset(),
#     "market_info":  frozenset(),
#     "general":      frozenset(),
# }


# def node_per_intent_tool_search(state: AgentState) -> AgentState:
#     console.print("[Node TVS] Per-intent tool vector search (v11.5)")

#     intents = state.get("intents") or []
#     if not intents:
#         console.print("  ⚠ No intents to search for")
#         return state

#     def _search_for_intent(intent: IntentItem) -> IntentItem:
#         et = intent.get("entity_type", "general")

#         # Purely educational — skip tool search entirely
#         if et == "general":
#             console.print(
#                 f"  [{intent['entity']}] entity_type=general → skip tool search"
#             )
#             return IntentItem(
#                 entity             = intent.get("entity", ""),
#                 entity_type        = "general",
#                 intent_description = intent.get("intent_description", ""),
#                 scheme_name        = intent.get("scheme_name"),
#                 amc_name           = intent.get("amc_name"),
#                 nse_symbol         = intent.get("nse_symbol"),
#                 tool_hint          = "",
#                 matched_tools      = [],
#                 resolved_codes      = {},
#                 mcp_result         = "",
#                 code_missing       = False,
#             )

#         # Build search query
#         if et in _NO_DB_MCP_TYPES:
#             # No-DB types: search on intent description alone
#             search_query = intent["intent_description"]
#         else:
#             # DB-backed types: prefix with entity type to improve relevance
#             search_query = f"{et} {intent['intent_description']}"

#         tools = _query_tool_registry(search_query, n_results=10)

#         # ── Strict family filter ──────────────────────────────────────────────
#         allowed = _ALLOWED_FAMILIES.get(et, frozenset())
#         if allowed:
#             filtered = [
#                 t for t in tools
#                 if _infer_entity_type_from_tool(t["tool_name"]) in allowed
#             ]
#             if filtered:
#                 tools = filtered
#                 console.print(
#                     f"  [TVS] family filter '{et}' → kept {len(tools)} tools"
#                 )
#             else:
#                 # No tools survived the strict filter — keep original set but warn
#                 console.print(
#                     f"  [TVS] ⚠ family filter '{et}' eliminated all tools "
#                     f"— keeping unfiltered results (may be imprecise)"
#                 )

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
#             resolved_codes      = {},
#             mcp_result         = "",
#             code_missing       = False,
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
#                 original     = intents[idx]
#                 updated[idx] = IntentItem(
#                     entity             = original.get("entity", ""),
#                     entity_type        = original.get("entity_type", "general"),
#                     intent_description = original.get("intent_description", ""),
#                     scheme_name        = original.get("scheme_name"),
#                     amc_name           = original.get("amc_name"),
#                     nse_symbol         = original.get("nse_symbol"),
#                     tool_hint          = "",
#                     matched_tools      = [],
#                     resolved_codes      = {},
#                     mcp_result         = "",
#                     code_missing       = False,
#                 )

#     state["intents"] = [updated[i] for i in sorted(updated)]

#     if state["intents"]:
#         first = state["intents"][0]
#         state["matched_tools"]     = first.get("matched_tools") or []
#         state["mcp_tool_hint"]     = first.get("tool_hint") or ""
#         state["primary_tool_hint"] = first.get("tool_hint") or ""

#     return state


# # ── Greeting handler ──────────────────────────────────────────────────────────

# def node_greeting_handler(state: AgentState) -> AgentState:
#     console.print("[Node B] Greeting handler")
#     state["final_answer"] = _llm_text(GREETING_PROMPT.format(query=state["user_query"]))
#     return state


# # ── General handler (vector search fallback) ──────────────────────────────────

# def node_general_handler(state: AgentState) -> AgentState:
#     """
#     Handles purely educational/definitional questions that have no matching
#     MCP tool. Uses vector search over knowledge base.
#     """
#     console.print("[Node C] General handler (vector search fallback)")
#     try:
#         state["vector_context"] = vs.query(state["user_query"], n_results=5)
#     except Exception as e:
#         console.print(f"  General vector search failed: {e}")
#         state["vector_context"] = []
#     return state


# # ══════════════════════════════════════════════════════════════════════════════
# # FIX M — Hardened Multi-Tiered Fuzzy Match Scoring System
# # ══════════════════════════════════════════════════════════════════════════════

# def _compute_smart_score(target: str, candidate: str) -> float:
#     """
#     Computes a contextual fuzzy score to prevent substring containment hijacks
#     (e.g., matching target 'Infosys' to candidate 'HCL Infosystems Ltd' with score 100).
#     """
#     target_clean = target.lower().strip()
#     candidate_clean = candidate.lower().strip()
    
#     # Tier 1: Exact matches are perfect
#     if target_clean == candidate_clean:
#         return 100.0
        
#     # Tier 2: Token alignment & edit ratio
#     ts_ratio = fuzz.token_sort_ratio(target_clean, candidate_clean)
#     ratio = fuzz.ratio(target_clean, candidate_clean)
#     part_ratio = fuzz.partial_ratio(target_clean, candidate_clean)
    
#     target_words = set(target_clean.split())
#     candidate_words = set(candidate_clean.split())
    
#     # Check if target is explicitly present as an independent whole word in candidate
#     has_exact_word_match = any(word in candidate_words for word in target_words)
    
#     score = max(ts_ratio, ratio)
    
#     if part_ratio > 90.0:
#         if has_exact_word_match:
#             # Upgrade score to reflect explicit containment of whole words (e.g. "Infosys" in "Infosys Limited")
#             score = max(score, part_ratio)
#         else:
#             # Heavy penalty if it's a sub-word hijack (e.g. "Infosys" inside "Infosystems")
#             score = max(score, part_ratio - 35.0)
#     else:
#         score = max(score, part_ratio)
        
#     return float(score)


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
            
#     # Fetch up to 10 company candidates to evaluate and prevent substring hijacks
#     matches = db.fuzzy_search_company(name, limit=10)
#     if matches:
#         best_match = None
#         best_score = -1.0
        
#         for cand in matches:
#             cand_name = cand.get("companyname") or ""
#             cand_sym  = cand.get("nsesymbol") or ""
            
#             score_name = _compute_smart_score(name, cand_name)
#             score_sym  = _compute_smart_score(name, cand_sym)
#             score = max(score_name, score_sym)
            
#             if score > best_score:
#                 best_score = score
#                 best_match = cand
                
#         if best_match and best_score >= 50.0:
#             console.print(f"  [Resolve Single] '{name}' matched to '{best_match.get('companyname')}' with score {best_score:.2f}")
#             return {
#                 "co_code":      best_match["co_code"],
#                 "company_info": best_match,
#                 "nse_symbol":   best_match.get("nsesymbol"),
#             }
#         else:
#             console.print(f"  [Resolve Single] '{name}' matches found but fell below confidence threshold (best={best_score:.2f})")
            
#     return {}


# # ══════════════════════════════════════════════════════════════════════════════
# # DB targeted table lookup
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
#         elif table == "bond_master":
#             cur.execute(
#                 "SELECT code, companyname, isin, nsesymbol FROM bond_master"
#             )
#         else:
#             cur.execute(f"SELECT {id_col}, {name_col} FROM {table}")

#         rows = [dict(r) for r in cur.fetchall()]
#         cur.close()
#         conn.close()

#         best_match: Optional[dict] = None
#         best_score: float          = 0.0

#         for row in rows:
#             candidate = str(row.get(name_col) or "")
#             score = _compute_smart_score(entity, candidate)
            
#             if table == "bond_master":
#                 isin_score = fuzz.ratio(
#                     entity.upper(), str(row.get("isin") or "").upper()
#                 )
#                 sym_score = fuzz.ratio(
#                     entity.upper(), str(row.get("nsesymbol") or "").upper()
#                 )
#                 score = max(score, isin_score, sym_score)

#             if score > best_score:
#                 best_score, best_match = score, row

#         if best_match and best_score >= 60.0:
#             console.print(
#                 f"  ✅ match='{best_match.get(name_col)}' "
#                 f"id={best_match.get(id_col)} score={best_score:.2f}"
#             )
#             return best_match

#         console.print(f"  ⚠ No confident match in {table} (best={best_score:.2f})")
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
#     Returns DB params to resolve for this intent.
#     """
#     params = ENTITY_TYPE_PARAM_MAP.get(entity_type)
#     if params is not None:
#         if params:
#             console.print(
#                 f"  📋 entity_type='{entity_type}' → authoritative params: {params}"
#             )
#         else:
#             console.print(
#                 f"  📋 entity_type='{entity_type}' → no DB params needed"
#             )
#         return params

#     # Fallback: infer from tool metadata
#     if tool_hint:
#         for t in matched_tools:
#             if t["tool_name"] == tool_hint:
#                 db_params = [
#                     p for p in t["required_parameters"] if p in PARAM_TO_TABLE_MAP
#                 ]
#                 if db_params:
#                     return db_params
#                 break

#         meta = _get_tool_meta(tool_hint)
#         if meta:
#             db_params = [
#                 p for p in meta["required_parameters"] if p in PARAM_TO_TABLE_MAP
#             ]
#             if db_params:
#                 return db_params

#     console.print(f"  📋 No DB params resolved for entity_type='{entity_type}'")
#     return []


# # ── Per-intent DB code resolver  (v11.5) ──────────────────────────────────────

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
#     needs_group     = "group"       in required_params
#     needs_bond_code = "bond_code"   in required_params

#     # ── MF scheme ─────────────────────────────────────────────────────────────
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

#     # ── MF AMC ────────────────────────────────────────────────────────────────
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

#     # ── Equity / company  AND  company-specific IPO (ipo_stock) ───────────────
#     if needs_co_code:
#         stock = _resolve_single(name, nse_symbol)
#         if stock:
#             codes["co_code"] = stock["co_code"]
#             result.update({
#                 "co_code":      stock["co_code"],
#                 "company_info": stock["company_info"],
#                 "nse_symbol":   stock["nse_symbol"],
#             })
#         else:
#             # Flag that we could not find a co_code — prevents MCP hallucination
#             result["code_missing"] = True
#             console.print(
#                 f"  ⚠ co_code not found for '{name}' — "
#                 f"MCP call will be skipped for this intent"
#             )

#     # ── ETF ───────────────────────────────────────────────────────────────────
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

#     # ── Index ─────────────────────────────────────────────────────────────────
#     if needs_indexcode:
#         cfg   = PARAM_TO_TABLE_MAP["index_code"]
#         match = _targeted_db_lookup(
#             entity=name, table=cfg["table"],
#             name_col=cfg["column"], id_col=cfg["id_field"],
#         )
#         if match:
#             codes["index_code"] = int(match["indexcode"])
#             result["resolved_index_name"] = match.get("group_name", name)

#     # ── Market group ──────────────────────────────────────────────────────────
#     if needs_group:
#         cfg   = PARAM_TO_TABLE_MAP["group"]
#         match = _targeted_db_lookup(
#             entity=name, table=cfg["table"],
#             name_col=cfg["column"], id_col=cfg["id_field"],
#         )
#         if match:
#             codes["group"] = match["group_name"]
#             result["resolved_group_name"] = match["group_name"]
#         else:
#             console.print(
#                 f"  ⚠ group not found in group_master for '{name}' — using raw name"
#             )
#             codes["group"] = name
#             result["resolved_group_name"] = name

#     # ── Bond ──────────────────────────────────────────────────────────────────
#     if needs_bond_code:
#         cfg   = PARAM_TO_TABLE_MAP["bond_code"]
#         match = _targeted_db_lookup(
#             entity=name, table=cfg["table"],
#             name_col=cfg["column"], id_col=cfg["id_field"],
#         )
#         if match:
#             codes["bond_code"] = int(match["code"])
#             result["resolved_bond_name"] = match.get("companyname", name)
#         else:
#             result["code_missing"] = True
#             console.print(f"  ⚠ bond_code not found in bond_master for '{name}'")

#     result["mcp_resolved_codes"] = codes
#     return result


# # ── Resolve codes for all intents ─────────────────────────────────────────────

# def _resolve_intents_codes(intents: list[IntentItem]) -> list[IntentItem]:
#     def _resolve_one(intent: IntentItem) -> IntentItem:
#         et        = intent.get("entity_type", "general")
#         tool_hint = intent.get("tool_hint", "")
#         matched   = intent.get("matched_tools") or []

#         if et == "general" and tool_hint:
#             inferred = _infer_entity_type_from_tool(tool_hint)
#             if inferred != "general":
#                 et = inferred
#                 intent["entity_type"] = et

#         req_params = _get_required_params_for_entity(tool_hint, et, matched)

#         if not req_params:
#             console.print(
#                 f"  💎 [{intent['entity']}] entity_type='{et}' "
#                 f"→ no code resolution needed"
#             )
#             intent["resolved_codes"] = {}
#             intent["code_missing"]   = False
#             return intent

#         resolved = _resolve_entity_codes(
#             name            = intent.get("entity", ""),
#             scheme_name     = intent.get("scheme_name"),
#             amc_name        = intent.get("amc_name"),
#             nse_symbol      = intent.get("nse_symbol"),
#             required_params = req_params,
#         )
#         intent["resolved_codes"] = resolved.get("mcp_resolved_codes", {})
#         intent["code_missing"]   = bool(resolved.get("code_missing", False))

#         console.print(
#             f"  💎 [{intent['entity']}] "
#             f"resolved_codes={intent['resolved_codes']} "
#             f"code_missing={intent['code_missing']}"
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
#                 console.print(
#                     f"  [red]Code resolution failed for intent #{idx}: {exc}[/red]"
#                 )
#                 result_intents[idx] = intents[idx]

#     return [result_intents[i] for i in sorted(result_intents)]


# # ══════════════════════════════════════════════════════════════════════════════
# # MCP Pre-Resolution nodes
# # ══════════════════════════════════════════════════════════════════════════════

# def node_mcp_pre_resolve(state: AgentState) -> AgentState:
#     console.print("[Node MPR] Single-entity MCP pre-resolve (v11.5)")

#     intents = state.get("intents") or []
#     if not intents:
#         console.print("  ⚠ No intents; skipping resolve")
#         state["mcp_resolved_codes"] = {}
#         return state

#     resolved_intents            = _resolve_intents_codes(intents)
#     state["intents"]            = resolved_intents
#     state["mcp_resolved_codes"] = (
#         resolved_intents[0].get("resolved_codes", {}) if resolved_intents else {}
#     )

#     console.print(f"  🏁 Primary resolved codes: {state['mcp_resolved_codes']}")
#     return state


# def node_mcp_multi_pre_resolve(state: AgentState) -> AgentState:
#     console.print("[Node MMPR] Multi-entity MCP pre-resolve (v11.5)")

#     intents = state.get("intents") or []
#     if not intents:
#         console.print("  ⚠ No intents; skipping resolve")
#         return state

#     resolved_intents            = _resolve_intents_codes(intents)
#     state["intents"]            = resolved_intents
#     state["mcp_resolved_codes"] = (
#         resolved_intents[0].get("resolved_codes", {}) if resolved_intents else {}
#     )

#     console.print(
#         f"  🏁 Resolved {len(resolved_intents)} intents. "
#         f"Primary codes: {state['mcp_resolved_codes']}"
#     )
#     return state


# # ══════════════════════════════════════════════════════════════════════════════
# # Node MTCI: MCP Tool Call — sequential per-intent in one session (v11.5)
# # ══════════════════════════════════════════════════════════════════════════════

# def _run_mcp_multi_in_thread(intents: list[dict]) -> list[dict]:
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
#                 loop.run_until_complete(
#                     asyncio.gather(*pending, return_exceptions=True)
#                 )
#         except Exception:
#             pass
#         loop.close()


# def node_mcp_tool_call_intents(state: AgentState) -> AgentState:
#     console.print("[Node MTCI] Sequential per-intent MCP calls — one session (v11.5)")

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

#     mcp_intents: list[IntentItem]     = []
#     blocked_intents: list[IntentItem] = []
#     general_intents: list[IntentItem] = []

#     for i in intents:
#         et           = i.get("entity_type", "general")
#         has_tool     = bool(i.get("tool_hint"))
#         code_missing = bool(i.get("code_missing", False))

#         if et == "general" and not has_tool:
#             general_intents.append(i)
#         elif code_missing:
#             blocked_intents.append(i)
#             console.print(
#                 f"  🚫 Blocked intent [{i.get('entity')}] — "
#                 f"required code not found in DB (entity_type={et})"
#             )
#         else:
#             mcp_intents.append(i)

#     if general_intents:
#         console.print(
#             f"  ℹ {len(general_intents)} general intent(s) answered via synthesis only"
#         )
#     if blocked_intents:
#         console.print(
#             f"  ⚠ {len(blocked_intents)} blocked intent(s) — company not found in DB"
#         )

#     # Fallback: no MCP intents at all
#     if not mcp_intents:
#         console.print("  ⚠ No MCP intents; falling back to raw user query")
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

#     try:
#         with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
#             future         = ex.submit(_run_mcp_multi_in_thread, mcp_intents)
#             filled_intents = future.result(timeout=600)
#     except concurrent.futures.TimeoutError:
#         console.print("  [red]Multi-intent MCP timed out[/red]")
#         filled_intents = [
#             {**dict(i), "mcp_result": "MCP call timed out."}
#             for i in mcp_intents
#         ]
#     except Exception as exc:
#         console.print(f"  [red]Multi-intent MCP failed: {exc}[/red]")
#         filled_intents = [
#             {**dict(i), "mcp_result": f"MCP call failed: {exc}"}
#             for i in mcp_intents
#         ]

#     # Assemble all result sets
#     all_intents = (
#         filled_intents
#         + [
#             {**dict(i), "mcp_result": "(answered from knowledge base)"}
#             for i in general_intents
#         ]
#         + [
#             {
#                 **dict(i),
#                 "mcp_result": (
#                     f"Sorry, I could not find '{i.get('entity')}' in our database. "
#                     f"Please check the company name and try again."
#                 ),
#             }
#             for i in blocked_intents
#         ]
#     )
#     state["intents"] = all_intents

#     state["intent_results"] = [
#         {
#             "entity":             i.get("entity", ""),
#             "entity_type":        i.get("entity_type", ""),
#             "intent_description": i.get("intent_description", ""),
#             "mcp_result":         i.get("mcp_result", ""),
#         }
#         for i in all_intents
#     ]

#     state["mcp_raw_result"] = "\n\n---\n\n".join(
#         f"[{r['entity']} / {r['intent_description']}]\n{r['mcp_result']}"
#         for r in state["intent_results"]
#         if r.get("mcp_result")
#     )
#     state["mcp_tool_calls_made"] = []
#     state["error"]               = None

#     total_chars = len(state["mcp_raw_result"])
#     console.print(
#         f"  ✅ {len(filled_intents)} MCP intent(s) resolved "
#         f"({len(general_intents)} general, {len(blocked_intents)} blocked). "
#         f"Total chars for synthesis: {total_chars}"
#     )
#     return state


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

# # ══════════════════════════════════════════════════════════════════════════════
# # MCP Synthesis (v11.5)
# # ══════════════════════════════════════════════════════════════════════════════

# def node_mcp_synthesis(state: AgentState) -> AgentState:
#     console.print("[Node MS] MCP synthesis (v11.5)")
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
#         state["final_answer"] = mcp_result
#     return state


# # ── General synthesis (vector search) ────────────────────────────────────────

# def node_synthesis(state: AgentState) -> AgentState:
#     console.print("[Node S] General synthesis (vector search)")
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
# # Router (v11.5)
# # ══════════════════════════════════════════════════════════════════════════════

# def route_after_classify(state: AgentState) -> str:
#     qt      = state.get("query_type", "general")
#     intents = state.get("intents") or []

#     if qt == "greeting":
#         return "greeting"

#     mcp_intents = [
#         i for i in intents
#         if i.get("tool_hint") or i.get("entity_type") not in ("general",)
#     ]

#     for idx, intent in enumerate(intents):
#         console.print(
#             f"  [Router] intent[{idx}] entity={intent.get('entity')} "
#             f"type={intent.get('entity_type')} "
#             f"tool_hint={intent.get('tool_hint') or '(none)'}"
#         )

#     if not mcp_intents or (
#         not any(i.get("tool_hint") for i in intents)
#         and all(i.get("entity_type") == "general" for i in intents)
#     ):
#         console.print("  → general (all intents are educational/no tools)")
#         return "general"

#     has_many         = len(intents) > 1
#     additional       = state.get("companies") or []
#     primary_type     = state.get("primary_entity_type", "general")
#     secondary_types  = {c.get("entity_type", "general") for c in additional}
#     is_heterogeneous = bool(secondary_types - {"general"} - {primary_type})

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
# # Graph (v11.5)
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



# ###  indexwise company 

# from __future__ import annotations

# import asyncio
# import concurrent.futures
# import json
# import logging
# import re
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

# # ── Types ─────────────────────────────────────────────────────────────────────

# class Turn(TypedDict):
#     role: str       # "user" | "assistant"
#     content: str


# class IntentItem(TypedDict, total=False):
#     entity:             str
#     entity_type:        str
#     intent_description: str
#     tool_hint:          str
#     scheme_name:        Optional[str]
#     amc_name:           Optional[str]
#     nse_symbol:         Optional[str]
#     resolved_name:      Optional[str]  # <-- Added to rigidly anchor canonical entity identity
#     matched_tools:      list[dict]
#     resolved_codes:     dict
#     mcp_result:         str
#     code_missing:       bool           # flag set when DB lookup failed for required code


# # All entity types that require NO DB code resolution but still go to MCP
# _NO_DB_MCP_TYPES = frozenset({
#     "exchange",
#     "ipo_list",
#     "nfo",
#     "news",
#     "announcement",
#     "market_info",
# })

# # ── Tool Family Mapping Matrix ───────────────────────────────────────────────
# _TOOL_FAMILY_HINTS: dict[str, str] = {
#     # ── Stock: requires co_code ───────────────────────────────────────────────
#     "get_company_stock_price": "stock",
#     "get_delayed_stock_price": "stock",
#     "get_nse_company_announcements":        "stock",
#     "get_bse_company_announcements":        "stock",
#     "get_company_result_schedule":          "stock",
#     "get_company_profile":                  "stock",
#     "get_company_background":               "stock",
#     "get_board_of_directors":                 "stock",
#     "get_company_bankers":                  "stock",
#     "get_management_biodata":                       "stock",
#     "get_subsidiaries_jvs":                     "stock",
#     "get_related_party_transactions":                    "stock",
#     "get_employee_count":                   "stock",
#     "get_capital_structure":                "stock",
#     "get_pledge_share_details":                     "stock",
#     "get_substantial_acquisitions":                      "stock",
#     "get_segment_data":                     "stock",
#     "get_r_and_d_expenditure":                          "stock",
#     "get_finished_products":                "stock",
#     "get_raw_materials":                    "stock",
#     "get_chronological_history":                    "stock",
#     "get_company_history":                  "stock",
#     "get_funds_holding_company":            "stock",
#     # Financials
#     "get_quarterly_results":                "stock",
#     "get_profit_loss":                      "stock",
#     "get_balance_sheet":                    "stock",
#     "get_cash_flow":                        "stock",
#     "get_half_yearly_results":              "stock",
#     "get_nine_months_results":              "stock",
#     "get_quarterly_trends" :                 "stock",
#     "get_shareholding_pattern":              "stock",
#     "get_major_shareholders":                "stock",
#     "get_quarterly_balance_sheet" :         "stock",
#     "get_yearly_results":                   "stock",
#     "get_annual_balance_sheet":              "stock",
#     "get_half_yearly_balance_sheet":         "stock",
#     "get_ttm_growth_trends":                 "stock",
#     "get_quarterly_revenue_trends":          "stock",
#     "get_quarterly_ebitda_trends":           "stock",
#     "get_quarterly_ebit_trends":             "stock",
#     "get_growth_data_quarterly":             "stock",
#     "get_growth_data_yearly":                "stock",
#     # Ratios
#     "get_key_financial_ratios":             "stock",
#     "get_daily_ratios":                     "stock",
#     "get_margin_ratios":                    "stock",
#     "get_valuation_ratios":                 "stock",
#     "get_all_basic_ratios":                 "stock",
#     "get_return_ratios":                    "stock",
#     "get_growth_ratios":                    "stock",
#     "get_performance_ratios":                "stock",
#     "get_efficiency_ratios":                 "stock",
#     "get_cashflow_ratios":                  "stock",
#     "get_liquidity_ratios":                 "stock",
#     "get_solvency_ratios":                  "stock",
#     "get_quarterly_ratios":                 "stock",
#     "get_yearly_ratios":                    "stock",
#     "get_shareholding":                     "stock",
#     "get_major_sharehold":                  "stock",
#     "get_financial_stability_ratios":       "stock",

#     # ── Company-specific IPO tools: require co_code — family = ipo_stock ─────
#     "get_anchor_investor":                  "ipo_stock",
#     "get_ipo_details":                      "ipo_stock",
#     "get_ipo_subscription_status":          "ipo_stock",
#     "get_ipo_synopsis":                     "ipo_stock",
#     "get_ipo_timeline":                     "ipo_stock",
#     "get_ipo_promoter_details":             "ipo_stock",
#     "get_ipo_listing_info":                 "ipo_stock",
#     "get_ipo_objects_of_issue":             "ipo_stock",
#     "get_ipo_anchor_investor_details":      "ipo_stock",
#     "get_ipo_industry_peers":                "ipo_stock",
#     "get_ipo_financials":                   "ipo_stock",
#     "get_ipo_product_services":             "ipo_stock",
#     "get_ipo_strength_details":             "ipo_stock",
#     "get_ipo_strategy_details":             "ipo_stock",
#     "get_ipo_risk_details":                 "ipo_stock",
#     "get_ipo_selling_shareholders":         "ipo_stock",
#     "get_ipo_allocation_details":           "ipo_stock",
#     "get_ipo_prospectus":                   "ipo_stock",
#     "get_ipo_lead_managers":                "ipo_stock",
#     "get_ipo_registrar":                    "ipo_stock",
#     "get_ipo_customer_details":              "ipo_stock",

#     # ── Index: requires index_code ────────────────────────────────────────────
#     "get_market_indices":                   "stock",   # no specific param but stock-adjacent
#     "get_index_companies":                  "index",

#     # ── Market: requires group (resolved from group_master) ───────────────────
#     "get_active_performer":                 "market",
#     "get_top_gainers":                      "market",
#     "get_top_losers":                       "market",
#     "get_out_under_performers":                   "market",
#     "get_52week_highs":                           "market",
#     "get_52week_lows":                      "market",
#     "get_new_highs_lows":                        "market",
#     "get_sector_companies":                 "market",

#     # ── Exchange: requires ex only ────────────────────────────────────────────
#     "get_advance_decline":                  "exchange",
#     "get_exchange_holidays":                "exchange",
    

#     # ── MF Scheme: requires mf_schcode ────────────────────────────────────────
#     "get_scheme_nav":                       "mf_scheme",
#     "get_investment_details":                "mf_scheme",
#     "get_expense_ratio":                    "mf_scheme",
#     "get_avg_maturity":                     "mf_scheme",
#     "get_scheme_aum":                       "mf_scheme",
#     "get_nav_historical":                   "mf_scheme",
#     "get_scheme_returns":                   "mf_scheme",
#     "get_lumpsum_returns":                  "mf_scheme",
#     "get_scheme_sip":                       "mf_scheme",
#     "get_mf_holdings":                      "mf_scheme",
#     "get_sector_allocation":                "mf_scheme",
#     "get_asset_allocation":                 "mf_scheme",
#     "get_portfolio_changes":                "mf_scheme",
#     "get_mcap_allocation":                  "mf_scheme",
#     "get_most_bought_sold":                  "mf_scheme",
#     "get_scheme_ratios":                    "mf_scheme",
#     "get_dividend_details":                 "mf_scheme",
#     "get_bse_star_scheme":                  "mf_scheme",
#     "compare_schemes":                      "mf_scheme",
#     "get_whats_in_out":                     "mf_scheme",
#     "get_scheme_sip_rules":                 "mf_scheme",
#     "get_scheme_sip_details":               "mf_scheme",

#     # ── MF AMC: requires mf_cocode ────────────────────────────────────────────
#     "get_fund_categories":                  "mf_amc",
#     "get_schemes_by_amc":                   "mf_amc",
#     "get_fund_profile":                     "mf_amc",
#     "get_fund_managers":                     "mf_amc",

#     # ── ETF: requires isin ────────────────────────────────────────────────────
#     "get_etf_quotes":                       "etf",
#     "get_etf_returns":                      "etf",
#     "get_etf_fundamentals":                 "etf",
#     "get_etf_about":                        "etf",
#     "get_etf_equity_holdings":              "etf",
#     "get_etf_monthly_portfolio":               "etf",
#     "get_etf_sector_allocatio":             "etf",
#     "get_etf_asset_allocation":             "etf",
#     "get_etf__":                             "etf",

#     # ── Bond: requires bond_code ──────────────────────────────────────────────
#     "get_debt_eod_prices_scripwise":        "bond",
#     "get_bond_details":                     "bond",
#     "get_bond_price_history":               "bond",
#     "get_bond_cashflow":                    "bond",
#     "get_bond_rating":                      "bond",
#     "get_bond_redemption":                  "bond",
#     "get_bond_interest":                    "bond",
#     "get_bond__":                            "bond",

#     # ── IPO list: NO entity code needed ──────────────────────────────────────
#     "get_forthcoming_ipos":                  "ipo_list",
#     "get_open_ipos":                        "ipo_list",
#     "get_closed_ipos":                      "ipo_list",
#     "get_new_ipo_listings":                          "ipo_list",
#     "get_best_ipo_performers":                         "ipo_list",
#     "get_ipo_master":                       "ipo_list",
#     "get_ipo_logo":                         "ipo_list",
#     "get_forthcoming_drh_filings":           "ipo_list",
#     "get_basis_of_allotment" :               "ipo_list",

#     # bond _ ipo
#     "get_open_bond_ipo":                      "ipo_list",
#     "get_forthcoming_bond_ipo" :               "ipo_list",

#     # ── NFO: no entity code needed ────────────────────────────────────────────
#     "get_new_fund_offer":                   "nfo",

#     # ── News: no entity code needed ───────────────────────────────────────────
#     "get_corporate_news":                   "news",
#     "get_mf_news":                          "news",
#     "get_mf_market_activity":                     "news",

#     # ── Announcement: no entity code needed ───────────────────────────────────
#     "get_bse_announcements":                 "announcement",
#     "get_nse_announcements":                 "announcement",

#     # ── Market Info: no entity code needed ────────────────────────────────────
#     "get_fund_house":                       "market_info",
#     "get_amfi_master":                      "market_info",
#     "get_fund_manager":                     "market_info",
#     "get_index_list":                       "market_info",
#     "get_results_today":                    "market_info",
#     "get_result_declarations":              "market_info",
#     "get_annual_declarations":              "market_info",
#     "get_fund_performance":                 "market_info",
#     "get_category_performance":             "market_info",
#     "get_sip_dates":                        "market_info",
#     "get_macro_economic_data":              "market_info",
#     "get_forthcoming_bond_ipo":             "market_info",
#     "get_open_bond_ipo":                    "market_info",
#     "get_debt_top_value":                   "market_info",
#     "get_debt_top_volume":                  "market_info",
#     "et_new_fund_offers" :                  "market_info",
#     "get_debt_market_watch":                "market_watch",
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
#     "co_code": {
#         "table":    "companies",
#         "column":   "companyname",
#         "id_field": "co_code",
#     },
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
#     "group": {
#         "table":    "group_master",
#         "column":   "group_name",
#         "id_field": "group_name",
#     },
#     "bond_code": {
#         "table":    "bond_master",
#         "column":   "companyname",
#         "id_field": "code",
#     },
# }

# ENTITY_TYPE_PARAM_MAP: dict[str, list[str]] = {
#     "stock":        ["co_code"],
#     "ipo_stock":    ["co_code"],    # company-specific IPO; same DB lookup as stock
#     "mf_scheme":    ["mf_schcode"],
#     "mf_amc":       ["mf_cocode"],
#     "etf":          ["isin"],
#     "index":        ["index_code"],
#     "market":       ["group"],
#     "bond":         ["bond_code"],
#     "exchange":     [],
#     "ipo_list":     [],
#     "nfo":          [],
#     "news":         [],
#     "announcement": [],
#     "market_info":  [],
#     "general":      [],
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

#     matched_tools:  list[dict]
#     intents:        list[IntentItem]
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


# # ── LLM Prompt Templates ──────────────────────────────────────────────────────

# CLASSIFY_EXTRACT_DECOMPOSE_PROMPT = """\
# You are a Lead Financial Systems Analyst for an Indian stock-market assistant. Your task is to decompose user queries into executable data intents.

# ## Context
# - History: {history}
# - Today's Date: {today}
# - Query: "{query}"

# ────────────────────────────────────────────────────────────
# STEP 1: SUBJECT IDENTIFICATION
# Identify every distinct subject the user is asking about. A subject is either:
# 1. A Specific Entity: (e.g., Reliance Industries, HDFC AMC, Nifty 50, SBI Small Cap Fund, Gold BeES ETF, HDFC NCD bond).
# 2. A Market Category/Concept: (e.g., "upcoming IPOs", "top gainers", "market news", "bulk deals", "top losers on NSE", "debt market watch").

# STEP 2: JSON GENERATION
# Generate exactly one intent item for every subject identified in Step 1.

# Return ONLY valid JSON:
# {{
#   "query_type": "<greeting|general|stock|mf_scheme|mf_amc|bond|comparison|investment>",
#   "is_broad": <true|false>,
#   "mcp_needed": <true|false>,
#   "intents": [
#     {{
#       "entity": "<entity name OR market concept name>",
#       "entity_type": "<see ENTITY TYPE RULES below>",
#       "intent_description": "<specific data point needed for this subject>",
#       "scheme_name": "<if mf_scheme, else empty>",
#       "amc_name": "<if mf_amc, else empty>",
#       "nse_symbol": "<NSE ticker if known, else empty>"
#     }}
#   ]
# }}

# ────────────────────────────────────────────────────────────
# ENTITY TYPE RULES (use exactly one of these values):

#   stock        → A specific listed company's EQUITY data. Use when the user asks about
#                  share price, stock quote, OHLC, 52-week high/low, delivery volume,
#                  delayed price, company profile, background, board of directors, bankers,
#                  management team, subsidiaries, related parties, employee count, capital
#                  structure, pledged shares, substantial shareholders, segment data, R&D,
#                  finished products, raw materials, chronological events, company history,
#                  quarterly/annual financial results, P&L, balance sheet, cash flow,
#                  half-yearly/nine-month results, TTM growth, revenue/EBITDA/EBIT trends,
#                  key financial ratios, daily ratios, margin ratios, valuation ratios,
#                  return ratios, growth ratios, performance ratios, cashflow ratios,
#                  liquidity ratios, solvency ratios, shareholding pattern, major shareholders.
#                  ⚠ CRITICAL: Use stock ONLY for equity/share data of a company.
#                  If the query is about the same company's IPO — use ipo_stock instead.
#                  If the query is about the same company's bond/NCD — use bond instead.
#                  Examples: "Reliance share price", "TCS quarterly results",
#                  "HDFC Bank balance sheet", "Infosys management team",
#                  "Wipro shareholding pattern", "ITC PE ratio"

#   ipo_stock    → Company-SPECIFIC IPO data where you know the company name.
#                  Use this when the user asks about IPO details, GMP, allotment status,
#                  anchor investors, anchor investor details, subscription status,
#                  IPO synopsis, IPO timeline, IPO promoter details, IPO listing info,
#                  IPO objects of issue, IPO financials, IPO products/services,
#                  IPO strengths, IPO strategy, IPO risk factors, IPO industry peers,
#                  IPO selling shareholders, IPO allocation details, IPO prospectus,
#                  IPO lead managers, IPO registrar, basis of allotment —
#                  all for a NAMED company.
#                  ⚠ CRITICAL: Use ipo_stock (NOT ipo_list) whenever a specific company
#                  name is mentioned alongside IPO-related data. A co_code DB lookup
#                  will be performed — the company must exist in our database.
#                  Examples: "Amir Chand IPO details", "TCS anchor investors",
#                  "HDFC Bank IPO subscription status", "Reliance IPO prospectus",
#                  "Zomato IPO allotment", "Paytm IPO risk factors",
#                  "LIC IPO lead managers", "Adani IPO timeline"

#   ipo_list     → Generic/market-wide IPO queries with NO specific company name.
#                  Use when the user wants a LIST, overview, or status of IPOs across
#                  the market — forthcoming IPOs, upcoming IPOs, open IPOs, closed IPOs,
#                  new listings, recently listed IPOs, best performing IPOs, IPO master
#                  list, IPO logo, IPO calendar.
#                  ⚠ CRITICAL: Use ipo_list (NOT ipo_stock) when no specific company
#                  is named, or when the user explicitly wants a list/overview of IPOs.
#                  No DB lookup is needed — goes straight to MCP.
#                  Examples: "upcoming IPOs", "open IPOs right now",
#                  "which IPOs are closing this week", "recent IPO listings",
#                  "best performing IPOs this month", "IPO master list",
#                  "forthcoming IPOs on NSE", "new listings today"

#   mf_scheme    → A specific named mutual fund SCHEME. Use when the user asks about
#                  NAV (current or historical), investment details, expense ratio,
#                  average maturity, AUM, scheme returns (1Y/3Y/5Y), lumpsum returns,
#                  SIP calculator, MF holdings/portfolio, sector allocation, asset
#                  allocation, portfolio changes, market-cap allocation, most bought
#                  stocks, scheme ratios, dividend history, BSE STAR platform details,
#                  scheme comparison, what's in/out of portfolio — for a NAMED scheme.
#                  ⚠ CRITICAL: The scheme name must be specific enough to look up in
#                  scheme_master. Provide scheme_name field in JSON.
#                  Examples: "Parag Parikh Flexi Cap NAV", "Axis Bluechip returns",
#                  "SBI Small Cap Fund portfolio", "Mirae Asset Large Cap expense ratio",
#                  "HDFC Mid-Cap Opportunities AUM", "compare Axis vs Mirae bluechip"

#   mf_amc       → A mutual fund HOUSE or AMC (Asset Management Company) — NOT a
#                  specific scheme. Use when the user asks about all schemes offered by
#                  a fund house, fund categories under an AMC, AMC profile/overview,
#                  or fund house details.
#                  ⚠ CRITICAL: Use mf_amc for the fund house itself, not its individual
#                  schemes. Provide amc_name field in JSON.
#                  Examples: "SBI Mutual Fund schemes", "Nippon AMC fund categories",
#                  "HDFC AMC profile", "Axis Mutual Fund all funds",
#                  "Franklin Templeton fund house details", "DSP AMC schemes list"

#   etf          → An exchange-traded fund. Use when the user asks about ETF quotes,
#                  ETF returns, ETF fundamentals, ETF profile/about, ETF equity holdings,
#                  ETF monthly portfolio, ETF sector allocation, ETF asset allocation —
#                  for a NAMED ETF. ETFs trade on exchange like stocks but track an index
#                  or commodity.
#                  ⚠ CRITICAL: Distinguish from regular MF schemes — ETFs have an ISIN
#                  and trade on NSE/BSE intraday. Gold ETF, Index ETF, Sectoral ETF.
#                  Examples: "Gold BeES NAV", "Nifty BeES returns", "SBI ETF Nifty 50",
#                  "Nippon India ETF holdings", "HDFC Sensex ETF fundamentals",
#                  "Bharat Bond ETF portfolio"

#   index        → A market INDEX. Use when the user asks about index constituents,
#                  companies within an index, index composition — for a NAMED index.
#                  ⚠ NOTE: For live index price/level use stock type (get_market_indices
#                  is stock-adjacent). Use index type specifically for index membership
#                  queries (which companies are in Nifty 50, Bank Nifty constituents).
#                  Examples: "Nifty 50 constituents", "Bank Nifty companies list",
#                  "Sensex component stocks", "Nifty Midcap 100 members",
#                  "which stocks are in Nifty IT index"

#   market       → Market-WIDE movers or screeners that require a market GROUP or
#                  segment (e.g. NSE, BSE, specific sector). Use when the user wants
#                  ranked lists across the market: top gainers, top losers, most active
#                  stocks, 52-week highs/lows, new highs, outperformers, underperformers,
#                  sector-wise top performers, delayed/active performers.
#                  ⚠ CRITICAL: This is NOT about a single company — it's about a
#                  ranked list across a market segment. A group code is resolved from
#                  group_master (e.g. "NSE", "BSE", "NIFTY50").
#                  Examples: "top gainers on NSE today", "most active stocks BSE",
#                  "52-week high stocks", "top losers Nifty", "new highs today",
#                  "which sectors are outperforming", "best performing sector stocks"

#   bond         → A specific named listed BOND, DEBENTURE, or NCD (Non-Convertible
#                  Debenture). Use when the user asks about bond EOD prices, bond details
#                  (coupon rate, face value, maturity), bond price history, bond cash
#                  flows, bond credit rating, bond redemption schedule, bond interest
#                  payment schedule — for a NAMED bond/NCD/debenture issuer.
#                  ⚠ CRITICAL: A bond is a DEBT instrument, not equity. If the same
#                  company has both equity and a bond, create SEPARATE intents —
#                  one type=stock, one type=bond. Resolved via bond_master using bond_code.
#                  Examples: "HDFC NCD coupon rate", "SBI bond price",
#                  "Tata Capital debenture details", "REC bond maturity date",
#                  "NHAI bond rating", "Power Finance bond cash flows",
#                  "Muthoot Finance NCD interest schedule"

#   exchange     → EXCHANGE-LEVEL aggregate data with no specific entity. Use when the
#                  user asks about overall market advance-decline ratio, market breadth,
#                  exchange trading holidays, market open/close schedule — for NSE or BSE
#                  as a whole. No DB lookup needed.
#                  ⚠ CRITICAL: This is about the exchange infrastructure itself, not
#                  individual stocks or sectors.
#                  Examples: "NSE advance decline ratio today", "BSE market holidays 2025",
#                  "how many stocks advanced on NSE", "market breadth today",
#                  "NSE trading holidays", "BSE upcoming holidays"

#   nfo          → New Fund Offers — mutual fund schemes that are currently open for
#                  subscription for the FIRST TIME. Use when the user asks about NFOs,
#                  new MF launches, upcoming fund offers, currently open NFOs.
#                  No DB lookup needed.
#                  ⚠ CRITICAL: NFO is different from IPO (IPO = company equity listing;
#                  NFO = new mutual fund scheme launch). Also different from open IPOs.
#                  Examples: "new fund offers this month", "currently open NFOs",
#                  "upcoming NFOs", "which mutual funds are launching",
#                  "new SBI fund offer", "NFO list today"

#   news         → Corporate or mutual fund NEWS and activity feeds. Use when the user
#                  asks for recent news about a company or the MF industry, press
#                  releases, corporate actions feed, MF industry news, MF activities.
#                  No DB lookup needed — news is fetched by category/keyword.
#                  ⚠ CRITICAL: Distinguish from announcements (which are formal
#                  regulatory filings on BSE/NSE). News is editorial/press content.
#                  Examples: "latest news on Reliance", "corporate news today",
#                  "MF industry news", "market news", "recent developments TCS",
#                  "mutual fund activity updates", "HDFC news today"

#   announcement → Formal regulatory ANNOUNCEMENTS filed on BSE or NSE. Use when the
#                  user asks about corporate announcements, exchange filings, regulatory
#                  disclosures, board meeting notices, AGM notices, results announcements,
#                  dividend declarations, merger/acquisition filings on BSE/NSE.
#                  No DB lookup needed.
#                  ⚠ CRITICAL: These are official exchange filings, not news articles.
#                  Examples: "BSE announcements today", "NSE corporate filings",
#                  "recent board meeting announcements", "dividend announcements BSE",
#                  "merger announcements NSE", "quarterly result announcements",
#                  "AGM notice filings"

#   market_info  → Broad market INFORMATION and analytics that don't belong to a single
#                  entity. Use for: fund manager lists, AMFI master data, fund performance
#                  rankings (category-wise), category-level MF performance, SIP transaction
#                  dates, full index list, result declaration calendars (who declares results
#                  today/this week), annual result declarations, macro-economic data
#                  (GDP, CPI, repo rate, inflation), debt market watch (overall bond market
#                  overview), top value traded bonds, top volume traded bonds,
#                  forthcoming bond IPOs (new bond/NCD issuances coming to market).
#                  No DB lookup needed.
#                  ⚠ CRITICAL: Use this when the query is about market-wide data, not
#                  a single named entity. If a specific company's results are asked,
#                  use stock. If a specific bond is asked, use bond.
#                  Examples: "macro economic indicators India", "repo rate today",
#                  "CPI inflation data", "top fund managers", "AMFI master data",
#                  "best performing MF categories", "SIP dates this month",
#                  "debt market overview", "top bonds by volume today",
#                  "forthcoming bond IPOs", "which companies declare results today",
#                  "result calendar this week", "all index list NSE"

#   general      → ONLY for purely educational or definitional questions that CANNOT
#                  be answered by any live market data tool. Use sparingly — only when
#                  the question is about concepts, definitions, explanations, or
#                  calculations that require no live data at all.
#                  ⚠ CRITICAL: Do NOT use general for anything that has live data
#                  available. "What is the PE ratio of TCS?" → stock (live data exists).
#                  "What is a PE ratio?" → general (definition only, no live data needed).
#                  "What is NAV?" → general. "What is the NAV of HDFC Top 100?" → mf_scheme.
#                  "What is a bond?" → general. "What is the coupon of REC bond?" → bond.
#                  Examples of TRUE general: "explain what SIP means",
#                  "what is the difference between NAV and share price",
#                  "how is expense ratio calculated", "what does EBITDA stand for",
#                  "explain dividend yield", "what is a debenture",
#                  "how does an NFO work", "what is market capitalisation"

# ────────────────────────────────────────────────────────────
# DECOMPOSITION RULES (CRITICAL):
# 1. THE 1:1 MAPPING: len(intents) MUST equal the number of subjects identified in Step 1.
# 2. NO BUNDLING: Two different subjects → two separate intent objects.
# 3. DATA POINT MERGING: Multiple metrics on the SAME entity → ONE intent.
# 4. MIXED QUERIES FULLY SUPPORTED: "Reliance price and bond market watch" →
#    intent[0] type=stock, intent[1] type=market_info.
#    "HDFC bond coupon rate and TCS share price" →
#    intent[0] type=bond, intent[1] type=stock.
#    Each intent is handled independently.
# 5. BOND vs STOCK: If a query mentions both a company's equity and its bond/NCD,
#    create SEPARATE intents — one type=stock, one type=bond.
# 6. IPO_STOCK vs IPO_LIST: This is the most important IPO split.
#    - Named company + IPO context → ipo_stock (co_code will be resolved from DB)
#    - No company name / market-wide IPO query → ipo_list (no DB lookup)
#    - "Amir Chand IPO details" → ipo_stock (company named)
#    - "What are the upcoming IPOs?" → ipo_list (no company named)
#    - "Reliance IPO prospectus" → ipo_stock (company named)
#    - "Best IPO performers this month" → ipo_list (no company named)
# 7. GENERAL LAST RESORT: Only assign general when you are certain no live data
#    tool can answer. When in doubt between general and any other type, prefer
#    the other type — live data is almost always available.

# MCP_NEEDED LOGIC:
# - Set to true if ANY intent requires live market data (prices, NAVs, ratios,
#   lists, news, bond prices, coupon data, etc.).
# - Set to false ONLY for pure greetings or purely educational/definitional
#   questions where every single intent is type=general.
# ────────────────────────────────────────────────────────────
# """

# GREETING_PROMPT = """\
# You are EQUIFIZ — a friendly Indian stock-market AI assistant.
# Respond warmly and briefly to the user's greeting or small talk.
# Mention 1-2 things you can help with (PE ratios, quarterly results,
# investment recommendations, live NAV, MF returns, IPO listings,
# bond prices, NCD details, etc.).

# User: {query}
# """

# MCP_SYNTHESIS_PROMPT = """\
# You are a knowledgeable Indian equity, mutual fund, and fixed-income analyst assistant.

# ## Contextual Grounding Rules (CRITICAL)
# - The user query may use colloquial short-names or contain spelling mistakes.
# - You MUST rely strictly on the canonical titles tagged inside the "Live Data Retrieved" boundaries below.
# - NEVER assume an entity dataset belongs to a larger distinct corporate group unless explicitly authorized by the data block. (For example, if the live data block is for "HCL Infosystems Ltd", you are forbidden from using or mentioning metrics or context parameters associated with "HCL Technologies").
# - Keep responses highly targeted to the resolved corporate identity.

# ## Conversation history (last 4 turns)
# {history}

# ## User Question
# {user_query}

# ## Live Data Retrieved (one block per sub-question)
# {mcp_result}

# ## Instructions
# 1. Answer EACH sub-question using the data block labelled for it above.
#    If there are multiple data blocks, address each one in turn.
# 2. Sub-questions marked "(answered from knowledge base)" should be answered
#    from your own knowledge directly in the response.
# 3. Sub-questions where the company was not found should politely inform the
#    user that no record was found and suggest verifying the company name.
# 4. Write in plain prose paragraphs only. No markdown, no bullet points,
#    no asterisks, no tables. Do NOT use the rupee symbol — write "Rs".
# 5. Present key numbers naturally woven into sentences.
# 6. If a specific data field is missing or the tool returned an error, say so briefly.
# 7. Use Indian number formatting (lakh, crore) for large figures.
# 8. For bond/debt data include coupon rate, maturity date, credit rating, and
#    face value where available.
# 9. Keep under 300 words unless the user explicitly asked for detail.
# 10. End with: This is not financial advice.
# """

# GENERAL_SYNTHESIS_PROMPT = """\
# You are a knowledgeable Indian equity and fixed-income analyst assistant.

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


# # ── LLM helpers ───────────────────────────────────────────────────────────────

# def _llm_json(prompt: str) -> dict:
#     resp  = llm.invoke([HumanMessage(content=prompt)])
#     text  = resp.content.strip()
#     clean = re.sub(r"^`{3}(?:json)?|`{3}$", "", text, flags=re.MULTILINE).strip()

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
# # Node AIQ: Classify + Extract + Decompose
# # ══════════════════════════════════════════════════════════════════════════════

# def node_classify_extract_and_decompose(state: AgentState) -> AgentState:
#     console.print("[Node AIQ] Classify + extract + decompose (v11.5-Hardened)")

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
#                 resolved_name      = None,  # Will be populated during pre-resolution
#                 tool_hint          = "",
#                 matched_tools      = [],
#                 resolved_codes      = {},
#                 mcp_result         = "",
#                 code_missing       = False,
#             ))

#         if not intents and qt not in ("greeting",):
#             console.print("  ⚠ LLM returned no intents — fallback single general intent")
#             intents.append(IntentItem(
#                 entity             = user_query,
#                 entity_type        = "general",
#                 intent_description = user_query,
#                 scheme_name        = None,
#                 amc_name           = None,
#                 nse_symbol         = None,
#                 resolved_name      = None,
#                 tool_hint          = "",
#                 matched_tools      = [],
#                 resolved_codes      = {},
#                 mcp_result         = "",
#                 code_missing       = False,
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
#                 "description":          meta.get("description", ""),
#                 "required_parameters": req_params,
#                 "parameters":           meta.get("parameters", ""),
#                 "score":                round(1 - dist, 3),
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
#                 "description":          meta.get("description", ""),
#                 "required_parameters": req_params,
#                 "parameters":           meta.get("parameters", ""),
#             }
#     except Exception as e:
#         console.print(f"  [ToolRegistry] get by id '{tool_name}' failed: {e}")
#     return None


# # ══════════════════════════════════════════════════════════════════════════════
# # Node TVS: Per-intent tool vector search  (v11.5)
# # ══════════════════════════════════════════════════════════════════════════════

# # Allowed tool families per entity_type.
# _ALLOWED_FAMILIES: dict[str, frozenset[str]] = {
#     "stock":        frozenset({"stock"}),
#     "ipo_stock":    frozenset({"ipo_stock"}),
#     "ipo_list":     frozenset({"ipo_list"}),
#     "mf_scheme":    frozenset({"mf_scheme"}),
#     "mf_amc":       frozenset({"mf_amc"}),
#     "etf":          frozenset({"etf"}),
#     "index":        frozenset({"index"}),
#     "market":       frozenset({"market"}),
#     "bond":         frozenset({"bond"}),
#     "exchange":     frozenset(),
#     "nfo":          frozenset(),
#     "news":         frozenset(),
#     "announcement": frozenset(),
#     "market_info":  frozenset(),
#     "general":      frozenset(),
# }


# def node_per_intent_tool_search(state: AgentState) -> AgentState:
#     console.print("[Node TVS] Per-intent tool vector search (v11.5)")

#     intents = state.get("intents") or []
#     if not intents:
#         console.print("  ⚠ No intents to search for")
#         return state

#     def _search_for_intent(intent: IntentItem) -> IntentItem:
#         et = intent.get("entity_type", "general")

#         # Purely educational — skip tool search entirely
#         if et == "general":
#             console.print(
#                 f"  [{intent['entity']}] entity_type=general → skip tool search"
#             )
#             return IntentItem(
#                 entity             = intent.get("entity", ""),
#                 entity_type        = "general",
#                 intent_description = intent.get("intent_description", ""),
#                 scheme_name        = intent.get("scheme_name"),
#                 amc_name           = intent.get("amc_name"),
#                 nse_symbol         = intent.get("nse_symbol"),
#                 resolved_name      = None,
#                 tool_hint          = "",
#                 matched_tools      = [],
#                 resolved_codes      = {},
#                 mcp_result         = "",
#                 code_missing       = False,
#             )

#         # Build search query
#         if et in _NO_DB_MCP_TYPES:
#             # No-DB types: search on intent description alone
#             search_query = intent["intent_description"]
#         else:
#             # DB-backed types: prefix with entity type to improve relevance
#             search_query = f"{et} {intent['intent_description']}"

#         tools = _query_tool_registry(search_query, n_results=10)

#         # ── Strict family filter ──────────────────────────────────────────────
#         allowed = _ALLOWED_FAMILIES.get(et, frozenset())
#         if allowed:
#             filtered = [
#                 t for t in tools
#                 if _infer_entity_type_from_tool(t["tool_name"]) in allowed
#             ]
#             if filtered:
#                 tools = filtered
#                 console.print(
#                     f"  [TVS] family filter '{et}' → kept {len(tools)} tools"
#                 )
#             else:
#                 # No tools survived the strict filter — keep original set but warn
#                 console.print(
#                     f"  [TVS] ⚠ family filter '{et}' eliminated all tools "
#                     f"— keeping unfiltered results (may be imprecise)"
#                 )

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
#             resolved_name      = None,
#             tool_hint          = tool_hint,
#             matched_tools      = tools,
#             resolved_codes      = {},
#             mcp_result         = "",
#             code_missing       = False,
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
#                 original     = intents[idx]
#                 updated[idx] = IntentItem(
#                     entity             = original.get("entity", ""),
#                     entity_type        = original.get("entity_type", "general"),
#                     intent_description = original.get("intent_description", ""),
#                     scheme_name        = original.get("scheme_name"),
#                     amc_name           = original.get("amc_name"),
#                     nse_symbol         = original.get("nse_symbol"),
#                     resolved_name      = None,
#                     tool_hint          = "",
#                     matched_tools      = [],
#                     resolved_codes      = {},
#                     mcp_result         = "",
#                     code_missing       = False,
#                 )

#     state["intents"] = [updated[i] for i in sorted(updated)]

#     if state["intents"]:
#         first = state["intents"][0]
#         state["matched_tools"]     = first.get("matched_tools") or []
#         state["mcp_tool_hint"]     = first.get("tool_hint") or ""
#         state["primary_tool_hint"] = first.get("tool_hint") or ""

#     return state


# # ── Greeting handler ──────────────────────────────────────────────────────────

# def node_greeting_handler(state: AgentState) -> AgentState:
#     console.print("[Node B] Greeting handler")
#     state["final_answer"] = _llm_text(GREETING_PROMPT.format(query=state["user_query"]))
#     return state


# # ── General handler (vector search fallback) ──────────────────────────────────

# def node_general_handler(state: AgentState) -> AgentState:
#     console.print("[Node C] General handler (vector search fallback)")
#     try:
#         state["vector_context"] = vs.query(state["user_query"], n_results=5)
#     except Exception as e:
#         console.print(f"  General vector search failed: {e}")
#         state["vector_context"] = []
#     return state


# # ══════════════════════════════════════════════════════════════════════════════
# # FIX M — Hardened Multi-Tiered Fuzzy Match Scoring System
# # ══════════════════════════════════════════════════════════════════════════════

# def _compute_smart_score(target: str, candidate: str) -> float:
#     target_clean = target.lower().strip()
#     candidate_clean = candidate.lower().strip()
    
#     # Tier 1: Exact matches are perfect
#     if target_clean == candidate_clean:
#         return 100.0
        
#     # Tier 2: Token alignment & edit ratio
#     ts_ratio = fuzz.token_sort_ratio(target_clean, candidate_clean)
#     ratio = fuzz.ratio(target_clean, candidate_clean)
#     part_ratio = fuzz.partial_ratio(target_clean, candidate_clean)
    
#     target_words = set(target_clean.split())
#     candidate_words = set(candidate_clean.split())
    
#     # Check if target is explicitly present as an independent whole word in candidate
#     has_exact_word_match = any(word in candidate_words for word in target_words)
    
#     score = max(ts_ratio, ratio)
    
#     if part_ratio > 90.0:
#         if has_exact_word_match:
#             # Upgrade score to reflect explicit containment of whole words (e.g. "Infosys" in "Infosys Limited")
#             score = max(score, part_ratio)
#         else:
#             # Heavy penalty if it's a sub-word hijack (e.g. "Infosys" inside "Infosystems")
#             score = max(score, part_ratio - 35.0)
#     else:
#         score = max(score, part_ratio)
        
#     return float(score)


# # ── Symbol resolution helper ──────────────────────────────────────────────────

# def _resolve_single(name: str, nse_symbol: Optional[str]) -> dict:
#     if nse_symbol:
#         row = db.lookup_by_nse_symbol(nse_symbol)
#         if row:
#             return {
#                 "co_code":      row["co_code"],
#                 "company_info": row,
#                 "nse_symbol":   nse_symbol,
#                 "resolved_name": row.get("companyname"),
#             }
            
#     # Fetch up to 10 company candidates to evaluate and prevent substring hijacks
#     matches = db.fuzzy_search_company(name, limit=10)
#     if matches:
#         best_match = None
#         best_score = -1.0
        
#         for cand in matches:
#             cand_name = cand.get("companyname") or ""
#             cand_sym  = cand.get("nsesymbol") or ""
            
#             score_name = _compute_smart_score(name, cand_name)
#             score_sym  = _compute_smart_score(name, cand_sym)
#             score = max(score_name, score_sym)
            
#             if score > best_score:
#                 best_score = score
#                 best_match = cand
                
#         if best_match and best_score >= 50.0:
#             console.print(f"  [Resolve Single] '{name}' matched to '{best_match.get('companyname')}' with score {best_score:.2f}")
#             return {
#                 "co_code":      best_match["co_code"],
#                 "company_info": best_match,
#                 "nse_symbol":   best_match.get("nsesymbol"),
#                 "resolved_name": best_match.get("companyname"),
#             }
#         else:
#             console.print(f"  [Resolve Single] '{name}' matches found but fell below confidence threshold (best={best_score:.2f})")
            
#     return {}


# # ══════════════════════════════════════════════════════════════════════════════
# # DB targeted table lookup
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
#         console.print(f"   🔍 DB lookup: table='{table}' entity='{entity}'")

#         if table == "scheme_master":
#             cur.execute(
#                 "SELECT mf_schcode, mf_cocode, sch_name, category FROM scheme_master"
#             )
#         elif table == "bond_master":
#             cur.execute(
#                 "SELECT code, companyname, isin, nsesymbol FROM bond_master"
#             )
#         else:
#             cur.execute(f"SELECT {id_col}, {name_col} FROM {table}")

#         rows = [dict(r) for r in cur.fetchall()]
#         cur.close()
#         conn.close()

#         best_match: Optional[dict] = None
#         best_score: float          = 0.0

#         for row in rows:
#             candidate = str(row.get(name_col) or "")
#             score = _compute_smart_score(entity, candidate)
            
#             if table == "bond_master":
#                 isin_score = fuzz.ratio(
#                     entity.upper(), str(row.get("isin") or "").upper()
#                 )
#                 sym_score = fuzz.ratio(
#                     entity.upper(), str(row.get("nsesymbol") or "").upper()
#                 )
#                 score = max(score, isin_score, sym_score)

#             if score > best_score:
#                 best_score, best_match = score, row

#         if best_match and best_score >= 60.0:
#             console.print(
#                 f"   ✅ match='{best_match.get(name_col)}' "
#                 f"id={best_match.get(id_col)} score={best_score:.2f}"
#             )
#             return best_match

#         console.print(f"   ⚠ No confident match in {table} (best={best_score:.2f})")
#         return None

#     except Exception as e:
#         console.print(f"   [_targeted_db_lookup] DB error on {table}: {e}")
#         return None


# # ══════════════════════════════════════════════════════════════════════════════
# # Required params resolver
# # ══════════════════════════════════════════════════════════════════════════════

# def _get_required_params_for_entity(
#     tool_hint:     str,
#     entity_type:   str,
#     matched_tools: list[dict],
# ) -> list[str]:
#     params = ENTITY_TYPE_PARAM_MAP.get(entity_type)
#     if params is not None:
#         if params:
#             console.print(
#                 f"   📋 entity_type='{entity_type}' → authoritative params: {params}"
#             )
#         else:
#             console.print(
#                 f"   📋 entity_type='{entity_type}' → no DB params needed"
#             )
#         return params

#     # Fallback: infer from tool metadata
#     if tool_hint:
#         for t in matched_tools:
#             if t["tool_name"] == tool_hint:
#                 db_params = [
#                     p for p in t["required_parameters"] if p in PARAM_TO_TABLE_MAP
#                 ]
#                 if db_params:
#                     return db_params
#                 break

#         meta = _get_tool_meta(tool_hint)
#         if meta:
#             db_params = [
#                 p for p in meta["required_parameters"] if p in PARAM_TO_TABLE_MAP
#             ]
#             if db_params:
#                 return db_params

#     console.print(f"   📋 No DB params resolved for entity_type='{entity_type}'")
#     return []


# # ── Per-intent DB code resolver  (v11.5-Hardened) ──────────────────────────────

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
#     needs_group     = "group"       in required_params
#     needs_bond_code = "bond_code"   in required_params

#     # ── MF scheme ─────────────────────────────────────────────────────────────
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

#     # ── MF AMC ────────────────────────────────────────────────────────────────
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

#     # ── Equity / company  AND  company-specific IPO (ipo_stock) ───────────────
#     if needs_co_code:
#         stock = _resolve_single(name, nse_symbol)
#         if stock:
#             codes["co_code"] = stock["co_code"]
#             result.update({
#                 "co_code":      stock["co_code"],
#                 "company_info": stock["company_info"],
#                 "nse_symbol":   stock["nse_symbol"],
#                 "resolved_company_name": stock["resolved_name"]
#             })
#         else:
#             result["code_missing"] = True
#             console.print(
#                 f"   ⚠ co_code not found for '{name}' — "
#                 f"MCP call will be skipped for this intent"
#             )

#     # ── ETF ───────────────────────────────────────────────────────────────────
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

#     # ── Index ─────────────────────────────────────────────────────────────────
#     if needs_indexcode:
#         cfg   = PARAM_TO_TABLE_MAP["index_code"]
#         match = _targeted_db_lookup(
#             entity=name, table=cfg["table"],
#             name_col=cfg["column"], id_col=cfg["id_field"],
#         )
#         if match:
#             codes["index_code"] = int(match["indexcode"])
#             result["resolved_index_name"] = match.get("group_name", name)

#     # ── Market group ──────────────────────────────────────────────────────────
#     if needs_group:
#         cfg   = PARAM_TO_TABLE_MAP["group"]
#         match = _targeted_db_lookup(
#             entity=name, table=cfg["table"],
#             name_col=cfg["column"], id_col=cfg["id_field"],
#         )
#         if match:
#             codes["group"] = match["group_name"]
#             result["resolved_group_name"] = match["group_name"]
#         else:
#             console.print(
#                 f"   ⚠ group not found in group_master for '{name}' — using raw name"
#             )
#             codes["group"] = name
#             result["resolved_group_name"] = name

#     # ── Bond ──────────────────────────────────────────────────────────────────
#     if needs_bond_code:
#         cfg   = PARAM_TO_TABLE_MAP["bond_code"]
#         match = _targeted_db_lookup(
#             entity=name, table=cfg["table"],
#             name_col=cfg["column"], id_col=cfg["id_field"],
#         )
#         if match:
#             codes["bond_code"] = int(match["code"])
#             result["resolved_bond_name"] = match.get("companyname", name)
#         else:
#             result["code_missing"] = True
#             console.print(f"   ⚠ bond_code not found in bond_master for '{name}'")

#     result["mcp_resolved_codes"] = codes
#     return result


# # ── Resolve codes for all intents ─────────────────────────────────────────────

# def _resolve_intents_codes(intents: list[IntentItem]) -> list[IntentItem]:
#     def _resolve_one(intent: IntentItem) -> IntentItem:
#         et        = intent.get("entity_type", "general")
#         tool_hint = intent.get("tool_hint", "")
#         matched   = intent.get("matched_tools") or []

#         if et == "general" and tool_hint:
#             inferred = _infer_entity_type_from_tool(tool_hint)
#             if inferred != "general":
#                 et = inferred
#                 intent["entity_type"] = et

#         req_params = _get_required_params_for_entity(tool_hint, et, matched)

#         if not req_params:
#             console.print(
#                 f"   💎 [{intent['entity']}] entity_type='{et}' "
#                 f"→ no code resolution needed"
#             )
#             intent["resolved_codes"] = {}
#             intent["code_missing"]   = False
#             intent["resolved_name"]  = intent.get("entity")
#             return intent

#         resolved = _resolve_entity_codes(
#             name            = intent.get("entity", ""),
#             scheme_name     = intent.get("scheme_name"),
#             amc_name        = intent.get("amc_name"),
#             nse_symbol      = intent.get("nse_symbol"),
#             required_params = req_params,
#         )
#         intent["resolved_codes"] = resolved.get("mcp_resolved_codes", {})
#         intent["code_missing"]   = bool(resolved.get("code_missing", False))
        
#         # Rigorous fallback cascade for matching the normalized string
#         intent["resolved_name"] = (
#             resolved.get("resolved_company_name") or
#             resolved.get("resolved_scheme_name") or
#             resolved.get("resolved_amc_name") or
#             resolved.get("resolved_etf_name") or
#             resolved.get("resolved_index_name") or
#             resolved.get("resolved_bond_name") or
#             intent.get("entity")
#         )

#         console.print(
#             f"   💎 [{intent['entity']}] -> Resolved Name: '{intent['resolved_name']}' "
#             f"resolved_codes={intent['resolved_codes']} "
#             f"code_missing={intent['code_missing']}"
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
#                 console.print(
#                     f"   [red]Code resolution failed for intent #{idx}: {exc}[/red]"
#                 )
#                 result_intents[idx] = intents[idx]

#     return [result_intents[i] for i in sorted(result_intents)]


# # ══════════════════════════════════════════════════════════════════════════════
# # MCP Pre-Resolution nodes
# # ══════════════════════════════════════════════════════════════════════════════

# def node_mcp_pre_resolve(state: AgentState) -> AgentState:
#     console.print("[Node MPR] Single-entity MCP pre-resolve (v11.5)")

#     intents = state.get("intents") or []
#     if not intents:
#         console.print("  ⚠ No intents; skipping resolve")
#         state["mcp_resolved_codes"] = {}
#         return state

#     resolved_intents            = _resolve_intents_codes(intents)
#     state["intents"]            = resolved_intents
#     state["mcp_resolved_codes"] = (
#         resolved_intents[0].get("resolved_codes", {}) if resolved_intents else {}
#     )

#     console.print(f"  🏁 Primary resolved codes: {state['mcp_resolved_codes']}")
#     return state


# def node_mcp_multi_pre_resolve(state: AgentState) -> AgentState:
#     console.print("[Node MMPR] Multi-entity MCP pre-resolve (v11.5)")

#     intents = state.get("intents") or []
#     if not intents:
#         console.print("  ⚠ No intents; skipping resolve")
#         return state

#     resolved_intents            = _resolve_intents_codes(intents)
#     state["intents"]            = resolved_intents
#     state["mcp_resolved_codes"] = (
#         resolved_intents[0].get("resolved_codes", {}) if resolved_intents else {}
#     )

#     console.print(
#         f"  🏁 Resolved {len(resolved_intents)} intents. "
#         f"Primary codes: {state['mcp_resolved_codes']}"
#     )
#     return state


# # ══════════════════════════════════════════════════════════════════════════════
# # Node MTCI: MCP Tool Call — sequential per-intent in one session (v11.5)
# # ══════════════════════════════════════════════════════════════════════════════

# def _run_mcp_multi_in_thread(intents: list[dict]) -> list[dict]:
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
#                 loop.run_until_complete(
#                     asyncio.gather(*pending, return_exceptions=True)
#                 )
#         except Exception:
#             pass
#         loop.close()


# def node_mcp_tool_call_intents(state: AgentState) -> AgentState:
#     console.print("[Node MTCI] Sequential per-intent MCP calls — one session (v11.5)")

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

#     mcp_intents: list[IntentItem]     = []
#     blocked_intents: list[IntentItem] = []
#     general_intents: list[IntentItem] = []

#     for i in intents:
#         et           = i.get("entity_type", "general")
#         has_tool     = bool(i.get("tool_hint"))
#         code_missing = bool(i.get("code_missing", False))

#         if et == "general" and not has_tool:
#             general_intents.append(i)
#         elif code_missing:
#             blocked_intents.append(i)
#             console.print(
#                 f"   🚫 Blocked intent [{i.get('entity')}] — "
#                 f"required code not found in DB (entity_type={et})"
#             )
#         else:
#             mcp_intents.append(i)

#     if general_intents:
#         console.print(
#             f"   ℹ {len(general_intents)} general intent(s) answered via synthesis only"
#         )
#     if blocked_intents:
#         console.print(
#             f"   ⚠ {len(blocked_intents)} blocked intent(s) — company not found in DB"
#         )

#     # Fallback: no MCP intents at all
#     if not mcp_intents:
#         console.print("   ⚠ No MCP intents; falling back to raw user query")
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

#     try:
#         with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
#             future         = ex.submit(_run_mcp_multi_in_thread, mcp_intents)
#             filled_intents = future.result(timeout=600)
#     except concurrent.futures.TimeoutError:
#         console.print("   [red]Multi-intent MCP timed out[/red]")
#         filled_intents = [
#             {**dict(i), "mcp_result": "MCP call timed out."}
#             for i in mcp_intents
#         ]
#     except Exception as exc:
#         console.print(f"   [red]Multi-intent MCP failed: {exc}[/red]")
#         filled_intents = [
#             {**dict(i), "mcp_result": f"MCP call failed: {exc}"}
#             for i in mcp_intents
#         ]

#     # Assemble all result sets
#     all_intents = (
#         filled_intents
#         + [
#             {**dict(i), "mcp_result": "(answered from knowledge base)"}
#             for i in general_intents
#         ]
#         + [
#             {
#                 **dict(i),
#                 "mcp_result": (
#                     f"Sorry, I could not find '{i.get('entity')}' in our database. "
#                     f"Please check the company name and try again."
#                 ),
#             }
#             for i in blocked_intents
#         ]
#     )
#     state["intents"] = all_intents

#     state["intent_results"] = [
#         {
#             "entity":             i.get("entity", ""),
#             "resolved_name":      i.get("resolved_name", i.get("entity", "")),
#             "entity_type":        i.get("entity_type", ""),
#             "intent_description": i.get("intent_description", ""),
#             "mcp_result":         i.get("mcp_result", ""),
#         }
#         for i in all_intents
#     ]

#     # Structural contextual containment packaging to stop upstream parameter hallucination
#     state["mcp_raw_result"] = "\n\n---\n\n".join(
#         f"--- RESOLVED ENTITY CANONICAL DATA FOR: {r.get('resolved_name', r['entity']).upper()} ---\n"
#         f"[Requested Segment: {r['intent_description']}]\n"
#         f"{r['mcp_result']}"
#         for r in state["intent_results"]
#         if r.get("mcp_result")
#     )
#     state["mcp_tool_calls_made"] = []
#     state["error"]               = None

#     total_chars = len(state["mcp_raw_result"])
#     console.print(
#         f"   ✅ {len(filled_intents)} MCP intent(s) resolved "
#         f"({len(general_intents)} general, {len(blocked_intents)} blocked). "
#         f"Total chars for synthesis: {total_chars}"
#     )
#     return state


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

# # ══════════════════════════════════════════════════════════════════════════════
# # MCP Synthesis (v11.5)
# # ══════════════════════════════════════════════════════════════════════════════

# def node_mcp_synthesis(state: AgentState) -> AgentState:
#     console.print("[Node MS] MCP synthesis (v11.5)")
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
#         state["final_answer"] = mcp_result
#     return state


# # ── General synthesis (vector search) ────────────────────────────────────────

# def node_synthesis(state: AgentState) -> AgentState:
#     console.print("[Node S] General synthesis (vector search)")
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
# # Router (v11.5)
# # ══════════════════════════════════════════════════════════════════════════════

# def route_after_classify(state: AgentState) -> str:
#     qt      = state.get("query_type", "general")
#     intents = state.get("intents") or []

#     if qt == "greeting":
#         return "greeting"

#     mcp_intents = [
#         i for i in intents
#         if i.get("tool_hint") or i.get("entity_type") not in ("general",)
#     ]

#     for idx, intent in enumerate(intents):
#         console.print(
#             f"   [Router] intent[{idx}] entity={intent.get('entity')} "
#             f"type={intent.get('entity_type')} "
#             f"tool_hint={intent.get('tool_hint') or '(none)'}"
#         )

#     if not mcp_intents or (
#         not any(i.get("tool_hint") for i in intents)
#         and all(i.get("entity_type") == "general" for i in intents)
#     ):
#         console.print("   → general (all intents are educational/no tools)")
#         return "general"

#     has_many         = len(intents) > 1
#     additional       = state.get("companies") or []
#     primary_type     = state.get("primary_entity_type", "general")
#     secondary_types  = {c.get("entity_type", "general") for c in additional}
#     is_heterogeneous = bool(secondary_types - {"general"} - {primary_type})

#     if has_many or qt == "comparison" or is_heterogeneous:
#         console.print(
#             f"   → multi_mcp "
#             f"(has_many={has_many}, comparison={qt == 'comparison'}, "
#             f"heterogeneous={is_heterogeneous})"
#         )
#         return "multi_mcp"

#     console.print(f"   → mcp_direct (single entity, type={primary_type})")
#     return "mcp_direct"


# # ══════════════════════════════════════════════════════════════════════════════
# # Graph (v11.5)
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



# from __future__ import annotations

# import asyncio
# import concurrent.futures
# import json
# import logging
# import re
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

# # ── Types ─────────────────────────────────────────────────────────────────────

# class Turn(TypedDict):
#     role: str       # "user" | "assistant"
#     content: str


# class IntentItem(TypedDict, total=False):
#     entity:             str
#     entity_type:        str
#     intent_description: str
#     tool_hint:          str
#     scheme_name:        Optional[str]
#     amc_name:           Optional[str]
#     nse_symbol:         Optional[str]
#     resolved_name:      Optional[str]  
#     matched_tools:      list[dict]
#     resolved_codes:     dict
#     mcp_result:         str
#     code_missing:       bool           


# _NO_DB_MCP_TYPES = frozenset({
#     "exchange",
#     "ipo_list",
#     "nfo",
#     "news",
#     "announcement",
#     "market_info",
# })

# # ── Tool Family Mapping Matrix ───────────────────────────────────────────────
# _TOOL_FAMILY_HINTS: dict[str, str] = {
#     "get_company_stock_price": "stock",
#     "get_delayed_stock_price": "stock",
#     "get_nse_company_announcements":        "stock",
#     "get_bse_company_announcements":        "stock",
#     "get_company_result_schedule":          "stock",
#     "get_company_profile":                  "stock",
#     "get_company_background":               "stock",
#     "get_board_of_directors":                 "stock",
#     "get_company_bankers":                  "stock",
#     "get_management_biodata":                       "stock",
#     "get_subsidiaries_jvs":                     "stock",
#     "get_related_party_transactions":                    "stock",
#     "get_employee_count":                   "stock",
#     "get_capital_structure":                "stock",
#     "get_pledge_share_details":                     "stock",
#     "get_substantial_acquisitions":                      "stock",
#     "get_segment_data":                     "stock",
#     "get_r_and_d_expenditure":                          "stock",
#     "get_finished_products":                "stock",
#     "get_raw_materials":                    "stock",
#     "get_chronological_history":                    "stock",
#     "get_company_history":                  "stock",
#     "get_funds_holding_company":            "stock",
#     "get_quarterly_results":                "stock",
#     "get_profit_loss":                      "stock",
#     "get_balance_sheet":                    "stock",
#     "get_cash_flow":                        "stock",
#     "get_half_yearly_results":              "stock",
#     "get_nine_months_results":              "stock",
#     "get_quarterly_trends" :                 "stock",
#     "get_shareholding_pattern":              "stock",
#     "get_major_shareholders":                "stock",
#     "get_quarterly_balance_sheet" :         "stock",
#     "get_yearly_results":                   "stock",
#     "get_annual_balance_sheet":              "stock",
#     "get_half_yearly_balance_sheet":         "stock",
#     "get_ttm_growth_trends":                 "stock",
#     "get_quarterly_revenue_trends":          "stock",
#     "get_quarterly_ebitda_trends":           "stock",
#     "get_quarterly_ebit_trends":             "stock",
#     "get_growth_data_quarterly":             "stock",
#     "get_growth_data_yearly":                "stock",
#     "get_key_financial_ratios":             "stock",
#     "get_daily_ratios":                     "stock",
#     "get_margin_ratios":                    "stock",
#     "get_valuation_ratios":                 "stock",
#     "get_all_basic_ratios":                 "stock",
#     "get_return_ratios":                    "stock",
#     "get_growth_ratios":                    "stock",
#     "get_performance_ratios":                "stock",
#     "get_efficiency_ratios":                 "stock",
#     "get_cashflow_ratios":                  "stock",
#     "get_liquidity_ratios":                 "stock",
#     "get_solvency_ratios":                  "stock",
#     "get_quarterly_ratios":                 "stock",
#     "get_yearly_ratios":                    "stock",
#     "get_shareholding":                     "stock",
#     "get_major_sharehold":                  "stock",
#     "get_financial_stability_ratios":       "stock",
#     "get_anchor_investor":                  "ipo_stock",
#     "get_ipo_details":                      "ipo_stock",
#     "get_ipo_subscription_status":          "ipo_stock",
#     "get_ipo_synopsis":                     "ipo_stock",
#     "get_ipo_timeline":                     "ipo_stock",
#     "get_ipo_promoter_details":             "ipo_stock",
#     "get_ipo_listing_info":                 "ipo_stock",
#     "get_ipo_objects_of_issue":             "ipo_stock",
#     "get_ipo_anchor_investor_details":      "ipo_stock",
#     "get_ipo_industry_peers":                "ipo_stock",
#     "get_ipo_financials":                   "ipo_stock",
#     "get_ipo_product_services":             "ipo_stock",
#     "get_ipo_strength_details":             "ipo_stock",
#     "get_ipo_strategy_details":             "ipo_stock",
#     "get_ipo_risk_details":                 "ipo_stock",
#     "get_ipo_selling_shareholders":         "ipo_stock",
#     "get_ipo_allocation_details":           "ipo_stock",
#     "get_ipo_prospectus":                   "ipo_stock",
#     "get_ipo_lead_managers":                "ipo_stock",
#     "get_ipo_registrar":                    "ipo_stock",
#     "get_ipo_customer_details":              "ipo_stock",
#     "get_market_indices":                   "stock",   
#     "get_index_companies":                  "index",
#     "get_active_performer":                 "market",
#     "get_top_gainers":                      "market",
#     "get_top_losers":                       "market",
#     "get_out_under_performers":                   "market",
#     "get_52week_highs":                           "market",
#     "get_52week_lows":                      "market",
#     "get_new_highs_lows":                        "market",
#     "get_sector_companies":                 "market",
#     "get_advance_decline":                  "exchange",
#     "get_exchange_holidays":                "exchange",
#     "get_scheme_nav":                       "mf_scheme",
#     "get_investment_details":                "mf_scheme",
#     "get_expense_ratio":                    "mf_scheme",
#     "get_avg_maturity":                     "mf_scheme",
#     "get_scheme_aum":                       "mf_scheme",
#     "get_nav_historical":                   "mf_scheme",
#     "get_scheme_returns":                   "mf_scheme",
#     "get_lumpsum_returns":                  "mf_scheme",
#     "get_scheme_sip":                       "mf_scheme",
#     "get_mf_holdings":                      "mf_scheme",
#     "get_sector_allocation":                "mf_scheme",
#     "get_asset_allocation":                 "mf_scheme",
#     "get_portfolio_changes":                "mf_scheme",
#     "get_mcap_allocation":                  "mf_scheme",
#     "get_most_bought_sold":                  "mf_scheme",
#     "get_scheme_ratios":                    "mf_scheme",
#     "get_dividend_details":                 "mf_scheme",
#     "get_bse_star_scheme":                  "mf_scheme",
#     "compare_schemes":                      "mf_scheme",
#     "get_whats_in_out":                     "mf_scheme",
#     "get_scheme_sip_rules":                 "mf_scheme",
#     "get_scheme_sip_details":               "mf_scheme",
#     "get_fund_categories":                  "mf_amc",
#     "get_schemes_by_amc":                   "mf_amc",
#     "get_fund_profile":                     "mf_amc",
#     "get_fund_managers":                     "mf_amc",
#     "get_etf_quotes":                       "etf",
#     "get_etf_returns":                      "etf",
#     "get_etf_fundamentals":                 "etf",
#     "get_etf_about":                        "etf",
#     "get_etf_equity_holdings":              "etf",
#     "get_etf_monthly_portfolio":               "etf",
#     "get_etf_sector_allocatio":             "etf",
#     "get_etf_asset_allocation":             "etf",
#     "get_debt_eod_prices_scripwise":        "bond",
#     "get_bond_details":                     "bond",
#     "get_bond_price_history":               "bond",
#     "get_bond_cashflow":                    "bond",
#     "get_bond_rating":                      "bond",
#     "get_bond_redemption":                  "bond",
#     "get_bond_interest":                    "bond",
#     "get_forthcoming_ipos":                  "ipo_list",
#     "get_open_ipos":                        "ipo_list",
#     "get_closed_ipos":                      "ipo_list",
#     "get_new_ipo_listings":                          "ipo_list",
#     "get_best_ipo_performers":                         "ipo_list",
#     "get_ipo_master":                       "ipo_list",
#     "get_ipo_logo":                         "ipo_list",
#     "get_forthcoming_drh_filings":           "ipo_list",
#     "get_basis_of_allotment" :               "ipo_list",
#     "get_open_bond_ipo":                      "ipo_list",
#     "get_forthcoming_bond_ipo" :               "ipo_list",
#     "get_new_fund_offer":                   "nfo",
#     "get_corporate_news":                   "news",
#     "get_mf_news":                          "news",
#     "get_mf_market_activity":                     "news",
#     "get_bse_announcements":                 "announcement",
#     "get_nse_announcements":                 "announcement",
#     "get_fund_house":                       "market_info",
#     "get_amfi_master":                      "market_info",
#     "get_fund_manager":                     "market_info",
#     "get_index_list":                       "market_info",
#     "get_results_today":                    "market_info",
#     "get_result_declarations":              "market_info",
#     "get_annual_declarations":              "market_info",
#     "get_fund_performance":                  "market_info",
#     "get_category_performance":              "market_info",
#     "get_sip_dates":                        "market_info",
#     "get_macro_economic_data":              "market_info",
#     "get_forthcoming_bond_ipo":             "market_info",
#     "get_open_bond_ipo":                    "market_info",
#     "get_debt_top_value":                   "market_info",
#     "get_debt_top_volume":                  "market_info",
#     "et_new_fund_offers" :                  "market_info",
#     "get_debt_market_watch":                "market_watch",
    
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
#     "co_code": {
#         "table":    "companies",
#         "column":   "companyname",
#         "id_field": "co_code",
#     },
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
#     "group": {
#         "table":    "group_master",
#         "column":   "group_name",
#         "id_field": "group_name",
#     },
#     "bond_code": {
#         "table":    "bond_master",
#         "column":   "companyname",
#         "id_field": "code",
#     },
# }

# ENTITY_TYPE_PARAM_MAP: dict[str, list[str]] = {
#     "stock":        ["co_code"],
#     "ipo_stock":    ["co_code"],    
#     "mf_scheme":    ["mf_schcode"],
#     "mf_amc":       ["mf_cocode"],
#     "etf":          ["isin"],
#     "index":        ["index_code"],
#     "market":       ["group"],
#     "bond":         ["bond_code"],
#     "exchange":     [],
#     "ipo_list":     [],
#     "nfo":          [],
#     "news":         [],
#     "announcement": [],
#     "market_info":  [],
#     "general":      [],
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

#     matched_tools:  list[dict]
#     intents:        list[IntentItem]
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


# # ── LLM Prompt Templates ──────────────────────────────────────────────────────

# CLASSIFY_EXTRACT_DECOMPOSE_PROMPT = """\
# You are a Lead Financial Systems Analyst for an Indian stock-market assistant. Your task is to decompose user queries into executable data intents.

# ## Context
# - History: {history}
# - Today's Date: {today}
# - Query: "{query}"

# ────────────────────────────────────────────────────────────
# STEP 1: SUBJECT IDENTIFICATION
# Identify every distinct subject the user is asking about. A subject is either:
# 1. A Specific Entity: (e.g., Reliance Industries, HDFC AMC, Nifty 50, SBI Small Cap Fund, Gold BeES ETF, HDFC NCD bond).
# 2. A Market Category/Concept: (e.g., "upcoming IPOs", "top gainers", "market news", "bulk deals", "top losers on NSE", "debt market watch").

# STEP 2: JSON GENERATION
# Generate exactly one intent item for every subject identified in Step 1.

# Return ONLY valid JSON:
# {{
#   "query_type": "<greeting|general|stock|mf_scheme|mf_amc|bond|comparison|investment>",
#   "is_broad": <true|false>,
#   "mcp_needed": <true|false>,
#   "intents": [
#     {{
#       "entity": "<entity name OR market concept name>",
#       "entity_type": "<see ENTITY TYPE RULES below>",
#       "intent_description": "<specific data point needed for this subject>",
#       "scheme_name": "<if mf_scheme, else empty>",
#       "amc_name": "<if mf_amc, else empty>",
#       "nse_symbol": "<NSE ticker if known, else empty>"
#     }}
#   ]
# }}

# ────────────────────────────────────────────────────────────
# ENTITY TYPE RULES:
#   stock        → Specific listed company's EQUITY data.
#   ipo_stock    → Company-SPECIFIC IPO data where you know the company name.
#   ipo_list     → Generic/market-wide IPO queries with NO specific company name.
#   mf_scheme    → Specific named mutual fund SCHEME.
#   mf_amc       → A mutual fund HOUSE or AMC.
#   etf          → An exchange-traded fund.
#   index        → A market INDEX constituents/membership list (e.g., Nifty IT, Nifty Defence).
#   market       → Market-WIDE movers or screeners (e.g., top gainers, 52-week highs).
#   bond         → Specific named listed BOND, DEBENTURE, or NCD.
#   exchange     → EXCHANGE-LEVEL aggregates (e.g., advance-decline ratio, trading holidays).
#   nfo          → New Fund Offers.
#   news         → Corporate or mutual fund news feeds.
#   announcement → Formal regulatory disclosures on BSE/NSE.
#   market_info  → Broad analytics (macro data, index levels list, result calendars).
#   general      → Educational or definitional conceptual tracking only.
# """

# GREETING_PROMPT = """\
# You are EQUIFIZ — a friendly Indian stock-market AI assistant.
# Respond warmly and briefly to the user's greeting or small talk.

# User: {query}
# """

# MCP_SYNTHESIS_PROMPT = """\
# You are a knowledgeable Indian equity, mutual fund, and fixed-income analyst assistant.

# ## Contextual Grounding Rules (CRITICAL)
# - You MUST rely strictly on the canonical records tagged inside the "Live Data Retrieved" block.
# - NEVER mix up records between distinct data segments. For instance, if data for NIFTY IT contains a specific list of stocks, do NOT mention or inject them into descriptions of the NIFTY DEFENCE segment.
# - If the live data returned for a requested entity is completely empty, zeroed, or displays database lookup indicators like "[]" or None, you must explicitly state that no data was returned for that entity, rather than guessing or filling it using historical knowledge.

# ## Data Safety & Quality Guardrails (STRICT)
# - NEVER expose internal database identifiers or structural system keys (such as "co_code", "mf_schcode").
# - Check the parsed data for structural backend issues. If it indicates server configuration warnings or system failures, explain to the user that the information is currently inaccessible.

# ## Conversation history (last 4 turns)
# {history}

# ## User Question
# {user_query}

# ## Live Data Retrieved
# {mcp_result}

# ## Instructions
# 1. Answer EACH entity component sequentially using only the explicitly labeled live data block provided for it.
# 2. If data is missing or marked as an error, briefly state that it cannot be verified right now.
# 3. Write in plain prose paragraphs only. No markdown, no bullet points, no asterisks, no tables. Do NOT use the rupee symbol — write "Rs".
# 4. Use Indian number formatting (lakh, crore) for big values.
# 5. Keep under 300 words.
# 6. End with: This is not financial advice.
# """

# GENERAL_SYNTHESIS_PROMPT = """\
# You are a knowledgeable Indian equity and fixed-income analyst assistant.

# ## Conversation history (last 4 turns)
# {history}

# ## User Question
# {user_query}

# ## Knowledge Base Context
# {vector_context}

# ## Instructions
# 1. Answer clearly and concisely.
# 2. Plain prose only — no markdown. Do NOT use the rupee symbol — write "Rs".
# 3. Under 200 words.
# """


# # ── LLM helpers ───────────────────────────────────────────────────────────────

# def _llm_json(prompt: str) -> dict:
#     resp  = llm.invoke([HumanMessage(content=prompt)])
#     text  = resp.content.strip()
#     clean = re.sub(r"^`{3}(?:json)?|`{3}$", "", text, flags=re.MULTILINE).strip()

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
# # Node AIQ: Classify + Extract + Decompose
# # ══════════════════════════════════════════════════════════════════════════════

# def node_classify_extract_and_decompose(state: AgentState) -> AgentState:
#     console.print("[Node AIQ] Classify + extract + decompose (v11.5-Hardened)")

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
#                 resolved_name      = None,  
#                 tool_hint          = "",
#                 matched_tools      = [],
#                 resolved_codes      = {},
#                 mcp_result         = "",
#                 code_missing       = False,
#             ))

#         if not intents and qt not in ("greeting",):
#             console.print("  ⚠ LLM returned no intents — fallback single general intent")
#             intents.append(IntentItem(
#                 entity             = user_query,
#                 entity_type        = "general",
#                 intent_description = user_query,
#                 scheme_name        = None,
#                 amc_name           = None,
#                 nse_symbol         = None,
#                 resolved_name      = None,
#                 tool_hint          = "",
#                 matched_tools      = [],
#                 resolved_codes      = {},
#                 mcp_result         = "",
#                 code_missing       = False,
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
#                 "description":          meta.get("description", ""),
#                 "required_parameters": req_params,
#                 "parameters":           meta.get("parameters", ""),
#                 "score":                round(1 - dist, 3),
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
#                 "description":          meta.get("description", ""),
#                 "required_parameters": req_params,
#                 "parameters":           meta.get("parameters", ""),
#             }
#     except Exception as e:
#         console.print(f"  [ToolRegistry] get by id '{tool_name}' failed: {e}")
#     return None


# # ══════════════════════════════════════════════════════════════════════════════
# # Node TVS: Per-intent tool vector search  (v11.5)
# # ══════════════════════════════════════════════════════════════════════════════

# _ALLOWED_FAMILIES: dict[str, frozenset[str]] = {
#     "stock":        frozenset({"stock"}),
#     "ipo_stock":    frozenset({"ipo_stock"}),
#     "ipo_list":     frozenset({"ipo_list"}),
#     "mf_scheme":    frozenset({"mf_scheme"}),
#     "mf_amc":       frozenset({"mf_amc"}),
#     "etf":          frozenset({"etf"}),
#     "index":        frozenset({"index"}),
#     "market":       frozenset({"market"}),
#     "bond":         frozenset({"bond"}),
#     "exchange":     frozenset(),
#     "nfo":          frozenset(),
#     "news":         frozenset(),
#     "announcement": frozenset(),
#     "market_info":  frozenset(),
#     "general":      frozenset(),
# }


# def node_per_intent_tool_search(state: AgentState) -> AgentState:
#     console.print("[Node TVS] Per-intent tool vector search (v11.5)")

#     intents = state.get("intents") or []
#     if not intents:
#         console.print("  ⚠ No intents to search for")
#         return state

#     def _search_for_intent(intent: IntentItem) -> IntentItem:
#         et = intent.get("entity_type", "general")

#         if et == "general":
#             console.print(
#                 f"  [{intent['entity']}] entity_type=general → skip tool search"
#             )
#             return IntentItem(
#                 entity             = intent.get("entity", ""),
#                 entity_type        = "general",
#                 intent_description = intent.get("intent_description", ""),
#                 scheme_name        = intent.get("scheme_name"),
#                 amc_name           = intent.get("amc_name"),
#                 nse_symbol         = intent.get("nse_symbol"),
#                 resolved_name      = None,
#                 tool_hint          = "",
#                 matched_tools      = [],
#                 resolved_codes      = {},
#                 mcp_result         = "",
#                 code_missing       = False,
#             )

#         if et in _NO_DB_MCP_TYPES:
#             search_query = intent["intent_description"]
#         else:
#             search_query = f"{et} {intent['intent_description']}"

#         tools = _query_tool_registry(search_query, n_results=10)

#         allowed = _ALLOWED_FAMILIES.get(et, frozenset())
#         if allowed:
#             filtered = [
#                 t for t in tools
#                 if _infer_entity_type_from_tool(t["tool_name"]) in allowed
#             ]
#             if filtered:
#                 tools = filtered
#                 console.print(
#                     f"  [TVS] family filter '{et}' → kept {len(tools)} tools"
#                 )
#             else:
#                 console.print(
#                     f"  [TVS] ⚠ family filter '{et}' eliminated all tools — keeping unfiltered results"
#                 )

#         tool_hint = tools[0]["tool_name"] if tools else ""

#         if tool_hint:
#             console.print(
#                 f"  [{intent['entity']}] intent='{intent['intent_description'][:50]}' → top tool={tool_hint}"
#             )
#         return IntentItem(
#             entity             = intent.get("entity", ""),
#             entity_type        = et,
#             intent_description = intent.get("intent_description", ""),
#             scheme_name        = intent.get("scheme_name"),
#             amc_name           = intent.get("amc_name"),
#             nse_symbol         = intent.get("nse_symbol"),
#             resolved_name      = None,
#             tool_hint          = tool_hint,
#             matched_tools      = tools,
#             resolved_codes      = {},
#             mcp_result         = "",
#             code_missing       = False,
#         )

#     updated: dict[int, IntentItem] = {}
#     with ThreadPoolExecutor(max_workers=min(6, len(intents))) as ex:
#         futures = {ex.submit(_search_for_intent, intent): idx for idx, intent in enumerate(intents)}
#         for future in as_completed(futures):
#             idx = futures[future]
#             try:
#                 updated[idx] = future.result()
#             except Exception as exc:
#                 console.print(f"  [red]TVS failed for intent #{idx}: {exc}[/red]")
#                 original     = intents[idx]
#                 updated[idx] = IntentItem(
#                     entity             = original.get("entity", ""),
#                     entity_type        = original.get("entity_type", "general"),
#                     intent_description = original.get("intent_description", ""),
#                     scheme_name        = original.get("scheme_name"),
#                     amc_name           = original.get("amc_name"),
#                     nse_symbol         = original.get("nse_symbol"),
#                     resolved_name      = None,
#                     tool_hint          = "",
#                     matched_tools      = [],
#                     resolved_codes      = {},
#                     mcp_result         = "",
#                     code_missing       = False,
#                 )

#     state["intents"] = [updated[i] for i in sorted(updated)]

#     if state["intents"]:
#         first = state["intents"][0]
#         state["matched_tools"]     = first.get("matched_tools") or []
#         state["mcp_tool_hint"]     = first.get("tool_hint") or ""
#         state["primary_tool_hint"] = first.get("tool_hint") or ""

#     return state


# # ── Greeting handler ──────────────────────────────────────────────────────────

# def node_greeting_handler(state: AgentState) -> AgentState:
#     console.print("[Node B] Greeting handler")
#     state["final_answer"] = _llm_text(GREETING_PROMPT.format(query=state["user_query"]))
#     return state


# # ── General handler (vector search fallback) ──────────────────────────────────

# def node_general_handler(state: AgentState) -> AgentState:
#     console.print("[Node C] General handler (vector search fallback)")
#     try:
#         state["vector_context"] = vs.query(state["user_query"], n_results=5)
#     except Exception as e:
#         console.print(f"  General vector search failed: {e}")
#         state["vector_context"] = []
#     return state


# # ══════════════════════════════════════════════════════════════════════════════
# # FIX M — Hardened Multi-Tiered Fuzzy Match Scoring System
# # ══════════════════════════════════════════════════════════════════════════════

# def _compute_smart_score(target: str, candidate: str) -> float:
#     target_clean = target.lower().strip()
#     candidate_clean = candidate.lower().strip()
    
#     if target_clean == candidate_clean:
#         return 100.0
        
#     ts_ratio = fuzz.token_sort_ratio(target_clean, candidate_clean)
#     ratio = fuzz.ratio(target_clean, candidate_clean)
#     part_ratio = fuzz.partial_ratio(target_clean, candidate_clean)
    
#     target_words = set(target_clean.split())
#     candidate_words = set(candidate_clean.split())
    
#     has_exact_word_match = any(word in candidate_words for word in target_words)
#     score = max(ts_ratio, ratio)
    
#     if part_ratio > 90.0:
#         if has_exact_word_match:
#             score = max(score, part_ratio)
#         else:
#             score = max(score, part_ratio - 35.0)
#     else:
#         score = max(score, part_ratio)
        
#     return float(score)


# def _resolve_single(name: str, nse_symbol: Optional[str]) -> dict:
#     name_upper = name.upper().strip()
#     symbol_to_check = nse_symbol.upper().strip() if nse_symbol else name_upper
    
#     if symbol_to_check:
#         row = db.lookup_by_nse_symbol(symbol_to_check)
#         if row:
#             return {
#                 "co_code":      row["co_code"],
#                 "company_info": row,
#                 "nse_symbol":   row.get("nsesymbol"),
#                 "resolved_name": row.get("companyname"),
#             }

#     matches = db.fuzzy_search_company(name, limit=15)
#     if matches:
#         for cand in matches:
#             cand_sym = str(cand.get("nsesymbol") or "").upper().strip()
#             if cand_sym == name_upper:
#                 return {
#                     "co_code":      cand["co_code"],
#                     "company_info": cand,
#                     "nse_symbol":   cand_sym,
#                     "resolved_name": cand.get("companyname"),
#                 }

#         best_match = None
#         best_score = -1.0
        
#         for cand in matches:
#             cand_name = cand.get("companyname") or ""
#             cand_sym  = cand.get("nsesymbol") or ""
            
#             score_name = _compute_smart_score(name, cand_name)
#             score_sym  = _compute_smart_score(name, cand_sym)
#             score = max(score_name, score_sym)
            
#             if score > best_score:
#                 best_score = score
#                 best_match = cand
                
#         if best_match and best_score >= 50.0:
#             return {
#                 "co_code":      best_match["co_code"],
#                 "company_info": best_match,
#                 "nse_symbol":   best_match.get("nsesymbol"),
#                 "resolved_name": best_match.get("companyname"),
#             }
            
#     return {}


# # ══════════════════════════════════════════════════════════════════════════════
# # DB targeted table lookup
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

#         if table == "scheme_master":
#             cur.execute("SELECT mf_schcode, mf_cocode, sch_name, category FROM scheme_master")
#         elif table == "bond_master":
#             cur.execute("SELECT code, companyname, isin, nsesymbol FROM bond_master")
#         else:
#             cur.execute(f"SELECT {id_col}, {name_col} FROM {table}")

#         rows = [dict(r) for r in cur.fetchall()]
#         cur.close()
#         conn.close()

#         best_match: Optional[dict] = None
#         best_score: float          = 0.0

#         for row in rows:
#             candidate = str(row.get(name_col) or "")
#             score = _compute_smart_score(entity, candidate)
            
#             if table == "bond_master":
#                 isin_score = fuzz.ratio(entity.upper(), str(row.get("isin") or "").upper())
#                 sym_score = fuzz.ratio(entity.upper(), str(row.get("nsesymbol") or "").upper())
#                 score = max(score, isin_score, sym_score)

#             if score > best_score:
#                 best_score, best_match = score, row

#         if best_match and best_score >= 60.0:
#             return best_match
#         return None

#     except Exception as e:
#         console.print(f"    [_targeted_db_lookup] DB error on {table}: {e}")
#         return None


# def _get_required_params_for_entity(
#     tool_hint:     str,
#     entity_type:   str,
#     matched_tools: list[dict],
# ) -> list[str]:
#     params = ENTITY_TYPE_PARAM_MAP.get(entity_type)
#     if params is not None:
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

#     return []


# # ── Per-intent DB code resolver ──────────────────────────────────────────────

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
#     needs_group     = "group"       in required_params
#     needs_bond_code = "bond_code"   in required_params

#     if needs_schcode:
#         search = scheme_name or name
#         cfg    = PARAM_TO_TABLE_MAP["mf_schcode"]
#         match  = _targeted_db_lookup(entity=search, table=cfg["table"], name_col=cfg["column"], id_col=cfg["id_field"])
#         if match:
#             codes["mf_schcode"] = int(match["mf_schcode"])
#             if "mf_cocode" in match:
#                 codes.setdefault("mf_cocode", int(match["mf_cocode"]))
#             result["resolved_scheme_name"] = match.get("sch_name", search)

#     if needs_cocode and not codes.get("mf_cocode"):
#         search = amc_name or name
#         cfg    = PARAM_TO_TABLE_MAP["mf_cocode"]
#         match  = _targeted_db_lookup(entity=search, table=cfg["table"], name_col=cfg["column"], id_col=cfg["id_field"])
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
#                 "resolved_company_name": stock["resolved_name"]
#             })
#         else:
#             result["code_missing"] = True

#     if needs_isin:
#         search = scheme_name or name
#         cfg    = PARAM_TO_TABLE_MAP["isin"]
#         match  = _targeted_db_lookup(entity=search, table=cfg["table"], name_col=cfg["column"], id_col=cfg["id_field"])
#         if match:
#             codes["isin"] = match["isin"]
#             result["resolved_etf_name"] = match.get("etfname", search)

#     if needs_indexcode:
#         cfg   = PARAM_TO_TABLE_MAP["index_code"]
#         match = _targeted_db_lookup(entity=name, table=cfg["table"], name_col=cfg["column"], id_col=cfg["id_field"])
#         if match:
#             codes["index_code"] = int(match["indexcode"])
#             result["resolved_index_name"] = match.get("group_name", name)

#     if needs_group:
#         cfg   = PARAM_TO_TABLE_MAP["group"]
#         match = _targeted_db_lookup(entity=name, table=cfg["table"], name_col=cfg["column"], id_col=cfg["id_field"])
#         if match:
#             codes["group"] = match["group_name"]
#             result["resolved_group_name"] = match["group_name"]
#         else:
#             codes["group"] = name
#             result["resolved_group_name"] = name

#     if needs_bond_code:
#         cfg   = PARAM_TO_TABLE_MAP["bond_code"]
#         match = _targeted_db_lookup(entity=name, table=cfg["table"], name_col=cfg["column"], id_col=cfg["id_field"])
#         if match:
#             codes["bond_code"] = int(match["code"])
#             result["resolved_bond_name"] = match.get("companyname", name)
#         else:
#             result["code_missing"] = True

#     result["mcp_resolved_codes"] = codes
#     return result


# def _resolve_intents_codes(intents: list[IntentItem]) -> list[IntentItem]:
#     def _resolve_one(intent: IntentItem) -> IntentItem:
#         et        = intent.get("entity_type", "general")
#         tool_hint = intent.get("tool_hint", "")
#         matched   = intent.get("matched_tools") or []

#         if et == "general" and tool_hint:
#             inferred = _infer_entity_type_from_tool(tool_hint)
#             if inferred != "general":
#                 et = inferred
#                 intent["entity_type"] = et

#         req_params = _get_required_params_for_entity(tool_hint, et, matched)

#         if not req_params:
#             intent["resolved_codes"] = {}
#             intent["code_missing"]   = False
#             intent["resolved_name"]  = intent.get("entity")
#             return intent

#         resolved = _resolve_entity_codes(
#             name            = intent.get("entity", ""),
#             scheme_name     = intent.get("scheme_name"),
#             amc_name        = intent.get("amc_name"),
#             nse_symbol      = intent.get("nse_symbol"),
#             required_params = req_params,
#         )
#         intent["resolved_codes"] = resolved.get("mcp_resolved_codes", {})
#         intent["code_missing"]   = bool(resolved.get("code_missing", False))
        
#         intent["resolved_name"] = (
#             resolved.get("resolved_company_name") or
#             resolved.get("resolved_scheme_name") or
#             resolved.get("resolved_amc_name") or
#             resolved.get("resolved_etf_name") or
#             resolved.get("resolved_index_name") or
#             resolved.get("resolved_bond_name") or
#             intent.get("entity")
#         )
#         return intent

#     result_intents: dict[int, IntentItem] = {}
#     with ThreadPoolExecutor(max_workers=min(6, len(intents) or 1)) as ex:
#         futures = {ex.submit(_resolve_one, intent): idx for idx, intent in enumerate(intents)}
#         for future in as_completed(futures):
#             idx = futures[future]
#             try:
#                 result_intents[idx] = future.result()
#             except Exception:
#                 result_intents[idx] = intents[idx]

#     return [result_intents[i] for i in sorted(result_intents)]


# # ══════════════════════════════════════════════════════════════════════════════
# # MCP Pre-Resolution nodes
# # ══════════════════════════════════════════════════════════════════════════════

# def node_mcp_pre_resolve(state: AgentState) -> AgentState:
#     console.print("[Node MPR] Single-entity MCP pre-resolve (v11.5)")
#     intents = state.get("intents") or []
#     if not intents:
#         state["mcp_resolved_codes"] = {}
#         return state

#     resolved_intents            = _resolve_intents_codes(intents)
#     state["intents"]            = resolved_intents
#     state["mcp_resolved_codes"] = resolved_intents[0].get("resolved_codes", {}) if resolved_intents else {}
#     return state


# def node_mcp_multi_pre_resolve(state: AgentState) -> AgentState:
#     console.print("[Node MMPR] Multi-entity MCP pre-resolve (v11.5)")
#     intents = state.get("intents") or []
#     if not intents:
#         return state

#     resolved_intents            = _resolve_intents_codes(intents)
#     state["intents"]            = resolved_intents
#     state["mcp_resolved_codes"] = resolved_intents[0].get("resolved_codes", {}) if resolved_intents else {}
#     return state


# # ══════════════════════════════════════════════════════════════════════════════
# # Node MTCI: MCP Tool Call — sequential per-intent in one session (v11.5)
# # ══════════════════════════════════════════════════════════════════════════════

# def _run_mcp_multi_in_thread(intents: list[dict]) -> list[dict]:
#     from mcp_client import run_mcp_query_multi, trim_results
#     loop = asyncio.new_event_loop()
#     asyncio.set_event_loop(loop)
#     try:
#         results = loop.run_until_complete(run_mcp_query_multi(intents))
#         return trim_results(results)
#     except Exception as e:
#         console.print(f"[bold red]MCP Multi Error:[/bold red] {e}")
#         return [{**i, "mcp_result": f"Error: {e}"} for i in intents]
#     finally:
#         loop.close()


# def node_mcp_tool_call_intents(state: AgentState) -> AgentState:
#     console.print("[Node MTCI] Sequential per-intent MCP calls — one session (v11.5)")

#     try:
#         import mcp_client  # noqa: F401
#     except ImportError:
#         state["mcp_raw_result"] = ""
#         state["intent_results"] = []
#         state["error"]          = "mcp_client_not_found"
#         return state

#     intents    = state.get("intents") or []
#     user_query = state.get("user_query", "")

#     mcp_intents: list[IntentItem]     = []
#     blocked_intents: list[IntentItem] = []
#     general_intents: list[IntentItem] = []

#     for i in intents:
#         et           = i.get("entity_type", "general")
#         has_tool     = bool(i.get("tool_hint"))
#         code_missing = bool(i.get("code_missing", False))

#         if et == "general" and not has_tool:
#             general_intents.append(i)
#         elif code_missing:
#             blocked_intents.append(i)
#         else:
#             mcp_intents.append(i)

#     if not mcp_intents:
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

#     try:
#         with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
#             future         = ex.submit(_run_mcp_multi_in_thread, mcp_intents)
#             filled_intents = future.result(timeout=600)
#     except Exception as exc:
#         filled_intents = [{**dict(i), "mcp_result": f"MCP call failed: {exc}"} for i in mcp_intents]

#     all_intents = (
#         filled_intents
#         + [{**dict(i), "mcp_result": "(answered from knowledge base)"} for i in general_intents]
#         + [{**dict(i), "mcp_result": f"Sorry, I could not find '{i.get('entity')}' in our database."} for i in blocked_intents]
#     )
#     state["intents"] = all_intents

#     state["intent_results"] = [
#         {
#             "entity":             i.get("entity", ""),
#             "resolved_name":      i.get("resolved_name", i.get("entity", "")),
#             "entity_type":        i.get("entity_type", ""),
#             "intent_description": i.get("intent_description", ""),
#             "mcp_result":         i.get("mcp_result", ""),
#         }
#         for i in all_intents
#     ]

#     # Clean raw outcomes from underlying HTML pollution or proxy timeout blocks
#     valid_segments = []
#     for r in state["intent_results"]:
#         res_str = str(r.get("mcp_result", ""))
        
#         # Rigorous Upstream Infrastructure Anomaly Scrubbing Rule (Stops 502/HTML leaks)
#         if any(indicator in res_str.lower() for indicator in ["<html", "502 bad gateway", "nginx/", "404 not found", "error response"]):
#             res_str = "Error: Upstream market analytics interface returned an invalid protocol wrapper. The server is busy. Please rephrase shortly."
#             state["error"] = "upstream_infrastructure_leak"
            
#         valid_segments.append(
#             f"--- RESOLVED ENTITY CANONICAL DATA FOR: {r.get('resolved_name', r['entity']).upper()} ---\n"
#             f"[Requested Segment: {r['intent_description']}]\n"
#             f"{res_str}"
#         )

#     state["mcp_raw_result"] = "\n\n---\n\n".join(valid_segments)
#     state["mcp_tool_calls_made"] = []
#     return state


# _MCP_ERROR_MESSAGES = {
#     "mcp_client_not_found": "Sorry, the live data service is currently unavailable. Please try again shortly.",
#     "mcp_call_timeout": "The live data request timed out. The server may be busy — please try again in a moment.",
#     "upstream_infrastructure_leak": "The data connection to the market stream encountered an operational disruption. I could not verify the requested information right now. Please rephrase shortly."
# }

# # ══════════════════════════════════════════════════════════════════════════════
# # MCP Synthesis (v11.5)
# # ══════════════════════════════════════════════════════════════════════════════

# def node_mcp_synthesis(state: AgentState) -> AgentState:
#     console.print("[Node MS] MCP synthesis (v11.5)")
#     error      = state.get("error", "")
#     mcp_result = state.get("mcp_raw_result", "")

#     if error in _MCP_ERROR_MESSAGES:
#         state["final_answer"] = _MCP_ERROR_MESSAGES[error]
#         return state
        
#     if not mcp_result:
#         state["final_answer"] = "No data was returned from the live server. Please try again shortly."
#         return state

#     mcp_snippet = mcp_result
#     if len(mcp_snippet) > 10000:
#         mcp_snippet = mcp_snippet[:7500] + "\n\n... [middle trimmed for length] ...\n\n" + mcp_snippet[-2000:]

#     prompt = MCP_SYNTHESIS_PROMPT.format(
#         history    = _format_history(state),
#         user_query = state.get("user_query", ""),
#         mcp_result = mcp_snippet,
#     )
#     try:
#         state["final_answer"] = _llm_text(prompt)
#     except Exception as e:
#         state["final_answer"] = mcp_result
#     return state


# def node_synthesis(state: AgentState) -> AgentState:
#     console.print("[Node S] General synthesis (vector search)")
#     vector_context = state.get("vector_context") or []
#     context_text   = "\n".join(f"- [{r['metadata'].get('api_name', '')}] {r['document']}" for r in vector_context)
#     prompt = GENERAL_SYNTHESIS_PROMPT.format(
#         history        = _format_history(state),
#         user_query     = state["user_query"],
#         vector_context = context_text or "No relevant context found.",
#     )
#     try:
#         state["final_answer"] = _llm_text(prompt)
#     except Exception as e:
#         state["final_answer"] = f"Synthesis failed: {e}"
#     return state


# # ══════════════════════════════════════════════════════════════════════════════
# # Router (v11.5)
# # ══════════════════════════════════════════════════════════════════════════════

# def route_after_classify(state: AgentState) -> str:
#     qt      = state.get("query_type", "general")
#     intents = state.get("intents") or []

#     if qt == "greeting":
#         return "greeting"

#     mcp_intents = [i for i in intents if i.get("tool_hint") or i.get("entity_type") not in ("general",)]

#     if not mcp_intents or (not any(i.get("tool_hint") for i in intents) and all(i.get("entity_type") == "general" for i in intents)):
#         return "general"

#     has_many         = len(intents) > 1
#     additional       = state.get("companies") or []
#     primary_type     = state.get("primary_entity_type", "general")
#     secondary_types  = {c.get("entity_type", "general") for c in additional}
#     is_heterogeneous = bool(secondary_types - {"general"} - {primary_type})

#     if has_many or qt == "comparison" or is_heterogeneous:
#         return "multi_mcp"

#     return "mcp_direct"


# # ══════════════════════════════════════════════════════════════════════════════
# # Graph (v11.5)
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


# _graph = None

# def get_graph():
#     global _graph
#     if _graph is None:
#         _graph = build_graph()
#     return _graph


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



# from __future__ import annotations

# import asyncio
# import concurrent.futures
# import json
# import logging
# import re
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

# # ── Types ─────────────────────────────────────────────────────────────────────

# class Turn(TypedDict):
#     role: str       # "user" | "assistant"
#     content: str


# class IntentItem(TypedDict, total=False):
#     entity:             str
#     entity_type:        str
#     intent_description: str
#     tool_hint:          str
#     scheme_name:        Optional[str]
#     amc_name:           Optional[str]
#     nse_symbol:         Optional[str]
#     resolved_name:      Optional[str]  
#     matched_tools:      list[dict]
#     resolved_codes:     dict
#     mcp_result:         str
#     code_missing:       bool           


# _NO_DB_MCP_TYPES = frozenset({
#     "exchange",
#     "ipo_list",
#     "nfo",
#     "news",
#     "announcement",
#     "market_info",
# })

# # ── Tool Family Mapping Matrix ───────────────────────────────────────────────
# _TOOL_FAMILY_HINTS: dict[str, str] = {
#     "get_company_stock_price": "stock",
#     "get_delayed_stock_price": "stock",
#     "get_nse_company_announcements":        "stock",
#     "get_bse_company_announcements":        "stock",
#     "get_company_result_schedule":          "stock",
#     "get_company_profile":                  "stock",
#     "get_company_background":               "stock",
#     "get_board_of_directors":               "stock",
#     "get_company_bankers":                  "stock",
#     "get_management_biodata":               "stock",
#     "get_subsidiaries_jvs":                 "stock",
#     "get_related_party_transactions":       "stock",
#     "get_employee_count":                   "stock",
#     "get_capital_structure":                "stock",
#     "get_pledge_share_details":             "stock",
#     "get_substantial_acquisitions":         "stock",
#     "get_segment_data":                     "stock",
#     "get_r_and_d_expenditure":              "stock",
#     "get_finished_products":                "stock",
#     "get_raw_materials":                    "stock",
#     "get_chronological_history":            "stock",
#     "get_company_history":                  "stock",
#     "get_funds_holding_company":            "stock",
#     "get_quarterly_results":                "stock",
#     "get_profit_loss":                      "stock",
#     "get_balance_sheet":                    "stock",
#     "get_cash_flow":                        "stock",
#     "get_half_yearly_results":              "stock",
#     "get_nine_months_results":              "stock",
#     "get_quarterly_trends" :                 "stock",
#     "get_shareholding_pattern":              "stock",
#     "get_major_shareholders":                "stock",
#     "get_quarterly_balance_sheet" :         "stock",
#     "get_yearly_results":                   "stock",
#     "get_annual_balance_sheet":              "stock",
#     "get_half_yearly_balance_sheet":         "stock",
#     "get_ttm_growth_trends":                 "stock",
#     "get_quarterly_revenue_trends":          "stock",
#     "get_quarterly_ebitda_trends":           "stock",
#     "get_quarterly_ebit_trends":             "stock",
#     "get_growth_data_quarterly":             "stock",
#     "get_growth_data_yearly":                "stock",
#     "get_key_financial_ratios":             "stock",
#     "get_daily_ratios":                     "stock",
#     "get_margin_ratios":                    "stock",
#     "get_valuation_ratios":                 "stock",
#     "get_all_basic_ratios":                 "stock",
#     "get_return_ratios":                    "stock",
#     "get_growth_ratios":                    "stock",
#     "get_performance_ratios":                "stock",
#     "get_efficiency_ratios":                 "stock",
#     "get_cashflow_ratios":                  "stock",
#     "get_liquidity_ratios":                 "stock",
#     "get_solvency_ratios":                  "stock",
#     "get_quarterly_ratios":                 "stock",
#     "get_yearly_ratios":                    "stock",
#     "get_shareholding":                     "stock",
#     "get_major_sharehold":                  "stock",
#     "get_financial_stability_ratios":       "stock",
#     "get_anchor_investor":                  "ipo_stock",
#     "get_ipo_details":                      "ipo_stock",
#     "get_ipo_subscription_status":          "ipo_stock",
#     "get_ipo_synopsis":                     "ipo_stock",
#     "get_ipo_timeline":                     "ipo_stock",
#     "get_ipo_promoter_details":             "ipo_stock",
#     "get_ipo_listing_info":                 "ipo_stock",
#     "get_ipo_objects_of_issue":             "ipo_stock",
#     "get_ipo_anchor_investor_details":      "ipo_stock",
#     "get_ipo_industry_peers":                "ipo_stock",
#     "get_ipo_financials":                   "ipo_stock",
#     "get_ipo_product_services":             "ipo_stock",
#     "get_ipo_strength_details":             "ipo_stock",
#     "get_ipo_strategy_details":             "ipo_stock",
#     "get_ipo_risk_details":                 "ipo_stock",
#     "get_ipo_selling_shareholders":         "ipo_stock",
#     "get_ipo_allocation_details":           "ipo_stock",
#     "get_ipo_prospectus":                   "ipo_stock",
#     "get_ipo_lead_managers":                "ipo_stock",
#     "get_ipo_registrar":                    "ipo_stock",
#     "get_ipo_customer_details":              "ipo_stock",
#     "get_market_indices":                   "stock",   
#     "get_index_companies":                  "index",
#     "get_active_performer":                 "market",
#     "get_top_gainers":                      "market",
#     "get_top_losers":                       "market",
#     "get_out_under_performers":              "market",
#     "get_52week_highs":                      "market",
#     "get_52week_lows":                      "market",
#     "get_new_highs_lows":                   "market",
#     "get_sector_companies":                 "sector",
#     "get_advance_decline":                  "exchange",
#     "get_exchange_holidays":                "exchange",
#     "get_scheme_nav":                       "mf_scheme",
#     "get_investment_details":                "mf_scheme",
#     "get_expense_ratio":                    "mf_scheme",
#     "get_avg_maturity":                     "mf_scheme",
#     "get_scheme_aum":                       "mf_scheme",
#     "get_nav_historical":                   "mf_scheme",
#     "get_scheme_returns":                   "mf_scheme",
#     "get_lumpsum_returns":                  "mf_scheme",
#     "get_scheme_sip":                       "mf_scheme",
#     "get_mf_holdings":                      "mf_scheme",
#     "get_sector_allocation":                "mf_scheme",
#     "get_asset_allocation":                 "mf_scheme",
#     "get_portfolio_changes":                "mf_scheme",
#     "get_mcap_allocation":                  "mf_scheme",
#     "get_most_bought_sold":                  "mf_scheme",
#     "get_scheme_ratios":                    "mf_scheme",
#     "get_dividend_details":                 "mf_scheme",
#     "get_bse_star_scheme":                  "mf_scheme",
#     "compare_schemes":                      "mf_scheme",
#     "get_whats_in_out":                     "mf_scheme",
#     "get_scheme_sip_rules":                 "mf_scheme",
#     "get_scheme_sip_details":               "mf_scheme",
#     "get_fund_categories":                  "mf_amc",
#     "get_schemes_by_amc":                   "mf_amc",
#     "get_fund_profile":                     "mf_amc",
#     "get_fund_managers":                     "mf_amc",
#     "get_etf_quotes":                       "etf",
#     "get_etf_returns":                      "etf",
#     "get_etf_fundamentals":                 "etf",
#     "get_etf_about":                        "etf",
#     "get_etf_equity_holdings":              "etf",
#     "get_etf_monthly_portfolio":               "etf",
#     "get_etf_sector_allocatio":             "etf",
#     "get_etf_asset_allocation":             "etf",
#     "get_debt_eod_prices_scripwise":        "bond",
#     "get_bond_details":                     "bond",
#     "get_bond_price_history":               "bond",
#     "get_bond_cashflow":                    "bond",
#     "get_bond_rating":                      "bond",
#     "get_bond_redemption":                  "bond",
#     "get_bond_interest":                    "bond",
#     "get_forthcoming_ipos":                  "ipo_list",
#     "get_open_ipos":                        "ipo_list",
#     "get_closed_ipos":                      "ipo_list",
#     "get_new_ipo_listings":                          "ipo_list",
#     "get_best_ipo_performers":                         "ipo_list",
#     "get_ipo_master":                       "ipo_list",
#     "get_ipo_logo":                         "ipo_list",
#     "get_forthcoming_drh_filings":           "ipo_list",
#     "get_basis_of_allotment" :               "ipo_list",
#     "get_open_bond_ipo":                      "ipo_list",
#     "get_forthcoming_bond_ipo" :               "ipo_list",
#     "get_new_fund_offer":                   "nfo",
#     "get_corporate_news":                   "news",
#     "get_mf_news":                          "news",
#     "get_mf_market_activity":                     "news",
#     "get_bse_announcements":                 "announcement",
#     "get_nse_announcements":                 "announcement",
#     "get_fund_house":                       "market_info",
#     "get_amfi_master":                      "market_info",
#     "get_fund_manager":                     "market_info",
#     "get_index_list":                       "market_info",
#     "get_results_today":                    "market_info",
#     "get_result_declarations":              "market_info",
#     "get_annual_declarations":              "market_info",
#     "get_fund_performance":                  "market_info",
#     "get_category_performance":              "market_info",
#     "get_sip_dates":                        "market_info",
#     "get_macro_economic_data":              "market_info",
#     "get_forthcoming_bond_ipo":             "market_info",
#     "get_open_bond_ipo":                    "market_info",
#     "get_debt_top_value":                   "market_info",
#     "get_debt_top_volume":                  "market_info",
#     "et_new_fund_offers" :                  "market_info",
#     "get_debt_market_watch":                "market_watch",
#     "get_index_constituents":               "index"
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
#     "co_code": {
#         "table":    "companies",
#         "column":   "companyname",
#         "id_field": "co_code",
#     },
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
#     "group": {
#         "table":    "group_master",
#         "column":   "group_name",
#         "id_field": "group_name",
#     },
#     "bond_code": {
#         "table":    "bond_master",
#         "column":   "companyname",
#         "id_field": "code",
#     },
#     "sector_code": {
#         "table":"comppanies",
#         "column" : "sectornmae",
#         "id_field": "sectorcode"
#     }
# }

# ENTITY_TYPE_PARAM_MAP: dict[str, list[str]] = {
#     "stock":        ["co_code"],
#     "ipo_stock":    ["co_code"],    
#     "mf_scheme":    ["mf_schcode"],
#     "mf_amc":       ["mf_cocode"],
#     "etf":          ["isin"],
#     "index":        ["index_code"],
#     "market":       ["group"],
#     "bond":         ["bond_code"],
#     "sector":       ["sector_code"],
#     "exchange":     [],
#     "ipo_list":     [],
#     "nfo":          [],
#     "news":         [],
#     "announcement": [],
#     "market_info":  [],
#     "general":      [],
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

#     matched_tools:  list[dict]
#     intents:        list[IntentItem]
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


# # ── LLM Prompt Templates ──────────────────────────────────────────────────────

# CLASSIFY_EXTRACT_DECOMPOSE_PROMPT = """\
# You are a Lead Financial Systems Analyst for an Indian stock-market assistant. Your task is to decompose user queries into executable data intents.

# ## Context
# - History: {history}
# - Today's Date: {today}
# - Query: "{query}"

# ────────────────────────────────────────────────────────────
# STEP 1: SUBJECT IDENTIFICATION
# Identify every distinct subject the user is asking about. A subject is either:
# 1. A Specific Entity: (e.g., Reliance Industries, HDFC AMC, Nifty 50, SBI Small Cap Fund, Gold BeES ETF, HDFC NCD bond).
# 2. A Market Category/Concept: (e.g., "upcoming IPOs", "top gainers", "market news", "bulk deals", "top losers on NSE", "debt market watch").

# STEP 2: JSON GENERATION
# Generate exactly one intent item for every subject identified in Step 1.

# Return ONLY valid JSON:
# {{
#   "query_type": "<greeting|general|stock|mf_scheme|mf_amc|bond|comparison|investment>",
#   "is_broad": <true|false>,
#   "mcp_needed": <true|false>,
#   "intents": [
#     {{
#       "entity": "<entity name OR market concept name>",
#       "entity_type": "<see ENTITY TYPE RULES below>",
#       "intent_description": "<specific data point needed for this subject>",
#       "scheme_name": "<if mf_scheme, else empty>",
#       "amc_name": "<if mf_amc, else empty>",
#       "nse_symbol": "<NSE ticker if known, else empty>"
#     }}
#   ]
# }}

# ────────────────────────────────────────────────────────────
# ENTITY TYPE RULES:
#   stock        → Specific listed company's EQUITY data.
#   ipo_stock    → Company-SPECIFIC IPO data where you know the company name.
#   ipo_list     → Generic/market-wide IPO queries with NO specific company name.
#   mf_scheme    → Specific named mutual fund SCHEME.
#   mf_amc       → A mutual fund HOUSE or AMC.
#   etf          → An exchange-traded fund.
#   index        → A market INDEX constituents/membership list (e.g., Nifty IT, Nifty Defence).
#   market       → Market-WIDE movers or screeners (e.g., top gainers, 52-week highs).
#   bond         → Specific named listed BOND, DEBENTURE, or NCD.
#   exchange     → EXCHANGE-LEVEL aggregates (e.g., advance-decline ratio, trading holidays).
#   nfo          → New Fund Offers.
#   news         → Corporate or mutual fund news feeds.
#   announcement → Formal regulatory disclosures on BSE/NSE.
#   market_info  → Broad analytics (macro data, index levels list, result calendars).
#   general      → Educational or definitional conceptual tracking only.
# """

# GREETING_PROMPT = """\
# You are EQUIFIZ — a friendly Indian stock-market AI assistant.
# Respond warmly and briefly to the user's greeting or small talk.

# User: {query}
# """

# MCP_SYNTHESIS_PROMPT = """\
# You are a knowledgeable Indian equity, mutual fund, and fixed-income analyst assistant.

# ## Contextual Grounding Rules (CRITICAL)
# - You MUST rely strictly on the canonical records tagged inside the "Live Data Retrieved" block.
# - NEVER mix up records between distinct data segments. For instance, if data for NIFTY IT contains a specific list of stocks, do NOT mention or inject them into descriptions of the NIFTY DEFENCE segment.
# - If the live data returned for a requested entity is completely empty, zeroed, or displays database lookup indicators like "[]" or None, you must explicitly state that no data was returned for that entity, rather than guessing or filling it using historical knowledge.

# ## Data Safety & Quality Guardrails (STRICT)
# - NEVER expose internal database identifiers or structural system keys (such as "co_code", "mf_schcode").
# - Check the parsed data for structural backend issues. If it indicates server configuration warnings or system failures, explain to the user that the information is currently inaccessible.

# ## Conversation history (last 4 turns)
# {history}

# ## User Question
# {user_query}

# ## Live Data Retrieved
# {mcp_result}

# ## Instructions
# 1. Answer EACH entity component sequentially using only the explicitly labeled live data block provided for it.
# 2. If data is missing or marked as an error, briefly state that it cannot be verified right now.
# 3. Write in plain prose paragraphs only. No markdown, no bullet points, no asterisks, no tables. Do NOT use the rupee symbol — write "Rs".
# 4. Use Indian number formatting (lakh, crore) for big values.
# 5. Keep under 300 words.
# 6. End with: This is not financial advice.
# """

# GENERAL_SYNTHESIS_PROMPT = """\
# You are a knowledgeable Indian equity and fixed-income analyst assistant.

# ## Conversation history (last 4 turns)
# {history}

# ## User Question
# {user_query}

# ## Knowledge Base Context
# {vector_context}

# ## Instructions
# 1. Answer clearly and concisely.
# 2. Plain prose only — no markdown. Do NOT use the rupee symbol — write "Rs".
# 3. Under 200 words.
# """


# # ── LLM helpers ───────────────────────────────────────────────────────────────

# def _llm_json(prompt: str) -> dict:
#     resp  = llm.invoke([HumanMessage(content=prompt)])
#     text  = resp.content.strip()
#     clean = re.sub(r"^`{3}(?:json)?|`{3}$", "", text, flags=re.MULTILINE).strip()

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
# # Node AIQ: Classify + Extract + Decompose
# # ══════════════════════════════════════════════════════════════════════════════

# def node_classify_extract_and_decompose(state: AgentState) -> AgentState:
#     console.print("[Node AIQ] Classify + extract + decompose (v11.5-Hardened)")

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
#                 resolved_name      = None,  
#                 tool_hint          = "",
#                 matched_tools      = [],
#                 resolved_codes      = {},
#                 mcp_result         = "",
#                 code_missing       = False,
#             ))

#         if not intents and qt not in ("greeting",):
#             console.print("  ⚠ LLM returned no intents — fallback single general intent")
#             intents.append(IntentItem(
#                 entity             = user_query,
#                 entity_type        = "general",
#                 intent_description = user_query,
#                 scheme_name        = None,
#                 amc_name           = None,
#                 nse_symbol         = None,
#                 resolved_name      = None,
#                 tool_hint          = "",
#                 matched_tools      = [],
#                 resolved_codes      = {},
#                 mcp_result         = "",
#                 code_missing       = False,
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


# # ── Chroma tool registry helpers (read-only) ──────────────────────────────────

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
#                 "description":          meta.get("description", ""),
#                 "required_parameters": req_params,
#                 "parameters":           meta.get("parameters", ""),
#                 "score":                round(1 - dist, 3),
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
#                 "description":          meta.get("description", ""),
#                 "required_parameters": req_params,
#                 "parameters":           meta.get("parameters", ""),
#             }
#     except Exception as e:
#         console.print(f"  [ToolRegistry] get by id '{tool_name}' failed: {e}")
#     return None


# # ══════════════════════════════════════════════════════════════════════════════
# # Node TVS: Per-intent tool vector search  (v11.5)
# # ══════════════════════════════════════════════════════════════════════════════

# _ALLOWED_FAMILIES: dict[str, frozenset[str]] = {
#     "stock":        frozenset({"stock"}),
#     "ipo_stock":    frozenset({"ipo_stock"}),
#     "ipo_list":     frozenset({"ipo_list"}),
#     "mf_scheme":    frozenset({"mf_scheme"}),
#     "mf_amc":       frozenset({"mf_amc"}),
#     "etf":          frozenset({"etf"}),
#     "index":        frozenset({"index"}),
#     "market":       frozenset({"market"}),
#     "bond":         frozenset({"bond"}),
#     "exchange":     frozenset(),
#     "nfo":          frozenset(),
#     "news":         frozenset(),
#     "announcement": frozenset(),
#     "market_info":  frozenset(),
#     "general":      frozenset(),
# }


# def node_per_intent_tool_search(state: AgentState) -> AgentState:
#     console.print("[Node TVS] Per-intent tool vector search (v11.5)")

#     intents = state.get("intents") or []
#     if not intents:
#         console.print("  ⚠ No intents to search for")
#         return state

#     def _search_for_intent(intent: IntentItem) -> IntentItem:
#         et = intent.get("entity_type", "general")

#         if et == "general":
#             console.print(
#                 f"  [{intent['entity']}] entity_type=general → skip tool search"
#             )
#             return IntentItem(
#                 entity             = intent.get("entity", ""),
#                 entity_type        = "general",
#                 intent_description = intent.get("intent_description", ""),
#                 scheme_name        = intent.get("scheme_name"),
#                 amc_name           = intent.get("amc_name"),
#                 nse_symbol         = intent.get("nse_symbol"),
#                 resolved_name      = None,
#                 tool_hint          = "",
#                 matched_tools      = [],
#                 resolved_codes      = {},
#                 mcp_result         = "",
#                 code_missing       = False,
#             )

#         if et in _NO_DB_MCP_TYPES:
#             search_query = intent["intent_description"]
#         else:
#             search_query = f"{et} {intent['intent_description']}"

#         tools = _query_tool_registry(search_query, n_results=10)

#         allowed = _ALLOWED_FAMILIES.get(et, frozenset())
#         if allowed:
#             filtered = [
#                 t for t in tools
#                 if _infer_entity_type_from_tool(t["tool_name"]) in allowed
#             ]
#             if filtered:
#                 tools = filtered
#                 console.print(
#                     f"  [TVS] family filter '{et}' → kept {len(tools)} tools"
#                 )
#             else:
#                 console.print(
#                     f"  [TVS] ⚠ family filter '{et}' eliminated all tools — keeping unfiltered results"
#                 )

#         tool_hint = tools[0]["tool_name"] if tools else ""

#         if tool_hint:
#             console.print(
#                 f"  [{intent['entity']}] intent='{intent['intent_description'][:50]}' → top tool={tool_hint}"
#             )
#         return IntentItem(
#             entity             = intent.get("entity", ""),
#             entity_type        = et,
#             intent_description = intent.get("intent_description", ""),
#             scheme_name        = intent.get("scheme_name"),
#             amc_name           = intent.get("amc_name"),
#             nse_symbol         = intent.get("nse_symbol"),
#             resolved_name      = None,
#             tool_hint          = tool_hint,
#             matched_tools      = tools,
#             resolved_codes      = {},
#             mcp_result         = "",
#             code_missing       = False,
#         )

#     updated: dict[int, IntentItem] = {}
#     with ThreadPoolExecutor(max_workers=min(6, len(intents))) as ex:
#         futures = {ex.submit(_search_for_intent, intent): idx for idx, intent in enumerate(intents)}
#         for future in as_completed(futures):
#             idx = futures[future]
#             try:
#                 updated[idx] = future.result()
#             except Exception as exc:
#                 console.print(f"  [red]TVS failed for intent #{idx}: {exc}[/red]")
#                 original     = intents[idx]
#                 updated[idx] = IntentItem(
#                     entity             = original.get("entity", ""),
#                     entity_type        = original.get("entity_type", "general"),
#                     intent_description = original.get("intent_description", ""),
#                     scheme_name        = original.get("scheme_name"),
#                     amc_name           = original.get("amc_name"),
#                     nse_symbol         = original.get("nse_symbol"),
#                     resolved_name      = None,
#                     tool_hint          = "",
#                     matched_tools      = [],
#                     resolved_codes      = {},
#                     mcp_result         = "",
#                     code_missing       = False,
#                 )

#     state["intents"] = [updated[i] for i in sorted(updated)]

#     if state["intents"]:
#         first = state["intents"][0]
#         state["matched_tools"]     = first.get("matched_tools") or []
#         state["mcp_tool_hint"]     = first.get("tool_hint") or ""
#         state["primary_tool_hint"] = first.get("tool_hint") or ""

#     return state


# # ── Greeting handler ──────────────────────────────────────────────────────────

# def node_greeting_handler(state: AgentState) -> AgentState:
#     console.print("[Node B] Greeting handler")
#     state["final_answer"] = _llm_text(GREETING_PROMPT.format(query=state["user_query"]))
#     return state


# # ── General handler (vector search fallback) ──────────────────────────────────

# def node_general_handler(state: AgentState) -> AgentState:
#     console.print("[Node C] General handler (vector search fallback)")
#     try:
#         state["vector_context"] = vs.query(state["user_query"], n_results=5)
#     except Exception as e:
#         console.print(f"  General vector search failed: {e}")
#         state["vector_context"] = []
#     return state


# # ══════════════════════════════════════════════════════════════════════════════
# # FIX M — Hardened Multi-Tiered Fuzzy Match Scoring System
# # ══════════════════════════════════════════════════════════════════════════════

# def _compute_smart_score(target: str, candidate: str) -> float:
#     target_clean = target.lower().strip()
#     candidate_clean = candidate.lower().strip()
    
#     if target_clean == candidate_clean:
#         return 100.0
        
#     ts_ratio = fuzz.token_sort_ratio(target_clean, candidate_clean)
#     ratio = fuzz.ratio(target_clean, candidate_clean)
#     part_ratio = fuzz.partial_ratio(target_clean, candidate_clean)
    
#     target_words = set(target_clean.split())
#     candidate_words = set(candidate_clean.split())
    
#     has_exact_word_match = any(word in candidate_words for word in target_words)
#     score = max(ts_ratio, ratio)
    
#     if part_ratio > 90.0:
#         if has_exact_word_match:
#             score = max(score, part_ratio)
#         else:
#             score = max(score, part_ratio - 35.0)
#     else:
#         score = max(score, part_ratio)
        
#     return float(score)


# def _resolve_single(name: str, nse_symbol: Optional[str]) -> dict:
#     name_upper = name.upper().strip()
#     symbol_to_check = nse_symbol.upper().strip() if nse_symbol else name_upper
    
#     if symbol_to_check:
#         row = db.lookup_by_nse_symbol(symbol_to_check)
#         if row:
#             return {
#                 "co_code":      row["co_code"],
#                 "company_info": row,
#                 "nse_symbol":   row.get("nsesymbol"),
#                 "resolved_name": row.get("companyname"),
#             }

#     matches = db.fuzzy_search_company(name, limit=15)
#     if matches:
#         for cand in matches:
#             cand_sym = str(cand.get("nsesymbol") or "").upper().strip()
#             if cand_sym == name_upper:
#                 return {
#                     "co_code":      cand["co_code"],
#                     "company_info": cand,
#                     "nse_symbol":   cand_sym,
#                     "resolved_name": cand.get("companyname"),
#                 }

#         best_match = None
#         best_score = -1.0
        
#         for cand in matches:
#             cand_name = cand.get("companyname") or ""
#             cand_sym  = cand.get("nsesymbol") or ""
            
#             score_name = _compute_smart_score(name, cand_name)
#             score_sym  = _compute_smart_score(name, cand_sym)
#             score = max(score_name, score_sym)
            
#             if score > best_score:
#                 best_score = score
#                 best_match = cand
                
#         if best_match and best_score >= 50.0:
#             return {
#                 "co_code":      best_match["co_code"],
#                 "company_info": best_match,
#                 "nse_symbol":   best_match.get("nsesymbol"),
#                 "resolved_name": best_match.get("companyname"),
#             }
            
#     return {}


# # ══════════════════════════════════════════════════════════════════════════════
# # DB targeted table lookup (Fully Dynamic Intersection Matrix + Exchange Affinity)
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

#         if table == "scheme_master":
#             cur.execute("SELECT mf_schcode, mf_cocode, sch_name, category FROM scheme_master")
#         elif table == "bond_master":
#             cur.execute("SELECT code, companyname, isin, nsesymbol FROM bond_master")
#         else:
#             # Hardened approach: Extract everything dynamically to prevent table field syntax crashes
#             cur.execute(f"SELECT * FROM {table}")

#         rows = [dict(r) for r in cur.fetchall()]
#         cur.close()
#         conn.close()

#         entity_upper = entity.upper()
#         entity_clean = re.sub(r"[^A-Za-z0-9]", "", entity_upper).strip()
#         query_tokens = {t for t in re.split(r"[^A-Za-z0-9]", entity_upper) if len(t) > 1}

#         best_match: Optional[dict] = None
#         best_score: float          = -100.0  

#         for row in rows:
#             # Dynamic candidate extraction mapping
#             candidate = str(row.get(name_col) or row.get("group") or row.get("group_name") or row.get("groupname") or "")
#             cand_upper = candidate.upper()
#             cand_clean = re.sub(r"[^A-Za-z0-9]", "", cand_upper).strip()
            
#             score = float(_compute_smart_score(entity, candidate))
            
#             if table == "group_master":
#                 cand_tokens = set(re.split(r"[^A-Za-z0-9]", cand_upper))
#                 if len(cand_clean) >= 8 and "_" not in cand_upper:
#                     chunks = [cand_clean[i:i+3] for i in range(0, len(cand_clean), 3)]
#                     cand_tokens.update(chunks)

#                 # Strict trailing sub-sector string token matching
#                 is_exact_sector_match = False
#                 for q_tok in query_tokens:
#                     if q_tok not in ("NIFTY", "BSE", "NSE", "INDEX"):
#                         if cand_upper.endswith(f"_{q_tok}") or cand_clean.endswith(q_tok):
#                             is_exact_sector_match = True

#                 # Dynamic Exchange Affinity Matrix Mapping
#                 row_exchange = str(row.get("exchange") or "").upper().strip()
#                 has_nse_affinity = any(k in entity_clean for k in ("NIFTY", "NSE"))
#                 has_bse_affinity = any(k in entity_clean for k in ("BSE", "SENSEX"))

#                 exchange_mismatch = False
#                 if has_nse_affinity and row_exchange == "BSE":
#                     exchange_mismatch = True
#                 elif has_bse_affinity and row_exchange == "NSE":
#                     exchange_mismatch = True

#                 has_token_intersection = any(
#                     (q_tok in cand_clean or any(q_tok in c_tok or c_tok in q_tok for c_tok in cand_tokens))
#                     for q_tok in query_tokens if q_tok not in ("NIFTY", "BSE", "NSE")
#                 )

#                 is_exact_root = (cand_clean in ("NIFTY", "BSE", "NSE"))
#                 query_has_modifiers = len(query_tokens - {"NIFTY", "BSE", "NSE", "INDEX", "STOCKS"}) > 0

#                 # Impose precision scoring rules
#                 if exchange_mismatch:
#                     score = -200.0  # Expel mismatching exchange rows out of bounds instantly
#                 elif is_exact_sector_match:
#                     score = 150.0  # Force exact matched sub-sector to the top rank position
#                 elif is_exact_root and query_has_modifiers:
#                     score -= 75.0  # Impose container penalty to stop basic parent container hijacking
#                 elif has_token_intersection:
#                     score += 35.0

#             if table == "bond_master":
#                 isin_score = fuzz.ratio(entity.upper(), str(row.get("isin") or "").upper())
#                 sym_score = fuzz.ratio(entity.upper(), str(row.get("nsesymbol") or "").upper())
#                 score = max(score, isin_score, sym_score)

#             if score > best_score:
#                 best_score, best_match = score, row

#         if best_match and best_score >= 45.0:
#             resolved_id = best_match.get(id_col) or best_match.get("indexcode") or best_match.get("code")
#             resolved_name = best_match.get(name_col) or best_match.get("group") or best_match.get("group_name")
#             return {
#                 id_col: resolved_id,
#                 name_col: resolved_name
#             }
#         return None

#     except Exception as e:
#         console.print(f"    [_targeted_db_lookup] DB error on {table}: {e}")
#         return None


# def _get_required_params_for_entity(
#     tool_hint:     str,
#     entity_type:   str,
#     matched_tools: list[dict],
# ) -> list[str]:
#     params = ENTITY_TYPE_PARAM_MAP.get(entity_type)
#     if params is not None:
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

#     return []


# # ── Per-intent DB code resolver ──────────────────────────────────────────────

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
#     needs_group     = "group"       in required_params
#     needs_bond_code = "bond_code"   in required_params

#     if needs_schcode:
#         search = scheme_name or name
#         cfg    = PARAM_TO_TABLE_MAP["mf_schcode"]
#         match  = _targeted_db_lookup(entity=search, table=cfg["table"], name_col=cfg["column"], id_col=cfg["id_field"])
#         if match:
#             codes["mf_schcode"] = int(match["mf_schcode"])
#             if "mf_cocode" in match:
#                 codes.setdefault("mf_cocode", int(match["mf_cocode"]))
#             result["resolved_scheme_name"] = match.get("sch_name", search)

#     if needs_cocode and not codes.get("mf_cocode"):
#         search = amc_name or name
#         cfg    = PARAM_TO_TABLE_MAP["mf_cocode"]
#         match  = _targeted_db_lookup(entity=search, table=cfg["table"], name_col=cfg["column"], id_col=cfg["id_field"])
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
#                 "resolved_company_name": stock["resolved_name"]
#             })
#         else:
#             result["code_missing"] = True

#     if needs_isin:
#         search = scheme_name or name
#         cfg    = PARAM_TO_TABLE_MAP["isin"]
#         match  = _targeted_db_lookup(entity=search, table=cfg["table"], name_col=cfg["column"], id_col=cfg["id_field"])
#         if match:
#             codes["isin"] = match["isin"]
#             result["resolved_etf_name"] = match.get("etfname", search)

#     if needs_indexcode:
#         cfg   = PARAM_TO_TABLE_MAP["index_code"]
#         match = _targeted_db_lookup(entity=name, table=cfg["table"], name_col=cfg["column"], id_col=cfg["id_field"])
#         if match:
#             try:
#                 codes["index_code"] = int(float(match[cfg["id_field"]]))
#             except (ValueError, TypeError, KeyError):
#                 codes["index_code"] = int(float(match.get("indexcode") or match.get("index_code")))
#             result["resolved_index_name"] = match.get(cfg["column"]) or match.get("group") or match.get("group_name")

#     if needs_group:
#         cfg   = PARAM_TO_TABLE_MAP["group"]
#         match = _targeted_db_lookup(entity=name, table=cfg["table"], name_col=cfg["column"], id_col=cfg["id_field"])
#         if match:
#             codes["group"] = match.get(cfg["id_field"]) or match.get("group") or match.get("group_name")
#             result["resolved_group_name"] = codes["group"]
#         else:
#             codes["group"] = name
#             result["resolved_group_name"] = name

#     if needs_bond_code:
#         cfg   = PARAM_TO_TABLE_MAP["bond_code"]
#         match = _targeted_db_lookup(entity=name, table=cfg["table"], name_col=cfg["column"], id_col=cfg["id_field"])
#         if match:
#             codes["bond_code"] = int(match["code"])
#             result["resolved_bond_name"] = match.get("companyname", name)
#         else:
#             result["code_missing"] = True

#     result["mcp_resolved_codes"] = codes
#     return result


# def _resolve_intents_codes(intents: list[IntentItem]) -> list[IntentItem]:
#     def _resolve_one(intent: IntentItem) -> IntentItem:
#         et        = intent.get("entity_type", "general")
#         tool_hint = intent.get("tool_hint", "")
#         matched   = intent.get("matched_tools") or []

#         if et == "general" and tool_hint:
#             inferred = _infer_entity_type_from_tool(tool_hint)
#             if inferred != "general":
#                 et = inferred
#                 intent["entity_type"] = et

#         req_params = _get_required_params_for_entity(tool_hint, et, matched)

#         if not req_params:
#             intent["resolved_codes"] = {}
#             intent["code_missing"]   = False
#             intent["resolved_name"]  = intent.get("entity")
#             return intent

#         resolved = _resolve_entity_codes(
#             name            = intent.get("entity", ""),
#             scheme_name     = intent.get("scheme_name"),
#             amc_name        = intent.get("amc_name"),
#             nse_symbol      = intent.get("nse_symbol"),
#             required_params = req_params,
#         )
#         intent["resolved_codes"] = resolved.get("mcp_resolved_codes", {})
#         intent["code_missing"]   = bool(resolved.get("code_missing", False))
        
#         intent["resolved_name"] = (
#             resolved.get("resolved_company_name") or
#             resolved.get("resolved_scheme_name") or
#             resolved.get("resolved_amc_name") or
#             resolved.get("resolved_etf_name") or
#             resolved.get("resolved_index_name") or
#             resolved.get("resolved_bond_name") or
#             intent.get("entity")
#         )
#         return intent

#     result_intents: dict[int, IntentItem] = {}
#     with ThreadPoolExecutor(max_workers=min(6, len(intents) or 1)) as ex:
#         futures = {ex.submit(_resolve_one, intent): idx for idx, intent in enumerate(intents)}
#         for future in as_completed(futures):
#             idx = futures[future]
#             try:
#                 result_intents[idx] = future.result()
#             except Exception:
#                 result_intents[idx] = intents[idx]

#     return [result_intents[i] for i in sorted(result_intents)]


# # ══════════════════════════════════════════════════════════════════════════════
# # MCP Pre-Resolution nodes
# # ══════════════════════════════════════════════════════════════════════════════

# def node_mcp_pre_resolve(state: AgentState) -> AgentState:
#     console.print("[Node MPR] Single-entity MCP pre-resolve (v11.5)")
#     intents = state.get("intents") or []
#     if not intents:
#         state["mcp_resolved_codes"] = {}
#         return state

#     resolved_intents            = _resolve_intents_codes(intents)
#     state["intents"]            = resolved_intents
#     state["mcp_resolved_codes"] = resolved_intents[0].get("resolved_codes", {}) if resolved_intents else {}
#     return state


# def node_mcp_multi_pre_resolve(state: AgentState) -> AgentState:
#     console.print("[Node MMPR] Multi-entity MCP pre-resolve (v11.5)")
#     intents = state.get("intents") or []
#     if not intents:
#         return state

#     resolved_intents            = _resolve_intents_codes(intents)
#     state["intents"]            = resolved_intents
#     state["mcp_resolved_codes"] = resolved_intents[0].get("resolved_codes", {}) if resolved_intents else {}
#     return state


# # ══════════════════════════════════════════════════════════════════════════════
# # Node MTCI: MCP Tool Call
# # ══════════════════════════════════════════════════════════════════════════════

# def _run_mcp_multi_in_thread(intents: list[dict]) -> list[dict]:
#     from mcp_client import run_mcp_query_multi, trim_results
#     loop = asyncio.new_event_loop()
#     asyncio.set_event_loop(loop)
#     try:
#         results = loop.run_until_complete(run_mcp_query_multi(intents))
#         return trim_results(results)
#     except Exception as e:
#         console.print(f"[bold red]MCP Multi Error:[/bold red] {e}")
#         return [{**i, "mcp_result": f"Error: {e}"} for i in intents]
#     finally:
#         loop.close()


# def node_mcp_tool_call_intents(state: AgentState) -> AgentState:
#     console.print("[Node MTCI] Sequential per-intent MCP calls — one session (v11.5)")

#     try:
#         import mcp_client  # noqa: F401
#     except ImportError:
#         state["mcp_raw_result"] = ""
#         state["intent_results"] = []
#         state["error"]          = "mcp_client_not_found"
#         return state

#     intents    = state.get("intents") or []
#     user_query = state.get("user_query", "")

#     mcp_intents: list[IntentItem]     = []
#     blocked_intents: list[IntentItem] = []
#     general_intents: list[IntentItem] = []

#     for i in intents:
#         et           = i.get("entity_type", "general")
#         has_tool     = bool(i.get("tool_hint"))
#         code_missing = bool(i.get("code_missing", False))

#         if et == "general" and not has_tool:
#             general_intents.append(i)
#         elif code_missing:
#             blocked_intents.append(i)
#         else:
#             mcp_intents.append(i)

#     if not mcp_intents:
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

#     try:
#         with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
#             future         = ex.submit(_run_mcp_multi_in_thread, mcp_intents)
#             filled_intents = future.result(timeout=600)
#     except Exception as exc:
#         filled_intents = [{**dict(i), "mcp_result": f"MCP call failed: {exc}"} for i in mcp_intents]

#     all_intents = (
#         filled_intents
#         + [{**dict(i), "mcp_result": "(answered from knowledge base)"} for i in general_intents]
#         + [{**dict(i), "mcp_result": f"Sorry, I could not find '{i.get('entity')}' in our database."} for i in blocked_intents]
#     )
#     state["intents"] = all_intents

#     state["intent_results"] = [
#         {
#             "entity":             i.get("entity", ""),
#             "resolved_name":      i.get("resolved_name", i.get("entity", "")),
#             "entity_type":        i.get("entity_type", ""),
#             "intent_description": i.get("intent_description", ""),
#             "mcp_result":         i.get("mcp_result", ""),
#         }
#         for i in all_intents
#     ]

#     valid_segments = []
#     for r in state["intent_results"]:
#         res_str = str(r.get("mcp_result", ""))
        
#         if any(indicator in res_str.lower() for indicator in ["<html", "502 bad gateway", "nginx/", "404 not found", "error response"]):
#             res_str = "Error: Upstream market analytics interface returned an invalid protocol wrapper. The server is busy. Please rephrase shortly."
#             state["error"] = "upstream_infrastructure_leak"
            
#         valid_segments.append(
#             f"--- RESOLVED ENTITY CANONICAL DATA FOR: {r.get('resolved_name', r['entity']).upper()} ---\n"
#             f"[Requested Segment: {r['intent_description']}]\n"
#             f"{res_str}"
#         )

#     state["mcp_raw_result"] = "\n\n---\n\n".join(valid_segments)
#     state["mcp_tool_calls_made"] = []
#     return state


# _MCP_ERROR_MESSAGES = {
#     "mcp_client_not_found": "Sorry, the live data service is currently unavailable. Please try again shortly.",
#     "mcp_call_timeout": "The live data request timed out. The server may be busy — please try again in a moment.",
#     "upstream_infrastructure_leak": "The data connection to the market stream encountered an operational disruption. I could not verify the requested information right now. Please rephrase shortly."
# }

# # ══════════════════════════════════════════════════════════════════════════════
# # MCP Synthesis (v11.5)
# # ══════════════════════════════════════════════════════════════════════════════

# def node_mcp_synthesis(state: AgentState) -> AgentState:
#     console.print("[Node MS] MCP synthesis (v11.5)")
#     error = state.get("error", "")
#     mcp_result = state.get("mcp_raw_result", "")

#     if error in _MCP_ERROR_MESSAGES:
#         state["final_answer"] = _MCP_ERROR_MESSAGES[error]
#         return state
        
#     if not mcp_result:
#         state["final_answer"] = "No data was returned from the live server. Please try again shortly."
#         return state

#     mcp_snippet = mcp_result
#     if len(mcp_snippet) > 10000:
#         mcp_snippet = mcp_snippet[:7500] + "\n\n... [middle trimmed for length] ...\n\n" + mcp_snippet[-2000:]

#     prompt = MCP_SYNTHESIS_PROMPT.format(
#         history    = _format_history(state),
#         user_query = state.get("user_query", ""),
#         mcp_result = mcp_snippet,
#     )
#     try:
#         state["final_answer"] = _llm_text(prompt)
#     except Exception as e:
#         state["final_answer"] = mcp_result
#     return state


# def node_synthesis(state: AgentState) -> AgentState:
#     console.print("[Node S] General synthesis (vector search)")
#     vector_context = state.get("vector_context") or []
#     context_text   = "\n".join(f"- [{r['metadata'].get('api_name', '')}] {r['document']}" for r in vector_context)
#     prompt = GENERAL_SYNTHESIS_PROMPT.format(
#         history        = _format_history(state),
#         user_query     = state["user_query"],
#         vector_context = context_text or "No relevant context found.",
#     )
#     try:
#         state["final_answer"] = _llm_text(prompt)
#     except Exception as e:
#         state["final_answer"] = f"Synthesis failed: {e}"
#     return state


# # ══════════════════════════════════════════════════════════════════════════════
# # Router (v11.5)
# # ══════════════════════════════════════════════════════════════════════════════

# def route_after_classify(state: AgentState) -> str:
#     qt      = state.get("query_type", "general")
#     intents = state.get("intents") or []

#     if qt == "greeting":
#         return "greeting"

#     mcp_intents = [i for i in intents if i.get("tool_hint") or i.get("entity_type") not in ("general",)]

#     if not mcp_intents or (not any(i.get("tool_hint") for i in intents) and all(i.get("entity_type") == "general" for i in intents)):
#         return "general"

#     has_many         = len(intents) > 1
#     additional       = state.get("companies") or []
#     primary_type     = state.get("primary_entity_type", "general")
#     secondary_types  = {c.get("entity_type", "general") for c in additional}
#     is_heterogeneous = bool(secondary_types - {"general"} - {primary_type})

#     if has_many or qt == "comparison" or is_heterogeneous:
#         return "multi_mcp"

#     return "mcp_direct"


# # ══════════════════════════════════════════════════════════════════════════════
# # Graph Build Sequence
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


# _graph = None

# def get_graph():
#     global _graph
#     if _graph is None:
#         _graph = build_graph()
#     return _graph


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


# ###thik e ache

# from __future__ import annotations

# import asyncio
# import concurrent.futures
# import json
# import logging
# import re
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

# # ── Types ─────────────────────────────────────────────────────────────────────

# class Turn(TypedDict):
#     role: str       # "user" | "assistant"
#     content: str


# class IntentItem(TypedDict, total=False):
#     entity:             str
#     entity_type:        str
#     intent_description: str
#     tool_hint:          str
#     scheme_name:        Optional[str]
#     amc_name:           Optional[str]
#     nse_symbol:         Optional[str]
#     sector_name:        Optional[str]  # Added for Sector tracking
#     resolved_name:      Optional[str]  
#     matched_tools:      list[dict]
#     resolved_codes:     dict
#     mcp_result:         str
#     code_missing:       bool           


# _NO_DB_MCP_TYPES = frozenset({
#     "exchange",
#     "ipo_list",
#     "nfo",
#     "news",
#     "announcement",
#     "market_info",
# })

# # ── Tool Family Mapping Matrix ───────────────────────────────────────────────
# _TOOL_FAMILY_HINTS: dict[str, str] = {
#     "get_company_stock_price": "stock",
#     "get_delayed_stock_price": "stock",
#     "get_nse_company_announcements":        "stock",
#     "get_bse_company_announcements":        "stock",
#     "get_company_result_schedule":          "stock",
#     "get_company_profile":                  "stock",
#     "get_company_background":               "stock",
#     "get_board_of_directors":               "stock",
#     "get_company_bankers":                  "stock",
#     "get_management_biodata":               "stock",
#     "get_subsidiaries_jvs":                 "stock",
#     "get_related_party_transactions":       "stock",
#     "get_employee_count":                   "stock",
#     "get_capital_structure":                "stock",
#     "get_pledge_share_details":             "stock",
#     "get_substantial_acquisitions":         "stock",
#     "get_segment_data":                     "stock",
#     "get_r_and_d_expenditure":              "stock",
#     "get_finished_products":                "stock",
#     "get_raw_materials":                    "stock",
#     "get_chronological_history":            "stock",
#     "get_company_history":                  "stock",
#     "get_funds_holding_company":            "stock",
#     "get_quarterly_results":                "stock",
#     "get_profit_loss":                      "stock",
#     "get_balance_sheet":                    "stock",
#     "get_cash_flow":                        "stock",
#     "get_half_yearly_results":              "stock",
#     "get_nine_months_results":              "stock",
#     "get_quarterly_trends" :                 "stock",
#     "get_shareholding_pattern":              "stock",
#     "get_major_shareholders":                "stock",
#     "get_quarterly_balance_sheet" :         "stock",
#     "get_yearly_results":                   "stock",
#     "get_annual_balance_sheet":              "stock",
#     "get_half_yearly_balance_sheet":         "stock",
#     "get_ttm_growth_trends":                 "stock",
#     "get_quarterly_revenue_trends":          "stock",
#     "get_quarterly_ebitda_trends":           "stock",
#     "get_quarterly_ebit_trends":             "stock",
#     "get_growth_data_quarterly":             "stock",
#     "get_growth_data_yearly":                "stock",
#     "get_key_financial_ratios":             "stock",
#     "get_daily_ratios":                     "stock",
#     "get_margin_ratios":                    "stock",
#     "get_valuation_ratios":                 "stock",
#     "get_all_basic_ratios":                 "stock",
#     "get_return_ratios":                    "stock",
#     "get_growth_ratios":                    "stock",
#     "get_performance_ratios":                "stock",
#     "get_efficiency_ratios":                 "stock",
#     "get_cashflow_ratios":                  "stock",
#     "get_liquidity_ratios":                 "stock",
#     "get_solvency_ratios":                  "stock",
#     "get_quarterly_ratios":                 "stock",
#     "get_yearly_ratios":                    "stock",
#     "get_shareholding":                     "stock",
#     "get_major_sharehold":                  "stock",
#     "get_financial_stability_ratios":       "stock",
#     "get_anchor_investor":                  "ipo_stock",
#     "get_ipo_details":                      "ipo_stock",
#     "get_ipo_subscription_status":          "ipo_stock",
#     "get_ipo_synopsis":                     "ipo_stock",
#     "get_ipo_timeline":                     "ipo_stock",
#     "get_ipo_promoter_details":             "ipo_stock",
#     "get_ipo_listing_info":                 "ipo_stock",
#     "get_ipo_objects_of_issue":             "ipo_stock",
#     "get_ipo_anchor_investor_details":      "ipo_stock",
#     "get_ipo_industry_peers":                "ipo_stock",
#     "get_ipo_financials":                   "ipo_stock",
#     "get_ipo_product_services":             "ipo_stock",
#     "get_ipo_strength_details":             "ipo_stock",
#     "get_ipo_strategy_details":             "ipo_stock",
#     "get_ipo_risk_details":                 "ipo_stock",
#     "get_ipo_selling_shareholders":         "ipo_stock",
#     "get_ipo_allocation_details":           "ipo_stock",
#     "get_ipo_prospectus":                   "ipo_stock",
#     "get_ipo_lead_managers":                "ipo_stock",
#     "get_ipo_registrar":                    "ipo_stock",
#     "get_ipo_customer_details":              "ipo_stock",
#     "get_market_indices":                   "stock",   
#     "get_index_companies":                  "index",
#     "get_active_performer":                 "market",
#     "get_top_gainers":                      "market",
#     "get_top_losers":                       "market",
#     "get_out_under_performers":              "market",
#     "get_52week_highs":                      "market",
#     "get_52week_lows":                      "market",
#     "get_new_highs_lows":                   "market",
#     "get_sector_companies":                 "sector",
#     "get_advance_decline":                  "exchange",
#     "get_exchange_holidays":                "exchange",
#     "get_scheme_nav":                       "mf_scheme",
#     "get_investment_details":                "mf_scheme",
#     "get_expense_ratio":                    "mf_scheme",
#     "get_avg_maturity":                     "mf_scheme",
#     "get_scheme_aum":                       "mf_scheme",
#     "get_nav_historical":                   "mf_scheme",
#     "get_scheme_returns":                   "mf_scheme",
#     "get_lumpsum_returns":                  "mf_scheme",
#     "get_scheme_sip":                       "mf_scheme",
#     "get_mf_holdings":                      "mf_scheme",
#     "get_sector_allocation":                "mf_scheme",
#     "get_asset_allocation":                 "mf_scheme",
#     "get_portfolio_changes":                "mf_scheme",
#     "get_mcap_allocation":                  "mf_scheme",
#     "get_most_bought_sold":                  "mf_scheme",
#     "get_scheme_ratios":                    "mf_scheme",
#     "get_dividend_details":                 "mf_scheme",
#     "get_bse_star_scheme":                  "mf_scheme",
#     "compare_schemes":                      "mf_scheme",
#     "get_whats_in_out":                     "mf_scheme",
#     "get_scheme_sip_rules":                 "mf_scheme",
#     "get_scheme_sip_details":               "mf_scheme",
#     "get_fund_categories":                  "mf_amc",
#     "get_schemes_by_amc":                   "mf_amc",
#     "get_fund_profile":                     "mf_amc",
#     "get_fund_managers":                     "mf_amc",
#     "get_etf_quotes":                       "etf",
#     "get_etf_returns":                      "etf",
#     "get_etf_fundamentals":                 "etf",
#     "get_etf_about":                        "etf",
#     "get_etf_equity_holdings":              "etf",
#     "get_etf_monthly_portfolio":               "etf",
#     "get_etf_sector_allocatio":             "etf",
#     "get_etf_asset_allocation":             "etf",
#     "get_debt_eod_prices_scripwise":        "bond",
#     "get_bond_details":                     "bond",
#     "get_bond_price_history":               "bond",
#     "get_bond_cashflow":                    "bond",
#     "get_bond_rating":                      "bond",
#     "get_bond_redemption":                  "bond",
#     "get_bond_interest":                    "bond",
#     "get_forthcoming_ipos":                  "ipo_list",
#     "get_open_ipos":                        "ipo_list",
#     "get_closed_ipos":                      "ipo_list",
#     "get_new_ipo_listings":                          "ipo_list",
#     "get_best_ipo_performers":                         "ipo_list",
#     "get_ipo_master":                       "ipo_list",
#     "get_ipo_logo":                         "ipo_list",
#     "get_forthcoming_drh_filings":           "ipo_list",
#     "get_basis_of_allotment" :               "ipo_list",
#     "get_open_bond_ipo":                      "ipo_list",
#     "get_forthcoming_bond_ipo" :               "ipo_list",
#     "get_new_fund_offer":                   "nfo",
#     "get_corporate_news":                   "news",
#     "get_mf_news":                          "news",
#     "get_mf_market_activity":                     "news",
#     "get_bse_announcements":                 "announcement",
#     "get_nse_announcements":                 "announcement",
#     "get_fund_house":                       "market_info",
#     "get_amfi_master":                      "market_info",
#     "get_fund_manager":                     "market_info",
#     "get_index_list":                       "market_info",
#     "get_results_today":                    "market_info",
#     "get_result_declarations":              "market_info",
#     "get_annual_declarations":              "market_info",
#     "get_fund_performance":                  "market_info",
#     "get_category_performance":              "market_info",
#     "get_sip_dates":                        "market_info",
#     "get_macro_economic_data":              "market_info",
#     "get_forthcoming_bond_ipo":             "market_info",
#     "get_open_bond_ipo":                    "market_info",
#     "get_debt_top_value":                   "market_info",
#     "get_debt_top_volume":                  "market_info",
#     "et_new_fund_offers" :                  "market_info",
#     "get_debt_market_watch":                "market_watch",
#     "get_index_constituents":               "index"
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
#     "co_code": {
#         "table":    "companies",
#         "column":   "companyname",
#         "id_field": "co_code",
#     },
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
#     "group": {
#         "table":    "group_master",
#         "column":   "group_name",
#         "id_field": "group_name",
#     },
#     "bond_code": {
#         "table":    "bond_master",
#         "column":   "companyname",
#         "id_field": "code",
#     },
#     "sector_code": {
#         "table":    "companies",      # Fixed potential typos from parameters snippet
#         "column" :  "sectorname",    # Fixed potential typos from parameters snippet
#         "id_field": "sectorcode"
#     }
# }

# ENTITY_TYPE_PARAM_MAP: dict[str, list[str]] = {
#     "stock":        ["co_code"],
#     "ipo_stock":    ["co_code"],    
#     "mf_scheme":    ["mf_schcode"],
#     "mf_amc":       ["mf_cocode"],
#     "etf":          ["isin"],
#     "index":        ["index_code"],
#     "market":       ["group"],
#     "bond":         ["bond_code"],
#     "sector":       ["sector_code"],  # Mapped Sector criteria
#     "exchange":     [],
#     "ipo_list":     [],
#     "nfo":          [],
#     "news":         [],
#     "announcement": [],
#     "market_info":  [],
#     "general":      [],
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

#     matched_tools:  list[dict]
#     intents:        list[IntentItem]
#     intent_results: list[dict]

#     nse_symbol:   Optional[str]
#     co_code:      Optional[int]
#     company_info: Optional[dict]
#     sector_name:  Optional[str]  # Tracked inside Global State Context

#     mcp_resolved_codes: dict
#     mcp_scheme_name:    Optional[str]
#     mcp_amc_name:       Optional[str]

#     companies: list[dict]

#     vector_context: list[dict]

#     mcp_raw_result:      str
#     mcp_tool_calls_made: list[str]

#     final_answer: str
#     error:        Optional[str]


# # ── LLM Prompt Templates ──────────────────────────────────────────────────────

# # CLASSIFY_EXTRACT_DECOMPOSE_PROMPT = """\
# # You are a Lead Financial Systems Analyst for an Indian stock-market assistant. Your task is to decompose user queries into executable data intents.

# # ## Context
# # - History: {history}
# # - Today's Date: {today}
# # - Query: "{query}"

# # ────────────────────────────────────────────────────────────
# # STEP 1: SUBJECT IDENTIFICATION
# # Identify every distinct subject the user is asking about. A subject is either:
# # 1. A Specific Entity: (e.g., Reliance Industries, HDFC AMC, Nifty 50, SBI Small Cap Fund, Gold BeES ETF, HDFC NCD bond, Automobile Sector, IT Sector).
# # 2. A Market Category/Concept: (e.g., "upcoming IPOs", "top gainers", "market news", "bulk deals", "top losers on NSE", "debt market watch").

# # STEP 2: JSON GENERATION
# # Generate exactly one intent item for every subject identified in Step 1.

# # Return ONLY valid JSON:
# # {{
# #   "query_type": "<greeting|general|stock|mf_scheme|mf_amc|bond|comparison|investment|sector>",
# #   "is_broad": <true|false>,
# #   "mcp_needed": <true|false>,
# #   "intents": [
# #     {{
# #       "entity": "<entity name OR market concept name OR sector name>",
# #       "entity_type": "<see ENTITY TYPE RULES below>",
# #       "intent_description": "<specific data point needed for this subject>",
# #       "scheme_name": "<if mf_scheme, else empty>",
# #       "amc_name": "<if mf_amc, else empty>",
# #       "nse_symbol": "<NSE ticker if known, else empty>",
# #       "sector_name": "<if industry sector entity, extract clean sector name here, else empty>"
# #     }}
# #   ]
# # }}

# # ────────────────────────────────────────────────────────────
# # ENTITY TYPE RULES:
# #   stock        → Specific listed company's EQUITY data.
# #   sector       → Specific industry or macroeconomic market sector (e.g., "Automobile", "FMCG", "Banking", "IT").
# #   ipo_stock    → Company-SPECIFIC IPO data where you know the company name.
# #   ipo_list     → Generic/market-wide IPO queries with NO specific company name.
# #   mf_scheme    → Specific named mutual fund SCHEME.
# #   mf_amc       → A mutual fund HOUSE or AMC.
# #   etf          → An exchange-traded fund.
# #   index        → A market INDEX constituents/membership list (e.g., Nifty IT, Nifty Defence).
# #   market       → Market-WIDE movers or screeners (e.g., top gainers, 52-week highs).
# #   bond         → Specific named listed BOND, DEBENTURE, or NCD.
# #   exchange     → EXCHANGE-LEVEL aggregates (e.g., advance-decline ratio, trading holidays).
# #   nfo          → New Fund Offers.
# #   news         → Corporate or mutual fund news feeds.
# #   announcement → Formal regulatory disclosures on BSE/NSE.
# #   market_info  → Broad analytics (macro data, index levels list, result calendars).
# #   general      → Educational or definitional conceptual tracking only.
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
# 1. A Specific Entity: (e.g., Reliance Industries, HDFC AMC, Nifty 50, SBI Small Cap Fund, Gold BeES ETF, HDFC NCD bond, Automobile Sector, Financial Sector).
# 2. A Market Category/Concept: (e.g., "upcoming IPOs", "top gainers", "market news", "bulk deals", "top losers on NSE", "debt market watch").

# STEP 2: JSON GENERATION
# Generate exactly one intent item for every subject identified in Step 1.

# Return ONLY valid JSON:
# {{
#   "query_type": "<greeting|general|stock|mf_scheme|mf_amc|bond|comparison|investment|sector>",
#   "is_broad": <true|false>,
#   "mcp_needed": <true|false>,
#   "intents": [
#     {{
#       "entity": "<entity name OR market concept name OR sector name>",
#       "entity_type": "<see ENTITY TYPE RULES below>",
#       "intent_description": "<specific data point needed for this subject>",
#       "scheme_name": "<if mf_scheme, else empty>",
#       "amc_name": "<if mf_amc, else empty>",
#       "nse_symbol": "<NSE ticker if known, else empty>",
#       "sector_name": "<if industry sector entity, extract clean sector name here, else empty>"
#     }}
#   ]
# }}

# ────────────────────────────────────────────────────────────
# ENTITY TYPE RULES (use exactly one of these values):

#   sector       → Specific macro-economic industry or business sector categories.
#                  Use when the user asks about lists of companies within an industry, sector-wise 
#                  performance metrics, sector allocations, or company tracking inside standard 
#                  macro-economic domains (e.g., Finance, IT, FMCG, Automobile, Pharma, Infrastructure, Energy).
#                  ⚠ CRITICAL RULE FOR SECTOR OVERRIDE: If the query mentions phrases like 
#                  "stocks under the X sector", "companies in the X industry", "X sector stocks list", 
#                  or "X sector index", you MUST select sector (NOT index, NOT stock, and NOT market).
#                  The word "index" in a phrase like "finance sector index" or "IT sector index" refers 
#                  to the sectoral classification metrics tracking—it is a sector intent.
#                  Examples: "List of stocks under the finance sector index" → sector, 
#                  "companies in automobile sector" → sector, "IT sector stocks" → sector.

#   index        → A concrete capital market benchmark INDEX structure. 
#                  Use ONLY when the user asks about components or constituents of a explicit, named 
#                  exchange-traded index (e.g., Nifty 50, Bank Nifty, Nifty Next 50, Sensex, Nifty Midcap 100, Nifty Smallcap 250).
#                  ⚠ CRITICAL BOUNDARY: Do NOT use index for general sector benchmarks like "finance sector index". 
#                  Use index only for specific tradeable indices.
#                  Examples: "Nifty 50 constituents", "Sensex component stocks", "Bank Nifty companies list".

#   stock        → A specific listed company's EQUITY data. Use when the user asks about
#                  share price, stock quote, OHLC, 52-week high/low, delivery volume,
#                  delayed price, company profile, background, board of directors, bankers,
#                  management team, subsidiaries, related parties, employee count, capital
#                  structure, pledged shares, substantial shareholders, segment data, R&D,
#                  finished products, raw materials, chronological events, company history,
#                  quarterly/annual financial results, P&L, balance sheet, cash flow,
#                  half-yearly/nine-month results, TTM growth, revenue/EBITDA/EBIT trends,
#                  key financial ratios, daily ratios, margin ratios, valuation ratios,
#                  return ratios, growth ratios, performance ratios, cashflow ratios,
#                  liquidity ratios, solvency ratios, shareholding pattern, major shareholders.
#                  ⚠ CRITICAL: Use stock ONLY for equity/share data of a single named company.
#                  Examples: "Reliance share price", "TCS quarterly results", "ITC PE ratio".

#   market       → Market-WIDE movers or screeners that require a market GROUP or
#                  SEGMENT (e.g. NSE, BSE). Use when the user wants ranked list aggregates across 
#                  the entire market: top gainers, top losers, most active stocks, 52-week highs/lows, 
#                  new highs, overall volume shockers, or advance-decline distributions.
#                  ⚠ CRITICAL: This is NOT about sorting single sector clusters—it is about cross-market screeners.
#                  Examples: "top gainers on NSE today", "52-week high stocks", "most active stocks BSE".

#   ipo_stock    → Company-SPECIFIC IPO data where you know the company name.
#                  Use this when the user asks about IPO details, GMP, allotment status,
#                  anchor investors, subscription status, or prospectus for a NAMED company.
#                  Examples: "Zomato IPO allotment", "Paytm IPO risk factors", "LIC IPO timeline".

#   ipo_list     → Generic/market-wide IPO queries with NO specific company name.
#                  Use when the user wants a LIST, overview, or status of IPOs across
#                  the market — forthcoming IPOs, upcoming IPOs, open IPOs, closed IPOs.
#                  Examples: "upcoming IPOs", "open IPOs right now", "recent IPO listings".

#   mf_scheme    → A specific named mutual fund SCHEME. Use when the user asks about
#                  NAV (current or historical), investment details, expense ratio,
#                  average maturity, AUM, scheme returns (1Y/3Y/5Y), lumpsum returns,
#                  SIP calculator, MF holdings/portfolio, or asset allocation for a NAMED scheme.
#                  Examples: "Parag Parikh Flexi Cap NAV", "SBI Small Cap Fund portfolio".

#   mf_amc       → A mutual fund HOUSE or AMC (Asset Management Company) — NOT a
#                  specific scheme. Use when the user asks about all schemes offered by
#                  a fund house, fund categories under an AMC, or AMC profile/overview.
#                  Examples: "SBI Mutual Fund schemes", "Nippon AMC fund categories".

#   etf          → An exchange-traded fund. Use when the user asks about ETF quotes,
#                  ETF returns, ETF fundamentals, ETF profile, or ETF holdings for a NAMED ETF.
#                  Examples: "Gold BeES NAV", "Nifty BeES returns", "SBI ETF Nifty 50".

#   bond         → A specific named listed BOND, DEBENTURE, or NCD (Non-Convertible
#                  Debenture). Use when the user asks about bond EOD prices, coupon rate, 
#                  face value, maturity, price history, cash flows, credit rating, or redemption.
#                  Examples: "HDFC NCD coupon rate", "SBI bond price", "NHAI bond rating".

#   exchange     → EXCHANGE-LEVEL aggregate data with no specific entity. Use when the
#                  user asks about overall market advance-decline ratio, market breadth,
#                  exchange trading holidays, or market open/close schedules for NSE or BSE as a whole.
#                  Examples: "NSE advance decline ratio today", "BSE market holidays".

#   nfo          → New Fund Offers — mutual fund schemes that are currently open for
#                  subscription for the FIRST TIME. Use when the user asks about NFOs,
#                  new MF launches, or upcoming fund offers.
#                  Examples: "new fund offers this month", "currently open NFOs".

#   news         → Corporate or mutual fund NEWS and activity feeds. Use when the user
#                  asks for recent news articles, press releases, or media updates.
#                  Examples: "latest news on Reliance", "corporate news today".

#   announcement → Formal regulatory ANNOUNCEMENTS filed on BSE or NSE. Use when the
#                  user asks about corporate announcements, exchange disclosures, regulatory filings, 
#                  board meeting notices, or AGM notices.
#                  Examples: "BSE announcements today", "NSE corporate filings".

#   market_info  → Broad market INFORMATION and analytics that don't belong to a single entity.
#                  Use for: fund manager lists, AMFI master data, fund performance rankings by category, 
#                  SIP transaction dates, result declaration calendars, or macro-economic data (GDP, CPI).
#                  Examples: "macro economic indicators India", "repo rate today", "result calendar this week".

#   general      → ONLY for purely educational or definitional questions that CANNOT
#                  be answered by any live market data tool.
#                  Examples: "explain what SIP means", "what is the difference between NAV and share price".

# ────────────────────────────────────────────────────────────
# DECOMPOSITION RULES (CRITICAL):
# 1. SECTOR CLASSIFICATION IS PARAMOUNT: Guard against assigning multi-word sector phrases into index or stock blocks. If the string queries fields or properties of a business sector, flag it cleanly as sector.
# 2. THE 1:1 MAPPING: len(intents) MUST equal the number of subjects identified in Step 1.
# 3. NO BUNDLING: Two different subjects → two separate intent objects.
# 4. DATA POINT MERGING: Multiple metrics on the SAME entity → ONE intent.

# MCP_NEEDED LOGIC:
# - Set to true if ANY intent requires live market data or relational database lookups.
# - Set to false ONLY for pure greetings or purely educational/definitional questions where every single intent is type=general.
# ────────────────────────────────────────────────────────────
# """

# GREETING_PROMPT = """\
# You are EQUIFIZ — a friendly Indian stock-market AI assistant.
# Respond warmly and briefly to the user's greeting or small talk.

# User: {query}
# """

# MCP_SYNTHESIS_PROMPT = """\
# You are a knowledgeable Indian equity, mutual fund, and fixed-income analyst assistant.

# ## Contextual Grounding Rules (CRITICAL)
# - You MUST rely strictly on the canonical records tagged inside the "Live Data Retrieved" block.
# - NEVER mix up records between distinct data segments. For instance, if data for NIFTY IT contains a specific list of stocks, do NOT mention or inject them into descriptions of the NIFTY DEFENCE segment.
# - If the live data returned for a requested entity is completely empty, zeroed, or displays database lookup indicators like "[]" or None, you must explicitly state that no data was returned for that entity, rather than guessing or filling it using historical knowledge.

# ## Data Safety & Quality Guardrails (STRICT)
# - NEVER expose internal database identifiers or structural system keys (such as "co_code", "mf_schcode").
# - Check the parsed data for structural backend issues. If it indicates server configuration warnings or system failures, explain to the user that the information is currently inaccessible.

# ## Conversation history (last 4 turns)
# {history}

# ## User Question
# {user_query}

# ## Live Data Retrieved
# {mcp_result}

# ## Instructions
# 1. Answer EACH entity component sequentially using only the explicitly labeled live data block provided for it.
# 2. If data is missing or marked as an error, briefly state that it cannot be verified right now.
# 3. Write in plain prose paragraphs only. No markdown, no bullet points, no asterisks, no tables. Do NOT use the rupee symbol — write "Rs".
# 4. Use Indian number formatting (lakh, crore) for big values.
# 5. Keep under 300 words.
# 6. End with: This is not financial advice.
# """

# GENERAL_SYNTHESIS_PROMPT = """\
# You are a knowledgeable Indian equity and fixed-income analyst assistant.

# ## Conversation history (last 4 turns)
# {history}

# ## User Question
# {user_query}

# ## Knowledge Base Context
# {vector_context}

# ## Instructions
# 1. Answer clearly and concisely.
# 2. Plain prose only — no markdown. Do NOT use the rupee symbol — write "Rs".
# 3. Under 200 words.
# """


# # ── LLM helpers ───────────────────────────────────────────────────────────────

# def _llm_json(prompt: str) -> dict:
#     resp  = llm.invoke([HumanMessage(content=prompt)])
#     text  = resp.content.strip()
#     clean = re.sub(r"^`{3}(?:json)?|`{3}$", "", text, flags=re.MULTILINE).strip()

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
# # Node AIQ: Classify + Extract + Decompose
# # ══════════════════════════════════════════════════════════════════════════════

# def node_classify_extract_and_decompose(state: AgentState) -> AgentState:
#     console.print("[Node AIQ] Classify + extract + decompose (v11.5-Hardened)")

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
#                 sector_name        = item.get("sector_name") or None,  # Mapped down here
#                 resolved_name      = None,  
#                 tool_hint          = "",
#                 matched_tools      = [],
#                 resolved_codes      = {},
#                 mcp_result         = "",
#                 code_missing       = False,
#             ))

#         if not intents and qt not in ("greeting",):
#             console.print("  ⚠ LLM returned no intents — fallback single general intent")
#             intents.append(IntentItem(
#                 entity             = user_query,
#                 entity_type        = "general",
#                 intent_description = user_query,
#                 scheme_name        = None,
#                 amc_name           = None,
#                 nse_symbol         = None,
#                 sector_name        = None,
#                 resolved_name      = None,
#                 tool_hint          = "",
#                 matched_tools      = [],
#                 resolved_codes      = {},
#                 mcp_result         = "",
#                 code_missing       = False,
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
#             state["sector_name"]          = first.get("sector_name")  # Propagated to top-level state
#         else:
#             state["primary_entity_type"]  = "general"
#             state["primary_query_intent"] = user_query
#             state["extracted_entity"]     = user_query
#             state["mcp_scheme_name"]      = None
#             state["mcp_amc_name"]         = None
#             state["nse_symbol"]           = None
#             state["sector_name"]          = None

#         state["companies"] = [
#             {
#                 "name":         i.get("entity", ""),
#                 "entity_type":  i.get("entity_type", "general"),
#                 "tool_hint":    "",
#                 "query_intent": i.get("intent_description", ""),
#                 "scheme_name":  i.get("scheme_name"),
#                 "amc_name":     i.get("amc_name"),
#                 "nse_symbol":   i.get("nse_symbol"),
#                 "sector_name":  i.get("sector_name"),
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
#         state["sector_name"]          = None

#     return state


# # ── Chroma tool registry helpers (read-only) ──────────────────────────────────

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
#                 "description":          meta.get("description", ""),
#                 "required_parameters": req_params,
#                 "parameters":           meta.get("parameters", ""),
#                 "score":                round(1 - dist, 3),
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
#                 "description":          meta.get("description", ""),
#                 "required_parameters": req_params,
#                 "parameters":           meta.get("parameters", ""),
#             }
#     except Exception as e:
#         console.print(f"  [ToolRegistry] get by id '{tool_name}' failed: {e}")
#     return None


# # ══════════════════════════════════════════════════════════════════════════════
# # Node TVS: Per-intent tool vector search  (v11.5)
# # ══════════════════════════════════════════════════════════════════════════════

# _ALLOWED_FAMILIES: dict[str, frozenset[str]] = {
#     "stock":        frozenset({"stock"}),
#     "ipo_stock":    frozenset({"ipo_stock"}),
#     "ipo_list":     frozenset({"ipo_list"}),
#     "mf_scheme":    frozenset({"mf_scheme"}),
#     "mf_amc":       frozenset({"mf_amc"}),
#     "etf":          frozenset({"etf"}),
#     "index":        frozenset({"index"}),
#     "market":       frozenset({"market"}),
#     "bond":         frozenset({"bond"}),
#     "sector":       frozenset({"sector"}), # Added Sector family filter matching
#     "exchange":     frozenset(),
#     "nfo":          frozenset(),
#     "news":         frozenset(),
#     "announcement": frozenset(),
#     "market_info":  frozenset(),
#     "general":      frozenset(),
# }


# def node_per_intent_tool_search(state: AgentState) -> AgentState:
#     console.print("[Node TVS] Per-intent tool vector search (v11.5)")

#     intents = state.get("intents") or []
#     if not intents:
#         console.print("  ⚠ No intents to search for")
#         return state

#     def _search_for_intent(intent: IntentItem) -> IntentItem:
#         et = intent.get("entity_type", "general")

#         if et == "general":
#             console.print(
#                 f"  [{intent['entity']}] entity_type=general → skip tool search"
#             )
#             return IntentItem(
#                 entity             = intent.get("entity", ""),
#                 entity_type        = "general",
#                 intent_description = intent.get("intent_description", ""),
#                 scheme_name        = intent.get("scheme_name"),
#                 amc_name           = intent.get("amc_name"),
#                 nse_symbol         = intent.get("nse_symbol"),
#                 sector_name        = intent.get("sector_name"),
#                 resolved_name      = None,
#                 tool_hint          = "",
#                 matched_tools      = [],
#                 resolved_codes      = {},
#                 mcp_result         = "",
#                 code_missing       = False,
#             )

#         if et in _NO_DB_MCP_TYPES:
#             search_query = intent["intent_description"]
#         else:
#             search_query = f"{et} {intent['intent_description']}"

#         tools = _query_tool_registry(search_query, n_results=10)

#         allowed = _ALLOWED_FAMILIES.get(et, frozenset())
#         if allowed:
#             filtered = [
#                 t for t in tools
#                 if _infer_entity_type_from_tool(t["tool_name"]) in allowed
#             ]
#             if filtered:
#                 tools = filtered
#                 console.print(
#                     f"  [TVS] family filter '{et}' → kept {len(tools)} tools"
#                 )
#             else:
#                 console.print(
#                     f"  [TVS] ⚠ family filter '{et}' eliminated all tools — keeping unfiltered results"
#                 )

#         tool_hint = tools[0]["tool_name"] if tools else ""

#         if tool_hint:
#             console.print(
#                 f"  [{intent['entity']}] intent='{intent['intent_description'][:50]}' → top tool={tool_hint}"
#             )
#         return IntentItem(
#             entity             = intent.get("entity", ""),
#             entity_type        = et,
#             intent_description = intent.get("intent_description", ""),
#             scheme_name        = intent.get("scheme_name"),
#             amc_name           = intent.get("amc_name"),
#             nse_symbol         = intent.get("nse_symbol"),
#             sector_name        = intent.get("sector_name"),
#             resolved_name      = None,
#             tool_hint          = tool_hint,
#             matched_tools      = tools,
#             resolved_codes      = {},
#             mcp_result         = "",
#             code_missing       = False,
#         )

#     updated: dict[int, IntentItem] = {}
#     with ThreadPoolExecutor(max_workers=min(6, len(intents))) as ex:
#         futures = {ex.submit(_search_for_intent, intent): idx for idx, intent in enumerate(intents)}
#         for future in as_completed(futures):
#             idx = futures[future]
#             try:
#                 updated[idx] = future.result()
#             except Exception as exc:
#                 console.print(f"  [red]TVS failed for intent #{idx}: {exc}[/red]")
#                 original     = intents[idx]
#                 updated[idx] = IntentItem(
#                     entity             = original.get("entity", ""),
#                     entity_type        = original.get("entity_type", "general"),
#                     intent_description = original.get("intent_description", ""),
#                     scheme_name        = original.get("scheme_name"),
#                     amc_name           = original.get("amc_name"),
#                     nse_symbol         = original.get("nse_symbol"),
#                     sector_name        = original.get("sector_name"),
#                     resolved_name      = None,
#                     tool_hint          = "",
#                     matched_tools      = [],
#                     resolved_codes      = {},
#                     mcp_result         = "",
#                     code_missing       = False,
#                 )

#     state["intents"] = [updated[i] for i in sorted(updated)]

#     if state["intents"]:
#         first = state["intents"][0]
#         state["matched_tools"]     = first.get("matched_tools") or []
#         state["mcp_tool_hint"]     = first.get("tool_hint") or ""
#         state["primary_tool_hint"] = first.get("tool_hint") or ""

#     return state


# # ── Greeting handler ──────────────────────────────────────────────────────────

# def node_greeting_handler(state: AgentState) -> AgentState:
#     console.print("[Node B] Greeting handler")
#     state["final_answer"] = _llm_text(GREETING_PROMPT.format(query=state["user_query"]))
#     return state


# # ── General handler (vector search fallback) ──────────────────────────────────

# def node_general_handler(state: AgentState) -> AgentState:
#     console.print("[Node C] General handler (vector search fallback)")
#     try:
#         state["vector_context"] = vs.query(state["user_query"], n_results=5)
#     except Exception as e:
#         console.print(f"  General vector search failed: {e}")
#         state["vector_context"] = []
#     return state


# # ══════════════════════════════════════════════════════════════════════════════
# # FIX M — Hardened Multi-Tiered Fuzzy Match Scoring System
# # ══════════════════════════════════════════════════════════════════════════════

# def _compute_smart_score(target: str, candidate: str) -> float:
#     target_clean = target.lower().strip()
#     candidate_clean = candidate.lower().strip()
    
#     if target_clean == candidate_clean:
#         return 100.0
        
#     ts_ratio = fuzz.token_sort_ratio(target_clean, candidate_clean)
#     ratio = fuzz.ratio(target_clean, candidate_clean)
#     part_ratio = fuzz.partial_ratio(target_clean, candidate_clean)
    
#     target_words = set(target_clean.split())
#     candidate_words = set(candidate_clean.split())
    
#     has_exact_word_match = any(word in candidate_words for word in target_words)
#     score = max(ts_ratio, ratio)
    
#     if part_ratio > 90.0:
#         if has_exact_word_match:
#             score = max(score, part_ratio)
#         else:
#             score = max(score, part_ratio - 35.0)
#     else:
#         score = max(score, part_ratio)
        
#     return float(score)


# def _resolve_single(name: str, nse_symbol: Optional[str]) -> dict:
#     name_upper = name.upper().strip()
#     symbol_to_check = nse_symbol.upper().strip() if nse_symbol else name_upper
    
#     if symbol_to_check:
#         row = db.lookup_by_nse_symbol(symbol_to_check)
#         if row:
#             return {
#                 "co_code":      row["co_code"],
#                 "company_info": row,
#                 "nse_symbol":   row.get("nsesymbol"),
#                 "resolved_name": row.get("companyname"),
#             }

#     matches = db.fuzzy_search_company(name, limit=15)
#     if matches:
#         for cand in matches:
#             cand_sym = str(cand.get("nsesymbol") or "").upper().strip()
#             if cand_sym == name_upper:
#                 return {
#                     "co_code":      cand["co_code"],
#                     "company_info": cand,
#                     "nse_symbol":   cand_sym,
#                     "resolved_name": cand.get("companyname"),
#                 }

#         best_match = None
#         best_score = -1.0
        
#         for cand in matches:
#             cand_name = cand.get("companyname") or ""
#             cand_sym  = cand.get("nsesymbol") or ""
            
#             score_name = _compute_smart_score(name, cand_name)
#             score_sym  = _compute_smart_score(name, cand_sym)
#             score = max(score_name, score_sym)
            
#             if score > best_score:
#                 best_score = score
#                 best_match = cand
                
#         if best_match and best_score >= 50.0:
#             return {
#                 "co_code":      best_match["co_code"],
#                 "company_info": best_match,
#                 "nse_symbol":   best_match.get("nsesymbol"),
#                 "resolved_name": best_match.get("companyname"),
#             }
            
#     return {}


# # ══════════════════════════════════════════════════════════════════════════════
# # DB targeted table lookup (Fully Dynamic Intersection Matrix + Exchange Affinity)
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

#         if table == "scheme_master":
#             cur.execute("SELECT mf_schcode, mf_cocode, sch_name, category FROM scheme_master")
#         elif table == "bond_master":
#             cur.execute("SELECT code, companyname, isin, nsesymbol FROM bond_master")
#         elif table == "companies" and name_col == "sector_name":
#             # Distinct dynamic selector to optimize localized industry extraction performance
#             cur.execute("SELECT DISTINCT sectorcode, sectorname FROM companies WHERE sectorcode IS NOT NULL")
#         else:
#             cur.execute(f"SELECT * FROM {table}")

#         rows = [dict(r) for r in cur.fetchall()]
#         cur.close()
#         conn.close()

#         entity_upper = entity.upper()
#         entity_clean = re.sub(r"[^A-Za-z0-9]", "", entity_upper).strip()
#         query_tokens = {t for t in re.split(r"[^A-Za-z0-9]", entity_upper) if len(t) > 1}

#         best_match: Optional[dict] = None
#         best_score: float          = -100.0  

#         for row in rows:
#             candidate = str(row.get(name_col) or row.get("group") or row.get("group_name") or row.get("groupname") or "")
#             cand_upper = candidate.upper()
#             cand_clean = re.sub(r"[^A-Za-z0-9]", "", cand_upper).strip()
            
#             score = float(_compute_smart_score(entity, candidate))
            
#             if table == "group_master":
#                 cand_tokens = set(re.split(r"[^A-Za-z0-9]", cand_upper))
#                 if len(cand_clean) >= 8 and "_" not in cand_upper:
#                     chunks = [cand_clean[i:i+3] for i in range(0, len(cand_clean), 3)]
#                     cand_tokens.update(chunks)

#                 is_exact_sector_match = False
#                 for q_tok in query_tokens:
#                     if q_tok not in ("NIFTY", "BSE", "NSE", "INDEX"):
#                         if cand_upper.endswith(f"_{q_tok}") or cand_clean.endswith(q_tok):
#                             is_exact_sector_match = True

#                 row_exchange = str(row.get("exchange") or "").upper().strip()
#                 has_nse_affinity = any(k in entity_clean for k in ("NIFTY", "NSE"))
#                 has_bse_affinity = any(k in entity_clean for k in ("BSE", "SENSEX"))

#                 exchange_mismatch = False
#                 if has_nse_affinity and row_exchange == "BSE":
#                     exchange_mismatch = True
#                 elif has_bse_affinity and row_exchange == "NSE":
#                     exchange_mismatch = True

#                 has_token_intersection = any(
#                     (q_tok in cand_clean or any(q_tok in c_tok or c_tok in q_tok for c_tok in cand_tokens))
#                     for q_tok in query_tokens if q_tok not in ("NIFTY", "BSE", "NSE")
#                 )

#                 is_exact_root = (cand_clean in ("NIFTY", "BSE", "NSE"))
#                 query_has_modifiers = len(query_tokens - {"NIFTY", "BSE", "NSE", "INDEX", "STOCKS"}) > 0

#                 if exchange_mismatch:
#                     score = -200.0  
#                 elif is_exact_sector_match:
#                     score = 150.0  
#                 elif is_exact_root and query_has_modifiers:
#                     score -= 75.0  
#                 elif has_token_intersection:
#                     score += 35.0

#             if table == "bond_master":
#                 isin_score = fuzz.ratio(entity.upper(), str(row.get("isin") or "").upper())
#                 sym_score = fuzz.ratio(entity.upper(), str(row.get("nsesymbol") or "").upper())
#                 score = max(score, isin_score, sym_score)

#             if score > best_score:
#                 best_score, best_match = score, row

#         if best_match and best_score >= 45.0:
#             resolved_id = best_match.get(id_col) or best_match.get("indexcode") or best_match.get("code")
#             resolved_name = best_match.get(name_col) or best_match.get("group") or best_match.get("group_name")
#             return {
#                 id_col: resolved_id,
#                 name_col: resolved_name
#             }
#         return None

#     except Exception as e:
#         console.print(f"    [_targeted_db_lookup] DB error on {table}: {e}")
#         return None


# def _get_required_params_for_entity(
#     tool_hint:     str,
#     entity_type:   str,
#     matched_tools: list[dict],
# ) -> list[str]:
#     params = ENTITY_TYPE_PARAM_MAP.get(entity_type)
#     if params is not None:
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

#     return []


# # ── Per-intent DB code resolver ──────────────────────────────────────────────

# def _resolve_entity_codes(
#     name:            str,
#     scheme_name:     Optional[str],
#     amc_name:        Optional[str],
#     nse_symbol:      Optional[str],
#     sector_name:     Optional[str],  # Added method interface reference
#     required_params: list[str],
# ) -> dict:
#     codes:  dict = {}
#     result: dict = {"name": name}

#     needs_schcode   = "mf_schcode"  in required_params
#     needs_cocode    = "mf_cocode"   in required_params
#     needs_co_code   = "co_code"     in required_params
#     needs_isin      = "isin"        in required_params
#     needs_indexcode = "index_code"  in required_params
#     needs_group     = "group"       in required_params
#     needs_bond_code = "bond_code"   in required_params
#     needs_sectorcode = "sector_code" in required_params  # Hooked parameter constraint

#     if needs_schcode:
#         search = scheme_name or name
#         cfg    = PARAM_TO_TABLE_MAP["mf_schcode"]
#         match  = _targeted_db_lookup(entity=search, table=cfg["table"], name_col=cfg["column"], id_col=cfg["id_field"])
#         if match:
#             codes["mf_schcode"] = int(match["mf_schcode"])
#             if "mf_cocode" in match:
#                 codes.setdefault("mf_cocode", int(match["mf_cocode"]))
#             result["resolved_scheme_name"] = match.get("sch_name", search)

#     if needs_cocode and not codes.get("mf_cocode"):
#         search = amc_name or name
#         cfg    = PARAM_TO_TABLE_MAP["mf_cocode"]
#         match  = _targeted_db_lookup(entity=search, table=cfg["table"], name_col=cfg["column"], id_col=cfg["id_field"])
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
#                 "resolved_company_name": stock["resolved_name"]
#             })
#         else:
#             result["code_missing"] = True

#     if needs_isin:
#         search = scheme_name or name
#         cfg    = PARAM_TO_TABLE_MAP["isin"]
#         match  = _targeted_db_lookup(entity=search, table=cfg["table"], name_col=cfg["column"], id_col=cfg["id_field"])
#         if match:
#             codes["isin"] = match["isin"]
#             result["resolved_etf_name"] = match.get("etfname", search)

#     if needs_indexcode:
#         cfg   = PARAM_TO_TABLE_MAP["index_code"]
#         match = _targeted_db_lookup(entity=name, table=cfg["table"], name_col=cfg["column"], id_col=cfg["id_field"])
#         if match:
#             try:
#                 codes["index_code"] = int(float(match[cfg["id_field"]]))
#             except (ValueError, TypeError, KeyError):
#                 codes["index_code"] = int(float(match.get("indexcode") or match.get("index_code")))
#             result["resolved_index_name"] = match.get(cfg["column"]) or match.get("group") or match.get("group_name")

#     if needs_group:
#         cfg   = PARAM_TO_TABLE_MAP["group"]
#         match = _targeted_db_lookup(entity=name, table=cfg["table"], name_col=cfg["column"], id_col=cfg["id_field"])
#         if match:
#             codes["group"] = match.get(cfg["id_field"]) or match.get("group") or match.get("group_name")
#             result["resolved_group_name"] = codes["group"]
#         else:
#             codes["group"] = name
#             result["resolved_group_name"] = name

#     if needs_bond_code:
#         cfg   = PARAM_TO_TABLE_MAP["bond_code"]
#         match = _targeted_db_lookup(entity=name, table=cfg["table"], name_col=cfg["column"], id_col=cfg["id_field"])
#         if match:
#             codes["bond_code"] = int(match["code"])
#             result["resolved_bond_name"] = match.get("companyname", name)
#         else:
#             result["code_missing"] = True

#     if needs_sectorcode:
#         search = sector_name or name
#         cfg    = PARAM_TO_TABLE_MAP["sector_code"]
#         match  = _targeted_db_lookup(entity=search, table=cfg["table"], name_col=cfg["column"], id_col=cfg["id_field"])
#         if match:
#             try:
#                 codes["sector_code"] = int(float(match[cfg["id_field"]]))
#             except (ValueError, TypeError):
#                 codes["sector_code"] = match[cfg["id_field"]]
#             result["resolved_sector_name"] = match.get(cfg["column"], search)
#         else:
#             result["code_missing"] = True

#     result["mcp_resolved_codes"] = codes
#     return result


# def _resolve_intents_codes(intents: list[IntentItem]) -> list[IntentItem]:
#     def _resolve_one(intent: IntentItem) -> IntentItem:
#         et        = intent.get("entity_type", "general")
#         tool_hint = intent.get("tool_hint", "")
#         matched   = intent.get("matched_tools") or []

#         if et == "general" and tool_hint:
#             inferred = _infer_entity_type_from_tool(tool_hint)
#             if inferred != "general":
#                 et = inferred
#                 intent["entity_type"] = et

#         req_params = _get_required_params_for_entity(tool_hint, et, matched)

#         if not req_params:
#             intent["resolved_codes"] = {}
#             intent["code_missing"]   = False
#             intent["resolved_name"]  = intent.get("entity")
#             return intent

#         resolved = _resolve_entity_codes(
#             name            = intent.get("entity", ""),
#             scheme_name     = intent.get("scheme_name"),
#             amc_name        = intent.get("amc_name"),
#             nse_symbol      = intent.get("nse_symbol"),
#             sector_name     = intent.get("sector_name"),  # Parsed Sector element passed
#             required_params = req_params,
#         )
#         intent["resolved_codes"] = resolved.get("mcp_resolved_codes", {})
#         intent["code_missing"]   = bool(resolved.get("code_missing", False))
        
#         intent["resolved_name"] = (
#             resolved.get("resolved_company_name") or
#             resolved.get("resolved_scheme_name") or
#             resolved.get("resolved_amc_name") or
#             resolved.get("resolved_etf_name") or
#             resolved.get("resolved_index_name") or
#             resolved.get("resolved_bond_name") or
#             resolved.get("resolved_sector_name") or  # Sector parsing resolution check injection
#             intent.get("entity")
#         )
#         return intent

#     result_intents: dict[int, IntentItem] = {}
#     with ThreadPoolExecutor(max_workers=min(6, len(intents) or 1)) as ex:
#         futures = {ex.submit(_resolve_one, intent): idx for idx, intent in enumerate(intents)}
#         for future in as_completed(futures):
#             idx = futures[future]
#             try:
#                 result_intents[idx] = future.result()
#             except Exception:
#                 result_intents[idx] = intents[idx]

#     return [result_intents[i] for i in sorted(result_intents)]


# # ── MCP Pre-Resolution nodes ──────────────────────────────────────────────

# def node_mcp_pre_resolve(state: AgentState) -> AgentState:
#     console.print("[Node MPR] Single-entity MCP pre-resolve (v11.5)")
#     intents = state.get("intents") or []
#     if not intents:
#         state["mcp_resolved_codes"] = {}
#         return state

#     resolved_intents            = _resolve_intents_codes(intents)
#     state["intents"]            = resolved_intents
#     state["mcp_resolved_codes"] = resolved_intents[0].get("resolved_codes", {}) if resolved_intents else {}
#     return state


# def node_mcp_multi_pre_resolve(state: AgentState) -> AgentState:
#     console.print("[Node MMPR] Multi-entity MCP pre-resolve (v11.5)")
#     intents = state.get("intents") or []
#     if not intents:
#         return state

#     resolved_intents            = _resolve_intents_codes(intents)
#     state["intents"]            = resolved_intents
#     state["mcp_resolved_codes"] = resolved_intents[0].get("resolved_codes", {}) if resolved_intents else {}
#     return state


# # ── Node MTCI: MCP Tool Call ──────────────────────────────────────────────────

# def _run_mcp_multi_in_thread(intents: list[dict]) -> list[dict]:
#     from mcp_client import run_mcp_query_multi, trim_results
#     loop = asyncio.new_event_loop()
#     asyncio.set_event_loop(loop)
#     try:
#         results = loop.run_until_complete(run_mcp_query_multi(intents))
#         return trim_results(results)
#     except Exception as e:
#         console.print(f"[bold red]MCP Multi Error:[/bold red] {e}")
#         return [{**i, "mcp_result": f"Error: {e}"} for i in intents]
#     finally:
#         loop.close()


# def node_mcp_tool_call_intents(state: AgentState) -> AgentState:
#     console.print("[Node MTCI] Sequential per-intent MCP calls — one session (v11.5)")

#     try:
#         import mcp_client  # noqa: F401
#     except ImportError:
#         state["mcp_raw_result"] = ""
#         state["intent_results"] = []
#         state["error"]          = "mcp_client_not_found"
#         return state

#     intents    = state.get("intents") or []
#     user_query = state.get("user_query", "")

#     mcp_intents: list[IntentItem]     = []
#     blocked_intents: list[IntentItem] = []
#     general_intents: list[IntentItem] = []

#     for i in intents:
#         et           = i.get("entity_type", "general")
#         has_tool     = bool(i.get("tool_hint"))
#         code_missing = bool(i.get("code_missing", False))

#         if et == "general" and not has_tool:
#             general_intents.append(i)
#         elif code_missing:
#             blocked_intents.append(i)
#         else:
#             mcp_intents.append(i)

#     if not mcp_intents:
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

#     try:
#         with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
#             future         = ex.submit(_run_mcp_multi_in_thread, mcp_intents)
#             filled_intents = future.result(timeout=600)
#     except Exception as exc:
#         filled_intents = [{**dict(i), "mcp_result": f"MCP call failed: {exc}"} for i in mcp_intents]

#     all_intents = (
#         filled_intents
#         + [{**dict(i), "mcp_result": "(answered from knowledge base)"} for i in general_intents]
#         + [{**dict(i), "mcp_result": f"Sorry, I could not find '{i.get('entity')}' in our database."} for i in blocked_intents]
#     )
#     state["intents"] = all_intents

#     state["intent_results"] = [
#         {
#             "entity":             i.get("entity", ""),
#             "resolved_name":      i.get("resolved_name", i.get("entity", "")),
#             "entity_type":        i.get("entity_type", ""),
#             "intent_description": i.get("intent_description", ""),
#             "mcp_result":         i.get("mcp_result", ""),
#         }
#         for i in all_intents
#     ]

#     valid_segments = []
#     for r in state["intent_results"]:
#         res_str = str(r.get("mcp_result", ""))
        
#         if any(indicator in res_str.lower() for indicator in ["<html", "502 bad gateway", "nginx/", "404 not found", "error response"]):
#             res_str = "Error: Upstream market analytics interface returned an invalid protocol wrapper. The server is busy. Please rephrase shortly."
#             state["error"] = "upstream_infrastructure_leak"
            
#         valid_segments.append(
#             f"--- RESOLVED ENTITY CANONICAL DATA FOR: {r.get('resolved_name', r['entity']).upper()} ---\n"
#             f"[Requested Segment: {r['intent_description']}]\n"
#             f"{res_str}"
#         )

#     state["mcp_raw_result"] = "\n\n---\n\n".join(valid_segments)
#     state["mcp_tool_calls_made"] = []
#     return state


# _MCP_ERROR_MESSAGES = {
#     "mcp_client_not_found": "Sorry, the live data service is currently unavailable. Please try again shortly.",
#     "mcp_call_timeout": "The live data request timed out. The server may be busy — please try again in a moment.",
#     "upstream_infrastructure_leak": "The data connection to the market stream encountered an operational disruption. I could not verify the requested information right now. Please rephrase shortly."
# }

# # ── MCP Synthesis (v11.5) ────────────────────────────────────────────────────

# def node_mcp_synthesis(state: AgentState) -> AgentState:
#     console.print("[Node MS] MCP synthesis (v11.5)")
#     error = state.get("error", "")
#     mcp_result = state.get("mcp_raw_result", "")

#     if error in _MCP_ERROR_MESSAGES:
#         state["final_answer"] = _MCP_ERROR_MESSAGES[error]
#         return state
        
#     if not mcp_result:
#         state["final_answer"] = "No data was returned from the live server. Please try again shortly."
#         return state

#     mcp_snippet = mcp_result
#     if len(mcp_snippet) > 10000:
#         mcp_snippet = mcp_snippet[:7500] + "\n\n... [middle trimmed for length] ...\n\n" + mcp_snippet[-2000:]

#     prompt = MCP_SYNTHESIS_PROMPT.format(
#         history    = _format_history(state),
#         user_query = state.get("user_query", ""),
#         mcp_result = mcp_snippet,
#     )
#     try:
#         state["final_answer"] = _llm_text(prompt)
#     except Exception as e:
#         state["final_answer"] = mcp_result
#     return state


# def node_synthesis(state: AgentState) -> AgentState:
#     console.print("[Node S] General synthesis (vector search)")
#     vector_context = state.get("vector_context") or []
#     context_text   = "\n".join(f"- [{r['metadata'].get('api_name', '')}] {r['document']}" for r in vector_context)
#     prompt = GENERAL_SYNTHESIS_PROMPT.format(
#         history        = _format_history(state),
#         user_query     = state["user_query"],
#         vector_context = context_text or "No relevant context found.",
#     )
#     try:
#         state["final_answer"] = _llm_text(prompt)
#     except Exception as e:
#         state["final_answer"] = f"Synthesis failed: {e}"
#     return state


# # ── Router (v11.5) ────────────────────────────────────────────────────────────

# def route_after_classify(state: AgentState) -> str:
#     qt      = state.get("query_type", "general")
#     intents = state.get("intents") or []

#     if qt == "greeting":
#         return "greeting"

#     mcp_intents = [i for i in intents if i.get("tool_hint") or i.get("entity_type") not in ("general",)]

#     if not mcp_intents or (not any(i.get("tool_hint") for i in intents) and all(i.get("entity_type") == "general" for i in intents)):
#         return "general"

#     has_many         = len(intents) > 1
#     additional       = state.get("companies") or []
#     primary_type     = state.get("primary_entity_type", "general")
#     secondary_types  = {c.get("entity_type", "general") for c in additional}
#     is_heterogeneous = bool(secondary_types - {"general"} - {primary_type})

#     if has_many or qt == "comparison" or is_heterogeneous:
#         return "multi_mcp"

#     return "mcp_direct"


# # ── Graph Build Sequence ──────────────────────────────────────────────────────

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


# _graph = None

# def get_graph():
#     global _graph
#     if _graph is None:
#         _graph = build_graph()
#     return _graph


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






from __future__ import annotations

import asyncio
import concurrent.futures
import json
import logging
import re
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
logger = logging.getLogger(__name__)
TODAY = str(date.today())

# ── LLM ───────────────────────────────────────────────────────────────────────

llm = ChatOllama(base_url=OLLAMA_BASE_URL, model=OLLAMA_MODEL)

# ── Types ─────────────────────────────────────────────────────────────────────

class Turn(TypedDict):
    role: str       # "user" | "assistant"
    content: str


class IntentItem(TypedDict, total=False):
    entity:             str
    entity_type:        str
    intent_description: str
    tool_hint:          str
    scheme_name:        Optional[str]
    amc_name:           Optional[str]
    nse_symbol:         Optional[str]
    sector_name:        Optional[str]  
    resolved_name:      Optional[str]  
    matched_tools:      list[dict]
    resolved_codes:     dict
    mcp_result:         str
    code_missing:       bool           


_NO_DB_MCP_TYPES = frozenset({
    "exchange",
    "ipo_list",
    "nfo",
    "news",
    "announcement",
    "market_info",
})

# ── Tool Family Mapping Matrix ───────────────────────────────────────────────
_TOOL_FAMILY_HINTS: dict[str, str] = {
    "get_company_stock_price": "stock",
    "get_delayed_stock_price": "stock",
    "get_nse_company_announcements":        "stock",
    "get_bse_company_announcements":        "stock",
    "get_company_result_schedule":          "stock",
    "get_company_profile":                  "stock",
    "get_company_background":               "stock",
    "get_board_of_directors":               "stock",
    "get_company_bankers":                  "stock",
    "get_management_biodata":               "stock",
    "get_subsidiaries_jvs":                 "stock",
    "get_related_party_transactions":       "stock",
    "get_employee_count":                   "stock",
    "get_capital_structure":                "stock",
    "get_pledge_share_details":             "stock",
    "get_substantial_acquisitions":         "stock",
    "get_segment_data":                     "stock",
    "get_r_and_d_expenditure":              "stock",
    "get_finished_products":                "stock",
    "get_raw_materials":                    "stock",
    "get_chronological_history":            "stock",
    "get_company_history":                  "stock",
    "get_funds_holding_company":            "stock",
    "get_quarterly_results":                "stock",
    "get_profit_loss":                      "stock",
    "get_balance_sheet":                    "stock",
    "get_cash_flow":                        "stock",
    "get_half_yearly_results":              "stock",
    "get_nine_months_results":              "stock",
    "get_quarterly_trends" :                 "stock",
    "get_shareholding_pattern":             "stock",
    "get_major_shareholders":               "stock",
    "get_quarterly_balance_sheet" :         "stock",
    "get_yearly_results":                   "stock",
    "get_annual_balance_sheet":              "stock",
    "get_half_yearly_balance_sheet":         "stock",
    "get_ttm_growth_trends":                 "stock",
    "get_quarterly_revenue_trends":          "stock",
    "get_quarterly_ebitda_trends":           "stock",
    "get_quarterly_ebit_trends":             "stock",
    "get_growth_data_quarterly":             "stock",
    "get_growth_data_yearly":                "stock",
    "get_key_financial_ratios":             "stock",
    "get_daily_ratios":                     "stock",
    "get_margin_ratios":                    "stock",
    "get_valuation_ratios":                 "stock",
    "get_all_basic_ratios":                 "stock",
    "get_return_ratios":                    "stock",
    "get_growth_ratios":                    "stock",
    "get_performance_ratios":                "stock",
    "get_efficiency_ratios":                 "stock",
    "get_cashflow_ratios":                  "stock",
    "get_liquidity_ratios":                 "stock",
    "get_solvency_ratios":                  "stock",
    "get_quarterly_ratios":                 "stock",
    "get_yearly_ratios":                    "stock",
    "get_shareholding":                     "stock",
    "get_major_sharehold":                  "stock",
    "get_financial_stability_ratios":       "stock",
    "get_anchor_investor":                  "ipo_stock",
    "get_ipo_details":                      "ipo_stock",
    "get_ipo_subscription_status":          "ipo_stock",
    "get_ipo_synopsis":                     "ipo_stock",
    "get_ipo_timeline":                     "ipo_stock",
    "get_ipo_promoter_details":             "ipo_stock",
    "get_ipo_listing_info":                 "ipo_stock",
    "get_ipo_objects_of_issue":             "ipo_stock",
    "get_ipo_anchor_investor_details":      "ipo_stock",
    "get_ipo_industry_peers":                "ipo_stock",
    "get_ipo_financials":                   "ipo_stock",
    "get_ipo_product_services":             "ipo_stock",
    "get_ipo_strength_details":             "ipo_stock",
    "get_ipo_strategy_details":             "ipo_stock",
    "get_ipo_risk_details":                 "ipo_stock",
    "get_ipo_selling_shareholders":         "ipo_stock",
    "get_ipo_allocation_details":           "ipo_stock",
    "get_ipo_prospectus":                   "ipo_stock",
    "get_ipo_lead_managers":                "ipo_stock",
    "get_ipo_registrar":                    "ipo_stock",
    "get_ipo_customer_details":              "ipo_stock",
    "get_market_indices":                    "stock",   
    "get_index_companies":                  "index",
    "get_index_constituents":               "index",
    "get_active_performer":                 "market",
    "get_top_gainers":                      "market",
    "get_top_losers":                       "market",
    "get_out_under_performers":              "market",
    "get_52week_highs":                      "market",
    "get_52week_lows":                      "market",
    "get_new_highs_lows":                   "market",
    "get_sector_companies":                 "sector",
    "get_advance_decline":                  "exchange",
    "get_exchange_holidays":                "exchange",
    "get_scheme_nav":                       "mf_scheme",
    "get_investment_details":                "mf_scheme",
    "get_expense_ratio":                    "mf_scheme",
    "get_avg_maturity":                     "mf_scheme",
    "get_scheme_aum":                       "mf_scheme",
    "get_nav_historical":                   "mf_scheme",
    "get_scheme_returns":                   "mf_scheme",
    "get_lumpsum_returns":                  "mf_scheme",
    "get_scheme_sip":                       "mf_scheme",
    "get_mf_holdings":                      "mf_scheme",
    "get_sector_allocation":                "mf_scheme",
    "get_asset_allocation":                 "mf_scheme",
    "get_portfolio_changes":                "mf_scheme",
    "get_mcap_allocation":                  "mf_scheme",
    "get_most_bought_sold":                  "mf_scheme",
    "get_scheme_ratios":                    "mf_scheme",
    "get_dividend_details":                 "mf_scheme",
    "get_bse_star_scheme":                  "mf_scheme",
    "compare_schemes":                      "mf_scheme",
    "get_whats_in_out":                     "mf_scheme",
    "get_scheme_sip_rules":                 "mf_scheme",
    "get_scheme_sip_details":               "mf_scheme",
    "get_fund_categories":                  "mf_amc",
    "get_schemes_by_amc":                   "mf_amc",
    "get_fund_profile":                     "mf_amc",
    "get_fund_managers":                     "mf_amc",
    "get_etf_quotes":                       "etf",
    "get_etf_returns":                       "etf",
    "get_etf_fundamentals":                  "etf",
    "get_etf_about":                        "etf",
    "get_etf_equity_holdings":              "etf",
    "get_etf_monthly_portfolio":               "etf",
    "get_etf_sector_allocatio":             "etf",
    "get_etf_asset_allocation":             "etf",
    "get_debt_eod_prices_scripwise":        "bond",
    "get_bond_details":                     "bond",
    "get_bond_price_history":               "bond",
    "get_bond_cashflow":                    "bond",
    "get_bond_rating":                      "bond",
    "get_bond_redemption":                  "bond",
    "get_bond_interest":                    "bond",
    "get_forthcoming_ipos":                  "ipo_list",
    "get_open_ipos":                        "ipo_list",
    "get_closed_ipos":                      "ipo_list",
    "get_new_ipo_listings":                           "ipo_list",
    "get_best_ipo_performers":                         "ipo_list",
    "get_ipo_master":                       "ipo_list",
    "get_ipo_logo":                         "ipo_list",
    "get_forthcoming_drh_filings":           "ipo_list",
    "get_basis_of_allotment" :               "ipo_list",
    "get_open_bond_ipo":                      "ipo_list",
    "get_forthcoming_bond_ipo" :               "ipo_list",
    "get_new_fund_offer":                   "nfo",
    "get_corporate_news":                   "news",
    "get_mf_news":                          "news",
    "get_mf_market_activity":                     "news",
    "get_bse_announcements":                 "announcement",
    "get_nse_announcements":                 "announcement",
    "get_fund_house":                       "market_info",
    "get_amfi_master":                      "market_info",
    "get_fund_manager":                     "market_info",
    "get_index_list":                       "market_info",
    "get_results_today":                    "market_info",
    "get_result_declarations":              "market_info",
    "get_annual_declarations":              "market_info",
    "get_fund_performance":                  "market_info",
    "get_category_performance":              "market_info",
    "get_sip_dates":                        "market_info",
    "get_macro_economic_data":              "market_info",
    "get_debt_top_value":                   "market_info",
    "get_debt_top_volume":                  "market_info",
    "et_new_fund_offers" :                  "market_info",
    "get_debt_market_watch":                "market_watch",
    "get_index_constituents":               "index"
}


def _infer_entity_type_from_tool(tool_hint: str) -> str:
    if not tool_hint:
        return "general"
    tl = tool_hint.lower().strip()
    
    if tl in _TOOL_FAMILY_HINTS:
        return _TOOL_FAMILY_HINTS[tl]
        
    for tool_name, family in _TOOL_FAMILY_HINTS.items():
        if tool_name in tl or tl.startswith(tool_name):
            return family
            
    return "general"


# ── DB routing ────────────────────────────────────────────────────────────────

PARAM_TO_TABLE_MAP: dict[str, dict[str, str]] = {
    "co_code": {
        "table":    "companies",
        "column":   "companyname",
        "id_field": "co_code",
    },
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
    "isin": {
        "table":    "etf_master",
        "column":   "etfname",
        "id_field": "isin",
    },
    "index_code": {
        "table":    "group_master",
        "column":   "groupname",  
        "id_field": "indexcode",
    },
    "group": {
        "table":    "group_master",
        "column":   "groupname",
        "id_field": "groupname",
    },
    "bond_code": {
        "table":    "bond_master",
        "column":   "companyname",
        "id_field": "code",
    },
    "sector_code": {
        "table":    "companies",      
        "column" :  "sectorname",    
        "id_field": "sectorcode"
    }
}

ENTITY_TYPE_PARAM_MAP: dict[str, list[str]] = {
    "stock":        ["co_code"],
    "ipo_stock":    ["co_code"],    
    "mf_scheme":    ["mf_schcode"],
    "mf_amc":       ["mf_cocode"],
    "etf":          ["isin"],
    "index":        ["index_code"],
    "market":       ["group"],
    "bond":         ["bond_code"],
    "sector":       ["sector_code"],  
    "exchange":     [],
    "ipo_list":     [],
    "nfo":          [],
    "news":         [],
    "announcement": [],
    "market_info":  [],
    "general":      [],
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

    matched_tools:  list[dict]
    intents:        list[IntentItem]
    intent_results: list[dict]

    nse_symbol:   Optional[str]
    co_code:      Optional[int]
    company_info: Optional[dict]
    sector_name:  Optional[str]  

    mcp_resolved_codes: dict
    mcp_scheme_name:    Optional[str]
    mcp_amc_name:       Optional[str]

    companies: list[dict]

    vector_context: list[dict]

    mcp_raw_result:      str
    mcp_tool_calls_made: list[str]

    final_answer: str
    error:        Optional[str]


# ── LLM Prompt Templates ──────────────────────────────────────────────────────

CLASSIFY_EXTRACT_DECOMPOSE_PROMPT = """\\
You are a Lead Financial Systems Analyst for an Indian stock-market assistant. Your task is to decompose user queries into executable data intents.

## Context
- History: {history}
- Today's Date: {today}
- Query: "{query}"

────────────────────────────────────────────────────────────
STEP 1: SUBJECT IDENTIFICATION
Identify every distinct subject the user is asking about. A subject is either:
1. A Specific Entity: (e.g., Reliance Industries, HDFC AMC, Nifty 50, SBI Small Cap Fund, Gold BeES ETF, HDFC NCD bond, Automobile Sector, Financial Sector).
2. A Market Category/Concept: (e.g., "upcoming IPOs", "top gainers", "market news", "bulk deals", "top losers on NSE", "debt market watch").

STEP 2: JSON GENERATION
Generate exactly one intent item for every subject identified in Step 1.

Return ONLY valid JSON:
{{
  "query_type": "<greeting|general|stock|mf_scheme|mf_amc|bond|comparison|investment|sector>",
  "is_broad": <true|false>,
  "mcp_needed": <true|false>,
  "intents": [
    {{
      "entity": "<entity name OR market concept name OR sector name>",
      "entity_type": "<see ENTITY TYPE RULES below>",
      "intent_description": "<specific data point needed for this subject>",
      "scheme_name": "<if mf_scheme, else empty>",
      "amc_name": "<if mf_amc, else empty>",
      "nse_symbol": "<NSE ticker if known, else empty>",
      "sector_name": "<if industry sector entity, extract clean sector name here, else empty>"
    }}
  ]
}}

────────────────────────────────────────────────────────────
ENTITY TYPE RULES (use exactly one of these values):

  sector        → Specific macro-economic industry or business sector categories.
                 Use when the user asks about lists of companies within an industry, sector-wise 
                 performance metrics, sector allocations, or company tracking inside standard 
                 macro-economic domains (e.g., Finance, IT, FMCG, Automobile, Pharma, Infrastructure, Energy).
                 ⚠ CRITICAL RULE FOR SECTOR OVERRIDE: If the query mentions phrases like 
                 "stocks under the X sector", "companies in the X industry", "X sector stocks list", 
                 or "X sector index", you MUST select sector (NOT index, NOT stock, and NOT market).
                 The word "index" in a phrase like "finance sector index" or "IT sector index" refers 
                 to the sectoral classification metrics tracking—it is a sector intent.
                 Examples: "List of stocks under the finance sector index" → sector, 
                 "companies in automobile sector" → sector, "IT sector stocks" → sector.

  index         → A concrete capital market benchmark INDEX structure. 
                 Use ONLY when the user asks about components, constituents, or companies listing within an explicit, named 
                 exchange-traded index (e.g., Nifty 50, Bank Nifty, Nifty Midcap 100, Nifty Smallcap 250, Sensex).
                 ⚠ CRITICAL TOOL MATCHING HINT: Index requests MUST map exclusively to tools that fetch index constituents 
                 (like get_index_constituents or get_index_companies). Do not let index requests drift into IPO or stock price pools.
                 Examples: "Nifty 50 constituents", "stocks under nifty midcap", "Sensex component stocks list".

  stock         → A specific listed company's EQUITY data. Use when the user asks about
                 share price, stock quote, OHLC, 52-week high/low, delivery volume,
                 delayed price, company profile, background, board of directors, bankers,
                 management team, subsidiaries, related parties, employee count, capital
                 structure, pledged shares, substantial shareholders, segment data, R&D,
                 finished products, raw materials, chronological events, company history,
                 quarterly/annual financial results, P&L, balance sheet, cash flow,
                 half-yearly/nine-month results, TTM growth, revenue/EBITDA/EBIT trends,
                 key financial ratios, daily ratios, margin ratios, valuation ratios,
                 return ratios, growth ratios, performance ratios, cashflow ratios,
                 liquidity ratios, solvency ratios, shareholding pattern, major shareholders.
                 ⚠ CRITICAL: Use stock ONLY for equity/share data of a single named company.
                 Examples: "Reliance share price", "TCS quarterly results", "ITC PE ratio".

  market        → Market-WIDE movers or screeners that require a market GROUP or
                 SEGMENT (e.g. NSE, BSE). Use when the user wants ranked list aggregates across 
                 the entire market: top gainers, top losers, most active stocks, 52-week highs/lows, 
                 new highs, overall volume shockers, or advance-decline distributions.
                 ⚠ CRITICAL: This is NOT about sorting single sector clusters—it is about cross-market screeners.
                 Examples: "top gainers on NSE today", "52-week high stocks", "most active stocks BSE".

  ipo_stock     → Company-SPECIFIC IPO data where you know the company name.
                 Use this when the user asks about IPO details, GMP, allotment status,
                 anchor investors, subscription status, or prospectus for a NAMED company.
                 Examples: "Zomato IPO allotment", "Paytm IPO risk factors", "LIC IPO timeline".

  ipo_list      → Generic/market-wide IPO queries with NO specific company name.
                 Use when the user wants a LIST, overview, or status of IPOs across
                 the market — forthcoming IPOs, upcoming IPOs, open IPOs, closed IPOs.
                 Examples: "upcoming IPOs", "open IPOs right now", "recent IPO listings".

  mf_scheme     → A specific named mutual fund SCHEME. Use when the user asks about
                 NAV (current or historical), investment details, expense ratio,
                 average maturity, AUM, scheme returns (1Y/3Y/5Y), lumpsum returns,
                 SIP calculator, MF holdings/portfolio, or asset allocation for a NAMED scheme.
                 Examples: "Parag Parikh Flexi Cap NAV", "SBI Small Cap Fund portfolio".

  mf_amc        → A mutual fund HOUSE or AMC (Asset Management Company) — NOT a
                 specific scheme. Use when the user asks about all schemes offered by
                 a fund house, fund categories under an AMC, or AMC profile/overview.
                 Examples: "SBI Mutual Fund schemes", "Nippon AMC fund categories".

  etf           → An exchange-traded fund. Use when the user asks about ETF quotes,
                 ETF returns, ETF fundamentals, ETF profile, or ETF holdings for a NAMED ETF.
                 Examples: "Gold BeES NAV", "Nifty BeES returns", "SBI ETF Nifty 50".

  bond          → A specific named listed BOND, DEBENTURE, or NCD (Non-Convertible
                 Debenture). Use when the user asks about bond EOD prices, coupon rate, 
                 face value, maturity, price history, cash flows, credit rating, or redemption.
                 Examples: "HDFC NCD coupon rate", "SBI bond price", "NHAI bond rating".

  exchange      → EXCHANGE-LEVEL aggregate data with no specific entity. Use when the
                 user asks about overall market advance-decline ratio, market breadth,
                 exchange trading holidays, or market open/close schedules for NSE or BSE as a whole.
                 Examples: "NSE advance decline ratio today", "BSE market holidays".

  nfo           → New Fund Offers — mutual fund schemes that are currently open for
                 subscription for the FIRST TIME. Use when the user asks about NFOs,
                 new MF launches, or upcoming fund offers.
                 Examples: "new fund offers this month", "currently open NFOs".

  news          → Corporate or mutual fund NEWS and activity feeds. Use when the user
                 asks for recent news articles, press releases, or media updates.
                 Examples: "latest news on Reliance", "corporate news today".

  announcement → Formal regulatory ANNOUNCEMENTS filed on BSE or NSE. Use when the
                 user asks about corporate announcements, exchange disclosures, regulatory filings, 
                 board meeting notices, or AGM notices.
                 Examples: "BSE announcements today", "NSE corporate filings".

  market_info  → Broad market INFORMATION and analytics that don't belong to a single entity.
                 Use for: fund manager lists, AMFI master data, fund performance rankings by category, 
                 SIP transaction dates, result declaration calendars, or macro-economic data (GDP, CPI).
                 Examples: "macro economic indicators India", "repo rate today", "result calendar this week".

  general      → ONLY for purely educational or definitional questions that CANNOT
                 be answered by any live market data tool.
                 Examples: "explain what SIP means", "what is the difference between NAV and share price".

────────────────────────────────────────────────────────────
DECOMPOSITION RULES (CRITICAL):
1. SECTOR CLASSIFICATION IS PARAMOUNT: Guard against assigning multi-word sector phrases into index or stock blocks. If the string queries fields or properties of a business sector, flag it cleanly as sector.
2. THE 1:1 MAPPING: len(intents) MUST equal the number of subjects identified in Step 1.
3. NO BUNDLING: Two different subjects → two separate intent objects.
4. DATA POINT MERGING: Multiple metrics on the SAME entity → ONE intent.

MCP_NEEDED LOGIC:
- Set to true if ANY intent requires live market data or relational database lookups.
- Set to false ONLY for pure greetings or purely educational/definitional questions where every single intent is type=general.
────────────────────────────────────────────────────────────
"""

GREETING_PROMPT = """\\
You are EQUIFIZ — a friendly Indian stock-market AI assistant.
Respond warmly and briefly to the user's greeting or small talk.

User: {query}
"""

MCP_SYNTHESIS_PROMPT = """\\
You are a knowledgeable Indian equity, mutual fund, and fixed-income analyst assistant.

## Contextual Grounding Rules (CRITICAL)
- You MUST rely strictly on the canonical records tagged inside the "Live Data Retrieved" block.
- NEVER mix up records between distinct data segments. For instance, if data for NIFTY IT contains a specific list of stocks, do NOT mention or inject them into descriptions of the NIFTY DEFENCE segment.
- If the live data returned for a requested entity is completely empty, zeroed, or displays database lookup indicators like "[]" or None, you must explicitly state that no data was returned for that entity, rather than guessing or filling it using historical knowledge.

## Data Safety & Quality Guardrails (STRICT)
- NEVER expose internal database identifiers or structural system keys (such as "co_code", "mf_schcode").
- Check the parsed data for structural backend issues. If it indicates server configuration warnings or system failures, explain to the user that the information is currently inaccessible.

## Conversation history (last 4 turns)
{history}

## User Question
{user_query}

## Live Data Retrieved
{mcp_result}

## Instructions
1. Answer EACH entity component sequentially using only the explicitly labeled live data block provided for it.
2. If data is missing or marked as an error, briefly state that it cannot be verified right now.
3. Write in plain prose paragraphs only. No markdown, no bullet points, no asterisks, no tables. Do NOT use the rupee symbol — write "Rs".
4. Use Indian number formatting (lakh, crore) for big values.
5. Keep under 300 words.
6. End with: This is not financial advice.
"""

GENERAL_SYNTHESIS_PROMPT = """\\
You are a knowledgeable Indian equity and fixed-income analyst assistant.

## Conversation history (last 4 turns)
{history}

## User Question
{user_query}

## Knowledge Base Context
{vector_context}

## Instructions
1. Answer clearly and concisely.
2. Plain prose only — no markdown. Do NOT use the rupee symbol — write "Rs".
3. Under 200 words.
"""


# ── LLM helpers ───────────────────────────────────────────────────────────────

def _llm_json(prompt: str) -> dict:
    resp = llm.invoke([HumanMessage(content=prompt)])
    text = resp.content.strip()
    clean = re.sub(r"^`{3}(?:json)?|`{3}$", "", text, flags=re.MULTILINE).strip()

    try:
        return json.loads(clean)
    except json.JSONDecodeError:
        pass

    match = re.search(r"(\\{.*\\})", clean, re.DOTALL)
    if match:
        greedy = match.group(1)
        try:
            return json.loads(greedy)
        except json.JSONDecodeError:
            try:
                fixed = re.sub(r",\\s*([\\]}])", r"\\1", greedy.replace("'", '"'))
                return json.loads(fixed)
            except Exception:
                pass

    logger.error(f"[JSON FAIL] Raw LLM Output:\\n{text}")
    raise ValueError("Could not parse JSON from LLM response.")


def _llm_text(prompt: str) -> str:
    resp = llm.invoke([HumanMessage(content=prompt)])
    return resp.content.strip()


def _format_history(state: AgentState, n: int = 8) -> str:
    history = state.get("conversation_history") or []
    return "\\n".join(
        f"{t['role'].upper()}: {t['content']}" for t in history[-n:]
    ) or "(no prior conversation)"


def _strip_markdown(text: str) -> str:
    lines = text.splitlines()
    lines = [l for l in lines if not re.match(r"^\\s*\\|", l)]
    text = "\\n".join(lines)
    text = text.replace("₹", "Rs ")
    text = re.sub(r"\\*{1,3}(.+?)\\*{1,3}", r"\\1", text, flags=re.DOTALL)
    text = re.sub(r"_{1,3}(.+?)_{1,3}", r"\\1", text, flags=re.DOTALL)
    text = re.sub(r"(?m)^[ \\t]*(?:[-*#]+|[0-9]+\\.)[ \\t]+", "", text)
    text = re.sub(r"\\n{3,}", "\\n\\n", text)
    return text.strip()


# ══════════════════════════════════════════════════════════════════════════════
# Node AIQ: Classify + Extract + Decompose
# ══════════════════════════════════════════════════════════════════════════════

def node_classify_extract_and_decompose(state: AgentState) -> AgentState:
    console.print("[Node AIQ] Classify + extract + decompose (v11.5-Hardened)")

    user_query = state.get("user_query", "")
    history_text = _format_history(state, n=4)

    state["matched_tools"] = []
    state["intents"] = []
    state["companies"] = []

    try:
        result = _llm_json(
            CLASSIFY_EXTRACT_DECOMPOSE_PROMPT.format(
                history = history_text,
                today = TODAY,
                query = user_query,
            )
        )

        qt = result.get("query_type", "general")
        mcp = bool(result.get("mcp_needed", False))
        raw_intents = result.get("intents") or []

        intents: list[IntentItem] = []
        for item in raw_intents:
            et = item.get("entity_type", "general")
            intents.append(IntentItem(
                entity = item.get("entity", ""),
                entity_type = et,
                intent_description = item.get("intent_description", ""),
                scheme_name = item.get("scheme_name") or None,
                amc_name = item.get("amc_name") or None,
                nse_symbol = item.get("nse_symbol") or None,
                sector_name = item.get("sector_name") or None,  
                resolved_name = None,  
                tool_hint = "",
                matched_tools = [],
                resolved_codes = {},
                mcp_result = "",
                code_missing = False,
            ))

        if not intents and qt not in ("greeting",):
            console.print("  ⚠ LLM returned no intents — fallback single general intent")
            intents.append(IntentItem(
                entity = user_query,
                entity_type = "general",
                intent_description = user_query,
                scheme_name = None,
                amc_name = None,
                nse_symbol = None,
                sector_name = None,
                resolved_name = None,
                tool_hint = "",
                matched_tools = [],
                resolved_codes = {},
                mcp_result = "",
                code_missing = False,
            ))

        state["query_type"] = qt
        state["mcp_needed"] = mcp
        state["intents"] = intents

        if intents:
            first = intents[0]
            state["primary_entity_type"] = first.get("entity_type", "general")
            state["primary_query_intent"] = first.get("intent_description", "")
            state["extracted_entity"] = first.get("entity", user_query)
            state["mcp_scheme_name"] = first.get("scheme_name")
            state["mcp_amc_name"] = first.get("amc_name")
            state["nse_symbol"] = first.get("nse_symbol")
            state["sector_name"] = first.get("sector_name")  
        else:
            state["primary_entity_type"] = "general"
            state["primary_query_intent"] = user_query
            state["extracted_entity"] = user_query
            state["mcp_scheme_name"] = None
            state["mcp_amc_name"] = None
            state["nse_symbol"] = None
            state["sector_name"] = None

        state["companies"] = [
            {
                "name": i.get("entity", ""),
                "entity_type": i.get("entity_type", "general"),
                "tool_hint": "",
                "query_intent": i.get("intent_description", ""),
                "scheme_name": i.get("scheme_name"),
                "amc_name": i.get("amc_name"),
                "nse_symbol": i.get("nse_symbol"),
                "sector_name": i.get("sector_name"),
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
        state["query_type"] = "general"
        state["mcp_needed"] = False
        state["primary_entity_type"] = "general"
        state["primary_query_intent"] = user_query
        state["extracted_entity"] = user_query
        state["companies"] = []
        state["intents"] = []
        state["sector_name"] = None

    return state


# ── Chroma tool registry helpers (read-only) ──────────────────────────────────

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
            raw_req = meta.get("required_parameters", "") or ""
            req_params = [p.strip() for p in raw_req.split(",") if p.strip()]
            tools.append({
                "tool_name":           meta.get("name", tool_id),
                "description":          meta.get("description", ""),
                "required_parameters": req_params,
                "parameters":           meta.get("parameters", ""),
                "score":                round(1 - dist, 3),
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
            meta = res["metadatas"][0]
            raw_req = meta.get("required_parameters", "") or ""
            req_params = [p.strip() for p in raw_req.split(",") if p.strip()]
            return {
                "tool_name":           meta.get("name", tool_name),
                "description":          meta.get("description", ""),
                "required_parameters": req_params,
                "parameters":           meta.get("parameters", ""),
            }
    except Exception as e:
        console.print(f"  [ToolRegistry] get by id '{tool_name}' failed: {e}")
    return None


# ══════════════════════════════════════════════════════════════════════════════
# Node TVS: Per-intent tool vector search  (v11.5)
# ══════════════════════════════════════════════════════════════════════════════

_ALLOWED_FAMILIES: dict[str, frozenset[str]] = {
    "stock":        frozenset({"stock"}),
    "ipo_stock":    frozenset({"ipo_stock"}),
    "ipo_list":     frozenset({"ipo_list"}),
    "mf_scheme":    frozenset({"mf_scheme"}),
    "mf_amc":       frozenset({"mf_amc"}),
    "etf":          frozenset({"etf"}),
    "index":        frozenset({"index"}),
    "market":       frozenset({"market"}),
    "bond":         frozenset({"bond"}),
    "sector":       frozenset({"sector"}), 
    "exchange":     frozenset(),
    "nfo":          frozenset(),
    "news":         frozenset(),
    "announcement": frozenset(),
    "market_info":  frozenset(),
    "general":      frozenset(),
}


def node_per_intent_tool_search(state: AgentState) -> AgentState:
    console.print("[Node TVS] Per-intent tool vector search (v11.5-Hardened)")

    intents = state.get("intents") or []
    if not intents:
        console.print("  ⚠ No intents to search for")
        return state

    def _search_for_intent(intent: IntentItem) -> IntentItem:
        et = intent.get("entity_type", "general")

        if et == "general":
            console.print(
                f"  [{intent['entity']}] entity_type=general → skip tool search"
            )
            return IntentItem(
                entity = intent.get("entity", ""),
                entity_type = "general",
                intent_description = intent.get("intent_description", ""),
                scheme_name = intent.get("scheme_name"),
                amc_name = intent.get("amc_name"),
                nse_symbol = intent.get("nse_symbol"),
                sector_name = intent.get("sector_name"),
                resolved_name = None,
                tool_hint = "",
                matched_tools = [],
                resolved_codes = {},
                mcp_result = "",
                code_missing = False,
            )

        if et in _NO_DB_MCP_TYPES:
            search_query = intent["intent_description"]
        elif et == "index":
            search_query = f"get_index_constituents get_index_companies index constituents components list {intent['intent_description']}"
        else:
            search_query = f"{et} {intent['intent_description']}"

        tools = _query_tool_registry(search_query, n_results=10)

        allowed = _ALLOWED_FAMILIES.get(et, frozenset())
        if allowed:
            filtered = [
                t for t in tools
                if _infer_entity_type_from_tool(t["tool_name"]) in allowed
            ]
            if filtered:
                tools = filtered
                console.print(
                    f"  [TVS] family filter '{et}' → kept {len(tools)} tools"
                )
            else:
                console.print(
                    f"  [TVS] ⚠ family filter '{et}' eliminated all tools — keeping unfiltered results"
                )

        # HARD STRUCTURAL FALLBACK FOR BENCHMARK INDEX OR SECTORS
        if et == "index" and not any(_infer_entity_type_from_tool(t["tool_name"]) == "index" for t in tools):
            fallback_tools = ["get_index_constituents", "get_index_companies"]
            for f_tool in fallback_tools:
                meta = _get_tool_meta(f_tool)
                if meta:
                    meta["score"] = 0.99
                    tools.insert(0, meta)

        tool_hint = tools[0]["tool_name"] if tools else ""

        if tool_hint:
            console.print(
                f"  [{intent['entity']}] intent='{intent['intent_description'][:50]}' → top tool={tool_hint}"
            )
        return IntentItem(
            entity = intent.get("entity", ""),
            entity_type = et,
            intent_description = intent.get("intent_description", ""),
            scheme_name = intent.get("scheme_name"),
            amc_name = intent.get("amc_name"),
            nse_symbol = intent.get("nse_symbol"),
            sector_name = intent.get("sector_name"),
            resolved_name = None,
            tool_hint = tool_hint,
            matched_tools = tools,
            resolved_codes = {},
            mcp_result = "",
            code_missing = False,
        )

    updated: dict[int, IntentItem] = {}
    with ThreadPoolExecutor(max_workers=min(6, len(intents))) as ex:
        futures = {ex.submit(_search_for_intent, intent): idx for idx, intent in enumerate(intents)}
        for future in as_completed(futures):
            idx = futures[future]
            try:
                updated[idx] = future.result()
            except Exception as exc:
                console.print(f"  [red]TVS failed for intent #{idx}: {exc}[/red]")
                original = intents[idx]
                updated[idx] = IntentItem(
                    entity = original.get("entity", ""),
                    entity_type = original.get("entity_type", "general"),
                    intent_description = original.get("intent_description", ""),
                    scheme_name = original.get("scheme_name"),
                    amc_name = original.get("amc_name"),
                    nse_symbol = original.get("nse_symbol"),
                    sector_name = original.get("sector_name"),
                    resolved_name = None,
                    tool_hint = "",
                    matched_tools = [],
                    resolved_codes = {},
                    mcp_result = "",
                    code_missing = False,
                )

    state["intents"] = [updated[i] for i in sorted(updated)]

    if state["intents"]:
        first = state["intents"][0]
        state["matched_tools"] = first.get("matched_tools") or []
        state["mcp_tool_hint"] = first.get("tool_hint") or ""
        state["primary_tool_hint"] = first.get("tool_hint") or ""

    return state


# ── Greeting handler ──────────────────────────────────────────────────────────

def node_greeting_handler(state: AgentState) -> AgentState:
    console.print("[Node B] Greeting handler")
    state["final_answer"] = _llm_text(GREETING_PROMPT.format(query=state["user_query"]))
    return state


# ── General handler (vector search fallback) ──────────────────────────────────

def node_general_handler(state: AgentState) -> AgentState:
    console.print("[Node C] General handler (vector search fallback)")
    try:
        state["vector_context"] = vs.query(state["user_query"], n_results=5)
    except Exception as e:
        console.print(f"  General vector search failed: {e}")
        state["vector_context"] = []
    return state


# ══════════════════════════════════════════════════════════════════════════════
# FIX M — Hardened Multi-Tiered Fuzzy Match Scoring System
# ══════════════════════════════════════════════════════════════════════════════

def _compute_smart_score(target: str, candidate: str) -> float:
    target_clean = target.lower().strip()
    candidate_clean = candidate.lower().strip()
    
    if target_clean == candidate_clean:
        return 100.0
        
    ts_ratio = fuzz.token_sort_ratio(target_clean, candidate_clean)
    ratio = fuzz.ratio(target_clean, candidate_clean)
    part_ratio = fuzz.partial_ratio(target_clean, candidate_clean)
    
    target_words = set(target_clean.split())
    candidate_words = set(candidate_clean.split())
    
    has_exact_word_match = any(word in candidate_words for word in target_words)
    score = max(ts_ratio, ratio)
    
    if part_ratio > 90.0:
        if has_exact_word_match:
            score = max(score, part_ratio)
        else:
            score = max(score, part_ratio - 35.0)
    else:
        score = max(score, part_ratio)
        
    return float(score)


def _resolve_single(name: str, nse_symbol: Optional[str]) -> dict:
    name_upper = name.upper().strip()
    symbol_to_check = nse_symbol.upper().strip() if nse_symbol else name_upper
    
    if symbol_to_check:
        row = db.lookup_by_nse_symbol(symbol_to_check)
        if row:
            return {
                "co_code":      row["co_code"],
                "company_info": row,
                "nse_symbol":   row.get("nsesymbol"),
                "resolved_name": row.get("companyname"),
            }

    matches = db.fuzzy_search_company(name, limit=15)
    if matches:
        for cand in matches:
            cand_sym = str(cand.get("nsesymbol") or "").upper().strip()
            if cand_sym == name_upper:
                return {
                    "co_code":      cand["co_code"],
                    "company_info": cand,
                    "nse_symbol":   cand_sym,
                    "resolved_name": cand.get("companyname"),
                }

        best_match = None
        best_score = -1.0
        
        for cand in matches:
            cand_name = cand.get("companyname") or ""
            cand_sym  = cand.get("nsesymbol") or ""
            
            score_name = _compute_smart_score(name, cand_name)
            score_sym  = _compute_smart_score(name, cand_sym)
            score = max(score_name, score_sym)
            
            if score > best_score:
                best_score = score
                best_match = cand
                
        if best_match and best_score >= 50.0:
            return {
                "co_code":      best_match["co_code"],
                "company_info": best_match,
                "nse_symbol":   best_match.get("nsesymbol"),
                "resolved_name": best_match.get("companyname"),
            }
            
    return {}


# ══════════════════════════════════════════════════════════════════════════════
# DB targeted table lookup (Fully Dynamic Intersection Matrix)
# ══════════════════════════════════════════════════════════════════════════════

def _targeted_db_lookup(
    entity:   str,
    table:    str,
    name_col: str,
    id_col:   str,
) -> Optional[dict]:
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        if table == "scheme_master":
            cur.execute("SELECT mf_schcode, mf_cocode, sch_name, category FROM scheme_master")
        elif table == "bond_master":
            cur.execute("SELECT code, companyname, isin, nsesymbol FROM bond_master")
        elif table == "companies" and name_col == "sectorname":
            cur.execute("SELECT DISTINCT sectorcode, sectorname FROM companies WHERE sectorcode IS NOT NULL")
        else:
            cur.execute(f"SELECT * FROM {table}")

        rows = [dict(r) for r in cur.fetchall()]
        cur.close()
        conn.close()

        entity_upper = entity.upper()
        entity_clean = re.sub(r"[^A-Za-z0-9]", "", entity_upper).strip()
        query_tokens = {t for t in re.split(r"[^A-Za-z0-9]", entity_upper) if len(t) > 1}

        best_match: Optional[dict] = None
        best_score: float = -100.0  

        for row in rows:
            candidate = str(row.get(name_col) or row.get("group") or row.get("group_name") or row.get("groupname") or "")
            cand_upper = candidate.upper()
            cand_clean = re.sub(r"[^A-Za-z0-9]", "", cand_upper).strip()
            
            score = float(_compute_smart_score(entity, candidate))
            
            if table == "group_master":
                cand_tokens = set(re.split(r"[^A-Za-z0-9]", cand_upper))
                if len(cand_clean) >= 8 and "_" not in cand_upper:
                    chunks = [cand_clean[i:i+3] for i in range(0, len(cand_clean), 3)]
                    cand_tokens.update(chunks)

                is_exact_sector_match = False
                for q_tok in query_tokens:
                    if q_tok not in ("NIFTY", "BSE", "NSE", "INDEX"):
                        if cand_upper.endswith(f"_{q_tok}") or cand_clean.endswith(q_tok):
                            is_exact_sector_match = True

                row_exchange = str(row.get("exchange") or "").upper().strip()
                has_nse_affinity = any(k in entity_clean for k in ("NIFTY", "NSE"))
                has_bse_affinity = any(k in entity_clean for k in ("BSE", "SENSEX"))

                exchange_mismatch = False
                if has_nse_affinity and row_exchange == "BSE":
                    exchange_mismatch = True
                elif has_bse_affinity and row_exchange == "NSE":
                    exchange_mismatch = True

                has_token_intersection = any(
                    (q_tok in cand_clean or any(q_tok in c_tok or c_tok in q_tok for c_tok in cand_tokens))
                    for q_tok in query_tokens if q_tok not in ("NIFTY", "BSE", "NSE")
                )

                is_exact_root = (cand_clean in ("NIFTY", "BSE", "NSE"))
                query_has_modifiers = len(query_tokens - {"NIFTY", "BSE", "NSE", "INDEX", "STOCKS"}) > 0

                if exchange_mismatch:
                    score = -200.0  
                elif is_exact_sector_match:
                    score = 150.0  
                elif is_exact_root and query_has_modifiers:
                    score -= 75.0  
                elif has_token_intersection:
                    score += 35.0

            if table == "bond_master":
                isin_score = fuzz.ratio(entity.upper(), str(row.get("isin") or "").upper())
                sym_score = fuzz.ratio(entity.upper(), str(row.get("nsesymbol") or "").upper())
                score = max(score, isin_score, sym_score)

            if score > best_score:
                best_score, best_match = score, row

        if best_match and best_score >= 45.0:
            resolved_id = best_match.get(id_col) or best_match.get("indexcode") or best_match.get("code") or best_match.get("sectorcode")
            resolved_name = best_match.get(name_col) or best_match.get("group") or best_match.get("group_name")
            return {
                id_col: resolved_id,
                name_col: resolved_name
            }
        return None

    except Exception as e:
        console.print(f"     [_targeted_db_lookup] DB error on {table}: {e}")
        return None


def _get_required_params_for_entity(
    tool_hint:     str,
    entity_type:   str,
    matched_tools: list[dict],
) -> list[str]:
    params = ENTITY_TYPE_PARAM_MAP.get(entity_type)
    if params is not None:
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

    return []


# ── Per-intent DB code resolver ──────────────────────────────────────────────

def _resolve_entity_codes(
    name:            str,
    scheme_name:     Optional[str],
    amc_name:        Optional[str],
    nse_symbol:      Optional[str],
    sector_name:     Optional[str],  
    required_params: list[str],
) -> dict:
    codes:  dict = {}
    result: dict = {"name": name}

    needs_schcode    = "mf_schcode"  in required_params
    needs_cocode     = "mf_cocode"   in required_params
    needs_co_code    = "co_code"     in required_params
    needs_isin       = "isin"        in required_params
    needs_indexcode = "index_code"  in required_params
    needs_group      = "group"       in required_params
    needs_bond_code = "bond_code"    in required_params
    needs_sectorcode = "sector_code" in required_params 

    if needs_schcode:
        search = scheme_name or name
        cfg = PARAM_TO_TABLE_MAP["mf_schcode"]
        match = _targeted_db_lookup(entity=search, table=cfg["table"], name_col=cfg["column"], id_col=cfg["id_field"])
        if match:
            codes["mf_schcode"] = int(match["mf_schcode"])
            if "mf_cocode" in match:
                codes.setdefault("mf_cocode", int(match["mf_cocode"]))
            result["resolved_scheme_name"] = match.get("sch_name", search)

    if needs_cocode and not codes.get("mf_cocode"):
        search = amc_name or name
        cfg = PARAM_TO_TABLE_MAP["mf_cocode"]
        match = _targeted_db_lookup(entity=search, table=cfg["table"], name_col=cfg["column"], id_col=cfg["id_field"])
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
                "resolved_company_name": stock["resolved_name"]
            })
        else:
            result["code_missing"] = True

    if needs_isin:
        search = scheme_name or name
        cfg = PARAM_TO_TABLE_MAP["isin"]
        match = _targeted_db_lookup(entity=search, table=cfg["table"], name_col=cfg["column"], id_col=cfg["id_field"])
        if match:
            codes["isin"] = match["isin"]
            result["resolved_etf_name"] = match.get("etfname", search)

    if needs_indexcode:
        cfg = PARAM_TO_TABLE_MAP["index_code"]
        match = _targeted_db_lookup(entity=name, table=cfg["table"], name_col=cfg["column"], id_col=cfg["id_field"])
        if match:
            try:
                codes["index_code"] = int(float(match[cfg["id_field"]]))
            except (ValueError, TypeError, KeyError):
                codes["index_code"] = int(float(match.get("indexcode") or match.get("index_code")))
            result["resolved_index_name"] = match.get(cfg["column"]) or match.get("group") or match.get("group_name")

    if needs_group:
        cfg = PARAM_TO_TABLE_MAP["group"]
        match = _targeted_db_lookup(entity=name, table=cfg["table"], name_col=cfg["column"], id_col=cfg["id_field"])
        if match:
            codes["group"] = match.get(cfg["id_field"]) or match.get("group") or match.get("group_name")
            result["resolved_group_name"] = codes["group"]
        else:
            codes["group"] = name
            result["resolved_group_name"] = name

    if needs_bond_code:
        cfg = PARAM_TO_TABLE_MAP["bond_code"]
        match = _targeted_db_lookup(entity=name, table=cfg["table"], name_col=cfg["column"], id_col=cfg["id_field"])
        if match:
            codes["bond_code"] = int(match["code"])
            result["resolved_bond_name"] = match.get("companyname", name)
        else:
            result["code_missing"] = True

    if needs_sectorcode:
        search = sector_name or name
        cfg = PARAM_TO_TABLE_MAP["sector_code"]
        match = _targeted_db_lookup(entity=search, table=cfg["table"], name_col=cfg["column"], id_col=cfg["id_field"])
        if match:
            try:
                codes["sector_code"] = int(float(match[cfg["id_field"]]))
            except (ValueError, TypeError):
                codes["sector_code"] = match[cfg["id_field"]]
            result["resolved_sector_name"] = match.get(cfg["column"], search)
        else:
            result["code_missing"] = True

    result["mcp_resolved_codes"] = codes
    return result


def _resolve_intents_codes(intents: list[IntentItem]) -> list[IntentItem]:
    def _resolve_one(intent: IntentItem) -> IntentItem:
        et = intent.get("entity_type", "general")
        tool_hint = intent.get("tool_hint", "")
        matched = intent.get("matched_tools") or []

        if et == "general" and tool_hint:
            inferred = _infer_entity_type_from_tool(tool_hint)
            if inferred != "general":
                et = inferred
                intent["entity_type"] = et

        req_params = _get_required_params_for_entity(tool_hint, et, matched)

        if not req_params:
            intent["resolved_codes"] = {}
            intent["code_missing"] = False
            intent["resolved_name"] = intent.get("entity")
            return intent

        resolved = _resolve_entity_codes(
            name = intent.get("entity", ""),
            scheme_name = intent.get("scheme_name"),
            amc_name = intent.get("amc_name"),
            nse_symbol = intent.get("nse_symbol"),
            sector_name = intent.get("sector_name"),  
            required_params = req_params,
        )
        intent["resolved_codes"] = resolved.get("mcp_resolved_codes", {})
        intent["code_missing"] = bool(resolved.get("code_missing", False))
        
        intent["resolved_name"] = (
            resolved.get("resolved_company_name") or
            resolved.get("resolved_scheme_name") or
            resolved.get("resolved_amc_name") or
            resolved.get("resolved_etf_name") or
            resolved.get("resolved_index_name") or
            resolved.get("resolved_bond_name") or
            resolved.get("resolved_sector_name") or  
            intent.get("entity")
        )
        return intent

    result_intents: dict[int, IntentItem] = {}
    with ThreadPoolExecutor(max_workers=min(6, len(intents) or 1)) as ex:
        futures = {ex.submit(_resolve_one, intent): idx for idx, intent in enumerate(intents)}
        for future in as_completed(futures):
            idx = futures[future]
            try:
                result_intents[idx] = future.result()
            except Exception:
                result_intents[idx] = intents[idx]

    return [result_intents[i] for i in sorted(result_intents)]


# ── MCP Pre-Resolution nodes ──────────────────────────────────────────────

def node_mcp_pre_resolve(state: AgentState) -> AgentState:
    console.print("[Node MPR] Single-entity MCP pre-resolve (v11.5)")
    intents = state.get("intents") or []
    if not intents:
        state["mcp_resolved_codes"] = {}
        return state

    resolved_intents = _resolve_intents_codes(intents)
    state["intents"] = resolved_intents
    state["mcp_resolved_codes"] = resolved_intents[0].get("resolved_codes", {}) if resolved_intents else {}
    return state


def node_mcp_multi_pre_resolve(state: AgentState) -> AgentState:
    console.print("[Node MMPR] Multi-entity MCP pre-resolve (v11.5)")
    intents = state.get("intents") or []
    if not intents:
        return state

    resolved_intents = _resolve_intents_codes(intents)
    state["intents"] = resolved_intents
    state["mcp_resolved_codes"] = resolved_intents[0].get("resolved_codes", {}) if resolved_intents else {}
    return state


# ── Node MTCI: MCP Tool Call ──────────────────────────────────────────────────

def _run_mcp_multi_in_thread(intents: list[dict]) -> list[dict]:
    from mcp_client import run_mcp_query_multi, trim_results
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        results = loop.run_until_complete(run_mcp_query_multi(intents))
        return trim_results(results)
    except Exception as e:
        console.print(f"[bold red]MCP Multi Error:[/bold red] {e}")
        return [{**i, "mcp_result": f"Error: {e}"} for i in intents]
    finally:
        loop.close()


def node_mcp_tool_call_intents(state: AgentState) -> AgentState:
    console.print("[Node MTCI] Sequential per-intent MCP calls — one session (v11.5)")

    try:
        import mcp_client  # noqa: F401
    except ImportError:
        state["mcp_raw_result"] = ""
        state["intent_results"] = []
        state["error"] = "mcp_client_not_found"
        return state

    intents = state.get("intents") or []
    user_query = state.get("user_query", "")

    mcp_intents: list[IntentItem] = []
    blocked_intents: list[IntentItem] = []
    general_intents: list[IntentItem] = []

    for i in intents:
        et = i.get("entity_type", "general")
        has_tool = bool(i.get("tool_hint"))
        code_missing = bool(i.get("code_missing", False))

        if et == "general" and not has_tool:
            general_intents.append(i)
        elif code_missing:
            blocked_intents.append(i)
        else:
            mcp_intents.append(i)

    if not mcp_intents:
        from mcp_client import run_mcp_query
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            raw = loop.run_until_complete(run_mcp_query(user_query))
            loop.close()
        except Exception as exc:
            raw = f"MCP call failed: {exc}"
        state["mcp_raw_result"] = raw
        state["intent_results"] = []
        state["mcp_tool_calls_made"] = []
        state["error"] = None
        return state

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            future = ex.submit(_run_mcp_multi_in_thread, mcp_intents)
            filled_intents = future.result(timeout=600)
    except Exception as exc:
        filled_intents = [{**dict(i), "mcp_result": f"MCP call failed: {exc}"} for i in mcp_intents]

    all_intents = (
        filled_intents
        + [{**dict(i), "mcp_result": "(answered from knowledge base)"} for i in general_intents]
        + [{**dict(i), "mcp_result": f"Sorry, I could not find '{i.get('entity')}' in our database."} for i in blocked_intents]
    )
    state["intents"] = all_intents

    state["intent_results"] = [
        {
            "entity":               i.get("entity", ""),
            "resolved_name":      i.get("resolved_name", i.get("entity", "")),
            "entity_type":        i.get("entity_type", ""),
            "intent_description": i.get("intent_description", ""),
            "mcp_result":         i.get("mcp_result", ""),
        }
        for i in all_intents
    ]

    valid_segments = []
    for r in state["intent_results"]:
        res_str = str(r.get("mcp_result", ""))
        
        if any(indicator in res_str.lower() for indicator in ["<html", "502 bad gateway", "nginx/", "404 not found", "error response"]):
            res_str = "Error: Upstream market analytics interface returned an invalid protocol wrapper. The server is busy. Please rephrase shortly."
            state["error"] = "upstream_infrastructure_leak"
            
        valid_segments.append(
            f"--- RESOLVED ENTITY CANONICAL DATA FOR: {r.get('resolved_name', r['entity']).upper()} ---\\n"
            f"[Requested Segment: {r['intent_description']}]\\n"
            f"{res_str}"
        )

    state["mcp_raw_result"] = "\\n\\n---\\n\\n".join(valid_segments)
    state["mcp_tool_calls_made"] = []
    return state


_MCP_ERROR_MESSAGES = {
    "mcp_client_not_found": "Sorry, the live data service is currently unavailable. Please try again shortly.",
    "mcp_call_timeout": "The live data request timed out. The server may be busy — please try again in a moment.",
    "upstream_infrastructure_leak": "The data connection to the market stream encountered an operational disruption. I could not verify the requested information right now. Please rephrase shortly."
}

# ── MCP Synthesis (v11.5) ────────────────────────────────────────────────────

def node_mcp_synthesis(state: AgentState) -> AgentState:
    console.print("[Node MS] MCP synthesis (v11.5)")
    error = state.get("error", "")
    mcp_result = state.get("mcp_raw_result", "")

    if error in _MCP_ERROR_MESSAGES:
        state["final_answer"] = _MCP_ERROR_MESSAGES[error]
        return state
        
    if not mcp_result:
        state["final_answer"] = "No data was returned from the live server. Please try again shortly."
        return state

    mcp_snippet = mcp_result
    if len(mcp_snippet) > 10000:
        mcp_snippet = mcp_snippet[:7500] + "\\n\\n... [middle trimmed for length] ...\\n\\n" + mcp_snippet[-2000:]

    prompt = MCP_SYNTHESIS_PROMPT.format(
        history = _format_history(state),
        user_query = state.get("user_query", ""),
        mcp_result = mcp_snippet,
    )
    try:
        state["final_answer"] = _llm_text(prompt)
    except Exception as e:
        state["final_answer"] = mcp_result
    return state


def node_synthesis(state: AgentState) -> AgentState:
    console.print("[Node S] General synthesis (vector search)")
    vector_context = state.get("vector_context") or []
    context_text = "\\n".join(f"- [{r['metadata'].get('api_name', '')}] {r['document']}" for r in vector_context)
    prompt = GENERAL_SYNTHESIS_PROMPT.format(
        history = _format_history(state),
        user_query = state["user_query"],
        vector_context = context_text or "No relevant context found.",
    )
    try:
        state["final_answer"] = _llm_text(prompt)
    except Exception as e:
        state["final_answer"] = f"Synthesis failed: {e}"
    return state


# ── Router (v11.5) ────────────────────────────────────────────────────────────

def route_after_classify(state: AgentState) -> str:
    qt = state.get("query_type", "general")
    intents = state.get("intents") or []

    if qt == "greeting":
        return "greeting"

    mcp_intents = [i for i in intents if i.get("tool_hint") or i.get("entity_type") not in ("general",)]

    if not mcp_intents or (not any(i.get("tool_hint") for i in intents) and all(i.get("entity_type") == "general" for i in intents)):
        return "general"

    has_many = len(intents) > 1
    additional = state.get("companies") or []
    primary_type = state.get("primary_entity_type", "general")
    secondary_types = {c.get("entity_type", "general") for c in additional}
    is_heterogeneous = bool(secondary_types - {"general"} - {primary_type})

    if has_many or qt == "comparison" or is_heterogeneous:
        return "multi_mcp"

    return "mcp_direct"


# ── Graph Build Sequence ──────────────────────────────────────────────────────

def build_graph() -> Any:
    g = StateGraph(AgentState)

    g.add_node("classify_extract_decompose", node_classify_extract_and_decompose)
    g.add_node("per_intent_tool_search",     node_per_intent_tool_search)
    g.add_node("greeting_handler",            node_greeting_handler)
    g.add_node("general_handler",             node_general_handler)
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


_graph = None

def get_graph():
    global _graph
    if _graph is None:
        _graph = build_graph()
    return _graph


def run_query(
    user_query:           str,
    conversation_history: Optional[list[Turn]] = None,
    force_refresh:        bool = False,
) -> tuple[str, list[Turn]]:
    history = conversation_history or []
    result = get_graph().invoke(
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


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

# """
# mf_equifiz_server.py — Equifiz Mutual Fund MCP Server

# Design principles:
#   - Every tool returns ONLY the fields the LLM needs to answer the user.
#   - Internal IDs (mf_cocode, mf_schcode, co_code, classcode, etc.) are
#     NEVER surfaced to the end-user answer — they are plumbing only.
#   - Resolvers (resolve_mf_fund / resolve_mf_scheme) use the local DB cache;
#     all data tools hit the REST API and filter to a tight field whitelist.
#   - _require_int is GONE. All tools use _coerce_int() instead, which
#     accepts int, float, or string — matching the pre-injected code format
#     sent by graph.py's node_mcp_tool_call.
# """

# from __future__ import annotations

# import json
# import logging
# import os
# import re
# from typing import Optional

# import psycopg2
# import psycopg2.extras
# import requests
# from dotenv import load_dotenv
# from fastmcp import FastMCP
# from rapidfuzz import fuzz

# load_dotenv()

# logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
# logger = logging.getLogger("equifiz_mf")

# # ── Config ─────────────────────────────────────────────────────────────────────

# FUZZY_THRESHOLD = int(os.getenv("FUZZY_THRESHOLD", "75"))
# BASE_URL        = os.getenv("EQUIFIZ_API_BASE_URL", "https://equifizapis.cmots.com/api")
# TOKEN           = os.getenv("EQUIFIZ_TOKEN", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1bmlxdWVfbmFtZSI6ImVxdWlmaXphcGlzIiwicm9sZSI6IkFkbWluIiwibmJmIjoxNzc2OTMyODcyLCJleHAiOjE4MDkxNjAwNzIsImlhdCI6MTc3NjkzMjg3MiwiaXNzIjoiaHR0cDovL2xvY2FsaG9zdDo1MDE5MSIsImF1ZCI6Imh0dHA6Ly9sb2NhbGhvc3Q6NTAxOTEifQ.lz6do_yCsDQTFz5E-qi4w825YvjFY7lWv_l1qWG4W9I")

# DB = {
#     "host":     os.getenv("POSTGRES_HOST",     "localhost"),
#     "port":     int(os.getenv("POSTGRES_PORT", "5432")),
#     "dbname":   os.getenv("POSTGRES_DB",       "equifiz"),
#     "user":     os.getenv("POSTGRES_USER",     "postgres"),
#     "password": os.getenv("POSTGRES_PASSWORD", "1234"),
# }

# # ── Alias maps ─────────────────────────────────────────────────────────────────

# AMC_ALIAS_MAP: dict[str, str] = {
#     "hdfc mf": "HDFC Mutual Fund", "hdfc": "HDFC Mutual Fund",
#     "sbi mf": "SBI Mutual Fund", "sbi": "SBI Mutual Fund",
#     "icici pru": "ICICI Prudential Mutual Fund",
#     "icici prudential": "ICICI Prudential Mutual Fund",
#     "icici": "ICICI Prudential Mutual Fund",
#     "axis mf": "Axis Mutual Fund", "axis": "Axis Mutual Fund",
#     "mirae": "Mirae Asset Mutual Fund", "mirae asset": "Mirae Asset Mutual Fund",
#     "nippon": "Nippon India Mutual Fund", "nippon india": "Nippon India Mutual Fund",
#     "reliance mf": "Nippon India Mutual Fund",
#     "kotak": "Kotak Mahindra Mutual Fund", "kotak mahindra": "Kotak Mahindra Mutual Fund",
#     "dsp": "DSP Mutual Fund",
#     "franklin": "Franklin Templeton Mutual Fund",
#     "franklin templeton": "Franklin Templeton Mutual Fund",
#     "aditya birla": "Aditya Birla Sun Life Mutual Fund",
#     "absl": "Aditya Birla Sun Life Mutual Fund",
#     "birla sun life": "Aditya Birla Sun Life Mutual Fund",
#     "uti": "UTI Mutual Fund",
#     "ppfas": "Parag Parikh Financial Advisory Services",
#     "parag parikh": "Parag Parikh Financial Advisory Services",
#     "quant": "Quant Mutual Fund", "tata": "Tata Mutual Fund",
#     "invesco": "Invesco Mutual Fund",
#     "motilal": "Motilal Oswal Mutual Fund", "motilal oswal": "Motilal Oswal Mutual Fund",
#     "canara": "Canara Robeco Mutual Fund", "canara robeco": "Canara Robeco Mutual Fund",
#     "edelweiss": "Edelweiss Mutual Fund", "bandhan": "Bandhan Mutual Fund",
#     "navi": "Navi Mutual Fund", "pgim": "PGIM India Mutual Fund",
#     "whiteoak": "WhiteOak Capital Mutual Fund", "white oak": "WhiteOak Capital Mutual Fund",
#     "360 one": "360 ONE Mutual Fund", "360one": "360 ONE Mutual Fund",
# }

# SCHEME_ALIAS_MAP: dict[str, str] = {
#     "ppfas flexi": "Parag Parikh Flexi Cap Fund",
#     "parag parikh flexi": "Parag Parikh Flexi Cap Fund",
#     "sbi bluechip": "SBI Bluechip Fund",
#     "hdfc flexi": "HDFC Flexi Cap Fund",
#     "hdfc top 100": "HDFC Top 100 Fund",
#     "icici bluechip": "ICICI Prudential Bluechip Fund",
#     "mirae bluechip": "Mirae Asset Large Cap Fund",
#     "mirae emerging": "Mirae Asset Emerging Bluechip Fund",
#     "mirae tax saver": "Mirae Asset Tax Saver Fund",
#     "axis bluechip": "Axis Bluechip Fund",
#     "axis small cap": "Axis Small Cap Fund",
#     "nippon small cap": "Nippon India Small Cap Fund",
#     "sbi small cap": "SBI Small Cap Fund",
#     "kotak flexi": "Kotak Flexi Cap Fund",
#     "dsp flexi": "DSP Flexi Cap Fund",
#     "quant small cap": "Quant Small Cap Fund",
#     "quant flexi": "Quant Flexi Cap Fund",
#     "motilal midcap": "Motilal Oswal Midcap Fund",
# }

# # ── HTTP session ───────────────────────────────────────────────────────────────

# _session = requests.Session()
# _session.headers.update({
#     "Authorization": f"Bearer {TOKEN}",
#     "Content-Type": "application/json",
# })

# # ── DB cache ───────────────────────────────────────────────────────────────────

# _mf_amc_cache:    list[dict] = []
# _mf_scheme_cache: list[dict] = []


# def _db():
#     return psycopg2.connect(**DB)


# def _load_mf_amc_cache() -> list[dict]:
#     global _mf_amc_cache
#     if _mf_amc_cache:
#         return _mf_amc_cache
#     try:
#         conn = _db()
#         with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
#             cur.execute("SELECT mf_cocode, lname, nameamc, fund_type FROM fund_house")
#             _mf_amc_cache = [dict(r) for r in cur.fetchall()]
#         conn.close()
#     except Exception as e:
#         logger.error("MF AMC cache load failed: %s", e)
#     return _mf_amc_cache


# def _load_mf_scheme_cache() -> list[dict]:
#     global _mf_scheme_cache
#     if _mf_scheme_cache:
#         return _mf_scheme_cache
#     try:
#         conn = _db()
#         with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
#             cur.execute(
#                 "SELECT mf_schcode, mf_cocode, sch_name, category, isin, amficode "
#                 "FROM scheme_master"
#             )
#             _mf_scheme_cache = [dict(r) for r in cur.fetchall()]
#         conn.close()
#     except Exception as e:
#         logger.error("MF Scheme cache load failed: %s", e)
#     return _mf_scheme_cache


# # ── Query cleaning ─────────────────────────────────────────────────────────────

# _MF_NOISE = [
#     r"^(what is|what are|what's|give me|show me|tell me|find|get|check|fetch)\s+(the\s+)?",
#     r"^(nav|returns?|aum|performance|details?|info|information)\s+(of|for|about)\s+",
#     r"^(details?\s+(of|about|for)\s+)",
#     r"\b(fund|mutual fund|scheme|plan|option|growth|idcw|direct|regular|dividend)\b",
# ]


# def _clean_mf_query(query: str) -> str:
#     q = query.strip().rstrip("?!.")
#     for p in _MF_NOISE:
#         q = re.sub(p, " ", q, flags=re.IGNORECASE).strip()
#     return re.sub(r"\s{2,}", " ", q).strip() or query


# # ── HTTP helper ────────────────────────────────────────────────────────────────

# def _get(url: str) -> tuple[list | dict | None, str | None]:
#     try:
#         r = _session.get(url, timeout=15)
#         r.raise_for_status()
#         return r.json(), None
#     except requests.HTTPError as e:
#         return None, f"API error {e.response.status_code}: {e.response.text[:200]}"
#     except Exception as e:
#         return None, f"Request failed: {e}"


# # ── Field-pick helpers ─────────────────────────────────────────────────────────

# def _pick(record: dict, fields: list[str]) -> dict:
#     """Return only the requested keys (case-insensitive), skipping None/empty."""
#     low = {k.lower(): v for k, v in record.items()}
#     out = {}
#     for f in fields:
#         v = low.get(f.lower())
#         if v is not None and v != "":
#             out[f] = v
#     return out


# def _format_rows(rows: list[dict], fields: list[str], cap: int = 20) -> str:
#     rows = rows[:cap]
#     lines = []
#     for i, row in enumerate(rows, 1):
#         picked = _pick(row, fields)
#         lines.append(f"[{i}] " + "  |  ".join(f"{k}: {v}" for k, v in picked.items()))
#     return "\n".join(lines) if lines else "No data."


# def _format_single(row: dict, fields: list[str]) -> str:
#     picked = _pick(row, fields)
#     return "\n".join(f"  {k:<28}: {v}" for k, v in picked.items()) or "No data."


# def _as_list(data) -> list[dict]:
#     if isinstance(data, list):
#         return data
#     if isinstance(data, dict):
#         return [data]
#     return []


# # ── Integer coercion helper ────────────────────────────────────────────────────
# # Replaces _require_int. Accepts int, float (e.g. 770.0), or numeric string.
# # Returns (int_value, None) on success, or (None, error_string) on failure.

# # def _coerce_int(value, param_name: str, resolver: str) -> tuple[int | None, str | None]:
# #     """
# #     Safely coerce value to int. Handles:
# #       - int  → returned as-is
# #       - float → truncated to int (API sometimes sends 770.0)
# #       - str  → parsed as float then int
# #       - None → returns error directing caller to use resolver
# #     """
# #     if value is None:
# #         return None, (
# #             f"'{param_name}' is required. "
# #             f"Call {resolver} first to obtain it, or ensure [PRE-RESOLVED CODES] "
# #             f"block contains {param_name}=<number>."
# #         )
# #     try:
# #         return int(float(str(value))), None
# #     except (TypeError, ValueError):
# #         return None, f"'{param_name}' must be a number, got: {value!r}"

# # def _coerce_int(value, param_name: str, resolver: str) -> tuple[int | None, str | None]:
# #     if value is None:
# #         return None, f"'{param_name}' is required. Call {resolver} first."
    
# #     try:
# #         # Convert to string and handle cases like "mf_schcode=27" or "27.0"
# #         clean_val = str(value).lower()
# #         if "=" in clean_val:
# #             clean_val = clean_val.split("=")[-1]
        
# #         return int(float(clean_val.strip())), None
# #     except (TypeError, ValueError):
# #         return None, f"'{param_name}' must be a number, got: {value!r}"
    
# def _coerce_int(value, param_name: str, resolver: str) -> tuple[int | None, str | None]:
#     if value is None or str(value).strip() == "":
#         return None, f"Missing {param_name}. Please call {resolver} first with the fund name."
    
#     try:
#         # Regex to extract only digits from strings like 'mf_schcode: 770' or '770.0'
#         num_match = re.search(r"(\d+)", str(value))
#         if num_match:
#             return int(num_match.group(1)), None
#         return int(float(str(value))), None
#     except (TypeError, ValueError):
#         return None, f"Invalid {param_name}: {value}. Expected a numeric ID."
    

# def _normalise_period(period: str) -> str:
#     p = period.strip().upper()
#     if p in ("M", "MONTH", "MONTHS"):
#         return "M"
#     if p in ("D", "DAY", "DAYS"):
#         logger.warning("period='%s' not valid; defaulting to Y", period)
#     return "Y"


# # ── Fuzzy matchers ─────────────────────────────────────────────────────────────

# def _fuzzy_mf_amc(query: str, limit: int = 5, threshold: int = FUZZY_THRESHOLD) -> list[dict]:
#     q = query.lower().strip()
#     scored = []
#     for amc in _load_mf_amc_cache():
#         nl = (amc.get("lname") or "").lower()
#         ns = (amc.get("nameamc") or "").lower()
#         score = max(
#             fuzz.token_set_ratio(q, nl), fuzz.token_set_ratio(q, ns),
#             fuzz.partial_ratio(q, nl),   fuzz.partial_ratio(q, ns),
#         )
#         if score >= threshold:
#             scored.append((amc, score))
#     scored.sort(key=lambda x: x[1], reverse=True)
#     return [a for a, _ in scored[:limit]]


# def _fuzzy_mf_scheme(query: str, mf_cocode: int | None = None,
#                      limit: int = 5, threshold: int = FUZZY_THRESHOLD) -> list[dict]:
#     q = query.lower().strip()
#     scored = []
#     for sch in _load_mf_scheme_cache():
#         if mf_cocode and sch.get("mf_cocode") != mf_cocode:
#             continue
#         name = (sch.get("sch_name") or "").lower()
#         score = max(fuzz.token_set_ratio(q, name), fuzz.partial_ratio(q, name))
#         if score >= threshold:
#             scored.append((sch, score))
#     scored.sort(key=lambda x: x[1], reverse=True)
#     return [s for s, _ in scored[:limit]]


# # ── API endpoint map ───────────────────────────────────────────────────────────

# EP = {
#     "fund_house":           f"{BASE_URL}/Fund_House",
#     "fund_category_amc":    f"{BASE_URL}/FundCategoryAMCWise/{{mf_cocode}}/{{category}}",
#     "scheme_master":        f"{BASE_URL}/SchemeMaster/{{mf_cocode}}",
#     "amfi_master":          f"{BASE_URL}/AMFIMaster",
#     "fund_manager":         f"{BASE_URL}/FundManager",
#     "sip_dates":            f"{BASE_URL}/SIP_Dates/{{plan}}",
#     "bse_star_scheme":      f"{BASE_URL}/BSEStarSchemeMaster/{{mf_schcode}}",
#     "fund_profile":         f"{BASE_URL}/fund-profile/{{mf_cocode}}",
#     "investment_details":   f"{BASE_URL}/InvestmentDetails/{{mf_schcode}}",
#     "expense_ratio":        f"{BASE_URL}/ExpenseRatios",
#     "avg_maturity":         f"{BASE_URL}/AvgerageMaturityData",
#     "scheme_aum":           f"{BASE_URL}/SchemeAUMHist",
#     "new_fund_offer":       f"{BASE_URL}/NewFundOffer",
#     "scheme_sip_swp":       f"{BASE_URL}/SchemeSIPSWPdetails/{{mf_schcode}}",
#     "scheme_comparison":    f"{BASE_URL}/SchemeComparsion/{{schcodes}}",
#     "fund_performance":     f"{BASE_URL}/FundPerformance/{{top}}/{{type}}/{{category}}",
#     "daily_nav":            f"{BASE_URL}/DailyNAV",
#     "nav_historical":       f"{BASE_URL}/SchemeNAVHistorical/{{mf_schcode}}/{{period}}/{{periodval}}",
#     "scheme_returns":       f"{BASE_URL}/SchemeReturns/{{mf_schcode}}",
#     "category_performance": f"{BASE_URL}/CategoryPerformance/{{type}}/{{top}}",
#     "mf_holding":           f"{BASE_URL}/MFHolding/{{mf_schcode}}",
#     "company_mf_holding":   f"{BASE_URL}/CompanyWiseMFHolding/{{co_code}}/{{top}}",
#     "sector_allocation":    f"{BASE_URL}/SchemeSectorAllocation/{{mf_schcode}}",
#     "asset_allocation":     f"{BASE_URL}/SchemeAssetAllocation/{{mf_schcode}}",
#     "whats_in_out":         f"{BASE_URL}/Whats_InOut/{{type}}/{{mf_schcode}}",
#     "mcap_equity":          f"{BASE_URL}/MCAP_EquityHolding/{{mf_schcode}}",
#     "most_sold_bought":     f"{BASE_URL}/MostsoldBought/{{mf_schcode}}",
#     "lumpsum_return":       f"{BASE_URL}/LumpSumSchemereturnDetails/{{mf_schcode}}",
#     "mf_activities":        f"{BASE_URL}/MFActivities",
#     "scheme_ratios":        f"{BASE_URL}/Scheme_Ratios",
#     "dividend_details":     f"{BASE_URL}/DividendDetails/{{mf_schcode}}",
#     "mf_news":              f"{BASE_URL}/MF_News/{{sno}}",
# }

# # ══════════════════════════════════════════════════════════════════════════════
# mcp = FastMCP("Equifiz MF Server")
# # ══════════════════════════════════════════════════════════════════════════════


# # ── RESOLVER TOOLS ─────────────────────────────────────────────────────────────

# @mcp.tool(description=(
#     "Resolve an AMC / fund-house name to its internal mf_cocode. "
#     "Call this BEFORE any AMC-level tool. Accepts brand names and aliases. "
#     "The returned mf_cocode is used only to call other tools — do NOT show it to the user."
# ))
# def resolve_mf_fund(query: str) -> str:
#     cleaned   = _clean_mf_query(query)
#     canonical = AMC_ALIAS_MAP.get(cleaned.lower().strip(), cleaned)
#     hits = _fuzzy_mf_amc(canonical, limit=1, threshold=FUZZY_THRESHOLD)
#     if not hits:
#         hits = _fuzzy_mf_amc(canonical, limit=1, threshold=65)
#     if hits:
#         h = hits[0]
#         return (
#             f"RESOLVED\n"
#             f"AMC Name  : {h.get('lname')}\n"
#             f"mf_cocode : {h.get('mf_cocode')}"
#         )
#     return (
#         f"NOT FOUND: '{canonical}'. "
#         "Try the full AMC name (e.g. 'HDFC Mutual Fund', 'Parag Parikh Financial Advisory Services')."
#     )


# @mcp.tool(description=(
#     "Resolve a mutual fund scheme name into mf_schcode. "
#     "CALL THIS FIRST before tools that require mf_schcode. "
#     "Read the returned mf_schcode and pass it exactly to the next tool. "
#     "Never skip this step unless mf_schcode is already given in [PRE-RESOLVED CODES]."
# ))
# def resolve_mf_scheme(query: str, mf_cocode: Optional[int] = None) -> str:
#     cleaned   = _clean_mf_query(query)
#     canonical = SCHEME_ALIAS_MAP.get(cleaned.lower().strip(), cleaned)
#     hits = _fuzzy_mf_scheme(canonical, mf_cocode=mf_cocode, limit=1, threshold=FUZZY_THRESHOLD)
#     if not hits:
#         hits = _fuzzy_mf_scheme(canonical, mf_cocode=None, limit=1, threshold=65)
#     if hits:
#         h = hits[0]
#         return (
#             f"RESOLVED\n"
#             f"Scheme    : {h.get('sch_name')}\n"
#             f"Category  : {h.get('category')}\n"
#             f"mf_schcode: {h.get('mf_schcode')}"
#         )
#     return (
#         f"NOT FOUND: '{canonical}'. "
#         "Try the full scheme name (e.g. 'Mirae Asset Emerging Bluechip Fund - Direct Plan (Growth)')."
#     )


# @mcp.tool(description=(
#     "Search mutual fund schemes by partial name, category, or keyword. "
#     "Returns scheme names for the user to pick from."
# ))
# def search_mf_schemes(query: str, limit: int = 5) -> str:
#     hits = _fuzzy_mf_scheme(query, limit=min(max(limit, 1), 10), threshold=55)
#     if not hits:
#         return f"No schemes found matching '{query}'."
#     lines = [f"Schemes matching '{query}':"]
#     for i, s in enumerate(hits, 1):
#         lines.append(f"  {i}. {s.get('sch_name')}  ({s.get('category')})")
#     return "\n".join(lines)


# # ── AMC-LEVEL TOOLS ────────────────────────────────────────────────────────────

# @mcp.tool(description="List all AMC / fund houses with scheme counts and AUM summary.")
# def get_all_fund_houses() -> str:
#     data, err = _get(EP["fund_house"])
#     if err:
#         return err
#     rows = _as_list(data)[:50]
#     FIELDS = ["lname", "fund_type", "osch", "csch", "sumoftotnav", "dateas"]
#     lines = [f"Fund Houses ({len(rows)} AMCs):", "─" * 55]
#     for i, row in enumerate(rows, 1):
#         p = _pick(row, FIELDS)
#         lines.append(
#             f"{i:>3}. {p.get('lname', 'N/A')}"
#             f"  [{p.get('fund_type', '')}]"
#             f"  Open: {p.get('osch', '?')}  Closed: {p.get('csch', '?')}"
#             f"  AUM: {p.get('sumoftotnav', 'N/A')}"
#         )
#     return "\n".join(lines)


# @mcp.tool(description=(
#     "List fund categories (Equity/Debt/Hybrid/All) for an AMC. "
#     "REQUIRES mf_cocode — call resolve_mf_fund first if not in PRE-RESOLVED CODES."
# ))
# def get_fund_categories(mf_cocode: int, category: str = "All") -> str:
#     val, err = _coerce_int(mf_cocode, "mf_cocode", "resolve_mf_fund")
#     if err:
#         return err
#     url  = EP["fund_category_amc"].format(mf_cocode=val, category=category)
#     data, err = _get(url)
#     if err:
#         return err
#     rows = _as_list(data)[:30]
#     if not rows:
#         return f"No categories found for the selected AMC under '{category}'."
#     lines = [f"Categories ({category}):"]
#     for row in rows:
#         p = _pick(row, ["maincategory", "vclass"])
#         lines.append(f"  • {p.get('maincategory', '')} — {p.get('vclass', '')}")
#     return "\n".join(lines)


# @mcp.tool(description=(
#     "List all scheme names offered by an AMC with their current NAV. "
#     "REQUIRES mf_cocode — call resolve_mf_fund first if not in PRE-RESOLVED CODES."
# ))
# def get_schemes_by_amc(mf_cocode: int) -> str:
#     val, err = _coerce_int(mf_cocode, "mf_cocode", "resolve_mf_fund")
#     if err:
#         return err
#     url  = EP["scheme_master"].format(mf_cocode=val)
#     data, err = _get(url)
#     if err:
#         return err
#     rows = _as_list(data)
#     if not rows:
#         return "No schemes found for the selected AMC."
#     lines = [f"Schemes ({len(rows)} total):"]
#     for i, row in enumerate(rows, 1):
#         p = _pick(row, ["sch_name", "category", "navrs"])
#         lines.append(
#             f"  {i:>3}. {p.get('sch_name', 'N/A')}"
#             f"  [{p.get('category', '')}]"
#             f"  NAV: ₹{p.get('navrs', 'N/A')}"
#         )
#     return "\n".join(lines)


# @mcp.tool(description=(
#     "Get fund profile / synopsis for an AMC: categories offered and class names. "
#     "REQUIRES mf_cocode — call resolve_mf_fund first if not in PRE-RESOLVED CODES."
# ))
# def get_fund_profile(mf_cocode: int) -> str:
#     val, err = _coerce_int(mf_cocode, "mf_cocode", "resolve_mf_fund")
#     if err:
#         return err
#     url  = EP["fund_profile"].format(mf_cocode=val)
#     data, err = _get(url)
#     if err:
#         return err
#     rows = _as_list(data)[:20]
#     if not rows:
#         return "No profile data found."
#     lines = ["Fund Profile — available scheme classes:"]
#     for row in rows:
#         p = _pick(row, ["VCLASS"])
#         if p.get("VCLASS"):
#             lines.append(f"  • {p['VCLASS']}")
#     return "\n".join(lines) if len(lines) > 1 else "No class data found."


# @mcp.tool(description="List all fund managers across AMCs with the schemes they manage and since when.")
# def get_fund_managers() -> str:
#     data, err = _get(EP["fund_manager"])
#     if err:
#         return err
#     rows = _as_list(data)[:30]
#     if not rows:
#         return "No fund manager data available."

#     sch_lookup = {s["mf_schcode"]: s["sch_name"] for s in _load_mf_scheme_cache()}

#     lines = [f"Fund Managers ({len(rows)} records):"]
#     for row in rows:
#         mgr   = row.get("fund_mgr") or row.get("FundMgr") or "N/A"
#         since = row.get("SinceDate") or row.get("sincedate") or ""
#         scode = row.get("mf_schcode") or row.get("MF_SCHCODE")
#         sname = sch_lookup.get(scode, "N/A") if scode else "N/A"
#         lines.append(f"  • {mgr}  |  Scheme: {sname}  |  Since: {since}")
#     return "\n".join(lines)


# # ── SCHEME-LEVEL TOOLS ─────────────────────────────────────────────────────────

# @mcp.tool(description=(
#     "Get today's NAV for a scheme: current NAV, previous NAV, change, % change. "
#     "REQUIRES mf_schcode — use PRE-RESOLVED CODES if available, else call resolve_mf_scheme first."
# ))
# def get_scheme_nav(mf_schcode: int) -> str:
#     val, err = _coerce_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
#     if err:
#         return err

#     data, err = _get(EP["daily_nav"])
#     if err:
#         return err

#     records = data.get("data", []) if isinstance(data, dict) else []
#     if not records:
#         return "No NAV data available."

#     match = next(
#         (r for r in records if int(float(r.get("mf_schcode", -1))) == val),
#         None,
#     )
#     if not match:
#         return f"NAV data not available for mf_schcode={val} today."

#     return (
#         f"Today's NAV — {match.get('mf_schname', 'N/A')}\n"
#         f"  Date       : {match.get('navdate', 'N/A')}\n"
#         f"  NAV        : ₹{match.get('nav', 'N/A')}\n"
#         f"  Prev NAV   : ₹{match.get('prevnav', 'N/A')}\n"
#         f"  Change     : {match.get('navchng', 'N/A')}  ({match.get('navperchng', 'N/A')}%)"
#     )


# @mcp.tool(description=(
#     "Get investment details: minimum investment, scheme objective, tax treatment. "
#     "REQUIRES mf_schcode — use PRE-RESOLVED CODES if available, else call resolve_mf_scheme first."
# ))
# def get_investment_details(mf_schcode: int) -> str:
#     val, err = _coerce_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
#     if err:
#         return err
#     url  = EP["investment_details"].format(mf_schcode=val)
#     data, err = _get(url)
#     if err:
#         return err
#     rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
#     if not rows:
#         return "No investment detail data found."
#     p = _pick(rows[0], ["inc_invest", "mininvt", "Objective", "taxbname", "TAXB"])
#     return (
#         f"Investment Details:\n"
#         f"  Min Investment      : ₹{p.get('mininvt', 'N/A')}\n"
#         f"  Inception Investment: ₹{p.get('inc_invest', 'N/A')}\n"
#         f"  Tax Treatment       : {p.get('taxbname') or p.get('TAXB', 'N/A')}\n"
#         f"  Objective           : {p.get('Objective', 'N/A')}"
#     )


# @mcp.tool(description=(
#     "Get expense ratio, entry/exit loads for a scheme. "
#     "REQUIRES mf_schcode — use PRE-RESOLVED CODES if available, else call resolve_mf_scheme first."
# ))
# def get_expense_ratio(mf_schcode: int) -> str:
#     val, err = _coerce_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
#     if err:
#         return err
#     data, err = _get(EP["expense_ratio"])
#     if err:
#         return err
#     rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)

#     match = next(
#         (r for r in rows if int(float(r.get("mf_schcode", -1))) == val),
#         None,
#     )
#     if not match:
#         return "Expense ratio data not found for this scheme."
#     p = _pick(match, ["EXPRATIO", "entry", "exit", "mininvt", "MaturityPeriod"])
#     return (
#         f"Expense & Load Details:\n"
#         f"  Expense Ratio  : {p.get('EXPRATIO', 'N/A')}%\n"
#         f"  Entry Load     : {p.get('entry', 'N/A')}\n"
#         f"  Exit Load      : {p.get('exit', 'N/A')}\n"
#         f"  Min Investment : ₹{p.get('mininvt', 'N/A')}"
#     )


# @mcp.tool(description=(
#     "Get average maturity, modified duration, YTM for DEBT fund schemes. "
#     "REQUIRES mf_schcode — use PRE-RESOLVED CODES if available, else call resolve_mf_scheme first."
# ))
# def get_avg_maturity(mf_schcode: int) -> str:
#     val, err = _coerce_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
#     if err:
#         return err
#     data, err = _get(EP["avg_maturity"])
#     if err:
#         return err
#     rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)

#     match = next(
#         (r for r in rows if int(float(r.get("mf_schcode", -1))) == val),
#         None,
#     )
#     if not match:
#         return "Average maturity data not found for this scheme."
#     p = _pick(match, ["avg_maturity", "ModDuration", "MacaulayDuration", "YTM", "AvgMaturityDate"])
#     return (
#         f"Debt Fund Metrics:\n"
#         f"  Avg Maturity      : {p.get('avg_maturity', 'N/A')}\n"
#         f"  Modified Duration : {p.get('ModDuration', 'N/A')}\n"
#         f"  Macaulay Duration : {p.get('MacaulayDuration', 'N/A')}\n"
#         f"  YTM               : {p.get('YTM', 'N/A')}%\n"
#         f"  As of Date        : {p.get('AvgMaturityDate', 'N/A')}"
#     )


# @mcp.tool(description=(
#     "Get historical AUM (Assets Under Management) for a scheme. "
#     "REQUIRES mf_schcode — use PRE-RESOLVED CODES if available, else call resolve_mf_scheme first."
# ))
# def get_scheme_aum(mf_schcode: int) -> str:
#     val, err = _coerce_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
#     if err:
#         return err
#     data, err = _get(EP["scheme_aum"])
#     if err:
#         return err
#     rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
#     matched = next(
#         (r for r in rows if int(float(r.get("mf_schcode", -1))) == val),
#         None,
#     )
#     if not matched:
#         return "AUM data not found for this scheme."
#     # matched may be a list of history records or a single record
#     history = matched if isinstance(matched, list) else [matched]
#     history = history[:24]
#     lines = ["Historical AUM:"]
#     for row in history:
#         p = _pick(row, ["AUMDate", "AUM"])
#         lines.append(f"  {p.get('AUMDate', 'N/A')}: ₹{p.get('AUM', 'N/A')} Cr")
#     return "\n".join(lines)


# @mcp.tool(description=(
#     "Get historical NAV for a scheme. "
#     "period: 'M' (months) or 'Y' (years). periodval: number of periods. "
#     "REQUIRES mf_schcode — use PRE-RESOLVED CODES if available, else call resolve_mf_scheme first."
# ))
# def get_nav_historical(mf_schcode: int, period: str = "Y", periodval: int = 1) -> str:
#     val, err = _coerce_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
#     if err:
#         return err
#     p  = _normalise_period(period)
#     pv = max(1, int(periodval) if periodval else 1)
#     url  = EP["nav_historical"].format(mf_schcode=val, period=p, periodval=pv)
#     data, err = _get(url)
#     if err:
#         return err
#     rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
#     if not rows:
#         return "No historical NAV data found."
#     lines = [f"Historical NAV (last {pv}{'Y' if p == 'Y' else 'M'}):"]
#     for row in rows:
#         pr = _pick(row, ["NavDate", "NAVRS", "adjnavrs"])
#         lines.append(f"  {pr.get('NavDate', 'N/A')}: ₹{pr.get('NAVRS', 'N/A')}")
#     return "\n".join(lines)


# @mcp.tool(description=(
#     "Get scheme returns across 1W/1M/3M/6M/1Y/3Y/5Y/Since Inception with benchmark comparison. "
#     "REQUIRES mf_schcode — use PRE-RESOLVED CODES if available, else call resolve_mf_scheme first."
# ))
# def get_scheme_returns(mf_schcode: int) -> str:
#     val, err = _coerce_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
#     if err:
#         return err
#     url  = EP["scheme_returns"].format(mf_schcode=val)
#     data, err = _get(url)
#     if err:
#         return err
#     rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
#     if not rows:
#         return "No returns data found."
#     row = rows[0]
#     p = _pick(row, ["sch_name", "Date", "1week", "1Month", "3Month", "6Month",
#                     "1Year", "3Year", "5Year", "Inception"])
#     return (
#         f"Returns — {p.get('sch_name', 'N/A')}  (as of {p.get('Date', 'N/A')})\n"
#         f"  1 Week   : {p.get('1week', 'N/A')}%\n"
#         f"  1 Month  : {p.get('1Month', 'N/A')}%\n"
#         f"  3 Months : {p.get('3Month', 'N/A')}%\n"
#         f"  6 Months : {p.get('6Month', 'N/A')}%\n"
#         f"  1 Year   : {p.get('1Year', 'N/A')}%\n"
#         f"  3 Years  : {p.get('3Year', 'N/A')}%\n"
#         f"  5 Years  : {p.get('5Year', 'N/A')}%\n"
#         f"  Inception: {p.get('Inception', 'N/A')}%"
#     )


# @mcp.tool(description=(
#     "Get lumpsum return details: 'If I invested ₹X N years ago, what is it worth now?' "
#     "Returns value and % return for 1W/1M/3M/6M/1Y/3Y/5Y/10Y/Since Inception. "
#     "REQUIRES mf_schcode — use PRE-RESOLVED CODES if available, else call resolve_mf_scheme first."
# ))
# def get_lumpsum_returns(mf_schcode: int) -> str:
#     val, err = _coerce_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
#     if err:
#         return err
#     url  = EP["lumpsum_return"].format(mf_schcode=val)
#     data, err = _get(url)
#     if err:
#         return err
#     rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
#     if not rows:
#         return "No lumpsum return data found."
#     r = rows[0]

#     def _rv(key): return r.get(key, "N/A")

#     return (
#         f"Lumpsum Returns (Investment: ₹{_rv('InvAmount')}):\n"
#         f"  1 Week    : ₹{_rv('W1LatestValue')}  ({_rv('W1Return_Abs')}% abs)\n"
#         f"  1 Month   : ₹{_rv('M1LatestValue')}  ({_rv('M1Return_Ann')}% ann)\n"
#         f"  3 Months  : ₹{_rv('M3LatestValue')}  ({_rv('M3Return_Ann')}% ann)\n"
#         f"  6 Months  : ₹{_rv('M6LatestValue')}  ({_rv('M6Return_Ann')}% ann)\n"
#         f"  1 Year    : ₹{_rv('Y1LatestValue')}  ({_rv('Y1Return_Ann')}% ann)\n"
#         f"  3 Years   : ₹{_rv('Y3LatestValue')}  ({_rv('Y3Return_Ann')}% ann)\n"
#         f"  5 Years   : ₹{_rv('Y5LatestValue')}  ({_rv('Y5Return_Ann')}% ann)\n"
#         f"  10 Years  : ₹{_rv('Y10LatestValue')}  ({_rv('Y10Return_Ann')}% ann)\n"
#         f"  Inception : ₹{_rv('InceptionLatestValue')}  ({_rv('InceptionReturn_Ann')}% ann)"
#     )


# @mcp.tool(description=(
#     "Compare multiple schemes side-by-side on returns, NAV, AUM, fund manager, exit load. "
#     "REQUIRES comma-separated mf_schcodes. Use resolve_mf_scheme to obtain each code."
# ))
# def compare_schemes(mf_schcodes: str) -> str:
#     if not mf_schcodes or not mf_schcodes.strip():
#         return "Provide a comma-separated list of scheme codes (e.g. '19283,204,17152')."
#     url  = EP["scheme_comparison"].format(schcodes=mf_schcodes.strip())
#     data, err = _get(url)
#     if err:
#         return err
#     rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
#     if not rows:
#         return "No comparison data found."
#     FIELDS = ["Sch_Name", "VClass", "NAVRS", "NavDate", "SchemeAssets",
#               "FundManager", "1WEEK", "1MONTH", "3MONTH", "6MONTH",
#               "1YEAR", "3YEAR", "5YEAR", "INCEPTION", "ExitLoad"]
#     lines = [f"Scheme Comparison ({len(rows)} schemes):"]
#     for row in rows:
#         p = _pick(row, FIELDS)
#         lines.append(f"\n  ── {p.get('Sch_Name', 'N/A')} [{p.get('VClass', '')}] ──")
#         lines.append(f"     NAV        : ₹{p.get('NAVRS', 'N/A')}  (as of {p.get('NavDate', 'N/A')})")
#         lines.append(f"     AUM        : ₹{p.get('SchemeAssets', 'N/A')} Cr")
#         lines.append(f"     Manager    : {p.get('FundManager', 'N/A')}")
#         lines.append(f"     Returns    : 1W {p.get('1WEEK','N/A')}% | 1M {p.get('1MONTH','N/A')}% | "
#                      f"1Y {p.get('1YEAR','N/A')}% | 3Y {p.get('3YEAR','N/A')}% | "
#                      f"5Y {p.get('5YEAR','N/A')}% | Since Inception {p.get('INCEPTION','N/A')}%")
#         lines.append(f"     Exit Load  : {p.get('ExitLoad', 'N/A')}")
#     return "\n".join(lines)


# @mcp.tool(description=(
#     "Get top-performing funds by type and category. "
#     "fund_type: 'Equity' (default) / 'Debt' / 'Hybrid'. "
#     "category: 'all' (default) or specific category name."
# ))
# def get_fund_performance(top: int = 10, fund_type: str = "Equity", category: str = "all") -> str:
#     ft = fund_type.strip().title()
#     if ft not in {"Equity", "Debt", "Hybrid"}:
#         return f"fund_type must be Equity, Debt, or Hybrid — got '{fund_type}'."
#     url  = EP["fund_performance"].format(top=top, type=ft, category=category)
#     data, err = _get(url)
#     if err:
#         return err
#     rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
#     if not rows:
#         return "No performance data found."
#     lines = [f"Top {len(rows)} {ft} Funds ({category}):"]
#     for i, row in enumerate(rows, 1):
#         p = _pick(row, ["schemename", "Scheme", "TypeName", "NavRs", "NavDate"])
#         name = p.get("schemename") or p.get("Scheme", "N/A")
#         lines.append(f"  {i:>3}. {name}  [{p.get('TypeName', '')}]  NAV: ₹{p.get('NavRs', 'N/A')}")
#     return "\n".join(lines)


# @mcp.tool(description="Get average category returns for Equity/Debt/Hybrid across time periods.")
# def get_category_performance(fund_type: str = "Equity", top: int = 20) -> str:
#     ft = fund_type.strip().title()
#     url  = EP["category_performance"].format(type=ft, top=top)
#     data, err = _get(url)
#     if err:
#         return err
#     rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
#     if not rows:
#         return "No category performance data found."
#     RFIELDS = ["typename", "ret1w", "ret1m", "ret3m", "ret6m",
#                "ret1y", "ret3y", "ret5y", "ret10y", "retinception", "categoryreturndate"]
#     lines = [f"Category Returns — {ft}:"]
#     for row in rows:
#         p = _pick(row, RFIELDS)
#         lines.append(
#             f"  {p.get('typename', 'N/A'):<35}"
#             f"  1Y: {p.get('ret1y','N/A')}%"
#             f"  3Y: {p.get('ret3y','N/A')}%"
#             f"  5Y: {p.get('ret5y','N/A')}%"
#         )
#     return "\n".join(lines)


# @mcp.tool(description="Get SIP/SWP/STP available dates and minimum amounts. plan: 'SIP' / 'SWP' / 'STP'.")
# def get_sip_dates(plan: str = "SIP") -> str:
#     p = plan.strip().upper()
#     if p not in ("SIP", "SWP", "STP"):
#         return f"plan must be 'SIP', 'SWP', or 'STP' — got '{plan}'."
#     url  = EP["sip_dates"].format(plan=p)
#     data, err = _get(url)
#     if err:
#         return err
#     rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
#     if not rows:
#         return f"No {p} date data found."
#     lines = [f"{p} Available Dates:"]
#     for row in rows[:10]:
#         pr = _pick(row, ["frequency", "d1", "d2", "d3"])
#         dates = ", ".join(str(pr[d]) for d in ["d1","d2","d3"] if pr.get(d))
#         lines.append(f"  {pr.get('frequency', 'N/A')}: {dates}")
#     return "\n".join(lines)


# @mcp.tool(description=(
#     "Get SIP/SWP dates and minimum amounts for a specific scheme. "
#     "REQUIRES mf_schcode — use PRE-RESOLVED CODES if available, else call resolve_mf_scheme first."
# ))
# def get_scheme_sip_details(mf_schcode: int) -> str:
#     val, err = _coerce_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
#     if err:
#         return err
#     url  = EP["scheme_sip_swp"].format(mf_schcode=val)
#     data, err = _get(url)
#     if err:
#         return err
#     rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
#     if not rows:
#         return "No SIP/SWP data found for this scheme."
#     lines = ["SIP / SWP Details:"]
#     for row in rows:
#         p = _pick(row, ["SIPDates", "minamt", "multamt", "avail_period"])
#         lines.append(
#             f"  Dates: {p.get('SIPDates', 'N/A')}"
#             f"  |  Min: ₹{p.get('minamt', 'N/A')}"
#             f"  |  Multiples: ₹{p.get('multamt', 'N/A')}"
#             f"  |  Period: {p.get('avail_period', 'N/A')}"
#         )
#     return "\n".join(lines)


# @mcp.tool(description=(
#     "Get top stock holdings of a fund with % holding, market value, sector. "
#     "REQUIRES mf_schcode — use PRE-RESOLVED CODES if available, else call resolve_mf_scheme first."
# ))
# def get_mf_holdings(mf_schcode: int) -> str:
#     val, err = _coerce_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
#     if err:
#         return err
#     url  = EP["mf_holding"].format(mf_schcode=val)
#     data, err = _get(url)
#     if err:
#         return err
#     rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
#     if not rows:
#         return "No holdings data found."
#     lines = [f"Top Holdings ({len(rows)} stocks):"]
#     for i, row in enumerate(rows, 1):
#         p = _pick(row, ["co_name", "perc_hold", "mktvalue", "no_shares", "iind_name", "type", "rating"])
#         lines.append(
#             f"  {i:>3}. {p.get('co_name', 'N/A')}"
#             f"  [{p.get('iind_name', p.get('type', ''))}]"
#             f"  {p.get('perc_hold', 'N/A')}%"
#             f"  ₹{p.get('mktvalue', 'N/A')} Cr"
#         )
#     return "\n".join(lines)


# @mcp.tool(description=(
#     "Get all mutual funds that hold a specific stock. "
#     "REQUIRES co_code — use the equifiz stock server's resolve_nse_symbol first, "
#     "or use the co_code from PRE-RESOLVED CODES if present."
# ))
# def get_funds_holding_company(co_code: int, top: int = 10) -> str:
#     val, err = _coerce_int(co_code, "co_code", "resolve_nse_symbol (stock server)")
#     if err:
#         return err
#     url  = EP["company_mf_holding"].format(co_code=val, top=top)
#     data, err = _get(url)
#     if err:
#         return err
#     rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
#     if not rows:
#         return "No mutual funds found holding this stock."
#     lines = [f"Funds holding this stock ({len(rows)}):"]
#     for i, row in enumerate(rows, 1):
#         p = _pick(row, ["sch_name", "invdate", "mktvalue", "perc_hold", "no_shares"])
#         lines.append(
#             f"  {i:>3}. {p.get('sch_name', 'N/A')}"
#             f"  |  {p.get('perc_hold', 'N/A')}% of portfolio"
#             f"  |  ₹{p.get('mktvalue', 'N/A')} Cr"
#         )
#     return "\n".join(lines)


# @mcp.tool(description=(
#     "Get sector allocation (IT/Banking/Pharma %) for a fund. "
#     "REQUIRES mf_schcode — use PRE-RESOLVED CODES if available, else call resolve_mf_scheme first."
# ))
# def get_sector_allocation(mf_schcode: int) -> str:
#     val, err = _coerce_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
#     if err:
#         return err
#     url  = EP["sector_allocation"].format(mf_schcode=val)
#     data, err = _get(url)
#     if err:
#         return err
#     rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
#     if not rows:
#         return "No sector allocation data found."
#     lines = [f"Sector Allocation (as of {rows[0].get('secdate', 'N/A')}):"]
#     for row in rows:
#         p = _pick(row, ["sector", "perc_hold", "value"])
#         lines.append(f"  {p.get('sector', 'N/A'):<35}: {p.get('perc_hold', 'N/A')}%")
#     return "\n".join(lines)


# @mcp.tool(description=(
#     "Get asset allocation (equity/debt/cash %) for a fund. "
#     "REQUIRES mf_schcode — use PRE-RESOLVED CODES if available, else call resolve_mf_scheme first."
# ))
# def get_asset_allocation(mf_schcode: int) -> str:
#     val, err = _coerce_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
#     if err:
#         return err
#     url  = EP["asset_allocation"].format(mf_schcode=val)
#     data, err = _get(url)
#     if err:
#         return err
#     rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
#     if not rows:
#         return "No asset allocation data found."
#     as_of = rows[0].get("currentmonth", "N/A")
#     lines = [f"Asset Allocation (as of {as_of}):"]
#     for row in rows:
#         p = _pick(row, ["assetname", "holding_currentmonth", "holding_prevmonth"])
#         lines.append(
#             f"  {p.get('assetname', 'N/A'):<20}"
#             f": {p.get('holding_currentmonth', 'N/A')}%"
#             f"  (prev: {p.get('holding_prevmonth', 'N/A')}%)"
#         )
#     return "\n".join(lines)


# @mcp.tool(description=(
#     "Get stocks recently added to / removed from a fund. "
#     "move_type: 'in' (new buys) / 'out' (sold) / 'un' (unchanged). "
#     "REQUIRES mf_schcode — use PRE-RESOLVED CODES if available, else call resolve_mf_scheme first."
# ))
# def get_portfolio_changes(mf_schcode: int, move_type: str = "in") -> str:
#     val, err = _coerce_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
#     if err:
#         return err
#     t = move_type.lower()
#     if t not in ("in", "out", "un"):
#         return "move_type must be 'in', 'out', or 'un'."
#     url   = EP["whats_in_out"].format(type=t, mf_schcode=val)
#     data, err = _get(url)
#     if err:
#         return err
#     rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
#     if not rows:
#         labels = {"in": "new buys", "out": "sold holdings", "un": "unchanged positions"}
#         return f"No {labels[t]} found for this fund."
#     label = {"in": "New Buys", "out": "Sold Positions", "un": "Unchanged"}[t]
#     lines = [f"{label} ({len(rows)} stocks):"]
#     for i, row in enumerate(rows, 1):
#         p = _pick(row, ["CO_NAME", "Perc_Hold", "mktvalue", "NO_SHARES", "AssetType"])
#         lines.append(
#             f"  {i:>3}. {p.get('CO_NAME', 'N/A')}"
#             f"  [{p.get('AssetType', '')}]"
#             f"  {p.get('Perc_Hold', 'N/A')}%"
#         )
#     return "\n".join(lines)


# @mcp.tool(description=(
#     "Get large/mid/small-cap allocation % for an equity fund. "
#     "REQUIRES mf_schcode — use PRE-RESOLVED CODES if available, else call resolve_mf_scheme first."
# ))
# def get_mcap_allocation(mf_schcode: int) -> str:
#     val, err = _coerce_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
#     if err:
#         return err
#     url  = EP["mcap_equity"].format(mf_schcode=val)
#     data, err = _get(url)
#     if err:
#         return err
#     rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
#     if not rows:
#         return "No market cap allocation data found."
#     as_of = rows[0].get("CurrPFDate", "N/A")
#     lines = [f"Market Cap Allocation (as of {as_of}):"]
#     for row in rows:
#         p = _pick(row, ["mcaptype", "perc_hold", "MarketValue"])
#         lines.append(f"  {p.get('mcaptype', 'N/A'):<15}: {p.get('perc_hold', 'N/A')}%")
#     return "\n".join(lines)


# @mcp.tool(description=(
#     "Get stocks most bought or sold by a fund this month. "
#     "REQUIRES mf_schcode — use PRE-RESOLVED CODES if available, else call resolve_mf_scheme first."
# ))
# def get_most_bought_sold(mf_schcode: int) -> str:
#     val, err = _coerce_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
#     if err:
#         return err
#     url  = EP["most_sold_bought"].format(mf_schcode=val)
#     data, err = _get(url)
#     if err:
#         return err
#     rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
#     if not rows:
#         return "No buy/sell activity data found."
#     lines = ["Most Bought / Sold This Month:"]
#     for i, row in enumerate(rows, 1):
#         p = _pick(row, ["CO_NAME", "currmktvalue", "prevmktvalue", "diff", "chng"])
#         chng = float(p.get("chng", 0) or 0)
#         direction = "▲ BUY" if chng >= 0 else "▼ SELL"
#         lines.append(
#             f"  {i:>3}. {direction}  {p.get('CO_NAME', 'N/A')}"
#             f"  |  Change: ₹{p.get('diff', 'N/A')} Cr ({chng:+.2f}%)"
#         )
#     return "\n".join(lines)


# @mcp.tool(description="Get currently open New Fund Offers (NFO) with objective, min investment, close date.")
# def get_new_fund_offers() -> str:
#     data, err = _get(EP["new_fund_offer"])
#     if err:
#         return err
#     rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
#     if not rows:
#         return "No open NFOs at this time."
#     lines = [f"Open NFOs ({len(rows)}):"]
#     for i, row in enumerate(rows, 1):
#         p = _pick(row, ["sch_name", "lname", "launc_date", "cldate", "mininvt", "schemetype"])
#         lines.append(
#             f"  {i:>3}. {p.get('sch_name', 'N/A')}"
#             f"  by {p.get('lname', 'N/A')}"
#             f"  [{p.get('schemetype', '')}]"
#             f"  Opens: {p.get('launc_date', 'N/A')}"
#             f"  Closes: {p.get('cldate', 'N/A')}"
#             f"  Min: ₹{p.get('mininvt', 'N/A')}"
#         )
#     return "\n".join(lines)


# @mcp.tool(description="Get overall MF market activity: gross equity/debt purchases and net flows.")
# def get_mf_market_activity() -> str:
#     data, err = _get(EP["mf_activities"])
#     if err:
#         return err
#     rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
#     if not rows:
#         return "No market activity data available."
#     FIELDS = ["TRANS_DATE", "EQ_GR_PURC", "EQ_GR_SALE", "EQ_G_NE_PS", "DE_GR_PURC", "DE_GR_SALE"]
#     lines = ["MF Market Activity (₹ Cr):"]
#     lines.append(f"  {'Date':<12}  {'Eq.Buy':>12}  {'Eq.Sell':>12}  {'Eq.Net':>12}  {'Debt Buy':>12}")
#     for row in rows:
#         p = _pick(row, FIELDS)
#         lines.append(
#             f"  {str(p.get('TRANS_DATE',''))[:10]:<12}"
#             f"  {str(p.get('EQ_GR_PURC','N/A')):>12}"
#             f"  {str(p.get('EQ_GR_SALE','N/A')):>12}"
#             f"  {str(p.get('EQ_G_NE_PS','N/A')):>12}"
#             f"  {str(p.get('DE_GR_PURC','N/A')):>12}"
#         )
#     return "\n".join(lines)


# @mcp.tool(description=(
#     "Get risk ratios: Beta, Alpha, Sharpe, Std Dev (SD), Treynor for a scheme. "
#     "REQUIRES mf_schcode — use PRE-RESOLVED CODES if available, else call resolve_mf_scheme first."
# ))
# def get_scheme_ratios(mf_schcode: int) -> str:
#     val, err = _coerce_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
#     if err:
#         return err
#     data, err = _get(EP["scheme_ratios"])
#     if err:
#         return err
#     rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
#     match = next(
#         (r for r in rows if str(r.get("MF_SCHCODE") or r.get("Mf_SchCode") or "") == str(val)),
#         None,
#     )
#     if not match:
#         return "Risk ratio data not found for this scheme."
#     p = _pick(match, ["Scheme_Nam", "DATE", "BETA", "SD", "TREYNOR", "ALPHA", "SHARPE"])
#     return (
#         f"Risk Ratios — {p.get('Scheme_Nam', 'N/A')}  (as of {p.get('DATE', 'N/A')})\n"
#         f"  Beta    : {p.get('BETA', 'N/A')}\n"
#         f"  Std Dev : {p.get('SD', 'N/A')}\n"
#         f"  Treynor : {p.get('TREYNOR', 'N/A')}\n"
#         f"  Alpha   : {p.get('ALPHA', 'N/A')}\n"
#         f"  Sharpe  : {p.get('SHARPE', 'N/A')}"
#     )


# @mcp.tool(description=(
#     "Get recent dividend announcements for a scheme. "
#     "REQUIRES mf_schcode — use PRE-RESOLVED CODES if available, else call resolve_mf_scheme first."
# ))
# def get_dividend_details(mf_schcode: int) -> str:
#     val, err = _coerce_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
#     if err:
#         return err
#     url  = EP["dividend_details"].format(mf_schcode=val)
#     data, err = _get(url)
#     if err:
#         return err
#     rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
#     if not rows:
#         return "No dividend announcements found for this scheme."
#     lines = ["Dividend History:"]
#     for row in rows:
#         p = _pick(row, ["sch_name", "DivAmount", "DIVPERPU", "Divtype", "DivDate", "RecordDate"])
#         lines.append(
#             f"  {p.get('DivDate', 'N/A')}"
#             f"  ₹{p.get('DivAmount', 'N/A')} per unit"
#             f"  [{p.get('Divtype', '')}]"
#             f"  Record Date: {p.get('RecordDate', 'N/A')}"
#         )
#     return "\n".join(lines)


# @mcp.tool(description=(
#     "Get latest MF industry news. "
#     "sno: '-' for latest batch, or a serial number for a specific article."
# ))
# def get_mf_news(sno: str = "-") -> str:
#     url  = EP["mf_news"].format(sno=sno.strip() or "-")
#     data, err = _get(url)
#     if err:
#         return err
#     rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
#     if not rows:
#         return "No news available."
#     lines = ["MF News:"]
#     for row in rows:
#         p = _pick(row, ["date", "heading", "caption", "section_name"])
#         lines.append(f"\n  [{p.get('date', 'N/A')}]  {p.get('heading', 'N/A')}")
#         if p.get("caption"):
#             lines.append(f"  {p['caption']}")
#     return "\n".join(lines)


# @mcp.tool(description=(
#     "Get BSE Star platform transaction details for a scheme: modes, SIP/SWP flags, settlement. "
#     "REQUIRES mf_schcode — use PRE-RESOLVED CODES if available, else call resolve_mf_scheme first."
# ))
# def get_bse_star_scheme(mf_schcode: int) -> str:
#     val, err = _coerce_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
#     if err:
#         return err
#     url  = EP["bse_star_scheme"].format(mf_schcode=val)
#     data, err = _get(url)
#     if err:
#         return err
#     rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
#     if not rows:
#         return "No BSE Star data found for this scheme."
#     p = _pick(rows[0], ["SCH_NAME", "Scheme_Type", "SIP_FLAG", "STP_FLAG", "SWP_FLAG",
#                         "Settlement_Type", "ISIN", "Dividend_Reinvestment_Flag"])
#     return (
#         f"BSE Star Details — {p.get('SCH_NAME', 'N/A')}\n"
#         f"  Scheme Type         : {p.get('Scheme_Type', 'N/A')}\n"
#         f"  SIP Available       : {'Yes' if p.get('SIP_FLAG') == 1 else 'No'}\n"
#         f"  STP Available       : {'Yes' if p.get('STP_FLAG') == 1 else 'No'}\n"
#         f"  SWP Available       : {'Yes' if p.get('SWP_FLAG') == 1 else 'No'}\n"
#         f"  Settlement Type     : {p.get('Settlement_Type', 'N/A')}\n"
#         f"  ISIN                : {p.get('ISIN', 'N/A')}"
#     )


# @mcp.tool(description="Get AMFI code mapping for a scheme (AMFI code, ISIN). Pass mf_schcode to filter.")
# def get_amfi_master(mf_schcode: Optional[int] = None) -> str:
#     data, err = _get(EP["amfi_master"])
#     if err:
#         return err
#     rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
#     if mf_schcode is not None:
#         # Coerce to int for comparison
#         val, err = _coerce_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
#         if not err:
#             rows = [r for r in rows if str(r.get("mf_schcode") or "") == str(val)]
#     rows = rows[:20]
#     if not rows:
#         return "No AMFI data found."
#     lines = ["AMFI Mapping:"]
#     for row in rows:
#         p = _pick(row, ["amficode", "growth_payoutisin", "reinvestmentisin"])
#         lines.append(
#             f"  AMFI Code : {p.get('amficode', 'N/A')}"
#             f"  |  Growth ISIN: {p.get('growth_payoutisin', 'N/A')}"
#             f"  |  Reinvest ISIN: {p.get('reinvestmentisin', 'N/A')}"
#         )
#     return "\n".join(lines)


# # ══════════════════════════════════════════════════════════════════════════════
# if __name__ == "__main__":
#     mcp.run()







"""
mf_equifiz_server.py — Equifiz Mutual Fund MCP Server

Design principles:
  - Every tool returns ONLY the fields the LLM needs to answer the user.
  - Internal IDs (mf_cocode, mf_schcode, co_code, classcode, etc.) are
    NEVER surfaced to the end-user answer — they are plumbing only.
  - Resolvers (resolve_mf_fund / resolve_mf_scheme) use the local DB cache;
    all data tools hit the REST API and filter to a tight field whitelist.
  - No generic _mf_tool / _fmt soup — each tool knows exactly what it wants.
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
logger = logging.getLogger("equifiz_mf")

# ── Config ─────────────────────────────────────────────────────────────────────

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

# ── Alias maps ─────────────────────────────────────────────────────────────────

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

# ── HTTP session ───────────────────────────────────────────────────────────────

_session = requests.Session()
_session.headers.update({
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
})

# ── DB cache ───────────────────────────────────────────────────────────────────

_mf_amc_cache:    list[dict] = []
_mf_scheme_cache: list[dict] = []
_company_co_cache: list[dict] = []
_group_cache: list[dict] = []



###################################################### scheme and fund house related function ##################################################
def _db():
    return psycopg2.connect(**DB)


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


# ── Query cleaning ─────────────────────────────────────────────────────────────

_MF_NOISE = [
    r"^(what is|what are|what's|give me|show me|tell me|find|get|check|fetch)\s+(the\s+)?",
    r"^(nav|returns?|aum|performance|details?|info|information)\s+(of|for|about)\s+",
    r"^(details?\s+(of|about|for)\s+)",
    r"\b(fund|mutual fund|scheme|plan|option|growth|idcw|direct|regular|dividend)\b",
]


def _clean_mf_query(query: str) -> str:
    q = query.strip().rstrip("?!.")
    for p in _MF_NOISE:
        q = re.sub(p, " ", q, flags=re.IGNORECASE).strip()
    return re.sub(r"\s{2,}", " ", q).strip() or query


# ── HTTP helper ────────────────────────────────────────────────────────────────

def _get(url: str) -> tuple[list | dict | None, str | None]:
    try:
        r = _session.get(url, timeout=15)
        r.raise_for_status()
        return r.json(), None
    except requests.HTTPError as e:
        return None, f"API error {e.response.status_code}: {e.response.text[:200]}"
    except Exception as e:
        return None, f"Request failed: {e}"


# ── Field-pick helpers ─────────────────────────────────────────────────────────

def _pick(record: dict, fields: list[str]) -> dict:
    """Return only the requested keys (case-insensitive), skipping None/empty."""
    low = {k.lower(): v for k, v in record.items()}
    out = {}
    for f in fields:
        v = low.get(f.lower())
        if v is not None and v != "":
            out[f] = v
    return out


def _format_rows(rows: list[dict], fields: list[str], cap: int = 20) -> str:
    rows = rows[:cap]
    lines = []
    for i, row in enumerate(rows, 1):
        picked = _pick(row, fields)
        lines.append(f"[{i}] " + "  |  ".join(f"{k}: {v}" for k, v in picked.items()))
    return "\n".join(lines) if lines else "No data."


def _format_single(row: dict, fields: list[str]) -> str:
    picked = _pick(row, fields)
    return "\n".join(f"  {k:<28}: {v}" for k, v in picked.items()) or "No data."


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


def _normalise_period(period: str) -> str:
    p = period.strip().upper()
    if p in ("M", "MONTH", "MONTHS"):
        return "M"
    if p in ("D", "DAY", "DAYS"):
        logger.warning("period='%s' not valid; defaulting to Y", period)
    return "Y"


# ── Fuzzy matchers ─────────────────────────────────────────────────────────────

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
#####################################################################################################################################


#################################################co_code and stock company related functions ###########################################

VALID_EXCHANGES   = {"BSE", "NSE"}
EXCHANGE_ALIASES  = {"BOMBAY": "BSE", "NATIONAL": "NSE", "B": "BSE", "N": "NSE"}

BRAND_MAP = {
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


def _load_company_cache() -> list[dict]:
    global _company_co_cache
    if _company_co_cache:
        return _company_co_cache
    try:
        conn = _db()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT co_code, companyname, nsesymbol, bsecode, sectorname, industryname, isin FROM companies")
            _company_co_cache = [dict(r) for r in cur.fetchall()]
        conn.close()
    except Exception as e:
        logger.error("Cache load failed: %s", e)
    return _company_co_cache

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

def _by_code(co_code: int) -> dict | None:
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

def _fuzzy(query: str, limit: int = 5, threshold: int = FUZZY_THRESHOLD) -> list[dict]:
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

# ── API helper ─────────────────────────────────────────────────────────────────

def _get(url: str, label: str) -> tuple[dict | list | None, str | None]:
    try:
        r = _session.get(url, timeout=15)
        r.raise_for_status()
        return r.json(), None
    except requests.HTTPError as e:
        return None, f"API error {e.response.status_code} for {label}: {e.response.text[:200]}"
    except Exception as e:
        return None, f"Error fetching {label}: {e}"

# ── Formatting ─────────────────────────────────────────────────────────────────

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

def _normalise_exchange(exchange: str) -> str:
    resolved = EXCHANGE_ALIASES.get(exchange.strip().upper(), exchange.strip().upper())
    if resolved not in VALID_EXCHANGES:
        raise ValueError(f"Invalid exchange '{exchange}'. Use 'NSE' or 'BSE'.")
    return resolved

def _clean_query(query: str) -> str:
    """Strip scaffolding phrases to extract the company name."""
    q = query.strip().rstrip("?!.")
    patterns = [
        r"^(what is|what are|what's|give me|show me|tell me about|find|get|check|analyse|analyze)\s+(the\s+)?",
        r"^(pe ratio|eps|mcap|revenue|profit|share price|price|financials|details?)\s+(of|for|about)\s+",
        r"^(details?\s+(of|about|for)\s+)",
        r"^(how is|is|are)\s+",
    ]
    for p in patterns:
        q = re.sub(p, "", q, flags=re.IGNORECASE).strip()
    # Remove noise words
    q = re.sub(r"\b(company|co|corp|ltd|limited|inc|stock|share|equity|nse|bse|listed|group)\b", "", q, flags=re.IGNORECASE)
    return re.sub(r"\s{2,}", " ", q).strip() or query

########################################################################################################################################


###################################################### group master related configuration #################################################

def _load_group_cache() -> list[dict]:
    global _group_cache
    if _group_cache:
        return _group_cache
    try:
        conn = _db()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT group_name,exchange FROM group_master")
            _group_cache = [dict(r) for r in cur.fetchall()]
        conn.close()
    except Exception as e:
        logger.error("Group cache load failed: %s", e)
    return _group_cache


def _fuzzy_group(query: str, exchange: str | None = None, limit: int = 1, threshold: int = 55) -> list[dict]:
    """
    Resolve a natural-language group query to rows from group_master.

    Strategy (scores are combined, highest wins):
      1. Exact match on group_name          → score 100
      2. token_set_ratio on group_name       → handles word-order noise
      3. partial_ratio  on display_name      → handles verbose labels
      4. Acronym expansion match             → "BN" → "BANKNIFTY"

    Returns up to `limit` rows sorted by descending score.
    """
    scored: list[tuple[dict, int]] = []
    q = query.strip().upper()
    q_lower = q.lower()

    # Build a lightweight acronym from the query (e.g. "bank nifty" → "BN")
    query_acronym = "".join(w[0] for w in q.split() if w)

    for grp in _load_group_cache():
        # Optionally restrict to a specific exchange
        if exchange and (grp.get("exchange") or "").upper() != exchange.upper():
            continue

        g_name    = (grp.get("group_name")    or "").upper()
        g_display = (grp.get("display_name")  or "").lower()

        # 1. Exact
        if q == g_name:
            scored.append((grp, 100))
            continue

        # 2. Fuzzy on group_name (handles "BANK NIFTY" → "BANKNIFTY", typos, etc.)
        score_name = max(
            fuzz.token_set_ratio(q_lower, g_name.lower()),
            fuzz.ratio(q, g_name),
            fuzz.partial_ratio(q, g_name),
        )

        # 3. Fuzzy on display_name / human label
        score_display = fuzz.token_set_ratio(q_lower, g_display)

        # 4. Acronym expansion:  user types "BN" and group is "BANKNIFTY"
        #    Build the group's own acronym and compare
        g_acronym = "".join(w[0] for w in g_name.split() if w)  # usually single-word → itself
        # For multi-word display names like "Bank Nifty":
        g_display_acronym = "".join(w[0].upper() for w in g_display.split() if w)

        score_acronym = max(
            fuzz.ratio(query_acronym, g_acronym),
            fuzz.ratio(query_acronym, g_display_acronym),
            # also try if the whole query IS a prefix of the group name
            100 if g_name.startswith(q) and len(q) >= 3 else 0,
        )

        final_score = max(score_name, score_display, score_acronym)

        if final_score >= threshold:
            scored.append((grp, final_score))

    scored.sort(key=lambda x: x[1], reverse=True)
    return [grp for grp, _ in scored[:limit]]


def _resolve_group(query: str, exchange: str | None = None) -> str | None:
    """
    Public helper: return the canonical group_name string or None.
    Logs a warning when the match score is mediocre (helpful for debugging).
    """
    results = _fuzzy_group(query, exchange=exchange, limit=1)
    if not results:
        logger.warning("Could not resolve group from query: %r", query)
        return None
    resolved = results[0]["group_name"]
    logger.debug("Resolved group %r → %r", query, resolved)
    return resolved


###########################################################################################################################################3


# ── API endpoint map ───────────────────────────────────────────────────────────

EP = {
    "fund_house":           f"{BASE_URL}/Fund_House",
    "fund_category_amc":    f"{BASE_URL}/FundCategoryAMCWise/{{mf_cocode}}/{{category}}",
    "scheme_master":        f"{BASE_URL}/SchemeMaster/{{mf_cocode}}",
    "amfi_master":          f"{BASE_URL}/AMFIMaster",
    "fund_manager":         f"{BASE_URL}/FundManager",
    "sip_dates":            f"{BASE_URL}/SIP_Dates/{{plan}}",
    "bse_star_scheme":      f"{BASE_URL}/BSEStarSchemeMaster/{{mf_schcode}}",
    "fund_profile":         f"{BASE_URL}/fund-profile/{{mf_cocode}}",
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
}

ENDPOINTS = {
    "key_ratios":       f"{BASE_URL}/KeyFinancialRatios/{{co_code}}/{{t}}",
    "daily_ratios":     f"{BASE_URL}/DailyRatios/{{co_code}}/{{t}}",
    "forthcoming_ipo":  f"{BASE_URL}/forthcomingipo/{{ex}}/Ipo/{{n}}",
    "open_ipo":         f"{BASE_URL}/OpenIssues/{{ex}}/Ipo/{{n}}",
    "closed_ipo":       f"{BASE_URL}/ClosedIssues/{{ex}}/IPO/{{n}}",
    "new_listing":      f"{BASE_URL}/Newlisting/{{ex}}/{{n}}",
    "best_ipo":         f"{BASE_URL}/BestPerformerIpo/{{ex}}/{{n}}",
    # ── Price feed ──────────────────────────────────────────────────────────
    "delayed_prices":   f"{BASE_URL}/BseNseDelayedPriceData/{{ex}}",
    "company_quotes":   f"{BASE_URL}/GetQuotes/{{co_code}}/{{ex}}",
    "indices":          f"{BASE_URL}/Indices",
    "exchange_holidays": f"{BASE_URL}/ExchangeHolidays/{{ex}}",
    "active_performer" : f"{BASE_URL}/MostActiveToppers/{{ex}}/{{group}}/value/{{record_count}}",
    "gainers" : f"{BASE_URL}/Gainers/{{ex}}/{{group}}/{{record_count}}",
    "loosers" : f"{BASE_URL}/losers/{{ex}}/{{group}}/{{record_count}}",
    "out_or_under_performers": f"{BASE_URL}/OutUnderPerformers/{{ex}}/{{group}}/{{performer}}/{{record_count}}",
    "advance_decline" : f"{BASE_URL}/AdvancesDeclines/{{ex}}",
    "52_weeks_high" : f"{BASE_URL}/FiftyTwoWeekHighEOD/{{ex}}/{{group}}/{{record_count}}",
    "52_weeks_lows" : f"{BASE_URL}/FiftyTwoWeekLowEOD/{{ex}}/{{group}}/{{record_count}}",
    "new_high_lows" : f"{BASE_URL}/NewHigh-NewLowEOD/{{group}}/{{high_or_low}}/{{period}}/{{record_count}}",
    
}

# ══════════════════════════════════════════════════════════════════════════════
mcp = FastMCP("Equifiz MF Server")
# ══════════════════════════════════════════════════════════════════════════════


# ── RESOLVER TOOLS ─────────────────────────────────────────────────────────────
# Resolvers exist ONLY to translate a human name → internal code for tool calls.
# They should NOT be shown to the end-user as answers.


################################################ schemes and funds related  tools ##############################################
@mcp.tool(description=(
    "Resolve an AMC / fund-house name to its internal mf_cocode. "
    "Call this BEFORE any AMC-level tool. Accepts brand names and aliases. "
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
        "Try the full AMC name (e.g. 'HDFC Mutual Fund', 'Parag Parikh Financial Advisory Services')."
    )

@mcp.tool(description=(
    "Resolve a mutual fund scheme name into mf_schcode. "
    "CALL THIS FIRST before tool that require mf_schcode." 
    "Read the returned mf_schcode and pass it exactly to the next tool. "
    "Never skip this step."
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


# ── AMC-LEVEL TOOLS ────────────────────────────────────────────────────────────

@mcp.tool(description="List all AMC / fund houses with scheme counts and AUM summary.")
def get_all_fund_houses() -> str:
    # Fields from API spec: lname, fund_type, osch, csch, isch, sumoftotnav, dateas
    data, err = _get(EP["fund_house"])
    if err:
        return err
    rows = _as_list(data)[:50]
    FIELDS = ["lname", "fund_type", "osch", "csch", "sumoftotnav", "dateas"]
    lines = [f"Fund Houses ({len(rows)} AMCs):", "─" * 55]
    for i, row in enumerate(rows, 1):
        p = _pick(row, FIELDS)
        lines.append(
            f"{i:>3}. {p.get('lname', 'N/A')}"
            f"  [{p.get('fund_type', '')}]"
            f"  Open: {p.get('osch', '?')}  Closed: {p.get('csch', '?')}"
            f"  AUM: {p.get('sumoftotnav', 'N/A')}"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "List fund categories (Equity/Debt/Hybrid/All) for an AMC. "
    "REQUIRES mf_cocode — call resolve_mf_fund first."
))
def get_fund_categories(mf_cocode: int, category: str = "All") -> str:
    # Fields from API spec: maincategory, vclass, vclasscode
    val, err = _require_int(mf_cocode, "mf_cocode", "resolve_mf_fund")
    if err:
        return err
    url  = EP["fund_category_amc"].format(mf_cocode=val, category=category)
    data, err = _get(url)
    if err:
        return err
    rows = _as_list(data)[:30]
    if not rows:
        return f"No categories found for the selected AMC under '{category}'."
    lines = [f"Categories ({category}):"]
    for row in rows:
        p = _pick(row, ["maincategory", "vclass"])
        lines.append(f"  • {p.get('maincategory', '')} — {p.get('vclass', '')}")
    return "\n".join(lines)


@mcp.tool(description=(
    "List all scheme names offered by an AMC with their current NAV. "
    "REQUIRES mf_cocode — call resolve_mf_fund first."
))
def get_schemes_by_amc(mf_cocode: int) -> str:
    # Fields from API spec: sch_name, navrs, category, amficode
    val, err = _require_int(mf_cocode, "mf_cocode", "resolve_mf_fund")
    if err:
        return err
    url  = EP["scheme_master"].format(mf_cocode=val)
    data, err = _get(url)
    if err:
        return err
    rows = _as_list(data)
    if not rows:
        return "No schemes found for the selected AMC."
    lines = [f"Schemes ({len(rows)} total):"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, ["sch_name", "category", "navrs"])
        lines.append(
            f"  {i:>3}. {p.get('sch_name', 'N/A')}"
            f"  [{p.get('category', '')}]"
            f"  NAV: ₹{p.get('navrs', 'N/A')}"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get fund profile / synopsis for an AMC: categories offered and class names. "
    "REQUIRES mf_cocode — call resolve_mf_fund first."
))
def get_fund_profile(mf_cocode: int) -> str:
    # Fields from API spec: VCLASS, VCLASSCODE
    val, err = _require_int(mf_cocode, "mf_cocode", "resolve_mf_fund")
    if err:
        return err
    url  = EP["fund_profile"].format(mf_cocode=val)
    data, err = _get(url)
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


@mcp.tool(description="List all fund managers across AMCs with the schemes they manage and since when.")
def get_fund_managers() -> str:
    # Fields from API spec: fund_mgr, mf_schcode (hidden), SinceDate
    # We show manager name + scheme (resolved from cache) + tenure
    data, err = _get(EP["fund_manager"])
    if err:
        return err
    rows = _as_list(data)[:30]
    if not rows:
        return "No fund manager data available."

    # Build a quick schcode→name lookup from cache
    sch_lookup = {s["mf_schcode"]: s["sch_name"] for s in _load_mf_scheme_cache()}

    lines = [f"Fund Managers ({len(rows)} records):"]
    for row in rows:
        mgr   = row.get("fund_mgr") or row.get("FundMgr") or "N/A"
        since = row.get("SinceDate") or row.get("sincedate") or ""
        scode = row.get("mf_schcode") or row.get("MF_SCHCODE")
        sname = sch_lookup.get(scode, "N/A") if scode else "N/A"
        lines.append(f"  • {mgr}  |  Scheme: {sname}  |  Since: {since}")
    return "\n".join(lines)


# ── SCHEME-LEVEL TOOLS ─────────────────────────────────────────────────────────

@mcp.tool(description=(
    "Get today's NAV for a scheme: current NAV, previous NAV, change, % change. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
))
def get_scheme_nav(mf_schcode: int) -> str:
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err

    data, err = _get(EP["daily_nav"])
    if err:
        return err

    records = data.get("data", []) if isinstance(data, dict) else []
    if not records:
        return "No NAV data available."

    # API returns mf_schcode as float (e.g. 770.0), so cast before comparing
    match = next(
        (r for r in records if int(float(r.get("mf_schcode", -1))) == val),
        None,
    )
    if not match:
        return f"NAV data not available for mf_schcode={val} today."

    return (
        f"Today's NAV — {match.get('mf_schname', 'N/A')}\n"
        f"  Date       : {match.get('navdate', 'N/A')}\n"
        f"  NAV        : ₹{match.get('nav', 'N/A')}\n"
        f"  Prev NAV   : ₹{match.get('prevnav', 'N/A')}\n"
        f"  Change     : {match.get('navchng', 'N/A')}  ({match.get('navperchng', 'N/A')}%)"
)


@mcp.tool(description=(
    "Get investment details: minimum investment, scheme objective, tax treatment. "
    "REQUIRES mf_schcode from resolve_mf_scheme. "
    "Do NOT call this tool without first calling resolve_mf_scheme."
    "Never call this with a guessed or example schcode like 123456."
))

def get_investment_details(mf_schcode: int) -> str:
    # Fields: inc_invest, mininvt, Objective, taxbname
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    url  = EP["investment_details"].format(mf_schcode=val)
    data, err = _get(url)
    if err:
        return err
    rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
    if not rows:
        return "No investment detail data found."
    p = _pick(rows[0], ["inc_invest", "mininvt", "Objective", "taxbname", "TAXB"])
    return (
        f"Investment Details:\n"
        f"  Min Investment      : ₹{p.get('mininvt', 'N/A')}\n"
        f"  Inception Investment: ₹{p.get('inc_invest', 'N/A')}\n"
        f"  Tax Treatment       : {p.get('taxbname') or p.get('TAXB', 'N/A')}\n"
        f"  Objective           : {p.get('Objective', 'N/A')}"
    )



@mcp.tool(description=(
    "Get expense ratio, entry/exit loads for a scheme. "
    "You MUST call resolve_mf_scheme first to get the mf_schcode. "
    "Do NOT call this tool without first calling resolve_mf_scheme. "
    "Never guess or invent an mf_schcode."
))

def get_expense_ratio(mf_schcode: int) -> str:
    # Fields: EXPRATIO, entry, exit (from SchemeProfileExpRatio / ExpenseRatios)
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    data, err = _get(EP["expense_ratio"])
    if err:
        return err
    rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
    match = next(
        (r for r in rows if int(float(r.get("mf_schcode", -1))) == val),
        None,
    )
    if not match:
        return "Expense ratio data not found for this scheme."
    p = _pick(match, ["EXPRATIO", "entry", "exit", "mininvt", "MaturityPeriod"])
    return (
        f"Expense & Load Details:\n"
        f"  Expense Ratio  : {p.get('EXPRATIO', 'N/A')}%\n"
        f"  Entry Load     : {p.get('entry', 'N/A')}\n"
        f"  Exit Load      : {p.get('exit', 'N/A')}\n"
        f"  Min Investment : ₹{p.get('mininvt', 'N/A')}"
    )



@mcp.tool(description=(
    "Get average maturity, modified duration, YTM for DEBT fund schemes. "
    "You MUST call resolve_mf_scheme first to get the mf_schcode. "
    "Do NOT call this tool without first calling resolve_mf_scheme. "
    "Never guess or invent an mf_schcode."
    "CRITICAL: This tool requires a validated 'mf_schcode'. "
    "You are FORBIDDEN from inventing this code. "
    "Workflow: 1. Call resolve_mf_scheme(name='...') 2. Use the returned 'mf_schcode' here."
))
def get_avg_maturity(mf_schcode: int) -> str:
    # Fields: avg_maturity, ModDuration, YTM, AvgMaturityDate, MacaulayDuration
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    data, err = _get(EP["avg_maturity"])
    if err:
        return err
    rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)

    match = next(
        (r for r in rows if int(float(r.get("mf_schcode", -1))) == val),
        None,
    )

    if not match:
        return "Average maturity data not found for this scheme."
    p = _pick(match, ["avg_maturity", "ModDuration", "MacaulayDuration", "YTM", "AvgMaturityDate"])

    return (
        f"Debt Fund Metrics:\n"
        f"  Avg Maturity      : {p.get('avg_maturity', 'N/A')}\n"
        f"  Modified Duration : {p.get('ModDuration', 'N/A')}\n"
        f"  Macaulay Duration : {p.get('MacaulayDuration', 'N/A')}\n"
        f"  YTM               : {p.get('YTM', 'N/A')}%\n"
        f"  As of Date        : {p.get('AvgMaturityDate', 'N/A')}"
    )


@mcp.tool(description=(
    "Get historical AUM (Assets Under Management) for a scheme. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
))
def get_scheme_aum(mf_schcode: int) -> str:
    # Fields: AUMDate, AUM
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    data, err = _get(EP["scheme_aum"])
    if err:
        return err
    rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
    matched = next(
        (r for r in rows if int(float(r.get("mf_schcode", -1))) == val),
        None,
    )
    if not matched:
        return "AUM data not found for this scheme."
    matched = matched[:24]  # up to 24 months
    lines = ["Historical AUM:"]
    for row in matched:
        p = _pick(row, ["AUMDate", "AUM"])
        lines.append(f"  {p.get('AUMDate', 'N/A')}: ₹{p.get('AUM', 'N/A')} Cr")
    return "\n".join(lines)


@mcp.tool(description=(
    "Get historical NAV for a scheme. "
    "period: 'M' (months) or 'Y' (years). periodval: number of periods. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
))
def get_nav_historical(mf_schcode: int, period: str = "Y", periodval: int = 1) -> str:
    # Fields: NavDate, NAVRS, adjnavrs
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    p  = _normalise_period(period)
    pv = max(1, int(periodval) if periodval else 1)
    url  = EP["nav_historical"].format(mf_schcode=val, period=p, periodval=pv)
    data, err = _get(url)
    if err:
        return err
    rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
    if not rows:
        return "No historical NAV data found."
    lines = [f"Historical NAV (last {pv}{'Y' if p == 'Y' else 'M'}):"]
    for row in rows:
        pr = _pick(row, ["NavDate", "NAVRS", "adjnavrs"])
        lines.append(f"  {pr.get('NavDate', 'N/A')}: ₹{pr.get('NAVRS', 'N/A')}")
    return "\n".join(lines)


@mcp.tool(description=(
    "Get scheme returns across 1W/1M/3M/6M/1Y/3Y/5Y/Since Inception with benchmark comparison. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
))
def get_scheme_returns(mf_schcode: int) -> str:
    # Fields: sch_name, Date, 1week, 1Month, 3Month, 6Month, 1Year, 3Year, 5Year, Inception
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    url  = EP["scheme_returns"].format(mf_schcode=val)
    data, err = _get(url)
    if err:
        return err
    rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
    if not rows:
        return "No returns data found."
    row = rows[0]
    p = _pick(row, ["sch_name", "Date", "1week", "1Month", "3Month", "6Month",
                    "1Year", "3Year", "5Year", "Inception"])
    return (
        f"Returns — {p.get('sch_name', 'N/A')}  (as of {p.get('Date', 'N/A')})\n"
        f"  1 Week   : {p.get('1week', 'N/A')}%\n"
        f"  1 Month  : {p.get('1Month', 'N/A')}%\n"
        f"  3 Months : {p.get('3Month', 'N/A')}%\n"
        f"  6 Months : {p.get('6Month', 'N/A')}%\n"
        f"  1 Year   : {p.get('1Year', 'N/A')}%\n"
        f"  3 Years  : {p.get('3Year', 'N/A')}%\n"
        f"  5 Years  : {p.get('5Year', 'N/A')}%\n"
        f"  Inception: {p.get('Inception', 'N/A')}%"
    )


@mcp.tool(description=(
    "Get lumpsum return details: 'If I invested ₹X N years ago, what is it worth now?' "
    "Returns value and % return for 1W/1M/3M/6M/1Y/3Y/5Y/10Y/Since Inception. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
))
def get_lumpsum_returns(mf_schcode: int) -> str:
    # Fields: InvAmount, W1LatestValue, W1Return_Abs, M1LatestValue, M1Return_Ann,
    #         M3/M6/Y1/Y3/Y5/Y10 variants, InceptionLatestValue, InceptionReturn_Ann
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    url  = EP["lumpsum_return"].format(mf_schcode=val)
    data, err = _get(url)
    if err:
        return err
    rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
    if not rows:
        return "No lumpsum return data found."
    r = rows[0]  # single record per scheme

    def _rv(key): return r.get(key, "N/A")

    return (
        f"Lumpsum Returns (Investment: ₹{_rv('InvAmount')}):\n"
        f"  1 Week    : ₹{_rv('W1LatestValue')}  ({_rv('W1Return_Abs')}% abs)\n"
        f"  1 Month   : ₹{_rv('M1LatestValue')}  ({_rv('M1Return_Ann')}% ann)\n"
        f"  3 Months  : ₹{_rv('M3LatestValue')}  ({_rv('M3Return_Ann')}% ann)\n"
        f"  6 Months  : ₹{_rv('M6LatestValue')}  ({_rv('M6Return_Ann')}% ann)\n"
        f"  1 Year    : ₹{_rv('Y1LatestValue')}  ({_rv('Y1Return_Ann')}% ann)\n"
        f"  3 Years   : ₹{_rv('Y3LatestValue')}  ({_rv('Y3Return_Ann')}% ann)\n"
        f"  5 Years   : ₹{_rv('Y5LatestValue')}  ({_rv('Y5Return_Ann')}% ann)\n"
        f"  10 Years  : ₹{_rv('Y10LatestValue')}  ({_rv('Y10Return_Ann')}% ann)\n"
        f"  Inception : ₹{_rv('InceptionLatestValue')}  ({_rv('InceptionReturn_Ann')}% ann)"
    )


@mcp.tool(description=(
    "Compare multiple schemes side-by-side on returns, NAV, AUM, fund manager, exit load. "
    "REQUIRES comma-separated mf_schcodes. Use resolve_mf_scheme to obtain each code."
))
def compare_schemes(mf_schcodes: str) -> str:
    # Fields: Sch_Name, VClass, NAVRS, NavDate, SchemeAssets, FundManager,
    #         1WEEK..5YEAR, INCEPTION, ExitLoad
    if not mf_schcodes or not mf_schcodes.strip():
        return "Provide a comma-separated list of scheme codes (e.g. '19283,204,17152')."
    url  = EP["scheme_comparison"].format(schcodes=mf_schcodes.strip())
    data, err = _get(url)
    if err:
        return err
    rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
    if not rows:
        return "No comparison data found."
    FIELDS = ["Sch_Name", "VClass", "NAVRS", "NavDate", "SchemeAssets",
              "FundManager", "1WEEK", "1MONTH", "3MONTH", "6MONTH",
              "1YEAR", "3YEAR", "5YEAR", "INCEPTION", "ExitLoad"]
    lines = [f"Scheme Comparison ({len(rows)} schemes):"]
    for row in rows:
        p = _pick(row, FIELDS)
        lines.append(f"\n  ── {p.get('Sch_Name', 'N/A')} [{p.get('VClass', '')}] ──")
        lines.append(f"     NAV        : ₹{p.get('NAVRS', 'N/A')}  (as of {p.get('NavDate', 'N/A')})")
        lines.append(f"     AUM        : ₹{p.get('SchemeAssets', 'N/A')} Cr")
        lines.append(f"     Manager    : {p.get('FundManager', 'N/A')}")
        lines.append(f"     Returns    : 1W {p.get('1WEEK','N/A')}% | 1M {p.get('1MONTH','N/A')}% | "
                     f"1Y {p.get('1YEAR','N/A')}% | 3Y {p.get('3YEAR','N/A')}% | "
                     f"5Y {p.get('5YEAR','N/A')}% | Since Inception {p.get('INCEPTION','N/A')}%")
        lines.append(f"     Exit Load  : {p.get('ExitLoad', 'N/A')}")
    return "\n".join(lines)


@mcp.tool(description=(
    "Get top-performing funds by type and category. "
    "fund_type: 'Equity' (default) / 'Debt' / 'Hybrid'. "
    "category: 'all' (default) or specific category name."
))
def get_fund_performance(top: int = 10, fund_type: str = "Equity", category: str = "all") -> str:
    # Fields: schemename, TypeName, NavRs, NavDate (from FundPerformance API)
    ft = fund_type.strip().title()
    if ft not in {"Equity", "Debt", "Hybrid"}:
        return f"fund_type must be Equity, Debt, or Hybrid — got '{fund_type}'."
    url  = EP["fund_performance"].format(top=top, type=ft, category=category)
    data, err = _get(url)
    if err:
        return err
    rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
    if not rows:
        return "No performance data found."
    lines = [f"Top {len(rows)} {ft} Funds ({category}):"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, ["schemename", "Scheme", "TypeName", "NavRs", "NavDate"])
        name = p.get("schemename") or p.get("Scheme", "N/A")
        lines.append(f"  {i:>3}. {name}  [{p.get('TypeName', '')}]  NAV: ₹{p.get('NavRs', 'N/A')}")
    return "\n".join(lines)


@mcp.tool(description="Get average category returns for Equity/Debt/Hybrid across time periods.")
def get_category_performance(fund_type: str = "Equity", top: int = 20) -> str:
    # Fields: typename, ret1w, ret1m, ret3m, ret6m, ret1y, ret3y, ret5y, ret10y, retinception
    ft = fund_type.strip().title()
    url  = EP["category_performance"].format(type=ft, top=top)
    data, err = _get(url)
    if err:
        return err
    rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
    if not rows:
        return "No category performance data found."
    RFIELDS = ["typename", "ret1w", "ret1m", "ret3m", "ret6m",
               "ret1y", "ret3y", "ret5y", "ret10y", "retinception", "categoryreturndate"]
    lines = [f"Category Returns — {ft}:"]
    for row in rows:
        p = _pick(row, RFIELDS)
        lines.append(
            f"  {p.get('typename', 'N/A'):<35}"
            f"  1Y: {p.get('ret1y','N/A')}%"
            f"  3Y: {p.get('ret3y','N/A')}%"
            f"  5Y: {p.get('ret5y','N/A')}%"
        )
    return "\n".join(lines)


@mcp.tool(description="Get SIP/SWP/STP available dates and minimum amounts. plan: 'SIP' / 'SWP' / 'STP'.")
def get_sip_dates(plan: str = "SIP") -> str:
    # Fields: plan, frequency, d1, d2, d3 (mf_schcode hidden)
    p = plan.strip().upper()
    if p not in ("SIP", "SWP", "STP"):
        return f"plan must be 'SIP', 'SWP', or 'STP' — got '{plan}'."
    url  = EP["sip_dates"].format(plan=p)
    data, err = _get(url)
    if err:
        return err
    rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
    if not rows:
        return f"No {p} date data found."
    lines = [f"{p} Available Dates:"]
    for row in rows[:10]:  # show a sample
        pr = _pick(row, ["frequency", "d1", "d2", "d3"])
        dates = ", ".join(str(pr[d]) for d in ["d1","d2","d3"] if pr.get(d))
        lines.append(f"  {pr.get('frequency', 'N/A')}: {dates}")
    return "\n".join(lines)


@mcp.tool(description=(
    "Get SIP/SWP dates and minimum amounts for a specific scheme. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
))
def get_scheme_sip_details(mf_schcode: int) -> str:
    # Fields: SIPDates, minamt, multamt, avail_period
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    url  = EP["scheme_sip_swp"].format(mf_schcode=val)
    data, err = _get(url)
    if err:
        return err
    rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
    if not rows:
        return "No SIP/SWP data found for this scheme."
    lines = ["SIP / SWP Details:"]
    for row in rows:
        p = _pick(row, ["SIPDates", "minamt", "multamt", "avail_period"])
        lines.append(
            f"  Dates: {p.get('SIPDates', 'N/A')}"
            f"  |  Min: ₹{p.get('minamt', 'N/A')}"
            f"  |  Multiples: ₹{p.get('multamt', 'N/A')}"
            f"  |  Period: {p.get('avail_period', 'N/A')}"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get top stock holdings of a fund with % holding, market value, sector. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
))
def get_mf_holdings(mf_schcode: int) -> str:
    # Fields: co_name, perc_hold, mktvalue, no_shares, iind_name, type, rating
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    url  = EP["mf_holding"].format(mf_schcode=val)
    data, err = _get(url)
    if err:
        return err
    rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
    if not rows:
        return "No holdings data found."
    lines = [f"Top Holdings ({len(rows)} stocks):"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, ["co_name", "perc_hold", "mktvalue", "no_shares", "iind_name", "type", "rating"])
        lines.append(
            f"  {i:>3}. {p.get('co_name', 'N/A')}"
            f"  [{p.get('iind_name', p.get('type', ''))}]"
            f"  {p.get('perc_hold', 'N/A')}%"
            f"  ₹{p.get('mktvalue', 'N/A')} Cr"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get all mutual funds that hold a specific stock. "
    "REQUIRES co_code — use the equifiz stock server's resolve_nse_symbol first."
))
def get_funds_holding_company(co_code: int, top: int = 10) -> str:
    # Fields: sch_name, invdate, mktvalue, perc_hold, totnav, no_shares
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol (stock server)")
    if err:
        return err
    url  = EP["company_mf_holding"].format(co_code=val, top=top)
    data, err = _get(url)
    if err:
        return err
    rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
    if not rows:
        return "No mutual funds found holding this stock."
    lines = [f"Funds holding this stock ({len(rows)}):"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, ["sch_name", "invdate", "mktvalue", "perc_hold", "no_shares"])
        lines.append(
            f"  {i:>3}. {p.get('sch_name', 'N/A')}"
            f"  |  {p.get('perc_hold', 'N/A')}% of portfolio"
            f"  |  ₹{p.get('mktvalue', 'N/A')} Cr"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get sector allocation (IT/Banking/Pharma %) for a fund. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
))
def get_sector_allocation(mf_schcode: int) -> str:
    # Fields: sector, perc_hold, value, secdate, oldperc_hold
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    url  = EP["sector_allocation"].format(mf_schcode=val)
    data, err = _get(url)
    if err:
        return err
    rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
    if not rows:
        return "No sector allocation data found."
    lines = [f"Sector Allocation (as of {rows[0].get('secdate', 'N/A')}):"]
    for row in rows:
        p = _pick(row, ["sector", "perc_hold", "value"])
        lines.append(f"  {p.get('sector', 'N/A'):<35}: {p.get('perc_hold', 'N/A')}%")
    return "\n".join(lines)


@mcp.tool(description=(
    "Get asset allocation (equity/debt/cash %) for a fund. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
))
def get_asset_allocation(mf_schcode: int) -> str:
    # Fields: assetname, holding_currentmonth, holding_prevmonth, currentmonth
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    url  = EP["asset_allocation"].format(mf_schcode=val)
    data, err = _get(url)
    if err:
        return err
    rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
    if not rows:
        return "No asset allocation data found."
    as_of = rows[0].get("currentmonth", "N/A")
    lines = [f"Asset Allocation (as of {as_of}):"]
    for row in rows:
        p = _pick(row, ["assetname", "holding_currentmonth", "holding_prevmonth"])
        lines.append(
            f"  {p.get('assetname', 'N/A'):<20}"
            f": {p.get('holding_currentmonth', 'N/A')}%"
            f"  (prev: {p.get('holding_prevmonth', 'N/A')}%)"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get stocks recently added to / removed from a fund. "
    "move_type: 'in' (new buys) / 'out' (sold) / 'un' (unchanged). "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
))
def get_portfolio_changes(mf_schcode: int, move_type: str = "in") -> str:
    # Fields: CO_NAME, Perc_Hold, mktvalue, NO_SHARES, PortfolioDate, AssetType
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    t = move_type.lower()
    if t not in ("in", "out", "un"):
        return "move_type must be 'in', 'out', or 'un'."
    url   = EP["whats_in_out"].format(type=t, mf_schcode=val)
    data, err = _get(url)
    if err:
        return err
    rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
    if not rows:
        labels = {"in": "new buys", "out": "sold holdings", "un": "unchanged positions"}
        return f"No {labels[t]} found for this fund."
    label = {"in": "New Buys", "out": "Sold Positions", "un": "Unchanged"}[t]
    lines = [f"{label} ({len(rows)} stocks):"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, ["CO_NAME", "Perc_Hold", "mktvalue", "NO_SHARES", "AssetType"])
        lines.append(
            f"  {i:>3}. {p.get('CO_NAME', 'N/A')}"
            f"  [{p.get('AssetType', '')}]"
            f"  {p.get('Perc_Hold', 'N/A')}%"
        )
    return "\n".join(lines)



@mcp.tool(description=(
    "Get large/mid/small-cap allocation % for an equity fund. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
))
def get_mcap_allocation(mf_schcode: int) -> str:
    # Fields: mcaptype, MarketValue, perc_hold, CurrPFDate
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    url  = EP["mcap_equity"].format(mf_schcode=val)
    data, err = _get(url)
    if err:
        return err
    rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
    if not rows:
        return "No market cap allocation data found."
    as_of = rows[0].get("CurrPFDate", "N/A")
    lines = [f"Market Cap Allocation (as of {as_of}):"]
    for row in rows:
        p = _pick(row, ["mcaptype", "perc_hold", "MarketValue"])
        lines.append(f"  {p.get('mcaptype', 'N/A'):<15}: {p.get('perc_hold', 'N/A')}%")
    return "\n".join(lines)


@mcp.tool(description=(
    "Get stocks most bought or sold by a fund this month. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
))
def get_most_bought_sold(mf_schcode: int) -> str:
    # Fields: CO_NAME, currmktvalue, prevmktvalue, diff, chng, CurrInvdate
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    url  = EP["most_sold_bought"].format(mf_schcode=val)
    data, err = _get(url)
    if err:
        return err
    rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
    if not rows:
        return "No buy/sell activity data found."
    lines = ["Most Bought / Sold This Month:"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, ["CO_NAME", "currmktvalue", "prevmktvalue", "diff", "chng"])
        chng = float(p.get("chng", 0) or 0)
        direction = "▲ BUY" if chng >= 0 else "▼ SELL"
        lines.append(
            f"  {i:>3}. {direction}  {p.get('CO_NAME', 'N/A')}"
            f"  |  Change: ₹{p.get('diff', 'N/A')} Cr ({chng:+.2f}%)"
        )
    return "\n".join(lines)


@mcp.tool(description="Get currently open New Fund Offers (NFO) with objective, min investment, close date.")
def get_new_fund_offers() -> str:
    # Fields: sch_name, lname, launc_date, cldate, mininvt, schemetype
    data, err = _get(EP["new_fund_offer"])
    if err:
        return err
    rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
    if not rows:
        return "No open NFOs at this time."
    lines = [f"Open NFOs ({len(rows)}):"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, ["sch_name", "lname", "launc_date", "cldate", "mininvt", "schemetype"])
        lines.append(
            f"  {i:>3}. {p.get('sch_name', 'N/A')}"
            f"  by {p.get('lname', 'N/A')}"
            f"  [{p.get('schemetype', '')}]"
            f"  Opens: {p.get('launc_date', 'N/A')}"
            f"  Closes: {p.get('cldate', 'N/A')}"
            f"  Min: ₹{p.get('mininvt', 'N/A')}"
        )
    return "\n".join(lines)


@mcp.tool(description="Get overall MF market activity: gross equity/debt purchases and net flows.")
def get_mf_market_activity() -> str:
    # Fields: TRANS_DATE, EQ_GR_PURC, EQ_GR_SALE, EQ_G_NE_PS, DE_GR_PURC, DE_GR_SALE
    data, err = _get(EP["mf_activities"])
    if err:
        return err
    rows =_as_list(data.get("data", []) if isinstance(data, dict) else data)  # ~12 months
    if not rows:
        return "No market activity data available."
    FIELDS = ["TRANS_DATE", "EQ_GR_PURC", "EQ_GR_SALE", "EQ_G_NE_PS", "DE_GR_PURC", "DE_GR_SALE"]
    lines = ["MF Market Activity (₹ Cr):"]
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
    "Get risk ratios: Beta, Alpha, Sharpe, Std Dev (SD), Treynor for a scheme. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
))
def get_scheme_ratios(mf_schcode: int) -> str:
    # Fields: Scheme_Nam, DATE, BETA, SD, TREYNOR, ALPHA, SHARPE (if present)
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    data, err = _get(EP["scheme_ratios"])
    if err:
        return err
    rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
    match = next(
        (r for r in rows if str(r.get("MF_SCHCODE") or r.get("Mf_SchCode") or "") == str(val)),
        None,
    )
    if not match:
        return "Risk ratio data not found for this scheme."
    p = _pick(match, ["Scheme_Nam", "DATE", "BETA", "SD", "TREYNOR", "ALPHA", "SHARPE"])
    return (
        f"Risk Ratios — {p.get('Scheme_Nam', 'N/A')}  (as of {p.get('DATE', 'N/A')})\n"
        f"  Beta    : {p.get('BETA', 'N/A')}\n"
        f"  Std Dev : {p.get('SD', 'N/A')}\n"
        f"  Treynor : {p.get('TREYNOR', 'N/A')}\n"
        f"  Alpha   : {p.get('ALPHA', 'N/A')}\n"
        f"  Sharpe  : {p.get('SHARPE', 'N/A')}"
    )


@mcp.tool(description=(
    "Get recent dividend announcements for a scheme. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
))
def get_dividend_details(mf_schcode: int) -> str:
    # Fields: sch_name, LNAME, DivPer, DivAmount, DIVPERPU, Divtype, DivDate, RecordDate
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    url  = EP["dividend_details"].format(mf_schcode=val)
    data, err = _get(url)
    if err:
        return err
    rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
    if not rows:
        return "No dividend announcements found for this scheme."
    lines = ["Dividend History:"]
    for row in rows:
        p = _pick(row, ["sch_name", "DivAmount", "DIVPERPU", "Divtype", "DivDate", "RecordDate"])
        lines.append(
            f"  {p.get('DivDate', 'N/A')}"
            f"  ₹{p.get('DivAmount', 'N/A')} per unit"
            f"  [{p.get('Divtype', '')}]"
            f"  Record Date: {p.get('RecordDate', 'N/A')}"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get latest MF industry news. "
    "sno: '-' for latest batch, or a serial number for a specific article."
))
def get_mf_news(sno: str = "-") -> str:
    # Fields: date, heading, caption, arttext, section_name
    url  = EP["mf_news"].format(sno=sno.strip() or "-")
    data, err = _get(url)
    if err:
        return err
    rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
    if not rows:
        return "No news available."
    lines = ["MF News:"]
    for row in rows:
        p = _pick(row, ["date", "heading", "caption", "section_name"])
        lines.append(f"\n  [{p.get('date', 'N/A')}]  {p.get('heading', 'N/A')}")
        if p.get("caption"):
            lines.append(f"  {p['caption']}")
    return "\n".join(lines)


@mcp.tool(description=(
    "Get BSE Star platform transaction details for a scheme: modes, SIP/SWP flags, settlement. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
))
def get_bse_star_scheme(mf_schcode: int) -> str:
    # Fields: SCH_NAME, Scheme_Type, SIP_FLAG, STP_FLAG, SWP_FLAG, Settlement_Type, ISIN
    val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
    if err:
        return err
    url  = EP["bse_star_scheme"].format(mf_schcode=val)
    data, err = _get(url)
    if err:
        return err
    rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
    if not rows:
        return "No BSE Star data found for this scheme."
    p = _pick(rows[0], ["SCH_NAME", "Scheme_Type", "SIP_FLAG", "STP_FLAG", "SWP_FLAG",
                        "Settlement_Type", "ISIN", "Dividend_Reinvestment_Flag"])
    return (
        f"BSE Star Details — {p.get('SCH_NAME', 'N/A')}\n"
        f"  Scheme Type         : {p.get('Scheme_Type', 'N/A')}\n"
        f"  SIP Available       : {'Yes' if p.get('SIP_FLAG') == 1 else 'No'}\n"
        f"  STP Available       : {'Yes' if p.get('STP_FLAG') == 1 else 'No'}\n"
        f"  SWP Available       : {'Yes' if p.get('SWP_FLAG') == 1 else 'No'}\n"
        f"  Settlement Type     : {p.get('Settlement_Type', 'N/A')}\n"
        f"  ISIN                : {p.get('ISIN', 'N/A')}"
    )


@mcp.tool(description="Get AMFI code mapping for a scheme (AMFI code, ISIN). Pass mf_schcode to filter.")
def get_amfi_master(mf_schcode: Optional[int] = None) -> str:
    # Fields: mf_schcode (filter only), growth_payoutisin, reinvestmentisin, amficode
    data, err = _get(EP["amfi_master"])
    if err:
        return err
    rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
    if mf_schcode:
        rows = [r for r in rows if str(r.get("mf_schcode") or "") == str(mf_schcode)]
    rows = rows[:20]
    if not rows:
        return "No AMFI data found."
    lines = ["AMFI Mapping:"]
    for row in rows:
        p = _pick(row, ["amficode", "growth_payoutisin", "reinvestmentisin"])
        lines.append(
            f"  AMFI Code : {p.get('amficode', 'N/A')}"
            f"  |  Growth ISIN: {p.get('growth_payoutisin', 'N/A')}"
            f"  |  Reinvest ISIN: {p.get('reinvestmentisin', 'N/A')}"
        )
    return "\n".join(lines)

#########################################################################################################################################

################################################company and stocks related tools ######################################################33

@mcp.tool(description=(
    "Resolve any company name, brand, or ticker into NSE symbol + co_code. "
    "CALL THIS FIRST before any financial-ratio tool. "
    "Accepts tickers (TCS), brand names (jio, dmart), or natural phrases."
))
def resolve_nse_symbol(query: str) -> str:
    cleaned   = _clean_query(query)
    canonical = BRAND_MAP.get(cleaned.lower().strip(), cleaned).strip()

    # Exact NSE symbol match
    if canonical.isupper() and len(canonical) <= 12 and " " not in canonical:
        row = _by_symbol(canonical)
        if row:
            return "RESOLVED (exact)\n" + _fmt_company(row)

    # High-confidence fuzzy
    hits = _fuzzy(canonical, limit=1, threshold=FUZZY_THRESHOLD)
    if hits:
        return "RESOLVED\n" + _fmt_company(hits[0])

    # Relaxed fuzzy
    hits = _fuzzy(canonical, limit=1, threshold=70)
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
        row = _by_code(co_code)
    if row is None:
        return f"NOT FOUND. Try resolve_nse_symbol(query='{nse_symbol or co_code}')."

    return "COMPANY DETAILS\n" + _fmt_company(row)


@mcp.tool(description=(
    "Search companies by partial name, sector, or industry keyword (e.g. 'pharma', 'banks', 'steel'). "
    "Returns ranked matches with co_code."
))
def search_companies(query: str, limit: int = 5) -> str:
    hits = _fuzzy(query, limit=min(max(limit, 1), 10), threshold=60)
    if not hits:
        return f"No companies found for '{query}'."
    lines = [f"Results for '{query}':"]
    for i, co in enumerate(hits, 1):
        lines.append(f"  {i}. {co.get('companyname')}  |  NSE: {co.get('nsesymbol')}  |  co_code: {co.get('co_code')}  |  Sector: {co.get('sectorname')}")
    return "\n".join(lines)


@mcp.tool(description=(
    "Fetch fundamental ratios: ROE, ROA, ROCE, margins, D/E, EPS, etc. "
    "REQUIRES co_code — call resolve_nse_symbol first. "
    "report_type: 's'=standalone (default), 'c'=consolidated."
))
def get_key_financial_ratios(co_code: int, report_type: str = "s") -> str:
    t = report_type if report_type in ("s", "c") else "s"
    url = ENDPOINTS["key_ratios"].format(co_code=co_code, t=t)
    data, err = _get(url, f"KeyFinancialRatios[{co_code}]")
    if err:
        return err
    return f"Key Financial Ratios — co_code: {co_code}  [{'Standalone' if t=='s' else 'Consolidated'}]\n" + "─"*55 + "\n" + _fmt(data)


@mcp.tool(description=(
    "Fetch live/daily market ratios: PE, PB, MCAP, EPS, dividend yield, 52w high/low. "
    "REQUIRES co_code — call resolve_nse_symbol first. "
    "report_type: 's'=standalone (default), 'c'=consolidated."
))
def get_daily_ratios(co_code: int, report_type: str = "s") -> str:
    t = report_type if report_type in ("s", "c") else "s"
    url = ENDPOINTS["daily_ratios"].format(co_code=co_code, t=t)
    data, err = _get(url, f"DailyRatios[{co_code}]")
    if err:
        return err
    return f"Daily Ratios — co_code: {co_code}  [{'Standalone' if t=='s' else 'Consolidated'}]\n" + "─"*55 + "\n" + _fmt(data)


def _ipo_tool(url: str, label: str) -> str:
    data, err = _get(url, label)
    if err:
        return err
    records = data if isinstance(data, list) else data.get("data", data) if isinstance(data, dict) else []
    if not records:
        return f"No {label} found."
    n = len(records) if isinstance(records, list) else 1
    return f"{label} ({n} record{'s' if n != 1 else ''})\n" + "─"*60 + "\n" + _fmt(records)


@mcp.tool(description="Fetch upcoming IPOs (announced, not yet open). exchange='NSE'/'BSE', count=10.")
def get_forthcoming_ipos(exchange: str = "NSE", count: int = 10) -> str:
    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)
    return _ipo_tool(ENDPOINTS["forthcoming_ipo"].format(ex=ex, n=count), f"Forthcoming IPOs [{ex}]")


@mcp.tool(description="Fetch IPOs currently open for subscription. exchange='NSE'/'BSE', count=10.")
def get_open_ipo_issues(exchange: str = "NSE", count: int = 10) -> str:
    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)
    return _ipo_tool(ENDPOINTS["open_ipo"].format(ex=ex, n=count), f"Open IPOs [{ex}]")


@mcp.tool(description="Fetch recently closed IPOs (bidding ended). exchange='NSE'/'BSE', count=10.")
def get_closed_ipos(exchange: str = "NSE", count: int = 10) -> str:
    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)
    return _ipo_tool(ENDPOINTS["closed_ipo"].format(ex=ex, n=count), f"Closed IPOs [{ex}]")


@mcp.tool(description="Fetch recently listed IPOs with listing price/gains. exchange='NSE'/'BSE', count=10.")
def get_new_ipo_listings(exchange: str = "NSE", count: int = 10) -> str:
    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)
    return _ipo_tool(ENDPOINTS["new_listing"].format(ex=ex, n=count), f"New IPO Listings [{ex}]")


@mcp.tool(description="Fetch top-performing IPOs by listing gains. exchange='NSE'/'BSE', count=10.")
def get_ipo_best_performers(exchange: str = "NSE", count: int = 10) -> str:
    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)
    return _ipo_tool(ENDPOINTS["best_ipo"].format(ex=ex, n=count), f"IPO Best Performers [{ex}]")


# ══════════════════════════════════════════════════════════════════════════════
# STOCK PRICE TOOLS  (BSE-NSE Price Feed)
# ══════════════════════════════════════════════════════════════════════════════

def _price_tool(url: str, label: str, co_code: int = None, ex: str = None) -> str:
    data, err = _get(url, label)
    if err:
        return err
    if not data:
        return f"No data found for {label}."

    records = data if isinstance(data, list) else [data]
    n = len(records)
    lines = [f"{label} ({n} record{'s' if n != 1 else ''})\n" + "─" * 60]
    for r in records:
        lines.append(_fmt(r))
    return "\n".join(lines)


@mcp.tool(description=(
    "Get current stock price, open, high, low, volume for a specific company. "
    "REQUIRES co_code — call resolve_nse_symbol first. exchange: 'NSE' (default) or 'BSE'."
))
def get_company_stock_price(co_code: int, exchange: str = "NSE") -> str:
    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)
    return _price_tool(ENDPOINTS["company_quotes"].format(co_code=co_code, ex=ex), f"Stock Price [{ex}]")



@mcp.tool(description="Get live index values with key metrics.")
def get_market_indices(exchange: str = "NSE") -> str:
    import json

    raw_data = _price_tool(ENDPOINTS["indices"], "Market Indices")

    try:
        data = json.loads(raw_data) if isinstance(raw_data, str) else raw_data

        # Normalize: handle both list and {"data": [...]} shapes
        if isinstance(data, dict):
            data = data.get("data") or data.get("indices") or list(data.values())[0]

        filtered = []
        for item in data:
            # Filter by exchange if the field exists
            if exchange and item.get("EXCHANGE", item.get("exchange", "")).upper() not in ("", exchange.upper()):
                continue

            filtered.append({
                "symbol":   item.get("SYMBOL")   or item.get("symbol")   or item.get("IndexName"),
                "ltp":      item.get("LTP")       or item.get("ltp")      or item.get("Close"),
                "change":   item.get("CHANGE")    or item.get("change")   or item.get("NetChange"),
                "pct":      item.get("PER_CHANGE")   or item.get("pchange")  or item.get("PercentChange"),
                "open":     item.get("OPEN")      or item.get("open"),
                "high":     item.get("HIGH")      or item.get("high"),
                "low":      item.get("LOW")       or item.get("low"),
                "prev":     item.get("PREV_CLOSE") or item.get("prevclose") or item.get("PreviousClose"),
            })

        # Drop keys with None values to keep the payload lean
        filtered = [{k: v for k, v in idx.items() if v is not None} for idx in filtered]

        return json.dumps(filtered, indent=None)  # compact, no pretty-print

    except Exception as e:
        # Return a hard cap — never let raw data flood the LLM
        return json.dumps({"error": str(e), "raw_preview": str(raw_data)[:500]})


@mcp.tool(description=(
    "Get exchanges holidays "
    "REQUIRES cexchange: 'NSE' (default) or 'BSE'."
))
def get_exchange_holidays(exchange: str = "NSE") -> str:
    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)
    return _price_tool(ENDPOINTS["exchange_holidays"].format(ex=ex), f"Stock Price [{ex}]")


# @mcp.tool(description=(
#     "get top performer, active stock performer"
#     "REQUIRES exchange, group, record count"
# ))

# def get_active_performer(exchange: str="NSE", group: str="BANKNIFTY", record_count: int=3):
#     try:
#         ex = _normalise_exchange(exchange=exchange)
#     except ValueError as e:
#         return str(e)
#     return _price_tool(ENDPOINTS["active_performer"].format(ex=ex,group=group,record_count=record_count),f"Stock Price [{ex}]")



#########################################################################################################################################


################################group related mcp tools##################################################


##gainers
@mcp.tool(description=(
    "Get top gainers"
    "REQUIRES exchange, group (e.g. 'BANKNIFTY', 'bank nifty', 'BN'), record_count."
))
def get_top_gainers(
    exchange: str = "NSE",
    group: str = "BANKNIFTY",
    record_count: int = 3,
):
    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)

    # ── Fuzzy-resolve the group name ──────────────────────────────────────────
    resolved_group = _resolve_group(group, exchange=ex)
    if resolved_group is None:
        # Fall back to whatever the caller passed, uppercased
        resolved_group = group.strip().upper()
        logger.warning("Using raw group value (no fuzzy match): %r", resolved_group)

    return _price_tool(
        ENDPOINTS["gainers"].format(
            ex=ex,
            group=resolved_group,
            record_count=record_count,
        ),
        f"top gainers [{ex} / {resolved_group}]",
    )


##losers
@mcp.tool(description=(
    "Get top loosers"
    "REQUIRES exchange, group (e.g. 'BANKNIFTY', 'bank nifty', 'BN'), record_count."
))
def get_top_loosers(
    exchange: str = "NSE",
    group: str = "BANKNIFTY",
    record_count: int = 3,
):
    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)

    # ── Fuzzy-resolve the group name ──────────────────────────────────────────
    resolved_group = _resolve_group(group, exchange=ex)
    if resolved_group is None:
        # Fall back to whatever the caller passed, uppercased
        resolved_group = group.strip().upper()
        logger.warning("Using raw group value (no fuzzy match): %r", resolved_group)

    return _price_tool(
        ENDPOINTS["loosers"].format(
            ex=ex,
            group=resolved_group,
            record_count=record_count,
        ),
        f"top loosers [{ex} / {resolved_group}]",
    )

##out or under performer
@mcp.tool(description=(
    "Get get outstanding or under performer"
    "REQUIRES exchange, group (e.g. 'BANKNIFTY', 'bank nifty', 'BN'), record_count, performance_type (e.g. 'out' or 'under)"
))
def out_or_under_performer(
    exchange: str = "NSE",
    group: str = "BANKNIFTY",
    record_count: int = 3,
    performance_type: str = "out"
):
    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)

    # ── Fuzzy-resolve the group name ──────────────────────────────────────────
    resolved_group = _resolve_group(group, exchange=ex)
    if resolved_group is None:
        # Fall back to whatever the caller passed, uppercased
        resolved_group = group.strip().upper()
        logger.warning("Using raw group value (no fuzzy match): %r", resolved_group)

    return _price_tool(
        ENDPOINTS["out_or_under_performers"].format(
            ex=ex,
            group=resolved_group,
            record_count=record_count,
            performance_type=performance_type
        ),
        f"under or out performer[{ex} / {resolved_group}]",
    )

# 52 weeks highs
@mcp.tool(description=(
    "Get get 52 weeks high"
    "REQUIRES exchange, group (e.g. 'BANKNIFTY', 'bank nifty', 'BN'), record_count"
))
def weeks_high_52(
    exchange: str = "NSE",
    group: str = "BANKNIFTY",
    record_count: int = 3,
):
    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)

    # ── Fuzzy-resolve the group name ──────────────────────────────────────────
    resolved_group = _resolve_group(group, exchange=ex)
    if resolved_group is None:
        # Fall back to whatever the caller passed, uppercased
        resolved_group = group.strip().upper()
        logger.warning("Using raw group value (no fuzzy match): %r", resolved_group)

    return _price_tool(
        ENDPOINTS["52_weeks_high"].format(
            ex=ex,
            group=resolved_group,
            record_count=record_count,
            
        ),
        f"52 weeks high[{ex} / {resolved_group}]",
    )

#52 weeks lows

@mcp.tool(description=(
    "Get 52 weeks lows"
    "REQUIRES exchange, group (e.g. 'BANKNIFTY', 'bank nifty', 'BN'), record_count"
))
def weeks_low_52(
    exchange: str = "NSE",
    group: str = "BANKNIFTY",
    record_count: int = 3,
):
    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)

    # ── Fuzzy-resolve the group name ──────────────────────────────────────────
    resolved_group = _resolve_group(group, exchange=ex)
    if resolved_group is None:
        # Fall back to whatever the caller passed, uppercased
        resolved_group = group.strip().upper()
        logger.warning("Using raw group value (no fuzzy match): %r", resolved_group)

    return _price_tool(
        ENDPOINTS["52_weeks_lows"].format(
            ex=ex,
            group=resolved_group,
            record_count=record_count,
            
        ),
        f"52 weeks lows [{ex} / {resolved_group}]",
    )

#advance decline

@mcp.tool(description=(
    "Get advance-decline statistics (number of gaining vs losing stocks) for an exchange. "
    "Use this when the user asks about market breadth, decline, falling stocks, advance/decline ratio. "
    "REQUIRES exchange like NSE or BSE."
))
def advance_decline(exchange: str = "NSE"):
    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)

    raw = _price_tool(
        ENDPOINTS["advance_decline"].format(ex=ex),
        f"advance_decline[{ex}]",
    )

    # Parse and filter to only relevant fields
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
        
        # If it's a list of records, filter each one
        records = data if isinstance(data, list) else data.get("data", [data])
        
        filtered = []
        for rec in records:
            filtered.append({
                "indexname": rec.get("indexname"),
                "adv": rec.get("adv"),        # advancing stocks
                "dec": rec.get("dec"),        # declining stocks
                "noc": rec.get("noc"),        # no change
                "ad": rec.get("ad"),          # advance/decline ratio
            })
        
        return json.dumps(filtered, indent=2)
    
    except Exception as e:
        # Return a hard cap — never let raw data flood the LLM
        return json.dumps({"error": str(e), "raw_preview": str(raw)[:500]})
    
    
# new highs new lows


@mcp.tool(description=(
    "Get get outstanding or under performer"
    "REQUIRES exchange, group (e.g. 'BANKNIFTY', 'bank nifty', 'BN'), record_count, performance_type (e.g. 'out' or 'under)"
))
def new_highs_lows(
    exchange: str = "NSE",
    group: str = "BANKNIFTY",
    record_count: int = 3,
    performance_type: str = "out"
):
    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)

    # ── Fuzzy-resolve the group name ──────────────────────────────────────────
    resolved_group = _resolve_group(group, exchange=ex)
    if resolved_group is None:
        # Fall back to whatever the caller passed, uppercased
        resolved_group = group.strip().upper()
        logger.warning("Using raw group value (no fuzzy match): %r", resolved_group)

    return _price_tool(
        ENDPOINTS["new_high_lows"].format(
            ex=ex,
            group=resolved_group,
            record_count=record_count,
            performance_type=performance_type
        ),
        f"under or out performer[{ex} / {resolved_group}]",
    )


#########################################################################################################

# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    mcp.run()
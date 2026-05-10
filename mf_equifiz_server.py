
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







# """
# mf_equifiz_server.py — Equifiz Mutual Fund MCP Server

# Design principles:
#   - Every tool returns ONLY the fields the LLM needs to answer the user.
#   - Internal IDs (mf_cocode, mf_schcode, co_code, classcode, etc.) are
#     NEVER surfaced to the end-user answer — they are plumbing only.
#   - Resolvers (resolve_mf_fund / resolve_mf_scheme) use the local DB cache;
#     all data tools hit the REST API and filter to a tight field whitelist.
#   - No generic _mf_tool / _fmt soup — each tool knows exactly what it wants.
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

# FUZZY_THRESHOLD = int(os.getenv("FUZZY_THRESHOLD", "85"))
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
# _company_co_cache: list[dict] = []
# _group_cache: list[dict] = []



# ###################################################### scheme and fund house related function ##################################################
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


# def _require_int(value, param_name: str, resolver: str) -> tuple[int | None, str | None]:
#     if value is None:
#         return None, f"'{param_name}' is required. Call {resolver} first to get it."
#     try:
#         return int(value), None
#     except (TypeError, ValueError):
#         return None, f"'{param_name}' must be an integer, got: {value!r}"


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
# #####################################################################################################################################


# #################################################co_code and stock company related functions ###########################################

# VALID_EXCHANGES   = {"BSE", "NSE"}
# EXCHANGE_ALIASES  = {"BOMBAY": "BSE", "NATIONAL": "NSE", "B": "BSE", "N": "NSE"}

# BRAND_MAP = {
#     "maggi": "Nestle India", "jio": "Reliance Industries",
#     "swiggy": "Bundl Technologies", "zomato": "Zomato",
#     "paytm": "One97 Communications", "nykaa": "FSN E-Commerce Ventures",
#     "dmart": "Avenue Supermarts", "airtel": "Bharti Airtel",
#     "vi": "Vodafone Idea", "idea": "Vodafone Idea",
#     "tcs": "Tata Consultancy Services", "infy": "Infosys",
#     "ril": "Reliance Industries", "reliance": "Reliance Industries",
#     "hdfc": "HDFC Bank", "icici": "ICICI Bank", "sbi": "State Bank of India",
#     "ongc": "Oil and Natural Gas Corporation", "ntpc": "NTPC",
#     "maruti": "Maruti Suzuki India", "hero": "Hero MotoCorp",
#     "bajaj": "Bajaj Auto", "tvs": "TVS Motor Company",
#     "asian paints": "Asian Paints", "titan": "Titan Company",
#     "policybazaar": "PB Fintech", "hcl": "HCL Technologies",
# }


# def _load_company_cache() -> list[dict]:
#     global _company_co_cache
#     if _company_co_cache:
#         return _company_co_cache
#     try:
#         conn = _db()
#         with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
#             cur.execute("SELECT co_code, companyname, nsesymbol, bsecode, sectorname, industryname, isin FROM companies")
#             _company_co_cache = [dict(r) for r in cur.fetchall()]
#         conn.close()
#     except Exception as e:
#         logger.error("Cache load failed: %s", e)
#     return _company_co_cache

# def _by_symbol(sym: str) -> dict | None:
#     try:
#         conn = _db()
#         with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
#             cur.execute("SELECT * FROM companies WHERE UPPER(nsesymbol) = %s", (sym.upper().strip(),))
#             row = cur.fetchone()
#         conn.close()
#         return dict(row) if row else None
#     except Exception as e:
#         logger.error("Symbol lookup failed: %s", e)
#         return None

# def _by_code(co_code: int) -> dict | None:
#     try:
#         conn = _db()
#         with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
#             cur.execute("SELECT * FROM companies WHERE co_code = %s", (co_code,))
#             row = cur.fetchone()
#         conn.close()
#         return dict(row) if row else None
#     except Exception as e:
#         logger.error("co_code lookup failed: %s", e)
#         return None

# def _fuzzy(query: str, limit: int = 5, threshold: int = FUZZY_THRESHOLD) -> list[dict]:
#     scored = []
#     q_lower, q_upper = query.lower(), query.upper()
#     for co in _load_company_cache():
#         score = max(
#             fuzz.token_set_ratio(q_lower, (co.get("companyname") or "").lower()),
#             fuzz.ratio(q_upper, (co.get("nsesymbol") or "").upper()),
#         )
#         if score >= threshold:
#             scored.append((co, score))
#     scored.sort(key=lambda x: x[1], reverse=True)
#     return [co for co, _ in scored[:limit]]

# # ── API helper ─────────────────────────────────────────────────────────────────

# def _get(url: str, label: str) -> tuple[dict | list | None, str | None]:
#     try:
#         r = _session.get(url, timeout=15)
#         r.raise_for_status()
#         return r.json(), None
#     except requests.HTTPError as e:
#         return None, f"API error {e.response.status_code} for {label}: {e.response.text[:200]}"
#     except Exception as e:
#         return None, f"Error fetching {label}: {e}"

# # ── Formatting ─────────────────────────────────────────────────────────────────

# def _fmt(data, indent="  ") -> str:
#     lines = []
#     if isinstance(data, dict):
#         for k, v in data.items():
#             if isinstance(v, (dict, list)):
#                 lines.append(f"{indent}{k}:")
#                 lines.append(_fmt(v, indent + "  "))
#             else:
#                 lines.append(f"{indent}{k:<30}: {v}")
#     elif isinstance(data, list):
#         for i, item in enumerate(data):
#             lines.append(f"{indent}[{i+1}]")
#             lines.append(_fmt(item, indent + "  "))
#     else:
#         lines.append(f"{indent}{data}")
#     return "\n".join(lines)

# def _fmt_company(row: dict) -> str:
#     return "\n".join([
#         f"Company    : {row.get('companyname', 'N/A')}",
#         f"NSE Symbol : {row.get('nsesymbol', 'N/A')}",
#         f"BSE Code   : {row.get('bsecode', 'N/A')}",
#         f"co_code    : {row.get('co_code', 'N/A')}",
#         f"Sector     : {row.get('sectorname', 'N/A')}",
#         f"Industry   : {row.get('industryname', 'N/A')}",
#         f"ISIN       : {row.get('isin', 'N/A')}",
#     ])

# def _normalise_exchange(exchange: str) -> str:
#     resolved = EXCHANGE_ALIASES.get(exchange.strip().upper(), exchange.strip().upper())
#     if resolved not in VALID_EXCHANGES:
#         raise ValueError(f"Invalid exchange '{exchange}'. Use 'NSE' or 'BSE'.")
#     return resolved

# def _clean_query(query: str) -> str:
#     """Strip scaffolding phrases to extract the company name."""
#     q = query.strip().rstrip("?!.")
#     patterns = [
#         r"^(what is|what are|what's|give me|show me|tell me about|find|get|check|analyse|analyze)\s+(the\s+)?",
#         r"^(pe ratio|eps|mcap|revenue|profit|share price|price|financials|details?)\s+(of|for|about)\s+",
#         r"^(details?\s+(of|about|for)\s+)",
#         r"^(how is|is|are)\s+",
#     ]
#     for p in patterns:
#         q = re.sub(p, "", q, flags=re.IGNORECASE).strip()
#     # Remove noise words
#     q = re.sub(r"\b(company|co|corp|ltd|limited|inc|stock|share|equity|nse|bse|listed|group)\b", "", q, flags=re.IGNORECASE)
#     return re.sub(r"\s{2,}", " ", q).strip() or query

# ########################################################################################################################################


# ###################################################### group master related configuration #################################################

# def _load_group_cache() -> list[dict]:
#     global _group_cache
#     if _group_cache:
#         return _group_cache
#     try:
#         conn = _db()
#         with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
#             cur.execute("SELECT group_name,exchange FROM group_master")
#             _group_cache = [dict(r) for r in cur.fetchall()]
#         conn.close()
#     except Exception as e:
#         logger.error("Group cache load failed: %s", e)
#     return _group_cache


# def _fuzzy_group(query: str, exchange: str | None = None, limit: int = 1, threshold: int = 55) -> list[dict]:
#     """
#     Resolve a natural-language group query to rows from group_master.

#     Strategy (scores are combined, highest wins):
#       1. Exact match on group_name          → score 100
#       2. token_set_ratio on group_name       → handles word-order noise
#       3. partial_ratio  on display_name      → handles verbose labels
#       4. Acronym expansion match             → "BN" → "BANKNIFTY"

#     Returns up to `limit` rows sorted by descending score.
#     """
#     scored: list[tuple[dict, int]] = []
#     q = query.strip().upper()
#     q_lower = q.lower()

#     # Build a lightweight acronym from the query (e.g. "bank nifty" → "BN")
#     query_acronym = "".join(w[0] for w in q.split() if w)

#     for grp in _load_group_cache():
#         # Optionally restrict to a specific exchange
#         if exchange and (grp.get("exchange") or "").upper() != exchange.upper():
#             continue

#         g_name    = (grp.get("group_name")    or "").upper()
#         g_display = (grp.get("display_name")  or "").lower()

#         # 1. Exact
#         if q == g_name:
#             scored.append((grp, 100))
#             continue

#         # 2. Fuzzy on group_name (handles "BANK NIFTY" → "BANKNIFTY", typos, etc.)
#         score_name = max(
#             fuzz.token_set_ratio(q_lower, g_name.lower()),
#             fuzz.ratio(q, g_name),
#             fuzz.partial_ratio(q, g_name),
#         )

#         # 3. Fuzzy on display_name / human label
#         score_display = fuzz.token_set_ratio(q_lower, g_display)

#         # 4. Acronym expansion:  user types "BN" and group is "BANKNIFTY"
#         #    Build the group's own acronym and compare
#         g_acronym = "".join(w[0] for w in g_name.split() if w)  # usually single-word → itself
#         # For multi-word display names like "Bank Nifty":
#         g_display_acronym = "".join(w[0].upper() for w in g_display.split() if w)

#         score_acronym = max(
#             fuzz.ratio(query_acronym, g_acronym),
#             fuzz.ratio(query_acronym, g_display_acronym),
#             # also try if the whole query IS a prefix of the group name
#             100 if g_name.startswith(q) and len(q) >= 3 else 0,
#         )

#         final_score = max(score_name, score_display, score_acronym)

#         if final_score >= threshold:
#             scored.append((grp, final_score))

#     scored.sort(key=lambda x: x[1], reverse=True)
#     return [grp for grp, _ in scored[:limit]]


# def _resolve_group(query: str, exchange: str | None = None) -> str | None:
#     """
#     Public helper: return the canonical group_name string or None.
#     Logs a warning when the match score is mediocre (helpful for debugging).
#     """
#     results = _fuzzy_group(query, exchange=exchange, limit=1)
#     if not results:
#         logger.warning("Could not resolve group from query: %r", query)
#         return None
#     resolved = results[0]["group_name"]
#     logger.debug("Resolved group %r → %r", query, resolved)
#     return resolved


# ###########################################################################################################################################3


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

# ENDPOINTS = {
#     "key_ratios":       f"{BASE_URL}/KeyFinancialRatios/{{co_code}}/{{t}}",
#     "daily_ratios":     f"{BASE_URL}/DailyRatios/{{co_code}}/{{t}}",
#     "forthcoming_ipo":  f"{BASE_URL}/forthcomingipo/{{ex}}/Ipo/{{n}}",
#     "open_ipo":         f"{BASE_URL}/OpenIssues/{{ex}}/Ipo/{{n}}",
#     "closed_ipo":       f"{BASE_URL}/ClosedIssues/{{ex}}/IPO/{{n}}",
#     "new_listing":      f"{BASE_URL}/Newlisting/{{ex}}/{{n}}",
#     "best_ipo":         f"{BASE_URL}/BestPerformerIpo/{{ex}}/{{n}}",
#     # ── Price feed ──────────────────────────────────────────────────────────
#     "delayed_prices":   f"{BASE_URL}/BseNseDelayedPriceData/{{ex}}",
#     "company_quotes":   f"{BASE_URL}/GetQuotes/{{co_code}}/{{ex}}",
#     "indices":          f"{BASE_URL}/Indices",
#     "exchange_holidays": f"{BASE_URL}/ExchangeHolidays/{{ex}}",
#     "active_performer" : f"{BASE_URL}/MostActiveToppers/{{ex}}/{{group}}/value/{{record_count}}",
#     "gainers" : f"{BASE_URL}/Gainers/{{ex}}/{{group}}/{{record_count}}",
#     "loosers" : f"{BASE_URL}/losers/{{ex}}/{{group}}/{{record_count}}",
#     "out_or_under_performers": f"{BASE_URL}/OutUnderPerformers/{{ex}}/{{group}}/{{performer}}/{{record_count}}",
#     "advance_decline" : f"{BASE_URL}/AdvancesDeclines/{{ex}}",
#     "52_weeks_high" : f"{BASE_URL}/FiftyTwoWeekHighEOD/{{ex}}/{{group}}/{{record_count}}",
#     "52_weeks_lows" : f"{BASE_URL}/FiftyTwoWeekLowEOD/{{ex}}/{{group}}/{{record_count}}",
#     "new_high_lows" : f"{BASE_URL}/NewHigh-NewLowEOD/{{group}}/{{high_or_low}}/{{period}}/{{record_count}}",
    
# }

# # ══════════════════════════════════════════════════════════════════════════════
# mcp = FastMCP("Equifiz MF Server")
# # ══════════════════════════════════════════════════════════════════════════════


# # ── RESOLVER TOOLS ─────────────────────────────────────────────────────────────
# # Resolvers exist ONLY to translate a human name → internal code for tool calls.
# # They should NOT be shown to the end-user as answers.


# ################################################ schemes and funds related  tools ##############################################
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
#     "CALL THIS FIRST before tool that require mf_schcode." 
#     "Read the returned mf_schcode and pass it exactly to the next tool. "
#     "Never skip this step."
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
#     # Fields from API spec: lname, fund_type, osch, csch, isch, sumoftotnav, dateas
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
#     "REQUIRES mf_cocode — call resolve_mf_fund first."
# ))
# def get_fund_categories(mf_cocode: int, category: str = "All") -> str:
#     # Fields from API spec: maincategory, vclass, vclasscode
#     val, err = _require_int(mf_cocode, "mf_cocode", "resolve_mf_fund")
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
#     "REQUIRES mf_cocode — call resolve_mf_fund first."
# ))
# def get_schemes_by_amc(mf_cocode: int) -> str:
#     # Fields from API spec: sch_name, navrs, category, amficode
#     val, err = _require_int(mf_cocode, "mf_cocode", "resolve_mf_fund")
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
#     "REQUIRES mf_cocode — call resolve_mf_fund first."
# ))
# def get_fund_profile(mf_cocode: int) -> str:
#     # Fields from API spec: VCLASS, VCLASSCODE
#     val, err = _require_int(mf_cocode, "mf_cocode", "resolve_mf_fund")
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
#     # Fields from API spec: fund_mgr, mf_schcode (hidden), SinceDate
#     # We show manager name + scheme (resolved from cache) + tenure
#     data, err = _get(EP["fund_manager"])
#     if err:
#         return err
#     rows = _as_list(data)[:30]
#     if not rows:
#         return "No fund manager data available."

#     # Build a quick schcode→name lookup from cache
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
#     "REQUIRES mf_schcode — call resolve_mf_scheme first."
# ))
# def get_scheme_nav(mf_schcode: int) -> str:
#     val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
#     if err:
#         return err

#     data, err = _get(EP["daily_nav"])
#     if err:
#         return err

#     records = data.get("data", []) if isinstance(data, dict) else []
#     if not records:
#         return "No NAV data available."

#     # API returns mf_schcode as float (e.g. 770.0), so cast before comparing
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
# )


# @mcp.tool(description=(
#     "Get investment details: minimum investment, scheme objective, tax treatment. "
#     "REQUIRES mf_schcode from resolve_mf_scheme. "
#     "Do NOT call this tool without first calling resolve_mf_scheme."
#     "Never call this with a guessed or example schcode like 123456."
# ))

# def get_investment_details(mf_schcode: int) -> str:
#     # Fields: inc_invest, mininvt, Objective, taxbname
#     val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
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
#     "You MUST call resolve_mf_scheme first to get the mf_schcode. "
#     "Do NOT call this tool without first calling resolve_mf_scheme. "
#     "Never guess or invent an mf_schcode."
# ))

# def get_expense_ratio(mf_schcode: int) -> str:
#     # Fields: EXPRATIO, entry, exit (from SchemeProfileExpRatio / ExpenseRatios)
#     val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
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
#     "You MUST call resolve_mf_scheme first to get the mf_schcode. "
#     "Do NOT call this tool without first calling resolve_mf_scheme. "
#     "Never guess or invent an mf_schcode."
#     "CRITICAL: This tool requires a validated 'mf_schcode'. "
#     "You are FORBIDDEN from inventing this code. "
#     "Workflow: 1. Call resolve_mf_scheme(name='...') 2. Use the returned 'mf_schcode' here."
# ))
# def get_avg_maturity(mf_schcode: int) -> str:
#     # Fields: avg_maturity, ModDuration, YTM, AvgMaturityDate, MacaulayDuration
#     val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
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
#     "REQUIRES mf_schcode — call resolve_mf_scheme first."
# ))
# def get_scheme_aum(mf_schcode: int) -> str:
#     # Fields: AUMDate, AUM
#     val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
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
#     matched = matched[:24]  # up to 24 months
#     lines = ["Historical AUM:"]
#     for row in matched:
#         p = _pick(row, ["AUMDate", "AUM"])
#         lines.append(f"  {p.get('AUMDate', 'N/A')}: ₹{p.get('AUM', 'N/A')} Cr")
#     return "\n".join(lines)


# @mcp.tool(description=(
#     "Get historical NAV for a scheme. "
#     "period: 'M' (months) or 'Y' (years). periodval: number of periods. "
#     "REQUIRES mf_schcode — call resolve_mf_scheme first."
# ))
# def get_nav_historical(mf_schcode: int, period: str = "Y", periodval: int = 1) -> str:
#     # Fields: NavDate, NAVRS, adjnavrs
#     val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
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
#     "REQUIRES mf_schcode — call resolve_mf_scheme first."
# ))
# def get_scheme_returns(mf_schcode: int) -> str:
#     # Fields: sch_name, Date, 1week, 1Month, 3Month, 6Month, 1Year, 3Year, 5Year, Inception
#     val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
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
#     "REQUIRES mf_schcode — call resolve_mf_scheme first."
# ))
# def get_lumpsum_returns(mf_schcode: int) -> str:
#     # Fields: InvAmount, W1LatestValue, W1Return_Abs, M1LatestValue, M1Return_Ann,
#     #         M3/M6/Y1/Y3/Y5/Y10 variants, InceptionLatestValue, InceptionReturn_Ann
#     val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
#     if err:
#         return err
#     url  = EP["lumpsum_return"].format(mf_schcode=val)
#     data, err = _get(url)
#     if err:
#         return err
#     rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
#     if not rows:
#         return "No lumpsum return data found."
#     r = rows[0]  # single record per scheme

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
#     # Fields: Sch_Name, VClass, NAVRS, NavDate, SchemeAssets, FundManager,
#     #         1WEEK..5YEAR, INCEPTION, ExitLoad
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
#     # Fields: schemename, TypeName, NavRs, NavDate (from FundPerformance API)
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
#     # Fields: typename, ret1w, ret1m, ret3m, ret6m, ret1y, ret3y, ret5y, ret10y, retinception
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
#     # Fields: plan, frequency, d1, d2, d3 (mf_schcode hidden)
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
#     for row in rows[:10]:  # show a sample
#         pr = _pick(row, ["frequency", "d1", "d2", "d3"])
#         dates = ", ".join(str(pr[d]) for d in ["d1","d2","d3"] if pr.get(d))
#         lines.append(f"  {pr.get('frequency', 'N/A')}: {dates}")
#     return "\n".join(lines)


# @mcp.tool(description=(
#     "Get SIP/SWP dates and minimum amounts for a specific scheme. "
#     "REQUIRES mf_schcode — call resolve_mf_scheme first."
# ))
# def get_scheme_sip_details(mf_schcode: int) -> str:
#     # Fields: SIPDates, minamt, multamt, avail_period
#     val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
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
#     "REQUIRES mf_schcode — call resolve_mf_scheme first."
# ))
# def get_mf_holdings(mf_schcode: int) -> str:
#     # Fields: co_name, perc_hold, mktvalue, no_shares, iind_name, type, rating
#     val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
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
#     "REQUIRES co_code — use the equifiz stock server's resolve_nse_symbol first."
# ))
# def get_funds_holding_company(co_code: int, top: int = 10) -> str:
#     # Fields: sch_name, invdate, mktvalue, perc_hold, totnav, no_shares
#     val, err = _require_int(co_code, "co_code", "resolve_nse_symbol (stock server)")
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
#     "REQUIRES mf_schcode — call resolve_mf_scheme first."
# ))
# def get_sector_allocation(mf_schcode: int) -> str:
#     # Fields: sector, perc_hold, value, secdate, oldperc_hold
#     val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
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
#     "REQUIRES mf_schcode — call resolve_mf_scheme first."
# ))
# def get_asset_allocation(mf_schcode: int) -> str:
#     # Fields: assetname, holding_currentmonth, holding_prevmonth, currentmonth
#     val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
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
#     "REQUIRES mf_schcode — call resolve_mf_scheme first."
# ))
# def get_portfolio_changes(mf_schcode: int, move_type: str = "in") -> str:
#     # Fields: CO_NAME, Perc_Hold, mktvalue, NO_SHARES, PortfolioDate, AssetType
#     val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
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
#     "REQUIRES mf_schcode — call resolve_mf_scheme first."
# ))
# def get_mcap_allocation(mf_schcode: int) -> str:
#     # Fields: mcaptype, MarketValue, perc_hold, CurrPFDate
#     val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
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
#     "REQUIRES mf_schcode — call resolve_mf_scheme first."
# ))
# def get_most_bought_sold(mf_schcode: int) -> str:
#     # Fields: CO_NAME, currmktvalue, prevmktvalue, diff, chng, CurrInvdate
#     val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
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
#     # Fields: sch_name, lname, launc_date, cldate, mininvt, schemetype
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
#     # Fields: TRANS_DATE, EQ_GR_PURC, EQ_GR_SALE, EQ_G_NE_PS, DE_GR_PURC, DE_GR_SALE
#     data, err = _get(EP["mf_activities"])
#     if err:
#         return err
#     rows =_as_list(data.get("data", []) if isinstance(data, dict) else data)  # ~12 months
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
#     "REQUIRES mf_schcode — call resolve_mf_scheme first."
# ))
# def get_scheme_ratios(mf_schcode: int) -> str:
#     # Fields: Scheme_Nam, DATE, BETA, SD, TREYNOR, ALPHA, SHARPE (if present)
#     val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
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
#     "REQUIRES mf_schcode — call resolve_mf_scheme first."
# ))
# def get_dividend_details(mf_schcode: int) -> str:
#     # Fields: sch_name, LNAME, DivPer, DivAmount, DIVPERPU, Divtype, DivDate, RecordDate
#     val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
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
#     # Fields: date, heading, caption, arttext, section_name
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
#     "REQUIRES mf_schcode — call resolve_mf_scheme first."
# ))
# def get_bse_star_scheme(mf_schcode: int) -> str:
#     # Fields: SCH_NAME, Scheme_Type, SIP_FLAG, STP_FLAG, SWP_FLAG, Settlement_Type, ISIN
#     val, err = _require_int(mf_schcode, "mf_schcode", "resolve_mf_scheme")
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
#     # Fields: mf_schcode (filter only), growth_payoutisin, reinvestmentisin, amficode
#     data, err = _get(EP["amfi_master"])
#     if err:
#         return err
#     rows = _as_list(data.get("data", []) if isinstance(data, dict) else data)
#     if mf_schcode:
#         rows = [r for r in rows if str(r.get("mf_schcode") or "") == str(mf_schcode)]
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

# #########################################################################################################################################

# ################################################company and stocks related tools ######################################################33

# @mcp.tool(description=(
#     "Resolve any company name, brand, or ticker into NSE symbol + co_code. "
#     "CALL THIS FIRST before any financial-ratio tool. "
#     "Accepts tickers (TCS), brand names (jio, dmart), or natural phrases."
# ))
# def resolve_nse_symbol(query: str) -> str:
#     cleaned   = _clean_query(query)
#     canonical = BRAND_MAP.get(cleaned.lower().strip(), cleaned).strip()

#     # Exact NSE symbol match
#     if canonical.isupper() and len(canonical) <= 12 and " " not in canonical:
#         row = _by_symbol(canonical)
#         if row:
#             return "RESOLVED (exact)\n" + _fmt_company(row)

#     # High-confidence fuzzy
#     hits = _fuzzy(canonical, limit=1, threshold=FUZZY_THRESHOLD)
#     if hits:
#         return "RESOLVED\n" + _fmt_company(hits[0])

#     # Relaxed fuzzy
#     hits = _fuzzy(canonical, limit=1, threshold=70)
#     if hits:
#         return "RESOLVED (low confidence — verify)\n" + _fmt_company(hits[0])

#     return f"NOT FOUND: '{canonical}'. Check the company name or NSE ticker."


# @mcp.tool(description=(
#     "Get full company profile by exact NSE symbol or co_code. "
#     "Use when you already have a precise identifier."
# ))
# def get_company_details(nse_symbol: Optional[str] = None, co_code: Optional[int] = None) -> str:
#     if not nse_symbol and co_code is None:
#         return "Error: provide nse_symbol or co_code."

#     row = _by_symbol(nse_symbol) if nse_symbol else None
#     if row is None and co_code is not None:
#         row = _by_code(co_code)
#     if row is None:
#         return f"NOT FOUND. Try resolve_nse_symbol(query='{nse_symbol or co_code}')."

#     return "COMPANY DETAILS\n" + _fmt_company(row)


# @mcp.tool(description=(
#     "Search companies by partial name, sector, or industry keyword (e.g. 'pharma', 'banks', 'steel'). "
#     "Returns ranked matches with co_code."
# ))
# def search_companies(query: str, limit: int = 5) -> str:
#     hits = _fuzzy(query, limit=min(max(limit, 1), 10), threshold=60)
#     if not hits:
#         return f"No companies found for '{query}'."
#     lines = [f"Results for '{query}':"]
#     for i, co in enumerate(hits, 1):
#         lines.append(f"  {i}. {co.get('companyname')}  |  NSE: {co.get('nsesymbol')}  |  co_code: {co.get('co_code')}  |  Sector: {co.get('sectorname')}")
#     return "\n".join(lines)


# @mcp.tool(description=(
#     "Fetch fundamental ratios: ROE, ROA, ROCE, margins, D/E, EPS, etc. "
#     "REQUIRES co_code — call resolve_nse_symbol first. "
#     "report_type: 's'=standalone (default), 'c'=consolidated."
# ))
# def get_key_financial_ratios(co_code: int, report_type: str = "s") -> str:
#     t = report_type if report_type in ("s", "c") else "s"
#     url = ENDPOINTS["key_ratios"].format(co_code=co_code, t=t)
#     data, err = _get(url, f"KeyFinancialRatios[{co_code}]")
#     if err:
#         return err
#     return f"Key Financial Ratios — co_code: {co_code}  [{'Standalone' if t=='s' else 'Consolidated'}]\n" + "─"*55 + "\n" + _fmt(data)


# @mcp.tool(description=(
#     "Fetch live/daily market ratios: PE, PB, MCAP, EPS, dividend yield, 52w high/low. "
#     "REQUIRES co_code — call resolve_nse_symbol first. "
#     "report_type: 's'=standalone (default), 'c'=consolidated."
# ))
# def get_daily_ratios(co_code: int, report_type: str = "s") -> str:
#     t = report_type if report_type in ("s", "c") else "s"
#     url = ENDPOINTS["daily_ratios"].format(co_code=co_code, t=t)
#     data, err = _get(url, f"DailyRatios[{co_code}]")
#     if err:
#         return err
#     return f"Daily Ratios — co_code: {co_code}  [{'Standalone' if t=='s' else 'Consolidated'}]\n" + "─"*55 + "\n" + _fmt(data)


# def _ipo_tool(url: str, label: str) -> str:
#     data, err = _get(url, label)
#     if err:
#         return err
#     records = data if isinstance(data, list) else data.get("data", data) if isinstance(data, dict) else []
#     if not records:
#         return f"No {label} found."
#     n = len(records) if isinstance(records, list) else 1
#     return f"{label} ({n} record{'s' if n != 1 else ''})\n" + "─"*60 + "\n" + _fmt(records)


# @mcp.tool(description="Fetch upcoming IPOs (announced, not yet open). exchange='NSE'/'BSE', count=10.")
# def get_forthcoming_ipos(exchange: str = "NSE", count: int = 10) -> str:
#     try:
#         ex = _normalise_exchange(exchange)
#     except ValueError as e:
#         return str(e)
#     return _ipo_tool(ENDPOINTS["forthcoming_ipo"].format(ex=ex, n=count), f"Forthcoming IPOs [{ex}]")


# @mcp.tool(description="Fetch IPOs currently open for subscription. exchange='NSE'/'BSE', count=10.")
# def get_open_ipo_issues(exchange: str = "NSE", count: int = 10) -> str:
#     try:
#         ex = _normalise_exchange(exchange)
#     except ValueError as e:
#         return str(e)
#     return _ipo_tool(ENDPOINTS["open_ipo"].format(ex=ex, n=count), f"Open IPOs [{ex}]")


# @mcp.tool(description="Fetch recently closed IPOs (bidding ended). exchange='NSE'/'BSE', count=10.")
# def get_closed_ipos(exchange: str = "NSE", count: int = 10) -> str:
#     try:
#         ex = _normalise_exchange(exchange)
#     except ValueError as e:
#         return str(e)
#     return _ipo_tool(ENDPOINTS["closed_ipo"].format(ex=ex, n=count), f"Closed IPOs [{ex}]")


# @mcp.tool(description="Fetch recently listed IPOs with listing price/gains. exchange='NSE'/'BSE', count=10.")
# def get_new_ipo_listings(exchange: str = "NSE", count: int = 10) -> str:
#     try:
#         ex = _normalise_exchange(exchange)
#     except ValueError as e:
#         return str(e)
#     return _ipo_tool(ENDPOINTS["new_listing"].format(ex=ex, n=count), f"New IPO Listings [{ex}]")


# @mcp.tool(description="Fetch top-performing IPOs by listing gains. exchange='NSE'/'BSE', count=10.")
# def get_ipo_best_performers(exchange: str = "NSE", count: int = 10) -> str:
#     try:
#         ex = _normalise_exchange(exchange)
#     except ValueError as e:
#         return str(e)
#     return _ipo_tool(ENDPOINTS["best_ipo"].format(ex=ex, n=count), f"IPO Best Performers [{ex}]")


# # ══════════════════════════════════════════════════════════════════════════════
# # STOCK PRICE TOOLS  (BSE-NSE Price Feed)
# # ══════════════════════════════════════════════════════════════════════════════

# def _price_tool(url: str, label: str, co_code: int = None, ex: str = None) -> str:
#     data, err = _get(url, label)
#     if err:
#         return err
#     if not data:
#         return f"No data found for {label}."

#     records = data if isinstance(data, list) else [data]
#     n = len(records)
#     lines = [f"{label} ({n} record{'s' if n != 1 else ''})\n" + "─" * 60]
#     for r in records:
#         lines.append(_fmt(r))
#     return "\n".join(lines)


# @mcp.tool(description=(
#     "Get current stock price, open, high, low, volume for a specific company. "
#     "REQUIRES co_code — call resolve_nse_symbol first. exchange: 'NSE' (default) or 'BSE'."
# ))
# def get_company_stock_price(co_code: int, exchange: str = "NSE") -> str:
#     try:
#         ex = _normalise_exchange(exchange)
#     except ValueError as e:
#         return str(e)
#     return _price_tool(ENDPOINTS["company_quotes"].format(co_code=co_code, ex=ex), f"Stock Price [{ex}]")



# @mcp.tool(description="Get live index values with key metrics.")
# def get_market_indices(exchange: str = "NSE") -> str:
#     import json

#     raw_data = _price_tool(ENDPOINTS["indices"], "Market Indices")

#     try:
#         data = json.loads(raw_data) if isinstance(raw_data, str) else raw_data

#         # Normalize: handle both list and {"data": [...]} shapes
#         if isinstance(data, dict):
#             data = data.get("data") or data.get("indices") or list(data.values())[0]

#         filtered = []
#         for item in data:
#             # Filter by exchange if the field exists
#             if exchange and item.get("EXCHANGE", item.get("exchange", "")).upper() not in ("", exchange.upper()):
#                 continue

#             filtered.append({
#                 "symbol":   item.get("SYMBOL")   or item.get("symbol")   or item.get("IndexName"),
#                 "ltp":      item.get("LTP")       or item.get("ltp")      or item.get("Close"),
#                 "change":   item.get("CHANGE")    or item.get("change")   or item.get("NetChange"),
#                 "pct":      item.get("PER_CHANGE")   or item.get("pchange")  or item.get("PercentChange"),
#                 "open":     item.get("OPEN")      or item.get("open"),
#                 "high":     item.get("HIGH")      or item.get("high"),
#                 "low":      item.get("LOW")       or item.get("low"),
#                 "prev":     item.get("PREV_CLOSE") or item.get("prevclose") or item.get("PreviousClose"),
#             })

#         # Drop keys with None values to keep the payload lean
#         filtered = [{k: v for k, v in idx.items() if v is not None} for idx in filtered]

#         return json.dumps(filtered, indent=None)  # compact, no pretty-print

#     except Exception as e:
#         # Return a hard cap — never let raw data flood the LLM
#         return json.dumps({"error": str(e), "raw_preview": str(raw_data)[:500]})


# @mcp.tool(description=(
#     "Get exchanges holidays "
#     "REQUIRES cexchange: 'NSE' (default) or 'BSE'."
# ))
# def get_exchange_holidays(exchange: str = "NSE") -> str:
#     try:
#         ex = _normalise_exchange(exchange)
#     except ValueError as e:
#         return str(e)
#     return _price_tool(ENDPOINTS["exchange_holidays"].format(ex=ex), f"Stock Price [{ex}]")


# # @mcp.tool(description=(
# #     "get top performer, active stock performer"
# #     "REQUIRES exchange, group, record count"
# # ))

# # def get_active_performer(exchange: str="NSE", group: str="BANKNIFTY", record_count: int=3):
# #     try:
# #         ex = _normalise_exchange(exchange=exchange)
# #     except ValueError as e:
# #         return str(e)
# #     return _price_tool(ENDPOINTS["active_performer"].format(ex=ex,group=group,record_count=record_count),f"Stock Price [{ex}]")



# #########################################################################################################################################


# ################################group related mcp tools##################################################


# ##gainers
# @mcp.tool(description=(
#     "Get top gainers"
#     "REQUIRES exchange, group (e.g. 'BANKNIFTY', 'bank nifty', 'BN'), record_count."
# ))
# def get_top_gainers(
#     exchange: str = "NSE",
#     group: str = "BANKNIFTY",
#     record_count: int = 3,
# ):
#     try:
#         ex = _normalise_exchange(exchange)
#     except ValueError as e:
#         return str(e)

#     # ── Fuzzy-resolve the group name ──────────────────────────────────────────
#     resolved_group = _resolve_group(group, exchange=ex)
#     if resolved_group is None:
#         # Fall back to whatever the caller passed, uppercased
#         resolved_group = group.strip().upper()
#         logger.warning("Using raw group value (no fuzzy match): %r", resolved_group)

#     return _price_tool(
#         ENDPOINTS["gainers"].format(
#             ex=ex,
#             group=resolved_group,
#             record_count=record_count,
#         ),
#         f"top gainers [{ex} / {resolved_group}]",
#     )


# ##losers
# @mcp.tool(description=(
#     "Get top loosers"
#     "REQUIRES exchange, group (e.g. 'BANKNIFTY', 'bank nifty', 'BN'), record_count."
# ))
# def get_top_loosers(
#     exchange: str = "NSE",
#     group: str = "BANKNIFTY",
#     record_count: int = 3,
# ):
#     try:
#         ex = _normalise_exchange(exchange)
#     except ValueError as e:
#         return str(e)

#     # ── Fuzzy-resolve the group name ──────────────────────────────────────────
#     resolved_group = _resolve_group(group, exchange=ex)
#     if resolved_group is None:
#         # Fall back to whatever the caller passed, uppercased
#         resolved_group = group.strip().upper()
#         logger.warning("Using raw group value (no fuzzy match): %r", resolved_group)

#     return _price_tool(
#         ENDPOINTS["loosers"].format(
#             ex=ex,
#             group=resolved_group,
#             record_count=record_count,
#         ),
#         f"top loosers [{ex} / {resolved_group}]",
#     )

# ##out or under performer
# @mcp.tool(description=(
#     "Get get outstanding or under performer"
#     "REQUIRES exchange, group (e.g. 'BANKNIFTY', 'bank nifty', 'BN'), record_count, performance_type (e.g. 'out' or 'under)"
# ))
# def out_or_under_performer(
#     exchange: str = "NSE",
#     group: str = "BANKNIFTY",
#     record_count: int = 3,
#     performance_type: str = "out"
# ):
#     try:
#         ex = _normalise_exchange(exchange)
#     except ValueError as e:
#         return str(e)

#     # ── Fuzzy-resolve the group name ──────────────────────────────────────────
#     resolved_group = _resolve_group(group, exchange=ex)
#     if resolved_group is None:
#         # Fall back to whatever the caller passed, uppercased
#         resolved_group = group.strip().upper()
#         logger.warning("Using raw group value (no fuzzy match): %r", resolved_group)

#     return _price_tool(
#         ENDPOINTS["out_or_under_performers"].format(
#             ex=ex,
#             group=resolved_group,
#             record_count=record_count,
#             performance_type=performance_type
#         ),
#         f"under or out performer[{ex} / {resolved_group}]",
#     )

# # 52 weeks highs
# @mcp.tool(description=(
#     "Get get 52 weeks high"
#     "REQUIRES exchange, group (e.g. 'BANKNIFTY', 'bank nifty', 'BN'), record_count"
# ))
# def weeks_high_52(
#     exchange: str = "NSE",
#     group: str = "BANKNIFTY",
#     record_count: int = 3,
# ):
#     try:
#         ex = _normalise_exchange(exchange)
#     except ValueError as e:
#         return str(e)

#     # ── Fuzzy-resolve the group name ──────────────────────────────────────────
#     resolved_group = _resolve_group(group, exchange=ex)
#     if resolved_group is None:
#         # Fall back to whatever the caller passed, uppercased
#         resolved_group = group.strip().upper()
#         logger.warning("Using raw group value (no fuzzy match): %r", resolved_group)

#     return _price_tool(
#         ENDPOINTS["52_weeks_high"].format(
#             ex=ex,
#             group=resolved_group,
#             record_count=record_count,
            
#         ),
#         f"52 weeks high[{ex} / {resolved_group}]",
#     )

# #52 weeks lows

# @mcp.tool(description=(
#     "Get 52 weeks lows"
#     "REQUIRES exchange, group (e.g. 'BANKNIFTY', 'bank nifty', 'BN'), record_count"
# ))
# def weeks_low_52(
#     exchange: str = "NSE",
#     group: str = "BANKNIFTY",
#     record_count: int = 3,
# ):
#     try:
#         ex = _normalise_exchange(exchange)
#     except ValueError as e:
#         return str(e)

#     # ── Fuzzy-resolve the group name ──────────────────────────────────────────
#     resolved_group = _resolve_group(group, exchange=ex)
#     if resolved_group is None:
#         # Fall back to whatever the caller passed, uppercased
#         resolved_group = group.strip().upper()
#         logger.warning("Using raw group value (no fuzzy match): %r", resolved_group)

#     return _price_tool(
#         ENDPOINTS["52_weeks_lows"].format(
#             ex=ex,
#             group=resolved_group,
#             record_count=record_count,
            
#         ),
#         f"52 weeks lows [{ex} / {resolved_group}]",
#     )

# #advance decline

# @mcp.tool(description=(
#     "Get advance-decline statistics (number of gaining vs losing stocks) for an exchange. "
#     "Use this when the user asks about market breadth, decline, falling stocks, advance/decline ratio. "
#     "REQUIRES exchange like NSE or BSE."
# ))
# def advance_decline(exchange: str = "NSE"):
#     try:
#         ex = _normalise_exchange(exchange)
#     except ValueError as e:
#         return str(e)

#     raw = _price_tool(
#         ENDPOINTS["advance_decline"].format(ex=ex),
#         f"advance_decline[{ex}]",
#     )

#     # Parse and filter to only relevant fields
#     try:
#         data = json.loads(raw) if isinstance(raw, str) else raw
        
#         # If it's a list of records, filter each one
#         records = data if isinstance(data, list) else data.get("data", [data])
        
#         filtered = []
#         for rec in records:
#             filtered.append({
#                 "indexname": rec.get("indexname"),
#                 "adv": rec.get("adv"),        # advancing stocks
#                 "dec": rec.get("dec"),        # declining stocks
#                 "noc": rec.get("noc"),        # no change
#                 "ad": rec.get("ad"),          # advance/decline ratio
#             })
        
#         return json.dumps(filtered, indent=2)
    
#     except Exception as e:
#         # Return a hard cap — never let raw data flood the LLM
#         return json.dumps({"error": str(e), "raw_preview": str(raw)[:500]})
    
    
# # new highs new lows


# @mcp.tool(description=(
#     "Get get outstanding or under performer"
#     "REQUIRES exchange, group (e.g. 'BANKNIFTY', 'bank nifty', 'BN'), record_count, performance_type (e.g. 'out' or 'under)"
# ))
# def new_highs_lows(
#     exchange: str = "NSE",
#     group: str = "BANKNIFTY",
#     record_count: int = 3,
#     performance_type: str = "out"
# ):
#     try:
#         ex = _normalise_exchange(exchange)
#     except ValueError as e:
#         return str(e)

#     # ── Fuzzy-resolve the group name ──────────────────────────────────────────
#     resolved_group = _resolve_group(group, exchange=ex)
#     if resolved_group is None:
#         # Fall back to whatever the caller passed, uppercased
#         resolved_group = group.strip().upper()
#         logger.warning("Using raw group value (no fuzzy match): %r", resolved_group)

#     return _price_tool(
#         ENDPOINTS["new_high_lows"].format(
#             ex=ex,
#             group=resolved_group,
#             record_count=record_count,
#             performance_type=performance_type
#         ),
#         f"under or out performer[{ex} / {resolved_group}]",
#     )


# #########################################################################################################


# ##################################### etf related functions ##############################################

# @mcp.tool(description=(
#     "this tools for thr query where the user wants to know about the details of etfs under a particular amc"
#     "this requires the mc_cocode"
# ))
# def etf_under_amc_funds(mf_cocode: int) -> str:
#     # Fields: sch_name, Date, 1week, 1Month, 3Month, 6Month, 1Year, 3Year, 5Year, Inception
#     val, err = _require_int(mf_cocode, "mf_cocode", "resolve_mf_fund")
#     try:
#         conn = _db()
#         with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
#             cur.execute(f"SELECT etfname,etfcategory FROM etf_master where mf_cmotscode = {val}")
#         conn.close()
#     except Exception as e:
#         logger.error("Group cache load failed: %s", e)
    

# # ══════════════════════════════════════════════════════════════════════════════
# if __name__ == "__main__":
#     mcp.run()




#########################################  new server 95 tool #########################################################

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
    # ── Stock: Financials ──────────────────────────────────────────────────────
    "quarterly_results":    f"{BASE_URL}/QuarterlyResults/{{co_code}}/{{t}}",
    "profit_loss":          f"{BASE_URL}/ProftandLoss/{{co_code}}/{{t}}",
    "balance_sheet":        f"{BASE_URL}/BalanceSheet/{{co_code}}/{{t}}",
    "cash_flow":            f"{BASE_URL}/CashFlow/{{co_code}}/{{t}}",
    "shareholding_detailed":f"{BASE_URL}/ShareHoldingPatternDetailed/{{co_code}}",
    "shareholding_1pct":    f"{BASE_URL}/ShareholdingMorethanOnePercent/{{co_code}}",
    "q_trend_revenue":      f"{BASE_URL}/QuarterlyTrendsrevenue/{{co_code}}",
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
    # ── Stock: Price / Market ──────────────────────────────────────────────────
    "company_quotes":       f"{BASE_URL}/GetQuotes/{{co_code}}/{{ex}}",
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
    "Get current stock price: LTP, open, high, low, close, volume, 52W high/low. "
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
    url = EP["company_quotes"].format(co_code=val, ex=ex)
    data, err = _get(url, f"StockPrice[{val}/{ex}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No price data found."
    r = rows[0]
    p = _pick(r, ["CompanyName", "LTP", "Open", "High", "Low", "Close",
                  "PrevClose", "Change", "PctChange", "Volume", "High52W", "Low52W", "NavDate"])
    lines = [f"Stock Price — {p.get('CompanyName', 'N/A')} [{ex}]"]
    for k, v in p.items():
        if k != "CompanyName":
            lines.append(f"  {k:<15}: {v}")
    return "\n".join(lines)


@mcp.tool(description="Get live index values (NIFTY 50, SENSEX, BANK NIFTY, etc.) with LTP, change and % change.")
def get_market_indices(exchange: str = "NSE") -> str:
    data, err = _get(EP["indices"], "Indices")
    if err:
        return err
    records = _rows(data)
    filtered = []
    for item in records:
        ex_val = (item.get("EXCHANGE") or item.get("exchange") or "").upper()
        if exchange and ex_val not in ("", exchange.upper()):
            continue
        filtered.append({
            "symbol": item.get("SYMBOL") or item.get("symbol") or item.get("IndexName"),
            "ltp":    item.get("LTP")    or item.get("ltp")    or item.get("Close"),
            "change": item.get("CHANGE") or item.get("change") or item.get("NetChange"),
            "pct":    item.get("PER_CHANGE") or item.get("pchange") or item.get("PercentChange"),
            "open":   item.get("OPEN")   or item.get("open"),
            "high":   item.get("HIGH")   or item.get("high"),
            "low":    item.get("LOW")    or item.get("low"),
            "prev":   item.get("PREV_CLOSE") or item.get("prevclose") or item.get("PreviousClose"),
        })
    filtered = [{k: v for k, v in idx.items() if v is not None} for idx in filtered]
    return json.dumps(filtered)


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
    "Get top active/value performing stocks for a group. "
    "group: e.g. 'NIFTY50', 'BANKNIFTY'. exchange: 'NSE' or 'BSE'."
))
def get_active_performers(exchange: str = "NSE", group: str = "NIFTY50", record_count: int = 5) -> str:
    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)
    resolved = _resolve_group(group, exchange=ex) or group.strip().upper()
    url = EP["active_performer"].format(ex=ex, group=resolved, record_count=record_count)
    data, err = _get(url, f"ActivePerformer[{ex}/{resolved}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No data found."
    lines = [f"Top Active Performers — {ex} / {resolved}:"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, ["CompanyName", "Symbol", "LTP", "Change", "PctChange", "Volume", "Value"])
        name = p.get("CompanyName") or p.get("Symbol", "N/A")
        lines.append(
            f"  {i:>3}. {name}"
            f"  LTP: {p.get('LTP','N/A')}"
            f"  Chg: {p.get('Change','N/A')} ({p.get('PctChange','N/A')}%)"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get top gaining stocks for a group. "
    "group: e.g. 'NIFTY50', 'BANKNIFTY'. exchange: 'NSE' or 'BSE'."
))
def get_top_gainers(exchange: str = "NSE", group: str = "NIFTY50", record_count: int = 5) -> str:
    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)
    resolved = _resolve_group(group, exchange=ex) or group.strip().upper()
    url = EP["gainers"].format(ex=ex, group=resolved, record_count=record_count)
    data, err = _get(url, f"Gainers[{ex}/{resolved}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No gainers data found."
    lines = [f"Top Gainers — {ex} / {resolved}:"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, ["CompanyName", "Symbol", "LTP", "Change", "PctChange", "Open", "PrevClose"])
        name = p.get("CompanyName") or p.get("Symbol", "N/A")
        lines.append(
            f"  {i:>3}. {name}"
            f"  LTP: {p.get('LTP','N/A')}"
            f"  ▲ {p.get('Change','N/A')} ({p.get('PctChange','N/A')}%)"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get top losing stocks for a group. "
    "group: e.g. 'NIFTY50', 'BANKNIFTY'. exchange: 'NSE' or 'BSE'."
))
def get_top_losers(exchange: str = "NSE", group: str = "NIFTY50", record_count: int = 5) -> str:
    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)
    resolved = _resolve_group(group, exchange=ex) or group.strip().upper()
    url = EP["losers"].format(ex=ex, group=resolved, record_count=record_count)
    data, err = _get(url, f"Losers[{ex}/{resolved}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No losers data found."
    lines = [f"Top Losers — {ex} / {resolved}:"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, ["CompanyName", "Symbol", "LTP", "Change", "PctChange", "Open", "PrevClose"])
        name = p.get("CompanyName") or p.get("Symbol", "N/A")
        lines.append(
            f"  {i:>3}. {name}"
            f"  LTP: {p.get('LTP','N/A')}"
            f"  ▼ {p.get('Change','N/A')} ({p.get('PctChange','N/A')}%)"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get outperforming or underperforming stocks vs their group index. "
    "performer: 'out' or 'under'. group: e.g. 'NIFTY50'. exchange: 'NSE' or 'BSE'."
))
def get_out_under_performers(
    exchange: str = "NSE", group: str = "NIFTY50",
    performer: str = "out", record_count: int = 5
) -> str:
    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)
    p = performer.lower()
    if p not in ("out", "under"):
        return "performer must be 'out' or 'under'."
    resolved = _resolve_group(group, exchange=ex) or group.strip().upper()
    url = EP["out_under_performers"].format(ex=ex, group=resolved, performer=p, record_count=record_count)
    data, err = _get(url, f"OutUnder[{ex}/{resolved}/{p}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return f"No {p}performer data found."
    label = "Outperformers" if p == "out" else "Underperformers"
    lines = [f"{label} — {ex} / {resolved}:"]
    for i, row in enumerate(rows, 1):
        pr = _pick(row, ["CompanyName", "Symbol", "LTP", "Change", "PctChange"])
        name = pr.get("CompanyName") or pr.get("Symbol", "N/A")
        lines.append(f"  {i:>3}. {name}  LTP: {pr.get('LTP','N/A')}  {pr.get('PctChange','N/A')}%")
    return "\n".join(lines)


@mcp.tool(description="Get advance-decline statistics (advancing vs declining stocks) for market breadth.")
def get_advance_decline(exchange: str = "NSE") -> str:
    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)
    url = EP["advance_decline"].format(ex=ex)
    data, err = _get(url, f"AdvanceDecline[{ex}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No advance-decline data."
    lines = [f"Advance / Decline — {ex}:"]
    for row in rows:
        p = _pick(row, ["indexname", "adv", "dec", "noc", "ad"])
        lines.append(
            f"  {p.get('indexname','N/A'):<30}"
            f"  Adv: {p.get('adv','N/A')}"
            f"  Dec: {p.get('dec','N/A')}"
            f"  Unch: {p.get('noc','N/A')}"
            f"  A/D Ratio: {p.get('ad','N/A')}"
        )
    return "\n".join(lines)


@mcp.tool(description="Get stocks at 52-week high. group: 'NIFTY50' or '-' for all. exchange: 'NSE' or 'BSE'.")
def get_52week_highs(exchange: str = "NSE", group: str = "-", record_count: int = 10) -> str:
    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)
    resolved = (_resolve_group(group, exchange=ex) if group != "-" else None) or "-"
    url = EP["52w_high"].format(ex=ex, group=resolved, record_count=record_count)
    data, err = _get(url, f"52WkHigh[{ex}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No 52-week high data found."
    lines = [f"52-Week Highs — {ex}:"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, ["CompanyName", "Symbol", "LTP", "High52W", "PctChange"])
        name = p.get("CompanyName") or p.get("Symbol", "N/A")
        lines.append(
            f"  {i:>3}. {name}"
            f"  LTP: {p.get('LTP','N/A')}"
            f"  52W High: {p.get('High52W','N/A')}"
        )
    return "\n".join(lines)


@mcp.tool(description="Get stocks at 52-week low. group: 'NIFTY50' or '-' for all. exchange: 'NSE' or 'BSE'.")
def get_52week_lows(exchange: str = "NSE", group: str = "-", record_count: int = 10) -> str:
    try:
        ex = _normalise_exchange(exchange)
    except ValueError as e:
        return str(e)
    resolved = (_resolve_group(group, exchange=ex) if group != "-" else None) or "-"
    url = EP["52w_low"].format(ex=ex, group=resolved, record_count=record_count)
    data, err = _get(url, f"52WkLow[{ex}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No 52-week low data found."
    lines = [f"52-Week Lows — {ex}:"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, ["CompanyName", "Symbol", "LTP", "Low52W", "PctChange"])
        name = p.get("CompanyName") or p.get("Symbol", "N/A")
        lines.append(
            f"  {i:>3}. {name}"
            f"  LTP: {p.get('LTP','N/A')}"
            f"  52W Low: {p.get('Low52W','N/A')}"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get stocks making new highs or new lows over a period. "
    "high_or_low: 'high' or 'low'. period: 'year', 'month', or 'week'. "
    "group: index group e.g. 'CNXMIDCAP'."
))
def get_new_highs_lows(
    group: str = "CNXMIDCAP", high_or_low: str = "high",
    period: str = "year", record_count: int = 10
) -> str:
    hl = high_or_low.lower()
    if hl not in ("high", "low"):
        return "high_or_low must be 'high' or 'low'."
    url = EP["new_high_low"].format(
        group=group.strip().upper(), high_or_low=hl,
        period=period.lower(), record_count=record_count
    )
    data, err = _get(url, f"NewHighLow[{group}/{hl}/{period}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No new high/low data found."
    lines = [f"New {hl.title()}s ({period}) — {group.upper()}:"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, ["CompanyName", "Symbol", "LTP", "PctChange"])
        name = p.get("CompanyName") or p.get("Symbol", "N/A")
        lines.append(f"  {i:>3}. {name}  LTP: {p.get('LTP','N/A')}")
    return "\n".join(lines)


@mcp.tool(description="Get list of all market indices with their codes. Use code with get_index_companies.")
def get_index_list() -> str:
    data, err = _get(EP["index_list"], "IndexList")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No index data found."
    lines = [f"Market Indices ({len(rows)} total):"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, ["index_code", "index_name", "exchange"])
        lines.append(
            f"  {i:>3}. {p.get('index_name','N/A'):<40}"
            f"  Code: {p.get('index_code','N/A')}"
            f"  [{p.get('exchange','')}]"
        )
    return "\n".join(lines)


@mcp.tool(description="Get companies in a specific market index. REQUIRES index_code — call get_index_list first.")
def get_index_companies(index_code: int) -> str:
    val, err = _require_int(index_code, "index_code", "get_index_list")
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
    lines = ["BSE Announcements:"]
    for i, row in enumerate(rows[:15], 1):
        p = _pick(row, ["CompanyName", "Symbol", "Category", "Headline", "Date"])
        lines.append(
            f"  [{str(p.get('Date',''))[:10]}]"
            f"  {p.get('CompanyName','N/A')} [{p.get('Symbol','')}]"
            f"  — {p.get('Headline', p.get('Category','N/A'))}"
        )
    return "\n".join(lines)


@mcp.tool(description="Get latest NSE corporate announcements.")
def get_nse_announcements() -> str:
    data, err = _get(EP["nse_announcement"], "NSEAnnouncement")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No NSE announcements found."
    lines = ["NSE Announcements:"]
    for i, row in enumerate(rows[:15], 1):
        p = _pick(row, ["CompanyName", "Symbol", "Category", "Headline", "Date"])
        lines.append(
            f"  [{str(p.get('Date',''))[:10]}]"
            f"  {p.get('CompanyName','N/A')} [{p.get('Symbol','')}]"
            f"  — {p.get('Headline', p.get('Category','N/A'))}"
        )
    return "\n".join(lines)


@mcp.tool(description="Get latest corporate news headlines. count: number of articles (default 10).")
def get_corporate_news(count: int = 10) -> str:
    url = EP["corporate_news"].format(n=count)
    data, err = _get(url, "CorporateNews")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No corporate news available."
    lines = ["Corporate News:"]
    for row in rows:
        p = _pick(row, ["date", "heading", "caption"])
        lines.append(f"\n  [{str(p.get('date',''))[:10]}]  {p.get('heading','N/A')}")
        if p.get("caption"):
            lines.append(f"  {str(p['caption'])[:150]}")
    return "\n".join(lines)


@mcp.tool(description="Get companies that declared results today.")
def get_results_today() -> str:
    data, err = _get(EP["results_today"], "ResultsToday")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No results declared today."
    lines = [f"Today's Results ({len(rows)} companies):"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, ["co_name", "symbol", "resultdate"])
        lines.append(
            f"  {i:>3}. {p.get('co_name','N/A')}"
            f"  [{p.get('symbol','')}]"
            f"  Result Date: {str(p.get('resultdate',''))[:10]}"
        )
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — STOCK FUNDAMENTAL RATIOS
# ═══════════════════════════════════════════════════════════════════════════════

# @mcp.tool(description=(
#     "Get key fundamental ratios: ROE, ROA, ROCE, EPS, D/E ratio, net profit margin. "
#     "REQUIRES co_code — call resolve_nse_symbol first. "
#     "report_type: 's' = standalone (default), 'c' = consolidated."
# ))
# def get_key_financial_ratios(co_code: int, report_type: str = "s") -> str:
#     val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
#     if err:
#         return err
#     t = report_type if report_type in ("s", "c") else "s"
#     url = EP["key_ratios"].format(co_code=val, t=t)
#     data, err = _get(url, f"KeyRatios[{val}]")
#     if err:
#         return err
#     rows = _rows(data)
#     if not rows:
#         return "No key ratio data found."
#     FIELDS = ["YRC", "Year", "EPS", "BVPS", "DPS", "ROE", "ROCE", "ROA",
#               "DERatio", "CurrentRatio", "NetProfitMargin", "OPM"]
#     lines = [f"Key Financial Ratios [{'Standalone' if t=='s' else 'Consolidated'}]:"]
#     for row in rows[:5]:
#         p = _pick(row, FIELDS)
#         yr = p.get("Year") or p.get("YRC", "N/A")
#         lines.append(
#             f"  {yr}  |  EPS: {p.get('EPS','N/A')}"
#             f"  ROE: {p.get('ROE','N/A')}%"
#             f"  ROCE: {p.get('ROCE','N/A')}%"
#             f"  D/E: {p.get('DERatio','N/A')}"
#             f"  NPM: {p.get('NetProfitMargin','N/A')}%"
#         )
#     return "\n".join(lines)


@mcp.tool(description=(
    "Get key financial ratios for a company (Debt-Equity, Current Ratio, Inventory Turnover, ROCE, etc.). "
    "Returns a table of metrics across the last 5 reporting periods. "
    "Values are mapped from COLUMNNAME rows and Y<YYYYMM> columns. "
    "REQUIRES co_code — call resolve_nse_symbol first. "
    "report_type: 's' = standalone (default), 'c' = consolidated."
))
def get_key_financial_ratios(co_code: int, report_type: str = "s") -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
        
    t = report_type.lower() if report_type in ("s", "c") else "s"
    url = EP["key_ratios"].format(co_code=val, t=t)
    data, err = _get(url, f"KeyRatios[{val}]")
    
    if err:
        return err
        
    rows = _rows(data)  # Extract data["data"]
    if not rows:
        return "No key ratio data found."

    # 1. Detect dynamic year columns (e.g., Y202503, Y202403)
    # We check the first row to find keys that match the year pattern
    year_cols = sorted(
        [k for k in rows[0].keys() if k.startswith("Y") and k[1:].isdigit()],
        reverse=True
    )[:5]

    # 2. Helper to format 'Y202503' into 'Mar 2025'
    def fmt_year(yc):
        try:
            yr = yc[1:5]
            mo_code = yc[5:]
            months = {'03': 'Mar', '06': 'Jun', '09': 'Sep', '12': 'Dec'}
            return f"{months.get(mo_code, mo_code)} {yr}"
        except Exception:
            return yc

    # 3. Build Table Header
    header = f"{'Metric':<35} " + "  ".join(f"{fmt_year(yc):>10}" for yc in year_cols)
    lines = [
        f"### Key Financial Ratios [{'Standalone' if t == 's' else 'Consolidated'}]",
        header,
        "-" * (35 + (12 * len(year_cols)))
    ]

    # 4. Iterate through rows (Metrics) and extract values for each year column
    for row in rows:
        metric_name = row.get("COLUMNNAME", "").strip()
        if not metric_name:
            continue
        
        # Build the row values string
        vals = "  ".join(f"{str(row.get(yc, 'N/A')):>10}" for yc in year_cols)
        lines.append(f"{metric_name:<35} {vals}")

    return "\n".join(lines)

@mcp.tool(description=(
    "Get live/daily market ratios: PE, PB, Market Cap, EPS (TTM), dividend yield, 52W high/low. "
    "REQUIRES co_code — call resolve_nse_symbol first. "
    "report_type: 's' = standalone (default), 'c' = consolidated."
))
def get_daily_ratios(co_code: int, report_type: str = "s") -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    t = report_type if report_type in ("s", "c") else "s"
    url = EP["daily_ratios"].format(co_code=val, t=t)
    data, err = _get(url, f"DailyRatios[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No daily ratio data found."
    r = rows[0]
    p = _pick(r, ["CompanyName", "PE", "PB", "MCAP", "EPS", "DivYield",
                  "High52W", "Low52W", "FaceValue", "BookValue"])
    lines = [f"Daily Market Ratios — {p.get('CompanyName','N/A')}:"]
    for k, v in p.items():
        if k != "CompanyName":
            lines.append(f"  {k:<15}: {v}")
    return "\n".join(lines)


@mcp.tool(description=(
    "Get margin ratios: Gross Margin, EBITDA Margin, EBIT Margin, Net Profit Margin, PAT Margin. "
    "REQUIRES co_code. report_type: 's' or 'c'."
))
def get_margin_ratios(co_code: int, report_type: str = "s") -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    t = report_type if report_type in ("s", "c") else "s"
    url = EP["margin_ratios"].format(co_code=val, t=t)
    data, err = _get(url, f"MarginRatios[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No margin ratio data found."
    lines = [f"Margin Ratios [{'Standalone' if t=='s' else 'Consolidated'}]:"]
    for row in rows[:5]:
        p = _pick(row, ["Year", "YRC", "GrossMargin", "EBITDAMargin", "EBITMargin",
                        "NetProfitMargin", "PATMargin", "OPM"])
        yr = p.get("Year") or p.get("YRC", "N/A")
        lines.append(
            f"  {yr}  |  GPM: {p.get('GrossMargin','N/A')}%"
            f"  EBITDA: {p.get('EBITDAMargin','N/A')}%"
            f"  NPM: {p.get('NetProfitMargin','N/A')}%"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get valuation ratios: PE, PB, EV/EBITDA, Price/Sales, Dividend Yield. "
    "REQUIRES co_code. report_type: 's' or 'c'."
))
def get_valuation_ratios(co_code: int, report_type: str = "s") -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    t = report_type if report_type in ("s", "c") else "s"
    url = EP["valuation_ratios"].format(co_code=val, t=t)
    data, err = _get(url, f"ValuationRatios[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No valuation ratio data found."
    lines = [f"Valuation Ratios [{'Standalone' if t=='s' else 'Consolidated'}]:"]
    for row in rows[:5]:
        p = _pick(row, ["Year", "YRC", "PE", "PB", "EVEBITDAMultiple",
                        "PriceSales", "DivYield", "MCAP", "EV"])
        yr = p.get("Year") or p.get("YRC", "N/A")
        lines.append(
            f"  {yr}  |  PE: {p.get('PE','N/A')}x"
            f"  PB: {p.get('PB','N/A')}x"
            f"  EV/EBITDA: {p.get('EVEBITDAMultiple','N/A')}x"
            f"  Div Yield: {p.get('DivYield','N/A')}%"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get return ratios: ROE, ROCE, ROA, ROIC across years. "
    "REQUIRES co_code. report_type: 's' or 'c'."
))
def get_return_ratios(co_code: int, report_type: str = "s") -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    t = report_type if report_type in ("s", "c") else "s"
    url = EP["return_ratios"].format(co_code=val, t=t)
    data, err = _get(url, f"ReturnRatios[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No return ratio data found."
    lines = [f"Return Ratios [{'Standalone' if t=='s' else 'Consolidated'}]:"]
    for row in rows[:5]:
        p = _pick(row, ["Year", "YRC", "ROE", "ROCE", "ROA", "ROIC"])
        yr = p.get("Year") or p.get("YRC", "N/A")
        lines.append(
            f"  {yr}  |  ROE: {p.get('ROE','N/A')}%"
            f"  ROCE: {p.get('ROCE','N/A')}%"
            f"  ROA: {p.get('ROA','N/A')}%"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get growth ratios: Revenue growth, PAT growth, EPS growth across years. "
    "REQUIRES co_code. report_type: 's' or 'c'."
))
def get_growth_ratios(co_code: int, report_type: str = "s") -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    t = report_type if report_type in ("s", "c") else "s"
    url = EP["growth_ratio"].format(co_code=val, t=t)
    data, err = _get(url, f"GrowthRatios[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No growth ratio data found."
    lines = [f"Growth Ratios [{'Standalone' if t=='s' else 'Consolidated'}]:"]
    for row in rows[:5]:
        p = _pick(row, ["Year", "YRC", "NetSalesGrowth", "PATGrowth", "EPSGrowth",
                        "EBITDAGrowth", "TotalIncomeGrowth"])
        yr = p.get("Year") or p.get("YRC", "N/A")
        lines.append(
            f"  {yr}  |  Rev Growth: {p.get('NetSalesGrowth','N/A')}%"
            f"  PAT Growth: {p.get('PATGrowth','N/A')}%"
            f"  EPS Growth: {p.get('EPSGrowth','N/A')}%"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get liquidity ratios: Current Ratio, Quick Ratio, Cash Ratio. "
    "REQUIRES co_code. report_type: 's' or 'c'."
))
def get_liquidity_ratios(co_code: int, report_type: str = "s") -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    t = report_type if report_type in ("s", "c") else "s"
    url = EP["liquidity_ratios"].format(co_code=val, t=t)
    data, err = _get(url, f"LiquidityRatios[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No liquidity ratio data found."
    lines = [f"Liquidity Ratios [{'Standalone' if t=='s' else 'Consolidated'}]:"]
    for row in rows[:5]:
        p = _pick(row, ["Year", "YRC", "CurrentRatio", "QuickRatio", "CashRatio"])
        yr = p.get("Year") or p.get("YRC", "N/A")
        lines.append(
            f"  {yr}  |  Current: {p.get('CurrentRatio','N/A')}x"
            f"  Quick: {p.get('QuickRatio','N/A')}x"
            f"  Cash: {p.get('CashRatio','N/A')}x"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get solvency/leverage ratios: D/E Ratio, Interest Coverage, Debt/EBITDA. "
    "REQUIRES co_code. report_type: 's' or 'c'."
))
def get_solvency_ratios(co_code: int, report_type: str = "s") -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    t = report_type if report_type in ("s", "c") else "s"
    url = EP["solvency_ratios"].format(co_code=val, t=t)
    data, err = _get(url, f"SolvencyRatios[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No solvency ratio data found."
    lines = [f"Solvency / Leverage Ratios [{'Standalone' if t=='s' else 'Consolidated'}]:"]
    for row in rows[:5]:
        p = _pick(row, ["Year", "YRC", "DERatio", "InterestCoverage", "DebtEBITDA",
                        "TotalDebtEquity", "LTDebtEquity"])
        yr = p.get("Year") or p.get("YRC", "N/A")
        lines.append(
            f"  {yr}  |  D/E: {p.get('DERatio','N/A')}x"
            f"  Int Coverage: {p.get('InterestCoverage','N/A')}x"
            f"  Debt/EBITDA: {p.get('DebtEBITDA','N/A')}x"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get efficiency ratios: Asset Turnover, Inventory Turnover, Receivables Turnover, Working Capital Days. "
    "REQUIRES co_code. report_type: 's' or 'c'."
))
def get_efficiency_ratios(co_code: int, report_type: str = "s") -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    t = report_type if report_type in ("s", "c") else "s"
    url = EP["efficiency_ratios"].format(co_code=val, t=t)
    data, err = _get(url, f"EfficiencyRatios[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No efficiency ratio data found."
    lines = [f"Efficiency Ratios [{'Standalone' if t=='s' else 'Consolidated'}]:"]
    for row in rows[:5]:
        p = _pick(row, ["Year", "YRC", "AssetTurnover", "InventoryTurnover",
                        "ReceivablesTurnover", "FixedAssetTurnover",
                        "DebtorDays", "InventoryDays", "CreditorDays"])
        yr = p.get("Year") or p.get("YRC", "N/A")
        lines.append(
            f"  {yr}  |  Asset T/O: {p.get('AssetTurnover','N/A')}x"
            f"  Inv T/O: {p.get('InventoryTurnover','N/A')}x"
            f"  Debtor Days: {p.get('DebtorDays','N/A')}"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get cash flow ratios: Operating CF/Sales, FCF Yield, Capex/Sales. "
    "REQUIRES co_code. report_type: 's' or 'c'."
))
def get_cashflow_ratios(co_code: int, report_type: str = "s") -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    t = report_type if report_type in ("s", "c") else "s"
    url = EP["cashflow_ratios"].format(co_code=val, t=t)
    data, err = _get(url, f"CashflowRatios[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No cash flow ratio data found."
    lines = [f"Cash Flow Ratios [{'Standalone' if t=='s' else 'Consolidated'}]:"]
    for row in rows[:5]:
        p = _pick(row, ["Year", "YRC", "OperatingCFSales", "FCFYield",
                        "CapexSales", "FreeCashFlow", "OperatingCF"])
        yr = p.get("Year") or p.get("YRC", "N/A")
        lines.append(
            f"  {yr}  |  OCF/Sales: {p.get('OperatingCFSales','N/A')}%"
            f"  FCF Yield: {p.get('FCFYield','N/A')}%"
            f"  Capex/Sales: {p.get('CapexSales','N/A')}%"
        )
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — STOCK FINANCIAL STATEMENTS
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.tool(description=(
    "Get quarterly P&L results: Revenue, EBITDA, PAT, EPS for recent quarters. "
    "REQUIRES co_code. report_type: 'S' = standalone (default), 'C' = consolidated."
))
def get_quarterly_results(co_code: int, report_type: str = "S") -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    t = report_type.upper() if report_type.upper() in ("S", "C") else "S"
    url = EP["quarterly_results"].format(co_code=val, t=t)
    data, err = _get(url, f"QuarterlyResults[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No quarterly results found."
    lines = [f"Quarterly Results [{'Standalone' if t=='S' else 'Consolidated'}]:"]
    for row in rows[:8]:
        p = _pick(row, ["QuarterEndDate", "NetSales", "EBITDA", "PAT", "EPS",
                        "EBITDAMargin", "PATMargin", "TotalIncome"])
        lines.append(
            f"  {str(p.get('QuarterEndDate',''))[:10]}"
            f"  NetSales: {p.get('NetSales','N/A')} Cr"
            f"  EBITDA: {p.get('EBITDA','N/A')} Cr"
            f"  PAT: {p.get('PAT','N/A')} Cr"
            f"  EPS: {p.get('EPS','N/A')}"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get annual Profit & Loss: Revenue, EBITDA, PAT, EPS for past years. "
    "REQUIRES co_code. report_type: 'S' or 'C'."
))
def get_profit_loss(co_code: int, report_type: str = "S") -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    t = report_type.upper() if report_type.upper() in ("S", "C") else "S"
    url = EP["profit_loss"].format(co_code=val, t=t)
    data, err = _get(url, f"P&L[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No P&L data found."
    lines = [f"Profit & Loss [{'Standalone' if t=='S' else 'Consolidated'}]:"]
    for row in rows[:6]:
        p = _pick(row, ["Year", "YRC", "NetSales", "TotalIncome", "EBITDA",
                        "EBIT", "PAT", "EPS", "EBITDAMargin", "PATMargin"])
        yr = p.get("Year") or p.get("YRC", "N/A")
        lines.append(
            f"  {yr}  |  NetSales: {p.get('NetSales','N/A')} Cr"
            f"  EBITDA: {p.get('EBITDA','N/A')} Cr"
            f"  PAT: {p.get('PAT','N/A')} Cr"
            f"  EPS: {p.get('EPS','N/A')}"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get annual Balance Sheet: Total Assets, Equity, Debt, Cash for past years. "
    "REQUIRES co_code. report_type: 'S' or 'C'."
))
def get_balance_sheet(co_code: int, report_type: str = "S") -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    t = report_type.upper() if report_type.upper() in ("S", "C") else "S"
    url = EP["balance_sheet"].format(co_code=val, t=t)
    data, err = _get(url, f"BalanceSheet[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No balance sheet data found."
    lines = [f"Balance Sheet [{'Standalone' if t=='S' else 'Consolidated'}]:"]
    for row in rows[:6]:
        p = _pick(row, ["Year", "YRC", "TotalAssets", "TotalEquity", "TotalDebt",
                        "CashandCashEquivalents", "NetWorth", "BookValue", "DERatio"])
        yr = p.get("Year") or p.get("YRC", "N/A")
        lines.append(
            f"  {yr}  |  Assets: {p.get('TotalAssets','N/A')} Cr"
            f"  Equity: {p.get('TotalEquity','N/A')} Cr"
            f"  Debt: {p.get('TotalDebt','N/A')} Cr"
            f"  Cash: {p.get('CashandCashEquivalents','N/A')} Cr"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get annual Cash Flow Statement: Operating CF, Investing CF, FCF. "
    "REQUIRES co_code. report_type: 'S' or 'C'."
))
def get_cash_flow(co_code: int, report_type: str = "S") -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    t = report_type.upper() if report_type.upper() in ("S", "C") else "S"
    url = EP["cash_flow"].format(co_code=val, t=t)
    data, err = _get(url, f"CashFlow[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No cash flow data found."
    lines = [f"Cash Flow Statement [{'Standalone' if t=='S' else 'Consolidated'}]:"]
    for row in rows[:6]:
        p = _pick(row, ["Year", "YRC", "OperatingCF", "InvestingCF",
                        "FinancingCF", "FreeCashFlow", "Capex"])
        yr = p.get("Year") or p.get("YRC", "N/A")
        lines.append(
            f"  {yr}  |  Operating: {p.get('OperatingCF','N/A')} Cr"
            f"  Investing: {p.get('InvestingCF','N/A')} Cr"
            f"  FCF: {p.get('FreeCashFlow','N/A')} Cr"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get quarterly revenue, EBITDA, and PAT trends. "
    "REQUIRES co_code — call resolve_nse_symbol first."
))
def get_quarterly_trends(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    url = EP["q_trend_revenue"].format(co_code=val)
    data, err = _get(url, f"QTrend[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No quarterly trend data found."
    lines = ["Quarterly Trends (Revenue / EBITDA / PAT):"]
    for row in rows[:8]:
        p = _pick(row, ["QuarterEndDate", "NetSales", "EBITDA", "PAT"])
        lines.append(
            f"  {str(p.get('QuarterEndDate',''))[:10]}"
            f"  Rev: {p.get('NetSales','N/A')} Cr"
            f"  EBITDA: {p.get('EBITDA','N/A')} Cr"
            f"  PAT: {p.get('PAT','N/A')} Cr"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get detailed shareholding pattern: promoter %, FII %, DII %, public % over quarters. "
    "REQUIRES co_code — call resolve_nse_symbol first."
))
def get_shareholding_pattern(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    url = EP["shareholding_detailed"].format(co_code=val)
    data, err = _get(url, f"Shareholding[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No shareholding data found."
    lines = ["Shareholding Pattern:"]
    for row in rows[:4]:
        p = _pick(row, ["QuarterEndDate", "PromoterHolding", "FIIHolding",
                        "DIIHolding", "PublicHolding"])
        lines.append(
            f"  {str(p.get('QuarterEndDate',''))[:10]}"
            f"  Promoter: {p.get('PromoterHolding','N/A')}%"
            f"  FII: {p.get('FIIHolding','N/A')}%"
            f"  DII: {p.get('DIIHolding','N/A')}%"
            f"  Public: {p.get('PublicHolding','N/A')}%"
        )
    return "\n".join(lines)


@mcp.tool(description=(
    "Get shareholders owning more than 1% stake. "
    "REQUIRES co_code — call resolve_nse_symbol first."
))
def get_major_shareholders(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    url = EP["shareholding_1pct"].format(co_code=val)
    data, err = _get(url, f"MajorShareholders[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No major shareholder data found."
    lines = ["Shareholders > 1%:"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, ["ShareHolderName", "PercentHolding", "NoOfShares", "Category"])
        lines.append(
            f"  {i:>3}. {p.get('ShareHolderName','N/A')}"
            f"  [{p.get('Category','')}]"
            f"  {p.get('PercentHolding','N/A')}%"
        )
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — COMPANY PROFILE & GOVERNANCE
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.tool(description=(
    "Get detailed company profile: incorporation date, chairman, auditor, "
    "address, face value, website, phone. "
    "REQUIRES co_code — call resolve_nse_symbol first."
))
def get_company_profile(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    url = EP["company_profile"].format(co_code=val)
    data, err = _get(url, f"CompanyProfile[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No company profile data found."
    r = rows[0]
    p = _pick(r, ["lname", "isin", "inc_dt", "chairman", "auditor", "fv", "mkt_lot",
                  "ho_add1", "ho_city", "ho_statename", "email", "internet", "ind_l_name", "tel1"])
    return (
        f"Company Profile:\n"
        f"  Company         : {p.get('lname','N/A')}\n"
        f"  ISIN            : {p.get('isin','N/A')}\n"
        f"  Incorporated    : {p.get('inc_dt','N/A')}\n"
        f"  Industry        : {p.get('ind_l_name','N/A')}\n"
        f"  Chairman        : {p.get('chairman','N/A')}\n"
        f"  Auditor         : {p.get('auditor','N/A')}\n"
        f"  Face Value      : {p.get('fv','N/A')}\n"
        f"  Market Lot      : {p.get('mkt_lot','N/A')}\n"
        f"  Address         : {p.get('ho_add1','')}, {p.get('ho_city','')}, {p.get('ho_statename','')}\n"
        f"  Email           : {p.get('email','N/A')}\n"
        f"  Website         : {p.get('internet','N/A')}\n"
        f"  Phone           : {p.get('tel1','N/A')}"
    )


@mcp.tool(description=(
    "Get company business background / description. "
    "REQUIRES co_code — call resolve_nse_symbol first."
))
def get_company_background(co_code: int) -> str:
    val, err = _require_int(co_code, "co_code", "resolve_nse_symbol")
    if err:
        return err
    url = EP["comp_background"].format(co_code=val)
    data, err = _get(url, f"CompBackground[{val}]")
    if err:
        return err
    rows = _rows(data)
    if not rows:
        return "No company background data found."
    r = rows[0]
    # Return the text fields useful for the LLM to answer
    p = _pick(r, ["lname", "ind_l_name", "isin", "memo", "background"])
    return (
        f"Company: {p.get('lname','N/A')} | Industry: {p.get('ind_l_name','N/A')}\n"
        f"{p.get('memo') or p.get('background','Background text available from API.')}"
    )


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
    "Get management team biodata / key executive descriptions. "
    "REQUIRES co_code — call resolve_nse_symbol first."
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
    lname = rows[0].get("lname", "N/A")
    lines = [f"Management Team — {lname}:"]
    for i, row in enumerate(rows, 1):
        p = _pick(row, ["ShortDescription", "memo"])
        lines.append(f"  {i:>3}. {p.get('ShortDescription','N/A')}")
    return "\n".join(lines)


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
        p = _pick(row, ["product_name", "unit", "capacity", "production", "sales"])
        lines.append(
            f"  {i:>3}. {p.get('product_name','N/A')}"
            f"  ({p.get('unit','')})"
            f"  Capacity: {p.get('capacity','N/A')}"
            f"  Production: {p.get('production','N/A')}"
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
        p = _pick(row, ["raw_material", "unit", "consumption", "value"])
        lines.append(
            f"  {i:>3}. {p.get('raw_material','N/A')}"
            f"  ({p.get('unit','')})"
            f"  Consumption: {p.get('consumption','N/A')}"
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
    "Get today's NAV: current NAV, previous NAV, change, % change. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
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
    "Get investment details: minimum investment, scheme objective, tax treatment. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
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
    "Get expense ratio, entry/exit loads for a scheme. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
))
def get_expense_ratio(mf_schcode: int) -> str:
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
    "Get average maturity, modified duration, YTM for DEBT fund schemes. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
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
    "Get historical AUM (Assets Under Management) for a scheme. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
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
    "Get historical NAV for a scheme. "
    "period: 'M' (months) or 'Y' (years). periodval: number of periods. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
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
        pr = _pick(row, ["NavDate", "NAVRS"])
        lines.append(f"  {pr.get('NavDate','N/A')}: {pr.get('NAVRS','N/A')}")
    return "\n".join(lines)


@mcp.tool(description=(
    "Get scheme returns: 1W/1M/3M/6M/1Y/3Y/5Y/Since Inception with benchmark comparison. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
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
    "Get lumpsum return details: value and % return across 1W/1M/3M/6M/1Y/3Y/5Y/10Y/Since Inception. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
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
    "Compare multiple schemes side-by-side: NAV, AUM, returns, fund manager, exit load. "
    "REQUIRES comma-separated mf_schcodes. Use resolve_mf_scheme for each."
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
    "Get top-performing funds by type and category. "
    "fund_type: 'Equity' / 'Debt' / 'Hybrid'. category: 'all' or specific category."
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


@mcp.tool(description="Get average category returns for Equity/Debt/Hybrid across time periods.")
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


@mcp.tool(description="Get SIP/SWP/STP available dates and minimum amounts. plan: 'SIP' / 'SWP' / 'STP'.")
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
    "Get SIP/SWP dates and minimum amounts for a specific scheme. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
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
    "Get top stock holdings of a fund with % holding, market value, sector. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
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
    "Get all mutual funds holding a specific stock. "
    "REQUIRES co_code — use resolve_nse_symbol first."
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
    "Get sector allocation (IT/Banking/Pharma %) for a fund. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
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
    "Get asset allocation (equity/debt/cash %) for a fund. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
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
    "Get stocks recently added to or removed from a fund portfolio. "
    "move_type: 'in' (new buys) / 'out' (sold) / 'un' (unchanged). "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
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
    "Get large/mid/small-cap allocation % for an equity fund. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
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
    "Get stocks most bought or sold by a fund this month. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
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


@mcp.tool(description="Get currently open New Fund Offers (NFO) with objective, min investment, close date.")
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


@mcp.tool(description="Get overall MF market activity: gross equity/debt purchases and net flows.")
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
    "Get risk ratios: Beta, Alpha, Sharpe, Std Dev, Treynor for a scheme. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
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
    "Get recent dividend announcements for a scheme. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
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
    "Get latest MF industry news. "
    "sno: '-' for latest batch, or a serial number for a specific article."
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
    "Get BSE Star platform transaction details for a scheme: modes, SIP/SWP flags, settlement. "
    "REQUIRES mf_schcode — call resolve_mf_scheme first."
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


@mcp.tool(description="Get AMFI code and ISIN mapping for a scheme. Pass mf_schcode to filter.")
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


# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    mcp.run()
"""
equifiz_client.py – Thin wrapper around all 9 EQUIFIZ REST endpoints.

Every public function returns a plain Python dict/list (already parsed JSON).
Auth is handled via the Bearer token in config.py.
"""

from __future__ import annotations
import logging
from typing import Any, Optional

import requests

from config.config import  EQUIFIZ_ENDPOINTS, CMOTS_TOKEN

logger = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({
    "Authorization": f"Bearer {CMOTS_TOKEN}",
    "Content-Type":  "application/json",
})


def _get(url: str, timeout: int = 15) -> Any:
    try:
        resp = SESSION.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error {e.response.status_code} for {url}")
        raise
    except Exception as e:
        logger.error(f"Request failed for {url}: {e}")
        raise


# ── 1. Company Master ────────────────────────────────────────────────────────

def fetch_company_master() -> list[dict]:
    """Full list of all listed companies (BSE + NSE)."""
    return _get(EQUIFIZ_ENDPOINTS["company_master"])


# ── 2. Quarterly Results ─────────────────────────────────────────────────────

def fetch_quarterly_results(co_code: int, report_type: str = "s") -> dict:
    """
    co_code    : CMOTS company code
    report_type: 's' = Standalone, 'c' = Consolidated
    """
    url = EQUIFIZ_ENDPOINTS["quarterly_results"].format(
        co_code=co_code, type=report_type
    )
    return _get(url)


# ── 3. Profit & Loss ─────────────────────────────────────────────────────────

def fetch_profit_and_loss(co_code: int, report_type: str = "s") -> dict:
    url = EQUIFIZ_ENDPOINTS["profit_and_loss"].format(
        co_code=co_code, type=report_type
    )
    return _get(url)


# ── 4. Balance Sheet ─────────────────────────────────────────────────────────

def fetch_balance_sheet(co_code: int, report_type: str = "s") -> dict:
    url = EQUIFIZ_ENDPOINTS["balance_sheet"].format(
        co_code=co_code, type=report_type
    )
    return _get(url)


# ── 5. Key Financial Ratios ──────────────────────────────────────────────────

def fetch_key_financial_ratios(co_code: int, report_type: str = "s") -> dict:
    url = EQUIFIZ_ENDPOINTS["key_financial_ratios"].format(
        co_code=co_code, type=report_type
    )
    return _get(url)


# ── 6. Daily Ratios (PE, MCAP, EPS, etc.) ───────────────────────────────────

def fetch_daily_ratios(co_code: int, report_type: str = "s") -> dict:
    """Real-time / EOD valuation metrics: PE, MCAP, EPS, PB, Div Yield…"""
    url = EQUIFIZ_ENDPOINTS["daily_ratios"].format(co_code=co_code,type=report_type)
    return _get(url)


# ── 7. Fund House / AMCs ─────────────────────────────────────────────────────

def fetch_fund_houses() -> list[dict]:
    return _get(EQUIFIZ_ENDPOINTS["fund_house"])


# ── 8. Scheme Master ─────────────────────────────────────────────────────────

def fetch_scheme_master(mf_cocode: int) -> list[dict]:
    url = EQUIFIZ_ENDPOINTS["scheme_master"].format(mf_cocode=mf_cocode)
    return _get(url)


# ── 9. Scheme Holding ────────────────────────────────────────────────────────

def fetch_scheme_holding(mf_cocode: int) -> list[dict]:
    url = EQUIFIZ_ENDPOINTS["scheme_holding"].format(mf_cocode=mf_cocode)
    return _get(url)

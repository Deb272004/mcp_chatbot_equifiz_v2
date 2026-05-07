"""
config/config.py — Central configuration for the EQUIFIZ Financial AI Agent.
Loads from .env; all secrets come from environment variables.
"""

import os
import sys
from dotenv import load_dotenv

load_dotenv()

# ─── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR         = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHROMA_DIR       = os.environ.get("CHROMA_DIR",       os.path.join(BASE_DIR, "chroma_store"))
UPLOAD_DIR       = os.environ.get("UPLOAD_DIR",       os.path.join(BASE_DIR, "data", "uploads"))
FASTEMBED_CACHE  = os.environ.get("FASTEMBED_CACHE",  os.path.join(BASE_DIR, ".fastembed_cache"))
LOG_DIR          = os.environ.get("LOG_DIR",           os.path.join(BASE_DIR, "logs"))

# ─── ChromaDB ─────────────────────────────────────────────────────────────────
CHROMA_PERSIST_DIR = CHROMA_DIR          # alias used by graph.py / vector_store.py
STATIC_COLLECTION  = "static_knowledge"
LIVE_COLLECTION    = "live_knowledge"
CHROMA_COLLECTION  = STATIC_COLLECTION    # default collection for vector_store.py

# ─── Embedding ────────────────────────────────────────────────────────────────
EMBEDDING_MODEL  = os.environ.get("EMBEDDING_MODEL", "BAAI/bge-large-en-v1.5")

# Ollama embedding (used by graph.py / vector_store.py when Ollama is active)
OLLAMA_BASE_URL      = os.environ.get("OLLAMA_BASE_URL", "http://ollama:11434")
OLLAMA_EMBEDDING_MODEL = os.environ.get("OLLAMA_EMBEDDING_MODEL", "embeddinggemma:latest")

# ─── Chunking ─────────────────────────────────────────────────────────────────
STATIC_CHUNK_SIZE    = int(os.environ.get("STATIC_CHUNK_SIZE",    "700"))
STATIC_CHUNK_OVERLAP = int(os.environ.get("STATIC_CHUNK_OVERLAP", "100"))
LIVE_CHUNK_SIZE      = int(os.environ.get("LIVE_CHUNK_SIZE",      "500"))
LIVE_CHUNK_OVERLAP   = int(os.environ.get("LIVE_CHUNK_OVERLAP",   "70"))

# ─── Retrieval ────────────────────────────────────────────────────────────────
TOP_K = int(os.environ.get("TOP_K", "6"))

# ─── LLM provider ─────────────────────────────────────────────────────────────
def _detect_llm_provider() -> str:
    explicit = os.environ.get("LLM_PROVIDER", "").lower()
    if explicit in ("groq", "ollama", "anthropic"):
        return explicit
    if os.environ.get("GROQ_API_KEY"):
        return "groq"
    if os.environ.get("OLLAMA_BASE_URL"):
        return "ollama"
    if os.environ.get("ANTHROPIC_API_KEY"):
        return "anthropic"
    return "groq"

LLM_PROVIDER = _detect_llm_provider()

# ── Groq ──────────────────────────────────────────────────────────────────────
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
GROQ_MODEL   = os.environ.get("GROQ_MODEL",   "llama-3.3-70b-versatile")
GROQ_TEMP    = float(os.environ.get("GROQ_TEMP", "0.0"))

# ── Ollama ────────────────────────────────────────────────────────────────────
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "gemma2:2b")

# ── Anthropic ─────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL   = os.environ.get("ANTHROPIC_MODEL",   "claude-sonnet-4-20250514")

MAX_TOKENS = int(os.environ.get("MAX_TOKENS", "1024"))

# ─── PostgreSQL ───────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST",     "localhost"),
    "port":     int(os.environ.get("POSTGRES_PORT", "5432")),
    "dbname":   os.environ.get("POSTGRES_DB",       "equifiz"),
    "user":     os.environ.get("POSTGRES_USER",     "postgres"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}

# ─── CMOTS / EQUIFIZ Live API ─────────────────────────────────────────────────
CMOTS_TOKEN    = os.environ.get("CMOTS_TOKEN",os.environ.get("EQUIFIZ_TOKEN", "yJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1bmlxdWVfbmFtZSI6InRvcnVzIiwicm9sZSI6IkFkbWluIiwibmJmIjoxNzc0NTA4MDE5LCJleHAiOjE3Nzc1MzIwMTksImlhdCI6MTc3NDUwODAxOSwiaXNzIjoiaHR0cDovL2xvY2FsaG9zdDo1MDE5MSIsImF1ZCI6Imh0dHA6Ly9sb2NhbGhvc3Q6NTAxOTEifQ.-xqrjR-QievVJXwkR3cSO7rvfT43EKEeCBfyr7r92Tk"))
EQUIFIZ_TOKEN  = CMOTS_TOKEN  # alias
CMOTS_BASE     = "https://equifizapis.cmots.com/api"
EQUIFIZ_BASE   = CMOTS_BASE   # alias

LIVE_APIS = {
    # Master data
    "company_master":       f"{CMOTS_BASE}/CompanyMaster",
    "fund_houses":          "http://jwttoken.cmots.com/EQUIFIZ/api/Fund_House",

    # Standalone
    "quarterly_results_s":  f"{CMOTS_BASE}/QuarterlyResults/{{co_code}}/s",
    "profit_loss_s":        f"{CMOTS_BASE}/ProftandLoss/{{co_code}}/s",
    "balance_sheet_s":      f"{CMOTS_BASE}/BalanceSheet/{{co_code}}/s",
    "key_ratios_s":         f"{CMOTS_BASE}/KeyFinancialRatios/{{co_code}}/s",
    "daily_ratios_s":       f"{CMOTS_BASE}/DailyRatios/{{co_code}}/s",

    # Consolidated
    "quarterly_results_c":  f"{CMOTS_BASE}/QuarterlyResults/{{co_code}}/c",
    "profit_loss_c":        f"{CMOTS_BASE}/ProftandLoss/{{co_code}}/c",
    "balance_sheet_c":      f"{CMOTS_BASE}/BalanceSheet/{{co_code}}/c",
    "key_ratios_c":         f"{CMOTS_BASE}/KeyFinancialRatios/{{co_code}}/c",
    "daily_ratios_c":       f"{CMOTS_BASE}/DailyRatios/{{co_code}}/c",

    # Mutual funds
    "scheme_master":        "http://jwttoken.cmots.com/EQUIFIZ/api/SchemeMaster/{mf_cocode}",
    "scheme_holding":       "http://jwttoken.cmots.com/EQUIFIZ/api/SchemeHolding/{mf_cocode}",
}

# equifiz_client.py uses these names
EQUIFIZ_ENDPOINTS = {
    "company_master":       f"{EQUIFIZ_BASE}/CompanyMaster",
    "quarterly_results":    f"{EQUIFIZ_BASE}/QuarterlyResults/{{co_code}}/{{type}}",
    "profit_and_loss":      f"{EQUIFIZ_BASE}/ProftandLoss/{{co_code}}/{{type}}",
    "balance_sheet":        f"{EQUIFIZ_BASE}/BalanceSheet/{{co_code}}/{{type}}",
    "key_financial_ratios": f"{EQUIFIZ_BASE}/KeyFinancialRatios/{{co_code}}/{{type}}",
    "daily_ratios":         f"{EQUIFIZ_BASE}/DailyRatios/{{co_code}}/{{type}}",
    "fund_house":           "http://jwttoken.cmots.com/EQUIFIZ/api/Fund_House",
    "scheme_master":        "http://jwttoken.cmots.com/EQUIFIZ/api/SchemeMaster/{mf_cocode}",
    "scheme_holding":       "http://jwttoken.cmots.com/EQUIFIZ/api/SchemeHolding/{mf_cocode}",
}

# ─── Auth ─────────────────────────────────────────────────────────────────────
API_SECRET_KEY       = os.environ.get("API_SECRET_KEY", "")
FIREBASE_CREDENTIALS = os.environ.get("FIREBASE_CREDENTIALS", "")

# ─── CORS ─────────────────────────────────────────────────────────────────────
ALLOWED_ORIGINS_RAW = os.environ.get("ALLOWED_ORIGINS", "*")
ALLOWED_ORIGINS     = [o.strip() for o in ALLOWED_ORIGINS_RAW.split(",") if o.strip()] or ["*"]

# ─── Rate limiting ────────────────────────────────────────────────────────────
RATE_LIMIT_CHAT  = int(os.environ.get("RATE_LIMIT_CHAT",  "30"))
RATE_LIMIT_VOICE = int(os.environ.get("RATE_LIMIT_VOICE", "10"))

# ─── Upload limits ────────────────────────────────────────────────────────────
MAX_UPLOAD_MB = int(os.environ.get("MAX_UPLOAD_MB", "20"))

# ─── Redis (optional — for cross-worker rate limiting) ────────────────────────
REDIS_URL = os.environ.get("REDIS_URL", "")

# ─── Supported file extensions ────────────────────────────────────────────────
SUPPORTED_EXT = [".txt", ".md", ".pdf", ".csv", ".json", ".docx", ".xlsx", ".xls", ".pptx"]

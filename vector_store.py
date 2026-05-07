"""
vector_store.py – ChromaDB-backed vector store with Ollama embeddinggemma:latest.

Stores:
  • Sector PE averages and descriptions
  • Analyst commentary snippets
  • Financial ratio definitions / benchmarks
  • Historical summary blurbs generated from past API responses
"""

from __future__ import annotations
import logging
from typing import Optional

import chromadb
from chromadb.config import Settings
import requests

from config.config import (
    CHROMA_PERSIST_DIR,
    CHROMA_COLLECTION,
    OLLAMA_BASE_URL,
    EMBEDDING_MODEL,
)

logger = logging.getLogger(__name__)



from chromadb.api.types import Documents, Embeddings
from fastembed import TextEmbedding
import numpy as np

class FastEmbedEmbeddingFunction(chromadb.EmbeddingFunction):
    """Uses FastEmbed for local embeddings."""
    
    def __init__(self, model_name: str = "BAAI/bge-large-en-v1.5"):
        """
        Initialize FastEmbed embedding function.
        
        Args:
            model_name: FastEmbed model name (default: BAAI/bge-large-en-v1.5)
        """
        self.model = TextEmbedding(model_name=model_name, threads=4)
    
    def __call__(self, input: Documents) -> Embeddings:
        """
        Generate embeddings for input documents.
        
        Args:
            input: List of text documents
            
        Returns:
            List of embedding vectors
        """
        embeddings = []
        
        # FastEmbed supports batch processing
        batch_embeddings = list(self.model.embed(input))
        
        for emb in batch_embeddings:
            embeddings.append(emb.tolist())  # Convert to list for ChromaDB
            
        return embeddings

# ── ChromaDB client & collection ─────────────────────────────────────────────

_client: Optional[chromadb.PersistentClient] = None
_collection = None

embedding_function = FastEmbedEmbeddingFunction()

def _get_collection():
    global _client, _collection
    if _collection is None:
        _client = chromadb.PersistentClient(
            path=CHROMA_PERSIST_DIR,
            settings=Settings(anonymized_telemetry=False),
        )

        # TEMP: run once then remove
        try:
            _client.delete_collection(CHROMA_COLLECTION)
        except:
            pass

        _collection = _client.get_or_create_collection(
            name=CHROMA_COLLECTION,
            embedding_function=embedding_function,
            metadata={"hnsw:space": "cosine"},
        )

    return _collection

# def _get_collection():
#     global _client, _collection
#     if _collection is None:
#         _client = chromadb.PersistentClient(
#             path=CHROMA_PERSIST_DIR,
#             settings=Settings(anonymized_telemetry=False),
#         )
#         _collection = _client.get_or_create_collection(
#             name=CHROMA_COLLECTION,
#             embedding_function=FastEmbedEmbeddingFunction(),
#             metadata={"hnsw:space": "cosine"},
#         )
#         logger.info(f"ChromaDB collection '{CHROMA_COLLECTION}' ready.")
#     return _collection

def embed(texts: list[str]) -> list[list[float]]:
    """Generate embeddings for a list of texts using the Ollama embedding model."""
    fn = FastEmbedEmbeddingFunction()
    return fn(texts)

# ── Public API ────────────────────────────────────────────────────────────────

def add_documents(
    docs: list[str],
    metadatas: list[dict],
    ids: list[str],
):
    """Add or update documents in the vector store."""
    col = _get_collection()
    col.upsert(documents=docs, metadatas=metadatas, ids=ids)
    logger.info(f"Upserted {len(docs)} documents into ChromaDB.")


def query(
    query_text: str,
    n_results: int = 5,
    where: Optional[dict] = None,
) -> list[dict]:
    """
    Semantic search.  Returns list of {document, metadata, distance} dicts.
    """
    col = _get_collection()
    kwargs: dict = {"query_texts": [query_text], "n_results": n_results}
    if where:
        kwargs["where"] = where

    results = col.query(**kwargs)

    output = []
    for doc, meta, dist in zip(
        results["documents"][0],
        results["metadatas"][0],
        results["distances"][0],
    ):
        output.append({"document": doc, "metadata": meta, "distance": dist})
    return output


# ── Seed financial knowledge ─────────────────────────────────────────────────

SEED_KNOWLEDGE = [
    # Sector PE benchmarks (approximate, as of 2025)
    {
        "id": "sector_pe_it",
        "doc": "Indian IT sector average PE ratio is around 25-35x. Companies like TCS, Infosys, Wipro trade at premium multiples due to high ROE and stable cash flows.",
        "meta": {"type": "sector_benchmark", "sector": "IT"},
    },
    {
        "id": "sector_pe_banking",
        "doc": "Banking sector PE in India typically ranges from 10-20x. Private banks like HDFC Bank, ICICI Bank command premium valuations over PSU banks.",
        "meta": {"type": "sector_benchmark", "sector": "Banking"},
    },
    {
        "id": "sector_pe_fmcg",
        "doc": "FMCG sector PE in India is typically 40-60x, reflecting stable earnings, strong brands, and high dividend payouts. Hindustan Unilever, Nestle India, ITC are key players.",
        "meta": {"type": "sector_benchmark", "sector": "FMCG"},
    },
    {
        "id": "sector_pe_pharma",
        "doc": "Pharma sector average PE is 20-35x. Sun Pharma, Dr Reddy's, Cipla are large caps. US FDA approvals and ANDA filings drive re-rating.",
        "meta": {"type": "sector_benchmark", "sector": "Pharma"},
    },
    {
        "id": "sector_pe_auto",
        "doc": "Auto sector PE is typically 15-25x. EV transition is a key theme for Tata Motors, Mahindra, Hero MotoCorp. Maruti Suzuki dominates passenger vehicles.",
        "meta": {"type": "sector_benchmark", "sector": "Auto"},
    },
    {
        "id": "sector_pe_realty",
        "doc": "Real estate sector PE in India ranges 20-40x. DLF, Godrej Properties, Prestige Estates are key names. RERA compliance and affordability trends matter.",
        "meta": {"type": "sector_benchmark", "sector": "Realty"},
    },
    {
        "id": "sector_pe_metal",
        "doc": "Metals and mining sector PE in India is cyclical, typically 8-15x. Tata Steel, JSW Steel, Hindalco are key companies. Global commodity prices drive earnings.",
        "meta": {"type": "sector_benchmark", "sector": "Metals"},
    },
    # Ratio definitions
    {
        "id": "ratio_pe",
        "doc": "PE Ratio (Price-to-Earnings) = Market Price per Share / EPS. A high PE implies growth expectations; compare within sector. Nifty50 historical average PE is ~20x.",
        "meta": {"type": "ratio_definition", "ratio": "PE"},
    },
    {
        "id": "ratio_pb",
        "doc": "PB Ratio (Price-to-Book) = Market Price / Book Value per Share. Banks and financials are often valued on PB. PB < 1 may indicate undervaluation.",
        "meta": {"type": "ratio_definition", "ratio": "PB"},
    },
    {
        "id": "ratio_mcap",
        "doc": "Market Cap (MCAP) = Current Share Price × Total Outstanding Shares. Large cap > ₹20,000 Cr, Mid cap ₹5,000–20,000 Cr, Small cap < ₹5,000 Cr (SEBI classification).",
        "meta": {"type": "ratio_definition", "ratio": "MCAP"},
    },
    {
        "id": "ratio_eps",
        "doc": "EPS (Earnings Per Share) = Net Profit / Total Shares. Trailing EPS uses last 12 months actuals; forward EPS uses consensus estimates. EPS growth drives re-rating.",
        "meta": {"type": "ratio_definition", "ratio": "EPS"},
    },
    {
        "id": "ratio_roe",
        "doc": "ROE (Return on Equity) = Net Profit / Shareholders Equity × 100. ROE > 15% is generally considered healthy. Consistent high ROE indicates competitive moat.",
        "meta": {"type": "ratio_definition", "ratio": "ROE"},
    },
    {
        "id": "ratio_dividend_yield",
        "doc": "Dividend Yield = Annual Dividend per Share / Market Price × 100. Coal India, ITC, ONGC are high dividend yield stocks in India. PSUs often have higher yields.",
        "meta": {"type": "ratio_definition", "ratio": "DivYield"},
    },
    # General market knowledge
    {
        "id": "nifty_index",
        "doc": "Nifty 50 is the benchmark index of NSE comprising 50 large-cap Indian companies. Sensex is BSE equivalent with 30 companies. Both reflect broad market sentiment.",
        "meta": {"type": "market_knowledge", "topic": "index"},
    },
    {
        "id": "fno_stocks",
        "doc": "F&O (Futures & Options) stocks are those permitted for derivative trading on NSE. These are typically large/mid cap with sufficient liquidity. About 200+ stocks are in F&O.",
        "meta": {"type": "market_knowledge", "topic": "derivatives"},
    },
    {
        "id": "mf_categories",
        "doc": "SEBI mutual fund categories: Large Cap, Mid Cap, Small Cap, Multi Cap, Flexi Cap, ELSS, Debt (liquid, short duration, gilt), Hybrid (balanced advantage, aggressive). NAV declared daily.",
        "meta": {"type": "market_knowledge", "topic": "mutual_funds"},
    },
]


def seed_knowledge():
    """Load default financial knowledge into ChromaDB (idempotent)."""
    docs  = [k["doc"]  for k in SEED_KNOWLEDGE]
    metas = [k["meta"] for k in SEED_KNOWLEDGE]
    ids   = [k["id"]   for k in SEED_KNOWLEDGE]
    add_documents(docs, metas, ids)
    logger.info(f"Seeded {len(docs)} financial knowledge documents.")


def add_api_response_summary(co_code: int, summary: str, data_type: str):
    """Store a generated natural-language summary of an API response for future retrieval."""
    doc_id = f"api_summary_{data_type}_{co_code}"
    add_documents(
        docs=[summary],
        metadatas=[{"type": "api_summary", "co_code": co_code, "data_type": data_type}],
        ids=[doc_id],
    )
    
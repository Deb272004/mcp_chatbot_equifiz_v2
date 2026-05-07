"""
seed_db.py — Run once after first deploy to seed ChromaDB with financial knowledge.

Usage:
    python seed_db.py

This script:
  1. Initialises the ChromaDB collection.
  2. Seeds sector benchmarks, ratio definitions, and market knowledge.
  3. Verifies the PostgreSQL company master table is reachable.
"""

import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
logger = logging.getLogger("seed_db")


def seed_vector_store():
    logger.info("Seeding ChromaDB financial knowledge...")
    try:
        import vector_store as vs
        vs.seed_knowledge()
        count = vs._get_collection().count()
        logger.info(f"ChromaDB ready — {count} documents in collection.")
    except Exception as e:
        logger.error(f"ChromaDB seed failed: {e}")
        sys.exit(1)


def check_postgres():
    logger.info("Checking PostgreSQL company master table...")
    try:
        import db
        count = db.init_db()
        logger.info(f"PostgreSQL OK — {count:,} companies in master table.")
    except Exception as e:
        logger.warning(
            f"PostgreSQL check failed: {e}\n"
            "Make sure the 'companies' table is populated before running queries."
        )


if __name__ == "__main__":
    seed_vector_store()
    check_postgres()
    logger.info("Seed complete. You can now start the API: uvicorn main:app --host 0.0.0.0 --port 8000")

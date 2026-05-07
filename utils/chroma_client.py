import chromadb
from config.config import CHROMA_DIR, STATIC_COLLECTION, LIVE_COLLECTION

_client = None

def get_client() -> chromadb.PersistentClient:
    global _client
    if _client is None:
        _client = chromadb.PersistentClient(path=CHROMA_DIR)
    return _client

def get_static_collection():
    return get_client().get_or_create_collection(STATIC_COLLECTION)

def get_live_collection():
    return get_client().get_or_create_collection(LIVE_COLLECTION)

from pathlib import Path
from typing import Optional
import chromadb
from chromadb.utils import embedding_functions

_CHROMA_PATH = str(Path(__file__).parent / "chroma_db")
EMBED_MODEL  = "embeddinggemma:latest"
OLLAMA_HOST  = "http://localhost:11434"

# Fix: Use Optional instead of the pipe operator to avoid TypeError
_client: Optional[chromadb.PersistentClient] = None
_collection = None

def get_chroma_collection(name: str = "equifiz_tools"):
    global _client, _collection
    
    if _collection is not None:
        return _collection
        
    try:
        # Initializing the persistent client
        _client = chromadb.PersistentClient(path=_CHROMA_PATH)
        
        ollama_ef = embedding_functions.OllamaEmbeddingFunction(
            model_name=EMBED_MODEL,
            url=f"{OLLAMA_HOST}/api/embeddings",
        )
        
        # Accessing the collection
        _collection = _client.get_collection(name=name, embedding_function=ollama_ef)
        return _collection
        
    except Exception as e:
        print(f"Error initializing ChromaDB: {e}")
        return None
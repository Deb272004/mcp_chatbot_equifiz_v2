

from pathlib import Path
from typing import Optional
import chromadb
from chromadb.api.types import Documents, Embeddings
from fastembed import TextEmbedding
from config.config import EMBEDDING_MODEL

# --- Configuration ---
_CHROMA_PATH = str(Path(__file__).parent / "chroma_store")
EMBED_MODEL = EMBEDDING_MODEL  # Match the model used in your indexer

# --- FastEmbed Integration ---
class FastEmbedEmbeddingFunction(chromadb.EmbeddingFunction):
    """Uses FastEmbed for local embeddings inside the collection queries."""
    
    def __init__(self, model_name: str = EMBED_MODEL):
        # Initializing FastEmbed with 4 threads for quick query embedding generation
        self.model = TextEmbedding(model_name=model_name, threads=4)
    
    def __call__(self, input: Documents) -> Embeddings:
        # FastEmbed.embed returns a generator, convert vectors to standard Python lists
        return [emb.tolist() for emb in self.model.embed(input)]


# Global singleton references
_client: Optional[chromadb.PersistentClient] = None
_collection = None

def get_chroma_collection(name: str = "equifiz_tools"):
    """
    Returns the cached ChromaDB collection using FastEmbed.
    Initializes the client and embedding function on the first call.
    """
    global _client, _collection
    
    if _collection is not None:
        return _collection
        
    try:
        # Initializing the persistent client
        _client = chromadb.PersistentClient(path=_CHROMA_PATH)
        
        # Swapped Ollama for your local FastEmbed function
        fastembed_ef = FastEmbedEmbeddingFunction()
        
        # Accessing the collection
        _collection = _client.get_collection(name=name, embedding_function=fastembed_ef)
        return _collection
        
    except Exception as e:
        print(f"❌ Error initializing ChromaDB with FastEmbed: {e}")
        return None
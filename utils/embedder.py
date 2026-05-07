from fastembed import TextEmbedding
from config.config import EMBEDDING_MODEL, FASTEMBED_CACHE
from typing import List

_model = None

def get_model() -> TextEmbedding:
    global _model
    if _model is None:
        _model = TextEmbedding(model_name=EMBEDDING_MODEL, cache_dir=FASTEMBED_CACHE)
    return _model

def embed(texts: List[str]) -> List[List[float]]:
    return list(get_model().embed(texts, batch_size=32))

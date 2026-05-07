"""
Processes uploaded documents for the chat-with-doc feature.
Returns chunks that get injected into retrieval at query time.
"""
import os
import tempfile
from typing import List

from langchain_text_splitters import RecursiveCharacterTextSplitter
from docling.document_converter import DocumentConverter
from langchain_community.document_loaders import TextLoader

from config.config import STATIC_CHUNK_SIZE, STATIC_CHUNK_OVERLAP
from logger.logger import get_logger

logger = get_logger("DocProcessor")

splitter = RecursiveCharacterTextSplitter(
    chunk_size=STATIC_CHUNK_SIZE,
    chunk_overlap=STATIC_CHUNK_OVERLAP
)
converter = DocumentConverter()


def process_uploaded_file(file_bytes: bytes, filename: str) -> List[str]:
    """Save to temp file, extract text, return chunks."""
    ext = os.path.splitext(filename)[1].lower()
    with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
        tmp.write(file_bytes)
        tmp_path = tmp.name

    try:
        if ext == ".txt":
            loader = TextLoader(tmp_path, encoding="utf-8")
            text = "\n".join([d.page_content for d in loader.load()])
        else:
            result = converter.convert(tmp_path)
            text = result.document.export_to_markdown()
    except Exception as e:
        logger.error(f"Doc processing failed: {e}")
        return []
    finally:
        os.unlink(tmp_path)

    chunks = splitter.split_text(text)
    logger.info(f"Processed '{filename}' → {len(chunks)} chunks")
    return chunks

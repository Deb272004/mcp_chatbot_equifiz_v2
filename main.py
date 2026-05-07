"""
Equifiz AI — FastAPI backend
Run:  uvicorn main:app --host 0.0.0.0 --port 8000 --workers 1

Endpoints:
  GET  /health
  GET  /metrics
  POST /chat                   ← orchestrated via LangGraph pipeline
  POST /chat/stream            ← streaming version
  POST /voice
  POST /sessions
  GET  /sessions
  GET  /sessions/{sid}
  PATCH /sessions/{sid}
  DELETE /sessions/{sid}
  POST /sessions/{sid}/share
  GET  /sessions/shared/{token}
  POST /upload-doc/{sid}
  DELETE /upload-doc/{sid}
  POST /saved-queries
  GET  /saved-queries
  DELETE /saved-queries/{qid}
  POST /ingest/live
  POST /ingest/company/{co_code}
  GET  /companies/search
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import json
import uuid
import time
import tempfile
import threading
from contextlib import asynccontextmanager
from collections import defaultdict
from typing import Optional, List

from fastapi import (
    FastAPI, UploadFile, File, HTTPException,
    BackgroundTasks, Depends, Request, Query,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse
from pydantic import BaseModel, Field
from starlette.middleware.base import BaseHTTPMiddleware

# ── Agent pipeline ─────────────────────────────────────────────────────────────
from graph import run_query, get_graph

# ── Utils ─────────────────────────────────────────────────────────────────────
from utils.session_store import (
    new_session, get_session, get_session_for_user, list_sessions, add_message,
    delete_session, share_session, get_session_by_token,
    save_query, list_saved_queries, delete_saved_query,
    update_session_title,
)
from utils.doc_store import set_doc, get_doc, delete_doc
from utils.doc_processor import process_uploaded_file
from utils.auth_manager import get_current_user, auth_status
from utils.rate_limiter import RateLimiter

# ── Config ────────────────────────────────────────────────────────────────────
from config.config import (
    LLM_PROVIDER, GROQ_MODEL, OLLAMA_MODEL, ANTHROPIC_MODEL, OLLAMA_BASE_URL,
    ALLOWED_ORIGINS, RATE_LIMIT_CHAT, RATE_LIMIT_VOICE,
)

# ── DB (company lookups) ──────────────────────────────────────────────────────
import db as company_db

# ── Logger ────────────────────────────────────────────────────────────────────
from logger.logger import get_logger

logger = get_logger("API")

# ── In-process metrics ────────────────────────────────────────────────────────
_metrics: dict = defaultdict(int)
_metrics_lock  = threading.Lock()

def _inc(key: str, n: int = 1):
    with _metrics_lock:
        _metrics[key] += n

# ── Whisper ───────────────────────────────────────────────────────────────────
_whisper_model = None
_whisper_lock  = threading.Lock()

def get_whisper():
    global _whisper_model
    if _whisper_model is None:
        with _whisper_lock:
            if _whisper_model is None:
                import whisper
                logger.info("Loading Whisper model (base)...")
                _whisper_model = whisper.load_model("base")
                logger.info("Whisper ready.")
    return _whisper_model


# ── Lifespan ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.warning(
        "Rate limiter is in-memory (single-process). "
        "Swap to Redis before scaling beyond --workers 1."
    )
    # Pre-warm the LangGraph pipeline
    def _warm_graph():
        try:
            logger.info("Warming LangGraph pipeline...")
            get_graph()
            logger.info("LangGraph pipeline ready.")
        except Exception as e:
            logger.warning(f"Graph warmup failed: {e}")

    # Pre-warm Whisper
    def _warm_whisper():
        try:
            import whisper as _w
            global _whisper_model
            logger.info("Warming Whisper model at startup...")
            _whisper_model = _w.load_model("base")
            logger.info("Whisper ready.")
        except Exception as e:
            logger.warning(f"Whisper warmup failed: {e}")

    threading.Thread(target=_warm_graph,   daemon=True).start()
    threading.Thread(target=_warm_whisper, daemon=True).start()
    yield


# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Equifiz AI",
    description="Financial research assistant API — powered by LangGraph + EQUIFIZ data",
    version="2.1.0",
    lifespan=lifespan,
)

# ── Upload size limit ─────────────────────────────────────────────────────────
MAX_UPLOAD_BYTES = int(os.environ.get("MAX_UPLOAD_MB", "20")) * 1024 * 1024

class UploadSizeLimitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        cl = request.headers.get("content-length")
        if cl and int(cl) > MAX_UPLOAD_BYTES:
            return JSONResponse(
                status_code=413,
                content={"detail": f"File too large. Max {MAX_UPLOAD_BYTES // (1024*1024)} MB."},
            )
        return await call_next(request)

# ── Request ID + latency middleware ───────────────────────────────────────────
class RequestIDMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        req_id = request.headers.get("X-Request-ID", str(uuid.uuid4())[:8])
        request.state.request_id = req_id
        t0 = time.perf_counter()
        response = await call_next(request)
        ms = int((time.perf_counter() - t0) * 1000)
        response.headers["X-Request-ID"]       = req_id
        response.headers["X-Response-Time-Ms"] = str(ms)
        _inc("requests_total")
        if response.status_code >= 500:
            _inc("errors_5xx")
        elif response.status_code == 429:
            _inc("errors_429")
        return response

app.add_middleware(UploadSizeLimitMiddleware)
app.add_middleware(RequestIDMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Rate limiters ─────────────────────────────────────────────────────────────
chat_limiter  = RateLimiter(max_calls=RATE_LIMIT_CHAT,  window_seconds=60)
voice_limiter = RateLimiter(max_calls=RATE_LIMIT_VOICE, window_seconds=60)


# ── Pydantic models ───────────────────────────────────────────────────────────

class ChatRequest(BaseModel):
    query:        str           = Field(..., min_length=1, max_length=2000)
    session_id:   Optional[str] = Field(None, max_length=36)
    force_refresh: bool         = False   # bypass today's ChromaDB cache

class ChatResponse(BaseModel):
    answer:     str
    session_id: str
    query_type: Optional[str] = None

class SessionCreate(BaseModel):
    title: Optional[str] = Field("", max_length=120)

class SessionTitleUpdate(BaseModel):
    title: str = Field(..., min_length=1, max_length=120)

class SaveQueryRequest(BaseModel):
    title: str = Field(..., min_length=1, max_length=120)
    query: str = Field(..., min_length=1, max_length=2000)

class IngestRequest(BaseModel):
    co_codes:   Optional[List[int]] = None
    mf_cocodes: Optional[List[int]] = None


# ─────────────────────────────────────────────────────────────────────────────
# HEALTH
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    llm_info = {
        "groq":      {"provider": "groq",      "model": GROQ_MODEL},
        "ollama":    {"provider": "ollama",     "model": OLLAMA_MODEL, "url": OLLAMA_BASE_URL},
        "anthropic": {"provider": "anthropic",  "model": ANTHROPIC_MODEL},
    }.get(LLM_PROVIDER, {"provider": LLM_PROVIDER, "model": "unknown"})
    return {
        "status":         "ok",
        "service":        "Equifiz AI",
        "llm":            llm_info,
        "whisper_loaded": _whisper_model is not None,
        **auth_status(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# METRICS
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/metrics", response_class=PlainTextResponse)
def metrics():
    """Prometheus-compatible counter metrics."""
    with _metrics_lock:
        snapshot = dict(_metrics)
    lines = ["# HELP equifiz_counter Simple event counters", "# TYPE equifiz_counter counter"]
    for k, v in sorted(snapshot.items()):
        lines.append(f'equifiz_counter{{name="{k}"}} {v}')
    return "\n".join(lines) + "\n"


# ─────────────────────────────────────────────────────────────────────────────
# CHAT  (orchestrated through LangGraph pipeline)
# ─────────────────────────────────────────────────────────────────────────────
@app.post("/chat", response_model=ChatResponse, dependencies=[Depends(chat_limiter)])
def chat_endpoint(req: ChatRequest, user_id: str = Depends(get_current_user)):
    """
    Main chat endpoint. Routes the query through the full LangGraph pipeline:
    classifier → entity extraction → symbol resolution → on-demand ingestion →
    vector retrieval → LLM synthesis.

    Conversation history is loaded from the session store and passed to the
    graph so the LLM can resolve pronouns and implicit company references.
    """
    # ── Session ──────────────────────────────────────────────────────────────
    sid = req.session_id
    if not sid:
        sid = new_session(title=req.query[:60], user_id=user_id)
    else:
        if not get_session_for_user(sid, user_id):
            raise HTTPException(status_code=404, detail="Session not found")

    session  = get_session_for_user(sid, user_id)
    history  = [
        {"role": m["role"], "content": m["content"]}
        for m in session["messages"]
    ]

    # ── Run LangGraph pipeline ────────────────────────────────────────────────
    try:
        answer, updated_history = run_query(
            user_query           = req.query,
            conversation_history = history,
            force_refresh        = req.force_refresh,
        )
    except Exception as e:
        logger.error(f"Pipeline error for query '{req.query[:60]}': {e}")
        _inc("pipeline_errors")
        raise HTTPException(status_code=500, detail=f"Pipeline error: {e}")

    # Persist only the two new turns (user + assistant) to avoid double-saving
    add_message(sid, "user",      req.query)
    add_message(sid, "assistant", answer)
    _inc("chat_requests")

    return ChatResponse(answer=answer, session_id=sid)


# ─────────────────────────────────────────────────────────────────────────────
# CHAT STREAM  (token-by-token, same pipeline)
# ─────────────────────────────────────────────────────────────────────────────
@app.post("/chat/stream", dependencies=[Depends(chat_limiter)])
async def chat_stream_endpoint(req: ChatRequest, user_id: str = Depends(get_current_user)):
    """
    Streaming chat — identical pipeline to POST /chat but streams the LLM
    answer word-by-word via text/plain SSE.

    Protocol:
      - Each chunk is raw text to append to the answer.
      - Final chunk starts with 'DONE:' + JSON: {"error": false}
    """
    sid = req.session_id
    if not sid:
        sid = new_session(title=req.query[:60], user_id=user_id)
    else:
        if not get_session_for_user(sid, user_id):
            raise HTTPException(status_code=404, detail="Session not found")

    session = get_session_for_user(sid, user_id)
    history = [
        {"role": m["role"], "content": m["content"]}
        for m in session["messages"]
    ]

    answer_parts: list[str] = []

    async def _generate():
        import asyncio
        import concurrent.futures

        loop = asyncio.get_event_loop()

        def _run():
            return run_query(
                user_query           = req.query,
                conversation_history = history,
                force_refresh        = req.force_refresh,
            )

        try:
            with concurrent.futures.ThreadPoolExecutor() as pool:
                answer, _ = await loop.run_in_executor(pool, _run)
        except Exception as e:
            logger.error(f"Stream pipeline error: {e}")
            yield f"Error: {e}"
            yield 'DONE:{"error": true}'
            return

        # Stream word-by-word (simple split for now; replace with true streaming LLM if needed)
        for word in answer.split(" "):
            answer_parts.append(word + " ")
            yield word + " "
            await asyncio.sleep(0)   # yield control to event loop

        add_message(sid, "user",      req.query)
        add_message(sid, "assistant", answer)
        _inc("chat_requests")
        yield 'DONE:{"error": false}'

    return StreamingResponse(_generate(), media_type="text/plain")


# ─────────────────────────────────────────────────────────────────────────────
# VOICE
# ─────────────────────────────────────────────────────────────────────────────
@app.post("/voice", response_model=ChatResponse, dependencies=[Depends(voice_limiter)])
async def voice_endpoint(
    request:    Request,
    file:       UploadFile = File(...),
    session_id: Optional[str] = None,
    force_refresh: bool = False,
    user_id:    str = Depends(get_current_user),
):
    """Transcribe audio with Whisper then route through the chat pipeline."""
    ALLOWED = {".wav", ".mp3", ".ogg", ".m4a", ".webm", ".flac"}
    ext = os.path.splitext(file.filename or "")[1].lower()
    if ext not in ALLOWED:
        raise HTTPException(status_code=400, detail=f"Unsupported audio format: {ext}")

    audio_bytes = await file.read()
    if len(audio_bytes) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="Audio file too large.")

    with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
        tmp.write(audio_bytes)
        tmp_path = tmp.name

    try:
        model  = get_whisper()
        result = model.transcribe(tmp_path, language="en")
        query  = result["text"].strip()
        logger.info(f"Transcribed: {query[:80]}")
    except Exception as e:
        logger.error(f"Whisper failed: {e}")
        _inc("whisper_errors")
        raise HTTPException(status_code=500, detail="Transcription failed.")
    finally:
        os.unlink(tmp_path)

    if not query:
        raise HTTPException(status_code=400, detail="Could not transcribe — please speak clearly.")

    _inc("voice_requests")
    return chat_endpoint(
        ChatRequest(query=query, session_id=session_id, force_refresh=force_refresh),
        user_id=user_id,
    )


# ─────────────────────────────────────────────────────────────────────────────
# SESSIONS
# ─────────────────────────────────────────────────────────────────────────────
@app.post("/sessions")
def create_session(req: SessionCreate, user_id: str = Depends(get_current_user)):
    sid = new_session(title=req.title, user_id=user_id)
    return get_session(sid)

@app.get("/sessions")
def get_sessions(user_id: str = Depends(get_current_user)):
    return list_sessions(user_id=user_id)

@app.get("/sessions/shared/{token}")   # public — intentionally no auth
def get_shared_session(token: str):
    session = get_session_by_token(token)
    if not session:
        raise HTTPException(status_code=404, detail="Invalid or expired share token")
    return session

@app.get("/sessions/{sid}")
def get_session_endpoint(sid: str, user_id: str = Depends(get_current_user)):
    session = get_session_for_user(sid, user_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return session

@app.patch("/sessions/{sid}")
def rename_session(sid: str, req: SessionTitleUpdate, user_id: str = Depends(get_current_user)):
    if not get_session_for_user(sid, user_id):
        raise HTTPException(status_code=404, detail="Session not found")
    update_session_title(sid, req.title)
    return get_session_for_user(sid, user_id)

@app.delete("/sessions/{sid}")
def delete_session_endpoint(sid: str, user_id: str = Depends(get_current_user)):
    if not get_session_for_user(sid, user_id):
        raise HTTPException(status_code=404, detail="Session not found")
    delete_session(sid)
    delete_doc(sid)
    return {"deleted": sid}

@app.post("/sessions/{sid}/share")
def share_session_endpoint(sid: str, user_id: str = Depends(get_current_user)):
    if not get_session_for_user(sid, user_id):
        raise HTTPException(status_code=404, detail="Session not found")
    token = share_session(sid)
    return {"share_token": token, "share_url": f"?share={token}"}


# ─────────────────────────────────────────────────────────────────────────────
# DOCUMENT UPLOAD
# ─────────────────────────────────────────────────────────────────────────────
@app.post("/upload-doc/{sid}")
async def upload_doc(
    sid: str,
    file: UploadFile = File(...),
    user_id: str = Depends(get_current_user),
):
    if not get_session_for_user(sid, user_id):
        raise HTTPException(status_code=404, detail="Session not found")

    file_bytes = await file.read()
    if len(file_bytes) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="File too large.")

    chunks = process_uploaded_file(file_bytes, file.filename)
    if not chunks:
        raise HTTPException(status_code=422, detail="Could not extract text from this file.")

    set_doc(sid, file.filename, chunks)
    logger.info(f"Doc uploaded for session {sid}: {file.filename} → {len(chunks)} chunks")
    return {
        "filename":   file.filename,
        "chunks":     len(chunks),
        "session_id": sid,
        "message":    f"Document ready. Ask questions about {file.filename}.",
    }

@app.delete("/upload-doc/{sid}")
def remove_doc(sid: str, user_id: str = Depends(get_current_user)):
    if not get_session_for_user(sid, user_id):
        raise HTTPException(status_code=404, detail="Session not found")
    delete_doc(sid)
    return {"message": "Document removed from session."}


# ─────────────────────────────────────────────────────────────────────────────
# SAVED QUERIES
# ─────────────────────────────────────────────────────────────────────────────
@app.post("/saved-queries")
def create_saved_query(req: SaveQueryRequest, user_id: str = Depends(get_current_user)):
    return save_query(req.title, req.query, user_id=user_id)

@app.get("/saved-queries")
def get_saved_queries(user_id: str = Depends(get_current_user)):
    return list_saved_queries(user_id=user_id)

@app.delete("/saved-queries/{qid}")
def delete_saved_query_endpoint(qid: str, user_id: str = Depends(get_current_user)):
    delete_saved_query(qid, user_id=user_id)
    return {"deleted": qid}


# ─────────────────────────────────────────────────────────────────────────────
# COMPANY SEARCH  (direct DB lookup — no LLM needed)
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/companies/search")
def search_companies(
    q:     str = Query(..., min_length=1, max_length=100, description="Company name or NSE symbol"),
    limit: int = Query(5, ge=1, le=20),
    user_id: str = Depends(get_current_user),
):
    """
    Fuzzy-search the local company master DB.
    Returns up to `limit` matching companies with co_code, name, NSE symbol, sector.
    """
    # Try exact NSE symbol first
    exact = company_db.lookup_by_nse_symbol(q.upper())
    if exact:
        return [exact]

    results = company_db.fuzzy_search_company(q, limit=limit)
    return results

@app.get("/companies/{co_code}")
def get_company(co_code: int, user_id: str = Depends(get_current_user)):
    """Look up a company by its CMOTS co_code."""
    row = company_db.lookup_by_co_code(co_code)
    if not row:
        raise HTTPException(status_code=404, detail=f"Company {co_code} not found")
    return row


# ─────────────────────────────────────────────────────────────────────────────
# LIVE INGESTION  (background task)
# ─────────────────────────────────────────────────────────────────────────────
@app.post("/ingest/live")
def trigger_live_ingest(
    req:              IngestRequest,
    background_tasks: BackgroundTasks,
    user_id:          str = Depends(get_current_user),
):
    """
    Trigger bulk live ingestion for a set of co_codes.
    Runs in background; returns immediately.
    """
    from graph import _ingest_co_code

    def _run_bulk(co_codes: List[int]):
        for cc in co_codes:
            try:
                ingested = _ingest_co_code(cc, force_refresh=True)
                logger.info(f"Bulk ingest co_code={cc}: {ingested}")
                _inc("bulk_ingested")
            except Exception as e:
                logger.error(f"Bulk ingest failed co_code={cc}: {e}")

    co_codes = req.co_codes or []
    background_tasks.add_task(_run_bulk, co_codes)
    _inc("ingest_triggers")
    logger.info(f"Live ingestion triggered by user={user_id} co_codes={co_codes}")
    return {
        "message":  "Live ingestion started in background.",
        "co_codes": co_codes,
    }

@app.post("/ingest/company/{co_code}")
def ingest_single_company(
    co_code:      int,
    force_refresh: bool = True,
    background_tasks: BackgroundTasks = None,
    user_id:      str = Depends(get_current_user),
):
    """
    On-demand ingestion for a single company — runs in background.
    """
    from graph import _ingest_co_code

    def _run():
        try:
            ingested = _ingest_co_code(co_code, force_refresh=force_refresh)
            logger.info(f"Single ingest co_code={co_code}: {ingested}")
        except Exception as e:
            logger.error(f"Single ingest failed co_code={co_code}: {e}")

    background_tasks.add_task(_run)
    return {
        "message":  f"Ingestion for co_code={co_code} started in background.",
        "co_code":  co_code,
    }

# Equifiz AI — Financial Research Assistant API

A production-ready FastAPI backend powered by a **LangGraph orchestration pipeline** that
routes financial queries through entity extraction → symbol resolution → on-demand EQUIFIZ
API ingestion → ChromaDB vector retrieval → LLM synthesis.

---

## Architecture

```
User Query
    │
    ▼
┌─────────────────────────────────────────────────────┐
│               LangGraph Pipeline                    │
│                                                     │
│  [Query Classifier]                                 │
│       │                                             │
│  greeting ──► Greeting Handler ──► END              │
│  general  ──► General Handler  ──► Synthesis ──► END│
│  company  ──► Entity Extraction                     │
│  comparison/investment ──► Entity Extraction        │
│                │                                    │
│         [Symbol Resolution]                         │
│                │                                    │
│           [DB Lookup]  ◄── PostgreSQL               │
│                │                                    │
│      [On-demand Ingestion] ◄── EQUIFIZ API          │
│                │                  │                 │
│                │            ChromaDB (write)         │
│                │                                    │
│       [Vector Retrieval] ◄── ChromaDB (read)        │
│                │                                    │
│           [LLM Synthesis] ◄── Groq / Ollama         │
│                │                                    │
│           Final Answer                              │
└─────────────────────────────────────────────────────┘
```

## Quick Start

### 1. Configure environment
```bash
cp .env.example .env
# Edit .env — set GROQ_API_KEY, CMOTS_TOKEN, POSTGRES_PASSWORD at minimum
```

### 2. Start with Docker Compose
```bash
docker compose up -d
```

### 3. Seed the database (first run only)
```bash
docker compose exec api python seed_db.py
```

### 4. Test
```bash
curl http://localhost:8000/health
curl -X POST http://localhost:8000/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "What is the PE ratio of TCS?"}'
```

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Service health + LLM info |
| GET | `/metrics` | Prometheus-compatible counters |
| POST | `/chat` | Main chat — full LangGraph pipeline |
| POST | `/chat/stream` | Streaming chat (word-by-word) |
| POST | `/voice` | Voice → Whisper → chat pipeline |
| POST | `/sessions` | Create a new chat session |
| GET | `/sessions` | List sessions for current user |
| GET | `/sessions/{sid}` | Get session with message history |
| PATCH | `/sessions/{sid}` | Rename session |
| DELETE | `/sessions/{sid}` | Delete session |
| POST | `/sessions/{sid}/share` | Generate shareable token |
| GET | `/sessions/shared/{token}` | View shared session (no auth) |
| POST | `/upload-doc/{sid}` | Upload PDF/DOCX/TXT to session |
| DELETE | `/upload-doc/{sid}` | Remove uploaded doc from session |
| POST | `/saved-queries` | Save a query for quick reuse |
| GET | `/saved-queries` | List saved queries |
| DELETE | `/saved-queries/{qid}` | Delete a saved query |
| GET | `/companies/search?q=` | Fuzzy company name/symbol search |
| GET | `/companies/{co_code}` | Look up company by CMOTS code |
| POST | `/ingest/live` | Bulk background ingestion |
| POST | `/ingest/company/{co_code}` | Single company re-ingestion |

---

## Project Structure

```
equifiz_agent/
├── main.py               # FastAPI app — all endpoints
├── graph.py              # LangGraph pipeline (9-step orchestration)
├── db.py                 # PostgreSQL company master lookups
├── equifiz_client.py     # EQUIFIZ REST API client
├── vector_store.py       # ChromaDB + Ollama embedding
├── company_db_2.py       # Extended company DB helpers
├── seed_db.py            # One-time DB seed script
│
├── config/
│   └── config.py         # Centralised configuration (all from env)
│
├── utils/
│   ├── auth.py           # Simple Bearer token dependency
│   ├── auth_manager.py   # Guest / Firebase auth manager
│   ├── chroma_client.py  # ChromaDB collection accessors
│   ├── doc_processor.py  # Document upload text extraction
│   ├── doc_store.py      # SQLite-backed uploaded doc store
│   ├── embedder.py       # fastembed wrapper
│   ├── rate_limiter.py   # Redis-backed sliding window limiter
│   ├── request_id.py     # X-Request-ID middleware
│   └── session_store.py  # SQLite-backed session + message store
│
├── logger/
│   └── logger.py         # File + console structured logging
│
├── Dockerfile
├── docker-compose.yml
├── .env.example
├── requirements.txt
└── README.md
```

---

## LangGraph Pipeline — Query Types

| Type | Route |
|------|-------|
| `greeting` | Friendly response, no API calls |
| `general` | Conceptual question → vector search only |
| `company` | Single company → full ingest + retrieval |
| `comparison` | 2+ companies → parallel ingest + compare |
| `investment` | Buy/invest query → scored ranking + advice |

---

## Auth Modes

| Mode | How to enable | Behaviour |
|------|---------------|-----------|
| **Guest** (default) | Leave `FIREBASE_CREDENTIALS` blank | All requests are `user_id=guest`. Optionally gate with `API_SECRET_KEY`. |
| **Firebase** | Set `FIREBASE_CREDENTIALS=/path/to/creds.json` | Full per-user session isolation via Firebase JWT. |

---

## Services (Docker Compose)

| Service | Image | Purpose |
|---------|-------|---------|
| `api` | (built locally) | FastAPI + LangGraph |
| `postgres` | postgres:16-alpine | Company master DB |
| `redis` | redis:7-alpine | Cross-worker rate limiting |
| `ollama` | ollama/ollama (commented) | Local LLM + embeddings |

---

## Environment Variables

See `.env.example` for the full list with descriptions.

Key variables:

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GROQ_API_KEY` | Yes (if using Groq) | — | Groq cloud LLM key |
| `CMOTS_TOKEN` | Yes | — | EQUIFIZ JWT token |
| `POSTGRES_PASSWORD` | Yes | — | PostgreSQL password |
| `ALLOWED_ORIGINS` | Yes | `*` | CORS allowed origins |
| `LLM_PROVIDER` | No | auto-detect | `groq` / `ollama` / `anthropic` |
| `API_SECRET_KEY` | No | — | Simple Bearer token gate |
| `FIREBASE_CREDENTIALS` | No | — | Path to Firebase JSON for per-user auth |
| `REDIS_URL` | No | — | Enables Redis rate limiter |

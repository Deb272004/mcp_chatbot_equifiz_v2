# ─────────────────────────────────────────────────────────────────────────────
# Equifiz AI — Dockerfile
# Multi-stage build: builder installs deps, runtime is lean.
# ─────────────────────────────────────────────────────────────────────────────

# ── Stage 1: Builder ──────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

# System deps needed to compile native extensions (psycopg2, chromadb, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        libffi-dev \
        ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip & install wheel for faster builds
RUN pip install --upgrade pip wheel

# Copy only requirements first — leverages Docker layer cache
COPY requirements.txt .

# Install all Python dependencies into a prefix we can copy
RUN pip install --prefix=/install --ignore-installed --no-cache-dir -r requirements.txt


# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM python:3.12-slim

LABEL maintainer="Equifiz Team"
LABEL description="Equifiz AI — Financial research assistant API"

# Runtime system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
        ffmpeg \
        curl \
        libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
COPY --from=builder /install /usr/local

WORKDIR /app

# Create non-root user for security
RUN useradd -m -u 1000 equifiz && \
    mkdir -p /app/data /app/logs /app/chroma_store /app/.fastembed_cache && \
    chown -R equifiz:equifiz /app

# Copy  code
COPY --chown=equifiz:equifiz . .

# Remove any leftover .env files — secrets come from Docker environment only
# RUN rm -f .env

USER equifiz

# Expose API port
EXPOSE 8000

# Health check — hits /health every 30s
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default command — single worker (rate limiter is in-memory)
# Scale with --workers N only after wiring Redis for rate limiting
CMD ["uvicorn", "main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "1", \
     "--log-level", "info", \
     "--access-log"]

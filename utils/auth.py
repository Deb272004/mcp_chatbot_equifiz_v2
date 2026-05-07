"""
Simple Bearer-token auth dependency for FastAPI.
Set API_SECRET_KEY env var to enable. If blank, auth is skipped (dev mode).
"""
from fastapi import Header, HTTPException, status
from config.config import API_SECRET_KEY


async def require_auth(authorization: str = Header(default="")):
    if not API_SECRET_KEY:
        return  # dev mode — no key configured, allow all
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or token != API_SECRET_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
            headers={"WWW-Authenticate": "Bearer"},
        )

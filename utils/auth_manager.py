"""
Dynamic auth manager — guest mode by default, Firebase when credentials are provided.

MODE 1  guest    No FIREBASE_CREDENTIALS set → shared session pool, optional API key gate
MODE 2  firebase FIREBASE_CREDENTIALS set    → per-user sessions, Firebase JWT required
"""
import os
from fastapi import Header, HTTPException, status

_FIREBASE_CREDS = os.environ.get("FIREBASE_CREDENTIALS", "")
_API_SECRET_KEY = os.environ.get("API_SECRET_KEY", "")

AUTH_MODE = "firebase" if _FIREBASE_CREDS else "guest"

_firebase_ready = False
if AUTH_MODE == "firebase":
    try:
        import firebase_admin
        from firebase_admin import credentials, auth as fb_auth
        if not firebase_admin._apps:
            cred = credentials.Certificate(_FIREBASE_CREDS)
            firebase_admin.initialize_app(cred)
        _firebase_ready = True
    except Exception as e:
        import logging
        logging.getLogger("AuthManager").error(
            f"FIREBASE_CREDENTIALS set but init failed: {e} — falling back to guest mode"
        )
        AUTH_MODE = "guest"


def get_current_user(authorization: str = Header(default="")) -> str:
    """
    FastAPI dependency. Returns a user-identity string.
    guest mode   → "guest"  (or validates shared API_SECRET_KEY if set)
    firebase mode → Firebase UID of the authenticated user
    """
    if AUTH_MODE == "firebase":
        return _verify_firebase(authorization)
    return _verify_guest(authorization)


def _verify_firebase(authorization: str) -> str:
    token = authorization.removeprefix("Bearer ").strip()
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        from firebase_admin import auth as fb_auth
        decoded = fb_auth.verify_id_token(token)
        return decoded["uid"]
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired token")


def _verify_guest(authorization: str) -> str:
    if not _API_SECRET_KEY:
        return "guest"
    token = authorization.removeprefix("Bearer ").strip()
    if token != _API_SECRET_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return "guest"


def auth_status() -> dict:
    return {
        "auth_mode":         AUTH_MODE,
        "firebase_ready":    _firebase_ready,
        "per_user_sessions": AUTH_MODE == "firebase",
    }

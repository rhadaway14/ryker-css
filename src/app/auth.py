import os
import time
import bcrypt
import jwt
from functools import wraps
from flask import Blueprint, request, jsonify, make_response

bp_auth = Blueprint("auth", __name__)

# -------- Config (via env) --------
USERNAME = os.getenv("APP_BASIC_USER", "admin")

# Store a bcrypt hash, not plaintext.
# Generate with:
#   python -c 'import bcrypt; print(bcrypt.hashpw(b"your-password", bcrypt.gensalt()).decode())'
DEFAULT_HASH = bcrypt.hashpw(b"secret", bcrypt.gensalt()).decode()
PASSWORD_HASH = os.getenv("APP_BASIC_HASH", DEFAULT_HASH)

JWT_SECRET = os.getenv("APP_JWT_SECRET", "change-me")
JWT_AUD = os.getenv("APP_JWT_AUD", "ryker-app")
JWT_ISS = os.getenv("APP_JWT_ISS", "ryker-auth")
JWT_TTL = int(os.getenv("APP_JWT_TTL", "3600"))  # seconds

COOKIE_NAME = "ryker_session"
COOKIE_SAMESITE = os.getenv("APP_COOKIE_SAMESITE", "Lax")  # "Lax" or "None"
COOKIE_SECURE = os.getenv("APP_COOKIE_SECURE", "true").lower() == "true"  # True for HTTPS; False only in dev over HTTP
COOKIE_HTTPONLY = True


# -------- Helpers --------
def _issue_token(sub: str):
    now = int(time.time())
    payload = {
        "sub": sub,
        "iss": JWT_ISS,
        "aud": JWT_AUD,
        "iat": now,
        "exp": now + JWT_TTL,
        "typ": "access",
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")


def _verify(token: str):
    return jwt.decode(token, JWT_SECRET, algorithms=["HS256"], audience=JWT_AUD, issuer=JWT_ISS)


def require_login(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        authz = request.headers.get("Authorization", "")
        token = None
        if authz.startswith("Bearer "):
            token = authz.split(" ", 1)[1]
        else:
            token = request.cookies.get(COOKIE_NAME)
        if not token:
            return jsonify({"error": "unauthorized"}), 401
        try:
            request.user = _verify(token)
        except Exception as ex:
            return jsonify({"error": "invalid token", "detail": str(ex)}), 401
        return f(*args, **kwargs)

    return wrapper


# -------- Routes --------
@bp_auth.post("/auth/login")
def login():
    data = request.get_json(silent=True) or {}
    u = (data.get("username") or "").strip()
    p = (data.get("password") or "").encode()

    # Single account check (constant-time compare not necessary here)
    if u != USERNAME:
        return jsonify({"error": "invalid credentials"}), 401
    if not bcrypt.checkpw(p, PASSWORD_HASH.encode()):
        return jsonify({"error": "invalid credentials"}), 401

    tok = _issue_token(u)
    resp = make_response(jsonify({"ok": True, "user": u}))
    # HttpOnly cookie; the SPA should not read it directly.
    resp.set_cookie(
        COOKIE_NAME,
        tok,
        max_age=JWT_TTL,
        secure=COOKIE_SECURE,
        httponly=COOKIE_HTTPONLY,
        samesite=COOKIE_SAMESITE,
        path="/",
    )
    return resp, 200


@bp_auth.post("/auth/logout")
def logout():
    resp = make_response(jsonify({"ok": True}))
    resp.delete_cookie(COOKIE_NAME, path="/")
    return resp, 200


@bp_auth.get("/auth/whoami")
def whoami():
    token = request.cookies.get(COOKIE_NAME)
    if not token:
        return jsonify({"authenticated": False}), 200
    try:
        claims = _verify(token)
        return jsonify({"authenticated": True, "claims": claims}), 200
    except Exception:
        return jsonify({"authenticated": False}), 200

# src/app/__init__.py
from pathlib import Path
from os import getenv
from dotenv import load_dotenv
from flask import Flask
from flask_cors import CORS
from azure.cosmos import CosmosClient

from .settings import Settings
from app.auth import bp_auth            # /auth/login, /auth/logout, /auth/whoami
from app.routes import bp as bp_api     # main API


def _parse_allowed_origins(env_val: str | None):
    """
    Return either '*' (public dev) or a list of explicit origins for CORS.
    Supports comma-separated values.
    """
    if not env_val:
        return ["http://localhost:5173", "http://127.0.0.1:5173"]
    val = env_val.strip()
    if val == "*":
        return "*"
    return [o.strip() for o in val.split(",") if o.strip()]


def create_app():
    # Load .env from repo root
    root = Path(__file__).resolve().parents[1]
    load_dotenv(dotenv_path=root / ".env")

    app = Flask(__name__)

    # ---- CORS (so SPA on different origin can talk to Flask with cookies) ----
    allowed = _parse_allowed_origins(getenv("ALLOW_ORIGINS"))
    CORS(
        app,
        resources={r"/*": {"origins": allowed}},
        supports_credentials=True,  # send/receive cookies
        allow_headers=["Content-Type", "Accept", "Authorization"],
        expose_headers=["Content-Type", "X-Continuation-Token"],
    )

    # ---- Settings / Cosmos wiring ----
    s = Settings.from_env()
    app.config["APP_SETTINGS"] = s

    cosmos = CosmosClient(s.COSMOS_ENDPOINT, credential=s.COSMOS_KEY)
    db = cosmos.get_database_client(s.COSMOS_DATABASE)
    coll = db.get_container_client(s.COSMOS_CONTAINER)
    app.config["COSMOS_CONTAINER"] = coll

    # ---- Blueprints ----
    app.register_blueprint(bp_auth)  # /auth/...
    app.register_blueprint(bp_api)   # /items, /search, ...

    return app

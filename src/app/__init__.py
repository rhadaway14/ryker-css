from pathlib import Path
from os import getenv

from dotenv import load_dotenv
from flask import Flask
from flask_cors import CORS
from azure.cosmos import CosmosClient
from azure.storage.blob import BlobServiceClient

from .settings import Settings
from app.auth import bp_auth            # /auth/login, /auth/logout, /auth/whoami
from app.routes import bp as bp_api     # /health, /search/one, /download/batch


def _parse_allowed_origins(env_val: str | None):
    if not env_val:
        return ["http://localhost:5173", "http://127.0.0.1:5173"]
    val = env_val.strip()
    if val == "*":
        return "*"
    return [o.strip() for o in val.split(",") if o.strip()]


def create_app():
    # Load .env from repo root (src/.env)
    root = Path(__file__).resolve().parents[1]
    load_dotenv(dotenv_path=root / ".env")

    app = Flask(__name__)

    allowed = _parse_allowed_origins(getenv("ALLOW_ORIGINS"))
    CORS(
        app,
        resources={r"/*": {"origins": allowed}},
        supports_credentials=True,
        allow_headers=["Content-Type", "Accept", "Authorization"],
        expose_headers=["Content-Type", "X-Continuation-Token"],
    )

    # ---- Settings / Cosmos / Blob wiring ----
    s = Settings.from_env()
    app.config["APP_SETTINGS"] = s

    cosmos = CosmosClient(s.COSMOS_ENDPOINT, credential=s.COSMOS_KEY)
    db = cosmos.get_database_client(s.COSMOS_DATABASE)
    coll = db.get_container_client(s.COSMOS_CONTAINER)
    app.config["COSMOS_CONTAINER"] = coll

    blob_service = BlobServiceClient(
        account_url=f"https://{s.BLOB_ACCOUNT}.blob.core.windows.net",
        credential=s.BLOB_KEY,
    )
    blob_container = blob_service.get_container_client(s.BLOB_CONTAINER)
    app.config["BLOB_CONTAINER_CLIENT"] = blob_container

    # ---- Blueprints ----
    app.register_blueprint(bp_auth)  # /auth/...
    app.register_blueprint(bp_api)   # /health, /search/one, /download/batch

    return app

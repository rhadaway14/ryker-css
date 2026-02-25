# src/app/settings.py
from __future__ import annotations

import os
from pydantic import BaseModel, Field


class Settings(BaseModel):
    # Cosmos
    COSMOS_ENDPOINT: str
    COSMOS_KEY: str
    COSMOS_DATABASE: str
    COSMOS_CONTAINER: str

    # Blob (what routes/blob helpers expect)
    BLOB_ACCOUNT: str
    BLOB_KEY: str
    BLOB_CONTAINER: str

    # Default SAS hours used elsewhere (can be overridden per-call)
    SAS_HOURS: int = Field(default=1)

    @classmethod
    def from_env(cls) -> "Settings":
        # Support your .env names and a couple of fallbacks
        cosmos_endpoint = os.getenv("COSMOS_ENDPOINT", "")
        cosmos_key = os.getenv("COSMOS_KEY", "")
        cosmos_db = os.getenv("COSMOS_DATABASE", "")
        cosmos_coll = os.getenv("COSMOS_CONTAINER", "")

        blob_account = (
            os.getenv("AZURE_STORAGE_ACCOUNT")
            or os.getenv("BLOB_ACCOUNT")
            or ""
        )
        blob_key = (
            os.getenv("AZURE_STORAGE_KEY")
            or os.getenv("BLOB_KEY")
            or ""
        )
        blob_container = (
            os.getenv("AZURE_BLOB_CONTAINER")
            or os.getenv("BLOB_CONTAINER")
            or "files"
        )

        sas_hours_raw = os.getenv("SAS_HOURS", "1")
        try:
            sas_hours = int(sas_hours_raw)
        except Exception:
            sas_hours = 1

        missing = []
        for name, val in [
            ("COSMOS_ENDPOINT", cosmos_endpoint),
            ("COSMOS_KEY", cosmos_key),
            ("COSMOS_DATABASE", cosmos_db),
            ("COSMOS_CONTAINER", cosmos_coll),
            ("AZURE_STORAGE_ACCOUNT/BLOB_ACCOUNT", blob_account),
            ("AZURE_STORAGE_KEY/BLOB_KEY", blob_key),
            ("AZURE_BLOB_CONTAINER/BLOB_CONTAINER", blob_container),
        ]:
            if not val:
                missing.append(name)
        if missing:
            raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

        return cls(
            COSMOS_ENDPOINT=cosmos_endpoint,
            COSMOS_KEY=cosmos_key,
            COSMOS_DATABASE=cosmos_db,
            COSMOS_CONTAINER=cosmos_coll,
            BLOB_ACCOUNT=blob_account,
            BLOB_KEY=blob_key,
            BLOB_CONTAINER=blob_container,
            SAS_HOURS=sas_hours,
        )

from __future__ import annotations

import io
import os
import zipfile
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from flask import Blueprint, current_app, jsonify, request, send_file

from azure.storage.blob import BlobServiceClient
from azure.storage.blob import BlobSasPermissions, generate_blob_sas

from app.auth import require_login  # <-- IMPORTANT: use the same auth/cookie logic as auth.py

bp = Blueprint("api", __name__)

# ----------------------------
# Helpers: pull shared clients/settings from app config (do NOT create at import time)
# ----------------------------

def _settings():
    # set in __init__.py: app.config["APP_SETTINGS"] = Settings.from_env()
    return current_app.config["APP_SETTINGS"]

def _cosmos_container():
    # set in __init__.py: app.config["COSMOS_CONTAINER"] = coll
    return current_app.config["COSMOS_CONTAINER"]

def _blob_container_client():
    s = _settings()
    account_url = f"https://{s.BLOB_ACCOUNT}.blob.core.windows.net"
    svc = BlobServiceClient(account_url=account_url, credential=s.BLOB_KEY)
    return svc.get_container_client(s.BLOB_CONTAINER)

def _safe_int(val: Any, default: int) -> int:
    try:
        return int(val)
    except Exception:
        return default

# Prevent Cosmos SQL injection via "field" param
ALLOWED_FIELDS = {
    "ClientID",
    "PortEntry",
    "SCAC",
    "MastBillNum",
    "ShipperRefNum",
    "RelDate",
    "EntryNum",
    "ImpName",
    "ContainerNum",
    "StatementNum",
    "ImporterID",
    "ImporterNumber",
}

def _make_cosmos_where(field: str, contains: bool, pk: Optional[str]) -> tuple[str, list[dict[str, Any]]]:
    """
    Returns (where_sql, params) for Cosmos SQL.
    """
    params: list[dict[str, Any]] = []
    clauses: list[str] = []

    if pk:
        clauses.append("c.pk = @pk")
        params.append({"name": "@pk", "value": pk})

    # value always provided by caller
    if contains:
        clauses.append(f"CONTAINS(c.{field}, @value, true)")
    else:
        clauses.append(f"c.{field} = @value")

    return " AND ".join(clauses), params


def _sas_url_for_blob(blob_name: str, filename: Optional[str] = None, hours: Optional[int] = None) -> str:
    """
    Generate a SAS URL for a blob in the configured container.
    """
    s = _settings()
    expiry_hours = int(hours or getattr(s, "SAS_HOURS", 1) or 1)

    sas = generate_blob_sas(
        account_name=s.BLOB_ACCOUNT,
        container_name=s.BLOB_CONTAINER,
        blob_name=blob_name,
        account_key=s.BLOB_KEY,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(hours=expiry_hours),
    )

    # Optional: content-disposition filename hint
    cd = ""
    if filename:
        cd = f"&rscd=attachment%3B%20filename%3D{_urlencode_filename(filename)}"

    return f"https://{s.BLOB_ACCOUNT}.blob.core.windows.net/{s.BLOB_CONTAINER}/{blob_name}?{sas}{cd}"

def _urlencode_filename(name: str) -> str:
    # minimal safe encoding for filename in query
    return (
        name.replace("%", "%25")
        .replace(" ", "%20")
        .replace('"', "%22")
        .replace("#", "%23")
        .replace("&", "%26")
        .replace("+", "%2B")
        .replace(";", "%3B")
    )

# ----------------------------
# Routes
# ----------------------------

@bp.get("/health")
def health():
    return jsonify({"ok": True})


@bp.get("/search/one")
@require_login
def search_one():
    """
    GET /search/one?field=SCAC&value=on&contains=true&page_size=50&continuation=...
    Returns: {"items":[...], "continuation":"..."}
    """
    field = (request.args.get("field") or "").strip()
    value = (request.args.get("value") or "").strip()
    pk = request.args.get("pk")
    contains = (request.args.get("contains") or "false").lower() == "true"
    page_size = _safe_int(request.args.get("page_size"), 50)
    continuation = request.args.get("continuation") or None

    if not field or not value:
        return jsonify({"error": "field and value are required"}), 400

    if field not in ALLOWED_FIELDS:
        return jsonify({"error": f"invalid field: {field}"}), 400

    # For dates, your UI typically does exact match
    if field == "RelDate":
        contains = False

    where_sql, params = _make_cosmos_where(field=field, contains=contains, pk=pk)
    params = [{"name": "@value", "value": value}] + params

    query = f"SELECT * FROM c WHERE {where_sql}"

    container = _cosmos_container()

    # IMPORTANT FIX (prevents your 500):
    # In many azure-cosmos versions, max_item_count MUST be passed to query_items(),
    # not to by_page().
    pager = (
        container.query_items(
            query=query,
            parameters=params,
            enable_cross_partition_query=True,
            max_item_count=page_size,  # <-- FIX
        )
        .by_page(continuation_token=continuation)  # <-- no max_item_count here
    )

    try:
        page = next(pager)
    except StopIteration:
        return jsonify({"items": [], "continuation": None}), 200

    items = list(page)
    next_token = getattr(pager, "continuation_token", None)

    return jsonify({"items": items, "continuation": next_token}), 200


@bp.post("/download/batch")
@require_login
def download_batch():
    """
    POST /download/batch
    Body: {"files":[{"blobName":"<path/in/container>", "filename":"optional.pdf"}, ...]}
    Returns: zip file download
    """
    data = request.get_json(silent=True) or {}
    files = data.get("files") or []

    if not isinstance(files, list) or len(files) == 0:
        return jsonify({"error": "files[] is required"}), 400

    blob_container = _blob_container_client()

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for f in files:
            if not isinstance(f, dict):
                continue

            blob_name = (f.get("blobName") or "").lstrip("/")
            filename = (f.get("filename") or os.path.basename(blob_name) or "file")

            if not blob_name:
                continue

            try:
                data_bytes = blob_container.get_blob_client(blob_name).download_blob().readall()
            except Exception as ex:
                data_bytes = f"ERROR: could not download {blob_name}\n{ex}".encode("utf-8")

            z.writestr(filename, data_bytes)

    buf.seek(0)
    return send_file(
        buf,
        mimetype="application/zip",
        as_attachment=True,
        download_name=f"ryker-download-{int(datetime.utcnow().timestamp())}.zip",
    )


@bp.get("/items/<item_id>/download")
@require_login
def download_single(item_id: str):
    """
    GET /items/<id>/download?blob=<blobName>&filename=<name>&hours=1
    Returns a redirect-ish payload containing a SAS URL (client can window.open it).
    NOTE: Your current UI usually uses f.url already, but this endpoint keeps buildDownloadUrl working.
    """
    blob_name = (request.args.get("blob") or "").lstrip("/")
    filename = request.args.get("filename")
    hours = request.args.get("hours")
    hours_i = _safe_int(hours, 1) if hours is not None else None

    if not blob_name:
        # fallback: try to read the item and pick first file blobName/url if present
        try:
            container = _cosmos_container()
            # If your container is partitioned by pk, you can pass pk via querystring in the future.
            doc = container.read_item(item=item_id, partition_key=item_id)
        except Exception:
            doc = None

        if doc and isinstance(doc, dict):
            files = doc.get("files") or []
            if files and isinstance(files, list) and isinstance(files[0], dict):
                blob_name = (files[0].get("blobName") or files[0].get("blobPath") or "").lstrip("/")
                filename = filename or files[0].get("name") or files[0].get("filename")

    if not blob_name:
        return jsonify({"error": "missing blob (provide ?blob=...)"}), 400

    url = _sas_url_for_blob(blob_name=blob_name, filename=filename, hours=hours_i)
    return jsonify({"ok": True, "url": url}), 200

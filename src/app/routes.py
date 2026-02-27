# src/app/routes.py
from __future__ import annotations

import io
import uuid
import zipfile
import datetime
from typing import Any, Dict, List, Optional, Tuple

from flask import Blueprint, current_app, jsonify, redirect, request, Response
from azure.cosmos import exceptions as cos_ex

from .settings import Settings

from azure.storage.blob import (
    BlobServiceClient,
    BlobSasPermissions,
    generate_blob_sas,
)
from urllib.parse import urlsplit, urlunsplit

bp = Blueprint("api", __name__)

# -------- helpers --------
def container():
    return current_app.config["COSMOS_CONTAINER"]

def settings() -> Settings:
    return current_app.config["APP_SETTINGS"]

def _blob_service(s: Settings) -> BlobServiceClient:
    return BlobServiceClient(
        f"https://{s.BLOB_ACCOUNT}.blob.core.windows.net",
        credential=s.BLOB_KEY,
    )

def _public_blob_url(s: Settings, blob_path: str) -> str:
    """
    blob_path may be:
      - "<container>/<blobName>"
      - "<blobName>"
      - full URL
    Returns a SAS-less public URL (ONLY for display; not for access if container is private).
    """
    if not blob_path:
        return ""
    bp = blob_path.strip()

    # If full URL, strip query/fragment and return
    if bp.startswith("http://") or bp.startswith("https://"):
        return _strip_sas(bp)

    # If starts with container/
    if bp.startswith(s.BLOB_CONTAINER + "/"):
        return f"https://{s.BLOB_ACCOUNT}.blob.core.windows.net/{bp}"

    # Otherwise treat as blobName
    return f"https://{s.BLOB_ACCOUNT}.blob.core.windows.net/{s.BLOB_CONTAINER}/{bp}"

def _strip_sas(url: str) -> str:
    """Remove any query/fragment from a blob URL (drop SAS)."""
    if not url:
        return url
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))

def _normalize_blob_name(s: Settings, value: str) -> str:
    """
    Accepts:
      - full URL: https://acct.blob.core.windows.net/container/blob?SAS
      - blobPath: container/blob
      - blobName: blob
    Returns:
      - blobName (no container prefix)
    """
    v = (value or "").strip()
    if not v:
        return ""

    # Full URL => extract path after .net/
    if f"://{s.BLOB_ACCOUNT}.blob.core.windows.net/" in v:
        try:
            path = v.split(".net/", 1)[1].split("?", 1)[0]
            if path.startswith(s.BLOB_CONTAINER + "/"):
                return path[len(s.BLOB_CONTAINER) + 1 :]
            # if it’s some other container, return everything after first slash
            return path.split("/", 1)[1]
        except Exception:
            pass

    # blobPath "container/blob"
    prefix = s.BLOB_CONTAINER + "/"
    if v.startswith(prefix):
        return v[len(prefix):]

    # blobName already
    return v

def _sanitize_doc_urls(s: Settings, doc: Dict[str, Any]) -> None:
    """
    Keep fileUrl SAS-free for DISPLAY only.
    UI should use /items/<id>/download to actually open/download the file.
    """
    bp_ = doc.get("blobPath")
    fu = doc.get("fileUrl")
    if isinstance(fu, str) and fu:
        doc["fileUrl"] = _strip_sas(fu)
    elif bp_:
        doc["fileUrl"] = _public_blob_url(s, bp_)

def _pager_results(it, continuation: Optional[str]) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Cosmos v4.7: by_page(continuation_token=...) only.
    Page size must be set via iterator creation (max_item_count).
    """
    page_iter = it.by_page(continuation_token=continuation or None)
    first_page = next(page_iter, None)
    items: List[Dict[str, Any]] = []
    if first_page is not None:
        for doc in first_page:
            items.append(doc)
    next_token = getattr(page_iter, "continuation_token", None)
    return items, next_token

def _sas_url_for_blob(s: Settings, blob_name: str, *, hours: int = 1, filename: Optional[str] = None) -> str:
    """
    Mint a fresh READ SAS URL for a blobName (no container prefix).
    """
    if not blob_name:
        raise ValueError("blob_name is required")

    # Use UTC times for SAS
    start = datetime.datetime.utcnow() - datetime.timedelta(minutes=5)
    expiry = datetime.datetime.utcnow() + datetime.timedelta(hours=hours)

    disp_name = filename or blob_name.split("/")[-1]
    content_disposition = f'inline; filename="{disp_name}"'

    sas = generate_blob_sas(
        account_name=s.BLOB_ACCOUNT,
        container_name=s.BLOB_CONTAINER,
        blob_name=blob_name,
        account_key=s.BLOB_KEY,
        permission=BlobSasPermissions(read=True),
        start=start,
        expiry=expiry,
        content_disposition=content_disposition,
        content_type="application/pdf",
    )
    return f"https://{s.BLOB_ACCOUNT}.blob.core.windows.net/{s.BLOB_CONTAINER}/{blob_name}?{sas}"

# -------- health --------
@bp.get("/health")
def health():
    return jsonify({"ok": True})

# -------- auth (placeholder) --------
@bp.post("/auth/login")
def login():
    # your existing auth logic here
    return jsonify({"ok": True})

@bp.post("/auth/logout")
def logout():
    # your existing auth logic here
    return jsonify({"ok": True})

# -------- get item --------
@bp.get("/items/<string:item_id>")
def get_item(item_id: str):
    pk = request.args.get("pk")
    if not pk:
        return jsonify({"error": "Missing query param 'pk'."}), 400
    try:
        doc = container().read_item(item=item_id, partition_key=pk)
        _sanitize_doc_urls(settings(), doc)
        return jsonify(doc)
    except cos_ex.CosmosResourceNotFoundError:
        return jsonify({"error": "Not found"}), 404

# -------- download (FIXED) --------
@bp.get("/items/<string:item_id>/download")
def download_item_pdf(item_id: str):
    """
    Redirects to a freshly minted SAS URL for the item PDF.
    The UI should link to THIS endpoint (see buildDownloadUrl in api.js).
    """
    pk = request.args.get("pk")
    if not pk:
        return jsonify({"error": "Missing query param 'pk'."}), 400

    try:
        doc = container().read_item(item=item_id, partition_key=pk)
        s = settings()

        blob_path = doc.get("blobPath")

        # If no blobPath, derive it from fileUrl and persist it.
        if not blob_path:
            file_url = doc.get("fileUrl")
            if not file_url:
                return jsonify({"error": "No PDF attached"}), 404
            try:
                path = file_url.split(".net/", 1)[1].split("?", 1)[0]  # "container/blob" (or similar)
                doc["blobPath"] = path
                container().replace_item(doc, doc)
                blob_path = path
            except Exception:
                return jsonify({"error": "No blobPath and could not parse fileUrl"}), 500

        # Normalize to blobName (no container prefix)
        blob_name = _normalize_blob_name(s, blob_path)

        # Optional: if you have a display filename in doc
        filename = doc.get("pdfFilename") or blob_name.split("/")[-1]

        # Mint SAS and redirect
        url = _sas_url_for_blob(s, blob_name, hours=24, filename=filename)
        return redirect(url, code=302)

    except cos_ex.CosmosResourceNotFoundError:
        return jsonify({"error": "Not found"}), 404

# -------- search (one field) --------
@bp.get("/search/one")
def search_one():
    field = request.args.get("field")
    value = request.args.get("value")
    pk = request.args.get("pk")
    contains = request.args.get("contains", "false").lower() == "true"
    page_size = int(request.args.get("page_size") or "50")
    continuation = request.args.get("continuation")

    if not field or value is None:
        return jsonify({"error": "field and value are required"}), 400

    # Build query (parameterized)
    if contains:
        where = f"CONTAINS(c.{field}, @value, true)"
    else:
        where = f"c.{field} = @value"

    if pk:
        where = f"{where} AND c.pk = @pk"

    query = f"SELECT * FROM c WHERE {where}"
    params = [{"name": "@value", "value": value}]
    if pk:
        params.append({"name": "@pk", "value": pk})

    it = container().query_items(
        query=query,
        parameters=params,
        enable_cross_partition_query=(pk is None),
        max_item_count=page_size,
    )

    docs, next_token = _pager_results(it, continuation)
    s = settings()
    for d in docs:
        _sanitize_doc_urls(s, d)

    return jsonify({"items": docs, "continuation": next_token})

# -------- zip download (example) --------
@bp.get("/items/zip")
def zip_items():
    """
    Example: zip multiple PDFs by passing ids and pk.
    """
    # Your existing zip logic here if you have it
    return jsonify({"error": "Not implemented"}), 501

# -------- On-demand SAS for a single blob (kept) --------
@bp.get("/blob/sas")
def blob_sas():
    """
    Mint a fresh SAS for a given blob.
    Query:
      - path: required; may be blobName ('0196792-6/foo.pdf'), blobPath ('files/0196792-6/foo.pdf'), or full URL
      - filename: optional; suggests Content-Disposition filename
      - att: 1 to force attachment; otherwise inline
      - hours: int override for expiry (default 1h)
      - content_type: override content type (default application/octet-stream)
    """
    s = settings()
    path = request.args.get("path")
    if not path:
        return jsonify({"error": "path is required"}), 400

    filename = request.args.get("filename")
    as_attachment = request.args.get("att", "0") == "1"
    hours_raw = request.args.get("hours")
    content_type = request.args.get("content_type") or "application/octet-stream"

    try:
        hours = int(hours_raw) if hours_raw else 1
    except Exception:
        hours = 1

    blob_name = _normalize_blob_name(s, path)

    start = datetime.datetime.utcnow() - datetime.timedelta(minutes=5)
    expiry = datetime.datetime.utcnow() + datetime.timedelta(hours=hours)

    sas = generate_blob_sas(
        account_name=s.BLOB_ACCOUNT,
        container_name=s.BLOB_CONTAINER,
        blob_name=blob_name,
        account_key=s.BLOB_KEY,
        permission=BlobSasPermissions(read=True),
        start=start,
        expiry=expiry,
        content_disposition=(
            f'attachment; filename="{filename or blob_name.split("/")[-1]}"'
            if as_attachment
            else f'inline; filename="{filename or blob_name.split("/")[-1]}"'
        ),
        content_type=content_type,
    )
    url = f"https://{s.BLOB_ACCOUNT}.blob.core.windows.net/{s.BLOB_CONTAINER}/{blob_name}?{sas}"
    return jsonify({"url": url})

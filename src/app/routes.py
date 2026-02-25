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
from .blobstore import upload_pdf_and_get_sas, sas_url_for_blob_path  # upload still used; SAS not stored

from azure.storage.blob import BlobServiceClient, BlobClient, BlobSasPermissions, generate_blob_sas
from urllib.parse import urlsplit, urlunsplit  # <-- added

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

def _normalize_blob_name(s: Settings, value: str) -> str:
    v = value.strip()
    if "://" + s.BLOB_ACCOUNT + ".blob.core.windows.net/" in v:
        try:
            path = v.split(".net/", 1)[1].split("?", 1)[0]
            if path.startswith(s.BLOB_CONTAINER + "/"):
                return path[len(s.BLOB_CONTAINER) + 1 :]
            return path.split("/", 1)[1]
        except Exception:
            pass
    prefix = s.BLOB_CONTAINER + "/"
    if v.startswith(prefix):
        return v[len(prefix):]
    return v

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

# ---- NEW: URL utilities ----
def _strip_sas(url: str) -> str:
    """Remove any query/fragment from a blob URL (drop SAS)."""
    if not url:
        return url
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))

def _public_blob_url(s: Settings, blob_path_or_name: str) -> str:
    """
    Build a public (no-SAS) URL from:
      - a full URL (strip SAS),
      - a blobPath like 'files/dir/file.pdf', or
      - a blobName like 'dir/file.pdf'
    """
    v = (blob_path_or_name or "").strip()

    # If already a full URL, just strip SAS and return.
    if v.startswith("http://") or v.startswith("https://"):
        return _strip_sas(v)

    # If caller passed 'files/dir/file.pdf' keep as-is, otherwise prefix container.
    if not v.startswith(s.BLOB_CONTAINER + "/"):
        v = f"{s.BLOB_CONTAINER}/{v}"

    return f"https://{s.BLOB_ACCOUNT}.blob.core.windows.net/{v}"

def _sanitize_doc_urls(s: Settings, doc: Dict[str, Any]) -> None:
    """
    Ensure doc['fileUrl'] is SAS-free and present if blobPath exists.
    """
    bp_ = doc.get("blobPath")
    fu = doc.get("fileUrl")
    if isinstance(fu, str) and fu:
        doc["fileUrl"] = _strip_sas(fu)
    elif bp_:
        doc["fileUrl"] = _public_blob_url(s, bp_)

# -------- health --------
@bp.get("/health")
def health():
    return jsonify({"ok": True})

# -------- items: list / get --------
@bp.get("/items")
def list_items():
    """
    List items by partition key (fast) or run an ad-hoc WHERE with `q`.
      - /items?pk=<partition-key>&page_size=10
      - /items?q=SELECT ... FROM c WHERE ...&page_size=10   (or just WHERE-clause)
    Supports continuation tokens via ?continuation=<token>
    """
    args = request.args
    page_size = int(args.get("page_size", "25") or "25")
    continuation = args.get("continuation")

    pk = args.get("pk")
    q = args.get("q")

    if q:
        query = q.strip()
        if not query.lower().startswith("select"):
            query = f"SELECT * FROM c WHERE {query}"
        params: List[Dict[str, Any]] = []
        if pk:
            if " where " in query.lower():
                query += " AND c.pk = @pk"
            else:
                query += " WHERE c.pk = @pk"
            params.append({"name": "@pk", "value": pk})
        it = container().query_items(
            query=query,
            parameters=params,
            enable_cross_partition_query=not bool(pk),
            max_item_count=page_size,
        )
    else:
        if not pk:
            return jsonify({"error": "Either 'pk' or 'q' is required."}), 400
        it = container().read_all_items(
            partition_key=pk,
            max_item_count=page_size,
        )

    items, next_token = _pager_results(it, continuation)

    # Return public (no-SAS) URLs
    s = settings()
    for doc in items:
        _sanitize_doc_urls(s, doc)

    return jsonify({"items": items, "continuation": next_token})

@bp.get("/items/<string:item_id>")
def get_item(item_id: str):
    pk = request.args.get("pk")
    if not pk:
        return jsonify({"error": "Missing query param 'pk'."}), 400
    try:
        doc = container().read_item(item=item_id, partition_key=pk)
        s = settings()
        _sanitize_doc_urls(s, doc)
        return jsonify(doc)
    except cos_ex.CosmosResourceNotFoundError:
        return jsonify({"error": "Not found"}), 404

# -------- items: create / upsert / delete --------
@bp.post("/items")
def create_item():
    """
    Create an item.
    Accepts:
      - JSON body: {"id": "...", "pk": "...", ...}
      - multipart/form-data with fields (id, pk, ...) and optional file=@<pdf>
        -> uploads PDF to Blob, stores blobPath, returns doc with fileUrl (public, no SAS)
    """
    is_multipart = bool(request.files) or (
        request.mimetype and request.mimetype.startswith("multipart/form-data")
    )

    if is_multipart:
        form = request.form.to_dict(flat=True)
        file = request.files.get("file")

        if not form.get("id") or not form.get("pk"):
            return jsonify({"error": "Body must include 'id' and 'pk'."}), 400

        doc: Dict[str, Any] = {**form}

        if file:
            s = settings()
            suffix = uuid.uuid4().hex[:8]
            blob_name = f"{form['pk']}/{form['id']}-{suffix}.pdf"
            pdf_bytes = file.read()

            # Upload using your existing helper (returns a SAS we don't store)
            _ = upload_pdf_and_get_sas(s, blob_name=blob_name, data=pdf_bytes)

            # Store stable public URL instead
            doc["blobPath"] = f"{s.BLOB_CONTAINER}/{blob_name}"
            doc["fileUrl"]  = _public_blob_url(s, doc["blobPath"])

        created = container().create_item(doc)
        # Ensure returned doc has sanitized URL too
        _sanitize_doc_urls(settings(), created)
        return jsonify(created), 201

    body = request.get_json(force=True, silent=True) or {}
    if "id" not in body or "pk" not in body:
        return jsonify({"error": "Body must include 'id' and 'pk'."}), 400

    # Normalize any incoming URL (strip SAS)
    s = settings()
    if "fileUrl" in body and isinstance(body["fileUrl"], str):
        body["fileUrl"] = _strip_sas(body["fileUrl"])
    elif body.get("blobPath"):
        body["fileUrl"] = _public_blob_url(s, body["blobPath"])

    created = container().create_item(body)
    _sanitize_doc_urls(s, created)
    return jsonify(created), 201

@bp.put("/items/<string:item_id>")
def upsert_item(item_id: str):
    body = request.get_json(force=True, silent=True) or {}
    pk = body.get("pk") or request.args.get("pk")
    if not pk:
        return jsonify({"error": "Request must include 'pk' (body or query)."}), 400

    # Normalize any incoming URL (strip SAS) before upsert
    s = settings()
    if "fileUrl" in body and isinstance(body["fileUrl"], str):
        body["fileUrl"] = _strip_sas(body["fileUrl"])
    elif body.get("blobPath") and not body.get("fileUrl"):
        body["fileUrl"] = _public_blob_url(s, body["blobPath"])

    body["id"] = item_id
    try:
        existing = container().read_item(item=item_id, partition_key=pk)
        if "blobPath" not in body and "blobPath" in existing:
            body["blobPath"] = existing["blobPath"]
            # refresh fileUrl from blobPath if not supplied
            if "fileUrl" not in body:
                body["fileUrl"] = _public_blob_url(s, body["blobPath"])
        replaced = container().replace_item(existing, body)
        _sanitize_doc_urls(s, replaced)
        return jsonify(replaced)
    except cos_ex.CosmosResourceNotFoundError:
        created = container().create_item(body)
        _sanitize_doc_urls(s, created)
        return jsonify(created), 201

@bp.delete("/items/<string:item_id>")
def delete_item(item_id: str):
    pk = request.args.get("pk")
    if not pk:
        return jsonify({"error": "Missing query param 'pk'."}), 400
    try:
        container().delete_item(item=item_id, partition_key=pk)
        return ("", 204)
    except cos_ex.CosmosResourceNotFoundError:
        return jsonify({"error": "Not found"}), 404

# -------- download redirect (prefer public URL) --------
@bp.get("/items/<string:item_id>/download")
def download_item_pdf(item_id: str):
    pk = request.args.get("pk")
    if not pk:
        return jsonify({"error": "Missing query param 'pk'."}), 400
    try:
        doc = container().read_item(item=item_id, partition_key=pk)
        blob_path = doc.get("blobPath")

        # If no blobPath, try to derive from existing URL and persist it.
        if not blob_path:
            file_url = doc.get("fileUrl")
            if not file_url:
                return jsonify({"error": "No PDF attached"}), 404
            try:
                path = file_url.split(".net/", 1)[1].split("?", 1)[0]
                doc["blobPath"] = path
                container().replace_item(doc, doc)
                blob_path = path
            except Exception:
                return jsonify({"error": "No blobPath and could not parse fileUrl"}), 500

        # Redirect to public (no-SAS) URL
        url = _public_blob_url(settings(), blob_path)
        return redirect(url, code=302)
    except cos_ex.CosmosResourceNotFoundError:
        return jsonify({"error": "Not found"}), 404

# -------- search (multi-field) --------
@bp.get("/search")
def search_items():
    """
    Multi-field search by any combination of:
      clientId, portEntry, scac, mastBillNum, shipperRefNum, relDate
    Optional:
      pk=<partition key>
      contains=true (substring match for text fields; ignored for relDate)
      page_size, continuation
    """
    args = request.args
    page_size = int(args.get("page_size", "25") or "25")
    continuation = args.get("continuation")
    pk = args.get("pk")
    use_contains = args.get("contains", "false").lower() == "true"

    fields_map = {
        "clientId": "ClientID",
        "portEntry": "PortEntry",
        "scac": "SCAC",
        "mastBillNum": "MastBillNum",
        "shipperRefNum": "ShipperRefNum",
        "relDate": "RelDate",
    }

    where: List[str] = []
    params: List[Dict[str, Any]] = []

    if pk:
        where.append("c.pk = @pk")
        params.append({"name": "@pk", "value": pk})

    for qs_name, prop in fields_map.items():
        val = args.get(qs_name)
        if not val:
            continue
        pname = f"@{qs_name}"
        if use_contains and qs_name != "relDate":
            where.append(f"CONTAINS(c.{prop}, {pname}, true)")
        else:
            where.append(f"c.{prop} = {pname}")
        params.append({"name": pname, "value": val})

    query = "SELECT * FROM c"
    if where:
        query += " WHERE " + " AND ".join(where)

    it = container().query_items(
        query=query,
        parameters=params,
        enable_cross_partition_query=not bool(pk),
        max_item_count=page_size,
    )

    items, next_token = _pager_results(it, continuation)

    # Public URLs on results
    s = settings()
    for doc in items:
        _sanitize_doc_urls(s, doc)

    return jsonify({"items": items, "continuation": next_token})

# -------- search (single-field) --------
@bp.get("/search/one")
def search_one():
    """
    Search by EXACTLY ONE of: ClientID, PortEntry, SCAC, MastBillNum, ShipperRefNum, RelDate.
    Optional:
      - pk=<partition key>
      - contains=true (substring match for text fields; ignored for RelDate)
      - page_size, continuation
    """
    args = request.args
    page_size = int(args.get("page_size", "25") or "25")
    continuation = args.get("continuation")
    pk = args.get("pk")
    use_contains = args.get("contains", "false").lower() == "true"

    fields_map = {
        "ClientID": "ClientID",
        "PortEntry": "PortEntry",
        "SCAC": "SCAC",
        "MastBillNum": "MastBillNum",
        "ShipperRefNum": "ShipperRefNum",
        "RelDate": "RelDate",
    }

    field = args.get("field")
    value = args.get("value")

    if not field or field not in fields_map:
        return jsonify({"error": "Query must include field=<ClientID|PortEntry|SCAC|MastBillNum|ShipperRefNum|RelDate>"}), 400
    if value is None or value == "":
        return jsonify({"error": "Query must include value=<search value>"}), 400

    where: List[str] = []
    params: List[Dict[str, Any]] = []

    if pk:
        where.append("c.pk = @pk")
        params.append({"name": "@pk", "value": pk})

    prop = fields_map[field]
    if use_contains and field != "RelDate":
        where.append(f"CONTAINS(c.{prop}, @val, true)")
    else:
        where.append(f"c.{prop} = @val")
    params.append({"name": "@val", "value": value})

    query = "SELECT * FROM c WHERE " + " AND ".join(where)

    it = container().query_items(
        query=query,
        parameters=params,
        enable_cross_partition_query=not bool(pk),
        max_item_count=page_size,
    )

    items, next_token = _pager_results(it, continuation)

    # Public URLs on results
    s = settings()
    for doc in items:
        _sanitize_doc_urls(s, doc)

    return jsonify({"items": items, "continuation": next_token})

# -------- Batch ZIP download (server-side) --------
@bp.post("/download/batch")
def download_batch_zip():
    """
    Body: { "items": [ { "blobName": "...", "filename": "..." }, { "blobPath": "files/..." } ] }
    Returns: application/zip with all requested blobs.
    """
    data = request.get_json(silent=True) or {}
    items = data.get("items", [])
    if not isinstance(items, list) or not items:
        return jsonify({"error": "No items provided"}), 400

    s = settings()
    bsvc = _blob_service(s)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for it in items:
            it = it or {}
            raw = it.get("blobName") or it.get("blobPath")
            if not raw:
                continue
            try:
                blob_name = _normalize_blob_name(s, raw)
                filename = it.get("filename") or blob_name.split("/")[-1]
                bc: BlobClient = bsvc.get_blob_client(container=s.BLOB_CONTAINER, blob=blob_name)
                stream = bc.download_blob()
                zf.writestr(filename, stream.readall())
            except Exception as ex:
                fallback_name = (it.get("filename") or "file") + ".ERROR.txt"
                zf.writestr(fallback_name, f"Failed to download '{raw}': {ex}")

    buf.seek(0)
    stamp = datetime.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    zip_name = f"ryker-files-{stamp}.zip"
    headers = {
        "Content-Type": "application/zip",
        "Content-Disposition": f'attachment; filename="{zip_name}"',
        "Cache-Control": "no-store",
    }
    return Response(buf.getvalue(), headers=headers)

# -------- On-demand SAS for a single blob --------
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
        content_disposition=(f'attachment; filename="{filename or blob_name.split("/")[-1]}"' if as_attachment
                             else f'inline; filename="{filename or blob_name.split("/")[-1]}"'),
        content_type=content_type,
    )
    url = f"https://{s.BLOB_ACCOUNT}.blob.core.windows.net/{s.BLOB_CONTAINER}/{blob_name}?{sas}"
    return jsonify({"url": url})

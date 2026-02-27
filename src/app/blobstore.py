# app/blobstore.py
from datetime import datetime, timedelta, timezone
from azure.storage.blob import (
    BlobServiceClient, ContentSettings, generate_blob_sas, BlobSasPermissions
)
from .settings import Settings

def _svc(settings: Settings) -> BlobServiceClient:
    if settings.AZURE_STORAGE_CONNECTION_STRING:
        return BlobServiceClient.from_connection_string(settings.AZURE_STORAGE_CONNECTION_STRING)
    if settings.AZURE_STORAGE_ACCOUNT and settings.AZURE_STORAGE_KEY:
        url = f"https://{settings.AZURE_STORAGE_ACCOUNT}.blob.core.windows.net"
        return BlobServiceClient(account_url=url, credential=settings.AZURE_STORAGE_KEY)
    raise RuntimeError("Blob storage credentials are not configured")


def upload_bytes_and_get_sas(
    settings: Settings,
    *,
    blob_name: str,
    data: bytes,
    content_type: str,
    download_name: str | None = None,
) -> str:
    """
    Upload bytes to <BLOB_CONTAINER>/<blob_name> and return a read-only SAS URL.

    content_type examples:
      - application/pdf
      - text/plain; charset=utf-8
    download_name:
      - If set, forces browser download name via Content-Disposition.
      - If None, browser decides how to render (PDF viewer / text viewer).
    """
    bsc = _svc(settings)
    container = bsc.get_container_client(settings.BLOB_CONTAINER)

    # create container if it doesn't exist
    try:
        container.create_container()
    except Exception:
        pass

    container.upload_blob(
        name=blob_name,
        data=data,
        overwrite=True,
        content_settings=ContentSettings(content_type=content_type),
    )

    ttl = settings.BLOB_SAS_TTL_MINUTES

    # Optional: force a download filename
    content_disposition = None
    if download_name:
        # "attachment" forces download; "inline" tries to render in-browser
        content_disposition = f'inline; filename="{download_name}"'

    sas = generate_blob_sas(
        account_name=bsc.account_name,
        container_name=settings.BLOB_CONTAINER,
        blob_name=blob_name,
        account_key=getattr(bsc.credential, "account_key", settings.AZURE_STORAGE_KEY),
        permission=BlobSasPermissions(read=True),
        start=datetime.now(timezone.utc) - timedelta(minutes=1),
        expiry=datetime.now(timezone.utc) + timedelta(minutes=ttl),

        # These override response headers when the SAS URL is used
        content_type=content_type,
        content_disposition=content_disposition,
    )

    return f"https://{bsc.account_name}.blob.core.windows.net/{settings.BLOB_CONTAINER}/{blob_name}?{sas}"


def sas_url_for_blob_path(
    settings: Settings,
    blob_path: str,
    *,
    content_type: str | None = None,
    download_name: str | None = None,
) -> str:
    """
    blob_path: "<container>/<blobName>"

    If content_type is provided, it forces the response Content-Type for the SAS URL.
    If download_name is provided, it sets Content-Disposition for the SAS URL.
    """
    bsc = _svc(settings)

    try:
        container_name, blob_name = blob_path.split("/", 1)
    except ValueError:
        raise ValueError("blob_path must be '<container>/<blobName>'")

    ttl = settings.BLOB_SAS_TTL_MINUTES

    content_disposition = None
    if download_name:
        content_disposition = f'inline; filename="{download_name}"'

    sas = generate_blob_sas(
        account_name=bsc.account_name,
        container_name=container_name,
        blob_name=blob_name,
        account_key=getattr(bsc.credential, "account_key", settings.AZURE_STORAGE_KEY),
        permission=BlobSasPermissions(read=True),
        start=datetime.now(timezone.utc) - timedelta(minutes=1),
        expiry=datetime.now(timezone.utc) + timedelta(minutes=ttl),

        content_type=content_type,
        content_disposition=content_disposition,
    )
    return f"https://{bsc.account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas}"

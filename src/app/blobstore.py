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

def upload_pdf_and_get_sas(settings: Settings, *, blob_name: str, data: bytes) -> str:
    """
    Uploads the PDF to <container>/<blob_name> and returns a read-only SAS URL.
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
        content_settings=ContentSettings(content_type="application/pdf"),
    )

    # Build SAS (account key based)
    ttl = settings.BLOB_SAS_TTL_MINUTES
    sas = generate_blob_sas(
        account_name=bsc.account_name,
        container_name=settings.BLOB_CONTAINER,
        blob_name=blob_name,
        account_key=getattr(bsc.credential, "account_key", settings.AZURE_STORAGE_KEY),
        permission=BlobSasPermissions(read=True),
        expiry=datetime.now(timezone.utc) + timedelta(minutes=ttl),
        start=datetime.now(timezone.utc) - timedelta(minutes=1),
    )
    return f"https://{bsc.account_name}.blob.core.windows.net/{settings.BLOB_CONTAINER}/{blob_name}?{sas}"


# add to the bottom of blobstore.py

def sas_url_for_blob_path(settings: Settings, blob_path: str) -> str:
    """
    blob_path: "<container>/<blobName>"
    returns: "https://<acct>.blob.core.windows.net/<container>/<blob>?<sas>"
    """
    bsc = _svc(settings)
    try:
        container_name, blob_name = blob_path.split("/", 1)
    except ValueError:
        raise ValueError("blob_path must be '<container>/<blobName>'")

    from datetime import datetime, timedelta, timezone
    from azure.storage.blob import generate_blob_sas, BlobSasPermissions

    sas = generate_blob_sas(
        account_name=bsc.account_name,
        container_name=container_name,
        blob_name=blob_name,
        account_key=getattr(bsc.credential, "account_key", settings.AZURE_STORAGE_KEY),
        permission=BlobSasPermissions(read=True),
        start=datetime.now(timezone.utc) - timedelta(minutes=1),
        expiry=datetime.now(timezone.utc) + timedelta(minutes=settings.BLOB_SAS_TTL_MINUTES),
    )
    return f"https://{bsc.account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas}"

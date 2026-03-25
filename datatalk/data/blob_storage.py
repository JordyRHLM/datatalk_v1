"""
Blob Storage — DataTalk
Guarda archivos subidos y PNGs de charts en Azure Blob Storage.
Reemplaza el storage local en producción.
"""
import os
import base64
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)


def _get_client():
    from azure.storage.blob import BlobServiceClient
    conn_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if not conn_str:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING no configurado en .env")
    return BlobServiceClient.from_connection_string(conn_str)


def _ensure_containers():
    """Crea los contenedores si no existen."""
    client = _get_client()
    for container in [
        os.environ.get("AZURE_STORAGE_CONTAINER_UPLOADS", "datatalk-uploads"),
        os.environ.get("AZURE_STORAGE_CONTAINER_CHARTS", "datatalk-charts"),
    ]:
        try:
            client.create_container(container)
            logger.info(f"Contenedor creado: {container}")
        except Exception:
            pass  # Ya existe


def upload_file(file_path: str, blob_name: str = None) -> str:
    """
    Sube un archivo local a Blob Storage.
    Retorna la URL del blob.
    """
    container = os.environ.get("AZURE_STORAGE_CONTAINER_UPLOADS", "datatalk-uploads")
    blob_name = blob_name or Path(file_path).name

    client = _get_client()
    container_client = client.get_container_client(container)

    with open(file_path, "rb") as f:
        container_client.upload_blob(blob_name, f, overwrite=True)

    account = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
    url = f"https://{account}.blob.core.windows.net/{container}/{blob_name}"
    logger.info(f"Archivo subido a Blob: {url}")
    return url


def upload_chart_png(png_base64: str, chart_name: str) -> str:
    """
    Sube un PNG (en base64) a Blob Storage.
    Retorna una URL pública con SAS válida por 2 horas.
    """
    from azure.storage.blob import generate_blob_sas, BlobSasPermissions

    container = os.environ.get("AZURE_STORAGE_CONTAINER_CHARTS", "datatalk-charts")
    blob_name = f"{chart_name}.png"
    account_name = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
    account_key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")

    png_bytes = base64.b64decode(png_base64)
    client = _get_client()
    container_client = client.get_container_client(container)
    container_client.upload_blob(blob_name, png_bytes, overwrite=True)

    # URL con SAS — válida 2 horas (para Teams Adaptive Cards)
    sas = generate_blob_sas(
        account_name=account_name,
        container_name=container,
        blob_name=blob_name,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.now(timezone.utc) + timedelta(hours=2),
    )
    url = f"https://{account_name}.blob.core.windows.net/{container}/{blob_name}?{sas}"
    logger.info(f"Chart PNG subido a Blob: {url}")
    return url


def list_uploaded_files() -> list[dict]:
    """Lista todos los archivos subidos con su URL y tamaño."""
    container = os.environ.get("AZURE_STORAGE_CONTAINER_UPLOADS", "datatalk-uploads")
    client = _get_client()
    container_client = client.get_container_client(container)
    account = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")

    files = []
    for blob in container_client.list_blobs():
        files.append({
            "name": blob.name,
            "size_kb": round(blob.size / 1024, 1),
            "last_modified": str(blob.last_modified),
            "url": f"https://{account}.blob.core.windows.net/{container}/{blob.name}",
        })
    return files


def download_to_memory(blob_name: str) -> bytes:
    """Descarga un archivo de Blob directo a memoria (sin guardarlo en disco)."""
    container = os.environ.get("AZURE_STORAGE_CONTAINER_UPLOADS", "datatalk-uploads")
    client = _get_client()
    blob_client = client.get_container_client(container).get_blob_client(blob_name)
    return blob_client.download_blob().readall()


def blob_available() -> bool:
    """Verifica si Blob Storage está configurado y accesible."""
    if not os.environ.get("AZURE_STORAGE_CONNECTION_STRING"):
        return False
    try:
        _get_client().list_containers(max_results=1)
        return True
    except Exception:
        return False
"""
Ruta de carga de archivos — sube Excel/CSV, lo limpia y lo registra en DuckDB.
"""
import os
import shutil
from fastapi import APIRouter, UploadFile, File, HTTPException
from datatalk.data.cleaner import clean_file
from datatalk.data.duck_engine import register_file
from datatalk.core.config import get_settings

router = APIRouter()
settings = get_settings()


@router.post("/")
async def upload_file(file: UploadFile = File(...)):
    """
    Sube un archivo Excel o CSV:
    1. Guarda el archivo en uploads/
    2. Limpieza ligera (fechas, nulls, duplicados, columnas)
    3. Registra como tabla en DuckDB
    4. Retorna schema detectado y reporte de limpieza
    """
    allowed = [".xlsx", ".xls", ".csv"]
    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in allowed:
        raise HTTPException(400, f"Formato no soportado. Usa: {allowed}")

    os.makedirs(settings.uploads_dir, exist_ok=True)
    file_path = os.path.join(settings.uploads_dir, file.filename)

    with open(file_path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    # Limpieza ligera
    df, report = clean_file(file_path)

    # Nombre de tabla = nombre de archivo sin extensión
    table_name = os.path.splitext(file.filename)[0].lower().replace(" ", "_")

    # Registrar en DuckDB
    import tempfile, pandas as pd
    clean_path = os.path.join(settings.uploads_dir, f"{table_name}_clean.parquet")
    df.to_parquet(clean_path, index=False)
    schema = register_file(table_name, clean_path)

    return {
        "table_name": table_name,
        "schema": schema,
        "cleaning_report": report,
    }

"""
Limpieza ligera al subir archivo — el "pipeline liviano" del proyecto.
Estandariza fechas, elimina filas vacías, normaliza nombres de columnas.
"""
import pandas as pd
import re
from pathlib import Path


def normalize_column_name(col: str) -> str:
    """Convierte 'Fecha Venta' → 'fecha_venta'"""
    col = col.strip().lower()
    col = re.sub(r"[áàä]", "a", col)
    col = re.sub(r"[éèë]", "e", col)
    col = re.sub(r"[íìï]", "i", col)
    col = re.sub(r"[óòö]", "o", col)
    col = re.sub(r"[úùü]", "u", col)
    col = re.sub(r"[ñ]", "n", col)
    col = re.sub(r"[^a-z0-9_]", "_", col)
    col = re.sub(r"_+", "_", col).strip("_")
    return col


def clean_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Limpia el DataFrame y retorna (df_limpio, reporte_de_cambios).
    """
    report = {"original_rows": len(df), "original_cols": len(df.columns), "changes": []}

    # 1. Normalizar nombres de columnas
    old_cols = df.columns.tolist()
    df.columns = [normalize_column_name(c) for c in df.columns]
    renamed = {o: n for o, n in zip(old_cols, df.columns) if o != n}
    if renamed:
        report["changes"].append({"type": "columns_renamed", "detail": renamed})

    # 2. Eliminar filas completamente vacías
    before = len(df)
    df = df.dropna(how="all")
    dropped = before - len(df)
    if dropped:
        report["changes"].append({"type": "empty_rows_removed", "count": dropped})

    # 3. Eliminar columnas completamente vacías
    empty_cols = df.columns[df.isna().all()].tolist()
    df = df.drop(columns=empty_cols)
    if empty_cols:
        report["changes"].append({"type": "empty_cols_removed", "cols": empty_cols})

    # 4. Detectar y parsear columnas de fecha automáticamente
    for col in df.columns:
        if any(kw in col for kw in ["fecha", "date", "dia", "mes"]):
            try:
                df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")
                report["changes"].append({"type": "date_parsed", "col": col})
            except Exception:
                pass

    # 5. Eliminar duplicados exactos
    before = len(df)
    df = df.drop_duplicates()
    dupes = before - len(df)
    if dupes:
        report["changes"].append({"type": "duplicates_removed", "count": dupes})

    report["final_rows"] = len(df)
    report["final_cols"] = len(df.columns)
    return df, report


def clean_file(file_path: str) -> tuple[pd.DataFrame, dict]:
    """Lee y limpia un archivo Excel o CSV."""
    path = Path(file_path)
    if path.suffix in [".xlsx", ".xls"]:
        df = pd.read_excel(file_path)
    elif path.suffix == ".csv":
        df = pd.read_csv(file_path)
    else:
        raise ValueError(f"Formato no soportado: {path.suffix}")
    return clean_dataframe(df)

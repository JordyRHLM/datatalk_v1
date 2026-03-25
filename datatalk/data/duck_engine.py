"""
DuckDB Engine — motor SQL analítico en memoria.
Es el "lake" que pide el reto: lee Excel, CSV, Parquet con SQL puro.
"""
import duckdb
import pandas as pd
from pathlib import Path
from typing import Optional
from datatalk.core.config import get_settings

settings = get_settings()
_conn: Optional[duckdb.DuckDBPyConnection] = None


def get_connection() -> duckdb.DuckDBPyConnection:
    """Retorna la conexión singleton a DuckDB."""
    global _conn
    if _conn is None:
        _conn = duckdb.connect(settings.duckdb_path)
    return _conn


def register_file(table_name: str, file_path: str) -> dict:
    """
    Registra un archivo Excel/CSV como tabla en DuckDB.
    Retorna el schema detectado.
    """
    conn = get_connection()
    path = Path(file_path)

    if path.suffix in [".xlsx", ".xls"]:
        df = pd.read_excel(file_path)
        conn.register(table_name, df)
    elif path.suffix == ".csv":
        conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_csv_auto('{file_path}')
        """)
    elif path.suffix == ".parquet":
        conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_parquet('{file_path}')
        """)
    else:
        raise ValueError(f"Formato no soportado: {path.suffix}")

    schema = conn.execute(f"DESCRIBE {table_name}").fetchdf()
    return schema.to_dict(orient="records")


def execute_sql(sql: str) -> pd.DataFrame:
    """Ejecuta SQL y retorna un DataFrame."""
    conn = get_connection()
    return conn.execute(sql).fetchdf()


def list_tables() -> list[str]:
    """Lista todas las tablas registradas."""
    conn = get_connection()
    result = conn.execute("SHOW TABLES").fetchdf()
    return result["name"].tolist() if not result.empty else []
